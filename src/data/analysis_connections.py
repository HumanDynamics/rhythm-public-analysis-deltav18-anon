from __future__ import absolute_import, division, print_function
import datetime

import pandas as pd
from config import *


def generate_analysis_connections_store_key(rssi_cutoff, table_name):
    """
    Generates a path to a connections table created using a specific rssi_cutoff
    :param rssi_cutoff:
    :param table_name:
    :return:
    """
    rssi_path = "_"+str(abs(rssi_cutoff)) # can't use "-" in key name
    return 'proximity/rssi'+rssi_path+'/'+table_name


def _analysis_create_m2m_filtered(rssi_cutoff):
    """
    Creates a table with the closest beacon for each member, and adds metadata to it
    :param rssi_cutoff:
    :return:
    """
    logger.info("Filtering m2m_comply, RSSI: {}".format(rssi_cutoff))
    m2m_comply = pd.read_hdf(analysis_store_path, 'proximity/member_to_member')
    m2m_comply_filtered = m2m_comply[m2m_comply.rssi_max >= rssi_cutoff].copy()

    store_key = generate_analysis_connections_store_key(rssi_cutoff, "m2m_comply_filtered")
    m2m_comply_filtered.to_hdf(analysis_store_path, store_key, mode="a", format="table", append=False)

    del m2m_comply
    return m2m_comply_filtered


def make_m2m_double_sided(m2m):
    """
    Takes a given (single sided) m2m table and makes it double sided. This means that for every a->b record, there will
    also be a b->a record
    :param m2m:
    :return:
    """
    m2m = m2m.reset_index()
    m2m_reversed = m2m.copy()
    m2m_reversed['temp'] = m2m_reversed['member2']
    m2m_reversed['member2'] = m2m_reversed['member1']
    m2m_reversed['member1'] = m2m_reversed['temp']
    del m2m_reversed['temp']
    m2m_dbl = m2m.append(m2m_reversed).set_index(['datetime','member1','member2'])
    return m2m_dbl


def add_companies_to_m2m(m2m):
    """
    Adds company names ot both sides of a m2m table
    :param m2m: m2m table. Index should be datetime,member1,member2
    :return:
    """
    members_store_key = "metadata/members"
    members = pd.read_hdf(analysis_store_path, members_store_key)

    m2m_with_company = pd.merge(left=m2m.reset_index(), right=members[['company']], left_on='member1', right_index=True)
    m2m_with_company = m2m_with_company.rename(columns={'company': 'company1'})
    m2m_with_company = pd.merge(left=m2m_with_company.reset_index(), right=members[['company']], left_on='member2',
                                right_index=True)
    m2m_with_company = m2m_with_company.rename(columns={'company': 'company2'})
    return m2m_with_company.set_index(['datetime','member1','member2'])


def _analysis_create_m2m_dbl(m2m, rssi_cutoff):
    logger.info("Making m2m double sided. Size before: {}".format(len(m2m)))
    m2m_dbl = make_m2m_double_sided(m2m)
    m2m_dbl['minutes'] = int(time_bins_size[:-1])/60
    store_key = generate_analysis_connections_store_key(rssi_cutoff, "m2m_dbl")
    m2m_dbl.to_hdf(analysis_store_path, store_key, mode="a", format="table", append=False)
    logger.info("Making m2m double sided. Size after: {}".format(len(m2m_dbl)))
    return m2m_dbl


def _analysis_agg_m2m(m2m, rssi_cutoff, freq, side1_column, side2_column):
    """
    Aggregated the m2m table to a certain level
    :param m2m table. level 0 should be the datetime
    :param rssi_cutoff:
    :param freq frequency (e.g. - D,W,etc)
    :param side1_column first column for group by (e.g. member 1 or company)
    :param side2_column second column for group by (e.g. member 2 or company)
    :return:
    """
    logger.info("Aggregating, frequency: {}, RSSI: {}".format(freq, rssi_cutoff))

    # Groupper is the way to go, because I don't care about missing values (as opposed to resample)
    m2m_agg = m2m.groupby([
        pd.Grouper(level=0, freq=freq), # level 0 should be datetime
        side1_column, side2_column
    ])[['minutes']].sum()

    #m2m_agg['minutes'] = m2m_agg['minutes'] * 15 / 60
    logger.info("Records: {}".format(len(m2m_agg)))
    return m2m_agg


def _analysis_create_m2m_dbl_daily(m2m_dbl, rssi_cutoff):
    """
    Aggregated the m2m table to a daily level
    :param m2m_dbl:
    :param rssi_cutoff:
    :return:
    """
    m2m_dbl_daily = _analysis_agg_m2m(m2m=m2m_dbl, rssi_cutoff=rssi_cutoff, freq='D', side1_column='member1', side2_column='member2')

    out_store_key = generate_analysis_connections_store_key(rssi_cutoff, "m2m_dbl_daily")
    m2m_dbl_daily.to_hdf(analysis_store_path, out_store_key, mode="a", format="table", append=False)
    return m2m_dbl_daily


def _analysis_create_m2m_dbl_annual(m2m_dbl, rssi_cutoff):
    """
    Aggregated the m2m table to a annual level
    :param m2m_dbl:
    :param rssi_cutoff:
    :return:
    """

    m2m_dbl_annual = _analysis_agg_m2m(m2m=m2m_dbl, rssi_cutoff=rssi_cutoff, freq='AS', side1_column='member1', side2_column='member2')

    out_store_key = generate_analysis_connections_store_key(rssi_cutoff, "m2m_dbl_annual")
    m2m_dbl_annual.to_hdf(analysis_store_path, out_store_key, mode="a", format="table", append=False)
    return m2m_dbl_annual


def _analysis_create_m2c_daily(m2m_dbl_with_company, rssi_cutoff):
    """
    Aggregated the m2m table to a daily level, with company on side 2
    :param m2m_dbl_with_company: a m2m table with company info on both sides
    :param rssi_cutoff:
    :return:
    """
    m2c_daily = _analysis_agg_m2m(m2m=m2m_dbl_with_company, rssi_cutoff=rssi_cutoff, freq='D', side1_column='member1', side2_column='company2')

    out_store_key = generate_analysis_connections_store_key(rssi_cutoff, "m2c_daily")
    m2c_daily.to_hdf(analysis_store_path, out_store_key, mode="a", format="table", append=False)
    return m2c_daily


def _analysis_create_m2c_annual(m2m_dbl_with_company, rssi_cutoff):
    """
    Aggregated the m2m table to a member2company, annual level
    :param m2m_dbl_with_company: a m2m table with company info on both sides
    :param rssi_cutoff:
    :return:
    """
    m2c_annual = _analysis_agg_m2m(m2m=m2m_dbl_with_company, rssi_cutoff=rssi_cutoff, freq='AS', side1_column='member1', side2_column='company2')

    out_store_key = generate_analysis_connections_store_key(rssi_cutoff, "m2c_annual")
    m2c_annual.to_hdf(analysis_store_path, out_store_key, mode="a", format="table", append=False)
    return m2c_annual


def _analysis_create_c2c_dbl_daily(m2m_dbl_with_company, rssi_cutoff):
    """
    Aggregated the m2m table to a company2company, daily level
    :param m2m_dbl_with_company: a m2m table with company info on both sides
    :param rssi_cutoff:
    :return:
    """
    c2c_dbl_daily = _analysis_agg_m2m(m2m=m2m_dbl_with_company, rssi_cutoff=rssi_cutoff, freq='D', side1_column='company1', side2_column='company2')

    # If we have the same company on both sides, we counted twice. So need to divide by 2
    c2c_dbl_daily = c2c_dbl_daily.reset_index()
    same_company_cond = (c2c_dbl_daily.company1 == c2c_dbl_daily.company2)
    c2c_dbl_daily.loc[same_company_cond, 'minutes'] = c2c_dbl_daily.loc[same_company_cond, 'minutes'] / 2
    c2c_dbl_daily.set_index(['datetime','company1','company2'], inplace=True)

    out_store_key = generate_analysis_connections_store_key(rssi_cutoff, "c2c_dbl_daily")
    c2c_dbl_daily.to_hdf(analysis_store_path, out_store_key, mode="a", format="table", append=False)
    return c2c_dbl_daily


def _analysis_create_c2c_dbl_annual(m2m_dbl_with_company, rssi_cutoff):
    """
    Aggregated the m2m table to a company2company, annual level
    :param m2m_dbl_with_company: a m2m table with company info on both sides
    :param rssi_cutoff:
    :return:
    """
    c2c_dbl_annual = _analysis_agg_m2m(m2m=m2m_dbl_with_company, rssi_cutoff=rssi_cutoff, freq='AS', side1_column='company1', side2_column='company2')

    # If we have the same company on both sides, we counted twice. So need to divide by 2
    c2c_dbl_annual = c2c_dbl_annual.reset_index()
    same_company_cond = (c2c_dbl_annual.company1 == c2c_dbl_annual.company2)
    c2c_dbl_annual.loc[same_company_cond, 'minutes'] = c2c_dbl_annual.loc[same_company_cond, 'minutes'] / 2
    c2c_dbl_annual.set_index(['datetime','company1','company2'], inplace=True)

    out_store_key = generate_analysis_connections_store_key(rssi_cutoff, "c2c_dbl_annual")
    c2c_dbl_annual.to_hdf(analysis_store_path, out_store_key, mode="a", format="table", append=False)
    return c2c_dbl_annual


def analysis_connections():
    """
    Creates connection tables in multiple levels
    :return:
    """
    logger.info("Analysis - connections")

    for rssi_cutoff in rssi_cutoffs:
        logger.info("##### RSSI cutoff: {}".format(rssi_cutoff))
        # m2m
        logger.info("Creating m2m tables")
        m2m_comply_filtered = _analysis_create_m2m_filtered(rssi_cutoff)
        _analysis_create_m2m_dbl(m2m_comply_filtered, rssi_cutoff)
        del m2m_comply_filtered

        m2m_dbl_store_key = generate_analysis_connections_store_key(rssi_cutoff, "m2m_dbl")
        m2m_dbl = pd.read_hdf(analysis_store_path, m2m_dbl_store_key)

        _analysis_create_m2m_dbl_daily(m2m_dbl, rssi_cutoff)
        _analysis_create_m2m_dbl_annual(m2m_dbl, rssi_cutoff)

        # m2c (member to company)
        logger.info("Creating m2c tables")
        m2m_dbl_with_company = add_companies_to_m2m(m2m_dbl)
        _analysis_create_m2c_daily(m2m_dbl_with_company, rssi_cutoff)
        _analysis_create_m2c_annual(m2m_dbl_with_company, rssi_cutoff)

        # c2c (company to company)
        logger.info("Creating c2c tables")
        _analysis_create_c2c_dbl_daily(m2m_dbl_with_company, rssi_cutoff)
        _analysis_create_c2c_dbl_annual(m2m_dbl_with_company, rssi_cutoff)

        logger.info('---------------------------------------')
        logger.info('Completed analysis connections!')



