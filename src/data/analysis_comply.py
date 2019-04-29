from __future__ import absolute_import, division, print_function
import datetime

import pandas as pd
from config import *


def _analysis_m1cb(m5cb, members_metadata, beacons_metadata):
    """
    Creates a table with the closest beacon for each member, and adds metadata to it
    :param m5cb:
    :param members_metadata:
    :param beacons_metadata:
    :return:
    """
    m1cb = m5cb[['beacon_0', 'rssi_0']].rename(columns={'beacon_0': 'beacon', 'rssi_0': 'rssi'}).reset_index()

    # Add beacon metadata
    m1cb = m1cb \
        .join(beacons_metadata[['company', 'type']], on='beacon').rename(
        columns={'company': 'beacon_company', 'type': 'beacon_type'})

    # add badge metadata
    m1cb = m1cb.join(members_metadata[['company']], on='member').rename(columns={'company': 'member_company'})

    # setting location type
    # Is it the badge's company beacon or not?
    logger.info("Preparing m1cb - setting location type")

    def set_location_type(row):
        if row['beacon_type'] == 'company':
            if row['member_company'] == row['beacon_company']:
                return 'at company'
            else:
                return 'at different company'
        else:
            return row['beacon_type']

    m1cb['location_type'] = m1cb.apply(set_location_type, axis=1)

    # Add nearby companies data
    # Checking if the "different company" beacon is a nearby beacon.
    logger.info("Preparing m1cb - adding nearby data")
    nearby_companies = beacons_metadata.reset_index().set_index('company').query('type=="company"') \
        ['nearby_companies'].fillna("")

    nearby_companies_dict = {}
    for company, nc in nearby_companies.iteritems():
        nearby_companies_dict[company] = set(nc.split(","))

    def set_nearby_company(row):
        if row['location_type'] == 'at different company':
            # handle funny cases for participants with no home beacon (example - researchers)
            if row['member_company'] not in nearby_companies_dict:
                return 'at far company'

            # regular case
            if row['beacon_company'] in nearby_companies_dict[row['member_company']]:
                return 'at nearby company'
            else:
                return 'at far company'
        else:
            return row['location_type']

    m1cb['location_type_nearby'] = m1cb.apply(set_nearby_company, axis=1)

    # If at nearby company, mark it as "at company"
    logger.info("Preparing m1cb - merging nearby and at company")

    def set_nearby_company_merged(row):
        if row['location_type_nearby'] == 'at nearby company':
            return 'at company'
        else:
            return row['location_type_nearby']

    m1cb['location_type_merged'] = m1cb.apply(set_nearby_company_merged, axis=1)
    m1cb.set_index(['datetime', 'member'], inplace=True)
    m1cb.sort_index(inplace=True)
    return m1cb


def _m5cb_method(m1cb):
    """
    Helper, computes a series with boolean compliance at each
    time/member bin using closest beacon (either board or not)
    """
    cb_comply = m1cb.copy()
    cb_comply['comply'] = False
    cb_comply.loc[cb_comply.beacon_type != 'board', 'comply'] = True
    return cb_comply['comply']


def _max_rssi_method(m2badge, board_threshold):
    """
    Helper, computes a series with boolean for compliance at
    each time/member bin using the max RSSI from nearby members (but not beacons)
    If the max_rssi is lower than the threshold (that is, "far"), the badge
    is not on the board.
    """
    # this is a hack. member ids have observed id < 16000
    # using the m2badge before cleaning to ensure we get all possible badges
    m2badge_members_only = m2badge.reset_index()
    m2badge_members_only = m2badge_members_only[m2badge_members_only.observed_id < 16000]
    m2badge_members_only = m2badge_members_only[['datetime', 'member', 'rssi']]

    # Find closest member badge
    maxrssi_comply = m2badge_members_only.groupby(['datetime', 'member'])
    maxrssi_comply = maxrssi_comply.agg('max').rename(columns={"rssi": "max_rssi"})  # .rename('max_rssi')
    maxrssi_comply.head()

    # Mark compliance based on max RSSI
    maxrssi_comply['comply'] = False
    maxrssi_comply.loc[maxrssi_comply.max_rssi <= board_threshold, 'comply'] = True
    maxrssi_comply.head()
    return maxrssi_comply['comply']


# filling the gaps in the series
def _fill_gaps_in_comply(s, time_bins_size, na_value):
    """
    Fills in gaps in compliance series
    """
    df = s.reset_index(level='member') # Removes the level "member" from index, keeps time
    df = df.groupby(['member']) \
        [['comply']] \
        .resample(time_bins_size) \
        .asfreq()

    df = df.fillna(value=na_value)  # If NaN, member has not complied
    df = df.reset_index().set_index(['datetime', 'member'])
    df.sort_index(inplace=True)
    return df['comply']


def _analysis_compliance(m2badge, m1cb, board_threshold=-48):
    """
    Determines compliance from a combination of the closest beacon
    and the average RSSI value from nearby badges.
    """

    # Calculate using the two conditions
    c_cb = _m5cb_method(m1cb)
    c_maxrssi = _max_rssi_method(m2badge, board_threshold)

    # If no closest beacon, mark as didn't comply. But if there's no closest member, mask as comply.
    # Remember that we require both conditions to be True to mark the time as complied
    c_cb_fill = _fill_gaps_in_comply(c_cb, time_bins_size, na_value=False)
    c_maxrssi_fill = _fill_gaps_in_comply(c_maxrssi, time_bins_size, na_value=True)
    del c_cb
    del c_maxrssi

    # combine the two methods. Use c_cb as the leading table
    combo = c_cb_fill & c_maxrssi_fill.reindex(c_cb_fill.index).fillna(value=True)
    del c_cb_fill
    del c_maxrssi_fill

    return combo


def _analysis_m2m_comply(m2m, m_comply):
    """ Removes m2m records in which one of the sides is on the board
    Parameters
    ----------
    m2m : Member to member
    m_comply : Compliance table (for each timeslice, whether the badge was worn or not)

    Returns
    -------
    pd.DataFrame :
        m2m when no side is on the board.
    """
    # add on_board indicator
    df = m2m.copy().reset_index()
    df = df.join(m_comply, on=['datetime', 'member1']).rename(columns={'comply': 'comply1'})
    df = df.set_index("datetime").reset_index()  # I don't know why, but if you don't this this it acts funny
    df = df.join(m_comply, on=['datetime', 'member2']).rename(columns={'comply': 'comply2'})

    # filter
    df = df[(df.comply1 == True) & (df.comply2 == True)]
    df.dropna(inplace=True)

    # cleanup
    df.drop('comply1', axis=1, inplace=True)
    df.drop('comply2', axis=1, inplace=True)
    df = df.reset_index().set_index(['datetime', 'member1', 'member2'])
    df.sort_index(inplace=True)
    return df


def _analyze_day(start_ts, end_ts):
    members_metadata = pd.read_csv(members_metadata_path).set_index('member')
    beacons_metadata = pd.read_csv(beacons_metadata_path).set_index('beacon')

    where = "datetime >= '" + str(start_ts) + "' & datetime < '" + str(end_ts) + "'"

    logger.info("Loading m2badge")
    m2badge = pd.read_hdf(dirty_store_path, 'proximity/member_to_badge', where=where)

    logger.info("Loading m5cb")
    m5cb = pd.read_hdf(clean_store_path, 'proximity/member_5_closest_beacons', where=where)

    logger.info("Loading m5cb from dirty")
    m5cb_dirty = pd.read_hdf(dirty_store_path, 'proximity/member_5_closest_beacons', where=where)

    logger.info('loading m2m')
    m2m = pd.read_hdf(clean_store_path, 'proximity/member_to_member', where=where)

    # --- m1cb
    logger.info("Preparing m1cb")
    m1cb = _analysis_m1cb(m5cb, members_metadata, beacons_metadata)

    logger.info("Preparing m1cb from dirty")
    m1cb_dirty = _analysis_m1cb(m5cb_dirty, members_metadata, beacons_metadata)

    # --- m_onboard
    logger.info("Preparing compliance table")
    m_comply = _analysis_compliance(m2badge, m1cb, board_threshold=-48)

    logger.info("Preparing compliance table from dirty (to be used to determine participants' start day")
    m_comply_dirty = _analysis_compliance(m2badge, m1cb_dirty, board_threshold=-48)

    # --- m2m_ncomply
    logger.info("Preparing m2m_comply")
    m2m_comply = _analysis_m2m_comply(m2m, m_comply)
    logger.info("m2m_comply: before {}, after {}".format(len(m2m),len(m2m_comply)))

    logger.info("Saving data")
    store = pd.HDFStore(analysis_store_path)
    store.append('proximity/member_closest_beacon', m1cb)
    store.append('proximity/member_comply', m_comply)
    store.append('proximity/member_comply_dirty', m_comply_dirty)
    store.append('proximity/member_to_member', m2m_comply)
    store.close()

    del m2badge
    del m5cb
    del m1cb
    del m_comply


def analysis_comply():
    """
    Create compliance tables and use them to cleans main datasets.
    Create compliance tables and use them to cleans main datasets.
    :return:
    """
    logger.info("Analysis - comply")

    ##################################################
    # Create list of dates to process
    ##################################################
    # Convert text into timestamps with timezone
    period1_start_ts = pd.Timestamp(period1_start, tz=time_zone)
    period1_end_ts = pd.Timestamp(period1_end, tz=time_zone)
    period2_start_ts = pd.Timestamp(period2_start, tz=time_zone)
    period2_end_ts = pd.Timestamp(period2_end, tz=time_zone)

    # Create periods
    period1_dates = pd.date_range(start=period1_start_ts, end=period1_end_ts, normalize=True, name='start').to_series(
        keep_tz=True)
    period2_dates = pd.date_range(start=period2_start_ts, end=period2_end_ts, normalize=True, name='start').to_series(
        keep_tz=True)

    # First and last date should be the exact start/end times
    period1_dates[0] = pd.Timestamp(period1_start, tz=time_zone)
    period1_dates = period1_dates.append(
        pd.date_range(start=period1_end_ts, end=period1_end_ts, normalize=False, name='start').to_series(keep_tz=True))

    period2_dates[0] = pd.Timestamp(period2_start, tz=time_zone)
    period2_dates = period2_dates.append(
        pd.date_range(start=period2_end_ts, end=period2_end_ts, normalize=False, name='start').to_series(keep_tz=True))


    ##################################################
    # Analyse data, one day at a time
    ##################################################
    # period 1
    for i in range(0, len(period1_dates) - 1, 1):
        logger.info('---------------------------------------')
        logger.info("Analysis comply: {} - {}".format(period1_dates[i],period1_dates[i+1]))
        _analyze_day(period1_dates[i], period1_dates[i+1])

    # Period 2
    for i in range(0, len(period2_dates) - 1, 1):
        logger.info('---------------------------------------')
        logger.info("Analysis comply: {} - {}".format(period2_dates[i], period2_dates[i + 1]))
        _analyze_day(period2_dates[i], period2_dates[i+1])

    logger.info('---------------------------------------')
    logger.info('Completed analysis comply!')



