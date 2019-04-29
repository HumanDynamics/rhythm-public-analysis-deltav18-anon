from __future__ import absolute_import, division, print_function
import datetime

import pandas as pd
from config import *


def _drop_in_time_slice(m2m, m2b, m5cb, time_slice, to_drop):
    """Drops certain members from data structures, only in a given time slice.
    This can be useful for removing people who weren't there on a specific day, or non-participants.
    """
    logger.debug("Removing data: {} {}".format(time_slice, to_drop))
    m2m.drop(m2m.loc[(time_slice, slice(None), to_drop), :].index, inplace=True)
    m2m.drop(m2m.loc[(time_slice, to_drop, slice(None)), :].index, inplace=True)
    m2b.drop(m2b.loc[(time_slice, to_drop, slice(None)), :].index, inplace=True)
    m5cb.drop(m5cb.loc[(time_slice, to_drop), :].index, inplace=True)


def _clean_m2m(where, participation_dates, battery_sundays):
    logger.info('loading m2m')
    m2m = pd.read_hdf(dirty_store_path, 'proximity/member_to_member', where=where)
    logger.info("original m2m len: {}".format(len(m2m)))

    if len(m2m) == 0:
        return

    logger.info('cleaning m2m')
    m2m.reset_index(inplace=True)

    # Mark all records as not to keep. This removes all non-participants
    m2m['keep'] = False

    # For m2m, we need to look on both sides. Therefore, for each participating member, we will
    # turn on a "keep" flag if the member is valid on either sides of the connection. Then, we will only keep
    # records in which both sides are valid
    logger.info('Keeping only dates relevant dates for each participant')

    i = 0
    total_count = len(participation_dates)
    for item, p in participation_dates.iterrows():
        i += 1
        logger.debug("({}/{}) {},{},{}".format(i, total_count, p.member, p.start_date_ts, p.end_date_ts))

        side1_cond = ((m2m.member1 == p.member) & (m2m.datetime >= p.start_date_ts) & (m2m.datetime < p.end_date_ts))
        m2m.loc[side1_cond, 'keep_1'] = True

        side2_cond = ((m2m.member2 == p.member) & (m2m.datetime >= p.start_date_ts) & (m2m.datetime < p.end_date_ts))
        m2m.loc[side2_cond, 'keep_2'] = True

    m2m.loc[(m2m.keep_1 == True) & (m2m.keep_2 == True), 'keep'] = True
    del m2m['keep_1']
    del m2m['keep_2']
    logger.info('So far, keeping {} rows'.format(len(m2m[m2m['keep'] == True])))

    # Remove times of battery changes
    logger.info('Removing times of battery changes')
    i = 0
    total_count = len(battery_sundays)
    for item, s in battery_sundays.iterrows():
        i += 1

        logger.debug("({}/{}) {},{}".format(i, total_count, s.battery_period_start, s.battery_period_end))

        cond = ((m2m.datetime >= s.battery_period_start) & (m2m.datetime <= s.battery_period_end))
        m2m.loc[cond, 'keep'] = False
    logger.info('So far, keeping {} rows'.format(len(m2m[m2m['keep'] == True])))

    m2m = m2m[m2m.keep == True]
    logger.info("after cleaning: {}".format(len(m2m)))

    del m2m['keep']
    m2m.set_index(['datetime','member1','member2'], inplace=True)

    logger.info("appending cleaned m2m to {}".format(clean_store_path))
    with pd.HDFStore(clean_store_path) as store:
        store.append('proximity/member_to_member', m2m)
    del m2m


def _clean_m2b(where, participation_dates, battery_sundays):
    logger.info('loading m2b')
    m2b = pd.read_hdf(dirty_store_path, 'proximity/member_to_beacon', where=where)
    logger.info("original m2b len: {}".format(len(m2b)))

    if len(m2b) == 0:
        return

    logger.info("cleaning m2b")
    m2b.reset_index(inplace=True)

    # Mark all records as not to keep. This removes all non-participants
    m2b['keep'] = False

    # Only keep data within participation dates
    logger.info('Keeping only dates relevant dates for each participant')
    i = 0
    total_count = len(participation_dates)
    for item, p in participation_dates.iterrows():
        i += 1
        logger.debug("({}/{}) {},{},{}".format(i, total_count, p.member, p.start_date_ts, p.end_date_ts))
        side1_cond = ((m2b.member == p.member) & (m2b.datetime >= p.start_date_ts) & (m2b.datetime < p.end_date_ts))
        m2b.loc[side1_cond, 'keep'] = True

    logger.info('So far, keeping {} rows'.format(len(m2b[m2b['keep'] == True])))

    # Remove times of battery changes
    logger.info('Removing times of battery changes')
    i = 0
    total_count = len(battery_sundays)
    for item, s in battery_sundays.iterrows():
        i += 1
        logger.debug("({}/{}) {},{}".format(i, total_count, s.battery_period_start, s.battery_period_end))
        cond = ((m2b.datetime >= s.battery_period_start) & (m2b.datetime <= s.battery_period_end))
        m2b.loc[cond, 'keep'] = False
    logger.info('So far, keeping {} rows'.format(len(m2b[m2b['keep'] == True])))

    m2b = m2b[m2b.keep == True]
    logger.info("after cleaning: {}".format(len(m2b)))

    del m2b['keep']
    m2b.set_index(['datetime','member','beacon'], inplace=True)

    logger.info("appending cleaned m2b to {}".format(clean_store_path))
    with pd.HDFStore(clean_store_path) as store:
        store.append('proximity/member_to_beacon', m2b)
    del m2b


def _clean_m5cb(where, participation_dates, battery_sundays):
    logger.info('loading m2b')
    m5cb = pd.read_hdf(dirty_store_path, 'proximity/member_5_closest_beacons', where=where)
    logger.info("original m2b len: {}".format(len(m5cb)))

    if len(m5cb) == 0:
        return

    logger.info("cleaning m2b")
    m5cb.reset_index(inplace=True)

    # Mark all records as not to keep. This removes all non-participants
    m5cb['keep'] = False

    # Only keep data within participation dates
    logger.info('Keeping only dates relevant dates for each participant')
    i = 0
    total_count = len(participation_dates)
    for item, p in participation_dates.iterrows():
        i += 1
        logger.debug("({}/{}) {},{},{}".format(i, total_count, p.member, p.start_date_ts, p.end_date_ts))
        side1_cond = ((m5cb.member == p.member) & (m5cb.datetime >= p.start_date_ts) & (m5cb.datetime < p.end_date_ts))
        m5cb.loc[side1_cond, 'keep'] = True

    logger.info('So far, keeping {} rows'.format(len(m5cb[m5cb['keep'] == True])))

    # Remove times of battery changes
    logger.info('Removing times of battery changes')
    i = 0
    total_count = len(battery_sundays)
    for item, s in battery_sundays.iterrows():
        i += 1
        logger.debug("({}/{}) {},{}".format(i, total_count, s.battery_period_start, s.battery_period_end))
        cond = ((m5cb.datetime >= s.battery_period_start) & (m5cb.datetime <= s.battery_period_end))
        m5cb.loc[cond, 'keep'] = False
    logger.info('So far, keeping {} rows'.format(len(m5cb[m5cb['keep'] == True])))

    m5cb = m5cb[m5cb.keep == True]
    logger.info("after cleaning: {}".format(len(m5cb)))

    del m5cb['keep']
    m5cb.set_index(['datetime', 'member'], inplace=True)

    logger.info("appending cleaned m5cb to {}".format(clean_store_path))
    with pd.HDFStore(clean_store_path) as store:
        store.append('proximity/member_5_closest_beacons', m5cb)
    del m5cb


def _clean_date_range(start_ts, end_ts, members_metadata):
    """
    Clean a given date range for all relevant dataframes
    """

    ##################################################
    # figure out what to drop and what to keep
    ##################################################
    where = "datetime >= '" + str(start_ts) + "' & datetime < '" + str(end_ts) + "'"

    # Convert text into timestamps with timezone
    period1_start_ts = pd.Timestamp(period1_start, tz=time_zone)
    period2_end_ts = pd.Timestamp(period2_end, tz=time_zone)

    # Start and end dates for participants
    participation_dates = members_metadata[members_metadata['participates'] == 1][['member', 'start_date', 'end_date']]
    participation_dates['start_date_ts'] = pd.to_datetime(participation_dates['start_date']).dt.tz_localize(time_zone)
    participation_dates['end_date_ts'] = pd.to_datetime(participation_dates['end_date']).dt.tz_localize(time_zone)
    del participation_dates['start_date']
    del participation_dates['end_date']

    # Remove times of battery changes - Sundays between 7:30pm and 11:30pm
    # create a list of dates, and choose only sundays
    battery_sundays = pd.date_range(period1_start_ts, period2_end_ts, freq='1D', normalize=True). \
        to_series(keep_tz=True).to_frame(name="su")

    battery_sundays = battery_sundays[battery_sundays.su.dt.dayofweek == 6]
    battery_sundays['battery_period_start'] = battery_sundays.su + pd.Timedelta(hours=19, minutes=30)
    battery_sundays['battery_period_end'] = battery_sundays.su + pd.Timedelta(hours=23, minutes=30)

    ##################################################
    # Clean
    ##################################################
    logger.info('---------------------------------------')
    _clean_m2m(where, participation_dates, battery_sundays)

    logger.info('---------------------------------------')
    _clean_m2b(where, participation_dates, battery_sundays)

    logger.info('---------------------------------------')
    _clean_m5cb(where, participation_dates, battery_sundays)



def clean_up_data():
    # remove dirty data if already there
    try:
        os.remove(clean_store_path)
    except OSError:
        pass

    logger.info("Cleaning up the data")
    members_metadata = pd.read_csv(members_metadata_path)
    members_metadata = members_metadata[members_metadata['member_id'].notnull()]

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
    # Clean data, one day at a time
    ##################################################
    # period 1
    for i in range(0, len(period1_dates) - 1, 1):
        logger.info('---------------------------------------')
        logger.info("Cleaning date: {} - {}".format(period1_dates[i],period1_dates[i+1]))
        _clean_date_range(period1_dates[i], period1_dates[i+1], members_metadata)

    # Period 2
    for i in range(0, len(period2_dates) - 1, 1):
        logger.info('---------------------------------------')
        logger.info("Cleaning date: {} - {}".format(period2_dates[i], period2_dates[i + 1]))
        _clean_date_range(period2_dates[i], period2_dates[i+1], members_metadata)

    logger.info('---------------------------------------')
    logger.info('Completed cleaning data!')
