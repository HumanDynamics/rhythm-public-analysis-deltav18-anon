# -*- coding: utf-8 -*-
import os, sys
import logging
import json
import pandas as pd
import numpy as np
import tarfile

# Add the open-badges package dir to the PATH
src_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir, "openbadge-analysis")
sys.path.append(src_dir)

# Import the data analysis tools
import openbadge_analysis as ob
import openbadge_analysis.preprocessing
import openbadge_analysis.core

logger = logging.getLogger(__name__)

### Various directories ###
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

# with open(os.path.join(project_dir, 'config.yaml')) as f:
#    config = yaml.load(f)

data_dir = os.path.join(project_dir, 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
interim_data_dir = os.path.join(data_dir, 'interim')
external_data_dir = os.path.join(data_dir, 'external')
temp_data_dir = os.path.expanduser('~/temp/deltav18_networking')

# General project settings
time_zone = 'US/Eastern'
log_version = '2.0'
time_bins_size = '15S'
project_start = '2018-05-10 17:00:00'
project_end = '2018-05-10 22:30:00'
project_time_slice = slice(project_start, project_end)

# Data cleaning settings
rssi_smooth_window_size = '1min' # Window size for smoothing
rssi_smooth_min_samples = 1      # Only calculate if window has at least this number of samples
time_bins_max_gap_size = 2 #  this is the maximum number of consecutive NaN values to forward/backward fill
shortest_off_board_bins_count = 5   #Shortest period for which the badge may be off board. Used for removing
                                    #"noisy" data, where the badge jumps off board and returns

members_metadata_filename = 'members_metadata_networking.csv'
beacons_metadata_filename = 'beacons_metadata_networking.csv'

beacons_metadata_path = os.path.join(external_data_dir, beacons_metadata_filename)
members_metadata_path = os.path.join(external_data_dir, members_metadata_filename)

raw_data_filename = 'deltav2018_networking.tar.gz'
raw_data_path = os.path.join(raw_data_dir, raw_data_filename)

# Ignored raw data files
ignored_files = []

byday_data_dir = os.path.join(temp_data_dir, 'byday')
proximity_byday_data_dir = os.path.join(byday_data_dir, 'proximity')
audio_byday_data_dir = os.path.join(byday_data_dir, 'audio')

dirty_store_path = os.path.join(interim_data_dir, 'data_dirty_networking.h5')
clean_store_path = os.path.join(interim_data_dir, 'data_networking.h5')
analysis_store_path = os.path.join(interim_data_dir, 'analysis_networking.h5')


def group_by_day():
    tar = tarfile.open(raw_data_path, 'r:gz')
    logger.info("Splitting data to separate days")
    for fileinfo in tar:
        if fileinfo.name in ignored_files:
            continue
        if fileinfo.name.find('proximity') >= 0:
            logger.debug("Reading from '{}'".format(fileinfo.name))
            fileobject = tar.extractfile(fileinfo)
            ob.preprocessing.split_raw_data_by_day(fileobject, proximity_byday_data_dir, 'proximity',
                                                   log_version=log_version)
    # elif fileinfo.name.find('audio') >= 0:
    #         print("Splitting '{}'".format(fileinfo.name))
    #         fileobject = tar.extractfile(fileinfo)
    #         ob.preprocessing.split_raw_data_by_day(fileobject, audio_byday_data_dir, audio', log_version=log_version)

    tar.close()

    _, _, proximity_byday_filenames = os.walk(proximity_byday_data_dir).next()
    # _, _, audio_byday_filenames = os.walk(audio_byday_data_dir).next()

    # TODO audio

    return proximity_byday_filenames, None


def process_proximity_by_day(proximity_byday_filenames):
    #members_metadata = pd.read_csv(members_metadata_path)

    beacons_metadata = pd.read_csv(beacons_metadata_path)
    """
    No need to calculate ids based on mac_address. IDs were set by the server
    """
    #beacons_metadata['id'] = beacons_metadata.apply(
    #    lambda row: ob.core.mac_address_to_id(row['badge_address']),
    #    axis=1
    #)

    try:
        os.remove(dirty_store_path)
    except OSError:
        pass
    store = pd.HDFStore(dirty_store_path)

    for filename in sorted(proximity_byday_filenames):
        logger.info("-------------------------------------------")
        logger.info("Processing proximity file '{}'".format(filename))

        logger.info("ID-to-member mapping")
        with open(os.path.join(proximity_byday_data_dir, filename), 'r') as f:
            idmap = ob.preprocessing.id_to_member_mapping(f, time_bins_size, tz=time_zone, fill_gaps=True)
            logger.info("idmap. Counter: {}".format(len(idmap)))
            store.append('metadata/id_to_member_mapping', idmap)            

        logger.info("Voltages")
        with open(os.path.join(proximity_byday_data_dir, filename), 'r') as f:
            voltages = ob.preprocessing.voltages(f, time_bins_size, tz=time_zone)
            store.append('other/voltages', voltages)
            del voltages

        logger.info("Member-to-badge proximity")
        with open(os.path.join(proximity_byday_data_dir, filename), 'r') as f:
            m2badge = ob.preprocessing.member_to_badge_proximity(f, time_bins_size, tz=time_zone)

            # Remove RSSI values that are invalid
            logger.info("Member-to-badge proximity - cleaning RSSIs. Count before: {}".format(len(m2badge)))
            m2badge = m2badge[m2badge['rssi'] < -10]
            logger.info("Member-to-badge proximity - cleaning RSSIs. Count after: {}".format(len(m2badge)))
            store.append('proximity/member_to_badge', m2badge)

        if len(m2badge) == 0:
            logger.info("Empty dataset. Skipping the rest")
            continue

        # Calculate other dataframes (note which version of m2badge i'm using)
        logger.info("Member-to-member proximity")
        m2m = ob.preprocessing.member_to_member_proximity(m2badge, idmap)
        logger.info("Member-to-member proximity. Count: {}".format(len(m2m)))
        store.append('proximity/member_to_member', m2m)

        del m2m

        logger.info("Member-to-beacon proximity")
        m2b_raw = ob.preprocessing.member_to_beacon_proximity(m2badge, beacons_metadata.set_index('id')['beacon'])
        logger.info("Member-to-beacon proximity. Count: {}".format(len(m2b_raw)))
        store.append('proximity/member_to_beacon_raw', m2b_raw)
        del m2badge

        if len(m2b_raw) == 0:
            logger.info("Empty dataset. Skipping the rest")
            continue

        # Smoothing RSSIs and filling gaps
        m2b_smooth = ob.preprocessing.member_to_beacon_proximity_smooth(
            m2b_raw, window_size=rssi_smooth_window_size, min_samples=rssi_smooth_min_samples)
        logger.info("Member-to-beacon proximity - Smooth. Count after: {}".format(len(m2b_smooth)))

        m2b = ob.preprocessing.member_to_beacon_proximity_fill_gaps(
            m2b_smooth, time_bins_size=time_bins_size, max_gap_size=time_bins_max_gap_size)
        logger.info("Member-to-beacon proximity - fill gaps. Count after: {}".format(len(m2b)))
        store.append('proximity/member_to_beacon', m2b)

        if len(m2b) == 0:
            logger.info("Empty dataset. Skipping the rest")
            continue

        logger.info("Member 5 closest beacons")
        if len(m2b) > 0:
            m5cb = m2b.reset_index().groupby(['datetime', 'member'])['rssi', 'beacon'] \
                .apply(lambda x: x.nlargest(5, columns=['rssi']) \
                       .reset_index(drop=True)[['beacon', 'rssi']]) \
                .unstack()[['beacon', 'rssi']]

            m5cb.columns = [col[0] + "_" + str(col[1]) for col in m5cb.columns.values]
            
            rssi_nan_value = -1
            values = {'rssi_0': rssi_nan_value, 'rssi_1': rssi_nan_value, 'rssi_2': rssi_nan_value, 'rssi_3': rssi_nan_value, 'rssi_4': rssi_nan_value}
            m5cb.fillna(value=values, inplace=True)

            logger.info("Member 5 closest beacons. Count: {}".format(len(m5cb)))
            store.append('proximity/member_5_closest_beacons', m5cb)
            del m5cb

        del m2b
    store.close()


def drop_in_time_slice(m2m, m2b, m5cb, time_slice, to_drop):
    """Drops certain members from data structures, only in a given time slice.
    This can be useful for removing people who weren't there on a specific day.
    """
    logger.debug("Removing data: {} {}".format(time_slice, to_drop))
    m2m.drop(m2m.loc[(time_slice, slice(None), to_drop), :].index, inplace=True)
    m2m.drop(m2m.loc[(time_slice, to_drop, slice(None)), :].index, inplace=True)
    m2b.drop(m2b.loc[(time_slice, to_drop, slice(None)), :].index, inplace=True)
    m5cb.drop(m5cb.loc[(time_slice, to_drop), :].index, inplace=True)


def clean_up_data():
    logger.info("Cleaning up the data - loading dirty data")
    members_metadata = pd.read_csv(members_metadata_path)
    members_metadata = members_metadata[members_metadata.member.notnull()]

    beacons_metadata = pd.read_csv(beacons_metadata_path)

    """
    No need to calculate ids based on mac_address. IDs were set by the server
    """
    #beacons_metadata['id'] = beacons_metadata.apply(
    #    lambda row: ob.core.mac_address_to_id(row['badge_address']),
    #    axis=1
    #)

    store = pd.HDFStore(dirty_store_path, 'r')
    m2m = store.get('proximity/member_to_member')
    m2b = store.get('proximity/member_to_beacon')
    m5cb = store.get('proximity/member_5_closest_beacons')
    store.close()

    # Remove data outside project time slice
    logger.debug("Cleaning up the data - removing data outside project time slice:\
     {}".format(project_time_slice))

    m2m = m2m.reset_index().set_index("datetime").loc[project_time_slice]
    m2m = m2m.reset_index().set_index(['datetime', 'member1', 'member2'])

    m2b = m2b.reset_index().set_index("datetime").loc[project_time_slice]
    m2b = m2b.reset_index().set_index(['datetime', 'member', 'beacon'])

    m5cb = m5cb.reset_index().set_index("datetime").loc[project_time_slice]
    m5cb = m5cb.reset_index().set_index(['datetime', 'member'])


    inactive_beacons = []
    removed_members = [
        (slice(None), # remove non-participants, EIRs and management
         members_metadata.query('participates == 0 or company == "EIR" or company == "Staff"')['member']),
    ]

    # For each participator, remove data from before that user joined
    starting_times = members_metadata[members_metadata['participates'] == 1][['member', 'fixed_time_badge_given']]
    for index, row in starting_times.iterrows():
        start_time = row['fixed_time_badge_given']
        member = row['member']
        removed_members.append((slice(project_start, start_time), [member]))

    # For each participator, remove data from after that user left
    endinging_times = members_metadata[members_metadata['participates'] == 1][['member', 'fixed_time_badge_returned']]
    for index, row in endinging_times.iterrows():
        end_time = row['fixed_time_badge_returned']
        member = row['member']
        removed_members.append((slice(end_time, project_end), [member]))

    logger.info("Cleaning up the data - starting to remove participants data")
    for (ts, members) in removed_members:
        drop_in_time_slice(m2m, m2b, m5cb, ts, members)

    # TODO inactive beacons

    try:
        os.remove(clean_store_path)
    except OSError:
        pass

    store = pd.HDFStore(clean_store_path)
    store.append('proximity/member_to_member', m2m)
    store.append('proximity/member_to_beacon', m2b)
    store.append('proximity/member_5_closest_beacons', m5cb)
    store.close()



def _analysis_m1cb(m5cb, members_metadata, beacons_metadata):
    m1cb = m5cb[['beacon_0', 'rssi_0']].rename(columns={'beacon_0': 'beacon', 'rssi_0': 'rssi'}).reset_index()

    # Add beacon metadata
    m1cb = m1cb \
        .join(beacons_metadata[['company', 'type', 'location']], on='beacon').rename(
        columns={'company': 'beacon_company', 'type': 'beacon_type', 'location': 'beacon_location'})

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
            if row['beacon_company'] in nearby_companies_dict[row['member_company']]:
                return 'at nearby company'
            else:
                return 'at far company'
        else:
            return row['location_type']
    m1cb['location_type_nearby'] = m1cb.apply(set_nearby_company ,axis=1)

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


def _analysis_m2m_cb(m2m, m1cb):
    m1cb_short = m1cb[['beacon', 'rssi', 'member_company', 'location_type_merged']]
    m2m_temp = m2m[['rssi']].reset_index().rename(columns={'rssi': 'm2m_rssi'})

    # adding side 1
    m2m_temp = m2m_temp.join(m1cb_short, on=['datetime', 'member1'])
    m2m_temp = m2m_temp.rename(
        columns={'beacon': 'beacon1', 'rssi': 'beacon_rssi1', 'member_company': 'member_company1',
                 'location_type_merged': 'location_type_merged1'})

    # adding side 2
    # I don't know why, but if you don't this this it acts funny
    m2m_temp2 = m2m_temp.set_index("datetime").reset_index()
    m2m_temp2 = m2m_temp2.join(m1cb_short, on=['datetime', 'member2']).rename(
        columns={'beacon': 'beacon2', 'rssi': 'beacon_rssi2', 'member_company': 'member_company2',
                 'location_type_merged': 'location_type_merged2'})

    # I don't know why, but if you don't this this it acts funny
    m2m_temp3 = m2m_temp2.set_index("datetime").reset_index()
    m2m_temp3.dropna(inplace=True)
    m2m_temp3['beacon1'] = m2m_temp3['beacon1'].astype(np.int64)
    m2m_temp3['beacon2'] = m2m_temp3['beacon2'].astype(np.int64)
    m2m_cb = m2m_temp3
    m2m_cb.set_index(['datetime', 'member1', 'member2'], inplace=True)
    return m2m_cb


def _analysis_member_on_board(m1cb, time_bins_size='1min', shortest_off_board_bins_count=5):
    """ Create a series that for each members tells when the badge was on the board
    Parameters
    ----------
    m1cb : Member to closest beacon

    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.

    shortest_off_board_bins_count : int
        Shortest period for which the badge may be off board. Used for removing
        "noisy" data, where the badge jumps off board and returns

    Returns
    -------
    pd.Series :
        Member on/off board indicator.
    """

    # Step 1 - was the closest beacon a board?
    df = m1cb.copy()
    df['on_board'] = np.where(df['location_type_merged'] == 'board', True, False)
    df = df[['on_board']]

    # Step 2 - resample, fill in gaps in data
    df = df.reset_index(level='member')
    df = df.groupby(['member']) \
        [['on_board']] \
        .resample(time_bins_size) \
        .asfreq()

    df = df.fillna(value=True)  # If NaN, it's on board

    # Step 3 - remove short intervals of off board
    def interval_size(x):  # add size for each interval
        return len(x)

    def remove_small_not_board_interval(x):
        x = x.copy()
        # show the previous value next to current one
        x['on_board_prev'] = x.on_board.shift(1)
        # Mark each new interval as 1
        x['new_interval'] = np.where(x.on_board == x.on_board_prev, 0, 1)
        # Count 1's. This will associate each row with it's interval number
        x['interval_num'] = x.new_interval.cumsum()

        x['interval_size'] = x.groupby(['interval_num'])['new_interval'].transform(interval_size)
        # if size is less than 5, just mark it as on-board
        x['on_board_final'] = np.where(x.interval_size < shortest_off_board_bins_count, True, x.on_board)
        return x[['on_board_final']]

    df = df.groupby(['member']).apply(remove_small_not_board_interval)
    df = df.reset_index().set_index(['datetime', 'member'])
    df.sort_index(inplace=True)
    return df.on_board_final.rename('on_board')


def _analysis_m2m_noboard(m2m, m_onboard):
    """ Removes m2m records in which one of the sides is on the board
    Parameters
    ----------
    m2m : Member to member
    m_onboard : Member on board series (for each timeslice, whether the badge was on the board)

    Returns
    -------
    pd.DataFrame :
        m2m when no side is on the board.
    """
    # add on_board indicator
    df = m2m.copy().reset_index()
    df = df.join(m_onboard, on=['datetime', 'member1']).rename(columns={'on_board': 'on_board1'})
    df = df.set_index("datetime").reset_index()  # I don't know why, but if you don't this this it acts funny
    df = df.join(m_onboard, on=['datetime', 'member2']).rename(columns={'on_board': 'on_board2'})

    # filter
    df = df[(df.on_board1 == False) & (df.on_board2 == False)]
    df.dropna(inplace=True)

    # cleanup
    #df.drop(columns=['on_board1', 'on_board2'], inplace=True)
    df.drop('on_board1', axis=1, inplace=True)
    df.drop('on_board2', axis=1, inplace=True)
    df = df.reset_index().set_index(['datetime', 'member1', 'member2'])
    df.sort_index(inplace=True)

    return df


def analysis():
    logger.info("Preparing analysis data")
    members_metadata = pd.read_csv(members_metadata_path).set_index('member')
    beacons_metadata = pd.read_csv(beacons_metadata_path).set_index('beacon')

    logger.info("Loading clean data")
    store = pd.HDFStore(clean_store_path, 'r')
    m5cb = store.get('proximity/member_5_closest_beacons')
    m2m = store.get('proximity/member_to_member')
    store.close()


    # --- m1cb
    #logger.info("Preparing m1cb")
    m1cb = _analysis_m1cb(m5cb, members_metadata, beacons_metadata)

    # --- m_onboard
    #logger.info("Preparing m_onboard")
    m_onboard = _analysis_member_on_board(m1cb, time_bins_size, shortest_off_board_bins_count)

    # --- m2m_noboard
    logger.info("Preparing m2m_noboard")
    m2m_noboard = _analysis_m2m_noboard(m2m, m_onboard)

    # --- m2m_cb
    #logger.info("Preparing m2m_cb")
    #logger.info("Preparing m2m_cb - Making data")
    #m2m_cb = _analysis_m2m_cb(m2m, m1cb)
    #m2m_cb_noboard = m2m_cb[(m2m_cb['location_type_merged1'] != 'board') & (m2m_cb['location_type_merged2'] != 'board')]

    #logger.info("Preparing m2m_cb. m2m Count: {}".format(len(m2m)))
    #logger.info("Preparing m2m_cb. m2m_cb Count: {}".format(len(m2m_cb)))
    #logger.info("Preparing m2m_cb. m2m_cb_noboard Count: {}".format(len(m2m_cb_noboard)))

    try:
        os.remove(analysis_store_path)
    except OSError:
        pass

    logger.info("Saving data")
    store = pd.HDFStore(analysis_store_path)
    store.append('proximity/member_1_closest_beacon', m1cb)
    #store.append('proximity/member_to_member_with_closest_beacon', m2m_cb)
    #store.append('proximity/member_to_member_with_closest_beacon_noboard', m2m_cb_noboard)
    store.append('proximity/m_onboard', m_onboard)
    store.append('proximity/member_to_member_noboard', m2m_noboard)
    store.close()


def analysis_test():
    logger.info("Preparing analysis data")
    members_metadata = pd.read_csv(members_metadata_path).set_index('member')
    beacons_metadata = pd.read_csv(beacons_metadata_path).set_index('beacon')

    logger.info("Loading clean data")
    store = pd.HDFStore(clean_store_path, 'r')
    m2m = store.get('proximity/member_to_member')
    store.close()

    logger.info("Loading analysis data")
    store = pd.HDFStore(analysis_store_path, 'r')
    m_onboard = store.get('proximity/m_onboard')
    store.close()

    # --- m2m_noboard
    logger.info("Preparing m2m_noboard")
    m2m_noboard = _analysis_m2m_noboard(m2m, m_onboard)

    logger.info("Saving data")
    store = pd.HDFStore(analysis_store_path)
    store.append('proximity/member_to_member_noboard', m2m_noboard)
    store.close()


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_fmt)

    fileHandler = logging.FileHandler("make_dataset.log")
    fileFormatter = logging.Formatter(log_fmt)
    fileHandler.setFormatter(fileFormatter )
    logger.addHandler(fileHandler)

    # Create by-day directories
    #os.makedirs(proximity_byday_data_dir)
    #os.makedirs(audio_byday_data_dir)

    # Group data by day
    #proximity_byday_filenames, audio_byday_filenames = group_by_day()
    _, _, proximity_byday_filenames = os.walk(proximity_byday_data_dir).next()
    #print(proximity_byday_filenames )

    # Process the proximity data
    #process_proximity_by_day(proximity_byday_filenames)

    # Clean up, removing members that weren't there and other things
    #clean_up_data()

    # Some analysis
    analysis()
    #analysis_test()

    # Remove by-day files
    #for filename in sorted(proximity_byday_filenames):
    #    os.remove(os.path.join(proximity_byday_data_dir, filename))
    #for filename in sorted(audio_byday_filenames):
    #    os.remove(os.path.join(audio_byday_data_dir, filename))

    # Remove by-day directories
    #os.removedirs(proximity_byday_data_dir)
    #os.removedirs(audio_byday_data_dir)
