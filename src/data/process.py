#from __future__ import absolute_import, division, print_function
import datetime
import json
import time
import glob
import gzip
import os
from multiprocessing import Pool

import pandas as pd
from config import *

import openbadge_analysis as ob
import openbadge_analysis.preprocessing


def group_by_hour():
    """
    Combines the downloaded raw data into hourly files
    """
    # create target dir if doesn't exist
    if not os.path.exists(proximity_data_dir):
        os.makedirs(proximity_data_dir)

    # iterate over files in raw directory
    i = 0
    proximity_filenames_gzipped = glob.glob(raw_data_proximity_filename_pattern)
    count = len(proximity_filenames_gzipped)
    proximity_split_filenames = set()
    for filepath in proximity_filenames_gzipped:
        i += 1
        filename = os.path.basename(filepath)      
        with gzip.open(filepath, 'r') as f:
            # ignore if file was empty (server returned 404)
            if f.readline()[:6] == '<html>':
                logger.info("Ignoring file {}  ({}/{})".format(filename[-35:], i, count))
                continue
        
        # need to open the file again so we don't skip the first line
        with gzip.open(filepath, 'r') as f:
            logger.info("Splitting file {}  ({}/{})".format(filename[-35:], i, count))
            if filename.find('proximity') >= 0 and filename.find('badgepi') >= 0:
                names = _split_raw_data_by_hour(f, proximity_data_dir, 'proximity')
                proximity_split_filenames |= names
    return proximity_split_filenames


def _split_raw_data_by_hour(fileobject, target, kind):
    """Splits the data from a raw data file into a single file for each day.

    Parameters
    ----------
    fileobject : object, supporting tell, readline, seek, and iteration.
        The raw data to be split, for instance, a file object open in read mode.

    target : str
        The directory into which the files will be written.  This directory must
        already exist.

    kind : str
        The kind of data being extracted, either 'audio' or 'proximity'.
    """
    # The hours fileobjects
    # It's a mapping from dates/hours (e.g. '2017-07-29-04') to fileobjects
    hour_files = {}

    # Read each line
    for line in fileobject:
        data = json.loads(line)

        # Keep only relevant data
        if not data['type'] == kind + ' received':
            continue

        # TODO modify to use time_zone variable
        # Extract the day/hour from the timestamp
        hour = datetime.datetime.fromtimestamp(data['data']['timestamp']).strftime("%Y%m%d-%H")+'.gz'

        # If no fileobject exists for that hour, create one
        if hour not in hour_files:
            hour_files[hour] = gzip.open(os.path.join(target, hour), 'a')

        # Write the data to the corresponding day file
        json.dump(data, hour_files[hour])
        hour_files[hour].write('\n')

    # Free the memory
    for f in hour_files.values():
        f.close()
    return {hour for hour in hour_files.keys()}


def process_proximity():
    # remove dirty data if already there
    try:
        os.remove(dirty_store_path)
    except OSError:
        pass

    proximity_filepaths_gzipped = sorted(glob.glob((os.path.join(proximity_data_dir,'*gz'))))

    # process and write files in groups of num_processors for max efficiency
    for i in range(0, len(proximity_filepaths_gzipped), num_processors):
        pool = Pool(num_processors)
        results = pool.map(_process_proximity_file, proximity_filepaths_gzipped[i:i+num_processors])
        pool.close()
        pool.join()
        _write_proximity(results)
        del results


def _process_proximity_file(filepath_zipped):
    '''
    Do all the processing on a single file, filename. Returns a dictionary
      mapping store names to dataframes, ready to be written to storage
      via write_proximity().
    '''
    filename = os.path.basename(filepath_zipped)
    output = {}
    beacons_metadata = pd.read_csv(beacons_metadata_path)
    members_metadata = pd.read_csv(members_metadata_path)
    logger.info("-------------------------------------------")
    logger.info("Processing proximity file '{}'".format(filename))

    logger.info("ID-to-member mapping")
    with gzip.open(filepath_zipped, 'r') as f:
        idmap = ob.preprocessing.id_to_member_mapping(members_metadata)
        logger.info("idmap. Counter: {}".format(len(idmap)))
        print(idmap.head())

    logger.info("Voltages")
    with gzip.open(filepath_zipped, 'r') as f:
        voltages = ob.preprocessing.voltages(f, time_bins_size, tz=time_zone)
        output['other/voltages'] = voltages
        del voltages

    logger.info("Member-to-badge proximity")
    with gzip.open(filepath_zipped, 'r') as f:
        m2badge = ob.preprocessing.member_to_badge_proximity(f, time_bins_size, tz=time_zone)

        # Remove RSSI values that are invalid
        logger.info("Member-to-badge proximity - cleaning RSSIs. Count before: {}".format(len(m2badge)))
        m2badge = m2badge[m2badge['rssi'] < -10]
        logger.info("Member-to-badge proximity - cleaning RSSIs. Count after: {}".format(len(m2badge)))
        output['proximity/member_to_badge'] = m2badge

    if len(m2badge) == 0:
        logger.info("Empty dataset. Skipping the rest")
        return output

    # Calculate other dataframes (note which version of m2badge i'm using)
    logger.info("Member-to-member proximity")
    m2m = ob.preprocessing.member_to_member_proximity(m2badge, idmap)
    logger.info("Member-to-member proximity. Count: {}".format(len(m2m)))
    output['proximity/member_to_member'] = m2m
    del m2m

    logger.info("Member-to-beacon proximity")
    m2b_raw = ob.preprocessing.member_to_beacon_proximity(m2badge, beacons_metadata.set_index('beacon_id')['beacon'])
    logger.info("Member-to-beacon proximity. Count: {}".format(len(m2b_raw)))
    output['proximity/member_to_beacon_raw'] = m2b_raw
    del m2badge

    if len(m2b_raw) == 0:
        logger.info("Empty dataset. Skipping the rest")
        return output

    # Smoothing RSSIs and filling gaps
    m2b_smooth = ob.preprocessing.member_to_beacon_proximity_smooth(
        m2b_raw, window_size=rssi_smooth_window_size, min_samples=rssi_smooth_min_samples)
    logger.info("Member-to-beacon proximity - Smooth. Count after: {}".format(len(m2b_smooth)))

    m2b = ob.preprocessing.member_to_beacon_proximity_fill_gaps(
        m2b_smooth, time_bins_size=time_bins_size, max_gap_size=time_bins_max_gap_size)
    logger.info("Member-to-beacon proximity - fill gaps. Count after: {}".format(len(m2b)))
    output['proximity/member_to_beacon'] = m2b

    if len(m2b) == 0:
        logger.info("Empty dataset. Skipping the rest")
        return output

    logger.info("Member 5 closest beacons")
    if len(m2b) > 0:
        m5cb = m2b.reset_index().groupby(['datetime', 'member'])['rssi', 'beacon'] \
            .apply(lambda x: x.nlargest(5, columns=['rssi']) \
                    .reset_index(drop=True)[['beacon', 'rssi']]) \
            .unstack()[['beacon', 'rssi']]

        m5cb.columns = [col[0] + "_" + str(col[1]) for col in m5cb.columns.values]

        rssi_nan_value = -1.0
        values = {'rssi_0': rssi_nan_value, 'rssi_1': rssi_nan_value,
                  'rssi_2': rssi_nan_value, 'rssi_3': rssi_nan_value,
                  'rssi_4': rssi_nan_value}
        m5cb.fillna(value=values, inplace=True)

        # add missing columns if needed (so append o HDFStore doesn't fail)
        for i in range(0,5,1):
            rssi_field_name = 'rssi_'+str(i)
            beacon_field_name = 'beacon_'+str(i)
            
            if rssi_field_name not in m5cb.columns.values:
                logger.debug("Adding missing field{}".format(rssi_field_name))
                m5cb[rssi_field_name] = rssi_nan_value

            if beacon_field_name not in m5cb.columns.values:
                logger.debug("Adding missing field {}".format(beacon_field_name))
                m5cb[beacon_field_name] = None    

        logger.info("Member 5 closest beacons. Count: {}".format(len(m5cb)))
        output['proximity/member_5_closest_beacons'] = m5cb
        del m5cb
    del m2b

    logger.info("Finished processing file {}".format(filename))
    return output

def _write_proximity(outputs):
    """
    Helper function; just writes "outputs" to an h5 file at dirty_store_path

        outputs - dict, maps store names to pandas DataFrames.

    Returns nothing
    """
    for i in range(len(outputs)):
        logger.info("writing {}/{}".format(i+1, len(outputs)))
        for name, table in outputs[i].iteritems():
            with pd.HDFStore(dirty_store_path) as store:
                store.append(name, table)
