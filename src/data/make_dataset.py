################################################################################
#                               make_dataset.py
#
# Usage:
#   Run the file with arguments 'download', 'process', or 'clean'
#
#   download:
#     - Downloads badgepi files in 'pi_range' for dates 'dates_to_download'
#         to directory data/raw
#
#   group:
#     - Rearranges the just-downloaded raw data (gzipped) into hourly files in
#         directory data/raw/proximity
#
#   process:
#     - Reads in the (gzipped) downloaded data
#     - Processes these into a bunch of dataframes for analysis, including
#         member to member, member to beacon, 5 closest beacons, etc.
#     - Writes all these dataframes to data/interim/data_dirty.h5 (overwrites)
#
#   clean:
#     - Reads in data from data_dirty.h5
#     - Runs the cleaning code based on members metadata provided, removing
#         non-participants and data points outside project time slice
#     - Writes m2m, m2b, and m5cb to data/interim/data_cleaned.h5 (appends)
#
# Assumed directory structure for /data:
# data
# |-- external
# |-- interim
# |   |-- analysis.h5
# |   |-- data_dirty.h5
# |   |-- analysis.h5
# |   `-- proximity
# |       `-- proximity_grouped.tar.gz
# |-- metadata
# |   |-- beacons.csv  <-- as exported from the server
# |   `-- members.csv  <-- as the google sheet, without title row
# |-- processed
# `-- raw
# |   |-- suvey_stuff.csv
#     `-- hub_data
#         |-- audio_from_server.tar.gz       <-- downloaded audio files
#         `-- proximity_from_server.tar.gz   <-- downloaded proximity files
###############################################################################

from __future__ import absolute_import, division, print_function

import os
import sys
import time
from multiprocessing import Pool, Queue

from config import *

from clean import clean_up_data
from download import download_data
from process import group_by_hour, process_proximity
from analysis import analyze_data

def main():
    start_time = time.time()
    if "download" in sys.argv:
        # download raw data from server
        raw_filenames = download_data(dates_to_download)
        print("\n\nfinished downloading data, ending. please gzip the data, then run group, process, and clean.")
        return

    if "group" in sys.argv:
        # Group available data by day
        group_by_hour()
        print("\n\nfinished grouping data, ending. Next, run process, and clean.")
        return

    if "process" in sys.argv:
        process_proximity()

        print('completed processing!')

    if "clean" in sys.argv:
        # clean up the data
        clean_up_data()

    if "analysis" in sys.argv:
        # create the analysis dataframes
        analyze_data()

    if "help" in sys.argv or len(sys.argv) == 1:
        print("Please use arguments 'download', 'group', 'process', or 'clean'.")
    print("Total runtime: %s seconds" % (time.time() - start_time))

if __name__ == '__main__':
    main()
