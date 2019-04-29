from __future__ import absolute_import, division, print_function
import logging
import os

# General project settings

# dict mapping months to date ranges in that month to download
dates = {6: (12, 29), 7: (30, 31), 8: (1, 15)}

dates_to_download = ["{:02d}-{:02d}".format(m, d) for m in sorted(dates.keys())
                     for d in range(dates[m][0], dates[m][1]+1)]
# print(dates_to_download)

time_zone = 'US/Eastern'
log_version = '2.0'
time_bins_size = '15S'
period1_start = '2018-06-12 12:00:00'
period1_end = '2018-06-29 23:59:45'
period2_start = '2018-07-30 00:00:00'
period2_end = '2018-08-15 22:59:45'
project_time_slices = [slice(period1_start, period1_end),
                       slice(period2_start, period2_end)]

# rssis to use in the analysis
rssi_cutoffs = [-51,-57,-60,-62,-65]

# range of raspberry pi numbers included in experiment. It's fine to include
#    sometimes-inactive pis, these files will be detected and ignored.
pi_range = range(12, 27)

# Number of processors to use in parallelized steps (download and process)
# Leave at least 1 processor on the machine available, so it stays responsive
num_processors = 8

# Data cleaning settings
rssi_smooth_window_size = '1min' # Window size for smoothing proximity_data_dir
rssi_smooth_min_samples = 1      # Only calculate if window has at least this number of samples
time_bins_max_gap_size = 2       # this is the maximum number of consecutive NaN values to fill

### Various directories ###
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
# project_dir = os.path.join('/home', 'kyeb', 'badges', 'test_data')
data_dir = os.path.join(project_dir, 'data')
raw_data_dir = os.path.join(data_dir, 'raw', 'hub_data')
interim_data_dir = os.path.join(data_dir, 'interim')
metadata_dir = os.path.join(data_dir, 'metadata')

# Currently configured to run with the spreadsheet called 'team members' as
# members.csv Might need some adaptations if you want to run using server's
# member metadata list (different column names)
members_metadata_filename = 'members.csv'
beacons_metadata_filename = 'beacons.csv'

beacons_metadata_path = os.path.join(metadata_dir, beacons_metadata_filename)
members_metadata_path = os.path.join(metadata_dir, members_metadata_filename)

raw_data_proximity_filename_pattern = os.path.join(raw_data_dir, '*proximity*.txt.gz')
proximity_data_dir = os.path.join(interim_data_dir, 'proximity')

dirty_store_path = os.path.join(interim_data_dir, 'data_dirty.h5')
clean_store_path = os.path.join(interim_data_dir, 'data_cleaned.h5')
analysis_store_path = os.path.join(interim_data_dir, 'analysis.h5')
analysis_notebooks_store_path = os.path.join(interim_data_dir, 'analysis_notebooks.h5')

surveys_anon_store_path = os.path.join(data_dir,'raw','surveys', 'surveys_anon.h5')
surveys_clean_store_path = os.path.join(interim_data_dir, 'surveys_clean.h5')

performance_anon_store_path = os.path.join(data_dir,'raw','performance', 'performance_anon.h5')
performance_clean_store_path = os.path.join(interim_data_dir, 'performance_clean.h5')

logger = logging.getLogger(__name__)
log_fmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_fmt)

fileHandler = logging.FileHandler("make_dataset.log")
fileFormatter = logging.Formatter(log_fmt)
fileHandler.setFormatter(fileFormatter)
logger.addHandler(fileHandler)
