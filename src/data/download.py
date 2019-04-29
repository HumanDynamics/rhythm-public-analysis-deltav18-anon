from __future__ import absolute_import, division, print_function
import time
import urllib
from multiprocessing import Pool

from config import *


def download_data(dates):
    '''
    Downloads the data for 'dates' in 'pi_range' to 'directory', in parallel
    '''
    url = "http://openbadgeprod.media.mit.edu/media/data/SQKYZR2SXK/badgepi-{}_proximity_2018-{}.txt"
    filenames = []
    pool = Pool(num_processors)
    for date in dates:
        # remove hourly files if they exist for the day
        try:
            for hour in range(24):
                name = "2018"+date[:2]+date[-2:]+"-"+str(hour).zfill(2)
                os.remove(os.path.join(proximity_data_dir, name))
            print('Removed old hourly files')
        except OSError:
            pass
        for pi in pi_range:
            _url = url.format(str(pi), date)
            filename = os.path.abspath(os.path.join(raw_data_dir, _url[-35:]))
            pool.apply_async(_download_file, (_url, filename,))
            filenames += [filename]
    pool.close()
    pool.join()
    t1 = time.time()
    return filenames


def _download_file(url, filename):
    '''
    Helper function, allows parallelization of download_data()
    '''
    print("Downloading {}".format(url))
    urllib.urlretrieve(url, filename)


if __name__ == "__main__":
    download_data(dates_to_download)
