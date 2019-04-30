from __future__ import absolute_import, division, print_function
import datetime

import pandas as pd
from config import *
from analysis_comply import *
from analysis_metadata import *
from analysis_connections import *


def analyze_data():
    """
    Runs domain-specific analysis on the data. In our case, we have two time periods, and therefor we'll run
    the analysis on each period (we fill in data gaps, so it doesn't make sense to have a large gap in the middle)
    :return:
    """
    logger.info("Analysing data")

    try:
        os.remove(analysis_store_path)
    except OSError:
        pass

    analysis_comply()
    logger.info("----------------------------------------------------------")
    analysis_metadata()
    logger.info("----------------------------------------------------------")
    analysis_connections()


