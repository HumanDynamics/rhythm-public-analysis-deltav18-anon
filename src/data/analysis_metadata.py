from __future__ import absolute_import, division, print_function
import datetime

import pandas as pd
from config import *


def _analysis_create_members():
    """
    Creates a table with members data
    :return:
    """
    logger.info("Creating members table")
    members_metadata = pd.read_csv(members_metadata_path)
    members_metadata = members_metadata.query('participates == 1').copy()

    members_metadata['start_date_ts'] = pd.to_datetime(members_metadata['start_date']).dt.tz_localize(time_zone)
    members_metadata['end_date_ts'] = pd.to_datetime(members_metadata['end_date']).dt.tz_localize(time_zone)
    del members_metadata['start_date']
    del members_metadata['end_date']
    members_metadata.set_index('member', inplace=True)

    out_store_key = "metadata/members"
    members_metadata.to_hdf(analysis_store_path, out_store_key, mode="a", format="table", append=False)


def analysis_metadata():
    """
    Creates connection tables in multiple levels
    :return:
    """
    logger.info("Analysis - metadata")

    _analysis_create_members()

    logger.info('---------------------------------------')
    logger.info('Completed analysis metadata!')



