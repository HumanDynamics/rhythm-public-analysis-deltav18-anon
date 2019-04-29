"""
Notes:
    It's tricky to do this without loading the entire dataframe into memory at
once.
    If you want to process by number of rows (say, 100,000 rows at a time),
then you can just use the "start=" and "stop=" args to pd.read_hdf(), but you
first need a sense of the number of rows in the dataframe.
    If you want to process by day, you have to load at least the index into
memory to see what days exist for loading, unless you want to just try every day
in a range (which wouldn't generalize very well).

So, for now, this must load the entire dataframe into memory, then can process
by day in parallel.
"""
from __future__ import absolute_import, division, print_function

import pandas as pd
import numpy as np

from multiprocessing import Pool

class Parallelize():
    """
    Usage:
    - Create a new Apply object by initializing with a function and a dataframe
    - Call Apply.compute() to run the function on each partition of the
        dataframe, by day
    - Returns a list of the results. If the results are dataframes, it should
        be trivial to pd.concat() them together.

    You can access the results of a computation later with the Parallel.result
       attribute. This also means results will be stored in memory until Python
       garbage collects both the returned results and the Parallel object.
    """

    def __init__(self, f, df, num_processes=6):
        """
        Initialize and instance of a Parallel object for computing.
        Args:
        - f: function to run
        - df: pd.DataFrame to run f on
        - num_processes: max number of processors to use in parallel. Leave at
            least 1 or 2 available so that your machine stays responsive.
        """
        self.f = f
        self.df = df
        self.num_processes = num_processes

    def compute(self, period='D'):
        """"
        Run the function self.f on the dataframe self.df. Return the results,
          and save them to self.results.

        Args:
         - period: 'D' to process by day, 'H' to process by hour. If I had more
             time, I would implement more options.
        """
        df = self.df
        if period == 'D':
            groups = [group[1] for group in df.groupby(pd.Grouper(level='datetime', freq='D'))]
        elif period == 'H':
            groups = [group[1] for group in df.groupby(pd.Grouper(level='datetime', freq='H'))]
        else:
            raise NotImplementedError
        pool = Pool(processes=self.num_processes)
        res = pool.map(self.f, groups)
        self.res, self.result, self.results = res, res, res
        return res



if __name__ == "__main__":
    # example usage, determines compliance by max_rssi for each day
    df = pd.read_hdf('../../data/interim/data_dirty.h5', 'proximity/member_to_badge')


    def max_rssi_method(m2badge, board_threshold=-48):
        """
        Function from analysis.py, computes a series with boolean for compliance at
        each time/member bin using average RSSI
        """
        m2badge = m2badge.reset_index().query("observed_id < 17009")
        m2badge = m2badge.set_index(['datetime', 'member', 'observed_id']).rssi
        gp = m2badge.groupby(['datetime', 'member'])
        df = gp.agg('max').rename("max_rssi")
        s = pd.Series(np.where(df > board_threshold, False, True), index=df.index)
        return s

    p = Parallelize(max_rssi_method, df)
    p.compute()
    print(p.res)
    print(pd.concat(p.res))
