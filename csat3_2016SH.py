# coding=utf-8
"""
aaa
"""
from common_methods import data_read, list_data_files, data_merge, data_split, data_split_multi
import pandas as pd
# Parameters ###########################################################################################################
DATA_PATH = r'd:\Desktop\present_work\01_ammonia\01_data\01_2016summer\01_meteorology'
TEMP_DIR = r'd:\Temp\CSAT_2016SH'
FREQ = '15T'
CSAT3 = dict(header=0,
             skiprows=[0, 2, 3],
             usecols=[0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
             na_values="NAN",
             parse_dates=[0])
INIT = 'TOA5_8329.ts'
EXT = '.dat'
DATA_PERIOD = pd.date_range('2016-06-27 12:30:00', '2016-07-18 09:00:00', freq=FREQ)
########################################################################################################################
if __name__ == '__main__':
    data = pd.DataFrame()
    # switch = input('Merge data first? Y/N ')
    try:
        data = data_read(temp_dir=TEMP_DIR)
    except FileNotFoundError:
        print("Merged data does not exist!")
        datafiles = list_data_files(data_dir=DATA_PATH, file_ext=EXT, file_init=INIT)
        data = data_merge(data_format=CSAT3, rawfiles=datafiles, temp_dir=TEMP_DIR)
    data_split(data, DATA_PERIOD, DATA_PATH + r'\02_eddy\data')
    data_split_multi(data, DATA_PERIOD, DATA_PATH + r'\02_eddy\data2')
