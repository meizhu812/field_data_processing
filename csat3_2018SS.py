# coding=utf-8
"""
aaa
"""
from common_methods import data_read, list_data_files, data_merge, data_split
import pandas as pd
# Parameters ###########################################################################################################
DATA_PATH = r'D:\Truman\Desktop\present_work\01_ammonia\01_data\03_2018summer\02South\01Meteorology'
TEMP_DIR = r'd:\Temp\CSAT_2018SS'
FREQ = '15T'
CSAT3 = dict(header=0,
             skiprows=[0, 2, 3],
             usecols=[0, 2, 3, 4, 5, 6],
             na_values="NAN",
             parse_dates=[0])
INIT = 'TOA5_10525.ts'
EXT = '.dat'
DATA_PERIOD = pd.date_range('2018-05-31 14:15:00', '2018-07-02 09:00:00', freq=FREQ)
########################################################################################################################
data = pd.DataFrame()
# switch = input('Merge data first? Y/N ')
try:
    data = data_read(temp_dir=TEMP_DIR)
except FileNotFoundError:
    print("Merged data does not exist!")
    datafiles = list_data_files(data_dir=DATA_PATH, file_ext=EXT, file_init=INIT)
    data = data_merge(data_format=CSAT3, rawfiles=datafiles, temp_dir=TEMP_DIR)
data_split(data, DATA_PERIOD, DATA_PATH + r'\02_eddy\data')
