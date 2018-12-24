# coding=utf-8
"""
aaa
"""
from common_methods import data_read, list_data_files, data_merge, data_fill, data_resample
import pandas as pd
# Parameters ###########################################################################################################
DATA_PATH = r'd:\Truman\Desktop\present_work\01_ammonia\01_data\01_2016summer\02_concentration\01_low2m\00_raw'
TEMP_DIR = r'd:\Temp\PICARRO_2016SL'
FREQ = '15T'
PICARRO = dict(sep='\s+',
               usecols=[0, 1, 17, 21],
               parse_dates=[[0, 1]])
INIT = 'AEDS2026'
EXT = '.dat'
########################################################################################################################
data = pd.DataFrame()
data_valid = pd.DataFrame()
try:
    data = data_read(temp_dir=TEMP_DIR)
except FileNotFoundError:
    datafiles = list_data_files(data_dir=DATA_PATH, file_ext=EXT, file_init=INIT)
    data = data_merge(data_format=PICARRO, rawfiles=datafiles, temp_dir=TEMP_DIR)
data, data_valid = data_fill(data, datatype="PICARRO")
data_resample(data, data_valid, FREQ, output_dir=DATA_PATH + r'\output')
