# coding=utf-8
"""
aaa
"""
from common_methods import data_read, list_data_files, data_merge, data_fill, data_resample
from pandas import DataFrame
# Parameters ###########################################################################################################
DATA_PATH = r'd:\Truman\Desktop\present_work\01_ammonia\01_data\03_2018summer\02South\02Concentration'
TEMP_DIR = r'd:\Temp\PICARRO_2018SS'
FREQ = '15T'
PICARRO = dict(sep='\s+',
               usecols=[0, 1, 17, 21],
               parse_dates=[[0, 1]])
INIT = 'AEDS2044'
EXT = '.dat'
########################################################################################################################
data = DataFrame()
data_valid = DataFrame()
try:
    data = data_read(temp_dir=TEMP_DIR)
except FileNotFoundError:
    datafiles = list_data_files(data_dir=DATA_PATH, file_ext=EXT, file_init=INIT)
    data = data_merge(data_format=PICARRO, rawfiles=datafiles, temp_dir=TEMP_DIR)
data, data_valid = data_fill(data, datatype="PICARRO")
data_resample(data, data_valid, FREQ, output_dir=DATA_PATH + r'\output')
