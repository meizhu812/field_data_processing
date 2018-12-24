# coding=utf-8
from common_methods import list_data_files_pro, grid_average, grid_file_grouping
import pandas as pd
# Parameters ###########################################################################################################
DATA_PATH = r'd:\Desktop\present_work\01_ammonia\02_prelim\03_Summer2018\01_footprint\North'
INIT = '18'
EXT = '.grd'
HOURS_GROUPS = {'00010203':[], '04050607':[], '08091011':[],  '12131415':[], '16171819':[], '20212223':[]}
########################################################################################################################
grid_files = list_data_files_pro(
    data_dir=DATA_PATH, file_ext=EXT,
    file_init=INIT)
day_groups = grid_file_grouping(grid_files, key_name='day', key_loc=slice(0, 6, 1), grid_groups={})
hours_groups = grid_file_grouping(grid_files, key_name='hour', key_loc=slice(6, 8, 1), grid_groups=HOURS_GROUPS)
grid_average(day_groups, DATA_PATH+'\\day')
grid_average(hour_groups, DATA_PATH+'\\hour')
x = input('Press Enter to exit...')
