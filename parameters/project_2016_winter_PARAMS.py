# coding=utf-8
"""
This file contains parameters to use in project 2016_winter.
"""
from pandas import date_range
from core import ProjectConfig, RawData

PROJECT = ProjectConfig(path=r'D:\Desktop\present_work\01_ammonia\01_data\02_2016winter',
                        freq='15T',
                        temp_path=r'D:\Temp\2016_winter',
                        cpus=16)
SONIC = RawData(project=PROJECT,
                sub_path=r'\01_meteorology\01_north',
                data_format=dict(header=0,
                                         skiprows=[0, 2, 3],
                                         usecols=[0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                                         na_values="NAN",
                                         parse_dates=[0]),
                file_pattern=dict(file_init='TOA5_7808.ts',
                                          file_ext='.dat'),
                data_period=date_range('2016-12-15 11:45:00', '2017-01-10 08:30:00', freq=PROJECT.FREQ))
AMMONIA_N = RawData(project=PROJECT,
                    sub_path=r'\02_concentration\01_north',
                    data_format=dict(sep='\s+',
                                             usecols=[0, 1, 17, 21],
                                             parse_dates=[[0, 1]]),
                    file_pattern=dict(file_init='AEDS2044',
                                              file_ext='.dat'))
AMMONIA_S= RawData(project=PROJECT,
                   sub_path=r'\02_concentration\02_south',
                   data_format=dict(sep='\s+',
                                             usecols=[0, 1, 17, 21],
                                             parse_dates=[[0, 1]]),
                   file_pattern=dict(file_init='AEDS2026',
                                              file_ext='.dat'))
