# coding=utf-8
"""
This file contains parameters to use in project 2016_summer.
"""
from pandas import date_range
from core import ProjectConfig, RawData

PROJECT = ProjectConfig(name='2016_summer',
                        path=r'D:\Desktop\present_work\01_ammonia\01_data\01_2016summer',
                        freq='15T',
                        temp_path=r'D:\Temp\2016_summer',
                        cpus=16)
SONIC = RawData(project=PROJECT,
                sub_path=r'\01_meteorology',
                data_format=dict(header=0,
                                         skiprows=[0, 2, 3],
                                         usecols=[0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                                         na_values="NAN",
                                         parse_dates=[0]),
                file_pattern=dict(file_init='TOA5_8329.ts',
                                          file_ext='.dat'),
                data_period=date_range('2016-06-27 12:30:00', '2016-07-18 09:00:00', freq=PROJECT.FREQ))
AMMONIA_H = RawData(project=PROJECT,
                    sub_path=r'\02_concentration\02_high8m',
                    data_format=dict(sep='\s+',
                                             usecols=[0, 1, 17, 21],
                                             parse_dates=[[0, 1]]),
                    file_pattern=dict(file_init='AEDS2044',
                                              file_ext='.dat'))
AMMONIA_L= RawData(project=PROJECT,
                   sub_path=r'\02_concentration\01_low2m',
                   data_format=dict(sep='\s+',
                                             usecols=[0, 1, 17, 21],
                                             parse_dates=[[0, 1]]),
                   file_pattern=dict(file_init='AEDS2026',
                                              file_ext='.dat'))
