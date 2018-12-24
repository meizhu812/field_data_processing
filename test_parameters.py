# coding=utf-8
from pandas import date_range
from parameter_templet import ProjectConfiguration,DataDescription

PROJECT = ProjectConfiguration(path=r'D:\Temp\test_data',
                               freq='15T',
                               temp_path=r'D:\Temp\test_temp',
                               cpus=16)
SONIC = DataDescription(project=PROJECT,
                        sub_path=r'\sonic_test',
                        data_format=dict(header=0,
                                         skiprows=[0, 2, 3],
                                         usecols=[0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                                         na_values="NAN",
                                         parse_dates=[0]),
                        file_pattern=dict(file_init='TOA5_8329.ts',
                                          file_ext='.dat'),
                        data_period=date_range('2016-06-27 12:30:00', '2016-07-18 09:00:00', freq=PROJECT.FREQ))
AMMONIA_H = DataDescription(project=PROJECT,
                            sub_path=r'\ammonia_test',
                            data_format=dict(sep='\s+',
                                             usecols=[0, 1, 17, 21],
                                             parse_dates=[[0, 1]]),
                            file_pattern=dict(file_init='AEDS2044',
                                              file_ext='.dat'))
