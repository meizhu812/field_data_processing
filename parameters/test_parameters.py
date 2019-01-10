# coding=utf-8
from pandas import date_range

PROJECT = dict(name="test_Project",
               path=r'D:\Truman\Temp\test_project',
               raw_sub=r'\00_raw',
               prep_sub=r'\01_prepared',
               freq='15T')
SONIC = dict(name="test_sonic",
             data_type='SONIC',
             sub_path=r'\test_sonic',
             data_format=dict(header=0,
                              skiprows=[0, 2, 3],
                              usecols=[0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                              na_values="NAN",
                              parse_dates=[0]),
             file_pattern=dict(file_init='TOA5_8329.ts',
                               file_ext='.dat'),
             data_period=date_range('2016-06-29 18:30:00', '2016-07-18 08:00:00', freq=PROJECT['freq']))
AMMONIA_H = dict(name="test_ammonia",
                 data_type='AMMONIA',
                 sub_path=r'\test_ammonia',
                 data_format=dict(sep='\s+',
                                  usecols=[0, 1, 17, 21],
                                  parse_dates=[[0, 1]]),
                 file_pattern=dict(file_init='AEDS2044',
                                   file_ext='.dat'))
