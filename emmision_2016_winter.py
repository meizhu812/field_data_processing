import pandas as pd
import numpy as np
from pandas import read_csv

DATA_PATH = r'd:\Desktop\present_work\01_ammonia\02_prelim\02_Winter2016\02_source'
FC_SUM = DATA_PATH + r'\fc_sum.csv'
C_N = DATA_PATH+r'\c_n.csv'
C_S = DATA_PATH+r'\c_s.csv'
SUM_UP = DATA_PATH+r'\sum_up.csv'
dateparse = lambda dates: pd.datetime.strptime(dates[0:10], '%y%m%d%H%M')
order_f = [' Datetm', ' Site_no', ' Fcsum(s/m)', ' Ratio(%)'] #todo
order_c = ['DATE_TIME', 'NH3_Raw']
fc_sum = pd.read_csv(FC_SUM, usecols=order_f, na_values=-9999, parse_dates=[0],date_parser=dateparse)
fc_sum.set_index(fc_sum.columns[0], inplace=True)
fc_n = fc_sum[fc_sum[' Site_no'] == '#1']
fc_s = fc_sum[fc_sum[' Site_no'] == '#2']
fc_n.drop(columns=[' Site_no'],inplace=True)
fc_s.drop(columns=[' Site_no'],inplace=True)
fc_n.columns=['Fc_n','R_n']
fc_s.columns=['Fc_s','R_s']
#c_n = read_csv(FP_SUM, skiprows=[0, 2], usecols=order, parse_dates=[[0, 1]], na_values=-9999) #todo
c_n = read_csv(C_N, usecols=order_c, parse_dates=[0], na_values=-9999)
c_n.set_index(c_n.columns[0], inplace=True)
c_n.columns=['c_n']
c_s = read_csv(C_S, usecols=order_c, parse_dates=[0], na_values=-9999)
c_s.set_index(c_s.columns[0], inplace=True)
c_s.columns=['c_s']
c_all = pd.merge(c_n, c_s, left_index=True, right_index=True, how='outer')
fc_all= pd.merge(fc_n, fc_s, left_index=True, right_index=True, how='outer')
sumup = pd.merge(fc_all, c_all, left_index=True, right_index=True, how='outer')
sumup.eval('Qn = (c_n -c_s)/Fc_n', inplace=True)
sumup.eval('Qs = (c_s -c_n)/Fc_s', inplace=True)
sumup.replace([np.inf, -np.inf], np.nan,inplace=True)
sumup.to_csv(SUM_UP)
# print(fc_n)
# print(c_all)
# print(c_s)
# print(c_n)
print(sumup)