# coding=utf-8
# noinspection SpellCheckingInspection
"""
a
"""
import pandas as pd

EDDY_PATH = r'd:\Desktop\present_work\01_ammonia\01_data\01_2016summer\01_meteorology\02_eddy\result'
# noinspection SpellCheckingInspection
INPUT_NAME = r'\eddypro_00_full_output_2018-12-27T125354_adv.csv'
# noinspection SpellCheckingInspection
OUTPUT_NAME = r'\02metdata.dat'
order = ['date', 'time', 'wind_dir', 'wind_speed', 'u*', 'L', 'H', 'air_density']
flux_full = pd.read_csv(EDDY_PATH + INPUT_NAME, skiprows=[0, 2], usecols=order, parse_dates=[[0, 1]], na_values=-9999)
flux_full.set_index(flux_full.columns[0], inplace=True)
flux_full.dropna(inplace=True)
flux_full['key'] = 1
out_order = ['wind_dir', 'wind_speed', 'sigma_v', 'u*', 'L', 'H', 'air_density', 'key']
flux_full['sigma_v'] = flux_full['u*'] * 1.92
flux_out = flux_full[out_order]
out_cols = ['wd(deg)', 'U(m/s)', 'Sgm_v', 'u*(m/s)', 'L(m)', 'H(J/m2)', 'rho(kg/m3)',
            'key(1/0==use ustar & Obu_L /use H_sensible heat)']
flux_out.columns = out_cols
flux_out.to_csv(EDDY_PATH + OUTPUT_NAME, date_format='%y%m%d%H%M', index_label='Datetime', sep='\t',
                float_format='%.3f')
