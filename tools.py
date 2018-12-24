from timer import *
import os
import pandas as pd
import numpy as np
from pandas.tseries.offsets import Second
from progress_bar import progress_bar as pgb


def list_datafiles(*, data_dir: str, file_ext: str, file_init: str) -> list:
    """
    #
    :param data_dir: folder containing datafiles (and in subfolders)
    :param file_ext: file extension（e.g. '.dat'）
    :param file_init: head of file names
    :return: list containing full path of datafiles
    """
    datafiles = []
    for (dir_name, dirs_here, files_here) in os.walk(data_dir):
        for filename in files_here:
            if filename.endswith(file_ext) and filename.startswith(file_init):
                pathname = os.path.join(dir_name, filename)
                datafiles.append(pathname)
    for datafile in datafiles:
        print(datafile[len(data_dir):])
    print('\n[ ' + str(len(datafiles)) + ' ] Data files found.')
    input("Check datafiles sequence, press Enter to continue...\n")
    return datafiles


def data_merge(data_type: str, datafiles: list, temp_dir: str) -> pd.DataFrame:
    """
    merge datafiles while filtering useless columns in raw data files; saving dataset to pickles
    :param data_type: "picarro" or "IRGASON"
    :param datafiles: list containing full path of datafiles
    :param temp_dir: pickles' location
    :return: DataFrame of a complete dataset
    """
    data = pd.DataFrame()
    counter = 0
    timer.start("Reading data files for merging...\n")
    for datafile in datafiles:

        if data_type == "picarro":
            data = pd.DataFrame.append(data,
                                       pd.read_csv(datafile, sep='\s+', usecols=[0, 1, 17, 21], parse_dates=[[0, 1]]),
                                       ignore_index=True)
        elif data_type == "csat3":
            data = pd.DataFrame.append(data,
                                       pd.read_csv(datafile, header=0, skiprows=[0, 2, 3],
                                                   usecols=[0, 2, 3, 4, 5, 7, 8, 10, 11], na_values="NAN",
                                                   parse_dates=[0]))
        elif data_type == "2018S":
            data = pd.DataFrame.append(data,
                                       pd.read_csv(datafile, header=0, skiprows=[0, 2, 3],
                                                   usecols=[0, 2, 3, 4, 5], na_values="NAN",
                                                   parse_dates=[0]))
        counter += 1
        pgb(counter, len(datafiles))
    print('All datafiles merged. ', end='\r')
    timer.stop()
    data.set_index(data.columns[0], inplace=True)  # set the timestamp column as index
    print(data.head())
    timer.start("Saving merged data to files... ")
    try:
        os.mkdir(temp_dir)
    except FileExistsError:
        pass
    data.to_pickle(temp_dir + r'\data_save')
    timer.stop()
    return data


def data_split(data: pd.DataFrame, split_time, output_dir):
    """
    :param data:
    :param data_type:
    :param st_devs:
    :param aver_freq:
    :return:
    """
    data_quality=pd.DataFrame([])
    data_quality['time']=split_time
    data_quality['count']=-1
    i = -1
    while i < (len(split_time)-2):
        i += 1
        try:
            data_part = data[split_time[i]:(split_time[i + 1])]
            data_quality.iloc[i + 1, 1] = len(data_part)
            part_name = str(split_time[i + 1].date()) + '_' + str(split_time[i + 1].time()).replace(':', '-')[:-3]
            data_part.to_csv(output_dir + '\\' + part_name + '.csv', index=False)
        except:
            print(split_time[i+1])
            data_quality.iloc[i + 1,1]=0
        # data_time1=data_part.index.strftime('%Y-%m-%d %H:%M:%S.%f').tolist()
        # data_time2=[]
        # for a1 in data_time1:
        #     a1=a1[:-5].rstrip('0').rstrip('.')
        #     data_time2.append(a1)
        # data_part['dt']=data_time2
        # data_part.drop(labels=['dt'], axis=1, inplace=True)
        # data_part.insert(0, 'dt', data_time2)
    data_quality.to_csv(output_dir+'\\'+'data_quality'+'.csv')


def data_read(data_dir, temp_dir):
    """

    :param data_dir:
    :param temp_dir:
    :return:
    """
    timer.start("Reading merged data from files... ")
    data = pd.read_pickle(temp_dir + r'\data_save')
    timer.stop
    return data


def data_filter(data: pd.DataFrame, data_type: str, st_devs: list, aver_freq: int):
    """

    :param data:
    :param data_type:
    :param st_devs:
    :param aver_freq:
    """
    if data_type == "picarro":
        data_cols = range(2)
    else:
        data_cols = range(8)


def data_fill(data, datatype):
    """

    :param data:
    :param datatype:
    :return:
    """
    if datatype == "picarro":
        fill_freq = '0.5S'
    else:
        fill_freq = '0.1S'
    timer.start("Filling missing data... ")
    data_valid = pd.Series(1, index=data.index, name='VALID')
    data = data.resample(fill_freq).mean().interpolate(method='time', limit=5)
    data_valid = data_valid.resample(fill_freq).sum()
    timer.stop

    return data, data_valid


def data_resample(data, data_valid, freq, output_dir):
    """

    :param data:
    :param data_valid:
    :param freq:
    :param output_dir:
    :return:
    """
    data_resamp = data.resample(freq).mean()
    data_valid_resamp = data_valid.resample(freq).sum()
    data_resamp.to_csv(output_dir + r'\data_resamp.csv')
    data_valid_resamp.to_csv(output_dir + r'\data_valid.csv')
    return ()

    # start_time=start_timer("Processing merged data file... ")
    # stop_timer(start_time)

    # data["DATACOUNT"]=1
    # datacounts=data.iloc[:,-1]
    # datacounts.to_pickle(output_dir+r'\datacounts_save')
    # datacounts_resamp=datacounts.resample('1S').sum()

# def write_param(work_dir,output_dir):\
# data=pd.read_csv(work_dir+r'\flux.csv',usecols=['date','time','wind_speed','u*','H'])

# def fp_avr(work_dir):
#     for (dirname, dirshere, fileshere) in os.walk(work_dir):
#         for filename in fileshere:
#             day = '01'
#             grd = pd.DataFrame()
#             if filename.endswith('01.grd'):
#                 day = '01'
#                 if filename[4:6] == day:
#                     grd = grd + pd.read_table(os.path.join(dirname, filename), sep='\s+', header=None, skiprows=5)
#                 else:
#                     outputfile = os.path.join(dirname, filename[:-7]) + '#01.grd'
#                     file = open(outputfile, 'w')
#                     file.write(
#                         'DSAA\n         150         150\n           0    7.500001E-01\n           0    7.500001E-01\n           0    %e\n' % (
#                             grd.max().max()))
#                     grd.to_csv(outputfile, sep=' ', header=False, index=False, mode='a', float_format='%.2e')
#                     grd = pd.DataFrame()
#                     day = filename[4:6]
