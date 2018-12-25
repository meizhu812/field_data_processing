# coding=utf-8
"""
aaa
"""
from os import mkdir, walk, path
from gadgets import Timer, ProgressBar as PgB
from pandas import DataFrame, Series, read_csv, read_pickle
from multiprocessing import Pool


# from pandas.tseries.offsets import Second


def list_data_files(*, data_dir: str, file_ext: str, file_init: str) -> list:
    """
    #
    :param data_dir: folder containing datafiles (also in sub-folders)
    :param file_ext: file extension（e.g. '.dat'）
    :param file_init: initials of file names
    :return: list containing full paths of datafiles
    """
    datafiles = []
    for (dir_name, dirs_here, files_here) in walk(data_dir):
        for filename in files_here:
            if filename.endswith(file_ext) and filename.startswith(file_init):
                pathname = path.join(dir_name, filename)
                datafiles.append(pathname)
    for datafile in datafiles:
        print(datafile[len(data_dir):])  # print heads and tails
    print('\n[ ' + str(len(datafiles)) + ' ] Data files found.')
    input("Check datafiles sequence, press Enter to continue...\n")
    return datafiles


def list_data_files_pro(*, data_dir: str, file_ext: str, file_init: str) -> list:
    data_files = []
    for (dir_name, dirs_here, files_here) in walk(data_dir):
        for filename in files_here:
            if filename.endswith(file_ext) and filename.startswith(file_init):
                pathname = path.join(dir_name, filename)
                data_file = {'path': pathname}
                data_files.append(data_file)
    for datafile in (data_files[:10] + data_files[-10:]):
        print(datafile['path'][len(data_dir):])  # print heads and tails
    print('\n[ ' + str(len(data_files)) + ' ] Data files found.')
    input("Check datafiles sequence, press Enter to continue...\n")
    return data_files


def data_merge(*, data_format: dict, rawfiles: list, temp_dir: str) -> DataFrame:
    """
    merge datafiles and filter useless columns in raw data files; saving merged data to pickles
    :param data_format: "picarro" or "IRGASON"
    :param rawfiles: list containing full path of raw datafiles
    :param temp_dir: pickles' location
    :return: DataFrame of a complete data set
    """
    data = DataFrame()
    pgb_merge = PgB()
    pgb_merge.start(len(rawfiles), "\nReading data files for merging...")
    for datafile in rawfiles:
        data = DataFrame.append(data, read_csv(datafile, **data_format))
        pgb_merge.advance()
        # elif data_format == "2018S":
        #     data = pd.DataFrame.append(data,
        #                                pd.read_csv(datafile, header=0, skiprows=[0, 2, 3],
        #                                            usecols=[0, 2, 3, 4, 5], na_values="NAN",
        #                                            parse_dates=[0]))
    print('All datafiles merged.')
    data.set_index(data.columns[0], inplace=True)  # set the timestamp column as index
    # print(data.head())
    timer_save = Timer()
    timer_save.start("\nSaving merged data to " + temp_dir)
    try:
        mkdir(temp_dir)
    except FileExistsError:
        pass
    data.to_pickle(temp_dir + r'\data_save')
    timer_save.stop()
    return data


def data_split(data: DataFrame, split_time, output_dir):
    """
    :param data:
    :param data_type:
    :param st_devs:
    :param aver_freq:
    :return:
    """
    print(__name__)
    input()
    data_quality = DataFrame([])
    data_quality['time'] = split_time
    data_quality['count'] = -1
    try:
        mkdir(output_dir)
    except FileExistsError:
        pass
    pgb_split = PgB()
    pgb_split.start(len(split_time) - 1, "\nSplitting data files...")
    i = -1
    while i < (len(split_time) - 2):
        i += 1
        try:
            data_part = data[split_time[i]:(split_time[i + 1])]
            data_quality.iloc[i + 1, 1] = len(data_part)
            part_name = str(split_time[i + 1].date()) + '_' + str(split_time[i + 1].time()).replace(':', '-')[:-3]
            data_part.to_csv(output_dir + '\\' + part_name + '.csv', index=False)
        except:
            # print(split_time[i + 1])
            data_quality.iloc[i + 1, 1] = 0
        pgb_split.advance()
        # pgb_split.show(i+1.0)
        # data_time1=data_part.index.strftime('%Y-%m-%d %H:%M:%S.%f').tolist()
        # data_time2=[]
        # for a1 in data_time1:
        #     a1=a1[:-5].rstrip('0').rstrip('.')
        #     data_time2.append(a1)
        # data_part['dt']=data_time2
        # data_part.drop(labels=['dt'], axis=1, inplace=True)
        # data_part.insert(0, 'dt', data_time2)
    pgb_split.end()
    data_quality.to_csv(output_dir + '\\' + 'data_quality' + '.csv')

def data_split_multi(data: DataFrame, split_time, output_dir):
    """
    :param data:
    :param data_type:
    :param st_devs:
    :param aver_freq:
    :return:
    """
    data_quality = DataFrame([])
    data_quality['time'] = split_time
    data_quality['count'] = -1
    try:
        mkdir(output_dir)
    except FileExistsError:
        pass
    timer_split=Timer()
    timer_split.start("\nSplitting data files...")
    i = -1
    jobs = []
    while i < (len(split_time) - 2):
        i += 1
        try:
            data_part = data[split_time[i]:(split_time[i + 1])]
            part_name = str(split_time[i + 1].date()) + '_' + str(split_time[i + 1].time()).replace(':', '-')[:-3]
            filepath= output_dir + '\\' + part_name + '.csv'
            job=[data_part, filepath]
            jobs.append(job)
        except:
            # print(split_time[i + 1])
            pass
    if __name__ == 'common_methods':
        with Pool(8) as p:
            output = list(p.map(split_file, jobs))
            print(output)
        # pgb_split.show(i+1.0)
        # data_time1=data_part.index.strftime('%Y-%m-%d %H:%M:%S.%f').tolist()
        # data_time2=[]
        # for a1 in data_time1:
        #     a1=a1[:-5].rstrip('0').rstrip('.')
        #     data_time2.append(a1)
        # data_part['dt']=data_time2
        # data_part.drop(labels=['dt'], axis=1, inplace=True)
        # data_part.insert(0, 'dt', data_time2)
    timer_split.stop()
    #data_quality.to_csv(output_dir + '\\' + 'data_quality' + '.csv')


def data_read(temp_dir):
    """
    :param temp_dir:
    :return:
    """
    timer_read = Timer()
    timer_read.start("\nReading merged data from files... ")
    data = read_pickle(temp_dir + r'\data_save')
    timer_read.stop()
    return data


def data_filter(data: DataFrame, data_type: str, st_devs: list, aver_freq: int):
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


def data_fill(data: DataFrame, datatype: str):
    """

    :param data:
    :param datatype:
    :return:
    """
    if datatype == "PICARRO":
        fill_freq = '0.5S'
    else:
        fill_freq = '0.1S'
    timer_fill = Timer()
    timer_fill.start("\nFilling missing data... ")
    data_valid = Series(1, index=data.index, name='VALID')
    data = data.resample(fill_freq).mean().interpolate(method='time', limit=5)
    data_valid = data_valid.resample(fill_freq).sum()
    timer_fill.stop()
    return data, data_valid


def data_resample(data, data_valid, freq, output_dir):
    """

    :param data:
    :param data_valid:
    :param freq:
    :param output_dir:
    :return:
    """
    timer_resamp = Timer()
    timer_resamp.start("\nResampling data... ")
    data_resamp = data.resample(freq).mean()
    data_valid_resamp = data_valid.resample(freq).sum()
    timer_resamp.stop()
    try:
        mkdir(output_dir)
    except FileExistsError:
        pass
    data_resamp.to_csv(output_dir + r'\data_resamp.csv')
    data_valid_resamp.to_csv(output_dir + r'\data_valid.csv')
    return ()


def grid_file_grouping(grid_files: list, key_name: str, key_loc: slice, grid_groups: dict):
    timer_group = Timer()
    timer_group.start('Begin grouping...')
    for grid_file in grid_files:
        grid_file[key_name] = grid_file['path'].split('\\')[-1][key_loc]
        for grid_group in grid_groups:
            if not grid_file[key_name] in grid_group:
                grid_groups[grid_file[key_name]] = []  # create new group
            grid_groups[grid_file[key_name]].append(grid_file['path'])
    timer_group.stop()
    return grid_groups


def grid_average(grid_groups, output_dir: str):
    pgb_average = PgB()
    pgb_average.start(len(grid_groups), 'Averaging groups...')
    for grid_group in grid_groups:
        average_grid = DataFrame
        i = 0
        for grid_file in grid_groups[grid_group]:
            if i == 0:
                average_grid = read_csv(grid_file, skiprows=[0, 1, 2, 3, 4], sep='\s+', header=None, index_col=False)
            else:
                average_grid += read_csv(grid_file, skiprows=[0, 1, 2, 3, 4], sep='\s+', header=None,
                                            index_col=False)
            i += 1
        average_grid /= i
        try:
            mkdir(output_dir)
        except FileExistsError:
            pass
        out_path = output_dir + '\\' + grid_group + '.grd'
        average_grid.to_csv(out_path, sep=' ', header=False, index=False)
        with open(out_path, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write('DSAA\n150 150\n0 0.7500001\n0 0.7500001\n0 1.000000\n' + content)
        pgb_average.advance()
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
