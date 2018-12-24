# coding=utf-8
"""
#
"""
from os import walk, path, makedirs, cpu_count
from time import sleep
from pandas import DataFrame, read_csv, concat, date_range
from multiprocessing import Pool
from accessories import Timer, show_progress
from itertools import islice


def list_data_files(*, target_dir: str, file_ext: str, file_init: str) -> list:
    data_files = []
    for (dir_name, dirs_here, files_here) in walk(target_dir):
        for file in files_here:
            if file.endswith(file_ext) and file.startswith(file_init):
                file_path = path.join(dir_name, file)
                data_file = {'file_path': file_path}
                data_files.append(data_file)
    for data_file in (data_files[:10] + data_files[-10:]):  # print heads and tails
        print(data_file['file_path'][len(target_dir):])  # print only file name
    print('\n[ ' + str(len(data_files)) + ' ] Data files found.')
    input("# Check sequence of data files, press Enter to continue...\n")
    return data_files


def read_data_file(data_file: dict, data_format: dict) -> DataFrame:
    datum = read_csv(data_file['file_path'], **data_format)
    datum.set_index(datum.columns[0], inplace=True)
    return datum

def write_data_file(data:DataFrame, path:str):
    data.to_csv(path, index=False)
    return path

def data_merge(*, data_files: list, data_format: dict, temp_dir: str, output_dir: str = '', cpus= cpu_count()):
    timer_merge = Timer()
    timer_merge.start("Reading data files for merging", "Initializing")
    with Pool(cpus) as p:
        timer_merge.switch("Reading with %i processes" % cpus)
        datum_async_list = p.starmap_async(read_data_file, [(data_file, data_format) for data_file in data_files])
        show_progress(datum_async_list)
        datum_list = datum_async_list.get()
    timer_merge.switch("Merging data")
    data_merged = concat(datum_list)
    timer_merge.switch("Saving data")
    makedirs(temp_dir, exist_ok=True)
    data_merged.to_pickle(temp_dir + r'\data_save')
    timer_merge.stop()
    return data_merged

def split_data(data_divided: DataFrame, divide_time, output_dir: str):
    i = 0
    while i < len(divide_time):
        try:
            data_part = data_divided[divide_time[i]:(divide_time[i + 1])]
            part_name = str(divide_time[i + 1].date()) + '_' + str(divide_time[i + 1].time()).replace(':', '-')[:-3]
            file_path = output_dir + '\\' + part_name + '.csv'
            write_data_file(data_part,file_path)
        except:
            # print(split_time[i + 1])
            pass
        i += 1

def data_period_divide(data: DataFrame, data_period, cpus= cpu_count()):
    chunk_size, extra = divmod(len(data_period), cpus * 4)
    if extra:
        chunk_size += 1
    split_time_iter = iter(data_period)
    while 1:
        try:
            divide_time = tuple(islice(split_time_iter, chunk_size))
            if not divide_time:
                return
            yield (data[divide_time[0]:divide_time[-1]], divide_time)
        except KeyError:
            pass



def data_split(data: DataFrame, data_period, output_dir: str, cpus= cpu_count()):
    data_quality = DataFrame([])
    data_quality['time'] = data_period
    data_quality['count'] = -1
    makedirs(output_dir, exist_ok=True)
    timer_split = Timer()
    timer_split.start("Splitting data files", "Processing data")
    data_period_divided_all = data_period_divide(data, data_period, cpus)
    with Pool(cpus) as p:

        timer_split.switch('Writing data files')
        for data_period_divided in data_period_divided_all:
            p.apply_async(split_data, (data_period_divided[0], data_period_divided[1], output_dir))
        p.close()
        p.join()



        # i = -1
        # data_and_paths = []
        # while i < (len(split_time) - 2):
        #     i += 1
        #     try:
        #         data_part = data[split_time[i]:(split_time[i + 1])]
        #         part_name = str(split_time[i + 1].date()) + '_' + str(split_time[i + 1].time()).replace(':', '-')[:-3]
        #         file_path = output_dir + '\\' + part_name + '.csv'


    # output.get()
    # print(output)
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

if __name__ == '__main__':
    from test_parameters import PROJECT, SONIC, AMMONIA_H

    sonic_files = list_data_files(target_dir=SONIC.PATH, **SONIC.FILE_PATTERN)
    sonic_data = data_merge(data_files=sonic_files,
                            data_format=SONIC.DATA_FORMAT,
                            temp_dir=SONIC.TEMP_PATH)
    data_split(sonic_data, SONIC.DATA_PERIOD, SONIC.TEMP_PATH)
    ammonia_files = list_data_files(target_dir=AMMONIA_H.PATH, **AMMONIA_H.FILE_PATTERN)
    ammonia_data = data_merge(data_files=ammonia_files,
                              data_format=AMMONIA_H.DATA_FORMAT,
                              temp_dir=AMMONIA_H.TEMP_PATH)
