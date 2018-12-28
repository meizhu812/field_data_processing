# coding=utf-8
"""
#
"""
import os
from pandas import DataFrame, read_csv, read_pickle, concat
from pandas.tseries.offsets import Minute
from multiprocessing import Pool, freeze_support
from accessories import Timer, show_progress
from itertools import islice


def get_data_files(*, data_path: str, file_init: str, file_ext: str) -> list:
    print("# Listing data files in folder:\n"
          "# [{}]\n".format(data_path))
    data_files = []
    for (dir_name, dirs_here, files_here) in os.walk(data_path):
        for file in files_here:
            if file.endswith(file_ext) and file.startswith(file_init):
                file_path = os.path.join(dir_name, file)
                data_files.append({'path': file_path, 'name': file})
    for data_file in data_files[:7]:  # print heads
        print(data_file['name'])
    print(6 * "...")
    for data_file in data_files[-7:]:  # print tails
        print(data_file['name'])
    print('\n# [ ' + str(len(data_files)) + ' ] Data files found.')
    input("# Check sequence of data files, press Enter to continue...\n")
    return data_files


def read_temp_data(temp_path):
    timer_read = Timer()
    timer_read.start("Reading merged data from temp file", "Reading")
    temp_file = temp_path + r'\data_save'
    print("# Reading data from temp file :\n"
          "# [{}]".format(temp_file))
    data = read_pickle(temp_file)
    timer_read.stop()
    return data


def read_data_file(data_file: dict, data_format: dict) -> DataFrame:
    datum = read_csv(data_file['path'], **data_format)
    datum.set_index(datum.columns[0], inplace=True)
    return datum


def data_merge(*, data_files: list, data_format: dict, temp_path: str, output_dir: str = '', cpus=os.cpu_count()):
    # if __name__ == 'datatools.processing':
    freeze_support()
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
    os.makedirs(temp_path, exist_ok=True)
    data_merged.to_pickle(temp_path + r'\data_save')
    timer_merge.stop()
    return data_merged


def split_data(data_divided: DataFrame, divide_time, output_dir: str):
    i = 0
    while i < len(divide_time):
        try:
            data_part = data_divided[divide_time[i]:(divide_time[i] + Minute(15))]
            part_name = str((divide_time[i] + Minute(15)).date()) + '_' + str(
                (divide_time[i] + Minute(15)).time()).replace(':', '-')[:-3]
            file_path = output_dir + '\\' + part_name + '.csv'
            data_part.to_csv(file_path, index=False)
        except:
            # print(split_time[i + 1])
            pass
        i += 1
    return True


def data_period_divide(data: DataFrame, data_period, cpus=os.cpu_count()):
    chunk_size, extra = divmod(len(data_period), cpus * 8)
    if extra:
        chunk_size += 1
    split_time_iter = iter(data_period)
    # TODO
    while 1:
        divide_time = tuple(islice(split_time_iter, chunk_size))
        if not divide_time:
            return
        successful_divided = False
        i = 0
        j = -1
        start_time = divide_time[i]
        end_time = divide_time[j] + Minute(15)
        # try:
        #     start_time = divide_time[i]
        #     start_data = data[start_time]
        #     del start_data
        # except KeyError:
        #     i += 1
        #     continue
        # try:
        #     end_time = divide_time[j] + Minute(15)
        #     end_data = data[end_time]
        #     del end_data
        # except KeyError:
        #     j -= 1
        #     continue
        # successful_divided = True
        try:
            yield (data[start_time:end_time], divide_time)
        except:
            pass


def data_split(data: DataFrame, data_period, output_dir: str, cpus=os.cpu_count()):
    print(__name__)
    # if __name__ == 'datatools.processing':
    freeze_support()
    data_quality = DataFrame([])
    data_quality['time'] = data_period
    data_quality['count'] = -1
    os.makedirs(output_dir, exist_ok=True)
    timer_split = Timer()
    timer_split.start("Splitting data files", "Processing data")
    data_period_divided_all = data_period_divide(data, data_period, cpus)
    p = Pool(cpus)
    timer_split.switch('Writing data files')
    results = [p.apply_async(split_data, (data_period_divided[0], data_period_divided[1], output_dir)) for
               data_period_divided in data_period_divided_all]
    print(len(results))
    for result in results:
        result.wait()

    p.close()
    p.join()


def data_resample(data, freq, output_dir):
    print("\nResampling data")
    data = data.tshift(8, freq='H')
    data = data.tshift(15, freq='T')
    data_resamp = data.resample(freq).mean()
    os.makedirs(output_dir, exist_ok=True)
    data_resamp.to_csv(output_dir + r'\data_resamp.csv')
    return ()


if __name__ == '__main__':
    from parameters.test_parameters import PROJECT, SONIC, AMMONIA_H

    try:
        sonic_data = read_temp_data(temp_path=SONIC.TEMP_PATH)
    except FileNotFoundError:
        print("Merged data does not exist!")
        sonic_files = get_data_files(data_path=SONIC.PATH, **SONIC.FILE_PATTERN)
        sonic_data = data_merge(data_files=sonic_files,
                                data_format=SONIC.DATA_FORMAT,
                                temp_path=SONIC.TEMP_PATH,
                                cpus=16)
    data_split(sonic_data, SONIC.DATA_PERIOD, SONIC.TEMP_PATH, cpus=16)
    ammonia_files = get_data_files(data_path=AMMONIA_H.PATH, **AMMONIA_H.FILE_PATTERN)
    ammonia_data = data_merge(data_files=ammonia_files,
                              data_format=AMMONIA_H.DATA_FORMAT,
                              temp_path=AMMONIA_H.TEMP_PATH)
    ammonia_data = ammonia_data.tshift(8, freq='H')
    ammonia_data = ammonia_data.tshift(15, freq='T')
    data_resample(ammonia_data, PROJECT.FREQ, AMMONIA_H.TEMP_PATH)
