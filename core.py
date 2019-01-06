from itertools import islice
from pandas.tseries.offsets import Minute
from pandas import concat, DataFrame, read_csv, read_pickle
from multiprocessing import freeze_support, Pool
from accessories import Timer, show_progress
import os
from _datatools import get_files_list


class ProjectConfig:
    def __init__(self, name: str, path: str, raw_sub: str, output_sub: str, freq: str, cpus: int):
        self.NAME = name
        self.PATH = path
        self.RAW_SUB = raw_sub
        self.OUTPUT_SUB = output_sub
        self.TEMP_PATH = self.PATH + r'\temp'
        self.FREQ = freq
        self.CPUS = cpus

    def __enter__(self):
        print("\nProcessing data from project[{}]".format(self.NAME))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Finished processing project[{}]".format(self.NAME))


class DataSet:
    def __init__(self, project: ProjectConfig, name: str, data_type: str, sub_path: str, data_format: dict, file_pattern,
                 data_period=None):
        self.PROJECT = project
        self.NAME = name
        self.TYPE = data_type
        self.RAW_PATH = project.PATH + project.RAW_SUB + sub_path
        self.TEMP_PATH = project.TEMP_PATH + sub_path + '\\'
        self.OUTPUT_PATH = project.PATH + project.OUTPUT_SUB + sub_path + '\\'
        self.DATA_FORMAT = data_format
        self.FILE_PATTERN = file_pattern
        self.DATA_PERIOD = data_period
        self.data_files = []
        self.data = DataFrame()

    def __enter__(self):
        print("\nProcessing data set [{}]".format(self.NAME))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Finished processing data set [{}]".format(self.NAME))

    def get_data(self):
        try:
            self.data = self._read_pickle()
        except FileNotFoundError:
            print("Pickle file does not exist!")
            self.data_files = get_files_list(path=self.RAW_PATH, **self.FILE_PATTERN)
            self._merge_data()

    def process_data(self):
        if self.TYPE == 'SONIC':
            self._sonic_split()
        elif self.TYPE == 'AMMONIA':
            self._ammonia_resample()
        else:
            print("Not processed.")

    def _read_pickle(self):
        pickle_path = self.TEMP_PATH + self.NAME
        print("# Reading data from pickle:\n"
              "# [{}]".format(pickle_path))
        data = read_pickle(pickle_path)
        return data

    def _save_data(self):
        pickle_path = self.TEMP_PATH + self.NAME
        print("# Saving data to pickle:\n"
              "# [{}]".format(pickle_path))
        self.data.to_pickle(pickle_path)

    @staticmethod
    def _read_data_file(data_file: dict, data_format: dict) -> DataFrame:
        datum = read_csv(data_file['path'], **data_format)
        datum.set_index(datum.columns[0], inplace=True)
        return datum

    def _merge_data(self):
        timer_merge = Timer()
        timer_merge.start("Reading data files for merging", "Initializing")
        with Pool(self.PROJECT.CPUS) as p:
            timer_merge.switch("Reading with %i processes" % self.PROJECT.CPUS)
            datum_async_list = p.starmap_async(self._read_data_file,
                                               [(data_file, self.DATA_FORMAT) for data_file in self.data_files])
            show_progress(datum_async_list)
            datum_list = datum_async_list.get()
        timer_merge.switch("Merging data")
        self.data = concat(datum_list)
        timer_merge.switch("Saving data")
        os.makedirs(self.TEMP_PATH, exist_ok=True)
        self._save_data()
        timer_merge.stop()

    def _sonic_split(self):
        data_quality = DataFrame([])
        data_quality['time'] = self.DATA_PERIOD
        data_quality['count'] = -1
        os.makedirs(self.OUTPUT_PATH, exist_ok=True)
        timer_split = Timer()
        timer_split.start("Splitting data files", "Processing data")
        data_period_divided_all = self.data_period_divide(self.data, self.DATA_PERIOD, self.PROJECT.CPUS)
        with Pool(self.PROJECT.CPUS) as p:
            timer_split.switch('Writing data files')
            results = [p.apply_async(self.split_data, (data_period_divided[0], data_period_divided[1], self.OUTPUT_PATH)) for
                       data_period_divided in data_period_divided_all]
            print(len(results))
            for result in results:
                result.wait()

    @staticmethod
    def data_period_divide(data: DataFrame, data_period, cpus=os.cpu_count()):
        chunk_size, extra = divmod(len(data_period), cpus * 8)
        if extra:
            chunk_size += 1
        split_time_iter = iter(data_period)
        while 1:
            divide_time = tuple(islice(split_time_iter, chunk_size))
            if not divide_time:
                return
            i = 0
            j = -1
            start_time = divide_time[i]
            end_time = divide_time[j] + Minute(15)
            try:
                yield (data[start_time:end_time], divide_time)
            except:
                pass

    @staticmethod
    def split_data(data_divided: DataFrame, divide_time, output_dir: str):
        i = 0
        while i < len(divide_time):
            try:
                data_part = data_divided[divide_time[i]:(divide_time[i] + Minute(15))]
                part_name = str((divide_time[i] + Minute(15)).date()) + '_' + str(
                    (divide_time[i] + Minute(15)).time()).replace(':', '-')[:-3]
                file_path = output_dir + '\\' + part_name + '.csv'
                data_part.to_csv(file_path, index=False)
            except Exception as e:
                print(e)
            i += 1
        return True

    def _ammonia_resample(self):
        print("\nResampling data")
        self.data = self.data.tshift(8, freq='H')
        self.data = self.data.tshift(15, freq='T')
        data_resamp = self.data.resample(self.PROJECT.FREQ).mean()
        os.makedirs(self.OUTPUT_PATH, exist_ok=True)
        data_resamp.to_csv(self.OUTPUT_PATH + r'\data_resamp.csv')
