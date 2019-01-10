from dataclasses import dataclass
from itertools import islice
from pandas.tseries.offsets import Minute
from pandas import concat, DataFrame, read_csv, read_pickle
from multiprocessing import freeze_support, Pool
from accessories import Timer, show_async_progress, show_map_progress
import os, time
from _datatools import get_files_list


@dataclass
class ProjectConfig:
    name: str
    path: str
    raw_sub: str
    prep_sub: str
    freq: str
    cores: int = os.cpu_count()

    def __post_init__(self):
        self.temp_path = self.path + r'\temp'

    def __enter__(self):
        print("\nProcessing data from project[{}]".format(self.name))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Finished processing project[{}]".format(self.name))


@dataclass
class RawData:
    project: ProjectConfig
    name: str
    data_type: str
    sub_path: str
    data_format: dict
    file_pattern: dict
    data_period: None = None
    data_files = []
    data = DataFrame()

    def __post_init__(self):
        self.raw_path = self.project.path + self.project.raw_sub + self.sub_path
        self.temp_path = self.project.temp_path + self.sub_path + '\\'
        self.output_path = self.project.path + self.project.prep_sub + self.sub_path + '\\'

    def __enter__(self):
        print("\nProcessing data set [{}]".format(self.name))
        time.sleep(0.5)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Finished processing data set [{}]".format(self.name))
        del self

    def get_data(self):
        try:
            self.data = self._read_pickle()
        except FileNotFoundError:
            print("Pickle file does not exist!")
            self.data_files = get_files_list(path=self.raw_path, **self.file_pattern)
            for datafile in self. data_files:
                datafile['data_format'] = self.data_format
            self._merge_data()
        except Exception as e:
            print(e)

    def process_data(self):
        if self.data_type == 'SONIC':
            self._sonic_split()
        elif self.data_type == 'AMMONIA':
            self._ammonia_resample()
        else:
            input("Check data type! Press Enter to continue.")

    def _read_pickle(self):
        pickle_path = self.temp_path + self.name
        print("# Reading data from pickle:\n"
              "# [{}]".format(pickle_path))
        data = read_pickle(pickle_path)
        return data

    def _save_data(self):
        pickle_path = self.temp_path + self.name
        print("# Saving data to pickle:\n"
              "# [{}]".format(pickle_path))
        self.data.to_pickle(pickle_path)

    @staticmethod
    def _read_data_file(data_file: dict) -> DataFrame:
        datum = read_csv(data_file['path'], **data_file['data_format'])
        datum.set_index(datum.columns[0], inplace=True)
        return datum

    def _merge_data(self):
        timer_merge = Timer()
        timer_merge.start("Reading data files for merging", "Initializing")
        with Pool(self.project.cores) as p:
            timer_merge.switch("Reading with {} processes".format(self.project.cores))
            datum_async_list = p.map_async(self._read_data_file, self.data_files)
            show_map_progress(datum_async_list)
        timer_merge.switch("Merging data")
        self.data = concat(datum_async_list.get())
        timer_merge.switch("Saving data")
        os.makedirs(self.temp_path, exist_ok=True)
        self._save_data()
        timer_merge.stop()

    @staticmethod
    def split_data_chunk(data_chunk: DataFrame, data_period, output_path: str):
        i = 0
        while i < len(data_period):
            start_time = data_period[i]
            end_time = start_time + Minute(15)
            try:
                data_part = data_chunk[start_time:end_time]
                part_name = str(end_time.date()) + '_' + str(end_time.time()).replace(':', '-')[:-3]
                file_path = output_path + '\\' + part_name + '.csv'
                data_part.to_csv(file_path, index=False)
            except Exception as e:
                print(e)
            i += 1
        return True

    def _sonic_split(self):
        os.makedirs(self.output_path, exist_ok=True)
        timer_split = Timer()
        timer_split.start("Splitting data files", "Processing data")
        data_chunks = self.chunk_data(self.data, self.data_period, self.project.cores)
        with Pool(self.project.cores) as p:
            timer_split.switch('Writing data files')
            results = [p.apply_async(self.split_data_chunk, (*data_chunk_n_period, self.output_path)) for
                       data_chunk_n_period in data_chunks]
            input()

            show_async_progress(results)
            timer_split.stop()

    @staticmethod
    def chunk_data(data: DataFrame, data_period, cpus=os.cpu_count()):
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
            except Exception as e:
                print(e)

    def _ammonia_resample(self):
        print("\nResampling data")
        self.data = self.data.tshift(8, freq='H')
        self.data = self.data.tshift(15, freq='T')
        data_resamp = self.data.resample(self.project.freq).mean()
        os.makedirs(self.output_path, exist_ok=True)
        data_resamp.to_csv(self.output_path + r'\data_resamp.csv')


def prepare_data(project_param, raw_data_param):
    with ProjectConfig(**project_param) as project:
        with RawData(project=project, **raw_data_param) as raw_data:
            raw_data.get_data()
            raw_data.process_data()
