from parameters.project_2018_summer_PARAMS import PROJECT, SONIC, AMMONIA_N, AMMONIA_S
from datatools import read_temp_data, get_data_files, data_merge, data_split, data_resample
from multiprocessing import freeze_support

if __name__ == '__main__':
    freeze_support()
    try:
        sonic_data = read_temp_data(temp_path=SONIC.TEMP_PATH)
    except FileNotFoundError:
        print("Merged data does not exist!")
        sonic_files = get_data_files(data_path=SONIC.PATH, **SONIC.FILE_PATTERN)
        sonic_data = data_merge(data_files=sonic_files,
                                data_format=SONIC.DATA_FORMAT,
                                temp_path=SONIC.TEMP_PATH,
                                cpus=16)
    data_split(sonic_data, SONIC.DATA_PERIOD, SONIC.OUTPUT_PATH, cpus=16)

    ammonia_files_n = get_data_files(data_path=AMMONIA_N.PATH, **AMMONIA_N.FILE_PATTERN)
    ammonia_data_n = data_merge(data_files=ammonia_files_n,
                                data_format=AMMONIA_N.DATA_FORMAT,
                                temp_path=AMMONIA_N.TEMP_PATH)
    data_resample(ammonia_data_n, PROJECT.FREQ, AMMONIA_N.OUTPUT_PATH)

    ammonia_files_s = get_data_files(data_path=AMMONIA_S.PATH, **AMMONIA_S.FILE_PATTERN)
    ammonia_data_s = data_merge(data_files=ammonia_files_s,
                                data_format=AMMONIA_S.DATA_FORMAT,
                                temp_path=AMMONIA_S.TEMP_PATH)
    data_resample(ammonia_data_s, PROJECT.FREQ, AMMONIA_S.OUTPUT_PATH)
