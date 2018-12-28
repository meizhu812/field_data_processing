from parameters.project_2016_summer_PARAMS import PROJECT, SONIC, AMMONIA_H, AMMONIA_L
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

    ammonia_files_h = get_data_files(data_path=AMMONIA_H.PATH, **AMMONIA_H.FILE_PATTERN)
    ammonia_data_h = data_merge(data_files=ammonia_files_h,
                                data_format=AMMONIA_H.DATA_FORMAT,
                                temp_path=AMMONIA_H.TEMP_PATH)
    data_resample(ammonia_data_h, PROJECT.FREQ, AMMONIA_H.OUTPUT_PATH)

    ammonia_files_l = get_data_files(data_path=AMMONIA_L.PATH, **AMMONIA_L.FILE_PATTERN)
    ammonia_data_l = data_merge(data_files=ammonia_files_l,
                                data_format=AMMONIA_L.DATA_FORMAT,
                                temp_path=AMMONIA_L.TEMP_PATH)
    data_resample(ammonia_data_l, PROJECT.FREQ, AMMONIA_L.OUTPUT_PATH)
