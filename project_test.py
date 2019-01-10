from multiprocessing import freeze_support
from core import prepare_data
from parameters.test_parameters import PROJECT, SONIC, AMMONIA_H

if __name__ == '__main__':
    freeze_support()
    prepare_data(PROJECT, SONIC)
    prepare_data(PROJECT, AMMONIA_H)
