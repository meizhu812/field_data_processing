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


def get_files_list(*, path: str, file_init: str, file_ext: str) -> list:
    print("# Listing files in folder:\n" +
          "# [{}]\n".format(path) +
          "# INIT:[{}]\t".format(file_init) + "EXT:[{}]\n".format(file_ext))
    files_list = []
    for (dir_name, dirs_here, files_here) in os.walk(path):
        for file in files_here:
            if file.endswith(file_ext) and file.startswith(file_init):
                file_path = os.path.join(dir_name, file)
                files_list.append({'path': file_path, 'dir': dir_name, 'name': file})
    for file in files_list[:7]:  # print heads
        print(file['name'])
    print(6 * "...")
    for file in files_list[-7:]:  # print tails
        print(file['name'])
    print('\n# [ ' + str(len(files_list)) + ' ] files found.')
    input("# Check sequence of data files, press Enter to continue...\n")
    return files_list





