from multiprocessing import freeze_support
from core import ProjectConfig, DataSet
from parameters.test_parameters import PROJECT, SONIC, AMMONIA_H

if __name__ == '__main__':
    freeze_support()
    with ProjectConfig(**PROJECT) as project:
        with DataSet(project=project, **SONIC) as sonic:
            sonic.get_data()
            sonic.process_data()
        with DataSet(project=project, **AMMONIA_H) as ammonia:
            ammonia.get_data()
            ammonia.process_data()
