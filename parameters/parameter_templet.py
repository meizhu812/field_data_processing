class ProjectConfiguration:
    def __init__(self, path: str, temp_path: str, freq: str, cpus: int):
        self.PATH = path
        self.TEMP_PATH = temp_path
        self.FREQ = freq
        self.CPUS = cpus


class DataDescription:
    def __init__(self, project: ProjectConfiguration, sub_path: str, data_format: dict, file_pattern, data_period=None):
        self.PATH = project.PATH + sub_path
        self.TEMP_PATH = project.TEMP_PATH + sub_path
        self.OUTPUT_PATH = self.PATH + r"\output"
        self.DATA_FORMAT = data_format
        self.FILE_PATTERN = file_pattern
        self.DATA_PERIOD = data_period
