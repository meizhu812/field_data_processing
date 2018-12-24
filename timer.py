import time


# noinspection PyMissingOrEmptyDocstring
class Timer:

    def __init__(self):
        self.start_time = 0

    def start(self, prompt):
        print(prompt)
        self.start_time = time.time()

    def stop(self):
        print('[Completed in ' + str(round(time.time() - self.start_time, 2)) + ' seconds.]')


timer = Timer()
