# coding=utf-8
"""
gadgets
"""
from time import time, sleep


class Timer:
    def __init__(self) -> None:
        self.phase = 0
        self.start_time = 0
        self.stop_time = 0
        self.last_time = 0
        self.process = ''
        self.action = ''

    def start(self, process, action):
        self.process = process
        self.action = action
        self.last_time = self.start_time = time()
        print("\n>>> %s...\n\n" % self.process)
        print("0%i> %s..." % (self.phase, self.action))

    def switch(self, new_action):
        print('0%i| [%s Completed in %s seconds.]\n' % (self.phase, self.action, self.elapsed))
        self.phase += 1
        self.action = new_action
        print("0%i> %s..." % (self.phase, self.action), 1)
        self.last_time = time()

    def stop(self):
        self.stop_time = time()
        print('0%i| [%s Completed in %s seconds.]\n\n' % (self.phase, self.action, self.elapsed))
        print('||| [%s Completed in %s seconds.]\n' % (self.process, self.elapsed_total))

    @property
    def elapsed(self) -> str:
        elapsed = str(round(time() - self.last_time, 2))
        return elapsed

    @property
    def elapsed_total(self) -> str:
        elapsed_total = str(round(time() - self.start_time, 2))
        return elapsed_total


class Progress:
    def __init__(self, result_object, result_type):
        self.result = result_object
        self.type = result_type
        if self.type == 'map':
            self.remain = self.total = self.result.__getattribute__('_number_left')
        elif self.type == 'apply_list':
            self.remain = self.total = len(self.result)
        else:
            print('!!!check code!!!')
        self.progress = 0
        self.output = 0
        self.start_time = time()

    def _show_remain_time(self):
        if self.progress == 0:
            return
        elapsed_time = time() - self.start_time
        remain_time = elapsed_time / self.progress * self.remain
        print("...%3i seconds remaining..." % remain_time, end='')

    def _show_progress(self):
        percentage_5 = self.progress / self.total * 100 // 5 * 5
        print("\r--%2i%%" % percentage_5, end='')

    def _check_progress(self):
        if self.type == 'map':
            self.remain = self.result.__getattribute__('_number_left')
            self.progress = self.total - self.remain
        elif self.type == 'apply_list':
            self.progress = 0
            for async_result in self.result:
                if async_result.ready():
                    self.progress += 1
                    self.remain = self.total - self.progress
        else:
            print('!!!check code!!!')

    def show(self):
        while self.remain > 0:
            self._check_progress()
            self._show_progress()
            if self.output % 10 == 0:
                self._show_remain_time()
            sleep(0.5)
            self.output += 1
        else:
            self._show_progress()
            print()
