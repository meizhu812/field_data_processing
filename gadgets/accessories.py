# coding=utf-8
"""
gadgets
"""
from time import time, sleep
from multiprocessing import pool


class Timer:
    def __init__(self) -> None:
        self.start_time = 0
        self.stop_time = 0
        self.last_time = 0
        self.process = ''
        self.action = ''

    def start(self, process, action):
        self.process = process
        self.action = action
        self.last_time = self.start_time = time()
        print(">>> Starting %s..." % self.process)
        print("> Starting %s..." % self.action)

    def switch(self, new_action):
        print('| [%s Completed in %s seconds.]' % (self.action, self.elapsed))
        self.action = new_action
        print("> Starting %s..." % self.action)
        self.last_time = time()

    def stop(self):
        self.stop_time = time()
        print('| [%s Completed in %s seconds.]' % (self.action, self.elapsed))
        print('||| [%s Completed in %s seconds.]\n' % (self.process, self.elapsed_total))

    @property
    def elapsed(self) -> str:
        elapsed = str(round(time() - self.last_time, 2))
        return elapsed

    @property
    def elapsed_total(self) -> str:
        elapsed_total = str(round(time() - self.start_time, 2))
        return elapsed_total


def show_progress(progress: pool.MapResult):
    total = progress.__getattribute__('_number_left')
    while not progress.ready():
        remain = progress.__getattribute__('_number_left')
        print("\r%i/%i remains" % (remain, total), end='')
        sleep(1)


class ProgressBar:
    """
    PGB
    """

    def __init__(self, target: int, total_segments: int = 20, major_divisor: int = 4, step_segments: int = 1):
        self._TARGET = target
        self._TOTAL_SEGMENTS = total_segments
        self._STEP_SEGMENTS = step_segments
        self._MAJOR_STEP_SEGMENTS = total_segments / major_divisor
        self._STEP_PROGRESS = target / total_segments * step_segments
        self.timer.target = target
        self._progress = 0
        self._bar_progress = 0
        self._next_major_step_segments = self._MAJOR_STEP_SEGMENTS

    def __setattr__(self, key, value):
        if key == '_progress':
            self.timer.progress = value
        self.__dict__[key] = value

    def start(self, prompt: str) -> None:
        """
        start
        """
        self.timer.start(prompt)

    def _update(self):
        if self._progress == self._TARGET:
            self._refresh_bar()
            self.timer.stop()
        elif self._segments >= self._next_major_step_segments:
            self._refresh_bar()
            self.timer.show_remain_time()
            self._next_major_step_segments += self._MAJOR_STEP_SEGMENTS
        else:
            self._refresh_bar()

    def advance(self) -> None:
        """
        Advance
        """
        self._progress += 1
        while self._progress - self._bar_progress >= self._STEP_PROGRESS:
            self._bar_progress += self._STEP_PROGRESS
            self._update()

    def _refresh_bar(self):
        print('\r' + self._progress_text, end='', flush=True)

    @property
    def _progress_text(self):
        progress_done = self._segments * '*'
        progress_undone = (self._TOTAL_SEGMENTS - self._segments) * ' '
        progress_percentage = str(int((self._segments / self._TOTAL_SEGMENTS * 100)))
        progress_text = '[%s%s]%s%s' % (progress_done, progress_undone, progress_percentage, '%')
        return progress_text

    @property
    def _segments(self):
        segments = int(self._bar_progress / self._STEP_PROGRESS * self._STEP_SEGMENTS)
        return segments


if __name__ == '__main__':
    timer_test = Timer()
    timer_test.start('Testing Timer...', 'First action')
    sleep(0.5)
    print(timer_test.elapsed)
    sleep(0.5)
    timer_test.stop()
    input()
    pgb = ProgressBar(100, total_segments=100, step_segments=4)
    pgb.start('Testing ProgressBar...')
    for i in range(100):
        sleep(0.1)
        pgb.advance()
    input()
