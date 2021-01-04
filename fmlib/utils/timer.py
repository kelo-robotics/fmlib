"""
Taken from: https://stackoverflow.com/a/48741004
"""
import threading

class RepeatTimer(threading.Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)