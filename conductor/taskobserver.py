"""
Observers for running tasks
"""

import logging

logger = logging.getLogger(__name__)

class ConsoleObserver(object):

    def __init__(self, processing_task):
        self.processing_task = processing_task
        self.progress = 0
        self.state = ""
        self.details = ""

    def __call__(self):
        msg = ""
        if self.processing_task.run_progress != self.progress:
            self.progress = self.processing_task.run_progress
            msg += " run_progress: {0.run_progress:03d}%"
        if self.processing_task.run_state != self.state:
            self.state = self.processing_task.run_state
            msg += " run_state: {0.run_state}"
        if self.processing_task.run_details != self.details:
            self.details = self.processing_task.run_details
            msg += " run_details: {0.run_details}"
        if msg != "":
            msg = ("{0.name}" + msg).format(self.processing_task)
            print(msg)
