# -*- coding: utf8 -*-
###############################################################################
#                              process_control.py                             #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

# Description:
# Library contains helper functions for thread control
import fcntl


class Lock(object):
    """
    Creates if not exist a new file for file locking.
    Needs the name of the file as argument.
    File can be locked and unlocked due the following methods.
    aquire - locks the file and runs the wrapped process in single thread.
    release - releases the file lock so same program can continue with the next process.
    """

    def __init__(self, filename):
        self.filename = filename
        # This will create it if it does not exist already
        self.handle = open(filename, 'w')

    def acquire(self):
        """
        Aquires file lock for the running process.
        """
        fcntl.flock(self.handle, fcntl.LOCK_EX)

    def release(self):
        """
        Releases file lock for the running process.
        """
        fcntl.flock(self.handle, fcntl.LOCK_UN)

    def __del__(self):
        self.handle.close()
