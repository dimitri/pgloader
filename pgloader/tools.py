# Author: Dimitri Fontaine <dimitri@dalibo.com>
#
# pgloader librairies

import os, sys, os.path, time, codecs
from cStringIO import StringIO

from options import DRY_RUN, PEDANTIC

class PGLoader_Error(Exception):
    """ Internal pgloader processing error """
    pass

class Reject:
    """ We log rejects into two files, reject_log and reject_data

    reject_log  contains some error messages and reasons
    reject_data contains input lines which this tool couldn't manage
    """

    def __init__(self, log, reject_log, reject_data):
        """ Constructor, with file names """
        self._log        = log
        self.reject_log  = reject_log
        self.reject_data = reject_data

        # we will open files on first error
        self.errors = 0

    def print_stats(self, name):
        """ give a summary """
        if DRY_RUN:
            return
        
        if self.errors == 0:
            self._log.info("No data were rejected")
        else:
            self._log.error("%d errors found into [%s] data",
                            self.errors, name)
            self._log.error("please read %s for errors log", self.reject_log)
            self._log.error("and %s for data still to process",
                            self.reject_data)

    def log(self, messages, data = None):
        """ log the messages into reject_log, and the data into reject_data

        We open the file on each request, cause we supose errors to be
        rare while the import process will take a long time.
        """

        if self.errors == 0:
            try:
                fd_log  = open(self.reject_log,  'wb+')
                fd_data = open(self.reject_data, 'wb+')
            except IOError, error:
                raise PGLoader_Error, error
        else:
            fd_log  = open(self.reject_log,  'ab+')
            fd_data = open(self.reject_data, 'ab+')

        # message has to be either a string or a list of strings
        if type(messages) == type("string"):
            error = messages + "\n"
            fd_log.write(error)

            if PEDANTIC and not VERBOSE:
                # (write the message just once)
                sys.stderr.write(error)
            
        else:
            error = None
            for m in messages:
                if error is None: error = m
                m += "\n"
                fd_log.write(m)
                
                if PEDANTIC:
                    sys.stderr.write(m)
                    
        # add a separation line between log entries
        fd_log.write("\n")

        # data has to be a string, a single input line or None
        # the input line is not chomped
        if data is not None:
            fd_data.write(data)

        # now we close the two fds
        for f in [fd_log, fd_data]:
            try:
                f.flush()
                f.close()
            except IOError, e:
                raise PGLoader_Error, e

        self.errors += 1

        if PEDANTIC:
            raise PGLoader_Error, error


def parse_config_string(str):
    """ parse a config string

    used for null and empty_string elements
    null = ""
    empty_string = "\ "

    this would result in null =="" and empty_string == '"\ "',
    which is not what we want.
    """

    if len(str) > 2:
        if (str[0] == str[-1] == '"') \
               or (str[0] == str[-1] == "'"):
            # we have a param = "foo" configuration, we want to return only
            # the foo
            return str[1:-1]

    return str

            
    
def read_path(strpath, log, path = [], check = True):
    """ read a path configuration element, discarding non-existing entries """
    import os.path

    for p in strpath.split(':'):
        path.append(p)

    if check:
        return check_path(path, log)
    else:
        return path

def check_path(path, log):
    """ removes non existant and non {directories, symlink} entries from path
    """
    path_ok = []

    for p in path:
        if os.path.exists(p):
            if os.path.isdir(p) or \
                   (os.path.islink(p) and os.path.isdir(os.path.realpath(p))):
                path_ok.append(p)
            else:
                log.warning("path entry '%s' is not a directory " + \
                            "or does not link to a directory", p)
        else:
            log.warning("path entry '%s' does not exists, ignored" % p)

    return path_ok


def check_dirname(path):
    """ check if given path dirname exists, try to create it if if doesn't """

    # try to create the log file and the directory where it lives
    logdir = os.path.dirname(path)
    if logdir and not os.path.exists(logdir):
        # logdir is not empty (not CWD) and does not exists
        try:
            os.makedirs(logdir)
        except (IOError, OSError), e:
            return False, e

    return True, None

    

def running_threads(threads):
    """ count running threads """
    running = 0
    for s in threads:
        if threads[s].isAlive():
            running += 1

    return running
