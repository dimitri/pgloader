# Author: Dimitri Fontaine <dim@tapoueh.org>
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
        self.lock        = None

        # we will open files on first error
        self.errors = 0

    def set_lock(self, lock):
        """ when used in a multi-threaded way, you want a common
        reject facility, protected from concurrent writes """
        self.lock = lock

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
        """ Acquire the lock if needed, do_log() and check for IOError """

        if self.lock:
            self._log.debug("Reject acquire")
            self.lock.acquire()

        try:
            self.do_log(messages, data)

        except IOError, e:
            raise PGLoader_Error, e

        except PGLoader_Error, e:
            raise 

        if self.lock:
            self._log.debug("Reject release")
            self.lock.release()

    def do_log(self, messages, data = None):
        """ log the messages into reject_log, and the data into reject_data

        We open the file on each request, cause we supose errors to be
        rare while the import process will take a long time.
        """

        if self.errors == 0:
            fd_log  = open(self.reject_log,  'wb+')
            fd_data = open(self.reject_data, 'wb+')
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
            f.flush()
            f.close()

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


def parse_pg_options(log, config, section, pg_options, overwrite=False):
    """ Get all the pg_options_ prefixed options from the section"""
    # PostgreSQL options must begin with the prefix pg_option_
    for o in [x for x in config.options(section) 
              if x.startswith('pg_option_')]:
        opt = o[len('pg_option_'):]
        val = config.get(section, o)

        # hysterical raisins
        for compat in ['client_encoding', 'lc_messages', 'datestyle']:
            if opt == compat and config.has_option(section, compat):
                log.warning("Ignoring %s.%s for %s.%s" \
                                % (section, o, section, opt))

        if opt not in compat and (overwrite or opt not in pg_options):
            pg_options[opt] = val

    return pg_options
    
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


def check_events(events, log, context = "is running"):
    """ wait until all events (list) are set """
    for t in events:
        events[t].wait()
        log.debug("thread %s %s" % (t, context))
    
    return
