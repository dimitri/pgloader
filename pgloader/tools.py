# -*- coding: ISO-8859-15 -*-
# Author: Dimitri Fontaine <dimitri@dalibo.com>
#
# pgloader librairies

import os, sys, os.path, time, codecs
from cStringIO import StringIO

from options import DRY_RUN, VERBOSE, DEBUG, PEDANTIC

class PGLoader_Error(Exception):
    """ Internal pgloader processing error """
    pass

class Reject:
    """ We log rejects into two files, reject_log and reject_data

    reject_log  contains some error messages and reasons
    reject_data contains input lines which this tool couldn't manage
    """

    def __init__(self, reject_log, reject_data):
        """ Constructor, with file names """
        self.reject_log  = reject_log
        self.reject_data = reject_data

        # we will open files on first error
        self.errors = 0

    def print_stats(self):
        """ give a summary """
        if DRY_RUN:
            return
        
        if self.errors == 0:
            print "## No data were rejected"
        else:
            print "## %d errors found into data" % self.errors
            print "   please read %s for errors log" % self.reject_log
            print "   and %s for data still to process" % self.reject_data

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

            
    
