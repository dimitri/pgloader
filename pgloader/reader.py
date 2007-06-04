# -*- coding: ISO-8859-15 -*-
# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader data reader interface and defaults

from tools    import PGLoader_Error, Reject, parse_config_string
from db       import db
from lo       import ifx_clob, ifx_blob

from options import DRY_RUN, VERBOSE, DEBUG, PEDANTIC
from options import TRUNCATE, VACUUM
from options import COUNT, FROM_COUNT, FROM_ID
from options import INPUT_ENCODING, PG_CLIENT_ENCODING
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import NEWLINE_ESCAPES

class DataReader:
    """
    Read some text formatted data, which look like CSV but are not:
     - no quoting support
     - multi-line support is explicit (via 
    """

    def __init__(self, db, filename, table, columns):
        """ init internal variables """
        self.db        = db
        self.filename  = filename
        self.table     = table
        self.columns   = columns

    def readconfig(self, name, config):
        """ read configuration section for common options

        name is configuration section name, conf the ConfigParser object

        specific option reading code is to be found on subclasses
        which implements read data parsing code.

        see textreader.py and csvreader.py
        """
        # optionnal null and empty_string per table parameters
        if config.has_option(name, 'null'):
            self.db.null = parse_config_string(config.get(name, 'null'))
        else:
            self.db.null = NULL

        if config.has_option(name, 'empty_string'):
            self.db.empty_string = parse_config_string(
                config.get(name, 'empty_string'))
        else:
            self.db.empty_string = EMPTY_STRING


        # optionnal field separator
        self.field_sep = FIELD_SEP
        if config.has_option(name, 'field_sep'):
            self.field_sep = config.get(name, 'field_sep')

            if not DRY_RUN:
                if self.db.copy_sep is None:
                    self.db.copy_sep = self.field_sep

        if DEBUG:
            print "null: '%s'" % self.db.null
            print "empty_string: '%s'" %  self.db.empty_string

    def readlines(self):
        """ read data from configured file, and generate (yields) for
        each data line: line, columns and rowid """
        pass
