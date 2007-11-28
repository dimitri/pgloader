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

    def __init__(self, db, reject, filename, input_encoding, table, columns):
        """ init internal variables """
        if DEBUG:
            print 'reader __init__', filename, table, columns
        
        self.db        = db
        self.filename  = filename
        self.input_encoding = input_encoding
        self.table     = table
        self.columns   = columns
        self.reject    = reject

        if self.input_encoding is None:
            if INPUT_ENCODING is not None:
                self.input_encoding = INPUT_ENCODING

    def readconfig(self, name, config):
        """ read configuration section for common options

        name is configuration section name, conf the ConfigParser object

        specific option reading code is to be found on subclasses
        which implements read data parsing code.

        see textreader.py and csvreader.py
        """

        if not DRY_RUN:
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

        # optionnal field separator, could be defined from template
        if 'field_sep' not in self.__dict__:
            self.field_sep = FIELD_SEP
        
        if config.has_option(name, 'field_sep'):
            self.field_sep = config.get(name, 'field_sep')

            if not DRY_RUN:
                if self.db.copy_sep is None:
                    self.db.copy_sep = self.field_sep

        if DEBUG and not DRY_RUN:
            print "reader.readconfig null: '%s'" % self.db.null
            print "reader.readconfig empty_string: '%s'" \
                  %  self.db.empty_string
            print "reader.readconfig field_sep: '%s'" % self.field_sep

    def readlines(self):
        """ read data from configured file, and generate (yields) for
        each data line: line, columns and rowid """
        pass
