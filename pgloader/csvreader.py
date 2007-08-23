# Author: Dimitri Fontaine <dimitri@dalibo.com>
#
# pgloader text format reader
#
# handles configuration, parse data, then pass them to database module for
# COPY preparation

import os, sys, os.path, time, codecs, csv
from cStringIO import StringIO

from tools    import PGLoader_Error, Reject, parse_config_string
from db       import db
from lo       import ifx_clob, ifx_blob
from reader   import DataReader

from options import DRY_RUN, VERBOSE, DEBUG, PEDANTIC
from options import TRUNCATE, VACUUM
from options import COUNT, FROM_COUNT, FROM_ID
from options import INPUT_ENCODING, PG_CLIENT_ENCODING
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import NEWLINE_ESCAPES

class CSVReader(DataReader):
    """
    Read some CSV formatted data
    """

    def readconfig(self, name, config):
        """ get this reader module configuration from config file """
        DataReader.readconfig(self, name, config)
        
        # optionnal doublequote: defaults to escaping, not doubling
        self.doublequote = False
        if config.has_option(name, 'doublequote'):
            self.trailing_sep = config.get(name, 'doublequote') == 'True'

        self.escapechar = None
        if config.has_option(name, 'escapechar'):
            self.escapechar = config.get(name, 'escapechar')[0]

        self.quotechar = '"'
        if config.has_option(name, 'quotechar'):
            self.quotechar = config.get(name, 'quotechar')[0]

        self.skipinitialspace = False
        if config.has_option(name, 'skipinitialspace'):
            self.skipinitialspace = config.get(name, 'skipinitialspace') == 'True'


    def readlines(self):
        """ read data from configured file, and generate (yields) for
        each data line: line, columns and rowid """

        # make a dialect, then implement a reader with it
        class pgloader_dialect(csv.Dialect):
            delimiter        = self.field_sep
            doublequote      = self.doublequote
            escapechar       = self.escapechar
            quotechar        = self.quotechar
            skipinitialspace = self.skipinitialspace

            lineterminator   = '\r\n'
            quoting          = csv.QUOTE_MINIMAL
            
        csv.register_dialect('pgloader', pgloader_dialect)

        if INPUT_ENCODING is not None:
            try:
                fd = codecs.open(self.filename, encoding = INPUT_ENCODING)
            except LookupError, e:
                # codec not found
                raise PGLoader_Error, "Input codec: %s" % e
        else:
            try:
                fd = open(self.filename, "rb")
            except IOError, error:
                raise PGLoader_Error, error

        # now read the lines
        for columns in csv.reader(fd, dialect = 'pgloader'):
            line = self.field_sep.join(columns)
            yield line, columns
