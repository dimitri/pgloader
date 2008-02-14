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

from options import DRY_RUN, PEDANTIC
from options import TRUNCATE, VACUUM
from options import COUNT, FROM_COUNT, FROM_ID
from options import INPUT_ENCODING, PG_CLIENT_ENCODING
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import NEWLINE_ESCAPES

class CSVReader(DataReader):
    """
    Read some CSV formatted data
    """

    def readconfig(self, config, name, template):
        """ get this reader module configuration from config file """
        DataReader.readconfig(self, config, name, template)

        self._getopt('doublequote', config, name, template, False)
        if self.doublequote is not False:
            self.doublequote = self.doublequote == 'True'
        
        self._getopt('escapechar', config, name, template, None)
        if self.escapechar is not None:
            self.escapechar = self.escapechar[0]

        self._getopt('quotechar', config, name, template, '"')
        self.quotechar = self.quotechar[0]

        self._getopt('skipinitialspace', config, name, template, False)
        if self.skipinitialspace is not False:
            self.skipinitialspace = self.skipinitialspace == 'True'

        for opt in ['doublequote', 'escapechar',
                    'quotechar', 'skipinitialspace']:
            
            self.log.debug("reader.readconfig %s: '%s'" % (opt, self.__dict__[opt]))

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

        if self.input_encoding is not None:
            try:
                fd = codecs.open(self.filename, encoding = self.input_encoding)
            except LookupError, e:
                # codec not found
                raise PGLoader_Error, "Input codec: %s" % e
        else:
            try:
                fd = open(self.filename, "rb")
            except IOError, error:
                raise PGLoader_Error, error

        if self.start is not None and self.start > 0:
            self.log.info("CSV Reader starting at offset %d" % self.start)
            fd.seek(self.start)

        # now read the lines
        for columns in csv.reader(fd, dialect = 'pgloader'):
            line = self.field_sep.join(columns)
            yield line, columns

            if self.end is not None and fd.tell() >= self.end:
                self.log.info("CSV Reader stoping, offset %d >= %d" % (fd.tell(), self.end))
                fd.close()
                return
            
