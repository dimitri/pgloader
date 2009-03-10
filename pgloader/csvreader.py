# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader text format reader
#
# handles configuration, parse data, then pass them to database module for
# COPY preparation

import os, sys, os.path, time, csv
from cStringIO import StringIO

from tools    import PGLoader_Error, Reject, parse_config_string
from db       import db
from lo       import ifx_clob, ifx_blob
from reader   import DataReader, UnbufferedFileReader

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

        self._getopt('doublequote', config, name, template, True)
        if self.doublequote is not True:
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
            
            self.log.debug("reader.readconfig %s: '%s'" \
                           % (opt, self.__dict__[opt]))

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

        self.fd = UnbufferedFileReader(self.filename, self.log,
                                       encoding = self.input_encoding,
                                       start    = self.start,
                                       end      = self.end,
                                       skip_head_lines = self.skip_head_lines)
        
        # don't forget COUNT and FROM_COUNT option in CSV mode
        nb_lines     = self.skip_head_lines
        begin_linenb = None
        last_line_nb = 1

        # now read the lines
        for columns in csv.reader(self.fd, dialect = 'pgloader'):
            # we count logical lines
            nb_lines += 1

            line         = self.field_sep.join(columns)
            offsets      = range(last_line_nb, self.fd.line_nb)
            last_line_nb = self.fd.line_nb
            
            if self.start:
                offsets = (self.start, offsets)

            yield offsets, line, columns
            
        return
