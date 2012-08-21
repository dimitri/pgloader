# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader text format reader
#
# handles configuration, parse data, then pass them to database module for
# COPY preparation

import os, sys, os.path, time
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

class TextReader(DataReader):
    """
    Read some text formatted data, which look like CSV but are not:
     - no quoting support
     - trailing separator trailing support
     - multi-line support is explicit (via field_count parameter)
     - newline escaping in multi-line content support
     - ...
    """

    def __init__(self, log, db, reject, filename, input_encoding,
                 table, columns, newline_escapes = None):
        """ init textreader with a newline_escapes parameter """
        DataReader.__init__(self, log, db, reject,
                            filename, input_encoding, table, columns)

        if 'newline_escapes' not in self.__dict__:
            self.newline_escapes = newline_escapes

        self.log.debug('reader.__init__: newline_escapes %s' \
                       % self.newline_escapes)

    def readconfig(self, config, name, template):
        """ get this reader module configuration from config file """
        DataReader.readconfig(self, config, name, template)

        # this will be called twice if templates are in used, so we
        # have to protect ourselves against removing already read
        # configurations while in second run.

        self._getopt('field_count', config, name, template, None, 'int')
        self._getopt('trailing_sep', config, name, template, False)
        if self.trailing_sep is not False:
            self.trailing_sep = self.trailing_sep == 'True'

        self.log.debug('reader.readconfig: field_count %s', self.field_count)
        self.log.debug('reader.readconfig: trailing_sep %s', self.trailing_sep)

    def readlines(self):
        """ read data from configured file, and generate (yields) for
        each data line: line, columns and rowid """

        # temporary feature for controlling when to begin real inserts
        # if first time launch, set to True.
        input_buffer = StringIO()
        nb_lines     = 0
        begin_linenb = None
        nb_plines    = 0

        tmp_offsets  = []
        offsets      = []

        ##
        # if neither -I nor -F was used, we can state that begin = 0
        if FROM_ID is None and self.skip_head_lines == 0:
            self.log.debug('beginning on first line')
            begin_linenb = 1

        self.fd = UnbufferedFileReader(self.filename, self.log,
                                       encoding        = self.input_encoding,
                                       start           = self.start,
                                       end             = self.end,
                                       skip_head_lines = self.skip_head_lines,
                                       check_count     = False,
                                       client_encoding = self.client_encoding)
        
        for line in self.fd:
            # we count real physical lines
            nb_plines += 1

            # and we store them in offsets for error messages
            tmp_offsets.append(nb_plines)
            self.log.debug('current offset %s' % tmp_offsets)

            if self.input_encoding is not None:
                # this may not be necessary, after all
                try:
                    line = line.encode(self.input_encoding)
                except UnicodeDecodeError, e:
                    reject.log(['Codec error', str(e)], input_line)
                    continue
                
            if self.field_count is not None:
                input_buffer.write(line)
                # act as if this were the last input_buffer for this line
                tmp     = self._chomp(input_buffer.getvalue())
                columns = self._split_line(tmp)
                nb_cols = len(columns)

                # check we got them all if not and field_count was
                # given, we have a multi-line input
                if nb_cols < self.field_count:
                    continue
                else:
                    # we have read all the logical line
                    line = tmp
                    input_buffer.close()
                    input_buffer = StringIO()

                    if nb_cols != self.field_count:
                        self.log.debug(line)
                        self.log.debug(str(columns))
                        self.reject.log(
                            'Error parsing columns on line ' +\
                            '%d [row %d]: found %d columns' \
                            % (nb_plines, nb_lines, nb_cols), line)
            else:
                # normal operation mode : one physical line is one
                # logical line. we didn't split input line yet
                line    = self._chomp(line)
                nb_cols = None
                columns = None

            if len(line) == 0:
                # skip empty lines
                continue

            # we count logical lines
            nb_lines  += 1
            offsets    = tmp_offsets[:]
            tmp_offsets = []

            if self.start:
                offsets = (self.start, offsets)

            ##
            # check for beginning if option -I was used
            if FROM_ID is not None:
                if columns is None:
                    columns = self._split_line(line)
                    
                rowids = self._rowids(columns)
                                      
                if FROM_ID == rowids:
                    begin_linenb = nb_lines
                    self.log.debug('reached beginning on line %d', nb_lines)

                elif begin_linenb is None:
                    # begin is set to 1 when we don't use neither -I nor -F
                    continue

            if COUNT is not None and self.fd.reading \
               and (nb_lines - begin_linenb + 1) > COUNT:
                
                self.log.info('reached line %d, stopping', nb_lines)
                break

            if columns is None:
                columns = self._split_line(line)

            self.log.debug('read data')

            # now, we may have to apply newline_escapes on configured columns
            if NEWLINE_ESCAPES or self.newline_escapes != []:
                columns = self._escape_newlines(columns)

            nb_cols = len(columns)
            if nb_cols != len(self.columns):
                self.log.debug(line)
                self.log.debug(str(columns))

                msg = 'Error parsing columns on line ' +\
                      '%d [row %d]: found %d columns' \
                      % (nb_plines, nb_lines, nb_cols)

                self.reject.log(msg, line)
                continue

            yield offsets, line, columns

    def _split_line(self, line):
        """ split given line and returns a columns list """
        last_sep = 0
        columns  = []
        pos      = line.find(self.field_sep, last_sep)
        
        while pos != -1:
            # don't consider backslash escaped separators melted into data
            # warning, we may find some data|\\|data
            # that is some escaped \ then a legal | separator
            i=1
            while pos-i >= 0 and line[pos-i] == '\\':
                i += 1

            # now i-1 is the number of \ preceding the separator
            # it's a legal separator only if i-1 is even
            if (i-1) % 2 == 0:
                # there's no need to keep escaped delimiters inside a column
                # and we want to avoid double-escaping them
                columns.append(line[last_sep:pos]
                               .replace("\\%s" % self.field_sep,
                                        self.field_sep))
                last_sep = pos + 1

            pos = line.find(self.field_sep, pos + 1)

        # append last column
        columns.append(line[last_sep:])
        return columns


    def _chomp(self, input_line):
        """ chomp end of line when necessary, and trailing_sep too """

        if len(input_line) == 0:
            self.log.debug('pgloader._chomp: skipping empty line')
            return input_line
        
        # chomp a copy of the input_line, we will need the original one
        line = input_line[:]

        if line[-2:] == "\r\n":
            line = line[:-2]

        elif line[-1] == "\r":
            line = line[:-1]

        elif line[-1] == "\n":
            line = line[:-1]

        # trailing separator to whipe out ?
        if self.trailing_sep \
            and line[-len(self.field_sep)] == self.field_sep:
            
            line = line[:-len(self.field_sep)]

        return line

    def _escape_newlines(self, columns):
        """ trim out newline escapes to be found inside data columns """
        self.log.debug('escaping columns newlines')
        self.log.debug(self.newline_escapes)

        for (ne_col, ne_esc) in self.newline_escapes:
            # don't forget configured col references use counting from 1
            ne_colnum = dict(self.columns)[ne_col] - 1
            self.log.debug('column %s[%d] escaped with %s',
                          ne_col, ne_colnum+1, ne_esc)

            col_data = columns[ne_colnum]
            
            if self.db and \
               (self.db.is_null(col_data) or self.db.is_empty(col_data)):
                self.log.debug('skipping null or empty column')
                continue

            escaped   = []
            tmp       = col_data

            for line in tmp.split('\n'):
                if len(line) == 0:
                    self.log.debug('skipping empty line')
                    continue

                self.log.debug('chomping: %s', line)
                    
                tmpline = self._chomp(line)
                if tmpline[-1] == ne_esc:
                    tmpline = tmpline[:-1]

                    # chomp out only escaping char, not newline itself
                    escaped.append(line[:len(tmpline)] + \
                                   line[len(tmpline)+1:])

                else:
                    # line does not end with escaping char, keep it
                    escaped.append(line)

            columns[ne_colnum] = '\n'.join(escaped)
        return columns

