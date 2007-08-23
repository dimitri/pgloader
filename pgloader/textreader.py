# Author: Dimitri Fontaine <dimitri@dalibo.com>
#
# pgloader text format reader
#
# handles configuration, parse data, then pass them to database module for
# COPY preparation

import os, sys, os.path, time, codecs
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

class TextReader(DataReader):
    """
    Read some text formatted data, which look like CSV but are not:
     - no quoting support
     - trailing separator trailing support
     - multi-line support is explicit (via field_count parameter)
     - newline escaping in multi-line content support
     - ...
    """

    def readconfig(self, name, config):
        """ get this reader module configuration from config file """
        DataReader.readconfig(self, name, config)
        
        # optionnal number of columns per line
        self.field_count = None
        if config.has_option(name, 'field_count'):
            self.field_count = config.getint(name, 'field_count')

        # optionnal trailing separator option
        self.trailing_sep = False
        if config.has_option(name, 'trailing_sep'):
            self.trailing_sep = config.get(name, 'trailing_sep') == 'True'

        # optionnal newline escaped option
        self.newline_escapes = []
        if config.has_option(name, 'newline_escapes'):
            if NEWLINE_ESCAPES is not None:
                # this parameter is globally set, will ignore local
                # definition
                print "Warning: ignoring %s newline_escapes option" % name
                print "         option is set to '%s' globally" \
                      % NEWLINE_ESCAPES
            else:
                self._parse_fields('newline_escapes',
                                   config.get(name, 'newline_escapes'),
                                   argtype = 'char')

        if NEWLINE_ESCAPES is not None:
            # set NEWLINE_ESCAPES for each table column
            self.newline_escapes = [(a, NEWLINE_ESCAPES)
                                    for (a, x) in self.columns]

        

    def readlines(self):
        """ read data from configured file, and generate (yields) for
        each data line: line, columns and rowid """

        # temporary feature for controlling when to begin real inserts
        # if first time launch, set to True.
        input_buffer = StringIO()
        nb_lines     = 0
        begin_linenb = None
        nb_plines    = 0

        ##
        # if neither -I nor -F was used, we can state that begin = 0
        if FROM_ID is None and FROM_COUNT == 0:
            if VERBOSE:
                print 'Notice: beginning on first line'
            begin_linenb = 1

        if INPUT_ENCODING is not None:
            try:
                fd = codecs.open(self.filename, encoding = INPUT_ENCODING)
            except LookupError, e:
                # codec not found
                raise PGLoader_Error, "Input codec: %s" % e
        else:
            try:
                fd = open(self.filename)
            except IOError, error:
                raise PGLoader_Error, error

        for line in fd:
            # we count real physical lines
            nb_plines += 1

            if INPUT_ENCODING is not None:
                # this may not be necessary, after all
                try:
                    line = line.encode(INPUT_ENCODING)
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
                        if DEBUG:
                            print line
                            print columns
                            print
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
            nb_lines += 1

            ##
            # if -F is used, count lines to skip, and skip them
            if FROM_COUNT > 0:
                if nb_lines < FROM_COUNT:
                    continue

                if nb_lines == FROM_COUNT:
                    begin_linenb = nb_lines
                    if VERBOSE:
                        print 'Notice: reached beginning on line %d' % nb_lines

            ##
            # check for beginning if option -I was used
            if FROM_ID is not None:
                if columns is None:
                    columns = self._split_line(line)
                    
                rowids = self._rowids(columns)
                                      
                if FROM_ID == rowids:
                    begin_linenb = nb_lines
                    if VERBOSE:
                        print 'Notice: reached beginning on line %d' % nb_lines

                elif begin_linenb is None:
                    # begin is set to 1 when we don't use neither -I nor -F
                    continue

            if COUNT is not None and begin_linenb is not None \
               and (nb_lines - begin_linenb + 1) > COUNT:
                
                if VERBOSE:
                    print 'Notice: reached line %d, stopping' % nb_lines
                break

            if columns is None:
                columns = self._split_line(line)

            if DEBUG:
                print 'Debug: read data'

            # now, we may have to apply newline_escapes on configured columns
            if NEWLINE_ESCAPES or self.newline_escapes != []:
                columns = self._escape_newlines(columns)

            nb_cols = len(columns)
            if nb_cols != len(self.columns):
                if DEBUG:
                    print line
                    print columns
                    print

                msg = 'Error parsing columns on line ' +\
                      '%d [row %d]: found %d columns' \
                      % (nb_plines, nb_lines, nb_cols)

                self.reject.log(msg, line)
                continue

            yield line, columns


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
            if DEBUG:
                print 'pgloader._chomp: skipping empty line'
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
        if DEBUG:
            print 'Debug: escaping columns newlines'
            print 'Debug:', self.newline_escapes

        for (ne_col, ne_esc) in self.newline_escapes:
            # don't forget configured col references use counting from 1
            ne_colnum = dict(self.columns)[ne_col] - 1
            if DEBUG:
                print 'Debug: column %s[%d] escaped with %s' \
                      % (ne_col, ne_colnum+1, ne_esc)

            col_data = columns[ne_colnum]
            
            if self.db.is_null(col_data) or self.db.is_empty(col_data):
                if DEBUG:
                    print 'Debug: skipping null or empty column'
                continue

            escaped   = []
            tmp       = col_data

            for line in tmp.split('\n'):
                if len(line) == 0:
                    if DEBUG:
                        print 'Debug: skipping empty line'
                    continue

                if DEBUG:
                    print 'Debug: chomping:', line
                    
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

