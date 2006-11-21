# -*- coding: ISO-8859-15 -*-
# Author: Dimitri Fontaine <dimitri@dalibo.com>
#
# pgloader main class
#
# handles configuration, parse data, then pass them to database module for
# COPY preparation

import os, sys, os.path, time, codecs
from cStringIO import StringIO

from tools    import PGLoader_Error, Reject, parse_config_string
from db       import db
from lo       import ifx_clob, ifx_blob

from options import DRY_RUN, VERBOSE, DEBUG, PEDANTIC
from options import TRUNCATE, VACUUM
from options import COUNT, FROM_COUNT, FROM_ID
from options import INPUT_ENCODING, PG_CLIENT_ENCODING
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import NEWLINE_ESCAPES

class PGLoader:
    """
    PGLoader reads some data file and depending on ts configuration,
    import data with COPY or update blob data with UPDATE.
    """

    def __init__(self, name, config, db):
        """ Init with a configuration section """
        # Some settings
        self.name = name
        self.db   = db

        self.index     = None
        self.columns   = None
        self.blob_cols = None

        self.config_errors = 0
        self.errors        = 0
        self.updates       = 0
        self.init_time     = time.time()

        # we may have to open several clob files while parsing the
        # unload data file, hence we keep track of them all
        self.blobs = {}

        if VERBOSE:
            print
            print "[%s] parse configuration" % self.name
        
        # some configuration elements don't have default value
        for opt in ('table', 'filename'):
            if  config.has_option(name, opt):
                self.__dict__[opt] = config.get(name, opt)
            else:
                print 'Error: please configure %s.%s' % (name, opt)
                self.config_errors += 1

        ##
        # reject log and data files defaults to /tmp/<section>.rej[.log]
        if config.has_option(name, 'reject_log'):
            self.reject_log = config.get(name, 'reject_log')
        else:
            self.reject_log = os.path.join('/tmp', '%s.rej.log' % name)
            if VERBOSE:
                print 'Notice: reject log in %s' % self.reject_log
            
        if config.has_option(name, 'reject_data'):
            self.reject_data = config.get(name, 'reject_data')
        else:
            self.reject_data = os.path.join('/tmp', '%s.rej' % name)
            if VERBOSE:
                print 'Notice: rejected data in %s' % self.reject_data

        # reject logging
        self.reject = Reject(self.reject_log, self.reject_data)
        

        # optionnal number of columns per line
        self.field_count = None
        if config.has_option(name, 'field_count'):
            self.field_count = config.getint(name, 'field_count')

        # optionnal field separator
        self.field_sep = FIELD_SEP
        if config.has_option(name, 'field_sep'):
            self.field_sep = config.get(name, 'field_sep')

            if not DRY_RUN:
                if self.db.copy_sep is None:
                    self.db.copy_sep = self.field_sep

        # optionnal trailing separator option
        self.trailing_sep = False
        if config.has_option(name, 'trailing_sep'):
            self.trailing_sep = config.get(name, 'trailing_sep') == 'True'

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

        # optionnal local option client_encoding
        if config.has_option(name, 'client_encoding'):
            self.db.client_encoding = parse_config_string(
                config.get(name, 'client_encoding'))

        if DEBUG:
            print "null: '%s'" % self.db.null
            print "empty_string: '%s'" %  self.db.empty_string
            print "client_encoding: '%s'" % self.db.client_encoding

        ##
        # we parse some columns definitions
        if config.has_option(name, 'index'):
            self._parse_fields('index', config.get(name, 'index'))

        if config.has_option(name, 'columns'):
            self._parse_fields('columns', config.get(name, 'columns'))

        if config.has_option(name, 'blob_columns'):
            self._parse_fields('blob_cols',
                               config.get(name, 'blob_columns'),
                               btype = True)

        if DEBUG:
            print 'index', self.index
            print 'columns', self.columns
            print 'blob_columns', self.blob_cols

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

        ##
        # How can we mix those columns definitions ?
        #  - we want to load table data with COPY
        #  - we want to load blob data into text or bytea field, with COPY
        #  - we want to do both
        
        if self.columns is None and self.blob_cols is None:
            # nothing to do ? perfect, done
            self.reject.log(
                "Error: in section '%s': " % self.name +\
                "please configure some work to do (columns, blob_cols)")
            self.config_errors += 1

        if self.blob_cols is not None and self.index is None:
            # if you want to load blobs, UPDATE need indexes
            # is this error still necessary?
            self.reject.log(
                "Error: in section '%s': " % self.name +\
                "for blob importing (blob_cols), please configure index")
            self.config_errors += 1

        ##
        # We have for example columns = col1:2, col2:1
        # this means the order of input columns is not the same as the
        # awaited order of COPY, so we want a mapping index, here [2, 1]
        if self.columns is not None:
            self.col_mapping = [i for (c, i) in self.columns]

        ##
        # if options.fromid is not None it has to be either a value,
        # when index is single key or a dict in a string, when index
        # is a multiple key
        global FROM_ID
        if FROM_ID is not None:
            if len(self.index) > 1:
                # we have to evaluate given string and see if it is a
                # dictionnary
                try:
                    d = {}
                    ids  = [x.strip() for x in FROM_ID.split(',')]
                    for id in ids:
                        k, v = id.split(':')
                        d[k] = v

                    FROM_ID = d
                except Exception, e:
                    self.reject.log(
                        'Error: unable to parse given key %s' % FROM_ID)
                    raise PGLoader_Error

            print 'Notice: composite key found, -I evaluated to %s' % FROM_ID

        if self.config_errors > 0:
            mesg = ['Configuration errors for section %s' % self.name,
                    'Please see reject log file %s' % self.reject_log]
            raise PGLoader_Error, '\n'.join(mesg)

        # Now reset database connection
        if not DRY_RUN:
            self.db.reset()

    def _parse_fields(self, attr, str, btype = False, argtype = 'int'):
        """ parse the user string str for fields definition to store
        into self.attr """

        def __getarg(arg, argtype):
            """ return arg depending on its type """
            if argtype == 'int':
                # arg is the target column index
                try:
                    arg = int(arg)
                except ValueError:
                    raise PGLoader_Error
                        
            elif argtype == 'char':
                # arg is an escape char
                if len(arg) > 1:
                    raise PGLoader_Error

            return arg

        f = self.__dict__[attr] = []

        try:
            for field_def in str.split(','):
                properties = [x.strip() for x in field_def.split(':')]

                if not btype:
                    # normal column definition, for COPY usage
                    colname, arg = properties
                    f.append((colname, __getarg(arg, argtype)))

                else:
                    # blob column definition, with blob type, for
                    # UPDATE usage
                    colname, arg, btype = properties
                    f.append((colname, __getarg(arg, argtype), btype))
                    
        except Exception, error:
            # FIXME: make some errors and write some error messages
            raise

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

    def _rowids(self, columns):
        """ get rowids for given input line """
        rowids = {}
        try:
            for id_name, id_col in self.index:
                rowids[id_name] = columns[id_col - 1]

        except IndexError, e:
            messages = [
                "Warning: couldn't get id %d on column #%d" % (id_name,
                                                               id_col), 
                str(e)
                ]
                
            self.reject.log(messages, line)

        return rowids

    def summary(self):
        """ return a (duration, updates, errors) tuple """
        self.duration = time.time() - self.init_time
        
        if self.reject is not None:
            self.errors = self.reject.errors

        if self.db is not None:
            self.updates = self.db.commited_rows

        return (self.duration, self.updates, self.errors)

    def print_stats(self):
        """ print out some statistics """

        if self.reject is not None:
            self.errors = self.reject.errors
            self.reject.print_stats()

        if self.db is not None:
            self.updates = self.db.commited_rows
            self.db.print_stats()
        return

    def run(self):
        """ depending on configuration, do given job """

        # Announce the beginning of the work
        print "[%s] data import" % self.name

        if TRUNCATE and not DRY_RUN:
            self.db.truncate(self.table)

        if self.columns is not None:
            print "Notice: COPY csv data"
            self.csv_import()

        elif self.blob_cols is not None:
            # elif: COPY process also blob data
            print "Notice: UPDATE blob data"

        # then show up some stats
        self.print_stats()

    def csv_import(self):
        """ import CSV data, using COPY """

        for line, columns in self.read_data():
            if self.blob_cols is not None:
                columns, rowids = self.read_blob(line, columns)

            if DEBUG:
                print self.col_mapping
                print len(columns), len(self.col_mapping)

            if False and VERBOSE:
                print line
                for i in [37, 44, 52, 38]:
                    print len(columns[i-1]), columns[i-1]
                print

            ##
            # Now we have to reorder the columns to match schema
            c_ordered = [columns[i-1] for i in self.col_mapping]

            if DRY_RUN or DEBUG:
                print line
                print c_ordered
                print len(c_ordered)
                print
                    
            if not DRY_RUN:
                self.db.copy_from(self.table, c_ordered, line, self.reject)

        if not DRY_RUN:
            # we may need a last COPY for the rest of data
            self.db.copy_from(self.table, None, None, self.reject, EOF = True)

        return

    def lo_import(self):
        """ import large object data, using UPDATEs """

        for line, columns in self.read_data():
            ##
            # read blob data and replace its PG escaped form into columns
            columns, rowids = self.read_blob(line, columns)
            
            # and insert it into database
            if DRY_RUN or DEBUG:
                print line
                print columns
                print

            if not DRY_RUN:
                self.db.insert_blob(self.table, self.index,
                                    rowids, cname, data, btype,
                                    line, self.reject)

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

    def read_data(self):
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


    def read_blob(self, line, columns):
        """ read blob data and replace columns values with PG escape
        data to give as input to COPY or UPDATE """

        # cache rowids, a single line ids will be the same whatever
        # the number of clob entries are configured
        rowids = None

        for (cname, c, btype) in self.blob_cols:
            try:
                blob_col = c-1
                blob = columns[blob_col].strip()
            except Exception, e:
                m = [
                    "Warning: couldn't get '%s' column (#%d)" % (cname, c),
                    str(e)
                    ]
                try:
                    self.reject.log(m, line)
                except PGLoader_Error:
                    # pedantic setting, but we want to continue
                    continue

            if blob != "":
                try:
                    (begin, length, blobname) = blob.split(CLOB_SEP)

                except Exception, e:
                    m = ["Warning: column '%s'" % cname + \
                         " is not a valid blob reference: %s" % blob,
                         str(e)
                         ]
                    self.reject.log(m, line)
                    continue

                # Informix sometimes output 0,0,0 as blob reference
                if begin == length == blobname == "0":
                    columns[blob_col] = ''
                    continue

                abs_blobname = os.path.join(os.path.dirname(self.filename),
                                            blobname)

                # We want to manage existing blob file
                if not os.access(abs_blobname, os.R_OK):
                     self.reject.log(
                         "Warning: Can't read blob file %s" % abs_blobname,
                         line)

                if abs_blobname not in self.blobs:

                    if btype not in ['ifx_blob', 'ifx_clob']:
                        msg = "Error: Blob type '%s' not supported" % mode
                        raise PGLoader_Error, msg

                    elif btype == 'ifx_blob':
                        self.blobs[abs_blobname] = ifx_blob(abs_blobname, 
                                                            self.field_sep)

                    elif btype == 'ifx_clob':
                        self.blobs[abs_blobname] = ifx_clob(abs_blobname)

                blob = self.blobs[abs_blobname]

                # we now need to get row id(s) from ordered columns
                if rowids is None:
                    rowids = self._rowids(columns)

                # get data from blob object
                data = blob.extract(rowids, c, begin, length)
                columns[blob_col] = data

                if DEBUG:
                    print 'Debug: read blob data'
                    print rowids
                    print data
                    print

        # no we have read all defined blob data, returns new columns
        return columns, rowids
                                           
                    
