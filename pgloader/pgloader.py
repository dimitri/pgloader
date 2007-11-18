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

from options import DRY_RUN, VERBOSE, DEBUG, QUIET, PEDANTIC
from options import TRUNCATE, VACUUM
from options import COUNT, FROM_COUNT, FROM_ID
from options import INPUT_ENCODING, PG_CLIENT_ENCODING
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import NEWLINE_ESCAPES
from options import UDC_PREFIX

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

        # optionnal local option client_encoding
        if config.has_option(name, 'client_encoding'):
            self.db.client_encoding = parse_config_string(
                config.get(name, 'client_encoding'))

        if DEBUG and not DRY_RUN:
            print "client_encoding: '%s'" % self.db.client_encoding

        # optionnal local option input_encoding
        self.input_encoding = None
        if config.has_option(name, 'input_encoding'):
            self.input_encoding = parse_config_string(
                config.get(name, 'input_encoding'))

        if DEBUG:
            print "input_encoding: '%s'" % self.input_encoding

        # optionnal local option datestyle
        if config.has_option(name, 'datestyle'):
            self.db.datestyle = config.get(name, 'datestyle')

        if DEBUG and not DRY_RUN:
            print "datestyle: '%s'" % self.db.datestyle


        ##
        # data filename
        for opt in ('table', 'filename'):
            if  config.has_option(name, opt):
                self.__dict__[opt] = config.get(name, opt)
            else:
                print 'Error: please configure %s.%s' % (name, opt)
                self.config_errors += 1

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


        ##
        # The config section can also provide user-defined colums
        # which are option beginning with options.UDC_PREFIX
        udcs = [o
                for o in config.options(name)
                if o[:len(UDC_PREFIX)] == UDC_PREFIX]
        
        if len(udcs) > 0:
            self.udcs = []
            for udc in udcs:
                udc_name  = udc[:]
                udc_name  = udc_name[udc_name.find('_')+1:]
                udc_value = config.get(name, udc)

                self.udcs.append((udc_name, udc_value))
        else:
            self.udcs = None

        if DEBUG:
            print 'udcs:', self.udcs

        # better check there's no user defined column overriding file
        # columns
        if self.udcs:
            errs = []
            cols = [c for (c, cn) in self.columns]
            for (udc_name, udc_value) in self.udcs:
                if udc_name in cols:
                    errs.append(udc_name)

            if errs:
                for c in errs:
                    print 'Error: %s is configured both as a ' % c +\
                          '%s.columns entry and as a user-defined column' \
                          % name

                self.config_errors += 1

        # we need the copy_columns parameter if user-defined columns
        # are used
        if self.udcs:
            if config.has_option(name, 'copy_columns'):
                namelist = [n for (n, c) in self.columns] + \
                           [n for (n, v) in self.udcs]

                copy_columns = config.get(name, 'copy_columns').split(',')
                self.copy_columns = [x.strip()
                                     for x in copy_columns
                                     if x.strip() in namelist]

                if len(self.copy_columns) != len(copy_columns):
                    print 'Error: %s.copy_columns refers to ' % name +\
                          'unconfigured columns '

                    self.config_errors += 1

            else:
                print 'Error: section %s does not define ' % name +\
                      'copy_columns but uses user-defined columns'

                self.config_errors += 1

        # in the copy_columns case, columnlist is that simple:
        self.columnlist = None
        if self.udcs:
            if self.copy_columns:
                self.columnlist = self.copy_columns

        if DEBUG:
            print 'udcs', self.udcs
            if self.udcs:
                print 'copy_columns', self.copy_columns

        ##
        # We have for example columns = col1:2, col2:1
        # this means the order of input columns is not the same as the
        # awaited order of COPY, so we want a mapping index, here [2, 1]
        #
        # The column mapping is to be done on all_columns, which
        # allows user to have their user-defined columns talken into
        # account in the COPY ordering.
        
        self.col_mapping = [i for (c, i) in self.columns]

        if self.col_mapping == range(1, len(self.columns)+1):
            # no mapping to do
            self.col_mapping = None

        ##
        # optionnal partial loading option (sequences case)
        #
        # self.columnlist is the column list to give to
        # COPY table(...) command, either the cols given in
        # the only_cols config, or the columns directly
        
        self.only_cols = None

        if config.has_option(name, 'only_cols'):
            if self.udcs:
                print 'Error: section %s defines both ' % name  +\
                      'user-defined columns and only_cols'

                self.config_errors += 1
            
            self.only_cols = config.get(name, 'only_cols')

            ##
            # first make an index list out of configuration
            # which contains coma separated ranges or values
            # as for example: only_cols = 1-3, 5
            try:
                only_cols = [x.strip() for x in self.only_cols.split(",")]
                expanded  = []

                # expand ranges
                for oc in only_cols:
                    if '-' in oc:
                        (a, b) = [int(x) for x in oc.split("-")]
                        for i in range(a, b+1):
                            expanded.append(i)
                    else:
                        expanded.append(int(oc))

                # we have to find colspec based on self.columns
                self.only_cols  = expanded
                self.columnlist = [self.columns[x-1][0] for x in expanded]

            except Exception, e:
                print 'Error: section %s, only_cols: ' % name +\
                      'configured range is invalid'
                raise PGLoader_Error, e

        if self.only_cols is None:
            if self.columnlist is None:
                # default case, no user-defined cols, no restriction
                self.columnlist = [n for (n, pos) in self.columns]

        if DEBUG:
            #print "columns", self.columns
            print "only_cols", self.only_cols
            #print "udcs", self.udcs
            print "columnlist", self.columnlist

        ##
        # This option is textreader specific, but being lazy and
        # short-timed, I don't make self._parse_fields() callable from
        # outside this class. Hence the code here.
        #
        # optionnal newline escaped option
        self.newline_escapes = []
        if config.has_option(name, 'newline_escapes'):
            if NEWLINE_ESCAPES is not None:
                # this parameter is globally set, will ignore local
                # definition
                if not QUIET:
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
        # data format, from which depend data reader
        self.format = None
        if config.has_option(name, 'format'):
            self.format = config.get(name, 'format')
            
            if self.format.lower() == 'csv':
                from csvreader import CSVReader 
                self.reader = CSVReader(self.db, self.reject,
                                        self.filename, self.input_encoding,
                                        self.table, self.columns)
            
            elif self.format.lower() == 'text':
                from textreader import TextReader
                self.reader = TextReader(self.db, self.reject,
                                         self.filename, self.input_encoding,
                                         self.table, self.columns,
                                         self.newline_escapes)
            
        if self.format is None:
            print 'Error: %s: format parameter needed' % name
            raise PGLoader_Error

        ##
        # parse the reader specific section options
        self.reader.readconfig(name, config)

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
            serial = 1
            
            for field_def in str.split(','):
                if argtype == 'int' and field_def.find(':') == -1:
                    # support for automatic ordering
                    properties = [field_def.strip(), serial]
                else:
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

                # update serial
                if argtype == 'int':
                    serial = int(arg) + 1
                    
        except Exception, error:
            # FIXME: make some errors and write some error messages
            raise
            
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
            self.reject.print_stats(self.name, QUIET)

        if not QUIET:
            if self.db is not None:
                self.updates = self.db.commited_rows
                self.db.print_stats()
        return

    def run(self):
        """ depending on configuration, do given job """

        # Announce the beginning of the work
        if not QUIET:
            print
            print "[%s]" % self.name

        if TRUNCATE and not DRY_RUN:
            self.db.truncate(self.table)

        if self.columns is not None:
            if not QUIET:
                print "Notice: COPY csv data"
            self.data_import()

        elif self.blob_cols is not None:
            # elif: COPY process also blob data
            if not QUIET:
                print "Notice: UPDATE blob data"

        # then show up some stats
        self.print_stats()

    def data_import(self):
        """ import CSV or TEXT data, using COPY """
        for line, columns in self.reader.readlines():
            if self.blob_cols is not None:
                columns, rowids = self.read_blob(line, columns)

            data = columns

            if self.udcs:
                dudcs = dict(self.udcs)
                ddict = dict(self.columns)
                data = []
                for c in self.copy_columns:
                    if c in ddict:
                        data.append(columns[ddict[c]-1])
                    else:
                        data.append(dudcs[c])

                if DEBUG:
                    print 'columns', columns
                    print 'data   ', data

            else:
                if self.col_mapping:
                    if DEBUG:
                        print 'col_mapping', self.col_mapping

                    data = [columns[i-1] for i in self.col_mapping]

                    if DEBUG:
                        print 'columns', columns
                        print 'data   ', data

                if self.only_cols:
                    # only consider data matched by self.only_cols
                    if self.col_mapping:
                        data = [columns[self.col_mapping[i-1]-1]
                                for i in self.only_cols]
                    else:
                        data = [columns[i-1] for i in self.only_cols]

            if DRY_RUN or DEBUG:
                print line
                print self.columnlist, data
                print
                    
            if not DRY_RUN:
                self.db.copy_from(self.table, self.columnlist,
                                  data, line, self.reject)

        if not DRY_RUN:
            # we may need a last COPY for the rest of data
            self.db.copy_from(self.table, self.columnlist,
                              None, None, self.reject, EOF = True)

        return

    ##
    # BLOB data reading/parsing
    # probably should be moved out from this file

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
                        self.blobs[abs_blobname] = \
                                                 ifx_clob(abs_blobname,
                                                          self.input_encoding)

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

