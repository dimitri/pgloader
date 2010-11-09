# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader main class
#
# handles configuration, parse data, then pass them to database module for
# COPY preparation

import os, sys, os.path, time, codecs, threading
from cStringIO import StringIO
from tempfile import gettempdir

from logger   import log, getLogger
from tools    import PGLoader_Error, Reject, parse_config_string, check_events
from db       import db
from lo       import ifx_clob, ifx_blob

from options import DRY_RUN, PEDANTIC
from options import TRUNCATE, VACUUM, TRIGGERS
from options import COUNT, FROM_COUNT, FROM_ID
from options import INPUT_ENCODING, PG_CLIENT_ENCODING
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import PG_OPTIONS
from options import NEWLINE_ESCAPES
from options import UDC_PREFIX
from options import REFORMAT_PATH
from options import REJECT_LOG_FILE, REJECT_DATA_FILE
from options import MAX_PARALLEL_SECTIONS
from options import DEFAULT_SECTION_THREADS, SECTION_THREADS, SPLIT_FILE_READING
from options import RRQUEUE_SIZE

class PGLoader(threading.Thread):
    """
    PGLoader reads some data file and depending on ts configuration,
    import data with COPY or update blob data with UPDATE.
    """

    def __init__(self, name, config, sem, (started, finished),
                 stats,
                 logname = None,
                 reject = None, queue = None, lock = None, copy_sep = None):
        """ Init with a configuration section """
        
        # logname is given when we use several Threads for reading
        # input file
        self.logname = logname
        if self.logname is None:
            self.logname = name
        self.log = getLogger(self.logname)

        threading.Thread.__init__(self, name = self.logname)

        # sem, stats and queue (if not None) are global objects
        # sem       is shared by all threads at the same level
        # started   is a threads.Event object to set() once in run()
        # finished  is a threads.Event object to set() once processing is over
        # stats     is a private entry of a shared dict
        # queue     is given when reading in round robin mode
        # lock      is a threading.Lock for reading sync
        # reject    is the common reject object
        #
        self.sem      = sem
        self.started  = started
        self.stats    = stats
        self.finished = finished
        self.queue    = queue
        self.lock     = lock
        self.reject   = reject

        # thereafter parameters are local
        self.name    = name
        self.config  = config

        # in the round-robin reader case, workers won't have
        # initialized a reader, thus won't read the configuration of
        # copy_sep.
        self.copy_sep = copy_sep

        self.template     = None
        self.use_template = None
        self.tsection     = None

        self.index     = None
        self.all_cols  = None
        self.columns   = None
        self.blob_cols = None

        self.config_errors = 0
        self.errors        = 0
        self.updates       = 0
        self.init_time     = None

        # we may have to open several clob files while parsing the
        # unload data file, hence we keep track of them all
        self.blobs = {}

        if config.has_option(name, 'template'):
            self.template = True
            self.log.debug("[%s] is a template", self.name)

        if not self.template:
            self.log.debug("[%s] parse configuration", self.name)

        if not self.template:
            # check if the section wants to use a template
            if config.has_option(name, 'use_template'):
                self.tsection = config.get(name, 'use_template')

        self._dbconfig(config)

        if self.tsection is not None:
            self.template = config.get(name, 'use_template')

            if not config.has_section(self.template):
                m = '%s refers to unknown template section %s' \
                    % (name, self.template)

                raise PGLoader_Error, m

            # first load template configuration
            self.log.debug("Reading configuration from template " +\
                           "section [%s]", self.template)

            self.real_log = self.log
            self.log = getLogger(self.template)

            try:
                self._read_conf(self.template, config, db,
                                want_template = True)
            except PGLoader_Error, e:
                self.log.error(e)
                m = "%s.use_template does not refer to a template section"\
                    % name
                raise PGLoader_Error, m

            # reinit self.template now its relative config section is read
            self.template = None
            self.log      = self.real_log

        if not self.template:
            # now load specific configuration
            self.log.debug("Reading configuration from section [%s]", name)
            self._read_conf(name, config, db)

        self.log.debug('%s init done' % name)

    def __del__(self):
        """ PGLoader destructor, we close the db connection """
        if not DRY_RUN:
            self.db.close()

    def _dbconfig(self, config):
        """ connects to database """
        section = 'pgsql'
    
        if DRY_RUN:
            log.info("dry run mode, not connecting to database")
            self.db = None
            return

        try:
            self.db = db(config.get(section, 'host'),
                         config.getint(section, 'port'),
                         config.get(section, 'base'),
                         config.get(section, 'user'),
                         config.get(section, 'pass'),
                         connect = False)

            for opt in ['client_encoding', 'datestyle', 'lc_messages']:
                if config.has_option(section, opt):
                    self.db.pg_options[opt] = \
                        parse_config_string(config.get(section, opt))

            # PostgreSQL options
            from tools import parse_pg_options
            parse_pg_options(self.log, config, section, self.db.pg_options)
            self.log.debug("_dbconfig: %s" % str(self.db.pg_options))

            if config.has_option(section, 'copy_every'):
                self.db.copy_every = config.getint(section, 'copy_every')

            if config.has_option(section, 'commit_every'):
                self.db.commit_every = config.getint(
                    section, 'commit_every')

            if self.copy_sep is not None:
                self.db.copy_sep = self.copy_sep
                self.log.debug("got copy_sep '%s' from self.copy_sep" \
                               % self.copy_sep)

            if config.has_option(section, 'copy_delimiter'):
                self.db.copy_sep = config.get(section, 'copy_delimiter')
                self.log.debug("got copy_sep '%s' from %s" \
                               % (self.db.copy_sep, section))
                
            elif config.has_option(self.tsection, 'copy_delimiter'):
                self.db.copy_sep = config.get(section, 'copy_delimiter')
                self.log.debug("got copy_sep '%s' from self.copy_sep" \
                               % (self.db.copy_sep, self.tsection))

            self.log.debug("_dbconnect copy_sep %s " % self.db.copy_sep)

            # we want self.db messages to get printed as from our section
            self.db.log = self.log

        except Exception, error:
            log.error("Could not initialize PostgreSQL connection")
            raise PGLoader_Error, error

    def _postinit(self):
        """ This has to be called while self.sem is acquired """
        # Now reset database connection
        if not DRY_RUN:
            self.db.reset()

        if not self.template and not DRY_RUN:
            # check we have properly configured the copy separator
            if self.db.copy_sep is None:
                self.log.debug("%s" % self.db)
                self.log.error("COPY sep is %s" % self.db.copy_sep)
                msg = "BUG: pgloader couldn't configure its COPY separator"
                raise PGLoader_Error, msg
        
    def _read_conf(self, name, config, db, want_template = False):
        """ init self from config section name  """

        # we'll need both of them from the globals
        global FROM_COUNT, FROM_ID

        if want_template and not config.has_option(name, 'template'):
            e = 'Error: section %s is not a template' % name
            raise PGLoader_Error, e

        ##
        # reject log and data files defaults to /tmp/<section>.rej[.log]
        if self.reject is None:
            # If we've been given a reject object, just use it, don't
            # even try to init a new one from configuration
            
            if config.has_option(name, 'reject_log'):
                self.reject_log = config.get(name, 'reject_log')

            if config.has_option(name, 'reject_data'):
                self.reject_data = config.get(name, 'reject_data')

            if not self.template and 'reject_log' not in self.__dict__:
                self.reject_log = os.path.join(gettempdir(), REJECT_LOG_FILE % name)

            if not self.template and 'reject_data' not in self.__dict__:
                self.reject_data = os.path.join(gettempdir(), REJECT_DATA_FILE % name)

            # reject logging
            if not self.template:
                self.reject = Reject(self.log,
                                     self.reject_log, self.reject_data)

                self.log.debug('reject log in %s', self.reject.reject_log)
                self.log.debug('rejected data in %s', self.reject.reject_data)

            else:
                # needed to instanciate self.reject while in template section
                self.reject = None

        # optionnal local option input_encoding
        self.input_encoding = INPUT_ENCODING
        if config.has_option(name, 'input_encoding'):
            self.input_encoding = parse_config_string(
                config.get(name, 'input_encoding'))
        self.log.debug("input_encoding: '%s'", self.input_encoding)

        # optionnal local option client_encoding and datestyle
        for opt in ['client_encoding', 'datestyle']:
            if config.has_option(name, opt):
                self.db.pg_options[opt] = parse_config_string(config.get(name, opt))

                if not DRY_RUN:
                    self.log.debug("%s: '%s'", opt, self.db.pg_options[opt])

        # optionnal local pg_options
        # precedence is given to command line parsing, which is in PG_OPTIONS
        from tools import parse_pg_options
        parse_pg_options(log, config, name, self.db.pg_options, overwrite=True)
        if not self.template:
            if PG_OPTIONS:
                self.db.pg_options.update(PG_OPTIONS)

        ##
        # data filename
        for opt in ('table', 'filename'):
            if config.has_option(name, opt):
                self.log.debug('%s.%s: %s', name, opt, config.get(name, opt))
                self.__dict__[opt] = config.get(name, opt)
            else:
                if not self.template and opt not in self.__dict__:
                    msg = "Error: Please configure %s.%s" % (name, opt)
                    raise PGLoader_Error, msg
                
                elif not self.template and not self.__dict__[opt]:
                    self.log.error('Error: please configure %s.%s', name, opt)
                    self.config_errors += 1
                    
                else:
                    # Reading Configuration Template section
                    # we want the attribute to exists for further usage
                    if opt not in self.__dict__:
                        self.__dict__[opt] = None

        ##
        # we parse some columns definitions
        if config.has_option(name, 'index'):
            self._parse_fields('index', config.get(name, 'index'))

        if config.has_option(name, 'columns'):
            conf_cols = config.get(name, 'columns')
            if conf_cols == '*':
                self.all_cols    = True
                self.db.all_cols = True

                # get column list from database
                self.columns = self.db.get_all_columns(self.table)
                self.log.info("columns = *, got %s", str(self.columns))

            else:
                self._parse_fields('columns', config.get(name, 'columns'))

        if config.has_option(name, 'blob_columns'):
            self._parse_fields('blob_cols',
                               config.get(name, 'blob_columns'),
                               btype = True)

        self.log.debug('index %s', str(self.index))
        self.log.debug('columns %s', str(self.columns))
        self.log.debug('blob_columns %s', str(self.blob_cols))

        if self.columns is None:
            if not self.template:
                self.log.error('%s has no columns defined', name)
                self.config_errors += 1

            else:
                # non critical error, and code thereafter wants to use
                # self.columns as a list
                self.columns = []

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

        self.log.debug('udcs: %s', str(self.udcs))

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
                    self.log.error('%s is configured both as a %s.columns ' +\
                                   'entry and as a user-defined column',
                                   c, name)

                self.config_errors += 1

        # we need the copy_columns parameter if user-defined columns
        # are used
        #
        # when using templates, we can read copy_columns setup without
        # having any user-defined column defined
        if self.template or self.udcs:
            if config.has_option(name, 'copy_columns'):
                namelist = [n for (n, c) in self.columns]
                if self.udcs:
                    namelist += [n for (n, v) in self.udcs]

                self.config_copy_columns = config.get(name, 'copy_columns')
                self.config_copy_columns = self.config_copy_columns.split(',')
                
                self.copy_columns = []
                for x in self.config_copy_columns:
                    x = x.strip(' \n\r')
                    # just add all the given columns in this pass
                    self.copy_columns.append(x)

                self.log.debug('config copy_columns %s',
                               str(self.config_copy_columns))
                self.log.debug('copy_columns %s', str(self.copy_columns))
                    
        if not self.template:
            # check for errors time!
            if 'copy_columns' in self.__dict__:
                namelist = [n for (n, c) in self.columns] + \
                           [n for (n, v) in self.udcs]

                for x in self.copy_columns:
                    if x not in namelist:
                        self.log.error('"%s" not in %s column list, ' + \
                                       'including user defined columns',
                                       x, name)
                        self.config_errors += 1
                        
                if len(self.copy_columns) != len(self.config_copy_columns):
                    self.log.error('%s.copy_columns refers to ' +\
                                   'unconfigured columns', name)

                    self.config_errors += 1
                    
            elif self.udcs:
                self.log.error('section %s does not define copy_columns ' +\
                               'but uses user-defined columns', name)

                self.config_errors += 1

        # in the copy_columns case, columnlist is that simple:
        self.columnlist = None
        if self.udcs:
            if 'copy_columns' in self.__dict__ and self.copy_columns:
                self.columnlist = self.copy_columns

        self.log.debug('udcs: %s', str(self.udcs))
        if self.udcs and 'copy_columns' in self.__dict__:
            self.log.debug('copy_columns %s', str(self.copy_columns))

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
                self.log.error('section %s defines both ' +\
                               'user-defined columns and only_cols', name)

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
                self.log.error('section %s, only_cols: ' +\
                               'configured range is invalid', name)
                raise PGLoader_Error, e

        if self.only_cols is None:
            if self.columnlist is None:
                # default case, no user-defined cols, no restriction
                self.columnlist = [n for (n, pos) in self.columns]

        self.log.debug("only_cols %s", str(self.only_cols))
        self.log.debug("columnlist %s", str(self.columnlist))

        if not self.template and self.only_cols is not None:
            self.db.all_cols = False

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
                self.log.warning("ignoring %s newline_escapes option" +\
                                 "option is set to '%s' globally",
                                 name, NEWLINE_ESCAPES)
            else:
                self._parse_fields('newline_escapes',
                                   config.get(name, 'newline_escapes'),
                                   argtype = 'char')

        if NEWLINE_ESCAPES is not None:
            # set NEWLINE_ESCAPES for each table column
            self.newline_escapes = [(a, NEWLINE_ESCAPES)
                                    for (a, x) in self.columns]

        self.log.debug("self.newline_escapes = '%s'" % self.newline_escapes)

        ##
        # Parallelism knobs, give preference to command line
        if SECTION_THREADS:
            self.section_threads = SECTION_THREADS
        elif config.has_option(name, 'section_threads'):
            self.section_threads = config.getint(name, 'section_threads')
        else:
            self.section_threads = DEFAULT_SECTION_THREADS

        if not self.template:
            # only log the definitive information
            self.log.info("Loading threads: %d" % self.section_threads)

        if config.has_option(name, 'split_file_reading'):
            self.split_file_reading = config.get(name, 'split_file_reading') == 'True'
        else:
            self.split_file_reading = SPLIT_FILE_READING

        self.rrqueue_size = RRQUEUE_SIZE
        if config.has_option(name, 'rrqueue_size'):
            self.rrqueue_size = config.getint(name, 'rrqueue_size')

        if self.rrqueue_size is None or self.rrqueue_size < 1:
            if DRY_RUN:
                # won't be used
                self.rrqueue_size = 1
            else:
                self.rrqueue_size = self.db.copy_every
            
        if not self.template:
            for opt in ('section_threads', 'split_file_reading'):
                self.log.debug('%s.%s = %s' % (name, opt, str(self.__dict__[opt])))

        if not self.template and self.split_file_reading:
            opt = 'split_file_reading'
            if FROM_COUNT is not None and FROM_COUNT > 0:
                raise PGLoader_Error, \
                      "Conflict: can't use both '%s' and '--from'" % opt

            if FROM_ID is not None:
                raise PGLoader_Error, \
                      "Conflict: can't use both '%s' and '--from-id'" % opt

        ##
        # Reader's init
        if config.has_option(name, 'format'):
            self.format = config.get(name, 'format')
        
        if not self.template and self.queue is None:
            # Only init self.reader in real section, not from
            # template.  self.reader.readconfig() will care about
            # reading its configuration from template and current
            # section.
            #
            # If self.queue is not None, we'll read data from it and
            # not from self.reader, so we don't care about it.

            if 'format' not in self.__dict__:
                raise PGLoader_Error, "Please configure %s.format" % name
                
            self.log.debug("File '%s' will be read in %s format" \
                           % (self.filename, self.format))

            if self.format.lower() == 'csv':
                from csvreader import CSVReader 
                self.reader = CSVReader(self.log, self.db, self.reject,
                                        self.filename,
                                        self.input_encoding,
                                        self.table, self.columns)

            elif self.format.lower() == 'text':
                from textreader import TextReader
                self.reader = TextReader(self.log, self.db, self.reject,
                                         self.filename,
                                         self.input_encoding,
                                         self.table, self.columns,
                                         self.newline_escapes)
                
            elif self.format.lower() == 'fixed':
                from fixedreader import FixedReader
                self.reader = FixedReader(self.log, self.db, self.reject,
                                          self.filename,
                                          self.input_encoding,
                                          self.table, self.columns)
                
            else:
                self.log.error("unknown format '%s'")
                raise PGLoader_Error, "Skipping section %s" % self.name

            self.log.debug('reader.readconfig()')
            self.reader.readconfig(config, name, self.tsection)


            if self.split_file_reading:
                if self.format.lower() == 'text' \
                   and (self.reader.field_count is not None \
                        or self.reader.trailing_sep):

                    # split_file_reading is not compatible with text
                    # mode when field_count or trailing_sep is used
                    # (meaning end of line could be found in the data)

                    raise PGLoader_Error, \
                          "Can't use split_file_reading with text " +\
                          "format when 'field_count' or 'trailing_sep' is used"

        ##
        # Some column might need reformating
        if config.has_option(name, 'reformat'):
            self._parse_fields('c_reformat', config.get(name, 'reformat'),
                               btype = True, argtype = 'string')
        else:
            # remember reformat could have been configured from template
            if 'c_reformat' not in self.__dict__:
                self.c_reformat = self.reformat = None

        self.log.debug('reformat %s', str(self.c_reformat))

        # check the configure reformating is available
        if not self.template and self.c_reformat:
            import imp
            self.reformat = []
            
            for r_colname, r_module, r_function in self.c_reformat:
                if r_colname not in self.columnlist:
                    self.log.error('%s.reformat refers to unknown column %s',
                                   name, r_colname)
                    self.config_errors += 1

                # load the given module name and function
                module = None
                try:
                    fp, pathname, description = \
                        imp.find_module(r_module, REFORMAT_PATH)

                    self.log.debug('Found %s at %s', r_module, str(pathname))
                    
                    module = imp.load_module(r_module,
                                             fp, pathname, description)
                    
                except ImportError, e:
                    self.log.error('%s failed to import reformat module "%s"',
                                   name, r_module)
                    self.log.error('from %s', str(REFORMAT_PATH))
                    self.log.error(e)
                    self.config_errors += 1


                if module:
                    if r_function in module.__dict__:
                        self.reformat.append((r_colname,
                                              module.__dict__[r_function]))
                    else:
                        self.log.error('reformat module %s has no %s function',
                                       r_module, r_function)
                        self.config_errors += 1

        if not self.template:
            self.log.debug('reformat %s', str(self.reformat))

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

            self.log.info('composite key found, -I evaluated to %s', FROM_ID)

        if self.config_errors > 0:
            mesg = 'Configuration errors for section %s' % self.name
            raise PGLoader_Error, mesg

    def _parse_fields(self, attr, str, btype = False, argtype = 'int'):
        """ parse the user string str for fields definition to store
        into self.attr """

        def _getarg(arg, argtype):
            """ return arg depending on its type """
            if argtype == 'int':
                # arg is the target column index
                try:
                    arg = int(arg)
                except ValueError, e:
                    raise PGLoader_Error, e
                        
            elif argtype == 'char':
                # arg is an escape char
                if len(arg) > 1:
                    raise PGLoader_Error, 'more than one character for char'

            elif argtype == 'string':
                # accept all inputs
                pass

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
                    f.append((colname, _getarg(arg, argtype)))

                else:
                    # blob column definition, with blob type, for
                    # UPDATE usage
                    colname, arg, btype = properties
                    f.append((colname, _getarg(arg, argtype), btype))

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
            self.reject.print_stats(self.name)

        if self.db is not None:
            self.updates = self.db.commited_rows
            self.db.print_stats()
        return

    def run(self):
        """ controling thread which dispatch the job """

        # care about number of threads launched
        self.log.debug("%s.sem.acquire()" % self.logname)
        self.sem.acquire()
        self.log.debug("%s acquired starting semaphore" % self.logname)

        # tell parent thread we are running now
        self.started.set()
        self.init_time = time.time()        
        
        try:
            # postinit (will call db.reset() which will get us connected)
            self.log.debug("%s postinit" % self.logname)
            self._postinit()

	    # do the actual processing in do_run and handle all exceptions
            self.do_run()
            
        except Exception, e:
            self.log.error(e)
            self.terminate(False)
            return

        self.terminate()
        return

    def do_run(self):

        # Announce the beginning of the work
        self.log.info("%s processing" % self.logname)

        if self.section_threads == 1:
            # when "No space left on device" where logs are sent,
            # we want to catch the exception
            if 'reader' in self.__dict__ and self.reader.start is not None:
                self.log.debug("Loading from offset %d to %d" \
                               %  (self.reader.start, self.reader.end))

            self.prepare_processing()
            self.process()
            self.finish_processing()

            return

        # Mutli-Threaded processing of current section
        # We want a common Lock() protected Reject object
        #
        # note: this will be done only once, children threads are
        # started with self.section_threads == 1
        self.reject.set_lock(threading.Lock())

        if self.split_file_reading:
            # start self.section_threads workers
            self.split_file_read()

        else:
            # here we need a special thread reading the file
            self.round_robin_read()

        return

    def terminate(self, success = True):
        """ Announce it's over and free the concurrency control semaphore """

        # force PostgreSQL connection closing, do not wait for garbage
        # collector
        if not DRY_RUN:
            self.db.close()

        try:
            self.log.info("releasing %s semaphore" % self.logname)
        except IOError, e:
            # ignore "No space left on device" or other errors here
            pass
        
        self.sem.release()
        
        # tell parent thread processing is now over, here
        try:
            self.log.info("Announce it's over")
        except IOError, e:
            pass
        
        self.success = success
        self.finished.set()
        return

    def split_file_read(self):
        """ Current thread will start self.section_threads threads,
        each one reading a part of the input file. """
        
        # init boundaries to give to each thread
        from stat import ST_SIZE
        previous   = 0
        filesize   = os.stat(self.filename)[ST_SIZE]
        boundaries = []
        for partn in range(self.section_threads):
            start = previous
            end   = (partn+1)*filesize / self.section_threads
            boundaries.append((start, end))

            previous = end + 1

        self.log.debug("Spliting input file of %d bytes %s" \
                      % (filesize, str(boundaries)))

        # Now check for real boundaries
        fd = file(self.filename)
        b  = 0
        for b in range(len(boundaries)):
            start, end = boundaries[b]
            fd.seek(end)
            dummy_str = fd.readline()

            # update both current boundary end and next start
            boundaries[b] = (start, fd.tell()-1)
            if (b+1) < len(boundaries):
                boundaries[b+1] = (fd.tell(), boundaries[b+1][1])

        fd.close()

        self.log.info("Spliting input file of %d bytes %s" \
                      % (filesize, str(boundaries)))

        self.prepare_processing()

        # now create self.section_threads PGLoader threads
        sem      = threading.BoundedSemaphore(self.section_threads)
        summary  = {}
        threads  = {}
        started  = {}
        finished = {}

        for current in range(self.section_threads):
            try:
                summary[current]  = []
                started[current]  = threading.Event()
                finished[current] = threading.Event()
                current_name      = "%s.%d" % (self.name, current)

                loader = PGLoader(self.name, self.config, sem,
                                  (started[current], finished[current]),
                                  summary[current],
                                  logname = current_name,
                                  reject  = self.reject)

                loader.section_threads = 1
                loader.reader.set_boundaries(boundaries[current])
                loader.dont_prepare_nor_finish = True

                threads[current_name] = loader
                threads[current_name].start()

            except Exception, e:
                raise

        # wait for workers to have started, then wait for them to terminate
        check_events(started, self.log, "is running")
        check_events(finished, self.log, "processing is over")
        
        self.finish_processing()
        self.duration = time.time() - self.init_time
        self.log.debug('No more threads are running, %s done' % self.name)

        stats = [0, 0]
        for s in summary:
            for i in range(2, len(summary[s])):
                stats[i-2] += summary[s][i]

        for x in [self.table, self.duration] + stats:
            self.stats.append(x)

        return

    def round_robin_read(self):
        """ Start self.section_threads threads to process data, this
        thread will read the input file and distribute the processing
        on a round-robin fashion"""
        self.prepare_processing()

        try:
            from RRRtools import RRReader
        except ImportError, e:
            raise PGLoader_Error, \
                  "Please upgrade to python 2.4 or newer: %s" % e
        
        queues   = {}
        locks    = {}
        sem      = threading.BoundedSemaphore(self.section_threads)
        summary  = {}
        threads  = {}
        started  = {}
        finished = {}

        for current in range(self.section_threads):
            queues[current] = RRReader()
            locks [current] = threading.Lock()

            # acquire the lock before starting worker thread
            # and release it once its queue if full
            self.log.debug("locks[%d].acquire" % current)
            locks[current].acquire()

            try:
                summary [current] = []
                started [current] = threading.Event()
                finished[current] = threading.Event()
                current_name      = "%s.%d" % (self.name, current)

                loader = PGLoader(self.name, self.config, sem,
                                  (started[current], finished[current]),
                                  summary[current],
                                  logname  = current_name,
                                  reject   = self.reject,
                                  queue    = queues[current],
                                  lock     = locks [current],
                                  copy_sep = self.db.copy_sep)

                loader.section_threads = 1
                loader.dont_prepare_nor_finish = True
                loader.done = False

                threads[current_name] = loader
                threads[current_name].start()

            except Exception, e:
                self.log.error("Couldn't start all workers thread")
                self.log.error(e)

        if len(threads) != self.section_threads:
            self.log.error("Couldn't start all threads, check previous errors")

            check_events([x for x in finished if threads[x].isAlive()],
                         self.log, "processing is over")            
            return

        check_events(started, self.log, "is running")
        
        # Now self.section_threads are started and we have a queue and
        # a Condition for each of them.
        #
        # read the input file here, and give each worker Thread is
        # share to process, in a round-robin fashion
        n = 0               # line number
        c = 0               # current
        p = c               # previous
        
        for offsets, line, columns in self.reader.readlines():
            if p != c:
                self.log.debug("read %d lines, queue to thread %s" % (n, c))

                # release p'thread (which will empty its queue) and
                # lock c'thread --- waiting until it has emptied its
                # queue
                self.log.debug("locks[%d].release" % p)
                locks[p].release()

                # only acquire next thread lock if it's not its first usage
                # cause we acquire() the lock at init time
                #
                # c is 0..self.section_threads, each thread process a
                # queue of self.rrqueue_size elements
                if n > self.rrqueue_size * (c+1):
                    self.log.debug("locks[%d].acquire" % c)
                    locks[c].acquire()
                
            queues[c].append((offsets, line, columns))
            n += 1
            p = c
            c = (n / self.rrqueue_size) % self.section_threads

        # save this for later reference
        last_p = p
        last_c = c

        # we could have some locks to release here
        self.log.debug("p=%d c=%d n=%d (n/rrqueue_size)%%%d=%d " \
                       % (p, c, n,
                          self.section_threads,
                          (n/self.rrqueue_size) % self.section_threads) + \
                       "(n+1/rrqueue_size)%%%d=%d" \
                       % (self.section_threads,
                          ((n+1)/self.rrqueue_size) % self.section_threads))
        
        if p != c or (n % self.rrqueue_size != 0):
            self.log.debug("locks[%d].release" % p)
            locks[p].release()

        # we could have read all the data and not needed all workers,
        # log it when it's the case, then set .done = True without
        # taking again a lock which we already have.
        if n < (self.section_threads * self.rrqueue_size):
            self.log.info("processed all data with only %d workers" % (c+1))

        # mark all worker threads has done
        k = threads.keys()
        for c in range(self.section_threads):
            # we don't need any lock.acquire if we didn't use the worker
            if n > (self.section_threads * self.rrqueue_size) \
               or c <= last_c:
                self.log.debug("locks[%d].acquire to set %s.done = True" \
                           % (c, k[c]))            
                locks[c].acquire()

            
            threads[k[c]].done = True
            
            self.log.debug("locks[%d].release (done set)" % c)
            locks[c].release()

        # wait for workers to finish processing
        check_events(finished, self.log,  "processing is over")
        
        self.finish_processing()
        self.duration = time.time() - self.init_time
        self.log.debug('%s done' % self.name)

        stats = [0, 0]
        for s in summary:
            for i in range(2, len(summary[s])):
                stats[i-2] += summary[s][i]

        for x in [self.table, self.duration] + stats:
            self.stats.append(x)

        return

    def readlines(self):
        """ return next line from either self.queue or self.reader """

        if self.queue is None:
            for offsets, line, columns in self.reader.readlines():
                yield offsets, line, columns

            return

        while not self.done:
            self.lock.acquire()

            if len(self.queue) > 0:
                self.log.debug("processing queue")
                for offsets, line, columns in self.queue.readlines():
                    yield offsets, line, columns

            self.lock.release()

        return

    def prepare_processing(self):
        """ Things to do before processing data """
        if 'dont_prepare_nor_finish' in self.__dict__:
            return
        
        if not DRY_RUN:
            if TRUNCATE:
                self.db.truncate(self.table)
                
            if TRIGGERS:
                self.db.disable_triggers(self.table)

    def finish_processing(self):
        """ Things to do after processing data """
        if 'dont_prepare_nor_finish' in self.__dict__:
            return

        if TRIGGERS and not DRY_RUN:
            self.db.enable_triggers(self.table)

        self.log.debug("loading done")
        return

    def update_summary(self):
        """ update the main summary """
        self.duration = time.time() - self.init_time
        
        if self.reject is not None:
            self.errors = self.reject.errors

        if DRY_RUN:
            self.commited_rows = 0
        else:
            self.commited_rows = self.db.commited_rows
            
        for x in [self.table, self.duration,
                  self.commited_rows, self.errors]:
            self.stats.append(x)

        # then show up some stats
        self.print_stats()
        
    def process(self):
        """ depending on configuration, do given job """

        if self.columns is not None:
            self.data_import()

        elif self.blob_cols is not None:
            # elif: COPY process also blob data
            self.log.info("UPDATE blob data")

        self.update_summary()

    def data_import(self):
        """ import CSV or TEXT data, using COPY """

        # some more practical data format of internals
        ddict = dict(self.columns)
        if self.reformat:
            drefc = dict(self.reformat)
                
        if self.udcs:
            dudcs = dict(self.udcs)

        for offsets, line, columns in self.readlines():
            self.log.debug('offsets %s', offsets)

            if self.blob_cols is not None:
                columns, rowids = self.read_blob(line, columns)

            if self.reformat:
                refc = dict(self.reformat)
                data = []
                for cname, cpos in self.columns:
                    if cname in drefc:
                        # reformat the column value
                        data.append(drefc[cname](self.reject,
                                                 columns[cpos-1]))
                    else:
                        data.append(columns[cpos-1])

                self.log.debug('reformat')
                self.log.debug('columns %s', str(columns))
                self.log.debug('data    %s', str(data))

                # we want next steps to take reformated data as input
                columns = data
                    
            if self.udcs:
                dudcs = dict(self.udcs)
                data = []
                for c in self.copy_columns:
                    if c in ddict:
                        data.append(columns[ddict[c]-1])
                    else:
                        data.append(dudcs[c])

                self.log.debug('udcs')
                self.log.debug('columns %s', str(columns))
                self.log.debug('data    %s', str(data))
                
                columns = data
                
            else:
                if self.col_mapping:
                    self.log.debug('col_mapping %s', str(self.col_mapping))

                    data = [columns[i-1] for i in self.col_mapping]

                    self.log.debug('columns %s', str(columns))
                    self.log.debug('data    %s', str(data))

                    columns = data

            if self.only_cols:
                data = [columns[i-1] for i in self.only_cols]

            if not self.reformat \
                   and not self.udcs \
                   and not self.col_mapping \
                   and not self.only_cols:
                data = columns

            if DRY_RUN:
                self.log.info('< %s', line)
                self.log.info('  %s', str(self.columnlist))
                self.log.info('> %s', str(data))
            else:
                self.log.debug('< %s', line)
                self.log.debug('  %s', str(self.columnlist))
                self.log.debug('> %s', str(data))
                    
            if not DRY_RUN:
                self.db.copy_from(self.table, self.columnlist,
                                  data, line, offsets, self.reject)

        if not DRY_RUN:
            # we may need a last COPY for the rest of data
            self.db.copy_from(self.table, self.columnlist,
                              None, None, None, self.reject, EOF = True)

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
            self.log.debug(line)
            self.log.debug(str(line))

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
                        self.blobs[abs_blobname] = ifx_blob(self.log,
                                                            abs_blobname, 
                                                            self.field_sep)

                    elif btype == 'ifx_clob':
                        self.blobs[abs_blobname] = \
                                                 ifx_clob(self.log,
                                                          abs_blobname,
                                                          self.input_encoding)

                blob = self.blobs[abs_blobname]

                # we now need to get row id(s) from ordered columns
                if rowids is None:
                    rowids = self._rowids(columns)

                # get data from blob object
                data = blob.extract(rowids, c, begin, length)
                columns[blob_col] = data

                self.log.debug('read blob data')
                self.log.debug(str(rowids))
                self.log.debug(str(data))

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

