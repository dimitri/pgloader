# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader main class
#
# handles configuration, parse data, then pass them to database module for
# COPY preparation

import os, sys, os.path, time, codecs, threading
from cStringIO import StringIO

from logger   import log, getLogger
from tools    import PGLoader_Error, Reject, parse_config_string
from db       import db
from lo       import ifx_clob, ifx_blob

from options import DRY_RUN, PEDANTIC
from options import TRUNCATE, VACUUM, TRIGGERS
from options import COUNT, FROM_COUNT, FROM_ID
from options import INPUT_ENCODING, PG_CLIENT_ENCODING
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import NEWLINE_ESCAPES
from options import UDC_PREFIX
from options import REFORMAT_PATH
from options import MAX_PARALLEL_SECTIONS
from options import SECTION_THREADS, SPLIT_FILE_READING

class PGLoader(threading.Thread):
    """
    PGLoader reads some data file and depending on ts configuration,
    import data with COPY or update blob data with UPDATE.
    """

    def __init__(self, name, config, sem, stats, logname = None):
        """ Init with a configuration section """
        threading.Thread.__init__(self, name = name)

        # sem and stats are global objects:
        # sem is shared by all threads at the same level, stats is a
        # private entry of a shared dict
        self.sem     = sem
        self.stats   = stats

        # thereafter parameters are local
        self.name    = name
        self.config  = config

        if logname is None:
            logname  = name
        self.log     = getLogger(logname)

        self.__dbconnect__(config)

        self.template     = None
        self.use_template = None

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

        if config.has_option(name, 'template'):
            self.template = True
            self.log.info("[%s] is a template", self.name)

        if not self.template:
            self.log.info("[%s] parse configuration", self.name)

        if not self.template:
            # check if the section wants to use a template
            if config.has_option(name, 'use_template'):
                self.template = config.get(name, 'use_template')

                if not config.has_section(self.template):
                    m = '%s refers to unknown template section %s' \
                        % (name, self.template)
                    
                    raise PGLoader_Error, m

                # first load template configuration
                self.log.info("Reading configuration from template " +\
                              "section [%s]", self.template)

                self.real_log = self.log
                self.log = getLogger(self.template)

                try:
                    self.__read_conf__(self.template, config, db,
                                       want_template = True)
                except PGLoader_Error, e:
                    self.log.error(e)
                    m = "%s.use_template does not refer to a template section"\
                        % name
                    raise PGLoader_Error, m

                # reinit self.template now its relative config section is read
                self.template = None
                self.log      = self.real_log

            # now load specific configuration
            self.log.info("Reading configuration from section [%s]", name)
            
            self.__read_conf__(name, config, db)

        # force reinit of self.reader, which depends on template and
        # specific options
        if 'reader' in self.__dict__:
            self.reader.__init__(self.log, self.db, self.reject,
                                 self.filename, self.input_encoding,
                                 self.table, self.columns)

        # Now reset database connection
        if not DRY_RUN:
            self.db.log = self.log
            self.db.reset()            

        self.log.debug('%s init done' % name)

    def __dbconnect__(self, config):
        """ connects to database """
        section = 'pgsql'
    
        if DRY_RUN:
            log.info("dry run mode, not connecting to database")
            return

        try:
            self.db = db(config.get(section, 'host'),
                         config.getint(section, 'port'),
                         config.get(section, 'base'),
                         config.get(section, 'user'),
                         config.get(section, 'pass'),
                         connect = False)

            if config.has_option(section, 'client_encoding'):
                self.db.client_encoding = parse_config_string(
                    config.get(section, 'client_encoding'))

            if config.has_option(section, 'lc_messages'):
                self.db.lc_messages = parse_config_string(
                    config.get(section, 'lc_messages'))

            if config.has_option(section, 'datestyle'):
                self.db.datestyle = parse_config_string(
                    config.get(section, 'datestyle'))

            if config.has_option(section, 'copy_every'):
                self.db.copy_every = config.getint(section, 'copy_every')

            if config.has_option(section, 'commit_every'):
                self.db.commit_every = config.getint(
                    section, 'commit_every')

            if config.has_option(section, 'copy_delimiter'):
                self.db.copy_sep = config.get(section, 'copy_delimiter')

        except Exception, error:
            log.error("Could not initialize PostgreSQL connection")
            raise PGLoader_Error, error
        
    def __read_conf__(self, name, config, db, want_template = False):
        """ init self from config section name  """

        # we'll need both of them from the globals
        global FROM_COUNT, FROM_ID

        if want_template and not config.has_option(name, 'template'):
            e = 'Error: section %s is not a template' % name
            raise PGLoader_Error, e

        ##
        # reject log and data files defaults to /tmp/<section>.rej[.log]
        if config.has_option(name, 'reject_log'):
            self.reject_log = config.get(name, 'reject_log')

        if config.has_option(name, 'reject_data'):
            self.reject_data = config.get(name, 'reject_data')
        
        if not self.template and 'reject_log' not in self.__dict__:
            self.reject_log = os.path.join('/tmp', '%s.rej.log' % name)
            
        if not self.template and 'reject_data' not in self.__dict__:
            self.reject_data = os.path.join('/tmp', '%s.rej' % name)

        # reject logging
        if not self.template:
            self.reject = Reject(self.log, self.reject_log, self.reject_data)

            self.log.info('reject log in %s', self.reject.reject_log)
            self.log.info('rejected data in %s', self.reject.reject_data)

        else:
            # needed to instanciate self.reader while in template section
            self.reject = None

        # optionnal local option client_encoding
        if config.has_option(name, 'client_encoding'):
            self.db.client_encoding = parse_config_string(
                config.get(name, 'client_encoding'))

        if not DRY_RUN:
            self.log.debug("client_encoding: '%s'", self.db.client_encoding)

        # optionnal local option input_encoding
        self.input_encoding = INPUT_ENCODING
        if config.has_option(name, 'input_encoding'):
            self.input_encoding = parse_config_string(
                config.get(name, 'input_encoding'))

        self.log.debug("input_encoding: '%s'", self.input_encoding)

        # optionnal local option datestyle
        if not DRY_RUN and config.has_option(name, 'datestyle'):
            self.db.datestyle = parse_config_string(
                config.get(name, 'datestyle'))

        if not DRY_RUN:
            self.log.debug("datestyle: '%s'", self.db.datestyle)

        ##
        # data filename
        for opt in ('table', 'filename'):
            if config.has_option(name, opt):
                self.log.debug('%s.%s: %s', name, opt, config.get(name, opt))
                self.__dict__[opt] = config.get(name, opt)
            else:
                if not self.template and not self.__dict__[opt]:
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

        ##
        # Parallelism knobs
        if config.has_option(name, 'section_threads'):
            self.section_threads = config.getint(name, 'section_threads')
        else:
            self.section_threads = SECTION_THREADS

        if config.has_option(name, 'split_file_reading'):
            self.split_file_reading = config.get(name, 'split_file_reading') == 'True'
        else:
            self.split_file_reading = SPLIT_FILE_READING
            
        if not self.template:
            for opt in ('section_threads', 'split_file_reading'):
                self.log.debug('%s.%s = %s' % (name, opt, str(self.__dict__[opt])))

        if not self.template and self.split_file_reading:
            if FROM_COUNT is not None and FROM_COUNT > 0:
                raise PGLoader_Error, \
                      "Conflict: can't use both 'split_file_reading' and '--from'"

            if FROM_ID is not None:
                raise PGLoader_Error, \
                      "Conflict: can't use both 'split_file_reading' and '--from-id'"

        ##
        # Reader's init
        if config.has_option(name, 'format'):
            self.format = config.get(name, 'format')

            if self.format.lower() == 'csv':
                from csvreader import CSVReader 
                self.reader = CSVReader(self.log, self.db, self.reject,
                                        self.filename, self.input_encoding,
                                        self.table, self.columns)

            elif self.format.lower() == 'text':
                from textreader import TextReader
                self.reader = TextReader(self.log, self.db, self.reject,
                                         self.filename, self.input_encoding,
                                         self.table, self.columns,
                                         self.newline_escapes)

        if 'reader' in self.__dict__:
            self.log.debug('reader.readconfig()')
            self.reader.readconfig(name, config)

        if not self.template and \
           ('format' not in self.__dict__ or self.format is None):
            # error only when not loading the Template part
            self.log.Error('%s: format parameter needed', name)
            raise PGLoader_Error

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

        def __getarg(arg, argtype):
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
            self.reject.print_stats(self.name)

        if self.db is not None:
            self.updates = self.db.commited_rows
            self.db.print_stats()
        return

    def run(self):
        """ controling thread which dispatch the job """

        # care about number of threads launched
        self.sem.acquire()
        
        # Announce the beginning of the work
        self.log.info("%s launched" % self.name)

        if self.section_threads == 1:
            if self.reader.start is not None:
                self.log.info("Loading from offset %d to %d" \
                              %  (self.reader.start, self.reader.end))

            self.prepare_processing()
            self.process()
            self.finish_processing()

            self.log.info("Releasing %s" % self.name)
            self.sem.release()

            return

        if self.split_file_reading:
            # this option is not compatible with text mode when
            # field_count is used (meaning end of line could be found
            # in the data)
            if self.format.lower() == 'text' and self.field_count is not None:
                raise PGLoader_Error, \
                      "Can't use split_file_reading with text " +\
                      "format when 'field_count' is used"
            
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
            sem     = threading.BoundedSemaphore(self.section_threads)
            summary = {}
            threads = {}
            running = 0
            
            for current in range(self.section_threads):
                try:
                    summary[current] = []
                    current_name     = "%s[%d]" % (self.name, current)

                    loader = PGLoader(self.name, self.config, sem,
                                      summary[current], current_name)

                    loader.section_threads = 1
                    loader.reader.set_boundaries(boundaries[current])
                    loader.dont_prepare_nor_finish = True

                    threads[current_name] = loader
                    threads[current_name].start()
                    running += 1

                except Exception, e:
                    raise

            # wait for loaders completion, first let them some time to
            # be started
            time.sleep(2)
            
            from tools import running_threads
            n = running_threads(threads)            
            log.info("Waiting for %d threads to terminate" % n)

            # Try to acquire all semaphore entries
            for i in range(self.section_threads):
                sem.acquire()
                log.info("Acquired %d times, " % (i+1) + \
                          "still waiting for %d threads to terminate" \
                          % running_threads(threads))

            self.finish_processing()
            self.duration = time.time() - self.init_time
            self.log.info('No more threads are running, %s done' % self.name)

            stats = [0, 0]
            for s in summary:
                for i in range(2, len(summary[s])):
                    stats[i-2] += summary[s][i]

            for x in [self.table, self.duration] + stats:
                self.stats.append(x)

        else:
            # here we need a special thread reading the file
            pass

        self.sem.release()
        self.log.info("%s released" % self.name)
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

        # then show up some stats
        self.print_stats()

        self.log.info("loading done")
        return

    def update_summary(self):
        """ update the main summary """
        self.duration = time.time() - self.init_time
        
        if self.reject is not None:
            self.errors = self.reject.errors
            
        for x in [self.table, self.duration, self.db.commited_rows, self.errors]:
            self.stats.append(x)
        
    def process(self):
        """ depending on configuration, do given job """

        if self.columns is not None:
            self.log.info("COPY csv data")
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
        
        for line, columns in self.reader.readlines():
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

