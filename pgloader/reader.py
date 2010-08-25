# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader data reader interface and defaults

import sys
from tools    import PGLoader_Error, Reject, parse_config_string
from db       import db
from lo       import ifx_clob, ifx_blob

from options import DRY_RUN, PEDANTIC
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

    def __init__(self, log, db, reject,
                 filename, input_encoding, table, columns):
        """ init internal variables """
        log.debug('reader __init__ %s %s %s', filename, table, columns)

        self.log       = log
        self.db        = db
        self.filename  = filename
        self.input_encoding = input_encoding
        self.table     = table
        self.columns   = columns
        self.reject    = reject
        self.mem_units = {'kB': 1024,
                          'MB': 1024*1024,
                          'GB': 1024*1024*1024,
                          'TB': 1024*1024*1024*1024}

        if self.input_encoding is None:
            if INPUT_ENCODING is not None:
                self.input_encoding = INPUT_ENCODING

        # (start, end) are used for split_file_reading mode
        # queue when in round_robin_read mode
        self.start = None
        self.end   = None

    def readconfig(self, config, name, template):
        """ read configuration section for common options

        name is configuration section name, conf the ConfigParser object

        template is the (maybe None) template section name declared in
        the use_template configuration option.

        specific option reading code is to be found on subclasses
        which implements read data parsing code.

        see textreader.py and csvreader.py
        """

        if not DRY_RUN:
            # optionnal null and empty_string per table parameters
            self._getopt('null', config, name, template, NULL)
            self.db.null = parse_config_string(self.null)

            self._getopt('empty_string', config, name, template, EMPTY_STRING)
            self.db.empty_string = parse_config_string(self.empty_string)

        self._getopt('field_sep', config, name, template, FIELD_SEP)
        self.field_sep = self.field_sep.decode('string-escape')

        ##
        # FROM_COUNT takes precedence over skip_head_lines
        if FROM_COUNT is None or FROM_COUNT == 0:
            self._getopt('skip_head_lines', config, name, template, 0, 'int')
        else:
            self.skip_head_lines = FROM_COUNT - 1

        if len(self.field_sep) != 1:
            raise PGLoader_Error, "field_sep must be 1 char, not %d (%s)" \
                  % (len(self.field_sep), self.field_sep)
        
        if not DRY_RUN:
            if self.db.copy_sep is None:
                self.db.copy_sep = self.field_sep

        if not DRY_RUN:
            self.log.debug("reader.readconfig null: '%s'" % self.db.null)
            self.log.debug("reader.readconfig empty_string: '%s'",
                           self.db.empty_string)
            self.log.debug("reader.db %s copy_sep %s" % (self.db, self.db.copy_sep))
            
        self.log.debug("reader.readconfig field_sep: '%s'", self.field_sep)
        self.log.debug("reader.readconfig skip_head_lines: %d",
                       self.skip_head_lines)

    def _getopt(self, option, config, section, template, default = None, opt_type = "char"):
        """ Init given configuration option """

        if config.has_option(section, option):
            self.__dict__[option] = config.get(section, option)
            self.log.debug("reader._getopt %s from %s is '%s'" % (option, section, self.__dict__[option]))

        elif template and config.has_option(template, option):
            self.__dict__[option] = config.get(template, option)
            self.log.debug("reader._getopt %s from %s is '%s'" \
                           % (option, template, self.__dict__[option]))

        elif option not in self.__dict__:
            self.log.debug("reader._getopt %s defaults to '%s'" \
                           % (option, default))
            self.__dict__[option] = default

        if opt_type == 'int' and self.__dict__[option] is not None:
            try:
                self.__dict__[option] = int(self.__dict__[option])
            except ValueError, e:
                self.log.error('Configuration option %s.%s is not an int: %s' \
                               % (section, option, self.__dict__[option]))
                raise PGLoader_Error, e

        elif opt_type == 'mem' and self.__dict__[option] is not None:
            try:
                opt = self.__dict__[option]
                if type(opt) == type("string") \
                       and len(opt) > 2 and opt [-2:] in self.mem_units:
                    unit = opt[-2:]
                    size = int(opt[:-2]) * self.mem_units[unit]
                    self.__dict__[option] = int(size)
                else:
                    self.__dict__[option] = int(self.__dict__[option])
            except ValueError, e:
                self.log.error('Configuration option %s.%s is not a memsize: %s' \
                               % (section, option, self.__dict__[option]))
                raise PGLoader_Error, e

        return self.__dict__[option]

    def readlines(self):
        """ read data from configured file, and generate (yields) for
        each data line: offsets, line, columns and rowid 

        offsets: list of line numbers where the line was read in the file
                 tuple of (reader offset, [list, of, line, numbers])

        the second case is used when in split file reading mode
        """
        pass

    def set_boundaries(self, (start, end)):
        """ set the boundaries of this reader """
        self.start = start
        self.end   = end

        self.log.debug("reader start=%d, end=%d" % (self.start, self.end))


class UnbufferedFileReader:
    """
    Allow to read a file line by line, avoiding any read-buffering
    effect. This allows for readers to reliably compare fd.tell()
    position after each line reading.
    """

    def __init__(self, filename, log,
                 mode = "rb", encoding = None,
                 start = None, end = None,
                 skip_head_lines = 0,
                 check_count = True):
        """ constructor """
        self.filename = filename
        self.log      = log
        self.mode     = mode
        self.encoding = encoding
        self.start    = start
        self.end      = end
        self.fd       = None
        self.position = 0
        self.line_nb  = 0

        # check_count can be set to False when phisical lines and logical
        # lines counts can diverge, like in textreader.py
        self.check_count = check_count
        self.skip_head_lines = skip_head_lines
        self.reading = self.skip_head_lines == 0

        # we don't yet force buffering, but...
        self.bufsize = -1
        
        if self.encoding is not None:
            try:
                import codecs
                if self.filename == 'sys.stdin':
                    f = sys.stdin
                else:
                    f = open(self.filename, self.mode, self.bufsize)

                self.fd = codecs.getreader(self.encoding)(f)
                self.log.info("Opened '%s' with encoding '%s'" \
                              % (self.filename, self.encoding))
            except LookupError, e:
                # codec not found
                raise PGLoader_Error, "Input codec: %s" % e
            except IOError, e:
                # file not found, for example
                raise PGLoader_Error, "IOError: %s" % e
            
        else:
            try:
                if self.filename == 'sys.stdin':
                    self.fd = sys.stdin
                else:
                    self.fd = open(self.filename, mode, self.bufsize)
            except IOError, error:
                raise PGLoader_Error, error

        if self.start:
            self.fd.seek(self.start)
            self.position = self.fd.tell()

        self.log.info("Opened '%s' in %s (fileno %s), ftell %d" \
                      % (self.filename, self.fd,
                         self.fd.fileno(), self.position))
        return

    def tell(self):
        return self.position

    def seek(self, position):
        self.fd.seek(position)
        self.position = self.fd.tell()

        return self.position
        
    def next(self):
        """ implement the iterator protocol """
        yield self.__iter__()

    def __iter__(self):
        """ read a line at a time """
        line = 'dumb non-empty init value'
        last_line_read = False
        
        while line != '':
            line = self.fd.readline()
            self.line_nb += 1

            ## try:
            ##     self.position = self.fd.tell()
            ## except IOError, error:
            ##     #IOError: [Errno 29] Illegal seek --- when stdin reaches EOF
            ##     self.log.info(error)
            ##     return

            ##
            # if -F is used, count lines to skip, and skip them
            if self.skip_head_lines > 0:
                if self.line_nb <= self.skip_head_lines:
                    continue

                if self.line_nb == self.skip_head_lines + 1:
                    self.reading = True
                    self.log.info('reached beginning on line %d', self.line_nb)


            # check if we already processed COUNT lines
            if self.check_count:
                if COUNT is not None and self.reading \
                   and (self.line_nb - self.skip_head_lines + 1) > COUNT:

                    self.log.info('reached line %d, stopping', self.line_nb)
                    return

            # check EOF (real or multi-readers)
            if line == '' or last_line_read:
                try:
                    self.log.debug("FileReader stoping, offset %d >= %s" \
                                   % (self.fd.tell(), self.end))
                except IOError, error:
                    #IOError: [Errno 29] Illegal seek --- when stdin reaches
                    # EOF should not happen as --load-from-stdin and
                    # --boundaries are not accepted at the same time
                    self.log.info(error)
                
                self.fd.close()
                return

            # check multi-reader boundaries
            if self.end is not None and self.fd.tell() >= self.end:
                # we want to process current line and stop at next
                # iteration
                self.log.info("Reached position %d, reading last line" \
                              % self.fd.tell())
                last_line_read = True

            if self.encoding is not None:
                yield line.encode(self.encoding)
            else:
                yield line

        return
    
