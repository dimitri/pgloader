# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader data reader interface and defaults

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
        if not DRY_RUN:
            if self.db.copy_sep is None:
                self.db.copy_sep = self.field_sep

        if not DRY_RUN:
            self.log.debug("reader.readconfig null: '%s'" % self.db.null)
            self.log.debug("reader.readconfig empty_string: '%s'",
                           self.db.empty_string)
            
        self.log.debug("reader.readconfig field_sep: '%s'", self.field_sep)

    def _getopt(self, option, config, section, template, default = None):
        """ Init given configuration option """

        if config.has_option(section, option):
            self.__dict__[option] = config.get(section, option)
            self.log.debug("reader._getopt %s from %s is '%s'" % (option, section, self.__dict__[option]))

        elif template and config.has_option(template, option):
            self.__dict__[option] = config.get(template, option)
            self.log.debug("reader._getopt %s from %s is '%s'" % (option, template, self.__dict__[option]))

        elif option not in self.__dict__:
            self.log.debug("reader._getopt %s defaults to '%s'" % (option, default))
            self.__dict__[option] = default

        return self.__dict__[option]

    def _open(self, mode = 'rb'):
        """ open self.filename wrt self.encoding """
        # we don't yet force buffering, but...
        self.bufsize = -1
        
        if self.input_encoding is not None:
            try:
                self.fd = codecs.open(self.filename,
                                      encoding  = self.input_encoding,
                                      buffering = self.bufsize)
            except LookupError, e:
                # codec not found
                raise PGLoader_Error, "Input codec: %s" % e
            except IOError, e:
                # file not found, for example
                raise PGLoader_Error, "IOError: %s" % e
        else:
            try:
                self.fd = open(self.filename, mode, self.bufsize)
            except IOError, error:
                raise PGLoader_Error, error

        return self.fd

    def readlines(self):
        """ read data from configured file, and generate (yields) for
        each data line: line, columns and rowid """
        pass

    def set_boundaries(self, (start, end)):
        """ set the boundaries of this reader """
        self.start = start
        self.end   = end

        self.log.info("reader start=%d, end=%d" % (self.start, self.end))

