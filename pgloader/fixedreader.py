# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader text format reader
#
# handles configuration, parse data, then pass them to database module for
# COPY preparation

import os, sys, os.path, time

from tools    import PGLoader_Error, Reject, parse_config_string
from db       import db
from reader   import DataReader, UnbufferedFileReader

from options import DRY_RUN, PEDANTIC
from options import TRUNCATE, VACUUM
from options import COUNT, FROM_COUNT, FROM_ID
from options import INPUT_ENCODING, PG_CLIENT_ENCODING
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import NEWLINE_ESCAPES

class FixedReader(DataReader):
    """
    Read fixed file format, configuration gives for each field
     - field name
     - start position
     - length
    """

    def readconfig(self, config, name, template):
        """ get this reader module configuration from config file """
        DataReader.readconfig(self, config, name, template)

        # this will be called twice if templates are in used, so we
        # have to protect ourselves against removing already read
        # configurations while in second run.

        self._getopt('fixed_specs', config, name, template, None)

        if self.fixed_specs:
            self.positions = {}
            # parse the fixed specs
            specs = [x.strip().split(':') for x in self.fixed_specs.strip().split(',')]
            try:
                for name, start, length in specs:
                    self.positions[name] = (int(start), int(length))
                    
            except ValueError, e:
                self.log.error("%s.fixed_specs, " + \
                               "start and length must be numbers", name)
                
                raise PGLoader_Error, \
                      "Please fix %s.fixed_specs configuration" % name
        else:
            msg = "section %s: fixed format type require 'fixed_specs'" % name
            raise PGLoader_Error, msg

        self.log.debug('reader.readconfig: positions %s', self.positions)

    def readlines(self):
        """ read data from configured file, and generate (yields) for
        each data line: line, columns and rowid """

        self.fd = UnbufferedFileReader(self.filename, self.log,
                                       encoding = self.input_encoding,
                                       start    = self.start,
                                       end      = self.end)

        line_nb = 0

        for line in self.fd:
            line_nb += 1
            line     = line.strip("\n")
            llen     = len(line)
            columns  = []

            for cname, cpos in self.columns:
                start, length = self.positions[cname]

                if llen < (start+length):
                    self.log.error("Line %d is too short " % line_nb +
                                   "(column %s requires len >= %d)" \
                                   % (cname, start+length))

                    msg = "Please review fixed_specs configuration"
                    raise PGLoader_Error, msg
                
                columns.append(line[start:start+length])

            yield line, columns
