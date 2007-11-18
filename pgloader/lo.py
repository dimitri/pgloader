# Author: Dimitri Fontaine <dimitri@dalibo.com>
#
# pgloader Large Object support

from cStringIO import StringIO
from tools     import PGLoader_Error
from options   import DRY_RUN, VERBOSE, DEBUG, PEDANTIC
from options   import INPUT_ENCODING

class ifx_lo:
    """ an Informix Large Object data file as given by UNLOAD """

    def __del__(self):
        """ close self.file on object destruction """
        self.file.close()

    def extract(self, rowid, field_nb, begin, length):
        """ extract given positionned data from Informix Clob out file, save
        them in a file and returns the filename where text is stored """

        # get stream position and data length
        begin  = long(begin, 16)
        length = long(length, 16)

        # get blob data
        self.file.seek(begin)
        try:
            content = self.file.read(length)
        except UnicodeDecodeError, e:
            # as of now, this is a fatal error
            print
            print 'Fatal error in clob file %s at position Ox%x' \
                  % (self.filename, begin)
            raise PGImport_Error, e

        return content

class ifx_clob(ifx_lo):
    """ Informix Text Large Object file """

    def __init__(self, filename, input_encoding):
        """ init a clob object  """
        self.file      = None
        self.filename  = filename

        if self.file is None:
            if input_encoding is not None:
                import codecs
                self.file = codecs.open(self.filename, 'r',
                                        encoding = input_encoding)
            else:
                self.file = open(self.filename, 'r')

            if VERBOSE:
                print "Notice: Opening informix clob file:", self.filename
                
class ifx_blob(ifx_lo):
    """ Informix Binary Large Object file """

    def __init__(self, filename, field_sep):
        """ init a clob object  """
        self.file      = None
        self.filename  = filename
        self.field_sep = field_sep # used by bytea_escape

        # some helpers for bytea escaping
        self.octals  = range(0, 32)
        self.octals += range(127, 256)

        if self.file is None:
            self.file = open(self.filename, 'rb')
            if VERBOSE:
                print "Notice: Opening informix blob file:", self.filename

    def bytea_escape(self, bitstring):
        """ escape chars from bitstring for PostgreSQL bytea input
        see http://www.postgresql.org/docs/8.1/static/datatype-binary.html
        """
        escaped = StringIO()
        bsize   = len(bitstring)
        pos     = 0

        while pos < bsize:
            c = bitstring[pos]
            o = ord(c)

            if o in self.octals or c == self.field_sep:
                # PostgreSQL wants octal numbers!
                escaped.write('\\%03o' % o)
            elif o == 39:
                escaped.write('\\047')
            elif o == 92:
                escaped.write('\\134')
            else:
                escaped.write(c)

            pos += 1

        r = escaped.getvalue()
        escaped.close()
        
        return r

    def extract(self, rowid, field_nb, begin, length):
        """ extract content, then bytea escape it """

        content = ifx_lo.extract(self, rowid, field_nb, begin, length)
        return self.bytea_escape(content)
            
