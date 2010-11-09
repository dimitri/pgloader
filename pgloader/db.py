# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader database connection handling
# COPY dichotomy on error

import os, sys, os.path, time, codecs, logging
from cStringIO import StringIO
from tempfile import gettempdir

from options import DRY_RUN, PEDANTIC, CLIENT_MIN_MESSAGES
from options import TRUNCATE, VACUUM
from options import INPUT_ENCODING, PG_CLIENT_ENCODING, DATESTYLE
from options import COPY_SEP, FIELD_SEP, CLOB_SEP, NULL, EMPTY_STRING
from options import PSYCOPG_VERSION
from options import PG_OPTIONS

from tools   import PGLoader_Error
from logger  import log

log.debug('Preferred psycopg version is %s' % PSYCOPG_VERSION)

if PSYCOPG_VERSION is None:
    # legacy import behavior
    log.debug('Trying psycopg2 then psycopg')
    try:
        import psycopg2.psycopg1 as psycopg
    except ImportError:
        log.info('No psycopg2 module found, trying psycopg1')

        try:
            import psycopg
        except ImportError, e:
            log.fatal('No psycopg module found')
            raise PGLoader_Error, e

elif PSYCOPG_VERSION == 1:
    try:
        log.info("Loading psycopg 1")
        import psycopg
    except ImportError, e:
        log.fatal("Can't load version %d of psycopg" % PSYCOPG_VERSION)
        raise PGLoader_Error, e
        
elif PSYCOPG_VERSION == 2:
    try:
        log.info("Loading psycopg 2")
        import psycopg2.psycopg1 as psycopg
    except ImportError, e:
        log.fatal("Can't load version %d of psycopg" % PSYCOPG_VERSION)
        raise PGLoader_Error, e

class db:
    """ a db connexion and utility class """
    def __init__(self,
                 dsn,
                 client_encoding = PG_CLIENT_ENCODING,
                 copy_every = 10000, commit_every = 1000, connect = True):
        """ Connects to the specified database """
        self.log     = log
        self.dbconn  = None
        self.dsn     = dsn

        # those parameters can be overwritten after db init
        # here's their default values
        self.copy_sep        = COPY_SEP
        self.copy_every      = copy_every
        self.commit_every    = commit_every
        self.null            = NULL
        self.empty_string    = EMPTY_STRING
        self.pg_options      = {}

        # this allows to specify configuration has columns = *
        # when true, we don't include column list in COPY statements
        self.all_cols = None

        if connect:
            self.reset()

    def __del__(self):
        """ db object destructor, we have to close the db connection """
        if self.dbconn is None:
            return

        if self.running_commands > 0:
            self.dbconn.commit()
            self.commits       += 1
            self.commited_rows += self.running_commands

        self.close()

    def close(self):
        """ close self.dbconn PostgreSQL connection """
        if self.dbconn is not None:
            try:
                self.log.info('closing current database connection')
            except IOError, e:
                # Ignore no space left on device...
                pass

            try:
                self.dbconn.close()
            except InterfaceError, e:
                # Ignore connection already closed
                pass
            self.dbconn = None

    def set_pg_options(self):
        """ set pg_options """
        for opt, val in self.pg_options.items():
            self.log.debug('Setting %s to %s', opt, val)
        
            sql = 'set session %s to %%s' % opt
            cursor = self.dbconn.cursor()
            try:
                cursor.execute(sql, [val])
            except (psycopg.ProgrammingError, psycopg.DataError), e:
                raise PGLoader_Error, e
            cursor.close()

    def get_all_columns(self, tablename):
        """ select the columns name list from catalog """

        if tablename.find('.') == -1:
            schemaname = 'public'
        else:
            try:
                schemaname, tablename = tablename.split('.')
            except ValueError, e:
                self.log.warning("db.get_all_columns: " + \
                                 "%s has more than one '.' separator" \
                                 % tablename)
                raise PGLoader_Error, e

        sql = """
  SELECT attname, attnum
    FROM pg_attribute
   WHERE attrelid = (SELECT oid
                       FROM pg_class
                      WHERE relname = %s
                            AND relnamespace = (SELECT oid
                                                  FROM pg_namespace
                                                 WHERE nspname = %s)
                    )
          AND attnum > 0 AND NOT attisdropped
ORDER BY attnum
"""
        self.log.debug("get_all_columns: %s %s %s" % (tablename, schemaname, sql))

        columns = []

        have_to_connect = self.dbconn is None
        if have_to_connect:
            self.reset()
            
        cursor  = self.dbconn.cursor()
        try:
            cursor.execute(sql, [tablename, schemaname])

            for row in cursor.fetchall():
                columns.append(row)

        except psycopg.ProgrammingError, e:
            raise PGLoader_Error, e

        cursor.close()

        if have_to_connect:
            self.close()

        return columns

    def reset(self):
        """ reset internal counters and open a new database connection """
        self.buffer            = None
        self.copy              = None # flag set to True when copy is called
        self.errors            = 0
        self.commits           = 0
        self.commited_rows     = 0
        self.running_commands  = 0
        self.last_commit_time  = time.time()
        self.first_commit_time = self.last_commit_time
        self.partial_coldef    = None

        try:
            self.close()
            self.log.debug('Debug: connecting to dns %s', self.dsn)

            self.dbconn = psycopg.connect(self.dsn)
            self.set_pg_options()
            
        except psycopg.OperationalError, e:
            # e.g. too many connections
            self.log.error(e)
            raise PGLoader_Error, "Can't connect to database"

    def print_stats(self):
        """ output some stats about recent activity """
        d = time.time() - self.first_commit_time
        u = self.commited_rows
        c = self.commits
        self.log.info(" %d rows copied in %d commits took %5.3f seconds", u, c, d)

        if self.errors > 0:
            self.log.error("%d database errors occured", self.errors)
            if self.copy and not VACUUM:
                self.log.info("Please VACUUM your database to recover space")
        else:
            if u > 0:
                self.log.info("No database error occured")
        return

    def is_null(self, value):
        """ return true if value is null, per configuration """
        return value == self.null

    def is_empty(self, value):
        """ return true if value is empty, per configuration """
        return value == self.empty_string

    def truncate(self, table):
        """ issue an SQL TRUNCATE TABLE on given table """
        if DRY_RUN:
            self.log.info("Won't truncate tables on dry-run mode")
            return
        
        sql = "TRUNCATE TABLE %s;" % table

        self.log.info('%s' % sql)
        
        try:
            cursor = self.dbconn.cursor()
            cursor.execute(sql)
            self.dbconn.commit()
        except Exception, error:
            self.log.error(error)
            raise PGLoader_Error, "Couldn't TRUNCATE table %s" % table
    
    def vacuum(self, table):
        """ issue VACUUM ANALYZE table """
        if DRY_RUN:
            self.log.info('no vacuum in dry-run mode')
            return -1

        sql = "VACUUM ANALYZE %s;" % table

        self.log.info('%s' % sql)
        
        try:
            cursor = self.dbconn.cursor()
            cursor.execute(sql)
            self.dbconn.commit()
        except Exception, error:
            self.log.error(error)
            raise PGLoader_Error, "Couldn't VACUUM table %s" % table

    def disable_triggers(self, table):
        """ issue ALTER TABLE table DISABLE TRIGGER ALL """
        if DRY_RUN:
            self.log.info("Won't disable triggers on dry-run mode")
            return
        
        sql = "ALTER TABLE %s DISABLE TRIGGER ALL;" % table

        self.log.info('%s' % sql)
        
        try:
            cursor = self.dbconn.cursor()
            cursor.execute(sql)
            self.dbconn.commit()
        except Exception, error:
            self.log.error(error)
            raise PGLoader_Error, "Couldn't DISABLE TRIGGERS on table %s" % table

    def enable_triggers(self, table):
        """ issue ALTER TABLE table ENABLE TRIGGER ALL """
        if DRY_RUN:
            self.log.info("Won't enable triggers on dry-run mode")
            return
        
        sql = "ALTER TABLE %s ENABLE TRIGGER ALL;" % table

        self.log.info('%s' % sql)
        
        try:
            cursor = self.dbconn.cursor()
            cursor.execute(sql)
            self.dbconn.commit()
        except Exception, error:
            self.log.error(error)
            raise PGLoader_Error, "Couldn't ENABLE TRIGGERS on table %s" % table

    
    def insert_blob(self, table, index, rowids,
                    blob_cname, data, btype,
                    input_line, reject):
        """ insert the given blob content into postgresql table

        return True on success, False on error
        """
        ok  = True
        sql = ""

        if btype == 'ifx_clob':
            data = data.replace("'", "\\'")
            sql = "UPDATE %s SET %s = %%s WHERE " % (table, blob_cname)

        elif btype == 'ifx_blob':
            data = data.tostring()
            sql = "UPDATE %s SET %s = %%s::bytea WHERE " % (table, blob_cname)

        values = [data]
        
        ##
        # Add a WHERE clause for each index
        first = True
        for name, col in index:
            if not first: sql  += " AND "
            else:         first = False
            
            sql += "%s = %%s" % name
            values.append(rowids[name])
        sql += ";"

        self.log.debug('%s' % sql)

        try:
            cursor = self.dbconn.cursor()
            cursor.execute(sql, values)

            # if execute raise an exception, don't count it as a
            # running command (waiting a commit)
            self.running_commands += 1

            if VERBOSE:
                str_rowids = ""
                for i,v in rowids.items():
                    if str_rowids != "": str_rowids += ", "
                    str_rowids += "%s:%s" % (i, v)
                self.log.debug('%s %s %s %6do', 
                          table, str_rowids, blob_cname, len(data))

            if self.running_commands == self.commit_every:
                now = time.time()
                self.dbconn.commit()
                
                self.commits += 1
                duration      = now - self.last_commit_time
                self.last_commit_time = now

                self.log.info("commit %d: %d updates in %5.3fs",
                         self.commits, self.running_commands, duration)

                self.commited_rows   += self.running_commands
                self.running_commands = 1

        except KeyboardInterrupt, error:
            # C-c was pressed, please stop processing
            self.dbconn.commit()
            raise PGLoader_Error, "Aborting on user demand (Interrupt)"

        except Exception, e:
            self.dbconn.commit()
            # don't use self.commited_rows here, it's only updated
            # after a commit
            self.log.error("update %d rejected: commiting (read log file %s)",
                      self.commits * self.commit_every + self.running_commands,
                      reject.reject_log)

            reject.log(str(e), input_line)
            self.errors += 1
            ok = False

        return ok

    def save_copy_buffer(self, tablename, debug = False):
        """ save copy buffer to a temporary file for further inspection """
        import tempfile
        (f, n) = tempfile.mkstemp(prefix='%s.' % tablename,
                                  suffix='.pgloader', dir=gettempdir())
        os.write(f, self.buffer.getvalue())
        os.close(f)
        if debug:
            self.log.debug("COPY data buffer saved in %s" % n)
        else:
            self.log.warning("COPY data buffer saved in %s" % n)
        return n

    def copy_from(self, table, columnlist,
                  columns, input_line, offsets,
                  reject, EOF = False):
        """ Generate some COPY SQL for PostgreSQL """
        ok = True
        if not self.copy: self.copy = True

        if EOF or self.running_commands == self.copy_every \
               and self.buffer is not None:
            # time to copy data to PostgreSQL table

            if self.buffer is None:
                self.log.warning("no data to COPY")
                return False
            
            ##
            # build the table colomns specs from parameters
            # ie. we always issue COPY table (col1, col2, ..., coln) commands
            tablename = table
            if self.all_cols:
                table = table
            else:
                table = "%s (%s) " % (table, ", ".join(columnlist))

            self.log.debug("COPY will use table definition: '%s'" % table)
                
            if CLIENT_MIN_MESSAGES <= logging.DEBUG:
                self.save_copy_buffer(tablename, debug = True)

            self.buffer.seek(0)
            now = time.time()
                
            try:
                cursor = self.dbconn.cursor()
                r = self.cursor_copy_from(cursor, self.buffer, table, self.copy_sep)
                self.dbconn.commit()

                self.commits         += 1
                duration              = now - self.last_commit_time
                self.last_commit_time = now

                self.log.info("COPY %d: %d rows copied in %5.3fs",
                         self.commits, self.running_commands, duration)

                # prepare next run
                self.buffer.close()
                self.buffer = None
                self.commited_rows   += self.running_commands
                self.running_commands = 0

            except (psycopg.ProgrammingError,
                    psycopg.DatabaseError), error:
                # rollback current transaction
                self.dbconn.rollback()

                self.log.warning('COPY error, trying to find on which line')
                if CLIENT_MIN_MESSAGES > logging.DEBUG:
                    # in DEBUG mode, copy buffer has already been saved
                    # to file
                    self.save_copy_buffer(tablename)

                # copy recovery process
                now = time.time()
                c, ok, ko = self.copy_from_buff(table, 
                                                self.buffer, 
                                                self.first_offsets,
                                                self.running_commands, 
                                                reject)

                duration              = now - self.last_commit_time
                self.commits         += c
                self.last_commit_time = now
                self.commited_rows   += ok
                self.errors          += ko

                self.log.warning('COPY error recovery done (%d/%d) in %5.3fs',
                            ko, ok, duration)

                # commit this transaction
                self.dbconn.commit()

                # recovery process has closed the buffer
                self.buffer = None
                self.running_commands = 0

        # prepare next run
        if self.buffer is None:
            self.buffer = StringIO()
            self.first_offsets = offsets

        self.prepare_copy_data(columns, input_line, reject)
        self.running_commands += 1
        return ok

    def copy_from_buff(self, table, buff, first_offsets, count, reject):
        """ If copy returned an error, try to detect wrong input line(s) """

        if count == 1:
            msg = self.copy_error_message(first_offsets, 0)
            reject.log(msg, buff.getvalue())
            buff.close()
            self.log.debug('found one more line in error')

            # returns commits, ok, ko
            return 0, 0, 1
        
        ##
        # Dichotomy
        # we cut the buffer into two buffers, try to copy from them
        a = StringIO()
        b = StringIO()
        n = 0
        m = count / 2

        # return values, copied lines and errors
        commits = ok = ko = 0 

        buff.seek(0)
        for line in buff.readlines():
            if n < m:
                a.write(line)
            else:
                b.write(line)
            n += 1

        # we don't need no more orgininal buff
        buff.close()

        self.log.debug('Trying to find errors, dividing %d lines in %d and %d',
                  count, m, n-m)

        # now we have two buffers to copy to PostgreSQL database
        cursor = self.dbconn.cursor()
        for (x, xcount) in [(a, m), (b, n-m)]:
            try:
                x.seek(0)
                self.cursor_copy_from(cursor, x, table, self.copy_sep)
                self.dbconn.commit()
                x.close()

                self.log.debug("COPY ERROR handling progress: %d rows copied",
                          xcount)

                x.close()
                commits += 1
                ok += xcount

            except Exception, error:
                self.dbconn.commit()
                
                # if a is only one line long, reject this line
                if xcount == 1:
                    ko += 1

                    linecount = 0
                    if x == b:
                        linecount += m

                    msg = self.copy_error_message(first_offsets, linecount)
                    msg += '\nCOPY error: %s' % error
                    reject.log(msg, x.getvalue())
                    self.log.debug('Notice: found one more line in error')
                    self.log.debug(x.getvalue())

                else:
                    new_offsets = first_offsets
                    if x == b:
                        new_offsets = [m + o for o in first_offsets]

                    _c, _o, _k = self.copy_from_buff(table, 
                                                     x,
                                                     new_offsets,
                                                     xcount, reject)
                    commits += _c
                    ok += _o
                    ko += _k

        return commits, ok, ko

    def cursor_copy_from(self, cursor, buffer, table, delimiter):
        """ call psycopg copy command, in expert mode if available """
        
        if hasattr(cursor, 'copy_expert'):
            self.log.debug("using copy_expert")
            sql = "COPY %s FROM STDOUT WITH DELIMITER '%s'" % (table, delimiter)
            return cursor.copy_expert(sql, buffer)
        else:
            return cursor.copy_from(buffer, table, delimiter)


    def prepare_copy_data(self, columns, input_line, reject):
        """ add a data line to copy buffer """
        if columns is not None:
            first_col = True

            for c in columns:
                # default text format COPY delimiter
                if not first_col: self.buffer.write(self.copy_sep)
                else:             first_col = False

                if self.is_null(c):
                    # null column value: \N
                    self.buffer.write('\N')

                elif self.is_empty(c):
                    # empty string has been read
                    self.log.debug("empty string read: '%s'" % c)
                    self.buffer.write('')

                else:
                    # for a list of chars to replace, please have a look to
                    # http://www.postgresql.org/docs/8.1/static/sql-copy.html
                    if INPUT_ENCODING is not None:
                        try:
                            c = c.encode(INPUT_ENCODING)
                        except UnicodeDecodeError, e:
                            reject.log(['Codec error', str(e)], input_line)

                    # in _split_line we remove delimiter escaping
                    # in order for backslash escaping not to de-escape it
                    # we then have to escape delimiters explicitely now
                    for orig, escaped in [('\\', '\\\\'),
                                          (self.copy_sep,
                                           '\\%s' % self.copy_sep),
                                          ('\b', '\\b'),
                                          ('\f', '\\f'),
                                          ('\n', '\\n'),
                                          ('\r', '\\r'),
                                          ('\t', '\\t'),
                                          ('\v', '\\v')]:
                        try:
                            c = c.replace(orig, escaped)
                        except TypeError, e:
                            self.log.error("db.prepare_copy_data columns %s"    % str(columns))
                            self.log.error("db.prepare_copy_data input_line %s" % str(input_line))
                            self.log.error("TypeError: '%s'.replace(%s, %s)"    % (c, orig, escaped))
                            raise PGLoader_Error, e

                    self.buffer.write(c)

            # end of row, \n
            self.buffer.write('\n')


    def copy_error_message(self, offsets, error_buff_offset):
        """ Build the COPY pgloader error message with line numbers """
        msg = 'COPY error on line'
        if type(offsets) == type([]):
            if len(offsets) > 1:
                msg += 's'
            msg += 's %s' % ' '.join([str(x + error_buff_offset)
                                      for x in offsets])
                
        else:
            # offsets is (start position (byte, ftell()), [line, numbers])
            msg += ' '

        return msg
