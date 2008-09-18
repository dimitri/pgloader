#! /usr/bin/env python
# Author: Dimitri Fontaine <dim@tapoueh.org>

"""
PostgreSQL data import tool, see included man page.
"""

import os, sys, os.path, time, codecs, logging, threading
from cStringIO import StringIO

import pgloader.options
import pgloader.tools
import pgloader.logger
from pgloader.tools import PGLoader_Error

def parse_options():
    """ Parse given options """
    import ConfigParser
    from optparse import OptionParser

    usage  = "%prog [-c <config_filename>] Section [Section ...]"
    parser = OptionParser(usage = usage)
    
    parser.add_option("--version", action = "store_true",
                      dest    = "version",
                      default = False,
                      help    = "show pgloader version")

    parser.add_option("-c", "--config", dest = "config",
                      default = "pgloader.conf",
                      help    = "configuration file, defauts to pgloader.conf")

    parser.add_option("-p", "--pedantic", action = "store_true",
                      dest    = "pedantic",
                      default = False,
                      help    = "pedantic mode, stop processing on warning")

    parser.add_option("-d", "--debug", action = "store_true",
                      dest    = "debug",
                      default = False,
                      help    = "add some debug information (a lot of)")

    parser.add_option("-v", "--verbose", action = "store_true",
                      dest    = "verbose",
                      default = False,
                      help    = "be verbose and about processing progress")

    parser.add_option("-q", "--quiet", action = "store_true",
                      dest    = "quiet",
                      default = False,
                      help    = "be quiet, only print out errors")

    parser.add_option("-l", "--level", dest = "loglevel",
                      default = None,
                      help    = "loglevel to use: ERROR, WARNING, INFO, DEBUG")

    parser.add_option("-L", "--logfile", dest = "logfile",
                      default = "/tmp/pgloader.log",
                      help    = "log file, defauts to /tmp/pgloader.log")

    parser.add_option("-s", "--summary", action = "store_true",
                      dest    = "summary",
                      default = False,
                      help    = "print a summary")

    parser.add_option("-n", "--dry-run", action = "store_true",
                      dest    = "dryrun",
                      default = False,
                      help    = "simulate operations, don't connect to the db")

    parser.add_option("-T", "--truncate", action = "store_true",
                      dest    = "truncate",
                      default = False,
                      help    = "truncate tables before importing data")

    parser.add_option("-D", "--disable-triggers", action = "store_true",
                      dest    = "triggers",
                      default = False,
                      help    = "Disable triggers before loading, Enable them again after")

    parser.add_option("-V", "--vacuum", action = "store_true",
                      dest = "vacuum",
                      default = False,
                      help    = "vacuum tables after data loading")

    parser.add_option("-C", "--count", dest = "count",
                      default = None, type = "int",
                      help    = "number of input lines to process")
    
    parser.add_option("-F", "--from", dest = "fromcount",
                      default = 0, type = "int",
                      help    = "number of input lines to skip")

    parser.add_option("-I", "--from-id", dest = "fromid",
                      default = None,
                      help    = "wait for given id on input to begin")

    parser.add_option("-E", "--encoding", dest = "encoding",
                      default = None,
                      help    = "input files encoding")

    parser.add_option("-R", "--reformat_path", dest = "reformat_path",
                      default = None,
                      help    = "PATH where to find reformat python modules")

    parser.add_option("-1", "--psycopg1", action = "store_true",
                      dest    = "psycopg1",
                      default = False,
                      help    = "Force usage of psycopg1")

    parser.add_option("-2", "--psycopg2", action = "store_true",
                      dest    = "psycopg2",
                      default = False,
                      help    = "Force usage of psycopg2")

    parser.add_option("--psycopg-version", dest = "psycopg_version",
                      default = None,
                      help    = "Force usage of given version of psycopg")

    (opts, args) = parser.parse_args()

    if opts.version:
        print "PGLoader version %s" % pgloader.options.PGLOADER_VERSION
        sys.exit(0)

    # check existence and read ability of config file
    if not os.path.exists(opts.config):
        print >>sys.stderr, \
              "Error: Configuration file %s does not exists" % opts.config
        print >>sys.stderr, parser.format_help()
        sys.exit(1)

    if not os.access(opts.config, os.R_OK):
        print >>sys.stderr, \
              "Error: Can't read configuration file %s" % opts.config
        print >>sys.stderr, parser.format_help()
        sys.exit(1)

    if opts.fromcount != 0 and opts.fromid is not None:
        print >>sys.stderr, \
              "Error: Can't set both options fromcount (-F) AND fromid (-I)"
        sys.exit(1)

    if opts.quiet and (opts.verbose or opts.debug):
        print >>sys.stderr, \
              "Error: Can't be verbose and quiet at the same time!"
        sys.exit(1)

    psyco_opts = 0
    for opt in [opts.psycopg1, opts.psycopg2, opts.psycopg_version]:
        if opt:
            psyco_opts += 1

    if psyco_opts > 1:
        print >>sys.stderr, \
              "Error: please use only one of the psycopg options"
        sys.exit(1)

    if opts.psycopg_version is not None:
        if opts.psycopg_version in ("1", "2"):
            opts.psycopg_version = int(opts.psycopg_version)

        else:
            print >>sys.stderr, \
                  "Error: psycopg_version can only be set to either 1 or 2"
            sys.exit(1)
        
    # if debug, then verbose
    if opts.debug:
        opts.verbose = True

    pgloader.options.DRY_RUN    = opts.dryrun
    pgloader.options.DEBUG      = opts.debug
    pgloader.options.VERBOSE    = opts.verbose
    pgloader.options.QUIET      = opts.quiet
    pgloader.options.SUMMARY    = opts.summary    
    pgloader.options.PEDANTIC   = opts.pedantic

    pgloader.options.TRUNCATE   = opts.truncate
    pgloader.options.VACUUM     = opts.vacuum
    pgloader.options.TRIGGERS   = opts.triggers
    
    pgloader.options.COUNT      = opts.count
    pgloader.options.FROM_COUNT = opts.fromcount
    pgloader.options.FROM_ID    = opts.fromid

    pgloader.options.INPUT_ENCODING = opts.encoding

    if opts.reformat_path:
        pgloader.options.REFORMAT_PATH = opts.reformat_path

    pgloader.options.LOG_FILE = opts.logfile

    if opts.loglevel:
        loglevel = pgloader.logger.level(opts.loglevel)
        pgloader.options.CLIENT_MIN_MESSAGES = loglevel
    elif opts.debug:
        pgloader.options.CLIENT_MIN_MESSAGES = logging.DEBUG
    elif opts.verbose:
        pgloader.options.CLIENT_MIN_MESSAGES = logging.INFO
    elif opts.quiet:
        pgloader.options.CLIENT_MIN_MESSAGES = logging.ERROR


    if opts.psycopg1:
        pgloader.options.PSYCOPG_VERSION = 1
    elif opts.psycopg2:
        pgloader.options.PSYCOPG_VERSION = 2
    else:
        pgloader.options.PSYCOPG_VERSION = opts.psycopg_version

    return opts.config, args

def parse_config(conffile):
    """ Parse the configuration file """
    section = 'pgsql'

    # Now read pgsql configuration section
    import ConfigParser
    config = ConfigParser.ConfigParser()

    try:
        config.read(conffile)
    except:
        print >>sys.stderr, "Error: Given file is not a configuration file"
        sys.exit(4)

    if not config.has_section(section):
        print >>sys.stderr, "Error: Please provide a [%s] section" % section
        sys.exit(5)

    # load some options
    # this has to be done after command line parsing
    from pgloader.options  import DRY_RUN, VERBOSE, DEBUG, PEDANTIC
    from pgloader.options  import NULL, EMPTY_STRING
    from pgloader.options  import CLIENT_MIN_MESSAGES, LOG_FILE
    from pgloader.tools    import check_dirname

    # first read the logging configuration
    if not CLIENT_MIN_MESSAGES:
        if config.has_option(section, 'client_min_messages'):
            cmm = config.get(section, 'client_min_messages')
            pgloader.options.CLIENT_MIN_MESSAGES = pgloader.logger.level(cmm)
        else:
            # CLIENT_MIN_MESSAGES has not been set at all
            pgloader.options.CLIENT_MIN_MESSAGES = logging.INFO

    if config.has_option(section, 'log_min_messages'):
        lmm = config.get(section, 'log_min_messages')
        pgloader.options.LOG_MIN_MESSAGES = pgloader.logger.level(lmm)
    else:
        pgloader.options.LOG_MIN_MESSAGES = logging.INFO

    if config.has_option(section, 'log_file'):
        # don't overload the command line -L option if given
        if not pgloader.options.LOG_FILE:
            pgloader.options.LOG_FILE = config.get(section, 'log_file')

    if pgloader.options.LOG_FILE:
        ok, logdir_mesg = check_dirname(pgloader.options.LOG_FILE)
        if not ok:
            # force default setting
            pgloader.options.LOG_FILE = pgloader.options.DEFAULT_LOG_FILE

    try:
        log = pgloader.logger.init(pgloader.options.CLIENT_MIN_MESSAGES,
                                   pgloader.options.LOG_MIN_MESSAGES,
                                   pgloader.options.LOG_FILE)
    except PGLoader_Error, e:
        try:
            log = pgloader.logger.init(pgloader.options.CLIENT_MIN_MESSAGES,
                                       pgloader.options.LOG_MIN_MESSAGES,
                                       pgloader.options.DEFAULT_LOG_FILE)

            log.warning(e)
            log.warning("Using default logfile %s",
                        pgloader.options.DEFAULT_LOG_FILE)
        except PGLoader_Error, e:
            print e
            sys.exit(8)
        
    pgloader.logger.log = log

    log.info("Logger initialized")
    if logdir_mesg:
        log.error(logdir_mesg)
        log.error("Default logfile %s has been used instead",
                  pgloader.options.LOG_FILE)

    if config.has_option(section, 'input_encoding'):
        input_encoding = pgloader.tools.parse_config_string(
            config.get(section, 'input_encoding'))
        pgloader.options.INPUT_ENCODING = input_encoding

    # optionnal global newline_escapes
    if config.has_option(section, 'newline_escapes'):
        setting = pgloader.tools.parse_config_string(
            config.get(section, 'newline_escapes'))
        pgloader.options.NEWLINE_ESCAPES = setting

    # Then there are null and empty_string optionnal parameters
    # They canbe overriden in specific table configuration
    if config.has_option(section, 'null'):
        pgloader.options.NULL = pgloader.tools.parse_config_string(
            config.get(section, 'null'))

    if config.has_option(section, 'empty_string'):
        pgloader.options.EMPTY_STRING = pgloader.tools.parse_config_string(
            config.get(section, 'empty_string'))

    if config.has_option(section, 'reformat_path'):
        # command line value is prefered to config format one
        if not pgloader.options.REFORMAT_PATH:
            rpath = config.get(section, 'reformat_path')
            pgloader.options.REFORMAT_PATH = rpath

    if config.has_option(section, 'max_parallel_sections'):
        mps = config.getint(section, 'max_parallel_sections')
        pgloader.options.MAX_PARALLEL_SECTIONS = mps

    return config

def myprint(l, line_prefix = "  ", cols = 78):
    """ pretty print list l elements """
    # some code for pretty print
    lines = []
    
    tmp = line_prefix
    for e in l:
        if len(tmp) + len(e) > cols:
            lines.append(tmp)
            tmp = line_prefix
            
        if tmp != line_prefix: tmp += " "
        tmp += e
        
    lines.append(tmp)

    return lines

def duration_pprint(duration):
    """ pretty print duration (human readable information) """
    if duration > 3600:
        h  = int(duration / 3600)
        m  = int((duration - 3600 * h) / 60)
        s  = duration - 3600 * h - 60 * m + 0.5
        return '%2dh%02dm%03.1f' % (h, m, s)
    
    elif duration > 60:
        m  = int(duration / 60)
        s  = duration - 60 * m
        return ' %02dm%06.3f' % (m, s)
        
    else:
        return '%10.3f' % duration

def print_summary(dbconn, sections, summary, td):
    """ print a pretty summary """
    from pgloader.options  import VERBOSE, DEBUG, QUIET, SUMMARY
    from pgloader.options  import DRY_RUN, PEDANTIC, VACUUM
    from pgloader.pgloader import PGLoader

    retcode = 0

    t= 'Table name        |    duration |    size |  copy rows |     errors '
    _= '===================================================================='

    tu = te = ts = 0 # total updates, errors, size

    if False and not DRY_RUN:
        dbconn.reset()
        cursor = dbconn.dbconn.cursor()

    s_ok = 0
    for s in sections:
        if s not in summary:
            continue

        s_ok += 1
        if s_ok == 1:
            # print pretty sumary header now
            print
            print t
            print _

        if summary[s]:
            t, d, u, e = summary[s]
            d = duration_pprint(d)
        else:
            t = s
            d = '%9s ' % '-'
            u = e = 0

        if False and not DRY_RUN:
            sql = "select pg_total_relation_size(%s), " + \
                  "pg_size_pretty(pg_total_relation_size(%s));"
            cursor.execute(sql, [t, t])
            octets, sp = cursor.fetchone()
            ts += octets

            if sp[5:] == 'bytes': sp = sp[:-5] + ' B'
        else:
            sp = '-'

        tn = s
        if len(tn) > 18:
            tn = s[0:15] + "..."

        print '%-18s| %ss | %7s | %10d | %10d' % (tn, d, sp, u, e)

        tu += u
        te += e

        if e > 0:
            retcode += 1

    if s_ok > 1:
        td = duration_pprint(td)

        # pretty size
        if False and not DRY_RUN:
            cursor.execute("select pg_size_pretty(%s);", [ts])
            [ts] = cursor.fetchone()
            if ts[5:] == 'bytes': ts = ts[:-5] + ' B'
        else:
            ts = '-'

        print _
        print 'Total             | %ss | %7s | %10d | %10d' \
              % (td, ts, tu, te)

        if False and not DRY_RUN:
            cursor.close()

    return retcode

def load_data():
    """ read option line and configuration file, then process data
    import of given section, or all sections if no section is given on
    command line """

    # first parse command line options, and set pgloader.options values
    # accordingly
    conffile, args = parse_options()

    # now init db connection
    config = parse_config(conffile)

    from pgloader.logger  import log
    from pgloader.tools   import read_path, check_path
    from pgloader.options import VERBOSE
    
    import pgloader.options
    if pgloader.options.REFORMAT_PATH:
        rpath  = read_path(pgloader.options.REFORMAT_PATH, log, check = False)
        crpath = check_path(rpath, log)
    else:
        rpath  = crpath  = None

    if not crpath:
        if rpath:
            # don't check same path entries twice
        
            default_rpath = set(crpath) \
                            - set(pgloader.options.DEFAULT_REFORMAT_PATH)
        else:
            default_rpath = pgloader.options.DEFAULT_REFORMAT_PATH
        
        pgloader.options.REFORMAT_PATH = check_path(default_rpath, log)
    else:
        pgloader.options.REFORMAT_PATH = rpath

    log.info('Reformat path is %s', pgloader.options.REFORMAT_PATH)

    # load some pgloader package modules
    from pgloader.options  import VERBOSE, DEBUG, QUIET, SUMMARY
    from pgloader.options  import DRY_RUN, PEDANTIC, VACUUM
    from pgloader.options  import MAX_PARALLEL_SECTIONS
    from pgloader.pgloader import PGLoader
    from pgloader.tools    import PGLoader_Error

    sections = []
    summary  = {}

    # args are meant to be configuration sections
    if len(args) > 0:
        for s in args:
            if config.has_section(s):
                sections.append(s)

    else:
        for s in config.sections():
            if s != 'pgsql':
                sections.append(s)

    log.info('Will consider following sections:')
    for line in myprint(sections):
        log.info(line)

    # we count time passed from now on
    begin = time.time()

    # we run through sorted section list
    sections.sort()

    threads  = {}
    started  = {}
    finished = {}
    current  = 0
    interrupted = False

    max_running = MAX_PARALLEL_SECTIONS
    if max_running == -1:
        max_running = len(sections)

    sem = threading.BoundedSemaphore(max_running)
    
    while current < len(sections):
        s = sections[current]

        try:
            loader      = None
            summary [s] = []
            started [s] = threading.Event()
            finished[s] = threading.Event()

            try:
                loader = PGLoader(s, config, sem,
                                  (started[s], finished[s]), summary[s])
            except PGLoader_Error, e:
                # could not initialize properly this loader, don't
                # ever wait for it
                started[s] .set()
                finished[s].set()
                log.error(e)

            except IOError, e:
                # No space left on device?  can't log it
                break

            if loader:
                if not loader.template:
                    filename       = loader.filename
                    input_encoding = loader.input_encoding
                    threads[s]     = loader

                    # .start() will sem.aquire(), so we won't have more
                    # than max_running threads running at any time.
                    log.debug("Starting a thread for %s" % s)
                    threads[s].start()
                else:
                    log.info("Skipping section %s, which is a template" % s)

                    for d in (summary, started, finished):
                        d.pop(s)

        except PGLoader_Error, e:
            if e == '':
                log.error('[%s] Please correct previous errors' % s)
            else:
                log.error('%s' % e)

            if PEDANTIC:
                # was: threads[s].print_stats()
                # but now thread[s] is no more alive
                pass

        except UnicodeDecodeError, e:
            log.error("can't open '%s' with given input encoding '%s'" \
                               % (filename, input_encoding))
                                    
        except KeyboardInterrupt:
            interrupted = True
            log.warning("Aborting on user demand (Interrupt)")
            break

        except IOError, e:
            # typically, No Space Left On Device, can't report nor continue
            break

        current += 1

    # get sure each thread is started, then each one is done
    from pgloader.tools import check_events

    check_events(started, log, "is running")
    log.info("All threads are started, wait for them to terminate")
    check_events(finished, log, "processing is over")

    # total duration
    td = time.time() - begin
    retcode = 0

    if SUMMARY and not interrupted:
        try:
            retcode = print_summary(None, sections, summary, td)
            print
        except PGLoader_Error, e:
            log.error("Can't print summary: %s" % e)

        except KeyboardInterrupt:
            pass

    return retcode

if __name__ == "__main__":
    try:
        ret = load_data()
    except Exception, e:
        sys.stderr.write(str(e) + '\n')
        sys.exit(1)

    except IOError, e:
        sys.stderr.write(str(e) + '\n')
        sys.exit(1)

    except KeyboardInterrupt, e:
        sys.stderr.write(str(e) + '\n')
        sys.exit(1)
        
    sys.exit(ret)
    
