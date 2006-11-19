#! /usr/bin/env python
# -*- coding: ISO-8859-15 -*-
# Author: Dimitri Fontaine <dimitri@dalibo.com>

"""
PostgreSQL data import tool, aimed to replace and extands pgloader.

Important features :
 - CSV file format import using COPY
 - multi-line input file
 - configurable amount of rows per COPY instruction
 - large object to TEXT or BYTEA field handling
   (only informix blobs and clobs supported as of now)
 - trailing slash optionnal removal (support informix UNLOAD file format)
 - begin processing at any line in the file, by number or row id
 - dry-run option, to validate input reading without connecting to database
 - pedantic option, to stop processing on warning
 - reject log and reject data files: you can reprocess refused data later
 - COPY errors recovery via redoing COPY with half files until file is
   one line long, then reject log this line

Please read the fine manual page pg_import(1) for command line usage
(options) and configuration file format.
"""

import os, sys, os.path, time, codecs
from cStringIO import StringIO

import pgloader.options
import pgloader.tools

def parse_options():
    """ Parse given options """
    import ConfigParser
    from optparse import OptionParser

    usage  = "%prog [-c <config_filename>] Section [Section ...]"
    parser = OptionParser(usage = usage)
    
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

    parser.add_option("-n", "--dry-run", action = "store_true",
                      dest    = "dryrun",
                      default = False,
                      help    = "simulate operations, don't connect to the db")

    parser.add_option("-T", "--truncate", action = "store_true",
                      dest = "truncate",
                      default = False,
                      help    = "truncate tables before importing data")

    parser.add_option("-V", "--vacuum", action = "store_true",
                      dest = "vacuum",
                      default = False,
                      help    = "vacuum database after having imported data")

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

    (opts, args) = parser.parse_args()

    # check existence en read ability of config file
    if not os.path.exists(opts.config):
        print "Error: Configuration file %s does not exists" % opts.config
        print parser.format_help()
        sys.exit(1)

    if not os.access(opts.config, os.R_OK):
        print "Error: Can't read configuration file %s" % opts.config
        print parser.format_help()
        sys.exit(1)

    if opts.verbose:
        print 'Using %s configuration file' % opts.config

    if opts.fromcount != 0 and opts.fromid is not None:
        print "Error: Can't set both options fromcount (-F) AND fromid (-I)"
        sys.exit(1)

    pgloader.options.DRY_RUN    = opts.dryrun
    pgloader.options.DEBUG      = opts.debug
    # if debug, then verbose
    pgloader.options.VERBOSE    = opts.verbose or opts.debug
    pgloader.options.PEDANTIC   = opts.pedantic

    pgloader.options.TRUNCATE   = opts.truncate
    pgloader.options.VACUUM     = opts.vacuum
    
    pgloader.options.COUNT      = opts.count
    pgloader.options.FROM_COUNT = opts.fromcount
    pgloader.options.FROM_ID    = opts.fromid

    pgloader.options.INPUT_ENCODING = opts.encoding

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
        print "Error: Given file is not a configuration file"
        sys.exit(4)

    if not config.has_section(section):
        print "Error: Please provide a [%s] section" % section
        sys.exit(5)

    # load some options
    # this has to be done after command line parsing
    from pgloader.options  import DRY_RUN, VERBOSE, DEBUG, PEDANTIC
    from pgloader.options  import NULL, EMPTY_STRING

    if DRY_RUN:
        if VERBOSE:
            print "Notice: dry run mode, not connecting to database"
        return config, None

    try:
        from pgloader.db import db
        
        dbconn = db(config.get(section, 'host'),
                    config.getint(section, 'port'),
                    config.get(section, 'base'),
                    config.get(section, 'user'),
                    config.get(section, 'pass'),
                    connect = False)

        if config.has_option(section, 'client_encoding'):
            dbconn.client_encoding = config.get(section, 'client_encoding')

        if config.has_option(section, 'copy_every'):
            dbconn.copy_every = config.getint(section, 'copy_every')

        if config.has_option(section, 'commit_every'):
            dbconn.commit_every = config.getint(section, 'commit_every')

        if config.has_option(section, 'copy_delimiter'):
            dbconn.copy_sep = config.get(section, 'copy_delimiter')

        # Then there are null and empty_string optionnal parameters
        # They canbe overriden in specific table configuration
        if config.has_option(section, 'null'):
            pgloader.options.NULL = pgloader.tools.parse_config_string(
                config.get(section, 'null'))

        if config.has_option(section, 'empty_string'):
            pgloader.options.EMPTY_STRING = pgloader.tools.parse_config_string(
                config.get(section, 'empty_string'))

        # optionnal global newline_escapes
        if config.has_option(section, 'newline_escapes'):
            setting = pgloader.tools.parse_config_string(
                config.get(section, 'newline_escapes'))
            pgloader.options.NEWLINE_ESCAPES = setting
            
    except Exception, error:
        print "Error: Could not initialize PostgreSQL connection:"
        print error
        sys.exit(6)

    return config, dbconn

def myprint(l, line_prefix = "  ", cols = 78):
    """ pretty print list l elements """
    # some code for pretty print
    tmp = line_prefix
    for e in l:
        if len(tmp) + len(e) > cols:
            print tmp
            tmp = line_prefix
            
        if tmp != line_prefix: tmp += " "
        tmp += e
    print tmp

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

def load_data():
    """ read option line and configuration file, then process data
    import of given section, or all sections if no section is given on
    command line """

    # first parse command line options, and set pgloader.options values
    # accordingly
    conffile, args = parse_options()

    # now init db connection
    config, dbconn = parse_config(conffile)

    # load some pgloader package modules
    from pgloader.options  import DRY_RUN, VERBOSE, DEBUG, PEDANTIC, VACUUM
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

    if VERBOSE:
        print 'Will consider following sections:'
        myprint(sections)

    # we count time passed from now on
    begin = time.time()

    # we run through sorted section list
    sections.sort()
    for s in sections:
        try:
            pgloader = PGLoader(s, config, dbconn)
            pgloader.run()
            
            summary[s] = (pgloader.name,) + pgloader.summary()
        except PGLoader_Error, e:
            if e == '':
                print '[%s] Please correct previous errors' % s
            else:
                print
                print 'Error: %s' % e

            if PEDANTIC:
                pgloader.print_stats()

        except KeyboardInterrupt:
            print "Aborting on user demand (Interrupt)"

    # total duration
    td = time.time() - begin

    retcode = 0
    
    # print a pretty summary
    t= 'Table name        |    duration |    size |    updates |     errors '
    _= '===================================================================='

    tu = te = ts = 0 # total updates, errors, size
    if not DRY_RUN:
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
        
        t, d, u, e = summary[s]
        d = duration_pprint(d)

        if not DRY_RUN:
            sql = "select pg_total_relation_size(%s), " + \
                  "pg_size_pretty(pg_total_relation_size(%s));"
            cursor.execute(sql, [t, t])
            octets, s = cursor.fetchone()
            ts += octets
            
            if s[5:] == 'bytes': s = s[:-5] + ' B'
        else:
            s = '-'
        
        print '%-18s| %ss | %7s | %10d | %10d' % (t, d, s, u, e)

        tu += u
        te += e

        if e > 0:
            retcode += 1

    if s_ok > 1:
        td = duration_pprint(td)

        # pretty size
        cursor.execute("select pg_size_pretty(%s);", [ts])
        [ts] = cursor.fetchone()
        if ts[5:] == 'bytes': ts = ts[:-5] + ' B'
        
        print _
        print 'Total             | %ss | %7s | %10d | %10d' % (td, ts, tu, te)

        if not DRY_RUN:
            cursor.close()

    print
    if VACUUM and not DRY_RUN:
        print 'vacuumdb... '
        try:
            dbconn.vacuum()
        except KeyboardInterrupt:
            pass    

    return retcode

if __name__ == "__main__":
    sys.exit(load_data())
    
