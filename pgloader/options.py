# Author: Dimitri Fontaine <dimitri@dalibo.com>
#
# Some common options, for each module to get them

PGLOADER_VERSION = '2.3.0~dev'

INPUT_ENCODING     = None
PG_CLIENT_ENCODING = 'latin9'
DATESTYLE          = None

COPY_SEP     = None
FIELD_SEP    = '|'
CLOB_SEP     = ','
NULL         = ''
EMPTY_STRING = '\ '

NEWLINE_ESCAPES = None

DEBUG      = False
VERBOSE    = False
QUIET      = False
SUMMARY    = False
DRY_RUN    = False
PEDANTIC   = False

TRUNCATE   = False
VACUUM     = False
TRIGGERS   = False

COUNT      = None
FROM_COUNT = None
FROM_ID    = None

UDC_PREFIX = 'udc_'

REFORMAT_PATH = None
DEFAULT_REFORMAT_PATH = ['/usr/share/python-support/pgloader/reformat']

MAX_THREADS           = 1
MAX_PARALLEL_SECTIONS = 1
SECTION_THREADS       = -1
SPLIT_FILE_READING    = False

CLIENT_MIN_MESSAGES = None
LOG_MIN_MESSAGES    = DEBUG
DEFAULT_LOG_FILE    = "/tmp/pgloader.log"
LOG_FILE            = None

