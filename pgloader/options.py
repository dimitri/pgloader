# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# Some common options, for each module to get them

from tempfile import gettempdir
import os

PGLOADER_VERSION = '2.3.4~dev1'

PSYCOPG_VERSION = None

INPUT_ENCODING     = None
PG_CLIENT_ENCODING = 'latin9'
PG_OPTIONS         = None
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

DEFAULT_MAX_PARALLEL_SECTIONS = 1
DEFAULT_SECTION_THREADS       = 1
MAX_PARALLEL_SECTIONS = None
SECTION_THREADS       = None
SPLIT_FILE_READING    = False
RRQUEUE_SIZE          = None

CLIENT_MIN_MESSAGES = None
LOG_MIN_MESSAGES    = DEBUG
DEFAULT_LOG_FILE    = os.path.join (gettempdir(), "pgloader.log")
LOG_FILE            = None

REJECT_LOG_FILE  = '%s.rej.log'
REJECT_DATA_FILE = '%s.rej'

LOAD_FROM_STDIN = None
LOAD_TO_TABLE   = None

FILE_BOUNDARIES = None  # (start, end) --- file positions in bytes
