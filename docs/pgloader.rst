PgLoader Reference Manual
=========================

pgloader loads data from various sources into PostgreSQL. It can
transform the data it reads on the fly and submit raw SQL before and
after the loading.  It uses the `COPY` PostgreSQL protocol to stream
the data into the server, and manages errors by filling a pair of
*reject.dat* and *reject.log* files.

pgloader operates either using commands which are read from files::

    pgloader commands.load

or by using arguments and options all provided on the command line::

    pgloader SOURCE TARGET

Arguments
---------

The pgloader arguments can be as many load files as needed, or a couple of
connection strings to a specific input file.

Source Connection String
^^^^^^^^^^^^^^^^^^^^^^^^

The source connection string format is as follows::

    format:///absolute/path/to/file.ext
    format://./relative/path/to/file.ext

Where format might be one of `csv`, `fixed`, `copy`, `dbf`, `db3` or `ixf`.::

    db://user:pass@host:port/dbname

Where db might be of `sqlite`, `mysql` or `mssql`.

When using a file based source format, pgloader also support natively
fetching the file from an http location and decompressing an archive if
needed. In that case it's necessary to use the `--type` option to specify
the expected format of the file. See the examples below.

Also note that some file formats require describing some implementation
details such as columns to be read and delimiters and quoting when loading
from csv.

For more complex loading scenarios, you will need to write a full fledge
load command in the syntax described later in this document.

Target Connection String
^^^^^^^^^^^^^^^^^^^^^^^^

The target connection string format is described in details later in this
document, see Section Connection String.

Options
-------

Inquiry Options
^^^^^^^^^^^^^^^

Use these options when you want to know more about how to use `pgloader`, as
those options will cause `pgloader` not to load any data.

  * `-h`, `--help`

    Show command usage summary and exit.

  * `-V`, `--version`

    Show pgloader version string and exit.

  * `-E`, `--list-encodings`

    List known encodings in this version of pgloader.

  * `-U`, `--upgrade-config`
    
    Parse given files in the command line as `pgloader.conf` files with the
    `INI` syntax that was in use in pgloader versions 2.x, and output the
    new command syntax for pgloader on standard output.


General Options
^^^^^^^^^^^^^^^

Those options are meant to tweak `pgloader` behavior when loading data.

  * `-v`, `--verbose`
    
    Be verbose.

  * `-q`, `--quiet`
    
    Be quiet.

  * `-d`, `--debug`
    
    Show debug level information messages.

  * `-D`, `--root-dir`
    
    Set the root working directory (default to "/tmp/pgloader").

  * `-L`, `--logfile`
    
    Set the pgloader log file (default to "/tmp/pgloader/pgloader.log").

  * `--log-min-messages`
    
    Minimum level of verbosity needed for log message to make it to the
    logfile. One of critical, log, error, warning, notice, info or debug.

  * `--client-min-messages`
    
    Minimum level of verbosity needed for log message to make it to the
    console. One of critical, log, error, warning, notice, info or debug.

  * `-S`, `--summary`
    
    A filename where to copy the summary output. When relative, the filename
    is expanded into `*root-dir*`.

    The format of the filename defaults to being *human readable*. It is
    possible to have the output in machine friendly formats such as *CSV*,
    *COPY* (PostgreSQL's own COPY format) or *JSON* by specifying a filename
    with the extension resp. `.csv`, `.copy` or `.json`.

  * `-l <file>`, `--load-lisp-file <file>`
    
    Specify a lisp <file> to compile and load into the pgloader image before
    reading the commands, allowing to define extra transformation function.
    Those functions should be defined in the `pgloader.transforms` package.
    This option can appear more than once in the command line.

  * `--dry-run`

    Allow testing a `.load` file without actually trying to load any data.
    It's useful to debug it until it's ok, in particular to fix connection
    strings.

  * `--on-error-stop`

    Alter pgloader behavior: rather than trying to be smart about error
    handling and continue loading good data, separating away the bad one,
    just stop as soon as PostgreSQL refuses anything sent to it. Useful to
    debug data processing, transformation function and specific type
    casting.

  * `--self-upgrade <directory>`

    Specify a <directory> where to find pgloader sources so that one of the
    very first things it does is dynamically loading-in (and compiling to
    machine code) another version of itself, usually a newer one like a very
    recent git checkout.

  * `--no-ssl-cert-verification`

    Uses the OpenSSL option to accept a locally issued server-side
    certificate, avoiding the following error message::

      SSL verify error: 20 X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY

    The right way to fix the SSL issue is to use a trusted certificate, of
    course. Sometimes though it's useful to make progress with the pgloader
    setup while the certificate chain of trust is being fixed, maybe by
    another team. That's when this option is useful.

Command Line Only Operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Those options are meant to be used when using `pgloader` from the command
line only, rather than using a command file and the rich command clauses and
parser. In simple cases, it can be much easier to use the *SOURCE* and
*TARGET* directly on the command line, then tweak the loading with those
options:

  * `--with "option"`

    Allows setting options from the command line. You can use that option as
    many times as you want. The option arguments must follow the *WITH*
    clause for the source type of the `SOURCE` specification, as described
    later in this document.

  * `--set "guc_name='value'"`

    Allows setting PostgreSQL configuration from the command line. Note that
    the option parsing is the same as when used from the *SET* command
    clause, in particular you must enclose the guc value with single-quotes.

  * `--field "..."`

    Allows setting a source field definition. Fields are accumulated in the
    order given on the command line. It's possible to either use a `--field`
    option per field in the source file, or to separate field definitions by
    a comma, as you would do in the *HAVING FIELDS* clause.

  * `--cast "..."`

    Allows setting a specific casting rule for loading the data.

  * `--type csv|fixed|db3|ixf|sqlite|mysql|mssql`

    Allows forcing the source type, in case when the *SOURCE* parsing isn't
    satisfying.

  * `--encoding <encoding>`

    Set the encoding of the source file to load data from.

  * `--before <filename>`

    Parse given filename for SQL queries and run them against the target
    database before loading the data from the source. The queries are parsed
    by pgloader itself: they need to be terminated by a semi-colon (;) and
    the file may include `\i` or `\ir` commands to *include* another file.

  * `--after <filename>`

    Parse given filename for SQL queries and run them against the target
    database after having loaded the data from the source. The queries are
    parsed in the same way as with the `--before` option, see above.

More Debug Information
^^^^^^^^^^^^^^^^^^^^^^

To get the maximum amount of debug information, you can use both the
`--verbose` and the `--debug` switches at the same time, which is equivalent
to saying `--client-min-messages data`. Then the log messages will show the
data being processed, in the cases where the code has explicit support for
it.

Batches And Retry Behaviour
---------------------------

To load data to PostgreSQL, pgloader uses the `COPY` streaming protocol.
While this is the faster way to load data, `COPY` has an important drawback:
as soon as PostgreSQL emits an error with any bit of data sent to it,
whatever the problem is, the whole data set is rejected by PostgreSQL.

To work around that, pgloader cuts the data into *batches* of 25000 rows
each, so that when a problem occurs it's only impacting that many rows of
data. Each batch is kept in memory while the `COPY` streaming happens, in
order to be able to handle errors should some happen.

When PostgreSQL rejects the whole batch, pgloader logs the error message
then isolates the bad row(s) from the accepted ones by retrying the batched
rows in smaller batches. To do that, pgloader parses the *CONTEXT* error
message from the failed COPY, as the message contains the line number where
the error was found in the batch, as in the following example::

    CONTEXT: COPY errors, line 3, column b: "2006-13-11"

Using that information, pgloader will reload all rows in the batch before
the erroneous one, log the erroneous one as rejected, then try loading the
remaining of the batch in a single attempt, which may or may not contain
other erroneous data.

At the end of a load containing rejected rows, you will find two files in
the *root-dir* location, under a directory named the same as the target
database of your setup. The filenames are the target table, and their
extensions are `.dat` for the rejected data and `.log` for the file
containing the full PostgreSQL client side logs about the rejected data.

The `.dat` file is formatted in PostgreSQL the text COPY format as documented
in `http://www.postgresql.org/docs/9.2/static/sql-copy.html#AEN66609`.

It is possible to use the following WITH options to control pgloader batch
behavior:

  - *on error stop*, *on error resume next*

    This option controls if pgloader is using building batches of data at
    all. The batch implementation allows pgloader to recover errors by
    sending the data that PostgreSQL accepts again, and by keeping away the
    data that PostgreSQL rejects.

    To enable retrying the data and loading the good parts, use the option
    *on error resume next*, which is the default to file based data loads
    (such as CSV, IXF or DBF).

    When migrating from another RDMBS technology, it's best to have a
    reproducible loading process. In that case it's possible to use *on
    error stop* and fix either the casting rules, the data transformation
    functions or in cases the input data until your migration runs through
    completion. That's why *on error resume next* is the default for SQLite,
    MySQL and MS SQL source kinds.

A Note About Performance
------------------------

pgloader has been developed with performance in mind, to be able to cope
with ever growing needs in loading large amounts of data into PostgreSQL.

The basic architecture it uses is the old Unix pipe model, where a thread is
responsible for loading the data (reading a CSV file, querying MySQL, etc)
and fills pre-processed data into a queue. Another threads feeds from the
queue, apply some more *transformations* to the input data and stream the
end result to PostgreSQL using the COPY protocol.

When given a file that the PostgreSQL `COPY` command knows how to parse, and
if the file contains no erroneous data, then pgloader will never be as fast
as just using the PostgreSQL `COPY` command.

Note that while the `COPY` command is restricted to read either from its
standard input or from a local file on the server's file system, the command
line tool `psql` implements a `\copy` command that knows how to stream a
file local to the client over the network and into the PostgreSQL server,
using the same protocol as pgloader uses.

A Note About Parallelism
------------------------

pgloader uses several concurrent tasks to process the data being loaded:

  - a reader task reads the data in and pushes it to a queue,
  
  - at last one write task feeds from the queue and formats the raw into the
    PostgreSQL COPY format in batches (so that it's possible to then retry a
    failed batch without reading the data from source again), and then sends
    the data to PostgreSQL using the COPY protocol.

The parameter *workers* allows to control how many worker threads are
allowed to be active at any time (that's the parallelism level); and the
parameter *concurrency* allows to control how many tasks are started to
handle the data (they may not all run at the same time, depending on the
*workers* setting).

We allow *workers* simultaneous workers to be active at the same time in the
context of a single table. A single unit of work consist of several kinds of
workers:

  - a reader getting raw data from the source,
  - N writers preparing and sending the data down to PostgreSQL.

The N here is setup to the *concurrency* parameter: with a *CONCURRENCY* of
2, we start (+ 1 2) = 3 concurrent tasks, with a *concurrency* of 4 we start
(+ 1 4) = 5 concurrent tasks, of which only *workers* may be active
simultaneously.

The defaults are `workers = 4, concurrency = 1` when loading from a database
source, and `workers = 8, concurrency = 2` when loading from something else
(currently, a file). Those defaults are arbitrary and waiting for feedback
from users, so please consider providing feedback if you play with the
settings.

As the `CREATE INDEX` threads started by pgloader are only waiting until
PostgreSQL is done with the real work, those threads are *NOT* counted into
the concurrency levels as detailed here.

By default, as many `CREATE INDEX` threads as the maximum number of indexes
per table are found in your source schema. It is possible to set the `max
parallel create index` *WITH* option to another number in case there's just
too many of them to create.

Source Formats
--------------

pgloader supports the following input formats:

  - csv, which includes also tsv and other common variants where you can
    change the *separator* and the *quoting* rules and how to *escape* the
    *quotes* themselves;

  - fixed columns file, where pgloader is flexible enough to accomodate with
    source files missing columns (*ragged fixed length column files* do
    exist);

  - PostgreSLQ COPY formatted files, following the COPY TEXT documentation
    of PostgreSQL, such as the reject files prepared by pgloader;

  - dbase files known as db3 or dbf file;

  - ixf formated files, ixf being a binary storage format from IBM;

  - sqlite databases with fully automated discovery of the schema and
    advanced cast rules;

  - mysql databases with fully automated discovery of the schema and
    advanced cast rules;

  - MS SQL databases with fully automated discovery of the schema and
    advanced cast rules.

Pgloader Commands Syntax
------------------------

pgloader implements a Domain Specific Language allowing to setup complex
data loading scripts handling computed columns and on-the-fly sanitization
of the input data. For more complex data loading scenarios, you will be
required to learn that DSL's syntax. It's meant to look familiar to DBA by
being inspired by SQL where it makes sense, which is not that much after
all.

The pgloader commands follow the same global grammar rules. Each of them
might support only a subset of the general options and provide specific
options.

::

    LOAD <source-type>
	     FROM <source-url>
           [ HAVING FIELDS <source-level-options> ]
		 INTO <postgresql-url>
           [ TARGET TABLE [ "<schema>" ]."<table name>" ]
           [ TARGET COLUMNS <columns-and-options> ]

	[ WITH <load-options> ]

	[ SET <postgresql-settings> ]

    [ BEFORE LOAD [ DO <sql statements> | EXECUTE <sql file> ] ... ]
    [  AFTER LOAD [ DO <sql statements> | EXECUTE <sql file> ] ... ]
	;

The main clauses are the `LOAD`, `FROM`, `INTO` and `WITH` clauses that each
command implements. Some command then implement the `SET` command, or some
specific clauses such as the `CAST` clause.

Templating with Mustache
------------------------

pgloader implements the https://mustache.github.io/ templating system so
that you may have dynamic parts of your commands. See the documentation for
this template system online.

A specific feature of pgloader is the ability to fetch a variable from the
OS environment of the pgloader process, making it possible to run pgloader
as in the following example::

    $ DBPATH=sqlite/sqlite.db pgloader ./test/sqlite-env.load
    
or in several steps::

    $ export DBPATH=sqlite/sqlite.db
    $ pgloader ./test/sqlite-env.load

The variable can then be used in a typical mustache fashion::

    load database
         from '{{DBPATH}}'
         into postgresql:///pgloader;

It's also possible to prepare a INI file such as the following::

    [pgloader]
    
    DBPATH = sqlite/sqlite.db

And run the following command, feeding the INI values as a *context* for
pgloader templating system::

    $ pgloader --context ./test/sqlite.ini ./test/sqlite-ini.load

The mustache templates implementation with OS environment support replaces
former `GETENV` implementation, which didn't work anyway.

Common Clauses
--------------

Some clauses are common to all commands:

FROM
^^^^

The *FROM* clause specifies where to read the data from, and each command
introduces its own variant of sources. For instance, the *CSV* source
supports `inline`, `stdin`, a filename, a quoted filename, and a *FILENAME
MATCHING* clause (see above); whereas the *MySQL* source only supports a
MySQL database URI specification.

INTO
^^^^

The PostgreSQL connection URI must contains the name of the target table
where to load the data into. That table must have already been created in
PostgreSQL, and the name might be schema qualified.

Then *INTO* option also supports an optional comma separated list of target
columns, which are either the name of an input *field* or the white space
separated list of the target column name, its PostgreSQL data type and a
*USING* expression.

The *USING* expression can be any valid Common Lisp form and will be read
with the current package set to `pgloader.transforms`, so that you can use
functions defined in that package, such as functions loaded dynamically with
the `--load` command line parameter.

Each *USING* expression is compiled at runtime to native code.

This feature allows pgloader to load any number of fields in a CSV file into
a possibly different number of columns in the database, using custom code
for that projection.

WITH
^^^^

Set of options to apply to the command, using a global syntax of either:

   - *key = value*
   - *use option*
   - *do not use option*

See each specific command for details.

All data sources specific commands support the following options:

  - *on error stop*, *on error resume next*
  - *batch rows = R*
  - *batch size = ... MB*
  - *prefetch rows = ...*

See the section BATCH BEHAVIOUR OPTIONS for more details.

In addition, the following settings are available:

   - *workers = W*
   - *concurrency = C*
   - *max parallel create index = I*

See section A NOTE ABOUT PARALLELISM for more details.

SET
^^^

This clause allows to specify session parameters to be set for all the
sessions opened by pgloader. It expects a list of parameter name, the equal
sign, then the single-quoted value as a comma separated list.

The names and values of the parameters are not validated by pgloader, they
are given as-is to PostgreSQL.

BEFORE LOAD DO
^^^^^^^^^^^^^^

You can run SQL queries against the database before loading the data from
the `CSV` file. Most common SQL queries are `CREATE TABLE IF NOT EXISTS` so
that the data can be loaded.

Each command must be *dollar-quoted*: it must begin and end with a double
dollar sign, `$$`. Dollar-quoted queries are then comma separated. No extra
punctuation is expected after the last SQL query.

BEFORE LOAD EXECUTE
^^^^^^^^^^^^^^^^^^^

Same behaviour as in the *BEFORE LOAD DO* clause. Allows you to read the SQL
queries from a SQL file. Implements support for PostgreSQL dollar-quoting
and the `\i` and `\ir` include facilities as in `psql` batch mode (where
they are the same thing).

AFTER LOAD DO
^^^^^^^^^^^^^

Same format as *BEFORE LOAD DO*, the dollar-quoted queries found in that
section are executed once the load is done. That's the right time to create
indexes and constraints, or re-enable triggers.

AFTER LOAD EXECUTE
^^^^^^^^^^^^^^^^^^

Same behaviour as in the *AFTER LOAD DO* clause. Allows you to read the SQL
queries from a SQL file. Implements support for PostgreSQL dollar-quoting
and the `\i` and `\ir` include facilities as in `psql` batch mode (where
they are the same thing).

AFTER CREATE SCHEMA DO
^^^^^^^^^^^^^^^^^^^^^^

Same format as *BEFORE LOAD DO*, the dollar-quoted queries found in that
section are executed once the schema has been created by pgloader, and
before the data is loaded. It's the right time to ALTER TABLE or do some
custom implementation on-top of what pgloader does, like maybe partitioning.

AFTER CREATE SCHEMA EXECUTE
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Same behaviour as in the *AFTER CREATE SCHEMA DO* clause. Allows you to read
the SQL queries from a SQL file. Implements support for PostgreSQL
dollar-quoting and the `\i` and `\ir` include facilities as in `psql` batch
mode (where they are the same thing).

Connection String
^^^^^^^^^^^^^^^^^

The `<postgresql-url>` parameter is expected to be given as a *Connection URI*
as documented in the PostgreSQL documentation at
http://www.postgresql.org/docs/9.3/static/libpq-connect.html#LIBPQ-CONNSTRING.

::

    postgresql://[user[:password]@][netloc][:port][/dbname][?option=value&...]

Where:

  - *user*

    Can contain any character, including colon (`:`) which must then be
    doubled (`::`) and at-sign (`@`) which must then be doubled (`@@`).

    When omitted, the *user* name defaults to the value of the `PGUSER`
    environment variable, and if it is unset, the value of the `USER`
    environment variable.

  - *password*

	Can contain any character, including the at sign (`@`) which must then
	be doubled (`@@`). To leave the password empty, when the *user* name
	ends with at at sign, you then have to use the syntax user:@.

    When omitted, the *password* defaults to the value of the `PGPASSWORD`
    environment variable if it is set, otherwise the password is left
    unset.
    
    When no *password* is found either in the connection URI nor in the
    environment, then pgloader looks for a `.pgpass` file as documented at
    https://www.postgresql.org/docs/current/static/libpq-pgpass.html. The
    implementation is not that of `libpq` though. As with `libpq` you can
    set the environment variable `PGPASSFILE` to point to a `.pgpass` file,
    and pgloader defaults to `~/.pgpass` on unix like systems and
    `%APPDATA%\postgresql\pgpass.conf` on windows. Matching rules and syntax
    are the same as with `libpq`, refer to its documentation.

  - *netloc*

    Can be either a hostname in dotted notation, or an ipv4, or an Unix
    domain socket path. Empty is the default network location, under a
    system providing *unix domain socket* that method is preferred, otherwise
    the *netloc* default to `localhost`.

	It's possible to force the *unix domain socket* path by using the syntax
	`unix:/path/to/where/the/socket/file/is`, so to force a non default
	socket path and a non default port, you would have:

	    postgresql://unix:/tmp:54321/dbname

    The *netloc* defaults to the value of the `PGHOST` environment
    variable, and if it is unset, to either the default `unix` socket path
    when running on a Unix system, and `localhost` otherwise.

    Socket path containing colons are supported by doubling the colons
    within the path, as in the following example:
    
        postgresql://unix:/tmp/project::region::instance:5432/dbname

  - *dbname*

	Should be a proper identifier (letter followed by a mix of letters,
	digits and the punctuation signs comma (`,`), dash (`-`) and underscore
	(`_`).

    When omitted, the *dbname* defaults to the value of the environment
    variable `PGDATABASE`, and if that is unset, to the *user* value as
    determined above.

  - *options*

    The optional parameters must be supplied with the form `name=value`, and
    you may use several parameters by separating them away using an
    ampersand (`&`) character.

    Only some options are supported here, *tablename* (which might be
    qualified with a schema name) *sslmode*, *host*, *port*, *dbname*,
    *user* and *password*.

    The *sslmode* parameter values can be one of `disable`, `allow`,
    `prefer` or `require`.

    For backward compatibility reasons, it's possible to specify the
    *tablename* option directly, without spelling out the `tablename=`
    parts.

    The options override the main URI components when both are given, and
    using the percent-encoded option parameters allow using passwords
    starting with a colon and bypassing other URI components parsing
    limitations.

Regular Expressions
^^^^^^^^^^^^^^^^^^^

Several clauses listed in the following accept *regular expressions* with
the following input rules:

  - A regular expression begins with a tilde sign (`~`),

  - is then followed with an opening sign,

  - then any character is allowed and considered part of the regular
    expression, except for the closing sign,

  - then a closing sign is expected.

The opening and closing sign are allowed by pair, here's the complete list
of allowed delimiters::

    ~//
    ~[]
    ~{}
    ~()
    ~<>
    ~""
    ~''
    ~||
    ~##

Pick the set of delimiters that don't collide with the *regular expression*
you're trying to input. If your expression is such that none of the
solutions allow you to enter it, the places where such expressions are
allowed should allow for a list of expressions.

Comments
^^^^^^^^

Any command may contain comments, following those input rules:

  - the `--` delimiter begins a comment that ends with the end of the
    current line,

  - the delimiters `/*` and `*/` respectively start and end a comment, which
    can be found in the middle of a command or span several lines.

Any place where you could enter a *whitespace* will accept a comment too.

Batch behaviour options
^^^^^^^^^^^^^^^^^^^^^^^

All pgloader commands have support for a *WITH* clause that allows for
specifying options. Some options are generic and accepted by all commands,
such as the *batch behaviour options*, and some options are specific to a
data source kind, such as the CSV *skip header* option.

The global batch behaviour options are:

  - *batch rows*

    Takes a numeric value as argument, used as the maximum number of rows
    allowed in a batch. The default is `25 000` and can be changed to try
    having better performance characteristics or to control pgloader memory
    usage;

  - *batch size*

    Takes a memory unit as argument, such as *20 MB*, its default value.
    Accepted multipliers are *kB*, *MB*, *GB*, *TB* and *PB*. The case is
    important so as not to be confused about bits versus bytes, we're only
    talking bytes here.

  - *prefetch rows*

    Takes a numeric value as argument, defaults to `100000`. That's the
    number of rows that pgloader is allowed to read in memory in each reader
    thread. See the *workers* setting for how many reader threads are
    allowed to run at the same time.

Other options are specific to each input source, please refer to specific
parts of the documentation for their listing and covering.

A batch is then closed as soon as either the *batch rows* or the *batch
size* threshold is crossed, whichever comes first. In cases when a batch has
to be closed because of the *batch size* setting, a *debug* level log
message is printed with how many rows did fit in the *oversized* batch.

