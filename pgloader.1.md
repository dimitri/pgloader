# pgloader(1) -- PostgreSQL data loader

## SYNOPSIS

    pgloader [<options>] [<command-file>]...
    pgloader [<options>] SOURCE TARGET

## DESCRIPTION

pgloader loads data from various sources into PostgreSQL. It can
transform the data it reads on the fly and submit raw SQL before and
after the loading.  It uses the `COPY` PostgreSQL protocol to stream
the data into the server, and manages errors by filling a pair of
*reject.dat* and *reject.log* files.

pgloader operates either using commands which are read from files:

    pgloader commands.load
    
or by using arguments and options all provided on the command line:

    pgloader SOURCE TARGET

## ARGUMENTS

The pgloader arguments can be as many load files as needed, or a couple of
connection strings to a specific input file.

### SOURCE CONNECTION STRING

The source connection string format is as follows:

    format:///absolute/path/to/file.ext
    format://./relative/path/to/file.ext
    
Where format might be one of `csv`, `fixed`, `copy`, `dbf`, `db3` or `ixf`.

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

### TARGET CONNECTION STRING

The target connection string format is described in details later in this
document, see Section Connection String.

## OPTIONS

### INQUIRY OPTIONS

Use these options when you want to know more about how to use `pgloader`, as
those options will cause `pgloader` not to load any data.

  * `-h`, `--help`:
    Show command usage summary and exit.

  * `-V`, `--version`:
    Show pgloader version string and exit.

  * `-E`, `--list-encodings`:
    List known encodings in this version of pgloader.

  * `-U`, `--upgrade-config`:
    Parse given files in the command line as `pgloader.conf` files with the
   `INI` syntax that was in use in pgloader versions 2.x, and output the
   new command syntax for pgloader on standard output.


### GENERAL OPTIONS

Those options are meant to tweak `pgloader` behavior when loading data.

  * `-v`, `--verbose`:
    Be verbose.

  * `-q`, `--quiet`:
    Be quiet.

  * `-d`, `--debug`:
    Show debug level information messages.

  * `-D`, `--root-dir`:
    Set the root working directory (default to "/tmp/pgloader").

  * `-L`, `--logfile`:
    Set the pgloader log file (default to "/tmp/pgloader.log").

  * `--log-min-messages`:
    Minimum level of verbosity needed for log message to make it to the
    logfile. One of critical, log, error, warning, notice, info or debug.

  * `--client-min-messages`:
    Minimum level of verbosity needed for log message to make it to the
    console. One of critical, log, error, warning, notice, info or debug.

  * `-S`, `--summary`:
    A filename where to copy the summary output. When relative, the filename
    is expanded into `*root-dir*`.
    
    The format of the filename defaults to being *human readable*. It is
    possible to have the output in machine friendly formats such as *CSV*,
    *COPY* (PostgreSQL's own COPY format) or *JSON* by specifying a filename
    with the extension resp. `.csv`, `.copy` or `.json`.

  * `-l <file>`, `--load-lisp-file <file>`:
    Specify a lisp <file> to compile and load into the pgloader image before
    reading the commands, allowing to define extra transformation function.
    Those functions should be defined in the `pgloader.transforms` package.
    This option can appear more than once in the command line.

  * `--self-upgrade <directory>`:

    Specify a <directory> where to find pgloader sources so that one of the
    very first things it does is dynamically loading-in (and compiling to
    machine code) another version of itself, usually a newer one like a very
    recent git checkout.

### COMMAND LINE ONLY OPERATIONS

Those options are meant to be used when using `pgloader` from the command
line only, rather than using a command file and the rich command clauses and
parser. In simple cases, it can be much easier to use the *SOURCE* and
*TARGET* directly on the command line, then tweak the loading with those
options:

  * `--with "option"`:
  
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

### MORE DEBUG INFORMATION

To get the maximum amount of debug information, you can use both the
`--verbose` and the `--debug` switches at the same time, which is equivalent
to saying `--client-min-messages data`. Then the log messages will show the
data being processed, in the cases where the code has explicit support for
it.

## USAGE EXAMPLES

Review the command line options and pgloader's version:

    pgloader --help
    pgloader --version

### Loading from a complex command

Use the command file as the pgloader command argument, pgloader will parse
that file and execute the commands found in it:

    pgloader --verbose ./test/csv-districts.load 

### CSV

Load data from a CSV file into a pre-existing table in your database:

    pgloader --type csv                                   \
             --field id --field field                     \
             --with truncate                              \
             --with "fields terminated by ','"            \
             ./test/data/matching-1.csv                   \
             postgres:///pgloader?tablename=matching

In that example the whole loading is driven from the command line, bypassing
the need for writing a command in the pgloader command syntax entirely. As
there's no command though, the extra inforamtion needed must be provided on
the command line using the `--type` and `--field` and `--with` switches.

For documentation about the available syntaxes for the `--field` and
`--with` switches, please refer to the CSV section later in the man page.

Note also that the PostgreSQL URI includes the target *tablename*.

### Reading from STDIN

File based pgloader sources can be loaded from the standard input, as in the
following example:

    pgloader --type csv                                         \
             --field "usps,geoid,aland,awater,aland_sqmi,awater_sqmi,intptlat,intptlong" \
             --with "skip header = 1"                          \
             --with "fields terminated by '\t'"                \
             -                                                 \
             postgresql:///pgloader?districts_longlat          \
             < test/data/2013_Gaz_113CDs_national.txt

The dash (`-`) character as a source is used to mean *standard input*, as
usual in Unix command lines. It's possible to stream compressed content to
pgloader with this technique, using the Unix pipe:

    gunzip -c source.gz | pgloader --type csv ... - pgsql:///target?foo

### Loading from CSV available through HTTP

The same command as just above can also be run if the CSV file happens to be
found on a remote HTTP location:

    pgloader --type csv                                                     \
             --field "usps,geoid,aland,awater,aland_sqmi,awater_sqmi,intptlat,intptlong" \
             --with "skip header = 1"                                       \
             --with "fields terminated by '\t'"                             \
             http://pgsql.tapoueh.org/temp/2013_Gaz_113CDs_national.txt     \
             postgresql:///pgloader?districts_longlat

Some more options have to be used in that case, as the file contains a
one-line header (most commonly that's column names, could be a copyright
notice). Also, in that case, we specify all the fields right into a single
`--field` option argument.

Again, the PostgreSQL target connection string must contain the *tablename*
option and you have to ensure that the target table exists and may fit the
data. Here's the SQL command used in that example in case you want to try it
yourself:

    create table districts_longlat
    (
             usps        text,
             geoid       text,
             aland       bigint,
             awater      bigint,
             aland_sqmi  double precision,
             awater_sqmi double precision,
             intptlat    double precision,
             intptlong   double precision
    );

Also notice that the same command will work against an archived version of
the same data, e.g.
http://pgsql.tapoueh.org/temp/2013_Gaz_113CDs_national.txt.gz.

Finally, it's important to note that pgloader first fetches the content from
the HTTP URL it to a local file, then expand the archive when it's
recognized to be one, and only then processes the locally expanded file.

In some cases, either because pgloader has no direct support for your
archive format or maybe because expanding the archive is not feasible in
your environment, you might want to *stream* the content straight from its
remote location into PostgreSQL. Here's how to do that, using the old battle
tested Unix Pipes trick:

    curl http://pgsql.tapoueh.org/temp/2013_Gaz_113CDs_national.txt.gz \
    | gunzip -c                                                        \
    | pgloader --type csv                                              \
               --field "usps,geoid,aland,awater,aland_sqmi,awater_sqmi,intptlat,intptlong"
               --with "skip header = 1"                                \
               --with "fields terminated by '\t'"                      \
               -                                                       \
               postgresql:///pgloader?districts_longlat

Now the OS will take care of the streaming and buffering between the network
and the commands and pgloader will take care of streaming the data down to
PostgreSQL.

### Migrating from SQLite

The following command will open the SQLite database, discover its tables
definitions including indexes and foreign keys, migrate those definitions
while *casting* the data type specifications to their PostgreSQL equivalent
and then migrate the data over:

    createdb newdb
    pgloader ./test/sqlite/sqlite.db postgresql:///newdb

### Migrating from MySQL

Just create a database where to host the MySQL data and definitions and have
pgloader do the migration for you in a single command line:

    createdb pagila
    pgloader mysql://user@localhost/sakila postgresql:///pagila

### Fetching an archived DBF file from a HTTP remote location

It's possible for pgloader to download a file from HTTP, unarchive it, and
only then open it to discover the schema then load the data:

    createdb foo
    pgloader --type dbf http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement/2013/dbf/historiq2013.zip postgresql:///foo

Here it's not possible for pgloader to guess the kind of data source it's
being given, so it's necessary to use the `--type` command line switch.

## BATCHES AND RETRY BEHAVIOUR

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
the error was found in the batch, as in the following example:

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
in [http://www.postgresql.org/docs/9.2/static/sql-copy.html#AEN66609]().

## A NOTE ABOUT PERFORMANCES

pgloader has been developed with performances in mind, to be able to cope
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

## SOURCE FORMATS

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

## PGLOADER COMMANDS SYNTAX

pgloader implements a Domain Specific Language allowing to setup complex
data loading scripts handling computed columns and on-the-fly sanitization
of the input data. For more complex data loading scenarios, you will be
required to learn that DSL's syntax. It's meant to look familiar to DBA by
being inspired by SQL where it makes sense, which is not that much after
all.

The pgloader commands follow the same global grammar rules. Each of them
might support only a subset of the general options and provide specific
options.

    LOAD <source-type>
	     FROM <source-url>     [ HAVING FIELDS <source-level-options> ]
		 INTO <postgresql-url> [ TARGET COLUMNS <columns-and-options> ]

	[ WITH <load-options> ]

	[ SET <postgresql-settings> ]
    
    [ BEFORE LOAD DO|EXECUTE [ <sql statements> | <sql file> ]
    [  AFTER LOAD DO|EXECUTE [ <sql statements> | <sql file> ]
	;

The main clauses are the `LOAD`, `FROM`, `INTO` and `WITH` clauses that each
command implements. Some command then implement the `SET` command, or some
specific clauses such as the `CAST` clause.

## COMMON CLAUSES

Some clauses are common to all commands:

  - *FROM*
  
    The *FROM* clause specifies where to read the data from, and each
    command introduces its own variant of sources. For instance, the *CSV*
    source supports `inline`, `stdin`, a filename, a quoted filename, and a
    *FILENAME MATCHING* clause (see above); whereas the *MySQL* source only
    supports a MySQL database URI specification.
    
    In all cases, the *FROM* clause is able to read its value from an
    environment variable when using the form `GETENV 'varname'`.

  - *INTO*

	The PostgreSQL connection URI must contains the name of the target table
	where to load the data into. That table must have already been created
	in PostgreSQL, and the name might be schema qualified.
    
    The *INTO* target database connection URI can be parsed from the value
    of an environment variable when using the form `GETENV 'varname'`.

	Then *INTO* option also supports an optional comma separated list of
	target columns, which are either the name of an input *field* or the
	white space separated list of the target column name, its PostgreSQL data
	type and a *USING* expression.

	The *USING* expression can be any valid Common Lisp form and will be
	read with the current package set to `pgloader.transforms`, so that you
	can use functions defined in that package, such as functions loaded
	dynamically with the `--load` command line parameter.

    Each *USING* expression is compiled at runtime to native code.
    
    This feature allows pgloader to load any number of fields in a CSV file
    into a possibly different number of columns in the database, using
    custom code for that projection.

  - *WITH*
  
    Set of options to apply to the command, using a global syntax of either:
    
       - *key = value*
       - *use option*
       - *do not use option*
      
    See each specific command for details.
    
  - *SET*

	This clause allows to specify session parameters to be set for all the
    sessions opened by pgloader. It expects a list of parameter name, the
    equal sign, then the single-quoted value as a comma separated list.

 	The names and values of the parameters are not validated by pgloader,
 	they are given as-is to PostgreSQL.

  - *BEFORE LOAD DO*

	 You can run SQL queries against the database before loading the data
	 from the `CSV` file. Most common SQL queries are `CREATE TABLE IF NOT
	 EXISTS` so that the data can be loaded.

	 Each command must be *dollar-quoted*: it must begin and end with a
	 double dollar sign, `$$`. Dollar-quoted queries are then comma
	 separated. No extra punctuation is expected after the last SQL query.

  - *BEFORE LOAD EXECUTE*

     Same behaviour as in the *BEFORE LOAD DO* clause. Allows you to read
     the SQL queries from a SQL file. Implements support for PostgreSQL
     dollar-quoting and the `\i` and `\ir` include facilities as in `psql`
     batch mode (where they are the same thing).

  - *AFTER LOAD DO*

	Same format as *BEFORE LOAD DO*, the dollar-quoted queries found in that
	section are executed once the load is done. That's the right time to
	create indexes and constraints, or re-enable triggers.

  - *AFTER LOAD EXECUTE*

     Same behaviour as in the *AFTER LOAD DO* clause. Allows you to read the
     SQL queries from a SQL file. Implements support for PostgreSQL
     dollar-quoting and the `\i` and `\ir` include facilities as in `psql`
     batch mode (where they are the same thing).

### Connection String

The `<postgresql-url>` parameter is expected to be given as a *Connection URI*
as documented in the PostgreSQL documentation at
http://www.postgresql.org/docs/9.3/static/libpq-connect.html#LIBPQ-CONNSTRING.

    postgresql://[user[:password]@][netloc][:port][/dbname][?option=value&...]

Where:

  - *user*

    Can contain any character, including colon (`:`) which must then be
    doubled (`::`) and at-sign (`@`) which must then be doubled (`@@`).
    
    When omitted, the *user* name defaults to the value of the `PGUSER`
    environment variable, and if it is unset, the value of the `USER`
    environment variable.

  - *password*

	Can contain any character, including that at sign (`@`) which must then
	be doubled (`@@`). To leave the password empty, when the *user* name
	ends with at at sign, you then have to use the syntax user:@.

    When omitted, the *password* defaults to the value of the `PGPASSWORD`
    environment variable if it is set, otherwise the password is left
    unset.

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
    
    Only two options are supported here, *tablename* (which might be
    qualified with a schema name) and *sslmode*.
    
    The *sslmode* parameter values can be one of `disable`, `allow`,
    `prefer` or `require`.
    
    For backward compatibility reasons, it's possible to specify the
    *tablename* option directly, without spelling out the `tablename=`
    parts.

### Regular Expressions

Several clauses listed in the following accept *regular expressions* with
the following input rules:

  - A regular expression begins with a tilde sign (`~`),

  - is then followed with an opening sign,

  - then any character is allowed and considered part of the regular
    expression, except for the closing sign,

  - then a closing sign is expected.

The opening and closing sign are allowed by pair, here's the complete list
of allowed delimiters:

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

### Comments

Any command may contain comments, following those input rules:

  - the `--` delimiter begins a comment that ends with the end of the
    current line,

  - the delimiters `/*` and `*/` respectively start and end a comment, which
    can be found in the middle of a command or span several lines.

Any place where you could enter a *whitespace* will accept a comment too.

### Batch behaviour options

All pgloader commands have support for a *WITH* clause that allows for
specifying options. Some options are generic and accepted by all commands,
such as the *batch behaviour options*, and some options are specific to a
data source kind, such as the CSV *skip header* option.

The global batch behaviour options are:

  - *batch rows*
  
    Takes a numeric value as argument, used as the maximum number of rows
    allowed in a batch. The default is `25 000` and can be changed to try
    having better performances characteristics or to control pgloader memory
    usage;
    
  - *batch size*
  
    Takes a memory unit as argument, such as *20 MB*, its default value.
    Accepted multipliers are *kB*, *MB*, *GB*, *TB* and *PB*. The case is
    important so as not to be confused about bits versus bytes, we're only
    talking bytes here.
    
  - *batch concurrency*
  
    Takes a numeric value as argument, defaults to `10`. That's the number
    of batches that pgloader is allows to build in memory, even when only a
    single batch at a time might be sent to PostgreSQL.
  
    Supporting more than a single batch being sent at a time is on the TODO
    list of pgloader, but is not implemented yet. This option is about
    controlling the memory needs of pgloader as a trade-off to the
    performances characteristics, and not about parallel activity of
    pgloader.
    
Other options are specific to each input source, please refer to specific
parts of the documentation for their listing and covering.

A batch is then closed as soon as either the *batch rows* or the *batch
size* threshold is crossed, whichever comes first. In cases when a batch has
to be closed because of the *batch size* setting, a *debug* level log
message is printed with how many rows did fit in the *oversized* batch.

## LOAD CSV

This command instructs pgloader to load data from a `CSV` file. Here's an
example:

    LOAD CSV
       FROM 'GeoLiteCity-Blocks.csv' WITH ENCODING iso-646-us
            HAVING FIELDS
            (
               startIpNum, endIpNum, locId
            )
       INTO postgresql://user@localhost:54393/dbname?geolite.blocks
            TARGET COLUMNS
            (
               iprange ip4r using (ip-range startIpNum endIpNum),
               locId
            )
       WITH truncate,
            skip header = 2,
            fields optionally enclosed by '"',
            fields escaped by backslash-quote,
            fields terminated by '\t'

        SET work_mem to '32 MB', maintenance_work_mem to '64 MB';

The `csv` format command accepts the following clauses and options:

  - *FROM*

    Filename where to load the data from. Accepts an *ENCODING* option. Use
    the `--list-encodings` option to know which encoding names are
    supported.

	The filename may be enclosed by single quotes, and could be one of the
	following special values:

	  - *inline*

        The data is found after the end of the parsed commands. Any number
        of empty lines between the end of the commands and the beginning of
        the data is accepted.

	  - *stdin*

	    Reads the data from the standard input stream.

      - *FILENAMES MATCHING*

        The whole *matching* clause must follow the following rule:

	        [ ALL FILENAMES | [ FIRST ] FILENAME ]
            MATCHING regexp
            [ IN DIRECTORY '...' ]

        The *matching* clause applies given *regular expression* (see above
        for exact syntax, several options can be used here) to filenames.
        It's then possible to load data from only the first match of all of
        them.
        
        The optional *IN DIRECTORY* clause allows specifying which directory
        to walk for finding the data files, and can be either relative to
        where the command file is read from, or absolute. The given
        directory must exists.

	The *FROM* option also supports an optional comma separated list of
	*field* names describing what is expected in the `CSV` data file,
	optionally introduced by the clause `HAVING FIELDS`.

	Each field name can be either only one name or a name following with
	specific reader options for that field, enclosed in square brackets and
	comma-separated. Supported per-field reader options are:

	  - *terminated by*

		See the description of *field terminated by* below.

		The processing of this option is not currently implemented.

	  - *date format*

	    When the field is expected of the date type, then this option allows
	    to specify the date format used in the file.

		Date format string are template strings modeled against the
		PostgreSQL `to_char` template strings support, limited to the
		following patterns:
        
          - YYYY, YYY, YY for the year part
          - MM for the numeric month part
          - DD for the numeric day part
          - HH, HH12, HH24 for the hour part
          - am, AM, a.m., A.M.
          - pm, PM, p.m., P.M.
          - MI for the minutes part
          - SS for the seconds part
          - MS for the milliseconds part (4 digits)
          - US for the microseconds part (6 digits)
          - unparsed punctuation signs: - . * # @ T / \ and space
          
        Here's an example of a *date format* specification:
        
            column-name [date format 'YYYY-MM-DD HH24-MI-SS.US']

      - *null if*

	    This option takes an argument which is either the keyword *blanks*
	    or a double-quoted string.

		When *blanks* is used and the field value that is read contains only
	    space characters, then it's automatically converted to an SQL `NULL`
	    value.

		When a double-quoted string is used and that string is read as the
		field value, then the field value is automatically converted to an
		SQL `NULL` value.

	  - *trim both whitespace*, *trim left whitespace*, *trim right whitespace*

	    This option allows to trim whitespaces in the read data, either from
	    both sides of the data, or only the whitespace characters found on
	    the left of the streaing, or only those on the right of the string.

  - *WITH*

    When loading from a `CSV` file, the following options are supported:

	  - *truncate*

		When this option is listed, pgloader issues a `TRUNCATE` command
		against the PostgreSQL target table before reading the data file.

	  - *skip header*

	    Takes a numeric value as argument. Instruct pgloader to skip that
	    many lines at the beginning of the input file.

      - *trim unquoted blanks*

	    When reading unquoted values in the `CSV` file, remove the blanks
	    found in between the separator and the value. That behaviour is the
	    default.

      - *keep unquoted blanks*

        When reading unquoted values in the `CSV` file, keep blanks found in
        between the separator and the value.

      - *fields optionally enclosed by*

	    Takes a single character as argument, which must be found inside
	    single quotes, and might be given as the printable character itself,
	    the special value \t to denote a tabulation character, or `0x` then
	    an hexadecimal value read as the ASCII code for the character.

		This character is used as the quoting character in the `CSV` file,
	    and defaults to double-quote.

      - *fields not enclosed*

        By default, pgloader will use the double-quote character as the
        enclosing character. If you have a CSV file where fields are not
        enclosed and are using double-quote as an expected ordinary
        character, then use the option *fields not enclosed* for the CSV
        parser to accept those values.

      - *fields escaped by*

	    Takes either the special value *backslash-quote* or *double-quote*.
	    This value is used to recognize escaped field separators when they
	    are to be found within the data fields themselves. Defaults to
	    *double-quote*.

      - *fields terminated by*

	    Takes a single character as argument, which must be found inside
	    single quotes, and might be given as the printable character itself,
	    the special value \t to denote a tabulation character, or `0x` then
	    an hexadecimal value read as the ASCII code for the character.

	    This character is used as the *field separator* when reading the
	    `CSV` data.

      - *lines terminated by*

	    Takes a single character as argument, which must be found inside
	    single quotes, and might be given as the printable character itself,
	    the special value \t to denote a tabulation character, or `0x` then
	    an hexadecimal value read as the ASCII code for the character.

        This character is used to recognize *end-of-line* condition when
        reading the `CSV` data.

## LOAD FIXED COLS

This command instructs pgloader to load data from a text file containing
columns arranged in a *fixed size* manner. Here's an example:

    LOAD FIXED
         FROM inline
              (
               a from  0 for 10,
               b from 10 for  8,
               c from 18 for  8,
               d from 26 for 17 [null if blanks, trim right whitespace]
              )
         INTO postgresql:///pgloader?fixed
              (
                 a, b,
                 c time using (time-with-no-separator c),
                 d
              )

         WITH truncate

          SET client_encoding to 'latin1',
              work_mem to '14MB',
              standard_conforming_strings to 'on'

    BEFORE LOAD DO
         $$ drop table if exists fixed; $$,
         $$ create table fixed (
             a integer,
             b date,
             c time,
             d text
            );
         $$;

     01234567892008052011431250firstline
        01234562008052115182300left blank-padded
     12345678902008052208231560another line
      2345609872014092914371500                 
      2345678902014092914371520

The `fixed` format command accepts the following clauses and options:

  - *FROM*

    Filename where to load the data from. Accepts an *ENCODING* option. Use
    the `--list-encodings` option to know which encoding names are
    supported.

	The filename may be enclosed by single quotes, and could be one of the
	following special values:

	  - *inline*

        The data is found after the end of the parsed commands. Any number
        of empty lines between the end of the commands and the beginning of
        the data is accepted.

	  - *stdin*

	    Reads the data from the standard input stream.

	The *FROM* option also supports an optional comma separated list of
	*field* names describing what is expected in the `FIXED` data file.

	Each field name is composed of the field name followed with specific
	reader options for that field. Supported per-field reader options are
	the following, where only *start* and *length* are required.

      - *start*

	    Position in the line where to start reading that field's value. Can
	    be entered with decimal digits or `0x` then hexadecimal digits.

      - *length*

	    How many bytes to read from the *start* position to read that
	    field's value. Same format as *start*.

    Those optional parameters must be enclosed in square brackets and
	comma-separated:

	  - *terminated by*

		See the description of *field terminated by* below.

		The processing of this option is not currently implemented.

	  - *date format*

	    When the field is expected of the date type, then this option allows
	    to specify the date format used in the file.

		Date format string are template strings modeled against the
		PostgreSQL `to_char` template strings support, limited to the
		following patterns:
        
          - YYYY, YYY, YY for the year part
          - MM for the numeric month part
          - DD for the numeric day part
          - HH, HH12, HH24 for the hour part
          - am, AM, a.m., A.M.
          - pm, PM, p.m., P.M.
          - MI for the minutes part
          - SS for the seconds part
          - MS for the milliseconds part (4 digits)
          - US for the microseconds part (6 digits)
          - unparsed punctuation signs: - . * # @ T / \ and space
          
        Here's an example of a *date format* specification:
        
            column-name [date format 'YYYY-MM-DD HH24-MI-SS.US']

      - *null if*

	    This option takes an argument which is either the keyword *blanks*
	    or a double-quoted string.

		When *blanks* is used and the field value that is read contains only
	    space characters, then it's automatically converted to an SQL `NULL`
	    value.

		When a double-quoted string is used and that string is read as the
		field value, then the field value is automatically converted to an
		SQL `NULL` value.

	  - *trim both whitespace*, *trim left whitespace*, *trim right whitespace*

	    This option allows to trim whitespaces in the read data, either from
	    both sides of the data, or only the whitespace characters found on
	    the left of the streaing, or only those on the right of the string.

  - *WITH*

    When loading from a `CSV` file, the following options are supported:

	  - *truncate*

		When this option is listed, pgloader issues a `TRUNCATE` command
		against the PostgreSQL target table before reading the data file.

	  - *skip header*

	    Takes a numeric value as argument. Instruct pgloader to skip that
	    many lines at the beginning of the input file.

## LOAD COPY FORMATTED FILES

This commands instructs pgloader to load from a file containing COPY TEXT
data as described in the PostgreSQL documentation. Here's an example:

    LOAD COPY
         FROM copy://./data/track.copy
              (
                trackid, track, album, media, genre, composer,
                milliseconds, bytes, unitprice
              )
         INTO postgresql:///pgloader?track_full
    
         WITH truncate
    
          SET client_encoding to 'latin1',
              work_mem to '14MB',
              standard_conforming_strings to 'on'
     
    BEFORE LOAD DO
         $$ drop table if exists track_full; $$,
         $$ create table track_full (
              trackid      bigserial,
              track        text,
              album        text,
              media        text,
              genre        text,
              composer     text,
              milliseconds bigint,
              bytes        bigint,
              unitprice    numeric
            );
         $$;

The `COPY` format command accepts the following clauses and options:

  - *FROM*

    Filename where to load the data from. This support local files, HTTP
    URLs and zip files containing a single dbf file of the same name. Fetch
    such a zip file from an HTTP address is of course supported.


  - *WITH*
  
    When loading from a `COPY` file, the following options are supported:
    
	  - *truncate*

		When this option is listed, pgloader issues a `TRUNCATE` command
		against the PostgreSQL target table before reading the data file.

	  - *skip header*

	    Takes a numeric value as argument. Instruct pgloader to skip that
	    many lines at the beginning of the input file.

## LOAD DBF

This command instructs pgloader to load data from a `DBF` file. Here's an
example:

    LOAD DBF
	    FROM http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement/2013/dbf/reg2013.dbf
        INTO postgresql://user@localhost/dbname
        WITH truncate, create table;

The `dbf` format command accepts the following clauses and options:

  - *FROM*

    Filename where to load the data from. This support local files, HTTP
    URLs and zip files containing a single dbf file of the same name. Fetch
    such a zip file from an HTTP address is of course supported.

  - *WITH*

    When loading from a `DBF` file, the following options are supported:

	  - *truncate*

		When this option is listed, pgloader issues a `TRUNCATE` command
		against the PostgreSQL target table before reading the data file.

	  - *create table*

		When this option is listed, pgloader creates the table using the
		meta data found in the `DBF` file, which must contain a list of
		fields with their data type. A standard data type conversion from
		DBF to PostgreSQL is done.

	  - *table name*

	    This options expects as its value the possibly qualified name of the
	    table to create.

## LOAD IXF

This command instructs pgloader to load data from an IBM `IXF` file. Here's
an example:

    LOAD IXF
        FROM data/nsitra.test1.ixf
        INTO postgresql:///pgloader?nsitra.test1
        WITH truncate, create table
    
      BEFORE LOAD DO
       $$ create schema if not exists nsitra; $$,
       $$ drop table if exists nsitra.test1; $$;

The `ixf` format command accepts the following clauses and options:

  - *FROM*

    Filename where to load the data from. This support local files, HTTP
    URLs and zip files containing a single ixf file of the same name. Fetch
    such a zip file from an HTTP address is of course supported.

  - *WITH*

    When loading from a `IXF` file, the following options are supported:

	  - *truncate*

		When this option is listed, pgloader issues a `TRUNCATE` command
		against the PostgreSQL target table before reading the data file.

	  - *create table*

		When this option is listed, pgloader creates the table using the
		meta data found in the `DBF` file, which must contain a list of
		fields with their data type. A standard data type conversion from
		DBF to PostgreSQL is done.

	  - *table name*

	    This options expects as its value the possibly qualified name of the
	    table to create.

## LOAD ARCHIVE

This command instructs pgloader to load data from one or more files contained
in an archive. Currently the only supported archive format is *ZIP*, and the
archive might be downloaded from an *HTTP* URL.

Here's an example:

    LOAD ARCHIVE
       FROM /Users/dim/Downloads/GeoLiteCity-latest.zip
       INTO postgresql:///ip4r

       BEFORE LOAD DO
         $$ create extension if not exists ip4r; $$,
         $$ create schema if not exists geolite; $$,
         $$ create table if not exists geolite.location
           (
              locid      integer primary key,
              country    text,
              region     text,
              city       text,
              postalcode text,
              location   point,
              metrocode  text,
              areacode   text
           );
         $$,
         $$ create table if not exists geolite.blocks
           (
              iprange    ip4r,
              locid      integer
           );
         $$,
         $$ drop index if exists geolite.blocks_ip4r_idx; $$,
         $$ truncate table geolite.blocks, geolite.location cascade; $$

       LOAD CSV
            FROM FILENAME MATCHING ~/GeoLiteCity-Location.csv/
                 WITH ENCODING iso-8859-1
                 (
                    locId,
                    country,
                    region     null if blanks,
                    city       null if blanks,
                    postalCode null if blanks,
                    latitude,
                    longitude,
                    metroCode  null if blanks,
                    areaCode   null if blanks
                 )
            INTO postgresql:///ip4r?geolite.location
                 (
                    locid,country,region,city,postalCode,
                    location point using (format nil "(~a,~a)" longitude latitude),
                    metroCode,areaCode
                 )
            WITH skip header = 2,
                 fields optionally enclosed by '"',
                 fields escaped by double-quote,
                 fields terminated by ','

      AND LOAD CSV
            FROM FILENAME MATCHING ~/GeoLiteCity-Blocks.csv/
                 WITH ENCODING iso-8859-1
                 (
                    startIpNum, endIpNum, locId
                 )
            INTO postgresql:///ip4r?geolite.blocks
                 (
                    iprange ip4r using (ip-range startIpNum endIpNum),
                    locId
                 )
            WITH skip header = 2,
                 fields optionally enclosed by '"',
                 fields escaped by double-quote,
                 fields terminated by ','

       FINALLY DO
         $$ create index blocks_ip4r_idx on geolite.blocks using gist(iprange); $$;

The `archive` command accepts the following clauses and options:

   - *FROM*

	 Filename or HTTP URI where to load the data from. When given an HTTP
	 URL the linked file will get downloaded locally before processing.

	 If the file is a `zip` file, the command line utility `unzip` is used
	 to expand the archive into files in `$TMPDIR`, or `/tmp` if `$TMPDIR`
	 is unset or set to a non-existing directory.

	 Then the following commands are used from the top level directory where
	 the archive has been expanded.

   - command [ *AND* command ... ]

	 A series of commands against the contents of the archive, at the moment
	 only `CSV`,`'FIXED` and `DBF` commands are supported.

     Note that commands are supporting the clause *FROM FILENAME MATCHING*
     which allows the pgloader command not to depend on the exact names of
     the archive directories.

	 The same clause can also be applied to several files with using the
	 spelling *FROM ALL FILENAMES MATCHING* and a regular expression.

	 The whole *matching* clause must follow the following rule:

	     FROM [ ALL FILENAMES | [ FIRST ] FILENAME ] MATCHING

   - *FINALLY DO*

	 SQL Queries to run once the data is loaded, such as `CREATE INDEX`.

## LOAD MYSQL DATABASE

This command instructs pgloader to load data from a database connection. The
only supported database source is currently *MySQL*, and pgloader supports
dynamically converting the schema of the source database and the indexes
building.

A default set of casting rules are provided and might be overloaded and
appended to by the command.

Here's an example:

    LOAD DATABASE
         FROM      mysql://root@localhost/sakila
         INTO postgresql://localhost:54393/sakila

     WITH include drop, create tables, create indexes, reset sequences

      SET maintenance_work_mem to '128MB',
          work_mem to '12MB',
          search_path to 'sakila'

     CAST type datetime to timestamptz drop default drop not null using zero-dates-to-null,
          type date drop not null drop default using zero-dates-to-null,
          -- type tinyint to boolean using tinyint-to-boolean,
          type year to integer

     MATERIALIZE VIEWS film_list, staff_list

     -- INCLUDING ONLY TABLE NAMES MATCHING ~/film/, 'actor'
     -- EXCLUDING TABLE NAMES MATCHING ~<ory>
     -- DECODING TABLE NAMES MATCHING ~/messed/, ~/encoding/ AS utf8

     BEFORE LOAD DO
     $$ create schema if not exists sakila; $$;

The `database` command accepts the following clauses and options:

  - *FROM*

	Must be a connection URL pointing to a MySQL database. At the moment
	only MySQL is supported as a pgloader source.

    If the connection URI contains a table name, then only this table is
  	migrated from MySQL to PostgreSQL.

  - *WITH*

    When loading from a `MySQL` database, the following options are
    supported, and the efault *WITH* clause is: *no truncate*, *create
    tables*, *include drop*, *create indexes*, *reset sequences*, *foreign
    keys*, *downcase identifiers*.

    *WITH* options:

      - *include drop*

		When this option is listed, pgloader drop in the PostgreSQL
		connection all the table whose names have been found in the MySQL
		database. This option allows for using the same command several
		times in a row until you figure out all the options, starting
		automatically from a clean environment.

      - *include no drop*

	    When this option is listed, pgloader will not include any `DROP`
	    statement when loading the data.

	  - *truncate*

        When this option is listed, pgloader issue the `TRUNCATE` command
        against each PostgreSQL table just before loading data into it.

	  - *no truncate*

		When this option is listed, pgloader issues no `TRUNCATE` command.

	  - *create tables*

		When this option is listed, pgloader creates the table using the
		meta data found in the `MySQL` file, which must contain a list of
		fields with their data type. A standard data type conversion from
		DBF to PostgreSQL is done.

      - *create no tables*

	    When this option is listed, pgloader skips the creation of table
	    before lading data, target tables must then already exist.

	  - *create indexes*

	    When this option is listed, pgloader gets the definitions of all the
	    indexes found in the MySQL database and create the same set of index
	    definitions against the PostgreSQL database.

      - *create no indexes*

	    When this option is listed, pgloader skips the creating indexes.

      - *foreign keys*

	    When this option is listed, pgloader gets the definitions of all the
	    foreign keys found in the MySQL database and create the same set of
	    foreign key definitions against the PostgreSQL database.

      - *no foreign keys*

	    When this option is listed, pgloader skips creating foreign keys.

	  - *reset sequences*

        When this option is listed, at the end of the data loading and after
        the indexes have all been created, pgloader resets all the
        PostgreSQL sequences created to the current maximum value of the
        column they are attached to.

		The options *schema only* and *data only* have no effects on this
		option.

      - *reset no sequences*

	    When this option is listed, pgloader skips resetting sequences after
	    the load.

		The options *schema only* and *data only* have no effects on this
		option.

	  - *downcase identifiers*

	    When this option is listed, pgloader converts all MySQL identifiers
	    (table names, index names, column names) to *downcase*, except for
	    PostgreSQL *reserved* keywords.

		The PostgreSQL *reserved* keywords are determined dynamically by
		using the system function `pg_get_keywords()`.

	  - *quote identifiers*

		When this option is listed, pgloader quotes all MySQL identifiers so
		that their case is respected. Note that you will then have to do the
		same thing in your application code queries.

	  - *schema only*

	    When this option is listed pgloader refrains from migrating the data
	    over. Note that the schema in this context includes the indexes when
	    the option *create indexes* has been listed.

	  - *data only*

	    When this option is listed pgloader only issues the `COPY`
	    statements, without doing any other processing.

  - *CAST*

	The cast clause allows to specify custom casting rules, either to
	overload the default casting rules or to amend them with special cases.

	A casting rule is expected to follow one of the forms:

	    type <mysql-type-name> [ <guard> ... ] to <pgsql-type-name> [ <option> ... ]
		column <table-name>.<column-name> [ <guards> ] to ...

    It's possible for a *casting rule* to either match against a MySQL data
    type or against a given *column name* in a given *table name*. That
    flexibility allows to cope with cases where the type `tinyint` might
    have been used as a `boolean` in some cases but as a `smallint` in
    others.

	The *casting rules* are applied in order, the first match prevents
	following rules to be applied, and user defined rules are evaluated
	first.

    The supported guards are:

	  - *when default 'value'*

	    The casting rule is only applied against MySQL columns of the source
	    type that have given *value*, which must be a single-quoted or a
	    double-quoted string.

	  - *when typemod expression*

	    The casting rule is only applied against MySQL columns of the source
	    type that have a *typemod* value matching the given *typemod
	    expression*. The *typemod* is separated into its *precision* and
	    *scale* components.

		Example of a cast rule using a *typemod* guard:

		    type char when (= precision 1) to char keep typemod

        This expression casts MySQL `char(1)` column to a PostgreSQL column
        of type `char(1)` while allowing for the general case `char(N)` will
        be converted by the default cast rule into a PostgreSQL type
        `varchar(N)`.

	The supported casting options are:

	  - *drop default*, *keep default*

        When the option *drop default* is listed, pgloader drops any
        existing default expression in the MySQL database for columns of the
        source type from the `CREATE TABLE` statement it generates.

	    The spelling *keep default* explicitly prevents that behaviour and
	    can be used to overload the default casting rules.

	  - *drop not null*, *keep not null*

        When the option *drop not null* is listed, pgloader drops any
        existing `NOT NULL` constraint associated with the given source
        MySQL datatype when it creates the tables in the PostgreSQL
        database.

	    The spelling *keep not null* explicitly prevents that behaviour and
	    can be used to overload the default casting rules.

      - *drop typemod*, *keep typemod*

	    When the option *drop typemod* is listed, pgloader drops any
	    existing *typemod* definition (e.g. *precision* and *scale*) from
	    the datatype definition found in the MySQL columns of the source
	    type when it created the tables in the PostgreSQL database.

	    The spelling *keep typemod* explicitly prevents that behaviour and
	    can be used to overload the default casting rules.

	  - *using*

	    This option takes as its single argument the name of a function to
	    be found in the `pgloader.transforms` Common Lisp package. See above
	    for details.

        It's possible to augment a default cast rule (such as one that
        applies against `ENUM` data type for example) with a *transformation
        function* by omitting entirely the `type` parts of the casting rule,
        as in the following example:

            column enumerate.foo using empty-string-to-null

  - *MATERIALIZE VIEWS*

    This clause allows you to implement custom data processing at the data
    source by providing a *view definition* against which pgloader will
    query the data. It's not possible to just allow for plain `SQL` because
    we want to know a lot about the exact data types of each column involved
    in the query output.

	This clause expect a comma separated list of view definitions, each one
	being either the name of an existing view in your database or the
	following expression:

	  *name* `AS` `$$` *sql query* `$$`

	The *name* and the *sql query* will be used in a `CREATE VIEW` statement
	at the beginning of the data loading, and the resulting view will then
	be dropped at the end of the data loading.

  - *MATERIALIZE ALL VIEWS*
  
    Same behaviour as *MATERIALIZE VIEWS* using the dynamic list of views as
    returned by MySQL rather than asking the user to specify the list.

  - *INCLUDING ONLY TABLE NAMES MATCHING*

	Introduce a comma separated list of table names or *regular expression*
	used to limit the tables to migrate to a sublist.

    Example:

	    INCLUDING ONLY TABLE NAMES MATCHING ~/film/, 'actor'

  - *EXCLUDING TABLE NAMES MATCHING*

    Introduce a comma separated list of table names or *regular expression*
    used to exclude table names from the migration. This filter only applies
    to the result of the *INCLUDING* filter.

	    EXCLUDING TABLE NAMES MATCHING ~<ory>

  - *DECODING TABLE NAMES MATCHING*

    Introduce a comma separated list of table names or *regular expressions*
    used to force the encoding to use when processing data from MySQL. If
    the data encoding known to you is different from MySQL's idea about it,
    this is the option to use.

        DECODING TABLE NAMES MATCHING ~/messed/, ~/encoding/ AS utf8

    You can use as many such rules as you need, all with possibly different
    encodings.

### LIMITATIONS

The `database` command currently only supports MySQL source database and has
the following limitations:

  - Views are not migrated,

	Supporting views might require implementing a full SQL parser for the
	MySQL dialect with a porting engine to rewrite the SQL against
	PostgreSQL, including renaming functions and changing some constructs.

	While it's not theoretically impossible, don't hold your breath.

  - Triggers are not migrated

	The difficulty of doing so is not yet assessed.

  - `ON UPDATE CURRENT_TIMESTAMP` is currently not migrated

	It's simple enough to implement, just not on the priority list yet.

  - Of the geometric datatypes, only the `POINT` database has been covered.
    The other ones should be easy enough to implement now, it's just not
    done yet.

### DEFAULT MySQL CASTING RULES

When migrating from MySQL the following Casting Rules are provided:

Numbers:

  - type int with extra auto_increment to serial when (< precision 10)
  - type int with extra auto_increment to bigserial when (<= 10 precision)
  - type int to int       when  (< precision 10)
  - type int to bigint    when  (<= 10 precision)
  - type tinyint   with extra auto_increment to serial
  - type smallint  with extra auto_increment to serial
  - type mediumint with extra auto_increment to serial
  - type bigint    with extra auto_increment to bigserial

  - type tinyint to boolean when (= 1 precision) using tinyint-to-boolean

  - type tinyint to smallint   drop typemod
  - type smallint to smallint  drop typemod
  - type mediumint to integer  drop typemod
  - type integer to integer    drop typemod
  - type float to float        drop typemod
  - type bigint to bigint      drop typemod
  - type double to double precision drop typemod

  - type numeric to numeric keep typemod
  - type decimal to decimal keep typemod

Texts:

  - type char       to varchar keep typemod
  - type varchar    to text
  - type tinytext   to text
  - type text       to text
  - type mediumtext to text
  - type longtext   to text

Binary:

  - type binary     to bytea
  - type varbinary  to bytea
  - type tinyblob   to bytea
  - type blob       to bytea
  - type mediumblob to bytea
  - type longblob   to bytea

Date:

  - type datetime when default "0000-00-00 00:00:00" and not null
    to timestamptz drop not null drop default
	using zero-dates-to-null

  - type datetime when default "0000-00-00 00:00:00"
    to timestamptz drop default
	using zero-dates-to-null

  - type timestamp when default "0000-00-00 00:00:00" and not null
    to timestamptz drop not null drop default
	using zero-dates-to-null

  - type timestamp when default "0000-00-00 00:00:00"
    to timestamptz drop default
	using zero-dates-to-null

  - type date when default "0000-00-00" to date drop default
	using zero-dates-to-null

  - type date to date
  - type datetime to timestamptz
  - type timestamp to timestamptz
  - type year to integer drop typemod

Geometric:

  - type point to point using pgloader.transforms::convert-mysql-point

Enum types are declared inline in MySQL and separately with a `CREATE TYPE`
command in PostgreSQL, so each column of Enum Type is converted to a type
named after the table and column names defined with the same labels in the
same order.

When the source type definition is not matched in the default casting rules
nor in the casting rules provided in the command, then the type name with
the typemod is used.

## LOAD SQLite DATABASE

This command instructs pgloader to load data from a SQLite file. Automatic
discovery of the schema is supported, including build of the indexes.

Here's an example:

    load database
         from sqlite:///Users/dim/Downloads/lastfm_tags.db
         into postgresql:///tags

     with include drop, create tables, create indexes, reset sequences

      set work_mem to '16MB', maintenance_work_mem to '512 MB';

The `sqlite` command accepts the following clauses and options:

  - *FROM*

    Path or HTTP URL to a SQLite file, might be a `.zip` file.

  - *WITH*

    When loading from a `SQLite` database, the following options are
    supported:

    When loading from a `SQLite` database, the following options are
    supported, and the default *WITH* clause is: *no truncate*, *create
    tables*, *include drop*, *create indexes*, *reset sequences*, *downcase
    identifiers*, *encoding 'utf-8'*.

      - *include drop*

		When this option is listed, pgloader drop in the PostgreSQL
		connection all the table whose names have been found in the SQLite
		database. This option allows for using the same command several
		times in a row until you figure out all the options, starting
		automatically from a clean environment.

      - *include no drop*

	    When this option is listed, pgloader will not include any `DROP`
	    statement when loading the data.

	  - *truncate*

        When this option is listed, pgloader issue the `TRUNCATE` command
        against each PostgreSQL table just before loading data into it.

	  - *no truncate*

		When this option is listed, pgloader issues no `TRUNCATE` command.

	  - *create tables*

		When this option is listed, pgloader creates the table using the
		meta data found in the `SQLite` file, which must contain a list of
		fields with their data type. A standard data type conversion from
		DBF to PostgreSQL is done.

      - *create no tables*

	    When this option is listed, pgloader skips the creation of table
	    before lading data, target tables must then already exist.

	  - *create indexes*

	     When this option is listed, pgloader gets the definitions of all
	     the indexes found in the SQLite database and create the same set of
	     index definitions against the PostgreSQL database.

      - *create no indexes*

	    When this option is listed, pgloader skips the creating indexes.

	  - *reset sequences*

        When this option is listed, at the end of the data loading and after
        the indexes have all been created, pgloader resets all the
        PostgreSQL sequences created to the current maximum value of the
        column they are attached to.

      - *reset no sequences*

	    When this option is listed, pgloader skips resetting sequences after
	    the load.

		The options *schema only* and *data only* have no effects on this
		option.

	  - *schema only*

	    When this option is listed pgloader will refrain from migrating the
	    data over. Note that the schema in this context includes the indexes
	    when the option *create indexes* has been listed.

	  - *data only*

	    When this option is listed pgloader only issues the `COPY`
	    statements, without doing any other processing.
        
      - *encoding*
      
        This option allows to control which encoding to parse the SQLite
        text data with. Defaults to UTF-8.

  - *CAST*

	The cast clause allows to specify custom casting rules, either to
	overload the default casting rules or to amend them with special cases.

    Please refer to the MySQL CAST clause for details.

  - *INCLUDING ONLY TABLE NAMES MATCHING*

	Introduce a comma separated list of table names or *regular expression*
	used to limit the tables to migrate to a sublist.

    Example:

	    INCLUDING ONLY TABLE NAMES MATCHING ~/film/, 'actor'

  - *EXCLUDING TABLE NAMES MATCHING*

    Introduce a comma separated list of table names or *regular expression*
    used to exclude table names from the migration. This filter only applies
    to the result of the *INCLUDING* filter.

	    EXCLUDING TABLE NAMES MATCHING ~<ory>

### DEFAULT SQLite CASTING RULES

When migrating from SQLite the following Casting Rules are provided:

Numbers:

  - type tinyint to smallint

  - type float to float   using float-to-string
  - type real to real     using float-to-string
  - type double to double precision     using float-to-string
  - type numeric to numeric     using float-to-string

Texts:

  - type character  to text drop typemod
  - type varchar    to text drop typemod
  - type nvarchar   to text drop typemod
  - type char       to text drop typemod
  - type nchar      to text drop typemod
  - type nvarchar   to text drop typemod
  - type clob       to text drop typemod

Binary:

  - type blob       to bytea

Date:

  - type datetime    to timestamptz using sqlite-timestamp-to-timestamp
  - type timestamp   to timestamptz using sqlite-timestamp-to-timestamp
  - type timestamptz to timestamptz using sqlite-timestamp-to-timestamp


## LOAD MS SQL DATABASE

This command instructs pgloader to load data from a MS SQL database.
Automatic discovery of the schema is supported, including build of the
indexes, primary and foreign keys constraints.

Here's an example:

    load database
         from mssql://user@host/dbname
         into postgresql:///dbname

    including only table names like 'GlobalAccount' in schema 'dbo'
    
    set work_mem to '16MB', maintenance_work_mem to '512 MB'

    before load do $$ drop schema if exists dbo cascade; $$;

The `mssql` command accepts the following clauses and options:

  - *FROM*

    Connection string to an existing MS SQL database server that listens and
    welcome external TCP/IP connection. As pgloader currently piggybacks on
    the FreeTDS driver, to change the port of the server please export the
    `TDSPORT` environment variable.

  - *WITH*

    When loading from a `MS SQL` database, the same options as when loading
    a `MySQL` database are supported. Please refer to the MySQL section.

  - *CAST*

	The cast clause allows to specify custom casting rules, either to
	overload the default casting rules or to amend them with special cases.

    Please refer to the MySQL CAST clause for details.

  - *INCLUDING ONLY TABLE NAMES LIKE '...' [, '...'] IN SCHEMA '...'*

	Introduce a comma separated list of table name patterns used to limit
	the tables to migrate to a sublist. More than one such clause may be
	used, they will be accumulated together.

    Example:

	    including only table names lile 'GlobalAccount' in schema 'dbo'

  - *EXCLUDING TABLE NAMES LIKE '...' [, '...'] IN SCHEMA '...'*

    Introduce a comma separated list of table name patterns used to exclude
    table names from the migration. This filter only applies to the result
    of the *INCLUDING* filter.

	    EXCLUDING TABLE NAMES MATCHING 'LocalAccount' in schema 'dbo'

### DEFAULT MS SQL CASTING RULES

When migrating from MS SQL the following Casting Rules are provided:

Numbers:

  - type tinyint to smallint

  - type float to float   using float-to-string
  - type real to real     using float-to-string
  - type double to double precision     using float-to-string
  - type numeric to numeric     using float-to-string
  - type decimal to numeric     using float-to-string
  - type money to numeric     using float-to-string
  - type smallmoney to numeric     using float-to-string

Texts:

  - type char      to text drop typemod
  - type nchat     to text drop typemod
  - type varchar   to text drop typemod
  - type nvarchar  to text drop typemod
  - type xml       to text drop typemod

Binary:

  - type binary    to bytea using byte-vector-to-bytea
  - type varbinary to bytea using byte-vector-to-bytea

Date:

  - type datetime    to timestamptz
  - type datetime2   to timestamptz

Others:

  - type bit to boolean
  - type hierarchyid to bytea
  - type geography to bytea
  - type uniqueidentifier to uuid using sql-server-uniqueidentifier-to-uuid

## TRANSFORMATION FUNCTIONS

Some data types are implemented in a different enough way that a
transformation function is necessary. This function must be written in
`Common lisp` and is searched in the `pgloader.transforms` package.

Some default transformation function are provided with pgloader, and you can
use the `--load` command line option to load and compile your own lisp file
into pgloader at runtime. For your functions to be found, remember to begin
your lisp file with the following form:

    (in-package #:pgloader.transforms)

The provided transformation functions are:

  - *zero-dates-to-null*

    When the input date is all zeroes, return `nil`, which gets loaded as a
    PostgreSQL `NULL` value.

  - *date-with-no-separator*

    Applies *zero-dates-to-null* then transform the given date into a format
    that PostgreSQL will actually process:

	    In:  "20041002152952"
		Out: "2004-10-02 15:29:52"
 
  - *time-with-no-separator*

    Transform the given time into a format that PostgreSQL will actually
    process:

	    In:  "08231560"
		Out: "08:23:15.60"
 
  - *tinyint-to-boolean*

    As MySQL lacks a proper boolean type, *tinyint* is often used to
    implement that. This function transforms `0` to `'false'` and anything
    else to `'true`'.

  - *bits-to-boolean*

    As MySQL lacks a proper boolean type, *BIT* is often used to implement
    that. This function transforms 1-bit bit vectors from `0` to `f` and any
    other value to `t`..

  - *int-to-ip*

    Convert an integer into a dotted representation of an ip4.

        In:  18435761
        Out: "1.25.78.177"

  - *ip-range*

    Converts a couple of integers given as strings into a range of ip4.

	    In:  "16825344" "16825599"
		Out: "1.0.188.0-1.0.188.255"

  - *convert-mysql-point*

    Converts from the `astext` representation of points in MySQL to the
    PostgreSQL representation.

        In:  "POINT(48.5513589 7.6926827)"
        Out: "(48.5513589,7.6926827)"

  - *float-to-string*

    Converts a Common Lisp float into a string suitable for a PostgreSQL float:

	    In:  100.0d0
		Out: "100.0"

  - *set-to-enum-array*

    Converts a string representing a MySQL SET into a PostgreSQL Array of
    Enum values from the set.

	    In: "foo,bar"
	    Out: "{foo,bar}"

  - *empty-string-to-null*

    Convert an empty string to a null.

  - *right-trimg*

    Remove whitespace at end of string.

  - *byte-vector-to-bytea*

    Transform a simple array of unsigned bytes to the PostgreSQL bytea Hex
    Format representation as documented at
    http://www.postgresql.org/docs/9.3/interactive/datatype-binary.html

  - *sqlite-timestamp-to-timestamp*
  
    SQLite type system is quite interesting, so cope with it here to produce
    timestamp literals as expected by PostgreSQL. That covers year only on 4
    digits, 0 dates to null, and proper date strings.

  - *sql-server-uniqueidentifier-to-uuid*
  
    The SQL Server driver receives data fo type uniqueidentifier as byte
    vector that we then need to convert to an UUID string for PostgreSQL
    COPY input format to process.
    
  - *unix-timestamp-to-timestamptz*
  
    Converts a unix timestamp (number of seconds elapsed since beginning of
    1970) into a proper PostgreSQL timestamp format.

## LOAD MESSAGES

This command is still experimental and allows receiving messages via
UDP using a syslog like format, and, depending on rule matching, loads
named portions of the data stream into a destination table.

    LOAD MESSAGES
        FROM syslog://localhost:10514/

     WHEN MATCHES rsyslog-msg IN apache
	  REGISTERING timestamp, ip, rest
             INTO postgresql://localhost/db?logs.apache
              SET guc_1 = 'value', guc_2 = 'other value'

     WHEN MATCHES rsyslog-msg IN others
      REGISTERING timestamp, app-name, data
             INTO postgresql://localhost/db?logs.others
              SET guc_1 = 'value', guc_2 = 'other value'

        WITH apache = rsyslog
             DATA   = IP REST
             IP     = 1*3DIGIT "." 1*3DIGIT "."1*3DIGIT "."1*3DIGIT
             REST   = ~/.*/

        WITH others = rsyslog;

As the command is still experimental the options might be changed in the
future and the details are not documented.

## AUTHOR

Dimitri Fontaine <dimitri@2ndQuadrant.fr>

## SEE ALSO

PostgreSQL COPY documentation at <http://www.postgresql.org/docs/9.3/static/sql-copy.html>.

The pgloader source code, binary packages, documentation and examples may be
downloaded from <http://pgloader.io/>.
