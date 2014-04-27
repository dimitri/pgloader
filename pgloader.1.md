# pgloader(1) -- PostgreSQL data loader

## SYNOPSIS

`pgloader` [<options>] [<command-file>]...

## DESCRIPTION

pgloader loads data from different sources into PostgreSQL. It can tranform
the data it reads on the fly and send raw SQL before and after the loading.
It uses the `COPY` PostgreSQL protocol to stream the data into the server,
and manages errors by filling a pair fo *reject.dat* and *reject.log* files.

pgloader operates from commands which are read from files:

    pgloader commands.load

## OPTIONS

  * `-h`, `--help`:
    Show command usage summary and exit.

  * `-V`, `--version`:
    Show pgloader version string and exit.

  * `-v`, `--verbose`:
    Be verbose.

  * `-q`, `--quiet:
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

  * `-E`, `--list-encodings`:
    List known encodings in this version of pgloader.

  * `-U`, `--upgrade-config`:
    Parse given files in the command line as `pgloader.conf` files with the
   `INI` syntax that was in use in pgloader versions 2.x, and output the
   new command syntax for pgloader on standard output.

  * -l <file>, --load <file>:
    Specify a lisp <file> to compile and load into the pgloader image before
    reading the commands, allowing to define extra transformation function.
    Those functions should be defined in the `pgloader.transforms` package.
    This option can appear more than once in the command line.

To get the maximum amount of debug information, you can use both the
`--verbose` and the `--debug` switches at the same time, which is equivalent
to saying `--client-min-messages data`. Then the log messages will show the
data being processed, in the cases where the code has explicit support for
it.

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

The `.dat` file is formated in PostgreSQL the text COPY format as documented
in [http://www.postgresql.org/docs/9.2/static/sql-copy.html#AEN66609]().

## A NOTE ABOUT PERFORMANCES

pgloader has been developped with performances in mind, to be able to cope
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

## COMMANDS

pgloader support the following commands:

  - `LOAD CSV`
  - `LOAD FIXED`
  - `LOAD DBF`
  - `LOAD SQLite`
  - `LOAD MYSQL`
  - `LOAD ARCHIVE`
  - `LOAD DATABASE`
  - `LOAD MESSAGES`

The pgloader commands follow the same grammar rules. Each of them might
support only a subset of the general options and provide specific options.

    LOAD <something>
	     FROM <source-url>  [ WITH <source-options> ]
		 INTO <postgresql-url>

	[ WITH <load-options> ]

	[ SET <postgresql-settings> ]
	;

The main clauses are the `LOAD`, `FROM`, `INTO` and `WITH` clauses that each
command implements. Some command then implement the `SET` command, or some
specific clauses such as the `CAST` clause.

### Connection String

The `<source-url>` parameter is expected to be given as a *Connection URI*
as documented in the PostgreSQL documentation at
http://www.postgresql.org/docs/9.3/static/libpq-connect.html#LIBPQ-CONNSTRING.

    postgresql://[user[:password]@][netloc][:port][/dbname][?schema.table]

Where:

  - *user*

    Can contain any character, including colon (`:`) which must then be
    doubled (`::`) and at-sign (`@`) which must then be doubled (`@@`).

  - *password*

	Can contain any character, including that at sign (`@`) which must then
	be doubled (`@@`). To leave the password empty, when the *user* name
	ends with at at sign, you then have to use the syntax user:@.

  - *netloc*

    Can be either a hostname in dotted notation, or an ipv4, or an unix
    domain socket path. Empty is the default network location, under a
    system providing *unix domain socket* that method is prefered, otherwise
    the *netloc* default to `localhost`.

	It's possible to force the *unix domain socket* path by using the syntax
	`unix:/path/to/where/the/socket/file/is`, so to force a non default
	socket path and a non default port, you would have:

	    postgresql://unix:/tmp:54321/dbname

  - *dbname*

	Should be a proper identifier (letter followed by a mix of letters,
	digits and the punctuation signs comma (`,`), dash (`-`) and underscore
	(`_`).

  - The only optionnal parameter should be a possibly qualified table name.

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
data source kind, such as the CSV *skip header* options.

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
    controling the memory needs of pgloader as a trade-off to the
    performances characteristics, and not about parallel activity of
    pgloader.
    
Other options are specific to each input source, please refer to specific
parts of the documentation for their listing and covering.

### LOAD CSV

This command instructs pgloader to load data from a `CSV` file. Here's an
example:

    LOAD CSV
       FROM 'GeoLiteCity-Blocks.csv' WITH ENCODING iso-646-us
            (
               startIpNum, endIpNum, locId
            )
       INTO postgresql://user@localhost:54393/dbname?geolite.blocks
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

	The *FROM* option also supports an optional comma separated list of
	*field* names describing what is expected in the `CSV` data file.

	Each field name can be either only one name or a name following with
	specific reader options for that field. Supported per-field reader
	options are:

	  - *terminated by*

		See the description of *field terminated by* below.

		The processing of this option is not currently implemented.

	  - *date format*

	    When the field is expected of the date type, then this option allows
	    to specify the date format used in the file.

		The processing of this option is not currently implemented.

      - *null if*

	    This option takes an argument which is either the keyword *blanks*
	    or a double-quoted string.

		When *blanks* is used and the field value that is read contains only
	    space characters, then it's automatically converted to an SQL `NULL`
	    value.

		When a double-quoted string is used and that string is read as the
		field value, then the field value is automatically converted to an
		SQL `NULL` value.

  - *INTO*

	The PostgreSQL connection URI must contains the name of the target table
	where to load the data into. That table must have already been created
	in PostgreSQL, and the name might be schema qualified.

	Then *INTO* option also supports an optional comma separated list of
	target columns, which are either the name of an input *field* or the
	whitespace separated list of the target column name, its PostgreSQL data
	type and a *USING* expression.

	The *USING* expression can be any valid Common Lisp form and will be
	read with the current package set to `pgloader.transforms`, so that you
	can use functions defined in that package, such as functions loaded
	dynamically with the `--load` command line parameter.

    Each *USING* expression is compiled at runtime to native code, and will
    be called in a context such as:

	    (destructuring-bind (field-name-1 field-name-2 ...)
		    row
		  (list column-name-1
		        column-name-2
				(expression column-name-1 column-name-2)))

    This feature allows pgloader to load any number of fields in a CSV file
    into a possibly different number of columns in the database, using
    custom code for that projection.

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
	    an hexadecimal value read as the ascii code for the character.

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
	    an hexadecimal value read as the ascii code for the character.

	    This character is used as the *field separator* when reading the
	    `CSV` data.

      - *lines terminated by*

	    Takes a single character as argument, which must be found inside
	    single quotes, and might be given as the printable character itself,
	    the special value \t to denote a tabulation character, or `0x` then
	    an hexadecimal value read as the ascii code for the character.

        This character is used to recognize *end-of-line* condition when
        reading the `CSV` data.

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

  - *AFTER LOAD DO*

	Same format as *BEFORE LOAD DO*, the dollar-quoted queries found in that
	section are executed once the load is done. That's the right time to
	create indexes and constraints, or re-enable triggers.

### LOAD FIXED COLS

This command instructs pgloader to load data from a text file containing
columns arranged in a *fixed size* manner. Here's an example:

    LOAD FIXED
         FROM inline (a 0 10, b 10 8, c 18 8, d 26 17)
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

	  - *terminated by*

		See the description of *field terminated by* below.

		The processing of this option is not currently implemented.

	  - *date format*

	    When the field is expected of the date type, then this option allows
	    to specify the date format used in the file.

		The processing of this option is not currently implemented.

      - *null if*

	    This option takes an argument which is either the keyword *blanks*
	    or a double-quoted string.

		When *blanks* is used and the field value that is read contains only
	    space characters, then it's automatically converted to an SQL `NULL`
	    value.

		When a double-quoted string is used and that string is read as the
		field value, then the field value is automatically converted to an
		SQL `NULL` value.

  - *INTO*

	The PostgreSQL connection URI must contains the name of the target table
	where to load the data into. That table must have already been created
	in PostgreSQL, and the name might be schema qualified.

	Then *INTO* option also supports an optional comma separated list of
	target columns, which are either the name of an input *field* or the
	whitespace separated list of the target column name, its PostgreSQL data
	type and a *USING* expression.

	The *USING* expression can be any valid Common Lisp form and will be
	read with the current package set to `pgloader.transforms`, so that you
	can use functions defined in that package, such as functions loaded
	dynamically with the `--load` command line parameter.

    Each *USING* expression is compiled at runtime to native code, and will
    be called in a context such as:

	    (destructuring-bind (field-name-1 field-name-2 ...)
		    row
		  (list column-name-1
		        column-name-2
				(expression column-name-1 column-name-2)))

    This feature allows pgloader to load any number of fields in a CSV file
    into a possibly different number of columns in the database, using
    custom code for that projection.

  - *WITH*

    When loading from a `CSV` file, the following options are supported:

	  - *truncate*

		When this option is listed, pgloader issues a `TRUNCATE` command
		against the PostgreSQL target table before reading the data file.

	  - *skip header*

	    Takes a numeric value as argument. Instruct pgloader to skip that
	    many lines at the beginning of the input file.

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

  - *AFTER LOAD DO*

	Same format as *BEFORE LOAD DO*, the dollar-quoted queries found in that
	section are executed once the load is done. That's the right time to
	create indexes and constraints, or re-enable triggers.

### LOAD DBF

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

  - *INTO*

  	The PostgreSQL connection URI. If it doesn't have a table name in the
  	target, then the name part of the filename will be used as a table name.

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

  - *SET*

	This clause allows to specify session parameters to be set for all the
    sessions opened by pgloader. It expects a list of parameter name, the
    equal sign, then the single-quoted value as a comma separated list.

 	The names and values of the parameters are not validated by pgloader,
 	they are given as-is to PostgreSQL.

### LOAD ARCHIVE

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

   - *INTO*

	 A PostgreSQL database connection URL is expected and will be used in
	 the *BEFORE LOAD DO* and *FINALLY DO* clauses.

   - *BEFORE LOAD DO*

	 You can run SQL queries against the database before loading from the
	 data files found in the archive. Most common SQL queries are `CREATE
	 TABLE IF NOT EXISTS` so that the data can be loaded.

	 Each command must be *dollar-quoted*: it must begin and end with a
	 double dollar sign, `$$`. Queries are then comma separated. No extra
	 punctuation is expected after the last SQL query.

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

### LOAD MYSQL DATABASE

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

  - *INTO*

  	The target PostgreSQL connection URI.

  - *WITH*

    When loading from a `MySQL` database, the following options are
    supported:

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

		When this topion is listed, pgloader issues no `TRUNCATE` command.

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

  - *SET*

    This clause allows to specify session parameters to be set for all the
    sessions opened by pgloader. It expects a list of parameter name, the
    equal sign, then the single-quoted value as a comma separated list.

	The names and values of the parameters are not validated by pgloader,
	they are given as-is to PostgreSQL.

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

	    The spelling *keep default* explicitely prevents that behavior and
	    can be used to overlad the default casting rules.

	  - *drop not null*, *keep not null*

        When the option *drop not null* is listed, pgloader drops any
        existing `NOT NULL` constraint associated with the given source
        MySQL datatype when it creates the tables in the PostgreSQL
        database.

	    The spelling *keep not null* explicitely prevents that behavior and
	    can be used to overlad the default casting rules.

      - *drop typemod*, *keep typemod*

	    When the option *drop typemod* is listed, pgloader drops any
	    existing *typemod* definition (e.g. *precision* and *scale*) from
	    the datatype definition found in the MySQL columns of the source
	    type when it created the tables in the PostgreSQL database.

	    The spelling *keep typemod* explicitely prevents that behavior and
	    can be used to overlad the default casting rules.

	  - *using*

	    This option takes as its single argument the name of a function to
	    be found un the `pgloader.transforms` Common Lisp package. See above
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

  - *BEFORE LOAD DO*

	 You can run SQL queries against the database before loading the data
	 from the `MySQL` database. You can use that clause to execute a `CREATE
	 SCHEMA IF NOT EXISTS` command in case you want to load your data into
	 some specific schema. To ensure your load happens in the right schema,
	 consider setting the `search_path` in the *SET* clause.

	 Each command must be *dollar-quoted*: it must begin and end with a
	 double dollar sign, `$$`. Dollar-quoted queries are then comma
	 separated. No extra punctuation is expected after the last SQL query.

  - *AFTER LOAD DO*

	Same format as *BEFORE LOAD DO*, the dollar-quoted queries found in that
	section are executed once the load is done.

### LIMITATIONS

The `database` command currently only supports MySQL source database and has
the following limitations:

  - Views are not migrated,

	Supporting views might require implemeting a full SQL parser for the
	MySQL dialect with a porting engine to rewrite the SQL against
	PostgreSQL, including renaming functions and changing some constructs.

	While it's not theorically impossible, don't hold your breath.

  - Triggers are not migrated

	The difficulty of doing so is not yet assessed.

  - `ON UPDATE CURRENT_TIMESTAMP` is currently not migrated

	It's simple enough to implement, just not on the priority list yet.

  - Of the geometric datatypes, onle the `POINT` database has been covered.
    The other ones should be easy enough to implement now, it's just not
    done yet.

  - The PostgreSQL `client_encoding` should be set to `UFT8` as pgloader is
    using that setting when asking MySQL for its data.

### DEFAULT MySQL CASTING RULES

When migrating from MySQL the following Casting Rules are provided:

Numbers:

  - type int to serial    when auto_increment and (< precision 10)
  - type int to bigserial when auto_increment and (<= 10 precision)
  - type int to int       when not auto_increment and (< precision 10)
  - type int to bigint    when not auto_increment and (<= 10 precision)
  - type smallint to serial  when auto_increment
  - type bigint to bigserial when auto_increment

  - type tinyint to boolean when (= 1 precision) using tinyint-to-boolean

  - type tinyint to smallint   drop typemod
  - type smallint to smallint  drop typemod
  - type mediumint to integer  drop typemod
  - type integer to integer    drop typemod
  - type float to float        drop typemod
  - type bigint to bigint      drop typemod
  - type double to double precision drop typemod

  - type numeric to numeric keep typemod
  - type decimal to deciman keep typemod

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

### LOAD SQLite DATABASE

This command instructs pgloader to load data from a SQLite file. Automatic
discovery of the schema is supported, including build of the indexes.

Here's an example:

    load database
         from sqlite:///Users/dim/Downloads/lastfm_tags.db
         into postgresql:///tags

     with drop tables, create tables, create indexes, reset sequences

      set work_mem to '16MB', maintenance_work_mem to '512 MB';

The `sqlite` command accepts the following clauses and options:

  - *FROM*

    Path or HTTP URL to a SQLite file, might be a `.zip` file.

  - *INTO*

  	The target PostgreSQL connection URI. If that URL containst a
  	*table-name* element, then that single table will get migrated.

  - *WITH*

    When loading from a `SQLite` database, the following options are
    supported:

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

		When this topion is listed, pgloader issues no `TRUNCATE` command.

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
	     data over. Note that the schema in this context includes the
	     indexes when the option *create indexes* has been listed.

	  - *data only*

	    When this option is listed pgloader only issues the `COPY`
	    statements, without doing any other processing.

  - *SET*

    This clause allows to specify session parameters to be set for all the
    sessions opened by pgloader. It expects a list of parameter name, the
    equal sign, then the single-quoted value as a comma separated list.

	The names and values of the parameters are not validated by pgloader,
	they are given as-is to PostgreSQL.

  - *INCLUDING ONLY TABLE NAMES MATCHING*

	Introduce a comma separated list of table names or *regular expression*
	used to limit the tables to migrate to a sublist.

    Example:

	    INCLUDING ONLY TABLE NAMES MATCHING ~/film/, 'actor'

  - *EXCLUDING TABLE NAMES MATCHING*

    Introduce a comma separated list of table names or *rugular expression*
    used to exclude table names from the migration. This filter only applies
    to the result of the *INCLUDING* filter.

	    EXCLUDING TABLE NAMES MATCHING ~<ory>

### TRANSFORMATION FUNCTIONS

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

  - *tinyint-to-boolean*

    As MySQL lacks a proper boolean type, *tinyint* is often used to
    implement that. This function transforms `0` to `'false'` and anything
    else to `'true`'.

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

  - *right-trimg*

    Remove whitespaces at end of string.

  - *byte-vector-to-bytea*

    Transform a simple array of unsigned bytes to the PostgreSQL bytea Hex
    Format representation as documented at
    http://www.postgresql.org/docs/9.3/interactive/datatype-binary.html

## LOAD MESSAGES

This command is still experimental and allows to receive messages in UDP
with a syslod like format, and depending on matching rules load named parts
them to a destination table.

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

The pgloader source code and all documentation may be downloaded from
<http://tapoueh.org/pgloader/>.
