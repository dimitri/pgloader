Command Syntax
==============

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

.. _common_clauses:

Command Clauses
---------------

The pgloader command syntax allows composing CLAUSEs together. Some clauses
are specific to the FROM source-type, most clauses are always available.

FROM
----

The *FROM* clause specifies where to read the data from, and each command
introduces its own variant of sources. For instance, the *CSV* source
supports `inline`, `stdin`, a filename, a quoted filename, and a *FILENAME
MATCHING* clause (see above); whereas the *MySQL* source only supports a
MySQL database URI specification.

INTO
----

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
----

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
---

This clause allows to specify session parameters to be set for all the
sessions opened by pgloader. It expects a list of parameter name, the equal
sign, then the single-quoted value as a comma separated list.

The names and values of the parameters are not validated by pgloader, they
are given as-is to PostgreSQL.

BEFORE LOAD DO
--------------

You can run SQL queries against the database before loading the data from
the `CSV` file. Most common SQL queries are `CREATE TABLE IF NOT EXISTS` so
that the data can be loaded.

Each command must be *dollar-quoted*: it must begin and end with a double
dollar sign, `$$`. Dollar-quoted queries are then comma separated. No extra
punctuation is expected after the last SQL query.

BEFORE LOAD EXECUTE
-------------------

Same behaviour as in the *BEFORE LOAD DO* clause. Allows you to read the SQL
queries from a SQL file. Implements support for PostgreSQL dollar-quoting
and the `\i` and `\ir` include facilities as in `psql` batch mode (where
they are the same thing).

AFTER LOAD DO
-------------

Same format as *BEFORE LOAD DO*, the dollar-quoted queries found in that
section are executed once the load is done. That's the right time to create
indexes and constraints, or re-enable triggers.

AFTER LOAD EXECUTE
------------------

Same behaviour as in the *AFTER LOAD DO* clause. Allows you to read the SQL
queries from a SQL file. Implements support for PostgreSQL dollar-quoting
and the `\i` and `\ir` include facilities as in `psql` batch mode (where
they are the same thing).

AFTER CREATE SCHEMA DO
----------------------

Same format as *BEFORE LOAD DO*, the dollar-quoted queries found in that
section are executed once the schema has been created by pgloader, and
before the data is loaded. It's the right time to ALTER TABLE or do some
custom implementation on-top of what pgloader does, like maybe partitioning.

AFTER CREATE SCHEMA EXECUTE
---------------------------

Same behaviour as in the *AFTER CREATE SCHEMA DO* clause. Allows you to read
the SQL queries from a SQL file. Implements support for PostgreSQL
dollar-quoting and the `\i` and `\ir` include facilities as in `psql` batch
mode (where they are the same thing).

Connection String
-----------------

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
-------------------

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
--------

Any command may contain comments, following those input rules:

  - the `--` delimiter begins a comment that ends with the end of the
    current line,

  - the delimiters `/*` and `*/` respectively start and end a comment, which
    can be found in the middle of a command or span several lines.

Any place where you could enter a *whitespace* will accept a comment too.

Batch behaviour options
-----------------------

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
