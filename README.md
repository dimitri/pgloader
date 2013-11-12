# PGLoader

pgloader is a data loading tool for PostgreSQL, using the `COPY` command.

Its main avantage over just using `COPY` or `\copy` and over using a
*Foreign Data Wrapper* is the transaction behaviour, where *pgloader* will
keep a separate file of rejected data and continue trying to `copy` good
data in your database.

The default PostgreSQL behaviour is transactional, which means that any
erroneous line in the input data (file or remote database) will stop the
bulk load for the whole table.

pgloader also implements data reformating, the main example of that being a
transformation from MySQL dates `0000-00-00` and `0000-00-00 00:00:00` to
PostgreSQL `NULL` value (because our calendar never had a *year zero*).

## Versioning

The pgloader version 1.x from a long time ago had been developped in `TCL`.
When faced with maintaining that code, the new emerging development team
(hi!) picked `python` instead because that made sense at the time. So
pgloader version 2.x were in python.

The current version of pgloader is the 3.x series, which is written in
[Common Lisp](http://cliki.net/) for better development flexibility, run
time performances, real threading.

The versioning is now following the Emacs model, where any X.0 release
number means you're using a development version (alpha, beta, or release
candidate). The next stable versions are going to be `3.1` then `3.2` etc.

## INSTALL

pgloader is now a Common Lisp program, tested using the
[SBCL](http://sbcl.org/) implementation with
[Quicklisp](http://www.quicklisp.org/beta/).

    $ apt-get install sbcl
    $ apt-get install libmysqlclient-dev libsqlite3-dev
	$ make pgloader
	$ ./build/pgloader.exe --help

### Patches

Several dependencies needed some patching for pgloader to be running fine,
the given `Makefile` will handle that for you. The goal is for those patches
to get included in the mainline version of the dependencies so that this
whole section and assorted `Makefile` business disappear for being
irrelevant.

#### Postmodern

The current version of the code depends on a recent version of
[Postmodern](http://marijnhaverbeke.nl/postmodern/postmodern.html) not found
in Quicklisp yet at the time of this writing. Currently the pgloader source
tree contains a patch to apply against postmodern sources, and the
`Makefile` will do the following for you:

Read https://github.com/marijnh/Postmodern/issues/39 for details.

#### cl-csv

The handling of `NULL` values in `CSV` files requires pgloader to have more
smarts than the default `cl-csv` code, so the `Makefile` will fetch my
branch including a fix for that.

Read https://github.com/AccelerationNet/cl-csv/pull/12 for details.

## The pgloader.lisp script

Now you can use the `#!` script or build a self-contained binary executable
file, as shown below.

    ./pgloader.lisp --help

Each time you run the `pgloader` command line, it will check that all its
dependencies are installed and compiled and if that's not the case fetch
them from the internet and prepare them (thanks to *Quicklisp*). So please
be patient while that happens and make sure we can actually connect and
download the dependencies.

## Build Self-Contained binary file

The `Makefile` target `pgloader` knows how to produce a Self Contained
Binary file for pgloader, named `pgloader.exe`:

    $ make pgloader

Note that the `Makefile` uses the `--compress-core` option, that should be
enabled in your local copy of `SBCL`. If that's not the case, it's probably
because you did compile and install `SBCL` yourself, so that you have a
decently recent version to use. Then you need to compile it with the
`--with-sb-core-compression` option.

You can also remove the `--compress-core` option by editing the `Makefile`
and removing the line where it appears.

The `make pgloader` command when successful outputs a `./build/pgloader.exe`
file for you to use.

## Usage

Give as many command files that you need to pgloader:

    $ ./build/pgloader.exe --help
    $ ./build/pgloader.exe <file.load>
	
See the documentation file `pgloader.1.md` for details. You can compile that
file into a manual page or an HTML page thanks to the `pandoc` application:

    $ apt-get install pandoc
	$ pandoc pgloader.1.md -o pgloader.1
	$ pandoc pgloader.1.md -o pgloader.html

## TODO

Some notes about what I intend to be working on next.

### tests

  - add needed pre-requisites in bootstrap.sh to run the MySQL and SQLite
    tests from the `make test` target without errors

### binary distribution

  - prepare an all-included binary for several platforms
  
### docs

  - host a proper website for the tool, with use cases and a tutorial

### error management

  - error management with a local buffer (done)
  - error reporting (done)
  - add input line number to log file?

### transformation and casts

  - add typemod expression to cast rules in the command language
  - add per-column support for cast rules in the system

### data output

  - PostgreSQL COPY Text format output for any supported input

#### convenience

  - automatic creation of schema even when loading from text files
  - pre-fetch some rows to guesstimate data types?

#### performances

  - some more parallelizing options
  - support for partitionning in pgloader itself

#### reformating

Data reformating is now going to have to happen in Common Lisp mostly, maybe
offer some other languages (cl-awk etc).

  - raw reformating, before rows are split

Have a try at something approaching:

    WITH data AS (
		COPY FROM ...
		RETURNING x, y
	)
	SELECT foo(x), bar(y)
	  FROM data
	 WHERE ...

A part of that needs to happen client-side, another part server-side, and
the grammar has to make it clear what happens where. Maybe add a WHERE
clause to the `COPY` or `LOAD` grammar for the client.

#### filtering

Add commands to pick different target tables depending on the data found
when reading from the source.

#### UI

  - add a web controler with pretty monitoring
  - launch new jobs from the web controler

#### crazy ideas

  - MySQL replication, reading from the binlog directly
  - plproxy (re-)sharding support
  - partitioning support
  - remote archiving support (with (delete returning *) insert into)
  
