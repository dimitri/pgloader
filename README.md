# PGLoader

[![Build Status](https://travis-ci.org/dimitri/pgloader.svg?branch=master)](https://travis-ci.org/dimitri/pgloader)
[![Join the chat at https://gitter.im/dimitri/pgloader](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/dimitri/pgloader?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Read The Docs Status](https://readthedocs.org/projects/pgloader/badge/?version=latest&style=plastic)](http://pgloader.readthedocs.io/en/latest/)

pgloader is a data loading tool for PostgreSQL, using the `COPY` command.

Its main advantage over just using `COPY` or `\copy`, and over using a
*Foreign Data Wrapper*, is its transaction behaviour, where *pgloader*
will keep a separate file of rejected data, but continue trying to
`copy` good data in your database.

The default PostgreSQL behaviour is transactional, which means that
*any* erroneous line in the input data (file or remote database) will
stop the entire bulk load for the table.

pgloader also implements data reformatting, a typical example of that
being the transformation of MySQL datestamps `0000-00-00` and
`0000-00-00 00:00:00` to PostgreSQL `NULL` value (because our calendar
never had a *year zero*).

## Documentation

Full documentation is available online, including manual pages of all the
pgloader sub-commands. Check out
[https://pgloader.readthedocs.io/](https://pgloader.readthedocs.io/en/latest/).

```
$ pgloader --help
pgloader [ option ... ] SOURCE TARGET
  --help -h                       boolean  Show usage and exit.
  --version -V                    boolean  Displays pgloader version and exit.
  --quiet -q                      boolean  Be quiet
  --verbose -v                    boolean  Be verbose
  --debug -d                      boolean  Display debug level information.
  --client-min-messages           string   Filter logs seen at the console (default: "warning")
  --log-min-messages              string   Filter logs seen in the logfile (default: "notice")
  --summary -S                    string   Filename where to copy the summary
  --root-dir -D                   string   Output root directory. (default: #P"/tmp/pgloader/")
  --upgrade-config -U             boolean  Output the command(s) corresponding to .conf file for v2.x
  --list-encodings -E             boolean  List pgloader known encodings and exit.
  --logfile -L                    string   Filename where to send the logs.
  --load-lisp-file -l             string   Read user code from files
  --dry-run                       boolean  Only check database connections, don't load anything.
  --on-error-stop                 boolean  Refrain from handling errors properly.
  --no-ssl-cert-verification      boolean  Instruct OpenSSL to bypass verifying certificates.
  --context -C                    string   Command Context Variables
  --with                          string   Load options
  --set                           string   PostgreSQL options
  --field                         string   Source file fields specification
  --cast                          string   Specific cast rules
  --type                          string   Force input source type
  --encoding                      string   Source expected encoding
  --before                        string   SQL script to run before loading the data
  --after                         string   SQL script to run after loading the data
  --self-upgrade                  string   Path to pgloader newer sources
  --regress                       boolean  Drive regression testing
```

## Usage

You can either give a command file to pgloader or run it all from the
command line, see the
[pgloader quick start](https://pgloader.readthedocs.io/en/latest/tutorial/tutorial.html#pgloader-quick-start) on
<https://pgloader.readthedocs.io> for more details.

    $ ./build/bin/pgloader --help
    $ ./build/bin/pgloader <file.load>

For example, for a full migration from SQLite:

    $ createdb newdb
    $ pgloader ./test/sqlite/sqlite.db postgresql:///newdb

Or for a full migration from MySQL, including schema definition (tables,
indexes, foreign keys, comments) and parallel loading of the corrected data:

    $ createdb pagila
    $ pgloader mysql://user@localhost/sakila postgresql:///pagila

## LICENCE

pgloader is available under [The PostgreSQL
Licence](http://www.postgresql.org/about/licence/).

## INSTALL

Please see full documentation at
[https://pgloader.readthedocs.io/](https://pgloader.readthedocs.io/en/latest/install.html).

If you're using debian, it's already available:

    $ apt-get install pgloader

If you're using docker, you can use the latest version built by the CI at
each commit to the master branch:

    $ docker pull ghcr.io/dimitri/pgloader:latest
    $ docker run --rm -it ghcr.io/dimitri/pgloader:latest pgloader --version
