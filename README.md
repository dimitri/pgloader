# PGLoader

[![Build Status](https://travis-ci.org/dimitri/pgloader.svg?branch=master)](https://travis-ci.org/dimitri/pgloader)
[![Join the chat at https://gitter.im/dimitri/pgloader](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/dimitri/pgloader?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Docker Build Status](https://img.shields.io/docker/cloud/build/dimitri/pgloader.svg)](https://cloud.docker.com/repository/docker/dimitri/pgloader)
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

## Versioning

pgloader version 1.x is quite old and was developed in `TCL`.
When faced with maintaining that code, the new emerging development
team (hi!) picked `python` instead because that made sense at the
time. So pgloader version 2.x was written in python.

The current version of pgloader is the 3.x series, which is written in
[Common Lisp](http://cliki.net/) for better development flexibility,
runtime performance, and support of real threading.

The versioning is now following the Emacs model, where any X.0 release
number means you're using a development version (alpha, beta, or release
candidate). The next stable versions are going to be `3.1` then `3.2` etc.

When using a development snapshot rather than a released version the version
number includes the git hash (in its abbreviated form):

  - `pgloader version "3.0.99"`
  
     Release candidate 9 for pgloader version 3.1, with a *git tag* named
     `v3.0.99` so that it's easy to checkout the same sources as the
     released code.
     
  - `pgloader version "3.0.fecae2c"`
  
     Development snapshot again *git hash* `fecae2c`. It's possible to have
     the same sources on another setup with using the git command `git
     checkout fecae2c`.

  - `pgloader version "3.1.0"`
  
     Stable release.

## LICENCE

pgloader is available under [The PostgreSQL Licence](http://www.postgresql.org/about/licence/).

## INSTALL

You can install pgloader directly from
[apt.postgresql.org](https://wiki.postgresql.org/wiki/Apt) and from official
debian repositories, see
[packages.debian.org/pgloader](https://packages.debian.org/search?keywords=pgloader).

    $ apt-get install pgloader

You can also use a **docker** image for pgloader at
<https://hub.docker.com/r/dimitri/pgloader/>:

    $ docker pull dimitri/pgloader
    $ docker run --rm --name pgloader dimitri/pgloader:latest pgloader --version
    $ docker run --rm --name pgloader dimitri/pgloader:latest pgloader --help

## Build from sources

pgloader is now a Common Lisp program, tested using the
[SBCL](http://sbcl.org/) (>= 1.2.5) and
[Clozure CL](http://ccl.clozure.com/) implementations with
[Quicklisp](http://www.quicklisp.org/beta/).

When building from sources, you should always build from the current git
`HEAD` as it's basically the only source that is managed in a way to ensure
it builds aginst current set of dependencies versions.

### Building from sources on debian

    $ apt-get install sbcl unzip libsqlite3-dev make curl gawk freetds-dev libzip-dev
    $ cd /path/to/pgloader
    $ make pgloader
    $ ./build/bin/pgloader --help

### Building from sources on RedHat/CentOS

See "Redhat / CentOS" in [INSTALL.md](INSTALL.md#redhat--centos)

### Building from sources on macOS

When using [brew](https://brew.sh), it should be a simple `brew install
--HEAD pgloader`.

When using [macports](https://www.macports.org), then we have a situation to
deal with with shared objects pgloader depends on, as reported in issue #161
at <https://github.com/dimitri/pgloader/issues/161#issuecomment-201162647>:

> I was able to get a clean build without having to disable compression after
> symlinking /usr/local/lib to /opt/local/lib. Note that I did not have
> anything installed to /usr/local/lib so I didn't lose anything here.

### Building from sources on Windows

Building pgloader on Windows is supported, thanks to Common Lisp
implementations being available on that platform, and to the Common Lisp
Standard for making it easy to write actually portable code.

It is recommended to have a look at the issues labelled with *Windows
support* if you run into trouble when building
pgloader:

<https://github.com/dimitri/pgloader/issues?utf8=✓&q=label%3A%22Windows%20support%22%20>

### Building Docker image from sources

You can build a Docker image from source using SBCL by default:

  $ docker build .

Or Clozure CL (CCL):

  $ docker build -f Dockerfile.ccl .

## More options when building from source

The `Makefile` target `pgloader` knows how to produce a Self Contained
Binary file for pgloader, found at `./build/bin/pgloader`:

    $ make pgloader

By default, the `Makefile` uses [SBCL](http://sbcl.org/) to compile your
binary image, though it's possible to build using
[CCL](http://ccl.clozure.com/).

    $ make CL=ccl pgloader

If using `SBCL` and it supports core compression, the make process will
use it to generate a smaller binary.  To force disabling core
compression, you may use:

    $ make COMPRESS_CORE=no pgloader

The `--compress-core` is unique to SBCL, so not used when `CC` is different
from the `sbcl` value.

You can also tweak the default amount of memory that the `pgloader` image
will allow itself using when running through your data (don't ask for more
than your current RAM tho):

    $ make DYNSIZE=8192 pgloader

The `make pgloader` command when successful outputs a `./build/bin/pgloader`
file for you to use.

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
