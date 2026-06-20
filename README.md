# pgloader

[![pgloader v4 CI](https://github.com/dimitri/pgloader/actions/workflows/clojure-integration-tests.yml/badge.svg?branch=v4-clojure-rewrite)](https://github.com/dimitri/pgloader/actions/workflows/clojure-integration-tests.yml)
[![Read The Docs](https://readthedocs.org/projects/pgloader/badge/?version=latest&style=plastic)](http://pgloader.readthedocs.io/en/latest/)

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

## Version 4 — Clojure/JVM rewrite

pgloader v4 is a full rewrite in Clojure, distributed as a single
self-contained JAR requiring **Java 21 or later**. It is a drop-in
replacement for v3 and accepts the same `.load` file syntax and
command-line flags.

Key improvements over v3:

- **Single JAR** — no native dependencies, no SBCL, no shared libraries
- **JDBC connection strings** — use `jdbc:mysql://`, `jdbc:postgresql://`,
  `jdbc:sqlserver://` etc. everywhere; driver-specific parameters pass
  through unchanged
- **Java heap** — no more Common Lisp heap exhaustion on large migrations;
  tune with standard `-Xmx` JVM flag
- **SSL via URI** — use `?sslmode=require` / `?sslmode=disable` in the
  connection string instead of `--no-ssl-cert-verification`

## Documentation

Full documentation is available at
[https://pgloader.readthedocs.io/](https://pgloader.readthedocs.io/en/latest/).

```
$ java -jar pgloader.jar --help
pgloader [ option ... ] SOURCE TARGET
  --help                          Show usage and exit.
  --version                       Display pgloader version and exit.
  --quiet                         Be quiet.
  --verbose                       Be verbose.
  --debug                         Display debug level information.
  --client-min-messages           Filter logs seen at the console (default: info)
  --log-min-messages              Filter logs seen in the logfile (default: info)
  --logfile                       Filename where to send the logs.
  --summary                       Filename where to copy the summary.
  --root-dir                      Output root directory (default: /tmp/pgloader/).
  --list-encodings                List pgloader known encodings and exit.
  --dry-run                       Check connections only, don't load anything.
  --on-error-stop                 Stop on first error instead of continuing.
```

## Usage

You can either give a command file to pgloader or run it all from the
command line, see the
[pgloader quick start](https://pgloader.readthedocs.io/en/latest/tutorial/tutorial.html#pgloader-quick-start)
for more details.

    $ java -jar pgloader.jar --help
    $ java -jar pgloader.jar <file.load>

For example, for a full migration from SQLite:

    $ createdb newdb
    $ java -jar pgloader.jar ./test/sqlite/sqlite.db postgresql:///newdb

Or for a full migration from MySQL, including schema definition (tables,
indexes, foreign keys, comments) and parallel loading of the corrected data:

    $ createdb pagila
    $ java -jar pgloader.jar mysql://user@localhost/sakila postgresql:///pagila

## INSTALL

### Java (v4 — recommended)

pgloader v4 requires **Java 21 or later**
([Temurin](https://adoptium.net/), OpenJDK, or any compatible distribution).

Download the latest pre-release JAR — this URL always points to the most
recent build from the `v4-clojure-rewrite` branch:

    $ curl -L -o pgloader.jar \
        https://github.com/dimitri/pgloader/releases/download/v4-dev/pgloader.jar
    $ java -jar pgloader.jar --version

To make it available system-wide:

    $ sudo install -m 755 pgloader.jar /usr/local/lib/pgloader.jar
    $ echo '#!/bin/sh\nexec java -jar /usr/local/lib/pgloader.jar "$@"' \
        | sudo tee /usr/local/bin/pgloader
    $ sudo chmod 755 /usr/local/bin/pgloader
    $ pgloader --version

### Docker

The latest image is built by CI on every push to `v4-clojure-rewrite`:

    $ docker pull ghcr.io/dimitri/pgloader:latest
    $ docker run --rm -it ghcr.io/dimitri/pgloader:latest pgloader --version

### Debian / Ubuntu (v3)

The v3 package is available in the Debian and Ubuntu repositories:

    $ apt-get install pgloader

A v4 Debian package is planned.

## LICENCE

pgloader is available under [The PostgreSQL
Licence](http://www.postgresql.org/about/licence/).
