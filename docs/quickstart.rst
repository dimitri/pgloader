Pgloader Quick Start
====================

In simple cases, pgloader is very easy to use.

CSV
---

Load data from a CSV file into a pre-existing table in your database::

    pgloader --type csv                                   \
             --field id --field field                     \
             --with truncate                              \
             --with "fields terminated by ','"            \
             ./test/data/matching-1.csv                   \
             postgres:///pgloader?tablename=matching

In that example the whole loading is driven from the command line, bypassing
the need for writing a command in the pgloader command syntax entirely. As
there's no command though, the extra information needed must be provided on
the command line using the `--type` and `--field` and `--with` switches.

For documentation about the available syntaxes for the `--field` and
`--with` switches, please refer to the CSV section later in the man page.

Note also that the PostgreSQL URI includes the target *tablename*.

Reading from STDIN
------------------

File based pgloader sources can be loaded from the standard input, as in the
following example::

    pgloader --type csv                                         \
             --field "usps,geoid,aland,awater,aland_sqmi,awater_sqmi,intptlat,intptlong" \
             --with "skip header = 1"                          \
             --with "fields terminated by '\t'"                \
             -                                                 \
             postgresql:///pgloader?districts_longlat          \
             < test/data/2013_Gaz_113CDs_national.txt

The dash (`-`) character as a source is used to mean *standard input*, as
usual in Unix command lines. It's possible to stream compressed content to
pgloader with this technique, using the Unix pipe::

    gunzip -c source.gz | pgloader --type csv ... - pgsql:///target?foo

Loading from CSV available through HTTP
---------------------------------------

The same command as just above can also be run if the CSV file happens to be
found on a remote HTTP location::

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
yourself::

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
the same data.

Streaming CSV data from an HTTP compressed file
-----------------------------------------------

Finally, it's important to note that pgloader first fetches the content from
the HTTP URL it to a local file, then expand the archive when it's
recognized to be one, and only then processes the locally expanded file.

In some cases, either because pgloader has no direct support for your
archive format or maybe because expanding the archive is not feasible in
your environment, you might want to *stream* the content straight from its
remote location into PostgreSQL. Here's how to do that, using the old battle
tested Unix Pipes trick::

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

Migrating from SQLite
---------------------

The following command will open the SQLite database, discover its tables
definitions including indexes and foreign keys, migrate those definitions
while *casting* the data type specifications to their PostgreSQL equivalent
and then migrate the data over::

    createdb newdb
    pgloader ./test/sqlite/sqlite.db postgresql:///newdb

Migrating from MySQL
--------------------

Just create a database where to host the MySQL data and definitions and have
pgloader do the migration for you in a single command line::

    createdb pagila
    pgloader mysql://user@localhost/sakila postgresql:///pagila

Fetching an archived DBF file from a HTTP remote location
---------------------------------------------------------

It's possible for pgloader to download a file from HTTP, unarchive it, and
only then open it to discover the schema then load the data::

    createdb foo
    pgloader --type dbf http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement/2013/dbf/historiq2013.zip postgresql:///foo

Here it's not possible for pgloader to guess the kind of data source it's
being given, so it's necessary to use the `--type` command line switch.
