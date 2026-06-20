Batch Processing
================

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

  - at least one write task feeds from the queue, formats the data into the
    PostgreSQL COPY format in batches (so that it's possible to then retry a
    failed batch without reading the data from source again), and then sends
    the data to PostgreSQL using the COPY protocol.

The parameter *workers* controls how many tables are copied simultaneously.
The defaults are `workers = 4` when loading from a database source and
`workers = 8` when loading from a file source. Those defaults are arbitrary
and waiting for feedback from users, so please consider providing feedback if
you play with the settings.

The parameter *concurrency* controls intra-table parallelism: how many
independent reader threads are started per table when *multiple readers per
thread* is active. It requires *concurrency* to be greater than 1 and the
source must support range partitioning (MySQL via primary-key ranges, or
PostgreSQL 14+ via ``ctid`` block ranges). The default is `concurrency = 1`
(a single reader per table).

Each parallel reader is paired with its own dedicated writer, so with
`concurrency = 4` pgloader runs 4 independent reader→writer pipelines for
that table simultaneously. Each pipeline reads a disjoint range of the
source table determined by the *chunk size* option (default 50 MB, converted
to rows or blocks depending on the source).

As the `CREATE INDEX` threads started by pgloader are only waiting until
PostgreSQL is done with the real work, those threads are *NOT* counted into
the worker or concurrency levels as detailed here.

By default, as many `CREATE INDEX` threads as the maximum number of indexes
per table are found in your source schema. It is possible to set the `max
parallel create index` *WITH* option to another number in case there are
just too many of them to create.

