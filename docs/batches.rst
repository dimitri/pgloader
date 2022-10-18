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

