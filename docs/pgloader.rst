Command Line
============

pgloader loads data from various sources into PostgreSQL. It can
transform the data it reads on the fly and submit raw SQL before and
after the loading.  It uses the `COPY` PostgreSQL protocol to stream
the data into the server, and manages errors by filling a pair of
*reject.dat* and *reject.log* files.

pgloader operates either using commands which are read from files::

    pgloader commands.load

or by using arguments and options all provided on the command line::

    pgloader SOURCE TARGET

Arguments
---------

The pgloader arguments can be as many load files as needed, or a couple of
connection strings to a specific input file.

Source Connection String
^^^^^^^^^^^^^^^^^^^^^^^^

The source connection string format is as follows::

    format:///absolute/path/to/file.ext
    format://./relative/path/to/file.ext

Where format might be one of `csv`, `fixed`, `copy`, `dbf`, `db3` or `ixf`.::

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

.. note:: **pgloader v4 (Clojure/JVM)** also accepts standard JDBC URLs
   wherever a source connection string is expected::

       jdbc:mysql://host:port/db?useSSL=false&allowPublicKeyRetrieval=true
       jdbc:postgresql://host:port/db?sslmode=require
       jdbc:sqlserver://host:port;databaseName=db;encrypt=true;trustServerCertificate=false

   JDBC URLs are passed directly to the JDBC driver so all
   driver-specific connection properties are honoured without any
   pgloader-side interpretation. See :ref:`connection_string` for
   details.

Target Connection String
^^^^^^^^^^^^^^^^^^^^^^^^

The target connection string format is described in details later in this
document, see Section Connection String.

Options
-------

Inquiry Options
^^^^^^^^^^^^^^^

Use these options when you want to know more about how to use pgloader, as
those options will cause pgloader not to load any data.

--help

    Show command usage summary and exit.

--version

    Show pgloader version string and exit.

--list-encodings

    List known encodings in this version of pgloader.

--upgrade-config

    Parse given files in the command line as ``pgloader.conf`` files with
    the INI syntax that was in use in pgloader versions 2.x, and output the
    new command syntax for pgloader on standard output.

    .. note:: **v4: removed.** INI configuration files are not supported in
       pgloader v4. If you have a v2-era ``pgloader.conf`` file, convert it
       manually using the v3 ``--upgrade-config`` output as a starting
       point, then adapt the resulting ``.load`` file for v4.


General Options
^^^^^^^^^^^^^^^

Those options are meant to tweak pgloader behavior when loading data.

--verbose

    Be verbose.

--quiet

    Be quiet.

--debug

    Show debug level information messages.

--root-dir

    Set the root working directory (defaults to ``/tmp/pgloader``).

--logfile

    Set the pgloader log file (defaults to ``/tmp/pgloader/pgloader.log``).

    .. note:: **v4 change:** ``--logfile`` adds a *second* log destination
       (in addition to the console) rather than replacing it. The root
       Logback appender writes to both simultaneously. If only file output is
       desired, combine with ``--client-min-messages error``.

--log-min-messages

    Minimum level of verbosity needed for log message to make it to the
    logfile. One of critical, log, error, warning, notice, info or debug.

    .. note:: **v4 change:** accepted level names are the standard Logback
       names: ``trace``, ``debug``, ``info``, ``warn``, ``error``
       (case-insensitive). The v3 names ``critical``, ``log``, ``notice``
       and ``data`` are not recognised; use ``error``, ``debug``, ``info``
       and ``trace`` respectively.

--client-min-messages

    Minimum level of verbosity needed for log message to make it to the
    console. One of critical, log, error, warning, notice, info or debug.

    .. note:: **v4 change:** same level-name mapping as ``--log-min-messages``
       above. In v4 this sets a ``ThresholdFilter`` on the console Logback
       appender, so it can be used to quiet the console independently of the
       root log level (e.g. ``--client-min-messages warn --logfile pgloader.log``
       keeps the logfile at INFO while only warnings and above reach the
       terminal).

--summary

    A filename where to copy the summary output. When relative, the filename
    is expanded into ``*root-dir*``.

    The format of the filename defaults to being *human readable*. It is

    possible to have the output in machine friendly formats such as *CSV*,
    *COPY* (PostgreSQL's own COPY format) or *JSON* by specifying a filename
    with the extension resp. ``.csv``, ``.copy`` or ``.json``.

--load-lisp-file <file>

    Specify a lisp <file> to compile and load into the pgloader image before
    reading the commands, allowing to define extra transformation function.
    Those functions should be defined in the ``pgloader.transforms``
    package. This option can appear more than once in the command line.

    .. note:: **v4: deprecated.** pgloader v4 does not load Lisp source
       files. The built-in transform functions (``zero-dates-to-null``,
       ``empty-string-to-null``, ``right-trim``, ``byte-vector-to-hex``,
       ``mysql-day-of-week``, and others) are all available without any
       extra files. If you rely on custom transforms defined in a Lisp file,
       contact the pgloader maintainers — a Clojure-based extension
       mechanism may be considered if there is demand.

--dry-run

    Allow testing a ``.load`` file without actually trying to load any data.
    It's useful to debug it until it's ok, in particular to fix connection
    strings.


--on-error-stop

    Alter pgloader behavior: rather than trying to be smart about error
    handling and continue loading good data, separating away the bad one,
    just stop as soon as PostgreSQL refuses anything sent to it. Useful to
    debug data processing, transformation function and specific type
    casting.

--self-upgrade <directory>

    Specify a <directory> where to find pgloader sources so that one of the
    very first things it does is dynamically loading-in (and compiling to
    machine code) another version of itself, usually a newer one like a very
    recent git checkout.

    .. note:: **v4: removed.** pgloader v4 is distributed as a single
       self-contained JAR. There is no Lisp runtime to reload sources into.
       To upgrade, replace the JAR file. The ``--self-upgrade`` flag exits
       with an error in v4.

--no-ssl-cert-verification

    Uses the OpenSSL option to accept a locally issued server-side
    certificate, avoiding the following error message::

      SSL verify error: 20 X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY

    The right way to fix the SSL issue is to use a trusted certificate, of
    course. Sometimes though it's useful to make progress with the pgloader
    setup while the certificate chain of trust is being fixed, maybe by
    another team. That's when this option is useful.

    .. note:: **v4: removed.** SSL behaviour is controlled entirely through
       the JDBC connection URL. Use ``?sslmode=disable`` to skip certificate
       verification, or ``?sslmode=require`` / ``?sslmode=verify-full`` for
       stricter modes. The ``--no-ssl-cert-verification`` flag exits with an
       error in v4.

Command Line Only Operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Those options are meant to be used when using pgloader from the command line
only, rather than using a command file and the rich command clauses and
parser. In simple cases, it can be much easier to use the *SOURCE* and
*TARGET* directly on the command line, then tweak the loading with those
options:

--with <option>

    Allows setting options from the command line. You can use that option as
    many times as you want. The option arguments must follow the *WITH*
    clause for the source type of the ``SOURCE`` specification, as described
    later in this document.

--set

    Allows setting PostgreSQL configuration from the command line. Note that
    the option parsing is the same as when used from the *SET* command
    clause, in particular you must enclose the guc value with single-quotes.

    Use ``--set "guc_name='value'"``.

--field

    Allows setting a source field definition. Fields are accumulated in the
    order given on the command line. It's possible to either use a
    ``--field`` option per field in the source file, or to separate field
    definitions by a comma, as you would do in the *HAVING FIELDS* clause.

--cast <rule>

    Allows setting a specific casting rule for loading the data.

--type <csv|fixed|db3|ixf|sqlite|mysql|mssql>

    Allows forcing the source type, in case when the *SOURCE* parsing isn't
    satisfying.

--encoding <encoding>

    Set the encoding of the source file to load data from.

--before <filename>

    Parse given filename for SQL queries and run them against the target
    database before loading the data from the source. The queries are parsed
    by pgloader itself: they need to be terminated by a semi-colon (;) and
    the file may include `\i` or `\ir` commands to *include* another file.

--after <filename>

    Parse given filename for SQL queries and run them against the target
    database after having loaded the data from the source. The queries are
    parsed in the same way as with the `--before` option, see above.

More Debug Information
^^^^^^^^^^^^^^^^^^^^^^

To get the maximum amount of debug information, you can use both the
`--verbose` and the `--debug` switches at the same time, which is equivalent
to saying `--client-min-messages data`. Then the log messages will show the
data being processed, in the cases where the code has explicit support for
it.
