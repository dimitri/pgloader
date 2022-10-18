CSV
===

This command instructs pgloader to load data from a `CSV` file. Because of
the complexity of guessing the parameters of a CSV file, it's simpler to
instruct pgloader with how to parse the data in there, using the full
pgloader command syntax and CSV specifications as in the following example.

Using advanced options and a load command file
----------------------------------------------

The command then would be:

::

   $ pgloader csv.load

And the contents of the ``csv.load`` file could be inspired from the following:

::

    LOAD CSV
       FROM 'GeoLiteCity-Blocks.csv' WITH ENCODING iso-646-us
            HAVING FIELDS
            (
               startIpNum, endIpNum, locId
            )
       INTO postgresql://user@localhost:54393/dbname
            TARGET TABLE geolite.blocks
            TARGET COLUMNS
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

Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

CSV Source Specification: FROM
------------------------------

Filename where to load the data from. Accepts an *ENCODING* option. Use the
`--list-encodings` option to know which encoding names are supported.

The filename may be enclosed by single quotes, and could be one of the
following special values:

  - *inline*

    The data is found after the end of the parsed commands. Any number
    of empty lines between the end of the commands and the beginning of
    the data is accepted.

  - *stdin*

    Reads the data from the standard input stream.

  - *FILENAME MATCHING*

    The whole *matching* clause must follow the following rule::

            [ ALL FILENAMES | [ FIRST ] FILENAME ]
            MATCHING regexp
            [ IN DIRECTORY '...' ]

    The *matching* clause applies given *regular expression* (see above
    for exact syntax, several options can be used here) to filenames.
    It's then possible to load data from only the first match of all of
    them.

    The optional *IN DIRECTORY* clause allows specifying which directory
    to walk for finding the data files, and can be either relative to
    where the command file is read from, or absolute. The given
    directory must exists.

Fields Specifications
---------------------
    
The *FROM* option also supports an optional comma separated list of *field*
names describing what is expected in the `CSV` data file, optionally
introduced by the clause `HAVING FIELDS`.

Each field name can be either only one name or a name following with
specific reader options for that field, enclosed in square brackets and
comma-separated. Supported per-field reader options are:

  - *terminated by*

     See the description of *field terminated by* below.

     The processing of this option is not currently implemented.

  - *date format*

    When the field is expected of the date type, then this option allows
    to specify the date format used in the file.

    Date format string are template strings modeled against the
    PostgreSQL `to_char` template strings support, limited to the
    following patterns:

      - YYYY, YYY, YY for the year part
      - MM for the numeric month part
      - DD for the numeric day part
      - HH, HH12, HH24 for the hour part
      - am, AM, a.m., A.M.
      - pm, PM, p.m., P.M.
      - MI for the minutes part
      - SS for the seconds part
      - MS for the milliseconds part (4 digits)
      - US for the microseconds part (6 digits)
      - unparsed punctuation signs: - . * # @ T / \ and space

    Here's an example of a *date format* specification::

        column-name [date format 'YYYY-MM-DD HH24-MI-SS.US']

  - *null if*

     This option takes an argument which is either the keyword *blanks*
     or a double-quoted string.

     When *blanks* is used and the field value that is read contains
     only space characters, then it's automatically converted to an SQL
     `NULL` value.

     When a double-quoted string is used and that string is read as the
     field value, then the field value is automatically converted to an
     SQL `NULL` value.

  - *trim both whitespace*, *trim left whitespace*, *trim right whitespace*

    This option allows to trim whitespaces in the read data, either from
    both sides of the data, or only the whitespace characters found on
    the left of the streaing, or only those on the right of the string.

CSV Loading Options: WITH
-------------------------

When loading from a `CSV` file, the following options are supported:

  - *truncate*

     When this option is listed, pgloader issues a `TRUNCATE` command
     against the PostgreSQL target table before reading the data file.

  - *drop indexes*

    When this option is listed, pgloader issues `DROP INDEX` commands
    against all the indexes defined on the target table before copying
    the data, then `CREATE INDEX` commands once the `COPY` is done.

    In order to get the best performance possible, all the indexes are
    created in parallel and when done the primary keys are built again
    from the unique indexes just created. This two step process allows
    creating the primary key index in parallel with the other indexes,
    as only the `ALTER TABLE` command needs an *access exclusive lock*
    on the target table.

  - *disable triggers*

    When this option is listed, pgloader issues an `ALTER TABLE ...
    DISABLE TRIGGER ALL` command against the PostgreSQL target table
    before copying the data, then the command `ALTER TABLE ... ENABLE
    TRIGGER ALL` once the `COPY` is done.

    This option allows loading data into a pre-existing table ignoring
    the *foreign key constraints* and user defined triggers and may
    result in invalid *foreign key constraints* once the data is loaded.
    Use with care.

  - *skip header*

    Takes a numeric value as argument. Instruct pgloader to skip that
    many lines at the beginning of the input file.

  - *csv header*

    Use the first line read after *skip header* as the list of csv field
    names to be found in the CSV file, using the same CSV parameters as
    for the CSV data.

  - *trim unquoted blanks*

    When reading unquoted values in the `CSV` file, remove the blanks
    found in between the separator and the value. That behaviour is the
    default.

  - *keep unquoted blanks*

    When reading unquoted values in the `CSV` file, keep blanks found in
    between the separator and the value.

  - *fields optionally enclosed by*

    Takes a single character as argument, which must be found inside single
    quotes, and might be given as the printable character itself, the
    special value \t to denote a tabulation character, the special value \'
    to denote a single-quote, or `0x` then an hexadecimal value read as the
    ASCII code for the character.

    The following options specify the same enclosing character, a single quote::

      fields optionally enclosed by '\''
      fields optionally enclosed by '0x27'

    This character is used as the quoting character in the `CSV` file,
    and defaults to double-quote.

  - *fields not enclosed*

    By default, pgloader will use the double-quote character as the
    enclosing character. If you have a CSV file where fields are not
    enclosed and are using double-quote as an expected ordinary
    character, then use the option *fields not enclosed* for the CSV
    parser to accept those values.

  - *fields escaped by*

    Takes either the special value *backslash-quote* or *double-quote*,
    or any value supported by the *fields terminated by* option (see
    below). This value is used to recognize escaped field separators
    when they are to be found within the data fields themselves.
    Defaults to *double-quote*.

  - *csv escape mode*

    Takes either the special value *quote* (the default) or *following*
    and allows the CSV parser to parse either only escaped field
    separator or any character (including CSV data) when using the
    *following* value.

  - *fields terminated by*

    Takes a single character as argument, which must be found inside
    single quotes, and might be given as the printable character itself,
    the special value \t to denote a tabulation character, or `0x` then
    an hexadecimal value read as the ASCII code for the character.

    This character is used as the *field separator* when reading the
    `CSV` data.

  - *lines terminated by*

    Takes a single character as argument, which must be found inside
    single quotes, and might be given as the printable character itself,
    the special value \t to denote a tabulation character, or `0x` then
    an hexadecimal value read as the ASCII code for the character.

    This character is used to recognize *end-of-line* condition when
    reading the `CSV` data.

