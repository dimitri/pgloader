Fixed Columns
=============

This command instructs pgloader to load data from a text file containing
columns arranged in a *fixed size* manner.

Using advanced options and a load command file
----------------------------------------------

The command then would be:

::

   $ pgloader fixed.load

And the contents of the ``fixed.load`` file could be inspired from the following:

::

    LOAD FIXED
         FROM inline
              (
               a from  0 for 10,
               b from 10 for  8,
               c from 18 for  8,
               d from 26 for 17 [null if blanks, trim right whitespace]
              )
         INTO postgresql:///pgloader
       TARGET TABLE fixed
              (
                 a, b,
                 c time using (time-with-no-separator c),
                 d
              )

         WITH truncate

          SET work_mem to '14MB',
              standard_conforming_strings to 'on'

    BEFORE LOAD DO
         $$ drop table if exists fixed; $$,
         $$ create table fixed (
             a integer,
             b date,
             c time,
             d text
            );
         $$;

     01234567892008052011431250firstline
        01234562008052115182300left blank-padded
     12345678902008052208231560another line
      2345609872014092914371500                 
      2345678902014092914371520

Note that the example comes from the test suite of pgloader, where we use
the advanced feature ``FROM inline`` that allows embedding the source data
within the command file. In most cases a more classic FROM clause loading
the data from a separate file would be used.

Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

Fixed File Format Source Specification: FROM
--------------------------------------------

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

  - *FILENAMES MATCHING*

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
names describing what is expected in the `FIXED` data file.

Each field name is composed of the field name followed with specific reader
options for that field. Supported per-field reader options are the
following, where only *start* and *length* are required.

  - *start*

    Position in the line where to start reading that field's value. Can
    be entered with decimal digits or `0x` then hexadecimal digits.

  - *length*

    How many bytes to read from the *start* position to read that
    field's value. Same format as *start*.

Those optional parameters must be enclosed in square brackets and
comma-separated:

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

    When *blanks* is used and the field value that is read contains only
    space characters, then it's automatically converted to an SQL `NULL`
    value.

    When a double-quoted string is used and that string is read as the
    field value, then the field value is automatically converted to an
    SQL `NULL` value.

  - *trim both whitespace*, *trim left whitespace*, *trim right whitespace*

    This option allows to trim whitespaces in the read data, either from
    both sides of the data, or only the whitespace characters found on
    the left of the streaing, or only those on the right of the string.

Fixed File Format Loading Options: WITH
---------------------------------------

When loading from a `FIXED` file, the following options are supported:

  - *truncate*

    When this option is listed, pgloader issues a `TRUNCATE` command
    against the PostgreSQL target table before reading the data file.

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

