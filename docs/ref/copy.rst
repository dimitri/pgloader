COPY
====

This commands instructs pgloader to load from a file containing COPY TEXT
data as described in the PostgreSQL documentation.

Using advanced options and a load command file
----------------------------------------------

The command then would be:

::

   $ pgloader copy.load

And the contents of the ``copy.load`` file could be inspired from the following:

::

    LOAD COPY
         FROM copy://./data/track.copy
              (
                trackid, track, album, media, genre, composer,
                milliseconds, bytes, unitprice
              )
         INTO postgresql:///pgloader
       TARGET TABLE track_full

         WITH truncate

          SET work_mem to '14MB',
              standard_conforming_strings to 'on'

    BEFORE LOAD DO
         $$ drop table if exists track_full; $$,
         $$ create table track_full (
              trackid      bigserial,
              track        text,
              album        text,
              media        text,
              genre        text,
              composer     text,
              milliseconds bigint,
              bytes        bigint,
              unitprice    numeric
            );
         $$;


Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

COPY Formatted Files Source Specification: FROM
-----------------------------------------------

Filename where to load the data from. This support local files, HTTP URLs
and zip files containing a single dbf file of the same name. Fetch such a
zip file from an HTTP address is of course supported.

  - *inline*

    The data is found after the end of the parsed commands. Any number of
    empty lines between the end of the commands and the beginning of the
    data is accepted.

  - *stdin*

    Reads the data from the standard input stream.

  - *FILENAMES MATCHING*

    The whole *matching* clause must follow the following rule::

      [ ALL FILENAMES | [ FIRST ] FILENAME ]
      MATCHING regexp
      [ IN DIRECTORY '...' ]

    The *matching* clause applies given *regular expression* (see above for
    exact syntax, several options can be used here) to filenames. It's then
    possible to load data from only the first match of all of them.

    The optional *IN DIRECTORY* clause allows specifying which directory to
    walk for finding the data files, and can be either relative to where the
    command file is read from, or absolute. The given directory must exists.

COPY Formatted File Options: WITH
---------------------------------


When loading from a `COPY` file, the following options are supported:

  - *delimiter*

    Takes a single character as argument, which must be found inside single
    quotes, and might be given as the printable character itself, the
    special value \t to denote a tabulation character, or `0x` then an
    hexadecimal value read as the ASCII code for the character.

    This character is used as the *delimiter* when reading the data, in a
    similar way to the PostgreSQL `COPY` option.

  - *null*

    Takes a quoted string as an argument (quotes can be either double quotes
    or single quotes) and uses that string as the `NULL` representation in
    the data.

    This is similar to the *null* `COPY` option in PostgreSQL.

  - *truncate*

    When this option is listed, pgloader issues a `TRUNCATE` command against
    the PostgreSQL target table before reading the data file.

  - *disable triggers*

    When this option is listed, pgloader issues an `ALTER TABLE ... DISABLE
    TRIGGER ALL` command against the PostgreSQL target table before copying
    the data, then the command `ALTER TABLE ... ENABLE TRIGGER ALL` once the
    `COPY` is done.

    This option allows loading data into a pre-existing table ignoring the
    *foreign key constraints* and user defined triggers and may result in
    invalid *foreign key constraints* once the data is loaded. Use with
    care.

  - *skip header*

    Takes a numeric value as argument. Instruct pgloader to skip that many
    lines at the beginning of the input file.
