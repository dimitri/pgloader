Transformation Functions
========================

Some data types are implemented in a different enough way that a
transformation function is necessary. This function must be written in
`Common lisp` and is searched in the `pgloader.transforms` package.

Some default transformation function are provided with pgloader, and you can
use the `--load` command line option to load and compile your own lisp file
into pgloader at runtime. For your functions to be found, remember to begin
your lisp file with the following form::

    (in-package #:pgloader.transforms)

The provided transformation functions are:

  - *zero-dates-to-null*

    When the input date is all zeroes, return `nil`, which gets loaded as a
    PostgreSQL `NULL` value.

  - *date-with-no-separator*

    Applies *zero-dates-to-null* then transform the given date into a format
    that PostgreSQL will actually process::

            In:  "20041002152952"
            Out: "2004-10-02 15:29:52"

  - *time-with-no-separator*

    Transform the given time into a format that PostgreSQL will actually
    process::

            In:  "08231560"
            Out: "08:23:15.60"

  - *tinyint-to-boolean*

    As MySQL lacks a proper boolean type, *tinyint* is often used to
    implement that. This function transforms `0` to `'false'` and anything
    else to `'true`'.

  - *bits-to-boolean*

    As MySQL lacks a proper boolean type, *BIT* is often used to implement
    that. This function transforms 1-bit bit vectors from `0` to `f` and any
    other value to `t`..

  - *int-to-ip*

    Convert an integer into a dotted representation of an ip4. ::

        In:  18435761
        Out: "1.25.78.177"

  - *ip-range*

    Converts a couple of integers given as strings into a range of ip4. ::

            In:  "16825344" "16825599"
            Out: "1.0.188.0-1.0.188.255"

  - *convert-mysql-point*

    Converts from the `astext` representation of points in MySQL to the
    PostgreSQL representation. ::

        In:  "POINT(48.5513589 7.6926827)"
        Out: "(48.5513589,7.6926827)"

  - *integer-to-string*

    Converts a integer string or a Common Lisp integer into a string
    suitable for a PostgreSQL integer. Takes care of quoted integers. ::

            In:  "\"0\""
            Out: "0"

  - *float-to-string*

    Converts a Common Lisp float into a string suitable for a PostgreSQL float::

            In:  100.0d0
            Out: "100.0"

  - *hex-to-dec*

    Converts a string containing an hexadecimal representation of a number
    into its decimal representation::

            In:  "deadbeef"
            Out: "3735928559"
            
  - *set-to-enum-array*

    Converts a string representing a MySQL SET into a PostgreSQL Array of
    Enum values from the set. ::

            In: "foo,bar"
            Out: "{foo,bar}"

  - *empty-string-to-null*

    Convert an empty string to a null.

  - *right-trim*

    Remove whitespace at end of string.

  - *remove-null-characters*

    Remove `NUL` characters (`0x0`) from given strings.

  - *byte-vector-to-bytea*

    Transform a simple array of unsigned bytes to the PostgreSQL bytea Hex
    Format representation as documented at
    http://www.postgresql.org/docs/9.3/interactive/datatype-binary.html

  - *sqlite-timestamp-to-timestamp*

    SQLite type system is quite interesting, so cope with it here to produce
    timestamp literals as expected by PostgreSQL. That covers year only on 4
    digits, 0 dates to null, and proper date strings.

  - *sql-server-uniqueidentifier-to-uuid*

    The SQL Server driver receives data fo type uniqueidentifier as byte
    vector that we then need to convert to an UUID string for PostgreSQL
    COPY input format to process.

  - *unix-timestamp-to-timestamptz*

    Converts a unix timestamp (number of seconds elapsed since beginning of
    1970) into a proper PostgreSQL timestamp format.

  - *varbinary-to-string*

    Converts binary encoded string (such as a MySQL `varbinary` entry) to a
    decoded text, using the table's encoding that may be overloaded with the
    *DECODING TABLE NAMES MATCHING* clause.
