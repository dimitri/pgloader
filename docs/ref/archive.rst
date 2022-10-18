Archive (http, zip)
===================

This command instructs pgloader to load data from one or more files contained
in an archive. Currently the only supported archive format is *ZIP*, and the
archive might be downloaded from an *HTTP* URL.

Using advanced options and a load command file
----------------------------------------------

The command then would be:

::

   $ pgloader archive.load

And the contents of the ``archive.load`` file could be inspired from the
following:

::

    LOAD ARCHIVE
       FROM /Users/dim/Downloads/GeoLiteCity-latest.zip
       INTO postgresql:///ip4r

       BEFORE LOAD
         DO $$ create extension if not exists ip4r; $$,
            $$ create schema if not exists geolite; $$,

         EXECUTE 'geolite.sql'

       LOAD CSV
            FROM FILENAME MATCHING ~/GeoLiteCity-Location.csv/
                 WITH ENCODING iso-8859-1
                 (
                    locId,
                    country,
                    region     null if blanks,
                    city       null if blanks,
                    postalCode null if blanks,
                    latitude,
                    longitude,
                    metroCode  null if blanks,
                    areaCode   null if blanks
                 )
            INTO postgresql:///ip4r?geolite.location
                 (
                    locid,country,region,city,postalCode,
                    location point using (format nil "(~a,~a)" longitude latitude),
                    metroCode,areaCode
                 )
            WITH skip header = 2,
                 fields optionally enclosed by '"',
                 fields escaped by double-quote,
                 fields terminated by ','

      AND LOAD CSV
            FROM FILENAME MATCHING ~/GeoLiteCity-Blocks.csv/
                 WITH ENCODING iso-8859-1
                 (
                    startIpNum, endIpNum, locId
                 )
            INTO postgresql:///ip4r?geolite.blocks
                 (
                    iprange ip4r using (ip-range startIpNum endIpNum),
                    locId
                 )
            WITH skip header = 2,
                 fields optionally enclosed by '"',
                 fields escaped by double-quote,
                 fields terminated by ','

       FINALLY DO
         $$ create index blocks_ip4r_idx on geolite.blocks using gist(iprange); $$;

Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

Archive Source Specification: FROM
----------------------------------

Filename or HTTP URI where to load the data from. When given an HTTP URL the
linked file will get downloaded locally before processing.

If the file is a `zip` file, the command line utility `unzip` is used to
expand the archive into files in `$TMPDIR`, or `/tmp` if `$TMPDIR` is unset
or set to a non-existing directory.

Then the following commands are used from the top level directory where the
archive has been expanded.

Archive Sub Commands
--------------------

  - command [ *AND* command ... ]

    A series of commands against the contents of the archive, at the moment
    only `CSV`,`'FIXED` and `DBF` commands are supported.

    Note that commands are supporting the clause *FROM FILENAME MATCHING*
    which allows the pgloader command not to depend on the exact names of
    the archive directories.

    The same clause can also be applied to several files with using the
    spelling *FROM ALL FILENAMES MATCHING* and a regular expression.

    The whole *matching* clause must follow the following rule::

      FROM [ ALL FILENAMES | [ FIRST ] FILENAME ] MATCHING

Archive Final SQL Commands
--------------------------
      
  - *FINALLY DO*

    SQL Queries to run once the data is loaded, such as `CREATE INDEX`.

