Loading MaxMind Geolite Data with pgloader
------------------------------------------

`MaxMind <http://www.maxmind.com/>`_ provides a free dataset for
geolocation, which is quite popular. Using pgloader you can download the
lastest version of it, extract the CSV files from the archive and load their
content into your database directly.

The Command
^^^^^^^^^^^

To load data with pgloader you need to define in a *command* the operations
in some details. Here's our example for loading the Geolite data::

    /*
     * Loading from a ZIP archive containing CSV files. The full test can be
     * done with using the archive found at
     * http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip
     *
     * And a very light version of this data set is found at
     * http://pgsql.tapoueh.org/temp/foo.zip for quick testing.
     */
    
    LOAD ARCHIVE
       FROM http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip
       INTO postgresql:///ip4r
    
       BEFORE LOAD DO
         $$ create extension if not exists ip4r; $$,
         $$ create schema if not exists geolite; $$,
         $$ create table if not exists geolite.location
           (
              locid      integer primary key,
              country    text,
              region     text,
              city       text,
              postalcode text,
              location   point,
              metrocode  text,
              areacode   text
           );
         $$,
         $$ create table if not exists geolite.blocks
           (
              iprange    ip4r,
              locid      integer
           );
         $$,
         $$ drop index if exists geolite.blocks_ip4r_idx; $$,
         $$ truncate table geolite.blocks, geolite.location cascade; $$
    
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

Note that while the *Geolite* data is using a pair of integers (*start*,
*end*) to represent *ipv4* data, we use the very poweful `ip4r
<https://github.com/RhodiumToad/ip4r>`_ PostgreSQL Extension instead.

The transformation from a pair of integers into an IP is done dynamically by
the pgloader process.

Also, the location is given as a pair of *float* columns for the *longitude*
and the *latitude* where PostgreSQL offers the
`point <http://www.postgresql.org/docs/9.3/interactive/functions-geometry.html>`_
datatype, so the pgloader command here will actually transform the data on
the fly to use the appropriate data type and its input representation.

Loading the data
^^^^^^^^^^^^^^^^

Here's how to start loading the data. Note that the ouput here has been
edited so as to facilitate its browsing online::

    $ pgloader archive.load
    ... LOG Starting pgloader, log system is ready.
    ... LOG Parsing commands from file "/Users/dim/dev/pgloader/test/archive.load"
    ... LOG Fetching 'http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip'
    ... LOG Extracting files from archive '//private/var/folders/w7/9n8v8pw54t1gngfff0lj16040000gn/T/pgloader//GeoLiteCity-latest.zip'
    
           table name       read   imported     errors            time
    -----------------  ---------  ---------  ---------  --------------
             download          0          0          0         11.592s
              extract          0          0          0          1.012s
          before load          6          6          0          0.019s
    -----------------  ---------  ---------  ---------  --------------
     geolite.location     470387     470387          0          7.743s
       geolite.blocks    1903155    1903155          0         16.332s
    -----------------  ---------  ---------  ---------  --------------
              finally          1          1          0         31.692s
    -----------------  ---------  ---------  ---------  --------------
    Total import time    2373542    2373542          0        1m8.390s

The timing of course includes the transformation of the *1.9 million* pairs
of integer into a single *ipv4 range* each. The *finally* step consists of
creating the *GiST* specialized index as given in the main command::

    CREATE INDEX blocks_ip4r_idx ON geolite.blocks USING gist(iprange);

That index will then be used to speed up queries wanting to find which
recorded geolocation contains a specific IP address::

    ip4r> select *
            from      geolite.location l
                 join geolite.blocks b using(locid)
           where iprange >>= '8.8.8.8';
           
    -[ RECORD 1 ]------------------
    locid      | 223
    country    | US
    region     | 
    city       | 
    postalcode | 
    location   | (-97,38)
    metrocode  | 
    areacode   | 
    iprange    | 8.8.8.8-8.8.37.255
    
    Time: 0.747 ms
