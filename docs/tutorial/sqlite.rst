Loading SQLite files with pgloader
----------------------------------

The SQLite database is a respected solution to manage your data with. Its
embeded nature makes it a source of migrations when a projects now needs to
handle more concurrency, which `PostgreSQL`__ is very good at. pgloader can help
you there.

__ http://www.postgresql.org/

In a Single Command Line
^^^^^^^^^^^^^^^^^^^^^^^^

You can ::

    $ createdb chinook
    $ pgloader https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite_AutoIncrementPKs.sqlite pgsql:///chinook

Done! All with the schema, data, constraints, primary keys and foreign keys,
etc. We also see an error with the Chinook schema that contains several
primary key definitions against the same table, which is not accepted by
PostgreSQL::

    2017-06-20T16:18:59.019000+02:00 LOG Data errors in '/private/tmp/pgloader/'
    2017-06-20T16:18:59.236000+02:00 LOG Fetching 'https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite_AutoIncrementPKs.sqlite'
    2017-06-20T16:19:00.664000+02:00 ERROR Database error 42P16: multiple primary keys for table "playlisttrack" are not allowed
    QUERY: ALTER TABLE playlisttrack ADD PRIMARY KEY USING INDEX idx_66873_sqlite_autoindex_playlisttrack_1;
    2017-06-20T16:19:00.665000+02:00 LOG report summary reset
                 table name       read   imported     errors      total time
    -----------------------  ---------  ---------  ---------  --------------
                      fetch          0          0          0          0.877s 
            fetch meta data         33         33          0          0.033s 
             Create Schemas          0          0          0          0.003s 
           Create SQL Types          0          0          0          0.006s 
              Create tables         22         22          0          0.043s 
             Set Table OIDs         11         11          0          0.012s 
    -----------------------  ---------  ---------  ---------  --------------
                      album        347        347          0          0.023s 
                     artist        275        275          0          0.023s 
                   customer         59         59          0          0.021s 
                   employee          8          8          0          0.018s 
                    invoice        412        412          0          0.031s 
                      genre         25         25          0          0.021s 
                invoiceline       2240       2240          0          0.034s 
                  mediatype          5          5          0          0.025s 
              playlisttrack       8715       8715          0          0.040s 
                   playlist         18         18          0          0.016s 
                      track       3503       3503          0          0.111s 
    -----------------------  ---------  ---------  ---------  --------------
    COPY Threads Completion         33         33          0          0.313s 
             Create Indexes         22         22          0          0.160s 
     Index Build Completion         22         22          0          0.027s 
            Reset Sequences          0          0          0          0.017s 
               Primary Keys         12          0          1          0.013s 
        Create Foreign Keys         11         11          0          0.040s 
            Create Triggers          0          0          0          0.000s 
           Install Comments          0          0          0          0.000s 
    -----------------------  ---------  ---------  ---------  --------------
          Total import time      15607      15607          0          1.669s 

You may need to have special cases to take care of tho. In advanced case you
can use the pgloader command.

The Command
^^^^^^^^^^^

To load data with pgloader you need to define in a *command* the operations in
some details. Here's our command::

    load database
         from 'sqlite/Chinook_Sqlite_AutoIncrementPKs.sqlite'
         into postgresql:///pgloader
    
     with include drop, create tables, create indexes, reset sequences
    
      set work_mem to '16MB', maintenance_work_mem to '512 MB';

Note that here pgloader will benefit from the meta-data information found in
the SQLite file to create a PostgreSQL database capable of hosting the data
as described, then load the data.

Loading the data
^^^^^^^^^^^^^^^^

Let's start the `pgloader` command with our `sqlite.load` command file::

    $ pgloader sqlite.load
    ... LOG Starting pgloader, log system is ready.
    ... LOG Parsing commands from file "/Users/dim/dev/pgloader/test/sqlite.load"
    ... WARNING Postgres warning: table "album" does not exist, skipping
    ... WARNING Postgres warning: table "artist" does not exist, skipping
    ... WARNING Postgres warning: table "customer" does not exist, skipping
    ... WARNING Postgres warning: table "employee" does not exist, skipping
    ... WARNING Postgres warning: table "genre" does not exist, skipping
    ... WARNING Postgres warning: table "invoice" does not exist, skipping
    ... WARNING Postgres warning: table "invoiceline" does not exist, skipping
    ... WARNING Postgres warning: table "mediatype" does not exist, skipping
    ... WARNING Postgres warning: table "playlist" does not exist, skipping
    ... WARNING Postgres warning: table "playlisttrack" does not exist, skipping
    ... WARNING Postgres warning: table "track" does not exist, skipping
                table name       read   imported     errors            time
    ----------------------  ---------  ---------  ---------  --------------
          create, truncate          0          0          0          0.052s
                     Album        347        347          0          0.070s
                    Artist        275        275          0          0.014s
                  Customer         59         59          0          0.014s
                  Employee          8          8          0          0.012s
                     Genre         25         25          0          0.018s
                   Invoice        412        412          0          0.032s
               InvoiceLine       2240       2240          0          0.077s
                 MediaType          5          5          0          0.012s
                  Playlist         18         18          0          0.008s
             PlaylistTrack       8715       8715          0          0.071s
                     Track       3503       3503          0          0.105s
    index build completion          0          0          0          0.000s
    ----------------------  ---------  ---------  ---------  --------------
            Create Indexes         20         20          0          0.279s
           reset sequences          0          0          0          0.043s
    ----------------------  ---------  ---------  ---------  --------------
      Total streaming time      15607      15607          0          0.476s

We can see that `pgloader <http://pgloader.io>`_ did download the file from
its HTTP URL location then *unziped* it before loading it.

Also, the *WARNING* messages we see here are expected as the PostgreSQL
database is empty when running the command, and pgloader is using the SQL
commands `DROP TABLE IF EXISTS` when the given command uses the `include
drop` option.

Note that the output of the command has been edited to facilitate its
browsing online.
