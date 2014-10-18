# Loading SQLite files with pgloader

The SQLite database is a respected solution to manage your data with. Its
embeded nature makes it a source of migrations when a projects now needs to
handle more concurrency, which [PostgreSQL](http://www.postgresql.org/) is
very good at. pgloader can help you there.

## The Command

To load data with [pgloader](http://pgloader.io/) you need to
define in a *command* the operations in some details. Here's our command:

    load database
         from 'sqlite/Chinook_Sqlite_AutoIncrementPKs.sqlite'
         into postgresql:///pgloader
    
     with include drop, create tables, create indexes, reset sequences
    
      set work_mem to '16MB', maintenance_work_mem to '512 MB';

You can see the full list of options in the
[pgloader reference manual](pgloader.1.html), with a complete description
of the options you see here.

Note that here pgloader will benefit from the meta-data information found in
the SQLite file to create a PostgreSQL database capable of hosting the data
as described, then load the data.

## Loading the data

Let's start the `pgloader` command with our `sqlite.load` command file:

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

We can see that [http://pgloader.io](pgloader) did download the file from
its HTTP URL location then *unziped* it before loading it.

Also, the *WARNING* messages we see here are expected as the PostgreSQL
database is empty when running the command, and pgloader is using the SQL
commands `DROP TABLE IF NOT EXISTS` when the given command uses the `include
drop` option.

Note that the output of the command has been edited to facilitate its
browsing online.
