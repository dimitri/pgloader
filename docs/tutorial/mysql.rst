Migrating from MySQL to PostgreSQL
----------------------------------

If you want to migrate your data over to `PostgreSQL
<http://www.postgresql.org>`_ from MySQL then pgloader is the tool of
choice!

Most tools around are skipping the main problem with migrating from MySQL,
which is to do with the type casting and data sanitizing that needs to be
done. pgloader will not leave you alone on those topics.

In a Single Command Line
^^^^^^^^^^^^^^^^^^^^^^^^

As an example, we will use the f1db database from <http://ergast.com/mrd/>
which which provides a historical record of motor racing data for
non-commercial purposes. You can either use their API or download the whole
database at `http://ergast.com/downloads/f1db.sql.gz
<http://ergast.com/downloads/f1db.sql.gz>`_. Once you've done that load the
database in MySQL::

    $ mysql -u root
    > create database f1db;
    > source f1db.sql

Now let's migrate this database into PostgreSQL in a single command line::

    $ createdb f1db
    $ pgloader mysql://root@localhost/f1db pgsql:///f1db

Done! All with schema, table definitions, constraints, indexes, primary
keys, *auto_increment* columns turned into *bigserial* , foreign keys,
comments, and if you had some MySQL default values such as *ON UPDATE
CURRENT_TIMESTAMP* they would have been translated to a `PostgreSQL before
update trigger
<https://www.postgresql.org/docs/current/static/plpgsql-trigger.html>`_
automatically.

::

    $ pgloader mysql://root@localhost/f1db pgsql:///f1db
    2017-06-16T08:56:14.064000+02:00 LOG Main logs in '/private/tmp/pgloader/pgloader.log'
    2017-06-16T08:56:14.068000+02:00 LOG Data errors in '/private/tmp/pgloader/'
    2017-06-16T08:56:19.542000+02:00 LOG report summary reset
                   table name       read   imported     errors      total time
    -------------------------  ---------  ---------  ---------  --------------
              fetch meta data         33         33          0          0.365s 
               Create Schemas          0          0          0          0.007s 
             Create SQL Types          0          0          0          0.006s 
                Create tables         26         26          0          0.068s 
               Set Table OIDs         13         13          0          0.012s 
    -------------------------  ---------  ---------  ---------  --------------
      f1db.constructorresults      11011      11011          0          0.205s 
                f1db.circuits         73         73          0          0.150s 
            f1db.constructors        208        208          0          0.059s 
    f1db.constructorstandings      11766      11766          0          0.365s 
                 f1db.drivers        841        841          0          0.268s 
                f1db.laptimes     413578     413578          0          2.892s 
         f1db.driverstandings      31420      31420          0          0.583s 
                f1db.pitstops       5796       5796          0          2.154s 
                   f1db.races        976        976          0          0.227s 
              f1db.qualifying       7257       7257          0          0.228s 
                 f1db.seasons         68         68          0          0.527s 
                 f1db.results      23514      23514          0          0.658s 
                  f1db.status        133        133          0          0.130s 
    -------------------------  ---------  ---------  ---------  --------------
      COPY Threads Completion         39         39          0          4.303s 
               Create Indexes         20         20          0          1.497s 
       Index Build Completion         20         20          0          0.214s 
              Reset Sequences          0         10          0          0.058s 
                 Primary Keys         13         13          0          0.012s 
          Create Foreign Keys          0          0          0          0.000s 
              Create Triggers          0          0          0          0.001s 
             Install Comments          0          0          0          0.000s 
    -------------------------  ---------  ---------  ---------  --------------
            Total import time     506641     506641          0          5.547s 

You may need to have special cases to take care of tho, or views that you
want to materialize while doing the migration. In advanced case you can use
the pgloader command.

The Command
^^^^^^^^^^^

To load data with pgloader you need to define in a *command* the operations
in some details. Here's our example for loading the `MySQL Sakila Sample
Database <http://dev.mysql.com/doc/sakila/en/>`_.

Here's our command::

    load database
         from      mysql://root@localhost/sakila
         into postgresql:///sakila
    
     WITH include drop, create tables, no truncate,
          create indexes, reset sequences, foreign keys
    
      SET maintenance_work_mem to '128MB', work_mem to '12MB', search_path to 'sakila'
    
     CAST type datetime to timestamptz
                    drop default drop not null using zero-dates-to-null,
          type date drop not null drop default using zero-dates-to-null
         
     MATERIALIZE VIEWS film_list, staff_list
    
     -- INCLUDING ONLY TABLE NAMES MATCHING ~/film/, 'actor'
     -- EXCLUDING TABLE NAMES MATCHING ~<ory>
    
     BEFORE LOAD DO
     $$ create schema if not exists sakila; $$;

Note that here pgloader will benefit from the meta-data information found in
the MySQL database to create a PostgreSQL database capable of hosting the
data as described, then load the data.

In particular, some specific *casting rules* are given here, to cope with
date values such as `0000-00-00` that MySQL allows and PostgreSQL rejects
for not existing in our calendar. It's possible to add per-column casting
rules too, which is useful is some of your `tinyint` are in fact `smallint`
while some others are in fact `boolean` values.

Finaly note that we are using the *MATERIALIZE VIEWS* clause of pgloader:
the selected views here will be migrated over to PostgreSQL *with their
contents*.

It's possible to use the *MATERIALIZE VIEWS* clause and give both the name
and the SQL (in MySQL dialect) definition of view, then pgloader creates the
view before loading the data, then drops it again at the end.

Loading the data
^^^^^^^^^^^^^^^^

Let's start the `pgloader` command with our `sakila.load` command file::

    $ pgloader sakila.load
    ... LOG Starting pgloader, log system is ready.
    ... LOG Parsing commands from file "/Users/dim/dev/pgloader/test/sakila.load"
       <WARNING: table "xxx" does not exists have been edited away>
    
                table name       read   imported     errors            time
    ----------------------  ---------  ---------  ---------  --------------
               before load          1          1          0          0.007s
           fetch meta data         45         45          0          0.402s
              create, drop          0         36          0          0.208s
    ----------------------  ---------  ---------  ---------  --------------
                     actor        200        200          0          0.071s
                   address        603        603          0          0.035s
                  category         16         16          0          0.018s
                      city        600        600          0          0.037s
                   country        109        109          0          0.023s
                  customer        599        599          0          0.073s
                      film       1000       1000          0          0.135s
                film_actor       5462       5462          0          0.236s
             film_category       1000       1000          0          0.070s
                 film_text       1000       1000          0          0.080s
                 inventory       4581       4581          0          0.136s
                  language          6          6          0          0.036s
                   payment      16049      16049          0          0.539s
                    rental      16044      16044          0          0.648s
                     staff          2          2          0          0.041s
                     store          2          2          0          0.036s
                 film_list        997        997          0          0.247s
                staff_list          2          2          0          0.135s
    Index Build Completion          0          0          0          0.000s
    ----------------------  ---------  ---------  ---------  --------------
            Create Indexes         41         41          0          0.964s
           Reset Sequences          0          1          0          0.035s
              Foreign Keys         22         22          0          0.254s
    ----------------------  ---------  ---------  ---------  --------------
         Total import time      48272      48272          0          3.502s

The *WARNING* messages we see here are expected as the PostgreSQL database
is empty when running the command, and pgloader is using the SQL commands
`DROP TABLE IF EXISTS` when the given command uses the `include drop`
option.

Note that the output of the command has been edited to facilitate its
browsing online.
