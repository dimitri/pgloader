# Migrating from MySQL with pgloader

If you want to migrate your data over to
[PostgreSQL](http://www.postgresql.org) from MySQL then pgloader is the tool
of choice!

Most tools around are skipping the main problem with migrating from MySQL,
which is to do with the type casting and data sanitizing that needs to be
done. pgloader will not leave you alone on those topics.

## The Command

To load data with [pgloader](http://pgloader.tapoueh.org/) you need to
define in a *command* the operations in some details. Here's our example for
loading the
[MySQL Sakila Sample Database](http://dev.mysql.com/doc/sakila/en/):

Here's our command:

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

You can see the full list of options in the
[pgloader reference manual](pgloader.1.html), with a complete description
of the options you see here.

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
view at bofore loading the data, then drops it again at the end.

## Loading the data

Let's start the `pgloader` command with our `sakila.load` command file:

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
`DROP TABLE IF NOT EXISTS` when the given command uses the `include drop`
option.

Note that the output of the command has been edited to facilitate its
browsing online.
