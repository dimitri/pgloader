Loading CSV Data with pgloader
------------------------------

CSV means *comma separated values* and is often found with quite varying
specifications. pgloader allows you to describe those specs in its command.

The Command
^^^^^^^^^^^

To load data with pgloader you need to define in a *command* the operations in
some details. Here's our example for loading CSV data::

     LOAD CSV
          FROM 'path/to/file.csv' (x, y, a, b, c, d)
          INTO postgresql:///pgloader?csv (a, b, d, c)
     
          WITH truncate,
               skip header = 1,
               fields optionally enclosed by '"',
               fields escaped by double-quote,
               fields terminated by ','
     
           SET client_encoding to 'latin1',
               work_mem to '12MB',
               standard_conforming_strings to 'on'
     
        BEFORE LOAD DO
         $$ drop table if exists csv; $$,
         $$ create table csv (
             a bigint,
             b bigint,
             c char(2),
             d text
            );
       $$;

The Data
^^^^^^^^

This command allows loading the following CSV file content::

    Header, with a © sign
    "2.6.190.56","2.6.190.63","33996344","33996351","GB","United Kingdom"
    "3.0.0.0","4.17.135.31","50331648","68257567","US","United States"
    "4.17.135.32","4.17.135.63","68257568","68257599","CA","Canada"
    "4.17.135.64","4.17.142.255","68257600","68259583","US","United States"
    "4.17.143.0","4.17.143.15","68259584","68259599","CA","Canada"
    "4.17.143.16","4.18.32.71","68259600","68296775","US","United States"

Loading the data
^^^^^^^^^^^^^^^^

Here's how to start loading the data. Note that the ouput here has been
edited so as to facilitate its browsing online::

    $ pgloader csv.load
    ... LOG Starting pgloader, log system is ready.
    ... LOG Parsing commands from file "/Users/dim/dev/pgloader/test/csv.load"
    
           table name       read   imported     errors            time
    -----------------  ---------  ---------  ---------  --------------
          before load          2          2          0          0.039s
    -----------------  ---------  ---------  ---------  --------------
                  csv          6          6          0          0.019s
    -----------------  ---------  ---------  ---------  --------------
    Total import time          6          6          0          0.058s

The result
^^^^^^^^^^

As you can see, the command described above is filtering the input and only
importing some of the columns from the example data file. Here's what gets
loaded in the PostgreSQL database::

    pgloader# table csv;
        a     |    b     | c  |       d        
    ----------+----------+----+----------------
     33996344 | 33996351 | GB | United Kingdom
     50331648 | 68257567 | US | United States
     68257568 | 68257599 | CA | Canada
     68257600 | 68259583 | US | United States
     68259584 | 68259599 | CA | Canada
     68259600 | 68296775 | US | United States
    (6 rows)
