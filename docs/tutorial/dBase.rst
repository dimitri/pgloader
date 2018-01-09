Loading dBase files with pgloader
---------------------------------

The dBase format is still in use in some places as modern tools such as
*Filemaker* and *Excel* offer some level of support for it. Speaking of
support in modern tools, pgloader is right there on the list too!

The Command
^^^^^^^^^^^

To load data with pgloader you need to define in a *command* the operations in
some details. Here's our example for loading a dBase file, using a file
provided by the french administration.

You can find more files from them at the `Insee
<http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement.asp>`_
website.

Here's our command::

    LOAD DBF
        FROM http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement/2013/dbf/historiq2013.zip
        INTO postgresql:///pgloader
        WITH truncate, create table
         SET client_encoding TO 'latin1';

Note that here pgloader will benefit from the meta-data information found in
the dBase file to create a PostgreSQL table capable of hosting the data as
described, then load the data.

Loading the data
^^^^^^^^^^^^^^^^

Let's start the `pgloader` command with our `dbf-zip.load` command file::

    $ pgloader dbf-zip.load
    ... LOG Starting pgloader, log system is ready.
    ... LOG Parsing commands from file "/Users/dim/dev/pgloader/test/dbf-zip.load"
    ... LOG Fetching 'http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement/2013/dbf/historiq2013.zip'
    ... LOG Extracting files from archive '//private/var/folders/w7/9n8v8pw54t1gngfff0lj16040000gn/T/pgloader//historiq2013.zip'
    
           table name       read   imported     errors            time
    -----------------  ---------  ---------  ---------  --------------
             download          0          0          0          0.167s
              extract          0          0          0          1.010s
     create, truncate          0          0          0          0.071s
    -----------------  ---------  ---------  ---------  --------------
         historiq2013       9181       9181          0          0.658s
    -----------------  ---------  ---------  ---------  --------------
    Total import time       9181       9181          0          1.906s

We can see that `pgloader <http://pgloader.io>`_ did download the file from
its HTTP URL location then *unziped* it before the loading itself.

Note that the output of the command has been edited to facilitate its
browsing online.
