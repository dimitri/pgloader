# pgloader

`pgloader` is a client side data loader for
[PostgreSQL](http://www.postgresql.org/). It knows how to find erroneous
rows in your input so that the good ones are loaded in the database and the
bad ones accumulated in a *reject file* for later processing.

`pgloader` supports `CSV` file format, *fixed sized* file format and *text*
file format with some advanced parsing options so as to be able to read
*Informix* `UNLOAD` output.

`pgloader` implements many interesting features such as column reformating
in python, user defined constant columns, column remapping between file and
database table, and some more: have a look at the tutorial.

## complete tutorial

See [pgloader tutorial](http://tapoueh.org/pgsql/pgloader.html) for a
complete tutorial explaining in details how to setup and use pgloader.

## credits

pgloader is sponsored by [2ndQuadrant](http://www.2ndquadrant.com/), we
offer commercial support and training if you need professional help.
