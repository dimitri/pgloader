Migrating a Redhift Database to PostgreSQL
==========================================

This command instructs pgloader to load data from a database connection.
Automatic discovery of the schema is supported, including build of the
indexes, primary and foreign keys constraints. A default set of casting
rules are provided and might be overloaded and appended to by the command.

The command and behavior are the same as when migration from a PostgreSQL
database source. pgloader automatically discovers that it's talking to a
Redshift database by parsing the output of the `SELECT version()` SQL query.

