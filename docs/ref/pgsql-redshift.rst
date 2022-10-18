Redshift to Postgres
====================

The command and behavior are the same as when migration from a PostgreSQL
database source, see :ref:`migrating_to_pgsql`. pgloader automatically
discovers that it's talking to a Redshift database by parsing the output of
the ``SELECT version()`` SQL query.

Redshift as a data source
^^^^^^^^^^^^^^^^^^^^^^^^^

Redshift is a variant of PostgreSQL version 8.0.2, which allows pgloader to
work with only a very small amount of adaptation in the catalog queries
used. In other words, migrating from Redshift to PostgreSQL works just the
same as when migrating from a PostgreSQL data source, including the
connection string specification.

Redshift as a data destination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Redshift variant of PostgreSQL 8.0.2 does not have support for the
``COPY FROM STDIN`` feature that pgloader normally relies upon. To use COPY
with Redshift, the data must first be made available in an S3 bucket.

First, pgloader must authenticate to Amazon S3. pgloader uses the following
setup for that:

  - ``~/.aws/config``

    This INI formatted file contains sections with your default region and
    other global values relevant to using the S3 API. pgloader parses it to
    get the region when it's setup in the ``default`` INI section.

    The environment variable ``AWS_DEFAULT_REGION`` can be used to override
    the configuration file value.
    
  - ``~/.aws/credentials``

    The INI formatted file contains your authentication setup to Amazon,
    with the properties ``aws_access_key_id`` and ``aws_secret_access_key``
    in the section ``default``. pgloader parses this file for those keys,
    and uses their values when communicating with Amazon S3.

    The environment variables ``AWS_ACCESS_KEY_ID`` and
    ``AWS_SECRET_ACCESS_KEY`` can be used to override the configuration file
    
  - ``AWS_S3_BUCKET_NAME``
    
    Finally, the value of the environment variable ``AWS_S3_BUCKET_NAME`` is
    used by pgloader as the name of the S3 bucket where to upload the files
    to COPY to the Redshift database. The bucket name defaults to
    ``pgloader``.

Then pgloader works as usual, see the other sections of the documentation
for the details, depending on the data source (files, other databases, etc).
When preparing the data for PostgreSQL, pgloader now uploads each batch into
a single CSV file, and then issue such as the following, for each batch:

::

  COPY <target_table_name>
        FROM 's3://<s3 bucket>/<s3-filename-just-uploaded>'
        FORMAT CSV
        TIMEFORMAT 'auto'
        REGION '<aws-region>'
        ACCESS_KEY_ID '<aws-access-key-id>'
        SECRET_ACCESS_KEY '<aws-secret-access-key>;

This is the only difference with a PostgreSQL core version, where pgloader
can rely on the classic ``COPY FROM STDIN`` command, which allows to send
data through the already established connection to PostgreSQL.
