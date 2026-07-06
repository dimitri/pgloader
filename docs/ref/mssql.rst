MS SQL to Postgres
==================

This command instructs pgloader to load data from a MS SQL database.
Automatic discovery of the schema is supported, including build of the
indexes, primary and foreign keys constraints.

Using default settings
----------------------

Here is the simplest command line example, which might be all you need:

::

   $ pgloader mssql://user@mshost/dbname pgsql://pguser@pghost/dbname

Using advanced options and a load command file
----------------------------------------------

The command then would be:

::

   $ pgloader ms.load

And the contents of the command file ``ms.load`` could be inspired from the
following:

::

    load database
         from mssql://user@host/dbname
         into postgresql:///dbname

    including only table names like 'GlobalAccount' in schema 'dbo'

    set work_mem to '16MB', maintenance_work_mem to '512 MB'

    before load do $$ drop schema if exists dbo cascade; $$;

Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

MS SQL Database Source Specification: FROM
------------------------------------------

Connection string to an existing MS SQL database server that accepts TCP/IP
connections. pgloader v4 uses the official Microsoft JDBC driver (mssql-jdbc)
so no FreeTDS or ODBC installation is required.

Both pgloader-native and JDBC URL formats are accepted::

    mssql://[user[:password]@][host][:port][/dbname]
    jdbc:sqlserver://host[:port][;param=value;...]

The JDBC URL format is particularly useful when you need to pass driver-specific
parameters such as ``encrypt``, ``trustServerCertificate``, or
``applicationIntent``:

.. code-block:: sql

    -- Native pgloader URI (encrypt=false by default)
    FROM mssql://sa:password@mssql:1433/mydb

    -- JDBC URL — no URL-encoding needed for special chars in password,
    --            and all MSSQL JDBC properties are available:
    FROM jdbc:sqlserver://mssql:1433;databaseName=mydb;user=sa;password=P@$$w0rd;encrypt=false

    -- Azure SQL / SQL Server with SSL:
    FROM jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=mydb;user=admin;password=secret;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net

When using the native ``mssql://`` URI, ``encrypt=false`` is set automatically
(matching the previous FreeTDS behaviour). Override it by using a JDBC URL.

MS SQL Database Migration Options: WITH
---------------------------------------

When loading from a `MS SQL` database, the same options as when loading a
`MYSQL` database are supported. Please refer to the MYSQL section. The
following options are added:

  - *create schemas*

    When this option is listed, pgloader creates the same schemas as found
    on the MS SQL instance. This is the default.

  - *create no schemas*

    When this option is listed, pgloader refrains from creating any schemas
    at all, you must then ensure that the target schema do exist.

MS SQL Database Casting Rules
-----------------------------

CAST
^^^^

The ``CAST`` clause overrides or extends the built-in type mapping rules
that pgloader applies when converting an MS SQL schema to PostgreSQL.

The built-in defaults map all character types (``char``, ``nchar``,
``varchar``, ``nvarchar``, ``ntext``) to ``text`` and discard the length
modifier.  Use ``CAST`` when you want to preserve types and/or length
modifiers.

User-defined rules are evaluated before the built-in defaults, and the
first matching rule wins.

Synopsis
""""""""

::

  CAST cast_rule [, ...]

where *cast_rule* is:

.. code-block:: text

  { type source_type | column table_name.column_name }
    [ guard ... ]
    [ to target_type ]
    [ option ... ]
    [ using function_name ]

where *guard* is one of:

.. code-block:: text

  when default 'string'
  when ( typemod_expression )
  and not null

and *option* is one of:

.. code-block:: text

  { drop | keep } default
  { drop | keep } typemod
  { drop | keep | set } not null
  { drop | keep } extra

Parameters
""""""""""

``type`` *source_type*
    Match all columns whose MS SQL data type is *source_type*
    (case-insensitive).  This is the most common form.

``column`` *table_name*.*column_name*
    Match a single column by its qualified name.  This form accepts the
    same *option* list as the ``type`` form but no *guard*.

``to`` *target_type*
    The PostgreSQL type to emit in ``CREATE TABLE``.  When omitted, the
    type from the first matching built-in rule is used and only the
    *options* take effect.

Guards
""""""

Guards narrow the set of columns a rule applies to.  Multiple guards on
the same rule are combined with AND semantics.

``when default '``\ *string*\ ``'``
    Match only columns whose default value equals *string*.

``when (`` *typemod_expression* ``)``
    Match only columns whose type modifier satisfies *typemod_expression*,
    a Common Lisp s-expression where ``precision`` and ``scale`` are bound
    to the modifier components.  For example, ``nvarchar(40)`` has
    ``precision = 40`` and ``scale = 0``; ``nvarchar(max)`` has
    ``precision = -1``.

    Supported operators: ``=``, ``<``, ``>``, ``<=``, ``>=``; combine
    with ``and`` and ``or``.

``and not null``
    Match only columns declared ``NOT NULL`` in the source.

Options
"""""""

``drop typemod`` / ``keep typemod``
    Controls whether the length modifier is carried into the PostgreSQL
    type definition.

    *  **Default for all built-in MSSQL rules**: ``drop typemod`` — the
       modifier is discarded.  ``nvarchar(40)`` with ``type nvarchar to
       varchar`` (or the built-in rule) produces a plain ``varchar``.
    *  ``keep typemod`` preserves it.  ``nvarchar(40)`` with
       ``type nvarchar to varchar keep typemod`` produces ``varchar(40)``.

``drop default`` / ``keep default``
    Controls whether the source column's ``DEFAULT`` expression is copied
    to the ``CREATE TABLE`` statement.  The default behaviour is ``drop
    default`` because MS SQL default expressions are often not valid
    PostgreSQL syntax.

``drop not null`` / ``keep not null`` / ``set not null``
    Controls the ``NOT NULL`` constraint on the target column.
    ``set not null`` adds the constraint even when the source column is
    nullable.

Examples
""""""""

Preserve character-type length modifiers instead of mapping everything to
``text``::

  CAST type nvarchar  to varchar keep typemod,
       type nchar     to char    keep typemod,
       type varchar   to varchar keep typemod,
       type char      to char    keep typemod

With these rules, ``nvarchar(40)`` becomes ``varchar(40)``,
``nchar(10)`` becomes ``char(10)``, and so on.  Without them the
built-in defaults would produce plain ``text`` for all four types.

Handle ``nvarchar(max)`` (the driver reports ``precision = -1``)
separately, keeping it as ``text``, while using a fixed-length type for
all other ``nvarchar`` columns::

  CAST type nvarchar when (= precision -1) to text drop typemod,
       type nvarchar                        to varchar keep typemod

Override a single column while keeping the general rule::

  CAST type    nvarchar          to varchar keep typemod,
       column  dbo.product.code  to char(8) drop typemod

MS SQL Views Support
--------------------

MS SQL views support allows pgloader to migrate view as if they were base
tables. This feature then allows for on-the-fly transformation from MS SQL
to PostgreSQL, as the view definition is used rather than the base data.

MATERIALIZE VIEWS
^^^^^^^^^^^^^^^^^

This clause allows you to implement custom data processing at the data
source by providing a *view definition* against which pgloader will query
the data. It's not possible to just allow for plain `SQL` because we want to
know a lot about the exact data types of each column involved in the query
output.

This clause expect a comma separated list of view definitions, each one
being either the name of an existing view in your database or the following
expression::

  *name* `AS` `$$` *sql query* `$$`

The *name* and the *sql query* will be used in a `CREATE VIEW` statement at
the beginning of the data loading, and the resulting view will then be
dropped at the end of the data loading.

MATERIALIZE ALL VIEWS
^^^^^^^^^^^^^^^^^^^^^

Same behaviour as *MATERIALIZE VIEWS* using the dynamic list of views as
returned by MS SQL rather than asking the user to specify the list.

MS SQL Partial Migration
------------------------


INCLUDING ONLY TABLE NAMES LIKE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table name patterns used to limit the
tables to migrate to a sublist. More than one such clause may be used, they
will be accumulated together.

The patterns use SQL Server's native ``LIKE`` syntax, meaning ``%`` matches
any sequence of characters and ``_`` matches any single character. Regular
expressions are not supported for MS SQL sources because SQL Server has no
native regex operator; the filter is applied directly in the ``WHERE`` clause
of the introspection query sent to the source database.

Example::

  including only table names like 'GlobalAccount' in schema 'dbo'

EXCLUDING TABLE NAMES LIKE
^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table name patterns used to exclude
table names from the migration. This filter only applies to the result of
the *INCLUDING* filter.

::

   excluding table names like 'LocalAccount' in schema 'dbo'

MS SQL Schema Transformations
-----------------------------

ALTER SCHEMA '...' RENAME TO '...'
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Allows to rename a schema on the flight, so that for instance the tables
found in the schema 'dbo' in your source database will get migrated into the
schema 'public' in the target database with this command::

  alter schema 'dbo' rename to 'public'

ALTER TABLE NAMES MATCHING ... IN SCHEMA '...'
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table names or *regular expressions*
that you want to target in the pgloader *ALTER TABLE* command. Available
actions are *SET SCHEMA*, *RENAME TO*, and *SET*::

    ALTER TABLE NAMES MATCHING ~/_list$/, 'sales_by_store', ~/sales_by/
      IN SCHEMA 'dbo'
     SET SCHEMA 'mv'
   
    ALTER TABLE NAMES MATCHING 'film' IN SCHEMA 'dbo' RENAME TO 'films'
    
    ALTER TABLE NAMES MATCHING ~/./ IN SCHEMA 'dbo' SET (fillfactor='40')
    
    ALTER TABLE NAMES MATCHING ~/./ IN SCHEMA 'dbo' SET TABLESPACE 'tlbspc'

You can use as many such rules as you need. The list of tables to be
migrated is searched in pgloader memory against the *ALTER TABLE* matching
rules, and for each command pgloader stops at the first matching criteria
(regexp or string).

No *ALTER TABLE* command is sent to PostgreSQL, the modification happens at
the level of the pgloader in-memory representation of your source database
schema. In case of a name change, the mapping is kept and reused in the
*foreign key* and *index* support.

The *SET ()* action takes effect as a *WITH* clause for the `CREATE TABLE`
command that pgloader will run when it has to create a table.

The *SET TABLESPACE* action takes effect as a *TABLESPACE* clause for the
`CREATE TABLE` command that pgloader will run when it has to create a table.

The matching is done in pgloader itself, with a Common Lisp regular
expression lib, so doesn't depend on the *LIKE* implementation of MS SQL,
nor on the lack of support for regular expressions in the engine.

Azure SQL / Azure Active Directory Authentication
-------------------------------------------------

pgloader v4 uses the Microsoft JDBC driver (``mssql-jdbc``) and bundles both
MSAL4J and the Azure Identity library (``azure-identity``), so all
non-browser-dependent Entra ID authentication modes work out of the box.
Always use a ``jdbc:sqlserver://`` URL for Azure SQL so that ``encrypt=true``
and ``authentication=`` reach the driver exactly as written.

Quick reference:

+--------------------------------------------+------------------------------------------------+
| Mode                                       | Typical use case                               |
+============================================+================================================+
| ``ActiveDirectoryServicePrincipal``        | CI/CD — app registration (client ID + secret)  |
+--------------------------------------------+------------------------------------------------+
| ``ActiveDirectoryServicePrincipalCertif.`` | CI/CD — app registration with certificate      |
+--------------------------------------------+------------------------------------------------+
| ``ActiveDirectoryManagedIdentity``         | Azure VM / App Service / AKS — no credentials |
+--------------------------------------------+------------------------------------------------+
| ``ActiveDirectoryDefault``                 | Developer laptop: ``az login`` fallback chain  |
+--------------------------------------------+------------------------------------------------+
| ``ActiveDirectoryIntegrated``              | Windows / Kerberos domain-joined machine       |
+--------------------------------------------+------------------------------------------------+
| ``ActiveDirectoryPassword``                | **Deprecated** — blocked by MFA enforcement    |
+--------------------------------------------+------------------------------------------------+

``ActiveDirectoryServicePrincipal``
    Authenticate as an Azure AD application (client ID + client secret) —
    the recommended mode for CI/CD pipelines and automated migrations::

        FROM "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=mydb;\
authentication=ActiveDirectoryServicePrincipal;\
AADSecurePrincipalId=<client-id>;AADSecurePrincipalSecret=<client-secret>;\
encrypt=true"

``ActiveDirectoryManagedIdentity``
    Authenticate using the managed identity of the Azure VM, App Service,
    AKS pod, or container that pgloader runs in — no credentials in the URL::

        FROM "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=mydb;\
authentication=ActiveDirectoryManagedIdentity;encrypt=true"

``ActiveDirectoryDefault``
    The recommended mode for developer workstations.  Uses the Azure
    Identity ``DefaultAzureCredential`` chain: environment variables →
    workload identity → managed identity → **Azure CLI** → Azure PowerShell.
    After a one-time ``az login``, no credentials need to appear in the URL::

        FROM "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=mydb;\
authentication=ActiveDirectoryDefault;encrypt=true;trustServerCertificate=false"

    This also works headlessly in WSL2: ``DefaultAzureCredential`` falls back
    to ``AzureCliCredential``, which shells out to ``az account
    get-access-token`` using your existing ``az login`` session — no browser
    popup is needed.

``ActiveDirectoryIntegrated``
    Authenticate via Kerberos on a domain-joined machine.  Requires a valid
    Kerberos ticket (``kinit`` on Linux/macOS, automatic on Windows)::

        FROM "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=mydb;\
authentication=ActiveDirectoryIntegrated;encrypt=true"

``ActiveDirectoryPassword``
    .. note::

       **Deprecated by Microsoft.**  This mode is incompatible with mandatory
       Entra ID multifactor authentication (MFA), which Microsoft now enforces
       by default across most tenants.  Attempting it in an MFA-required
       tenant returns ``AADSTS50076``.  Use ``ActiveDirectoryDefault`` or
       ``ActiveDirectoryServicePrincipal`` instead.

    ::

        FROM "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=mydb;\
authentication=ActiveDirectoryPassword;user=user@tenant.onmicrosoft.com;\
password=secret;encrypt=true;trustServerCertificate=false"

.. note::

   ``ActiveDirectoryInteractive`` makes MSAL4J open a system browser via
   ``xdg-open`` for an OAuth2 interactive login.  It works on native Windows,
   native macOS, and Linux desktops with a display server.  It **fails** in
   WSL2 (``linux_xdg_open_failed`` — WSL2 has no JVM-accessible browser
   integration), headless Linux, Docker/Podman containers, and CI pipelines.
   Use ``ActiveDirectoryDefault`` with an existing ``az login`` session instead.

   The native ``mssql://`` scheme defaults to ``encrypt=false`` to match
   on-premises SQL Server behaviour.  For Azure SQL use the
   ``jdbc:sqlserver://`` form shown above so that ``encrypt=true`` is set
   explicitly.

MS SQL Driver setup and encoding
--------------------------------

pgloader is using the `FreeTDS` driver, and internally expects the data to
be sent in utf-8. To achieve that, you can configure the FreeTDS driver with
those defaults, in the file `~/.freetds.conf`::

    [global]
        tds version = 7.4
        client charset = UTF-8

Default MS SQL Casting Rules
----------------------------

When migrating from MS SQL the following Casting Rules are provided:

Numbers::

  type tinyint to smallint

  type float to float   using float-to-string
  type real to real     using float-to-string
  type double to double precision     using float-to-string
  type numeric to numeric     using float-to-string
  type decimal to numeric     using float-to-string
  type money to numeric     using float-to-string
  type smallmoney to numeric     using float-to-string

Texts::

  type char      to text drop typemod
  type nchar     to text drop typemod
  type varchar   to text drop typemod
  type nvarchar  to text drop typemod
  type xml       to text drop typemod

Binary::

  type binary    to bytea using byte-vector-to-bytea
  type varbinary to bytea using byte-vector-to-bytea

Date::

  type datetime    to timestamptz
  type datetime2   to timestamptz

Others::

  type bit to boolean
  type hierarchyid to bytea
  type geography to bytea
  type uniqueidentifier to uuid using sql-server-uniqueidentifier-to-uuid

