(ns pgloader.load-file.grammar)

(def grammar
  "Instaparse grammar for the pgloader DSL (Phase 1 subset: CSV + MySQL)."
  "load-command = load-csv | load-copy | load-dbf | load-database | load-fixed | load-archive

    load-csv = <'LOAD'> <ws> <'CSV'>
               <ws> <'FROM'> <ws> from-source
               (<ws> source-encoding)?
               column-list?
               (<ws> having-fields)?
                <ws> <'INTO'> <ws> pg-uri
                (<ws> <'INTO'> (<'TABLE'>)? qualified-name)?
               (<ws> target-table-clause)?
                (<ws> target-columns-clause)?
                (<ws> column-list)?
                with-csv-clause?
               column-list?
               load-csv-clause*
               (<opt-ws> <';'>)?

    load-csv-clause = set-clause | before-load-do | after-load-do | having-fields

    load-copy = <'LOAD'> <ws> <'COPY'>
                <ws> <'FROM'> <ws> copy-source
                column-list?
                (<ws> source-encoding)?
                (<ws> having-fields)?
                 <ws> <'INTO'> <ws> pg-uri
                 (<ws> <'INTO'> (<'TABLE'>)? qualified-name)?
                (<ws> target-table-clause)?
                 (<ws> target-columns-clause)?
                 (<ws> column-list)?
                 with-copy-clause?
                column-list?
                load-copy-clause*
                <opt-ws> <';'>

    load-copy-clause = set-clause | before-load-do | after-load-do | having-fields

    load-dbf = <'LOAD'> <ws> <'DBF'>
               <ws> <'FROM'> <ws> dbf-source
               column-list?
               (<ws> source-encoding)?
               (<ws> having-fields)?
                <ws> <'INTO'> <ws> pg-uri
                (<ws> <'INTO'> (<'TABLE'>)? qualified-name)?
               (<ws> target-table-clause)?
                (<ws> target-columns-clause)?
                (<ws> column-list)?
                with-dbf-clause?
               column-list?
               load-dbf-clause*
               (<opt-ws> <';'>)?

    load-dbf-clause = set-clause | before-load-do | after-load-do | having-fields | cast-clause

    load-database = <'LOAD'> <ws> <'DATABASE'>
                    <ws> <'FROM'> <ws> source-uri
                    <ws> <'INTO'> <ws> pg-uri
                    load-database-clause*
                    (<opt-ws> <';'>)?

    load-database-clause = with-db-clause | set-clause | cast-clause
                          | alter-schema-clause | alter-table-clause
                          | materialize-views | including-only | excluding-only
                          | before-load-do | after-load-do | distribute-section
                          | decoding-as-clause

    distribute-section = distribute-clause+
    distribute-clause = distribute-using-from | distribute-using | distribute-reference
    distribute-reference = <'DISTRIBUTE'> <ws> table-name <ws> <'AS'> <ws> <'REFERENCE'> <ws> <'TABLE'>
    distribute-using = <'DISTRIBUTE'> <ws> table-name <ws> <'USING'> <ws> column-name
    distribute-using-from = <'DISTRIBUTE'> <ws> table-name <ws> <'USING'> <ws> column-name <ws> <'FROM'> <ws> from-table-list
    from-table-list = table-name (<opt-ws> <','> <opt-ws> table-name)*

   from-source = filepath | stdin-source | inline-source | glob-source | copy-source | dbf-source | http-source | csv-filename-matching
   copy-source = <'copy'> <'://'> #'[^\\s;]+'
   dbf-source  = dbf-filepath | http-source
   dbf-filepath = <'dbf'> <'://'> #'[^\\s;]+'
   http-source = #'https?://[^\\s;]+'
   filepath    = <'\\''> #\"[^']+\" <'\\''>
   stdin-source = <'stdin'>
   inline-source = <'inline'>
   glob-source = <'all'> <ws> <'filenames'> <ws> <'matching'> <ws> file-pattern <ws> <'in'> <ws> <'directory'> <ws> filepath
   file-pattern = (<'~/'> #'[^/]+' <'/'>) | (<'~<'> #'[^>]+' <'>'>)
    source-encoding = <'with'> <ws> <'encoding'> <ws> (quoted-string | bare-encoding)
    bare-encoding = #'[a-zA-Z][a-zA-Z0-9_-]*'

   load-fixed = <'LOAD'> <ws> <'FIXED'>
                <ws> <'FROM'> <ws> fixed-from-source
                (<ws> source-encoding)?
                (<ws> fixed-field-list)?
                <ws> <'INTO'> <ws> pg-uri
                (<ws> <'INTO'> (<'TABLE'>)? qualified-name)?
                (<ws> target-table-clause)?
                (<ws> target-columns-clause)?
                (<ws> column-list)?
                with-fixed-clause?
                load-fixed-clause*
                (<opt-ws> <';'>)?

    load-fixed-clause = set-clause | before-load-do | after-load-do

    fixed-from-source = fixed-filepath | inline-source | fixed-filename-matching
    fixed-filepath = <'fixed'> <'://'> #'[^\\s;(]+'
    fixed-filename-matching = <'FILENAME'> <ws> <'MATCHING'> <ws> file-pattern

    fixed-field-list = <'('> <opt-ws> fixed-field (<opt-ws> <','> <opt-ws> fixed-field)* <opt-ws> <')'>
    fixed-field = column-name <ws> <'from'> <ws> integer <ws> <'for'> <ws> integer fixed-field-option-group*
    fixed-field-option-group = <opt-ws> <'['> <opt-ws> fixed-field-option (<opt-ws> <','> <opt-ws> fixed-field-option)* <opt-ws> <']'>
    fixed-field-option = trim-right-ws | null-if-blanks-opt | null-if-spec
    trim-right-ws     = <'trim'> <ws> <'right'> <ws> <'whitespace'>
    null-if-blanks-opt = <'null'> <ws> <'if'> <ws> <'blanks'>

    with-fixed-clause = <'WITH'> <ws> fixed-option (<opt-ws> <','> <opt-ws> fixed-option)*
    fixed-option = truncate | create-tables | create-table | create-no-tables | batch-rows
                 | batch-size | batch-concurrency | disable-triggers | drop-indexes | fixed-header
    fixed-header = <'fixed'> <ws> <'header'>

   load-archive = <'LOAD'> <ws> <'ARCHIVE'>
                  <ws> <'FROM'> <ws> archive-from-source
                  (<ws> <'INTO'> <ws> pg-uri)?
                  load-archive-clause*
                  archive-sub-command+
                  <opt-ws> <';'>

    archive-from-source = http-source | filepath
    load-archive-clause = before-load-do | after-load-do
    archive-sub-command = load-fixed | load-csv | load-dbf
    csv-filename-matching = <'FILENAME'> <ws> <'MATCHING'> <ws> file-pattern

   with-copy-clause = <'WITH'> <ws> copy-option (<opt-ws> <','> <opt-ws> copy-option)*
    copy-option = truncate | create-tables | create-table | create-no-tables | option-delimiter | option-null | batch-rows | batch-size | batch-concurrency | disable-triggers | drop-indexes
    option-delimiter = <'delimiter'> <opt-ws> <'='> <opt-ws> quoted-char
    option-null = <'null'> <opt-ws> <'='> <opt-ws> quoted-string

   with-dbf-clause = <'WITH'> <ws> dbf-option (<opt-ws> <','> <opt-ws> dbf-option)*
    dbf-option = truncate | create-tables | create-table | create-no-tables | batch-rows | batch-size | batch-concurrency | disable-triggers | drop-indexes | dbf-encoding
    create-table = <'create'> <ws> <'table'>
    dbf-encoding = <'encoding'> <ws> quoted-string
   having-fields = <'having'> <ws> <'fields'> <ws> column-list
    target-table-clause = <'target'> <ws> <'table'> <ws> table-ref (<ws> target-column-def-list)?
    target-columns-clause = <'target'> <ws> <'columns'> <ws> target-column-def-list
    target-column-def-list = <'('> <opt-ws> target-column-def (<opt-ws> <','> <opt-ws> target-column-def)* <opt-ws> <')'>
    target-column-def = column-name (<ws> target-type-name)? (<ws> <'using'> <ws> using-expr)?
    using-expr = quoted-string | using-dq-string | reader-fn | s-expr
    using-dq-string = <'\"'> #'[^\"]+' <'\"'>
    reader-fn = <'#'> s-expr
    s-expr = <'('> s-expr-inner* <')'>
    s-expr-inner = s-expr | #'[^()]+'
    table-ref = qualified-name | table-name

   source-uri  = <\"'\"> #\"[^']+\" <\"'\"> | #'jdbc:[^\\s]+' | #'[a-zA-Z][a-zA-Z0-9+.-]*://[^\\s;]+'
   pg-uri      = <\"'\"> #\"[^']+\" <\"'\"> | #'jdbc:p(?:ostgresql|gsql)://[^\\s]+' | #'p(?:ostgresql|gsql)://[^\\s;]+'
   column-list = <'('> <opt-ws> column-item (<opt-ws> <','> <opt-ws> column-item)* <opt-ws> <')'>
     column-item = column-name (<ws> date-format-spec)? (<ws> null-if-spec)*
     date-format-spec = <'['> <opt-ws> <'date'> <ws> <'format'> <ws> quoted-string <opt-ws> <']'>
     null-if-spec = <'['> <opt-ws> null-if-clause (<opt-ws> <','> <opt-ws> null-if-clause)* <opt-ws> <']'>
     null-if-clause = <'null'> <ws> <'if'> <ws> (quoted-string | dq-string | null-if-blanks-kw)
     null-if-blanks-kw = 'blanks'
    column-name = #'[a-zA-Z_][a-zA-Z0-9_-]*' | <'\"'> #'[^\"]+' <'\"'>
    qualified-name = schema-name <'.'> table-name
    schema-name = #'[a-zA-Z_][a-zA-Z0-9_-]*' | <'\"'> #'[^\"]+' <'\"'>
    table-name  = #'[a-zA-Z_][a-zA-Z0-9_-]*' | <'\"'> #'[^\"]+' <'\"'>

   with-csv-clause  = <'WITH'> <ws> csv-option (<opt-ws> <','> <opt-ws> csv-option)*
    csv-option  = skip-header | fields-enclosed | fields-terminated | fields-escaped | fields-not-enclosed | csv-encoding | create-tables | create-no-tables | nullif | keep-unquoted-blanks | trim-unquoted-blanks | truncate | disable-triggers | batch-rows | batch-size | batch-concurrency | csv-header | lines-terminated | csv-escape-mode | drop-indexes
    drop-indexes = <'drop'> <ws> <'indexes'>
   csv-encoding = <'encoding'> <ws> quoted-string
   skip-header = <'skip'> <ws> <'header'> <opt-ws> <'='> <opt-ws> integer
    fields-enclosed  = <'fields'> <ws> (<'optionally'> <ws>)? <'enclosed'> <ws> <'by'> <ws> quoted-char
    fields-terminated = <'fields'> <ws> <'terminated'> <ws> <'by'> <ws> quoted-char
    fields-escaped   = <'fields'> <ws> <'escaped'> <ws> <'by'> <ws> escaped-quote
    fields-not-enclosed = <'fields'> <ws> <'not'> <ws> <'enclosed'>
    escaped-quote = quoted-char | backslash-escape | double-escape
    backslash-escape = <'backslash-quote'>
    double-escape = <'double-quote'>
    nullif          = <'null'> <ws> <'if'> <ws> quoted-string
    keep-unquoted-blanks = <'keep'> <ws> <'unquoted'> <ws> <'blanks'>
    trim-unquoted-blanks = <'trim'> <ws> <'unquoted'> <ws> <'blanks'>
    truncate     = <'truncate'>
    disable-triggers = <'disable'> <ws> <'triggers'>
    batch-rows  = <'batch'> <ws> <'rows'> <opt-ws> <'='> <opt-ws> integer
    batch-size  = <'batch'> <ws> <'size'> <opt-ws> <'='> <opt-ws> integer-size
    batch-concurrency = <'batch'> <ws> <'concurrency'> <opt-ws> <'='> <opt-ws> integer
    csv-header = <'csv'> <ws> <'header'>
    lines-terminated = <'lines'> <ws> <'terminated'> <ws> <'by'> <ws> quoted-char
    csv-escape-mode = <'csv'> <ws> <'escape'> <ws> <'mode'> <ws> <'following'>
    quoted-char = <'\\''> #\"[^']+\" <'\\''>

   with-db-clause   = <'WITH'> <ws> db-option (<opt-ws> <','> <opt-ws> db-option)*
    db-option   = include-drop | create-tables | create-indexes | create-no-tables
                | workers | concurrency | batch-rows | batch-size | prefetch-rows
                | on-error-stop | on-error-resume | identifier-case
                | multiple-readers | single-reader | rows-per-range | chunk-size
                | include-no-drop | truncate | disable-triggers
                | data-only | schema-only | foreign-keys | reset-sequences | no-reset-sequences
                | max-parallel-create-index
                | drop-schema | reindex | preserve-index-names | uniquify-index-names
    include-drop    = <'include'> <ws> <'drop'>
    include-no-drop = <'include'> <ws> <'no'> <ws> <'drop'>
    create-tables   = <'create'> <ws> <'tables'>
    create-no-tables = <'create'> <ws> <'no'> <ws> <'tables'>
    create-indexes  = <'create'> <ws> <'indexes'>
    on-error-stop   = <'on'> <ws> <'error'> <ws> <'stop'>
    on-error-resume = <'on'> <ws> <'error'> <ws> <'resume'> <ws> <'next'>
    multiple-readers = <'multiple'> <ws> <'readers'> <ws> <'per'> <ws> <'thread'>
    single-reader    = <'single'> <ws> <'reader'>
    chunk-size       = <'chunk'> <ws> <'size'> <opt-ws> <'='> <opt-ws> data-size
    data-size        = integer (<opt-ws> data-unit)?
    data-unit        = 'MB' | 'GB' | 'KB' | 'B'
    identifier-case  = quote-ids | downcase-ids | snake-case-ids
    quote-ids    = <'quote'> <ws> <'identifiers'>
    downcase-ids = <'downcase'> <ws> <'identifiers'>
    snake-case-ids = <'snake_case'> <ws> <'identifiers'>
    truncate     = <'truncate'>
    disable-triggers = <'disable'> <ws> <'triggers'>
    data-only    = <'data'> <ws> <'only'>
    schema-only  = <'schema'> <ws> <'only'>
    foreign-keys = <'foreign'> <ws> <'keys'>
    reset-sequences    = <'reset'> <ws> <'sequences'>
    no-reset-sequences = <'reset'> <ws> <'no'> <ws> <'sequences'>
    drop-schema           = <'drop'> <ws> <'schema'>
    reindex               = <'reindex'> | (<'drop'> <ws> <'indexes'>)
    preserve-index-names  = <'preserve'> <ws> <'index'> <ws> <'names'>
    uniquify-index-names  = <'uniquify'> <ws> <'index'> <ws> <'names'>
    workers     = <'workers'> <opt-ws> <'='> <opt-ws> integer
    concurrency = <'concurrency'> <opt-ws> <'='> <opt-ws> integer
    max-parallel-create-index = <'max'> <ws> <'parallel'> <ws> <'create'> <ws> <'index'> <opt-ws> <'='> <opt-ws> integer
    batch-rows  = <'batch'> <ws> <'rows'> <opt-ws> <'='> <opt-ws> integer
    batch-size  = <'batch'> <ws> <'size'> <opt-ws> <'='> <opt-ws> integer-size
    prefetch-rows = <'prefetch'> <ws> <'rows'> <opt-ws> <'='> <opt-ws> integer
    rows-per-range = <'rows'> <ws> <'per'> <ws> <'range'> <opt-ws> <'='> <opt-ws> integer

   integer-size = integer <'MB'> | integer <'GB'> | integer <'KB'> | integer

    set-clause  = set-pg-params | set-mysql-params | set-generic
    <set-pg-params>    = <'SET'> <ws> <'PostgreSQL'> <ws> <'PARAMETERS'> <ws> set-pg-list
    <set-mysql-params> = <'SET'> <ws> <'MySQL'> <ws> <'PARAMETERS'> <ws> set-mysql-list
    <set-generic>      = <'SET'> <ws> set-option (<opt-ws> <','> <opt-ws> set-option)*
    <set-pg-list>        = set-option (<opt-ws> <','> <opt-ws> set-option)*
    <set-mysql-list>     = set-mysql-option (<opt-ws> <','> <opt-ws> set-mysql-option)*
    set-option         = pg-var    <ws> <'to'>  <ws> quoted-string
    set-mysql-option   = mysql-var <opt-ws> <'='> <opt-ws> quoted-string
    pg-var             = #'[a-zA-Z_][a-zA-Z0-9_]*'
    mysql-var          = #'[a-zA-Z_][a-zA-Z0-9_]*'

    cast-clause = <'CAST'> <ws> cast-rule (<opt-ws> <','> <opt-ws> cast-rule)*
    cast-rule   = type-cast | column-cast
    type-cast   = <'type'> <ws> cast-type-name
                  (<ws> when-or-unsigned)?
                  (<ws> with-extra)?
                  (<ws> <'to'> <ws> target-type-name)?
                  (<ws> cast-option)*
    column-cast = <'column'> <ws> column-ref
                  (<ws> <'to'> <ws> target-type-name)?
                  (<ws> cast-option)*
    cast-option = drop-not-null | drop-default | set-not-null | keep-not-null
                | drop-typemod | keep-typemod | drop-extra | using-fn
    drop-not-null = <'drop'> <ws> <'not'> <ws> <'null'>
    drop-default  = <'drop'> <ws> <'default'>
    drop-extra    = <'drop'> <ws> <'extra'>
    set-not-null  = <'set'> <ws> <'not'> <ws> <'null'>
    keep-not-null = <'keep'> <ws> <'not'> <ws> <'null'>
    drop-typemod  = <'drop'> <ws> <'typemod'>
    keep-typemod  = <'keep'> <ws> <'typemod'>
    using-fn      = <'using'> <ws> fn-name
    fn-name     = #'[a-zA-Z][a-zA-Z0-9_-]*'
    when-or-unsigned = <'when'> <ws> (when-expr | <'unsigned'> | <'default'> <ws> when-default-val) (<ws> when-not-null)?
    when-not-null = <'and'> <ws> <'not'> <ws> <'null'>
    when-expr   = <'('> <opt-ws> when-inner* <opt-ws> <')'>
    when-inner  = when-expr | #'[^()]+'
    when-default-val = cast-type-name | dq-string
    with-extra   = <'with'> <ws> <'extra'> <ws> <'on'> <ws> <'update'> <ws> <'current'> <ws> <'timestamp'>
    column-ref  = schema-name <'.'> table-name <'.'> column-name
                | table-name <'.'> column-name
                | column-name
    cast-type-name   = word-part (<ws> word-part)*
    word-part        = #'[a-zA-Z][a-zA-Z0-9]*'
    target-type-name = cast-type-name | dq-string
    dq-string  = <'\"'> #'[^\"]+' <'\"'>

    alter-schema-clause = <'ALTER'> <ws> <'SCHEMA'> <ws> alter-schema-name
                          <ws> <'RENAME'> <ws> <'TO'> <ws> alter-schema-name
    alter-schema-name = quoted-string | quoted-identifier

    before-load-do = <'BEFORE'> <ws> <'LOAD'> <ws> <'DO'> <ws> quoted-command-list
    after-load-do  = <'AFTER'> <ws> <'LOAD'> <ws> <'DO'> <ws> quoted-command-list
    quoted-command-list = quoted-command (<opt-ws> <','> <opt-ws> quoted-command)*
    quoted-command  = <'$$'> #'(?:[^$]+|\\$(?!\\$))*' <'$$'>

    alter-table-clause = <'ALTER'> <ws> <'TABLE'> <ws> <'NAMES'> <ws> <'MATCHING'> <ws>
                         table-name-pattern-list <ws> alter-table-action
    alter-table-action = alter-table-set-schema | alter-table-rename | alter-table-set-params | alter-table-set-tablespace
    alter-table-set-schema     = <'SET'> <ws> <'SCHEMA'> <ws> quoted-string
    alter-table-rename         = <'RENAME'> <ws> <'TO'> <ws> (quoted-string | name-string)
    alter-table-set-params     = <'SET'> <ws> <'('> #'[^)]*' <')'>
    alter-table-set-tablespace = <'SET'> <ws> <'TABLESPACE'> <ws> quoted-string
    table-name-pattern-list    = table-name-pattern (<opt-ws> <','> <opt-ws> table-name-pattern)*
    table-name-pattern = <'~'> #'/[^/]+/' | quoted-string
    name-string = #'[a-zA-Z_][a-zA-Z0-9_-]*'

    materialize-views = materialize-all-views | materialize-named-views
    materialize-all-views   = <'MATERIALIZE'> <ws> <'ALL'> <ws> <'VIEWS'>
    materialize-named-views = <'MATERIALIZE'> <ws> <'VIEWS'> <ws> matview-list
    matview-list = matview-def (<opt-ws> <','> <opt-ws> matview-def)*
    matview-def = matview-name (<ws> <'as'> <ws> quoted-command)?
    matview-name = #'[a-zA-Z_][a-zA-Z0-9_]*'

    names-keyword = (<'MATCHING'> | <'LIKE'>)
    including-only = <'INCLUDING'> <ws> <'ONLY'> <ws> <'TABLE'> <ws> <'NAMES'> <ws> names-keyword <ws> table-name-pattern-list (<ws> <'IN'> <ws> <'SCHEMA'> <ws> quoted-string)?
    excluding-only = <'EXCLUDING'> <ws> <'TABLE'> <ws> <'NAMES'> <ws> names-keyword <ws> table-name-pattern-list (<ws> <'IN'> <ws> <'SCHEMA'> <ws> quoted-string)?

    decoding-as-clause = <'DECODING'> <ws> <'TABLE'> <ws> <'NAMES'> <ws> <'MATCHING'> <ws>
                         decoding-pattern-list <ws> <'AS'> <ws> (quoted-string | bare-encoding)
    decoding-pattern-list = table-name-pattern (<opt-ws> <','> <opt-ws> table-name-pattern)*

    quoted-identifier = <'\\''> #\"[^']*\" <'\\''>

    quoted-string = <'\\''> #\"[^']*\" <'\\''>
    dq-string     = <'\"'> #'[^\"]*' <'\"'>
    integer     = #'[0-9]+'
    ws          = #'\\s+'
    opt-ws      = #'\\s*'")
