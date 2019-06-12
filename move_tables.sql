DO $$DECLARE row record;
BEGIN
    FOR row IN SELECT tablename FROM pg_tables WHERE schemaname = 'temp'
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS blockstack_core.' || quote_ident(row.tablename) || ' CASCADE;';
        EXECUTE 'ALTER TABLE temp.' || quote_ident(row.tablename) || ' SET SCHEMA blockstack_core;';
    END LOOP;
END$$;
