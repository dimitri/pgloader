CREATE SCHEMA pgsql_source;

CREATE TABLE pgsql_source.artist (
    id   serial primary key,
    name text not null
);

CREATE TABLE pgsql_source.album (
    id        serial primary key,
    title     text not null,
    artist_id integer not null references pgsql_source.artist(id)
);

INSERT INTO pgsql_source.artist (name) VALUES ('Artist One'), ('Artist Two'), ('Artist Three');

INSERT INTO pgsql_source.album (title, artist_id) VALUES
    ('Album A', 1),
    ('Album B', 1),
    ('Album C', 2);
