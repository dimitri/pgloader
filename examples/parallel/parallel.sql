CREATE TABLE parallel (
 a integer primary key,
 b text
);

-- create the .data file
insert into parallel 
       select * from (select a, a::text
                        from generate_series(0, 1000 * 1000 * 1000) as t(a)) x;

\copy parallel to 'parallel/parallel.data' with delimiter ';' csv
