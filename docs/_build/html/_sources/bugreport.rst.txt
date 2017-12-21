Reporting Bugs
==============

pgloader is a software and as such contains bugs. Most bugs are easy to
solve and taken care of in a short delay. For this to be possible though,
bug reports need to follow those recommandations:

  - include pgloader version,
  - include problematic input and output,
  - include a description of the output you expected,
  - explain the difference between the ouput you have and the one you expected,
  - include a self-reproducing test-case

Test Cases to Reproduce Bugs
----------------------------

Use the *inline* source type to help reproduce a bug, as in the pgloader tests::

  LOAD CSV
     FROM INLINE
     INTO postgresql://dim@localhost/pgloader?public."HS"

     WITH truncate,
          fields terminated by '\t',
          fields not enclosed,
          fields escaped by backslash-quote,
          quote identifiers

      SET work_mem to '128MB',
          standard_conforming_strings to 'on',
          application_name to 'my app name'

   BEFORE LOAD DO
    $$ create extension if not exists hstore; $$,
    $$ drop table if exists "HS"; $$,
    $$ CREATE TABLE "HS"
       (
          id     serial primary key,
          kv     hstore
       )
    $$;
  
  
  1	email=>foo@example.com,a=>b
  2	test=>value
  3	a=>b,c=>"quoted hstore value",d=>other
  4	baddata


