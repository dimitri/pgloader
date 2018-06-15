drop database if exists db789;
create database db789;
use db789;

create table refrain (id char(1) primary key);
insert into refrain values ('a'), ('b'), ('c'), ('d');

create view proceed as select * from refrain where id > 'b';
