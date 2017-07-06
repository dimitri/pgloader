-- params: table-names
select n, n::regclass::oid
  from (values ~{('~a')~^,~}) as t(n);

