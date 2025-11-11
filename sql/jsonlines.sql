create extension pg_custom_copy_formats;

create table test (i int, f float, t text, jb jsonb);
create table test_in (i int, f float, t text, jb jsonb);

insert into test values (1, 0.1, '100', '{"a" : [1, 2, 3]}');

copy test to stdout with (format 'jsonlines');

copy test from stdin with (format 'jsonlines');
1    100.99    'hello'	  '{"a" : "foo"}'

select * from test order by 1;

