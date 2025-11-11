# pg_custom_copy_formats

An experimental implementation of custom COPY formats for PostgreSQL.

Avaialble formats are

- [JSON Lines](https://jsonlines.org/).

## Background

PostgreSQL's COPY command traditionally supports three built-in formats: text, CSV, and binary. In 2024, a proposal was made to make the COPY format system extensible, allowing third-party modules to implement custom formats. This proposal is currently under discussion in the PostgreSQL community ([CommitFest #4681](https://commitfest.postgresql.org/patch/4681/)).

The custom COPY formats can be built with the patched PostgreSQL available [here](https://github.com/MasahikoSawada/postgresql/tree/dev_custom-copy-format).

⚠️ **Important Note**: This extension requires patches from the above proposal, which have not yet been committed to PostgreSQL. As such, this implementation is experimental and intended to serve as a reference implementation for the proposed COPY format extension API.

# Build and Installation

`pg_custom_copy_formats` can be built in the same way as other extensions:

```bash
$ cd pg_custom_copy_formats
$ make USE_PGXS=1
$ make install USE_PGXS=1
```

```sql
$ echo "shared_preload_libraries = 'pg_custom_copy_formats' >> ${PGDATA}/postgresql.conf"
```

# JSON Lines

You can use `jsonlines` format in both COPY TO and COPY FROM commands.

## `COPY TO` with JSON Lines format

```sql
=# CREATE TABLE jl (id int, a text, b jsonb);
CREATE TABLE
=# INSERT INTO jl VALUES (1, 'hello', '{"test" : [1, true, {"num" : 42}]}'::jsonb), (2, 'hello world', 'true'), (999, null, '{"a" : 1}');
INSERT 0 3
=# TABLE jl;
 id  |      a      |                b
-----+-------------+----------------------------------
   1 | hello       | {"test": [1, true, {"num": 42}]}
   2 | hello world | true
 999 |             | {"a": 1}
(3 rows)

=# COPY jl TO '/tmp/test.jsonl' WITH (format 'jsonlines');
COPY 3

=# \! cat /tmp/test.jsonl
{"id":1,"a":"hello","b":{"test": [1, true, {"num": 42}]}}
{"id":2,"a":"hello world","b":true}
{"id":999,"a":null,"b":{"a": 1}}
```

## `COPY FROM` with JSON Lines format

```sql
=# \! cat /tmp/test.jsonl
{"a":"hello","id":1,"b":{"test": [1, true, {"num": 42}]}}
{"id":2,"b":true,"a":"hello world"}
{"id":999,"a":null,"b":{"a": 1}}
=# CREATE TABLE jl_load (id int, a text, b jsonb);
CREATE TABLE
=# COPY jl_load FROM '/tmp/test.jsonl' WITH (format 'jsonlines');
COPY 3
=# TABLE jl_load;
 id  |      a      |                b
-----+-------------+----------------------------------
   1 | hello       | {"test": [1, true, {"num": 42}]}
   2 | hello world | true
 999 |             | {"a": 1}
(3 rows)
```

## Compression supports

`'jsonlines'` format supports data compression using zlib. For `COPY TO` command, you can specify `compression` and `compression_detail` options:

```sql
#= COPY jl TO '/tmp/jl.jsonl.gz' WITH (format 'jsonlines', compression 'gzip', compression_detail 'level=2');
COPY 2
=# COPY jl FROM '/tmp/jl.jsonl.gz' WITH (format 'jsonlines');
COPY 2
```

The `COPY FROM` with `'jsonlines'` format automatically detects the compressed file by its extension.
