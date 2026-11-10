# CS165 Column Store

A column-oriented database server in C99, originally built for Harvard's
CS165 (Data Systems, Fall 2017) and revived in 2026.

- **Storage:** columns stored as contiguous `int` arrays, persisted to
  `database/` on shutdown and reloaded on startup.
- **Indexes:** B+ trees and sorted arrays, each clustered or unclustered.
  Clustering keeps the base data physically sorted on that column.
- **Scans:** batched selects over one column share a single multithreaded scan.
- **Joins:** nested-loop, and a partitioned hash join backed by an extensible
  hash table.
- **Updates:** `relational_insert`, `relational_update`, `relational_delete`,
  with every index on the table kept consistent.
- **Client/server:** a client reads queries on stdin and sends them to the
  server over a unix domain socket.

## Build and run

Needs a C99 compiler, `make` and Python 3 (for the tests). Works on macOS and
Linux.

```sh
make -C src            # optimized build: src/server and src/client
make -C src debug      # -O0 with AddressSanitizer + UBSan

src/server &           # run from the directory you want database/ in
src/client             # interactive prompt (or pipe a .dsl file in)
```

The server creates the socket `cs165_unix_socket` and the `database/`
directory in its working directory. `load("path.csv")` paths are resolved
from there too.

## Query language

```
create(db,"db1")
create(tbl,"tbl1",db1,2)
create(col,"col1",db1.tbl1)
create(col,"col2",db1.tbl1)
create(idx,db1.tbl1.col1,btree,clustered)      -- btree|sorted, clustered|unclustered
load("project_tests/data1.csv")                -- header row names the columns

relational_insert(db1.tbl1,5,10)
s1=select(db1.tbl1.col1,0,100)                 -- positions where 0 <= col1 < 100 (null = unbounded)
s2=select(s1,f1,null,50)                       -- filter an earlier selection by fetched values
f1=fetch(db1.tbl1.col2,s1)                     -- values of col2 at those positions
relational_update(db1.tbl1.col2,s1,7)
relational_delete(db1.tbl1,s1)

a=avg(f1)   s=sum(f1)   m=min(f1)   x=max(f1)   d=add(f1,f2)   e=sub(f1,f2)
t1,t2=join(f1,s1,f2,s2,hash)                   -- or nested-loop
print(f1,f2)

batch_queries()                                -- following selects run as one shared scan
...
batch_execute()
shutdown                                       -- persist everything and stop the server
```

Lines starting with `--` are comments. Queries have no `ORDER BY`, so result
order is up to the implementation. For example, a clustered table returns
rows in clustered key order.

## Tests

```sh
./run_tests.py         # 41 end-to-end tests in project_tests/
tests/fuzz.py          # randomized differential tests against a Python model
make -C src test       # same as ./run_tests.py
make -C src fuzz
```

- `run_tests.py` replays each `project_tests/testNN.dsl` against a fresh
  server and diffs the output with `testNN.exp`. The tests depend on each
  other (later tests read tables that earlier ones created and persisted), so
  they always run in order from an empty database. A result that differs only
  in row order is reported as `same rows, different order`.
- `tests/fuzz.py` builds tables with every index layout, then runs random
  inserts, updates, deletes, range selects and joins (with lots of duplicate
  keys), plus server restarts, and checks every answer against a model.
  Options: `--seeds`, `--rows`, `--ops`, `--paranoid` (verify every index after
  every write), `--trace FILE` (log every command sent).

CI runs both suites on Linux and macOS, once as a `-Werror` release build and
once under AddressSanitizer and UBSan.

## Layout

| Path | What it is |
|---|---|
| `src/server.c`, `src/client.c` | Socket server and client |
| `src/parse.c` | Query language parser |
| `src/db_operations.c` | Query execution: select, fetch, aggregates, joins, insert/update/delete |
| `src/db_index.c` | B+ tree and sorted-array indexes |
| `src/extensible_hash_table.c` | Hash table used by the hash join |
| `src/db_persistance.c` | Saving and loading the database |
| `src/db_manager.c`, `src/client_context.c` | Catalog (dbs, tables, columns) and per-client result handles |
| `project_tests/` | End-to-end tests and data |
| `experiments/` | 2017 scripts comparing btree and sorted indexes at 10^4 to 10^8 rows |
| `src/project0/` | The course's warm-up hash table assignment (not part of the server) |
| `Course_Instructions.md` | The original course README |
