# How the Column Store Works

A refresher on how you built this database in Fall 2017: what each part
does, how a query flows through it, and where to find things. It describes
the code as it is on the `modernize` branch, which is your 2017 design plus
the 2026 fixes (see [What changed in 2026](#what-changed-in-2026)). For an
assessment of the work rather than a description, see `REVIEW.md`.

## Contents

1. [The project in one paragraph](#the-project-in-one-paragraph)
2. [How you built it, in order](#how-you-built-it-in-order)
3. [The big picture](#the-big-picture)
4. [Client, server and the wire protocol](#client-server-and-the-wire-protocol)
5. [The data model](#the-data-model)
6. [Life of a query](#life-of-a-query)
7. [Selects, fetches and aggregates](#selects-fetches-and-aggregates)
8. [Shared scans](#shared-scans)
9. [Indexes](#indexes)
10. [Inserts, loads and clustering](#inserts-loads-and-clustering)
11. [Joins](#joins)
12. [Persistence](#persistence)
13. [Limits and assumptions](#limits-and-assumptions)
14. [What changed in 2026](#what-changed-in-2026)
15. [Where to find things](#where-to-find-things)

---

## The project in one paragraph

It's an in-memory column store written in C99. A client reads queries from
stdin and sends them over a unix socket to a server, which parses each
query, runs it and sends results back. Every column is a plain `int` array.
Queries work on *positions*: a select returns the row numbers that match,
and a fetch turns positions into values. That idea, late materialization, is
the core of a column store. You built the parser, the execution engine, two
kinds of index (B+ tree and sorted array, each clustered or unclustered),
multithreaded shared scans, nested-loop and hash joins, and persistence to
disk. You used only the C standard library and pthreads.

## How you built it, in order

The course split the project into milestones, and the 41 tests in
`project_tests/` follow them. Your commit history shows the order you built
things:

| When (2017) | What you built | Tests |
|---|---|---|
| Aug 30 – Sep 14 | Project 0: a chained hash table warm-up (`src/project0/`) | — |
| Oct 6 – 16 | `create`, inserts, and saving/loading the database ("Woo! my loading things all work!!!!") | 1–2 |
| Oct 21 – 28 | `print`, then `select` and `fetch`: Milestone 1 | 3 |
| Oct 30 – Nov 9 | `sum`, `avg`, `min`, `max`, `add`, `sub` | 4–9 |
| Nov 11 – 13 | Batched queries and a single-threaded shared scan: Milestone 2 | 10–17 |
| Dec 2 – 9 | B+ tree (insert Dec 5, search Dec 6), sorted index (Dec 7–8), saving indexes (Dec 9): Milestone 3 | 18–29 |
| Dec 10 – 11 | Nested-loop join, extensible hash table and hash join, all in about two days: Milestone 4 | 30–35 |
| Dec 11 – 14 | Multithreaded shared scans (Dec 11), tuning, experiments, and the update/delete milestone (left as stubs) | 36–41 |
| Dec 29 | Post-class cleanup | — |

You committed on 41 separate days. December was the crunch: indexes and
joins together took about ten days.

---

## The big picture

```
 stdin ──► client ──unix socket──► server
                                     │
                                     ▼
                          parse_command()          src/parse.c
                          builds a DbOperator
                          (create and load run right here)
                                     │
                                     ▼
                          execute_DbOperator()     src/db_operations.c
                          one switch over the operator type
                                     │
              ┌──────────────┬───────┴───────┬──────────────┐
              ▼              ▼               ▼              ▼
          select /       aggregates       joins        insert / update
          fetch          (sum, avg,       (hash,       / delete
          (uses the      min, max,        nested       (keeps indexes
          indexes)       add, sub)        loop)        consistent)
              │              │               │              │
              └──────────────┴───────┬───────┴──────────────┘
                                     ▼
                     results stored under a handle name
                     in the ClientContext (e.g. "s1")
                                     │
                                     ▼
                          print_to_client()        src/server.c
                          sends rows in 1 KB chunks
```

The catalog (databases, tables, columns) lives in `db_manager.c` and
`client_context.c`. Indexes live in `db_index.c` and
`extensible_hash_table.c`. Saving and loading live in `db_persistance.c`.

---

## Client, server and the wire protocol

The course gave you the client and the basic socket loop. You extended the
server side.

**The message format** (`include/message.h`). Every exchange is a fixed
header followed by a payload:

```c
typedef struct message {
    message_status status;   // OK_DONE, OK_WAIT_FOR_RESPONSE, an error code...
    int length;              // number of payload bytes that follow
    char* payload;           // not sent; the bytes follow the header
} message;
```

The client sends one query per line as a header plus the query text. The
server always replies with at least one message. For a `print`, the server
sends the output in chunks of up to 1 KB (`DEFAULT_QUERY_BUFFER_SIZE`).
Every chunk except the last carries `OK_WAIT_FOR_RESPONSE`, so the client
keeps reading until it sees `OK_DONE`.

**The server loop** (`server.c: main`, `handle_client`). The server accepts
one client at a time and handles it until the client disconnects or sends
`shutdown`. For each message it:

1. reads the header, then exactly `length` payload bytes (`recv_all`),
2. calls `parse_command`,
3. runs the operator through `execute_DbOperator` unless parsing already
   finished the work,
4. sends either the status message or the printed results.

On startup, the server loads `database/` if it exists. On `shutdown`, it
writes everything back (see [Persistence](#persistence)).

---

## The data model

All the core structs are in `include/cs165_api.h`.

**Catalog.**

```
Db  (a linked list: db_head, current_db)
 └─ Table[]          name, col_count, table_size (rows), table_length (capacity)
     │               primary_index → the clustered column, if any
     └─ Column[]     name, int* data, size_ptr → &table->table_size
                     index (void*), index_type (NONE/BTREE/SORTED), clustered
```

- Every column in a table points its `size_ptr` at the table's row count, so
  all columns always agree on length.
- A table is created with a fixed number of column slots
  (`create(tbl,"t",db,4)`). Each `create(col,...)` fills the next empty slot.
- Column data starts at 4096 entries (`DEFAULT_COLUMN_SIZE`) and doubles as
  rows arrive (`next_table_idx`).

**Results.** Anything a query produces is a `Result`:

```c
typedef struct Result {
    size_t num_tuples;
    size_t capacity;
    void *payload;        // the values
    DataType data_type;   // INDEX (positions), INT, LONG or DOUBLE
    ...
} Result;
```

- A select produces `INDEX` results: an array of `size_t` row positions.
- A fetch produces `INT` values.
- `sum` produces a `LONG`, and `avg` produces a `DOUBLE`.

**Generalized columns.** Many operators accept either a base column or an
earlier result. `GeneralizedColumn` is a tagged union of `Column*` and
`Result*`. The parser decides which one by looking for a dot in the name:
`db1.tbl1.col1` is a column, and `s1` is a result.

**Client context.** Each connection has a `ClientContext` that maps handle
names (`s1`, `f1`, `a`) to results. It's a growable array searched
linearly (`add_result_column`, `get_result`). Reusing a handle name replaces
the old result. The context also holds the pending shared scan while a batch
is open.

---

## Life of a query

Take this example:

```
s1=select(db1.tbl1.col1,0,100)
f1=fetch(db1.tbl1.col2,s1)
a=avg(f1)
print(a)
```

**Parsing** (`parse.c: parse_command`).

1. `trim_comments` strips `--` comments. Blank lines return `OK_DONE`
   immediately.
2. If there's an `=`, the text before it is the handle (`s1`). A comma
   before the `=` gives a second handle, which joins use (`r1,r2=join(...)`).
3. A chain of `strncmp` checks picks the command, and a `parse_*` function
   builds a `DbOperator`:

   ```c
   typedef struct DbOperator {
       OperatorType type;              // SELECT, FETCH, SUM, HASH_JOIN, ...
       OperatorFields operator_fields; // a union with one struct per operator
       int client_fd;
       ClientContext* context;
   } DbOperator;
   ```

4. `create(...)` and `load(...)` don't build an operator. They run directly
   inside the parser and return `OK_DONE`.

**Name lookup.** `process_lookup` splits `db.table.column` with `sscanf` and
walks the catalog (`get_valid_db`, `get_table_from_db`, `get_column_from_db`).

**Bounds.** `set_bounds` turns `select(col, low, high)` into a half-open
range `low <= v < high`. `null` becomes `INT_MIN` or `INT_MAX`.

**Execution.** `execute_DbOperator` is one `switch` on the operator type.
Each case calls a `process_*` function that stores its output in the client
context under the handle. `print` is the exception: it returns a
`PrintOperator` that the server streams back.

---

## Selects, fetches and aggregates

All in `db_operations.c`.

**`select` on a base column** (`select_from_col`). If the column has no
index, it scans every value. The scan doesn't branch:

```c
positions[num_tuples] = idx;                          // always write
num_tuples += (data[idx] >= low & data[idx] < high);  // advance only on a match
```

That keeps the speed the same at any selectivity, because there's no branch
for the CPU to mispredict. If the column has an index, the select goes to
`get_range_sorted`, `find_values_clustered` or `find_values_unclustered`
instead.

**`select` on an earlier result** (`select_from_selection`).
`select(s1, f1, low, high)` filters positions `s1` by the values `f1`
fetched at those positions. That's how a two-predicate query works:

```
s1=select(db1.tbl1.col1,0,100)
f1=fetch(db1.tbl1.col2,s1)
s2=select(s1,f1,null,50)        -- rows with 0 <= col1 < 100 and col2 < 50
```

**`fetch`** (`process_fetch`) gathers `column[position]` for each position.

**Aggregates.**

- `sum` and `avg` (`process_sum_avg`, `calculate_sum`) accumulate into a
  `long`. `avg` divides as a `double` and prints with two decimal places.
- `min` and `max` with one argument (`get_range_value`, `single_col_bound`)
  return the value.
- `min` and `max` with positions (`get_index_and_range`,
  `col_bound_and_index`) also return the positions where the extreme occurs.
- `add` and `sub` (`process_col_op`, `combine_columns`) combine two
  equal-length value columns element by element.

---

## Shared scans

This is Milestone 2. A batch looks like this:

```
batch_queries()
s1=select(db1.tbl1.col1,10,20)
s2=select(db1.tbl1.col1,500,900)
...
batch_execute()
```

**Queuing.** `batch_queries` creates a `SHARED_SCAN` operator and parks it in
`context->shared_scan`. While it's there, `execute_DbOperator` doesn't run
selects. It appends them to the batch and replies `OK_DONE`.

**Running** (`process_shared_scans`). `batch_execute` runs the whole batch:

1. It divides the queries into groups of about 5
   (`MIN_QUERIES_PER_THREAD`), using up to 10 threads plus one more for any
   remainder. It creates a new set of threads for each batch, with no thread
   pool.
2. For each group, it records the lowest low bound and highest high bound of
   all its queries.
3. Each thread (`shared_col_select`) makes **one pass** over the column. For
   each value, it first checks the group's overall bounds and skips the value
   if it's outside all of them. Otherwise it tests the value against every
   query in the group and appends the position to each one that matches.
4. It joins the threads and stores each query's result under its handle.

The idea is to read the column from memory once and share it across many
queries. `REVIEW.md` has 2026 benchmarks showing that on modern hardware
this is limited by computation rather than memory, which makes it the most
interesting part to extend.

---

## Indexes

Created with `create(idx, db.tbl.col, btree|sorted, clustered|unclustered)`,
which `parse_create_index` handles. The tests create indexes on empty tables
and then load, so every index is built one insert at a time.

**Clustered vs unclustered.**

- A **clustered** index means the base table is kept physically sorted on
  that column. Every column in the table is reordered with it. A table can
  have one clustered index (`table->primary_index`). Matching rows are always
  one contiguous range of positions.
- An **unclustered** index is a separate structure that maps values to row
  positions. The base data stays in insert order.

### Sorted index (`SortedIndex`, `db_index.c`)

```c
typedef struct SortedIndex {
    int* keys;               // sorted values
    size_t* col_positions;   // row of each key (unclustered only)
    size_t num_items;
    size_t allocated_space;
    bool has_positions;
} SortedIndex;
```

- **Clustered:** `keys` *is* the column's data array, which is already
  sorted, and there are no positions. A range lookup returns the run
  `[low_bound, high_bound)` directly. It costs no extra memory.
- **Unclustered:** a sorted copy of the values with a parallel array of row
  positions. A range lookup `memcpy`s that slice of positions.
- **Lookup:** `get_sorted_idx` checks the two ends, then calls
  `search_sorted`, the page-blocked binary search. `get_range_sorted` finds
  both bounds and nudges them past duplicates.
- **Insert** (`insert_into_sorted`): find the slot, `memmove` keys and
  positions right by one, and add 1 to every stored position at or after the
  new row's position.

### B+ tree (`BPTNode`, `db_index.c`)

Each node is **exactly 4096 bytes**, one page. `MAX_DEGREE 340` and the
`padding` field make it come out exact.

```c
typedef struct BPTNode {
    BPTMeta bpt_meta;          // union: leaf data or child pointers
    size_t num_elements;
    int node_vals[MAX_KEYS];   // 339 keys
    bool is_leaf;
    char padding[3];
} BPTNode;

// leaf:      col_pos[339]  (row position of each key), next_leaf, prev_leaf
// internal:  children[340], is_root, level
```

- **Leaves** hold keys with their row positions. They form a doubly linked
  list, so a range query finds the first leaf and then walks sideways.
- **Search** (`search_for_leaf`): at each internal node, take the first child
  whose separator is greater than the value. With duplicates that span
  leaves, `first_leaf_for_range` then walks back along `prev_leaf` to the
  first leaf that could hold the low bound.
- **Range select:**
  - `find_values_unclustered` copies matching positions leaf by leaf and
    stops at the first leaf that ends past the range.
  - `find_values_clustered` only needs the first and last matching positions,
    because the rows are contiguous. It returns `low_pos..high_pos`.
- **Insert** (`btree_insert_value`):
  1. `find_leaf` descends from the root and records the path in a stack
     (`BPTNodeStack`). You didn't keep parent pointers.
  2. `add_to_leaf` inserts into the leaf. If the leaf is full, `split_leaf`
     merges the new key into a temporary buffer of 340 and splits it in half,
     producing a `SplitNode` (left, right and the separator key).
  3. `rebalanced_insert` pops the parent off the stack and inserts the
     separator (`insert_into_tree_body`). If the parent is also full,
     `split_body_node` splits it and the process repeats one level up. If the
     stack runs out, a new root is created.
  4. If inserting shifts base rows (a clustered table), every stored position
     at or after the insert point is incremented first.

---

## Inserts, loads and clustering

**`relational_insert(db.tbl, v1, v2, ...)`** and every row of a `load` go
through `insert_into_table` (`db_operations.c`).

**No clustered index.** Append the row at the end, then insert each value
into its column's index, if there is one, at that row position.

**Clustered index.** The new row has to go where the clustered column stays
sorted:

1. Find the row position. For a clustered B+ tree, the code binary-searches
   the sorted base data. For a clustered sorted index, it uses
   `get_sorted_idx`.
2. If the row lands mid-table, `memmove` every column down by one to make
   room.
3. Insert into every index, telling each one that positions shifted.

The shift in step 2 and the renumbering in step 3 make loading into a
clustered or unclustered-sorted table take time proportional to the square
of the row count. `REVIEW.md` has the numbers, and fixing it with a bulk
build is the top optimization candidate.

**`load("file.csv")`** (`parse_load`): the CSV header names the columns
(`db1.tbl1.col1,db1.tbl1.col2`). The table is looked up from the first name,
and then each line is split with `strsep`, converted with `atoi`, and
inserted.

---

## Joins

Milestone 4. A join takes two (values, positions) pairs and returns two
position lists, one per side, for each matching pair:

```
f1=fetch(db1.tbl1.col1,s1)
f2=fetch(db1.tbl2.col1,s2)
r1,r2=join(f1,s1,f2,s2,hash)     -- or nested-loop
```

`parse_join` reads the four results and the algorithm name.

**Nested-loop join** (`process_nested_loop_join`). This is a blocked nested
loop: it walks both inputs in page-sized chunks of 1024 values, comparing
every left value in a chunk with every right value in the paired chunk. The
blocking keeps both chunks in cache while they're compared.

**Hash join** (`process_hash_join`). This works in three steps:

1. **Partition** (`partition_data`): split both sides into 256 partitions by
   `value % 256`. Matching keys always land in the same partition, so each
   partition can be joined independently, with a small hash table that fits
   in cache.
2. **Build** (`process_partition`): for each partition, put the *smaller*
   side into a fresh extensible hash table, choosing separately in every
   partition.
3. **Probe:** look up every value from the larger side and emit a pair of
   positions for each match.

**Extensible hash table** (`extensible_hash_table.c`). This is your own
implementation of extensible hashing:

- **Directory:** an array of bucket pointers, `2^global_depth` long. A key's
  bucket is the low `global_depth` bits of its hash. The hash is an
  integer-mixing function (`ext_hash_func`, two multiply-xorshift rounds).
- **Buckets:** each holds up to 340 entries. That's the
  `12n + 16 = 4096` calculation in the header, a 4-byte key plus an 8-byte
  position per entry, so a bucket fills one page. Each bucket also has a
  `local_depth`.
- **Insert into a full bucket** (`ext_hash_table_put`):
  1. If the bucket's local depth equals the global depth, double the
     directory. The new half points at the same buckets as the old half.
  2. Split the bucket on the next hash bit, repoint the directory entries
     that have that bit set to the new bucket, and retry the insert.
- **Duplicates:** if a full bucket holds only one key (a heavy duplicate), it
  grows instead of splitting, since splitting can never separate identical
  keys. That's a 2026 fix. In 2017 this case called `exit(-1)`.
- **Lookup** (`ext_hash_func_get`): returns every position stored for a key.

---

## Persistence

`db_persistance.c`, plus `dump_tree` in `db_index.c`. Everything is written
on `shutdown` (`shutdown_server` → `sync_db`) and read back by `db_startup`
when the server starts. All files are raw binary `fwrite`s into `database/`:

| File | Contents |
|---|---|
| `database.bin` | One `StorageGroup` record per database (name, table count), followed by one per table (name, column count, rows, capacity) |
| `db.tbl.bin` | The table's `Column` structs, written raw |
| `db.tbl.col.bin` | The column's `int` data |
| `db.tbl.col.index.bin` | Unclustered sorted index: all keys, then all positions. B+ tree: nodes in breadth-first order. Clustered sorted: no file, because the sorted data *is* the index. |

**Fixing up pointers.** The raw `Column` structs contain pointers that
aren't valid after a restart. `load_table` reads them for the names and
index settings, then resets `data`, `size_ptr` and `table` to fresh values.

**Saving the B+ tree without pointers.** `bfs_traverse_tree` writes nodes in
breadth-first order. `load_btree` reads them back in the same order with a
queue: for each internal node it reads `num_elements + 1` children, which
come next in the file, and attaches them. Leaves come out left to right, so
the loader relinks `prev_leaf` and `next_leaf` as it goes. The file has no
IDs or offsets.

---

## Limits and assumptions

- Only `int` columns. Positions are `size_t`.
- The whole database lives in memory. Disk is only written at shutdown, so
  a crash loses everything since the last clean shutdown.
- One client at a time. The server's `accept` loop is serial.
- Names are at most 63 characters (`MAX_SIZE_NAME 64`), and a CSV line is at
  most 4096 bytes.
- No `ORDER BY`. Row order is whatever the storage gives: insert order, or
  key order for a clustered table.
- Tables have a fixed number of column slots set at `create(tbl,...)`.

---

## What changed in 2026

The design is unchanged. The `modernize` branch:

- **Implemented update and delete** (`process_update`, `process_delete`).
  Both edit the base columns, then rebuild every index on the table from the
  data (`rebuild_table_indexes`).
- **Fixed bugs in the B+ tree, hash join, hash table and socket code.**
  `REVIEW.md` has the full list and why the course tests missed them.
- **Replaced Vagrant** with a plain `Makefile` (`make -C src`,
  `make -C src debug` for AddressSanitizer and UBSan).
- **Added a test runner** (`run_tests.py`), a randomized differential tester
  (`tests/fuzz.py`) that checks every answer against a Python model, and CI
  on Linux and macOS.

---

## Where to find things

| To understand... | Read |
|---|---|
| How a line of input becomes an operator | `parse.c: parse_command`, then the matching `parse_*` |
| How operators run | `db_operations.c: execute_DbOperator` (at the bottom of the file) |
| Scans | `db_operations.c: select_from_col`, `select_from_selection` |
| Shared scans | `parse.c` (`batch_queries` / `batch_execute`), `db_operations.c: process_shared_scans`, `shared_col_select` |
| Inserts and clustering | `db_operations.c: insert_into_table` |
| Update and delete | `db_operations.c: process_update`, `process_delete`, `rebuild_table_indexes` |
| Sorted index | `db_index.c`: `get_sorted_idx`, `search_sorted`, `get_range_sorted`, `insert_into_sorted` |
| B+ tree | `db_index.c`: `btree_insert_value`, `find_leaf`, `split_leaf`, `rebalanced_insert`, `find_values_*` |
| Joins | `db_operations.c`: `process_hash_join`, `partition_data`, `process_partition`, `process_nested_loop_join` |
| Hash table | `extensible_hash_table.c: ext_hash_table_put` |
| Saving and loading | `db_persistance.c`: `sync_db`, `dump_db_table`, `db_startup`, `load_table`, `load_btree` |
| Results and handles | `client_context.c`: `add_result_column`, `get_result` |
| The socket protocol | `include/message.h`, `server.c: handle_client`, `print_to_client`, `client.c: main` |
| All the structs | `include/cs165_api.h`, `include/db_index.h`, `extensible_hash_table.h` |
