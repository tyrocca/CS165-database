# Code Review: CS165 Column Store (Fall 2017)

A review of the original project as it was at commit `ef3c454` (December
2017), written in November 2026 after reviving it on the `modernize` branch.
It covers the code you wrote, not the course skeleton, and it's based on
running the code as well as reading it. The bugs listed here were all
reproduced, and the performance numbers were measured.

**Short version:** the design is good, and many of the low-level decisions
show real understanding of how hardware behaves. Where it fell short was
verification. The code was tested only against the course tests, which use
small, mostly sorted data with few duplicates, and the bugs were in exactly
the cases those tests never reached.

---

## What you did well

### 1. You thought about the hardware, not just the algorithm
This is the strongest part of the project. You didn't stop at the standard
algorithm in several places:

- **The sorted index binary search works in page-sized blocks**
  (`search_sorted` / `nearest_value` in `db_index.c`). It snaps the midpoint
  to a 1024-entry block and searches inside that block first, so each step of
  the search touches as few separate pages as possible. Most students write
  the textbook binary search and stop there.
- **B+ tree nodes and hash buckets are sized to fit a page.** The comment in
  `extensible_hash_table.h` works out `12n + 16 = 4096` to get 340 entries
  per bucket, and `MAX_DEGREE 340` follows the same reasoning. You picked
  these numbers on purpose.
- **The scan avoids branches.** `select_from_col` uses
  `num_tuples += (v >= lo & v < hi)` and always writes the position, instead
  of an `if`. You kept the branching version in a comment with a note about
  when each is better ("better for middle selectivity" vs "higher
  selectivity"). That's right: at around 50% selectivity the CPU can't
  predict the branch, and the branch-free version wins.
- **The hash join partitions the data first** (`partition_data`, 256
  partitions), then builds a small hash table per partition, always on the
  smaller side. That's the radix-partitioned join design used in real
  systems, so each per-partition table stays small enough to sit in cache.

### 2. The shared scan is a real shared scan
`batch_queries` / `batch_execute` groups pending selects, divides them among
threads, and has each thread make one pass over the column, testing every
value against its group of queries. It also computes the overall min/max of
the batch's ranges, so values outside every query are skipped with one check.
That's the core idea of shared scans: pay for reading memory once and reuse
it for many queries. The work division (`MIN_QUERIES_PER_THREAD`, plus an
extra thread for the remainder) is sensible.

### 3. You ran experiments to compare designs
`experiments/` has load and query scripts for btree vs sorted indexes from
10^4 to 10^8 rows, and comparisons of different scan strategies. Comparing
designs with measurements instead of assuming is the right habit, and at
10^8 rows the scale was ambitious.

### 4. The code is readable and documented
- Almost every function has a doc comment with `@brief`/`@param` (130 or so
  across `src/`), and the comments explain *why* as well as *what*, e.g.
  "check backwards to see if that leaf could contain this value (by checking
  the max ie: [0, 3, 3] <--> [3, 4, 5])".
- Names are consistent (`process_*` for execution, `parse_*` for parsing,
  `find_values_*` for index lookups), and files map cleanly to subsystems.
- 9 years later it still compiles on a different CPU architecture (Apple
  Silicon) and compiler with only minor warnings. Disciplined C99 holds up
  well.

### 5. The design held up
Every fix in the revival fit into your existing structure. Update and delete
slotted into the existing parse → execute pipeline, the B-tree fixes were
local to a few functions, and nothing had to be redesigned. That says the
module boundaries (parser, operators, indexes, persistence, client context)
were drawn in the right places.

### 6. Persistence works
Shutdown writes every column, the catalog, and every index (B-tree nodes
included) to `database/`, and startup restores them. Across hundreds of fuzz
runs with random restarts, saving and loading was never the source of a bug.

---

## What could have used work

### 1. Testing beyond the course tests (the main lesson)
Every serious bug was in a case the 41 course tests didn't exercise, and
`src/run_tests.py` never compared outputs unless you passed `--large`. So
for small runs, "tests pass" only meant "nothing crashed." The bugs a
randomized test found in a few minutes:

| Bug | Why the course tests missed it |
|---|---|
| B-tree range select returned nothing when the range started inside the first leaf (the early-exit checks were reversed) | Test ranges were wide or hit later leaves |
| Clustered B-tree insert put rows in the wrong place (a reversed `>` in `btree_find_insert_position`) | `data4.csv` was already sorted, so every insert was an append |
| B-tree internal nodes placed new children by comparing keys, which goes wrong when many duplicates create equal separators | Too few duplicates to fill several leaves with one key |
| Shifted B-tree inserts renumbered positions with a rule that broke on duplicates | Same |
| Hash join only probed with the first `small_size` rows of the larger side | Partitions were nearly balanced on test data |
| Extensible hash table called `exit(-1)` (killing the server) when one key had more than 340 duplicates | No join key repeated that often |
| `print` sent every 1 KB chunk with length 1024, and `recv` short reads weren't handled | Small outputs fit in one chunk |

All of these would have been caught by a small randomized test that runs
random operations against both the database and a simple model (a Python
list of rows), then compares the answers. That's now `tests/fuzz.py`. It's
the single most valuable habit to take from this project: **when you build a
complicated data structure, test it against a simple one that's obviously
correct.**

Two smaller things in the same vein:
- The `assert`s check that inputs are well formed (a leaf is full, an index
  is in range), but nothing checked results. A debug-only "is the leaf chain
  sorted, does every position point at a matching row" check after each
  insert would have caught the B-tree bugs on the first unsorted load.
- Common_Errors.txt (from the course staff) warned about exactly the socket
  bug: "Be careful to send and receive exactly the same number of bytes."
  Advice like that is a good checklist for a review pass.

### 2. Unfinished features left stubbed but silently succeeding
`relational_update` and `relational_delete` parsed as no-ops and returned
`OK`, so test38 and test40 "passed" because they print nothing. A stub that
reports success hides that work is missing. Returning an error such as
"not implemented" would have made the gap visible. The `delete_from_table`
helper also had its `memmove` source and destination reversed, so it would
have shifted rows the wrong way if it had been wired up.

### 3. Write paths take time proportional to n²
Measured on the revived code (each row loaded through `insert_into_table`):

| Load | 25k rows | 50k rows |
|---|---|---|
| No index | 10 ms | 11 ms |
| Sorted, clustered | 24 ms | 75 ms |
| Sorted, unclustered | 33 ms | 111 ms |
| B-tree, clustered | 168 ms | 631 ms |

The unindexed load handles 10M rows in 0.45 s. With a clustered index,
each unsorted row is inserted mid-table: every column is shifted with
`memmove`, and every other index's stored positions are renumbered. Doubling
the rows roughly quadruples the time. `load` is a bulk operation, and the
standard approach is to append everything, sort once by the clustered
column, then build each index in a single pass (bottom-up for the B-tree).

Storing row positions inside secondary indexes is also what makes every
insert so expensive. Column stores usually avoid renumbering by marking
deleted rows and sending inserts to a small separate write store that gets
merged in periodically.

### 4. Defensive C
- `sscanf("%[^.,\n]...")` in `process_lookup` has no width limit and writes
  into 64-byte buffers, so a long name overflows the stack. `%63[^.,\n]`
  fixes it.
- Errors were often handled by calling `exit()`, in the hash table and the
  socket code. In a server, one bad query or disconnected client shouldn't
  take down the database. Return an error to the caller instead.
- Global mutable state: `recurse_limit` in the hash table and `leafcount` /
  `current_level` in `db_index.c`. These are unsafe if the code is ever
  called from more than one thread, and the shared scan already uses
  threads.
- The client's `while (fgets(...), !feof(stdin))` drops a final line that
  has no trailing newline.

### 5. Cleanup
- Commented-out code and old versions of functions (there are two
  `shared_col_select` drafts in comments above the real one) make files
  harder to read. Git keeps the history, so it's safe to delete them.
- About 60 `TODO`s remain, some on correctness edge cases ("TODO: could
  break if empty", "TODO: edge case - table columns are not in order").
  Those deserved a test or an issue rather than a comment.
- `cs165_api.h` mixes the catalog structs, every operator, and the function
  declarations, and its first line is "BREAK APART THIS API (TODO MYSELF)".
  You were right.

### 6. Small API sharp edges
- Positions are `size_t` (8 bytes) everywhere. Row positions in a table of
  `int` columns fit in 4 bytes, which would halve the memory traffic of
  every select, fetch and join.
- The default print uses `%lf`, but the expected output was `%.2lf`, so
  test04 and test09 failed on formatting alone.

---

## If you did it again

1. Write a model-based fuzzer on day one, and run it under
   AddressSanitizer.
2. Make unimplemented commands return errors, not `OK`.
3. Bulk-load with sort then build. Treat single-row inserts as the uncommon
   case they are in a column store.
4. Keep the hardware-aware instincts (page-sized nodes, branch-free scans,
   partitioned joins, shared scans). Those were the best parts of the
   project, and they're the hardest to teach.

## Overall

For a one-semester project, this is a strong systems implementation with good
instincts about performance and a clean structure. Its weak spot was
verification, not design: the hard parts were mostly built well, and the
bugs were concentrated in edge cases (duplicates, unsorted input, partial
socket reads) that a randomized test would have caught quickly.
