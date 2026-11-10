# Code Review: CS165 Column Store (Fall 2017)

A review of the original project as it was at commit `ef3c454` (December
2017), written in November 2026 after reviving it on the `modernize` branch.
It's based on running and benchmarking the code as well as reading it. The
bugs listed here were all reproduced, and the performance numbers were
measured.

**Scope.** The course provided a skeleton: the client, the socket loop in
`server.c`, a stub parser, and the API header. You built everything else
alone, using only the C standard library and pthreads. That's about 5,000
lines of C written from scratch between August and December 2017:

| Component | Lines | What it is |
|---|---|---|
| `db_index.c` | 1,309 | B+ tree and sorted index, each clustered or unclustered |
| `db_operations.c` | 1,726 | Selects, fetches, aggregates, shared scans, hash and nested-loop joins |
| `db_persistance.c` | 596 | Saves and restores the catalog, columns and every index |
| `extensible_hash_table.c` | 345 | Extensible hashing (Fagin et al., 1979) used by the join |
| `parse.c` / `server.c` | ~1,200 added | The query language, batching, and result printing |

No third-party sorting, hashing, tree or threading library is involved. The
B+ tree with split propagation, the extensible hash table with directory
doubling, and the thread work division are all hand-built.

**Short version:** the design is good, and many of the low-level decisions
show real understanding of how hardware behaves. A few details are genuinely
clever. Where it fell short was verification. The code was tested only
against the course tests, which use small, mostly sorted data with few
duplicates, and the bugs were in exactly the cases those tests never reached.

---

## What you did well

### 1. You thought about the hardware, not just the algorithm
This is the strongest part of the project. In several places you didn't stop
at the textbook algorithm:

- **The scan avoids branches.** `select_from_col` uses
  `num_tuples += (v >= lo & v < hi)` and always writes the position, instead
  of an `if`. You kept the branching version in a comment with a note about
  when each is better. Re-measured in 2026, the reasoning holds up exactly
  (see [Interesting finds](#interesting-finds-from-re-benchmarking)): the
  branch-free scan is flat at about 2.4 ms per 10M rows, while the branching
  one jumps to 26.6 ms at 50% selectivity.
- **B+ tree nodes and hash buckets are sized to fit a page.** The comment in
  `extensible_hash_table.h` works out `12n + 16 = 4096` to get 340 entries
  per bucket, and `MAX_DEGREE 340` follows the same reasoning. You picked
  these numbers on purpose.
- **The hash join partitions the data first** (`partition_data`, 256
  partitions), then builds a small hash table per partition. That's the
  radix-partitioned join design used in real systems, so each per-partition
  table stays small enough to sit in cache.
- **The sorted index search was designed around pages.** `search_sorted`
  tries to keep each step inside one 1024-entry block. The goal was right,
  but the structure turned out slower than a plain binary search (see
  [Interesting finds](#interesting-finds-from-re-benchmarking)).

### 2. The shared scan is a real shared scan
`batch_queries` / `batch_execute` groups pending selects, divides them among
threads, and has each thread make one pass over the column, testing every
value against its group of queries. That's the core idea of shared scans:
pay for reading memory once and reuse it for many queries. The work division
(`MIN_QUERIES_PER_THREAD`, plus an extra thread for the remainder) is
sensible.

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

## Clever details

These are small decisions that show you were thinking a step further than
the assignment required.

### The B-tree is saved without saving any pointers
`dump_tree` writes nodes in breadth-first order, and `load_btree` reads them
back in the same order. Because a node's children always appear in the file
right after the nodes queued before them, the loader can reattach every
child pointer from file position alone. Leaves come out left to right, so
the loader also rebuilds the doubly linked leaf chain as it goes. The file
needs no node IDs, offsets or pointer fixups, and the structure on disk is
just the nodes. It's the simplest correct way to persist a pointer-based
tree, and it's the reason persistence never broke under fuzzing.

### The join picks its build side per partition
`process_partition` compares the left and right sizes *within each
partition* and builds the hash table on whichever is smaller there. Most
simple implementations choose once for the whole join. Choosing per
partition adapts to skew: if one key range is heavy on the left and another
is heavy on the right, each partition still builds on its small side.

### The shared scan rejects most values with one test
Each thread computes the overall low and high bound of the queries it owns,
and skips any value outside that envelope with a single comparison before
testing individual queries. That's a cheap form of indexing the *queries*
rather than the data, which is the central idea of Crescando (Unterbrunner
et al., VLDB 2009). You got there on your own with two integers.

### Extensible hashing for the join's build side
Most join implementations size a fixed hash table up front, because the
build side's size is known. You built extensible hashing instead, so buckets
split and the directory doubles as rows arrive. That was more work than a
fixed table, and it means the same structure works when the size isn't known
ahead of time. It also had the most interesting failure mode (see
[What could have used work](#1-testing-beyond-the-course-tests-the-main-lesson)):
many copies of one key can't be separated by splitting.

### The code records its own experiments
Comments like "METHOD 2 - always inc - this is slower" and "better for
middle selectivity" next to commented-out alternatives turn the source into a
lab notebook. Normally the review says to delete dead code, and it does say
that below, but these specific comments preserved results that were worth
re-checking nine years later.

---

## Interesting finds from re-benchmarking

All numbers below are from microbenchmarks that extract the 2017 code
unchanged and compare it against an alternative on the same data. They were
measured on an Apple M5 Max (128 GB) with Apple clang 21 at `-O2`, taking
the best of several runs. Different hardware will move the numbers, and the
shared scan result in particular may well have come out differently on a
2017 laptop.

### The branch-free scan: your note was right
Selecting from 10M random integers in [0, 1000):

| Selectivity | With `if` | Branch-free (yours) |
|---|---|---|
| 1% | 3.0 ms | 2.3 ms |
| 10% | 7.7 ms | 2.3 ms |
| 50% | 26.6 ms | 2.4 ms |
| 90% | 8.8 ms | 2.8 ms |
| 100% | 2.5 ms | 2.7 ms |

The branching version costs 11 times more at 50%, where the CPU can't
predict the branch, and is only competitive at the extremes. Your comment
predicted exactly this shape.

### The page-blocked binary search is slower than a plain one
`search_sorted` vs a textbook lower-bound binary search, 1M random lookups,
both returning identical positions:

| Rows | Plain binary search | `search_sorted` |
|---|---|---|
| 100k | 33 ns, 17 comparisons | 89 ns, 59 comparisons |
| 10M | 88 ns, 23 comparisons | 275 ns, 128 comparisons |
| 100M | 230 ns, 27 comparisons | 567 ns, 163 comparisons |

At each step it runs a full binary search inside one 1024-entry block, and
if the value isn't there, it still only halves the remaining range. So it
pays for both a full block search and the outer binary search. The idea
behind it (touch as few pages as possible per lookup) is correct. The
structure that achieves it is a small array of each block's first key,
searched first while it stays in cache, followed by a single search inside
one block. That's essentially a CSS-tree (Rao and Ross, VLDB 1999).

### The shared scan doesn't beat separate scans on modern hardware
Ten queries over 10M rows, each writing its matching positions:

| Selectivity per query | Shared, with `if` (yours) | Shared, branch-free | 10 separate branch-free scans |
|---|---|---|---|
| 1% | 55.9 ms | 50.4 ms | 23.9 ms |
| 10% | 67.2 ms | 48.1 ms | 24.0 ms |
| 50% | 104.6 ms | 49.4 ms | 24.4 ms |

Two surprises. First, the 2017 comment says the branch-free version was
slower inside the shared scan, but here it's faster at every selectivity.
Second, ten independent scans beat one shared scan by about 2 times. When
only counting matches, the two tie: 13 ms each at 10M rows, and 502 vs
508 ms at 400M rows (1.5 GB, well beyond cache).

The reason is that the shared scan is limited by computation, not memory.
Ten comparisons per value keep the core busy at about 3 GB/s of column data.
Ten separate scans read ten times the data but sustain about 30 GB/s, which
this machine's memory easily supplies. Writing ten output streams at once
also costs the shared version extra. Shared scans pay off when memory
bandwidth is the bottleneck: many threads, many more queries, or a slower
memory system. On 2017 hardware the trade-off may have favored you, but the
experiments don't record it, so this is worth measuring rather than assuming.

---

## Is any of it novel?

Not in the research sense, and that's not a criticism. Each major technique
has a known lineage: branch-free selection (Ross, 2002), radix-partitioned
hash joins (Manegold, Boncz and Kersten, 2000), shared scans (QPipe, 2005;
Crescando, 2009), extensible hashing (Fagin et al., 1979), and page-sized B+
tree nodes. CS165 teaches exactly this material. What stands out is that you
implemented all of it from scratch in one semester, chose the
hardware-conscious variant at almost every decision point, and added your own
refinements (pointer-free tree persistence, per-partition build sides, the
query envelope) where the assignment didn't ask for them.

---

## Worth extending

In order of how much you'd learn:

1. **Shared scans.** This is the most interesting direction, especially
   given the benchmark above. Sort each batch's predicates by their low
   bound and sweep, so the cost per row depends on how many queries overlap
   that value rather than on how many queries there are. Then split the
   *column* across threads instead of splitting the queries, so each thread
   reads only its part of memory. Finally, measure where shared beats
   separate as thread and query counts grow.
2. **The partitioned hash join.** Partitions currently grow with `realloc`.
   Counting rows per partition first, then writing each row straight into
   place, makes it a true radix join. Adding threads, or partitioning in two
   passes so each partition fits in L1, would make it a proper benchmark
   subject.
3. **A CSS-tree for the sorted index.** It replaces `search_sorted` and does
   what you originally intended. Compare it with your B+ tree using your
   existing `experiments/` scripts.
4. **Bulk load.** Append, sort once by the clustered column, then build each
   index in a single pass, bottom-up for the B+ tree. This fixes the
   quadratic loads described below.
5. **The experiments setup.** Turning `experiments/` into an automated
   before/after benchmark would make every one of the items above
   measurable.

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
  harder to read. Git keeps the history, so it's safe to delete them. The
  exception is a short note recording a measured result, which is worth
  keeping as prose.
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
4. Benchmark every "faster" design against the plain version before keeping
   it. The branch-free scan passed that test; the blocked binary search
   wouldn't have.
5. Keep the hardware-aware instincts (page-sized nodes, branch-free scans,
   partitioned joins, shared scans). Those were the best parts of the
   project, and they're the hardest to teach.

## Overall

For a solo, one-semester project built without libraries, this is a strong
systems implementation: about 5,000 lines covering two index types in four
configurations, two join algorithms, multithreaded shared scans, and full
persistence. It shows good instincts about performance, a clean structure,
and a few genuinely clever touches. Its weak spot was verification, not
design: the hard parts were mostly built well, and the bugs were
concentrated in edge cases (duplicates, unsorted input, partial socket
reads) that a randomized test would have caught quickly. The one design that
measurement now shows didn't pay off, the blocked binary search, failed for
the same underlying reason: it was never compared against the simple
version.
