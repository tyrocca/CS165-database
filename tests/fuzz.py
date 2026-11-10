#!/usr/bin/env python3
"""
Randomized differential test: run random loads, inserts, selects, updates and
deletes against the server and check every answer against a Python model.

Each seed builds a fresh database with one table per index layout (no index,
btree/sorted x clustered/unclustered), so the index maintenance paths for
update and delete all get exercised. Rows are compared as multisets since the
query language has no ORDER BY.

Usage:
    tests/fuzz.py                 # 20 seeds
    tests/fuzz.py --seeds 200 --rows 5000
"""
import argparse
import random
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
NCOLS = 3
SERVER_LOG = subprocess.DEVNULL

LAYOUTS = [
    ("plain", []),
    ("bt_cl", [("col1", "btree", "clustered")]),
    ("bt_un", [("col2", "btree", "unclustered")]),
    ("so_cl", [("col1", "sorted", "clustered")]),
    ("so_un", [("col2", "sorted", "unclustered")]),
    ("mixed", [("col1", "sorted", "clustered"), ("col2", "btree", "unclustered"),
               ("col3", "sorted", "unclustered")]),
    ("mixed2", [("col1", "btree", "clustered"), ("col2", "sorted", "unclustered"),
                ("col3", "btree", "unclustered")]),
]


class Server:
    def __init__(self, workdir, trace=None):
        self.workdir = workdir
        self.proc = None
        self.trace = trace

    def start(self):
        sock = self.workdir / "cs165_unix_socket"
        if sock.exists():
            sock.unlink()
        self.proc = subprocess.Popen([str(SRC / "server")], cwd=self.workdir,
                                     stdout=SERVER_LOG, stderr=SERVER_LOG)
        deadline = time.time() + 5
        while not sock.exists():
            if self.proc.poll() is not None or time.time() > deadline:
                sys.exit("server failed to start")
            time.sleep(0.02)

    def run(self, script):
        if self.proc is not None and self.proc.poll() is not None:
            raise RuntimeError("server exited unexpectedly (code %s)" % self.proc.returncode)
        if self.proc is None:
            self.start()
        if self.trace:
            self.trace.write(script)
        proc = subprocess.run([str(SRC / "client")], cwd=self.workdir, input=script,
                              capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            raise RuntimeError("client exited with %d on:\n%s\nstderr: %s"
                               % (proc.returncode, script, proc.stderr))
        out = proc.stdout
        if "shutdown" in script:
            self.proc.wait(timeout=10)
            self.proc = None
        return out

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()


def rows_where(rows, col, lo, hi):
    return [i for i, r in enumerate(rows) if lo <= r[col] < hi]


def run_seed(seed, nrows, nops, workdir, verbose, trace=None, paranoid=False):
    rng = random.Random(seed)
    # small value domain so there are lots of duplicates (stresses btree leaves)
    domain = rng.choice([10, 100, 1000, 100000])
    server = Server(workdir, trace)
    tables = {}
    setup = ['create(db,"fz")']
    for name, idxs in LAYOUTS:
        setup.append('create(tbl,"%s",fz,%d)' % (name, NCOLS))
        for c in range(1, NCOLS + 1):
            setup.append('create(col,"col%d",fz.%s)' % (c, name))
        for col, kind, cl in idxs:
            setup.append("create(idx,fz.%s.%s,%s,%s)" % (name, col, kind, cl))
        rows = [[rng.randrange(-domain, domain) for _ in range(NCOLS)]
                for _ in range(rng.randrange(1, nrows))]
        csv = workdir / ("%s.csv" % name)
        csv.write_text(",".join("fz.%s.col%d" % (name, c) for c in range(1, NCOLS + 1)) + "\n" +
                       "".join(",".join(map(str, r)) + "\n" for r in rows))
        setup.append('load("%s")' % csv.name)
        tables[name] = rows
    setup.append("shutdown")
    server.run("\n".join(setup) + "\n")

    def verify(name, what):
        rows = tables[name]
        for c in range(NCOLS):
            out = server.run("s=select(fz.%s.col%d,null,null)\na=fetch(fz.%s.col1,s)\n"
                             "b=fetch(fz.%s.col2,s)\nc=fetch(fz.%s.col3,s)\nprint(a,b,c)\n"
                             % (name, c + 1, name, name, name))
            got = Counter(out.split())
            want = Counter(",".join(map(str, r)) for r in rows)
            if got != want:
                print("  seed %d: %s index on col%d diverged after %s (got %d rows, want %d)"
                      % (seed, name, c + 1, what, sum(got.values()), len(rows)))
                return False
        return True

    failures = 0
    try:
        for op_num in range(nops):
            name = rng.choice(list(tables))
            rows = tables[name]
            col = rng.randrange(NCOLS)
            colname = "fz.%s.col%d" % (name, col + 1)
            lo = rng.randrange(-domain - 2, domain + 2)
            hi = lo + rng.choice([0, 1, 2, rng.randrange(1, 2 * domain + 3)])
            out_col = rng.randrange(NCOLS)
            kind = rng.choices(["select", "update", "delete", "insert", "join"], [5, 2, 1, 2, 1])[0]
            if kind == "join":
                # join two tables on a column, compare the joined pairs
                other = rng.choice(list(tables))
                ocol = rng.randrange(NCOLS)
                method = rng.choice(["hash", "nested-loop"])
                olo = rng.randrange(-domain - 2, domain + 2)
                ohi = olo + rng.randrange(1, 2 * domain + 3)
                out = server.run(
                    "p1=select(%s,%d,%d)\nf1=fetch(%s,p1)\n"
                    "p2=select(fz.%s.col%d,%d,%d)\nf2=fetch(fz.%s.col%d,p2)\n"
                    "t1,t2=join(f1,p1,f2,p2,%s)\n"
                    "a=fetch(fz.%s.col%d,t1)\nb=fetch(fz.%s.col%d,t2)\nprint(a,b)\n"
                    % (colname, lo, hi, colname, other, ocol + 1, olo, ohi, other, ocol + 1,
                       method, name, out_col + 1, other, out_col + 1))
                left = rows_where(rows, col, lo, hi)
                orows = tables[other]
                right = rows_where(orows, ocol, olo, ohi)
                by_key = {}
                for j in right:
                    by_key.setdefault(orows[j][ocol], []).append(j)
                want = Counter("%d,%d" % (rows[i][out_col], orows[j][out_col])
                               for i in left for j in by_key.get(rows[i][col], []))
                got = Counter(out.split())
                if got != want:
                    failures += 1
                    print("  seed %d op %d: %s join %s.col%d=%s.col%d: got %d pairs, want %d"
                          % (seed, op_num, method, name, col + 1, other, ocol + 1,
                             sum(got.values()), sum(want.values())))
                continue
            if kind == "insert":
                row = [rng.randrange(-domain, domain) for _ in range(NCOLS)]
                server.run("relational_insert(fz.%s,%s)\n" % (name, ",".join(map(str, row))))
                rows.append(row)
                if paranoid and not verify(name, "op %d insert %s" % (op_num, row)):
                    return 1
                continue
            if kind == "update":
                upd_col = rng.randrange(NCOLS)
                val = rng.randrange(-domain, domain)
                server.run("u=select(%s,%d,%d)\nrelational_update(fz.%s.col%d,u,%d)\n"
                           % (colname, lo, hi, name, upd_col + 1, val))
                for i in rows_where(rows, col, lo, hi):
                    rows[i][upd_col] = val
                if paranoid and not verify(name, "op %d update col%d where col%d in [%d,%d) = %d"
                                           % (op_num, upd_col + 1, col + 1, lo, hi, val)):
                    return 1
                continue
            if kind == "delete":
                server.run("d=select(%s,%d,%d)\nrelational_delete(fz.%s,d)\n" % (colname, lo, hi, name))
                keep = set(range(len(rows))) - set(rows_where(rows, col, lo, hi))
                tables[name] = rows = [rows[i] for i in sorted(keep)]
                if paranoid and not verify(name, "op %d delete where col%d in [%d,%d)"
                                           % (op_num, col + 1, lo, hi)):
                    return 1
                continue
            # select + fetch, compared as a multiset
            out = server.run("s=select(%s,%d,%d)\nf=fetch(fz.%s.col%d,s)\nprint(f)\n"
                             % (colname, lo, hi, name, out_col + 1))
            got = Counter(int(x) for x in out.split())
            want = Counter(rows[i][out_col] for i in rows_where(rows, col, lo, hi))
            if got != want:
                failures += 1
                print("  seed %d op %d: %s select(col%d,%d,%d) fetch col%d: got %d rows, want %d"
                      % (seed, op_num, name, col + 1, lo, hi, out_col + 1,
                         sum(got.values()), sum(want.values())))
                if verbose:
                    print("    missing:", dict(want - got), "extra:", dict(got - want))
                if failures > 3:
                    break
            # occasionally restart to check that everything persists
            if rng.random() < 0.02:
                server.run("shutdown\n")
                if paranoid:
                    for t in tables:
                        if not verify(t, "restart before op %d" % op_num):
                            return 1
    finally:
        server.stop()
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seeds", type=int, default=20)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--ops", type=int, default=150)
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--trace", help="write every command sent to this file")
    parser.add_argument("--no-build", action="store_true", help="skip running make first")
    parser.add_argument("--paranoid", action="store_true",
                        help="after every write, check each index of the table (slow)")
    parser.add_argument("--server-log", help="append server stdout/stderr to this file")
    args = parser.parse_args()

    if not args.no_build:
        subprocess.run(["make", "-s", "all"], cwd=SRC, check=True)
    global SERVER_LOG
    SERVER_LOG = open(args.server_log, "a") if args.server_log else subprocess.DEVNULL
    bad = []
    for seed in range(args.start, args.start + args.seeds):
        # a fresh directory per seed so no socket or database state carries over
        with tempfile.TemporaryDirectory() as tmp:
            trace = open(args.trace, "w") if args.trace else None
            if run_seed(seed, args.rows, args.ops, Path(tmp), args.verbose, trace, args.paranoid):
                bad.append(seed)
    print("%d/%d seeds passed" % (args.seeds - len(bad), args.seeds))
    if bad:
        print("failing seeds:", bad)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
