#!/usr/bin/env python3
"""
Run the end-to-end DSL tests in project_tests/ against a fresh server.

The tests depend on each other (later tests query tables created and
persisted by earlier ones), so they always run in order from a clean
database. The server runs from the repo root so `load("project_tests/...")`
paths resolve, and it is restarted after every test that ends in shutdown.

Usage:
    ./run_tests.py              # build, then run every test
    ./run_tests.py --upto 17    # run test01..test17
    ./run_tests.py --verbose    # show diffs for failures
"""
import argparse
import difflib
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
TESTS = ROOT / "project_tests"
SOCKET = ROOT / "cs165_unix_socket"
DATABASE = ROOT / "database"
TIMEOUT = 60


def start_server(log):
    if SOCKET.exists():
        SOCKET.unlink()
    server = subprocess.Popen([str(SRC / "server")], cwd=ROOT,
                              stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 5
    while not SOCKET.exists():
        if server.poll() is not None or time.time() > deadline:
            sys.exit("server failed to start (see %s)" % log.name)
        time.sleep(0.05)
    return server


def stop_server(server):
    if server is None:
        return
    try:
        server.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--upto", type=int, help="last test number to run")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="print a diff for each failure")
    parser.add_argument("--no-build", action="store_true",
                        help="skip running make first")
    args = parser.parse_args()

    if not args.no_build:
        subprocess.run(["make", "-s", "all"], cwd=SRC, check=True)

    tests = sorted(TESTS.glob("test*.dsl"))
    if args.upto:
        tests = [t for t in tests if int(t.stem[4:]) <= args.upto]

    shutil.rmtree(DATABASE, ignore_errors=True)
    out_dir = ROOT / "test_output"
    out_dir.mkdir(exist_ok=True)
    log = open(out_dir / "server.log", "w")

    failures = []
    server = None
    for test in tests:
        if server is None or server.poll() is not None:
            stop_server(server)
            server = start_server(log)

        start = time.time()
        with open(test) as stdin:
            try:
                proc = subprocess.run([str(SRC / "client")], cwd=ROOT, stdin=stdin,
                                      capture_output=True, text=True, timeout=TIMEOUT)
                actual = proc.stdout
            except subprocess.TimeoutExpired:
                actual = None
        elapsed = time.time() - start

        expected = test.with_suffix(".exp").read_text()
        (out_dir / (test.stem + ".out")).write_text(actual or "")
        name = test.stem
        if actual == expected:
            print("PASS  %s  %8.1f ms" % (name, elapsed * 1000))
        elif actual is not None and sorted(actual.splitlines()) == sorted(expected.splitlines()):
            # queries have no ORDER BY, so row order is implementation defined
            # (e.g. clustering physically reorders rows, joins emit by partition)
            print("PASS  %s  %8.1f ms  (same rows, different order)" % (name, elapsed * 1000))
        else:
            failures.append(name)
            reason = "timeout" if actual is None else "output differs"
            print("FAIL  %s  %8.1f ms  (%s)" % (name, elapsed * 1000, reason))
            if args.verbose and actual is not None:
                sys.stdout.writelines(difflib.unified_diff(
                    expected.splitlines(True), actual.splitlines(True),
                    "expected", "actual", n=1))

        if "shutdown" in test.read_text():
            stop_server(server)
            server = None

    if server is not None:
        server.terminate()
        stop_server(server)
    log.close()

    print("\n%d/%d passed" % (len(tests) - len(failures), len(tests)))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
