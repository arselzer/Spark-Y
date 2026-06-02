#!/usr/bin/env python3
"""Fetch SNAP graphs and build Postgres-loadable SQL dumps for Spark-Y.

The DBLP graph ships bundled (data/sql-dumps/snap_dblp.sql.gz). The other
SNAP graphs are large (Google/Patents/Wiki are 21–100 MB compressed, and
larger as SQL), so they're fetched on demand rather than committed.

Usage:
    python3 scripts/fetch_snap_graphs.py google              # one graph
    python3 scripts/fetch_snap_graphs.py google patents wiki # several
    python3 scripts/fetch_snap_graphs.py all --max-edges 2000000

Each produces data/sql-dumps/snap_<name>.sql.gz with a single edge table
<name>(fromNode INTEGER, toNode INTEGER) — matching the snap-queries.
Load it from the app (create a 'snap' database, then load the dump), or via
the Data Import UI. --max-edges caps the edge count to keep the dump small.
"""
import argparse
import gzip
import io
import os
import sys
import urllib.request

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "sql-dumps")

# name -> (snap.stanford.edu filename, table name)
GRAPHS = {
    "google":  ("web-Google.txt.gz", "google"),
    "patents": ("cit-Patents.txt.gz", "patents"),
    "wiki":    ("wiki-topcats.txt.gz", "wiki"),
    "dblp":    ("com-dblp.ungraph.txt.gz", "dblp"),
}
BASE = "https://snap.stanford.edu/data/"


def build(name: str, max_edges: int | None) -> None:
    fname, table = GRAPHS[name]
    url = BASE + fname
    out = os.path.abspath(os.path.join(OUT_DIR, f"snap_{name}.sql.gz"))
    print(f"[{name}] downloading {url}")
    raw = urllib.request.urlopen(url, timeout=120).read()
    text = gzip.decompress(raw).decode("utf-8", errors="replace")

    buf = io.StringIO()
    buf.write(f"-- SNAP {name} graph for Spark-Y demo (source: {url})\n")
    buf.write(f"DROP TABLE IF EXISTS {table} CASCADE;\n")
    buf.write(f"CREATE TABLE {table} (\n    fromNode INTEGER,\n    toNode INTEGER\n);\n\n")

    batch, n = [], 0
    BATCH = 1000
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            a, b = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        batch.append(f"({a}, {b})")
        n += 1
        if len(batch) >= BATCH:
            buf.write(f"INSERT INTO {table} (fromNode, toNode) VALUES\n")
            buf.write(",\n".join(batch)); buf.write(";\n")
            batch.clear()
        if max_edges and n >= max_edges:
            print(f"[{name}] capped at {max_edges} edges")
            break
    if batch:
        buf.write(f"INSERT INTO {table} (fromNode, toNode) VALUES\n")
        buf.write(",\n".join(batch)); buf.write(";\n")

    data = buf.getvalue().encode("utf-8")
    os.makedirs(OUT_DIR, exist_ok=True)
    with gzip.open(out, "wb", compresslevel=9) as f:
        f.write(data)
    print(f"[{name}] {n:,} edges -> {os.path.getsize(out):,} bytes gz -> {out}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("graphs", nargs="+", help="google patents wiki dblp | all")
    ap.add_argument("--max-edges", type=int, default=None, help="cap edges (smaller dump)")
    args = ap.parse_args()
    names = [g for g in GRAPHS if g != "dblp"] if args.graphs == ["all"] else args.graphs
    for name in names:
        if name not in GRAPHS:
            print(f"unknown graph '{name}' (choices: {', '.join(GRAPHS)})", file=sys.stderr)
            return 2
        build(name, args.max_edges)
    print("done. Load via the Data Import UI or: create a 'snap' DB then load the dump(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
