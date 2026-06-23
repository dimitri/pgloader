#!/usr/bin/env python3
"""
Aggregate pgloader bench JSON summaries and print a timing comparison table.

Each JSON file is a pgloader --summary output augmented with os-wall-ms by
the Makefile runner.  v4 and v3 use different JSON structures:

  v4: {"grand-total": {"total-nanos": N, ...},
       "phases": {"post": {"tables": [{"label": "COPY Wall-Clock Time",
                                       "total-time": N}, ...]}}}

  v3: {"SECS": N, ...}   (yason-encoded state struct; root "SECS" = elapsed s)

Both formats carry "os-wall-ms" added by the shell wrapper.
"""

import json
import os
import sys
from pathlib import Path
from statistics import median

SUMMARY_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/pgloader-bench")


def parse_v4(d):
    pgloader_s = d["grand-total"]["total-nanos"] / 1e9
    copy_wall_s = None
    for t in d.get("phases", {}).get("post", {}).get("tables", []):
        if t.get("label") == "COPY Wall-Clock Time":
            copy_wall_s = t["total-time"] / 1e9
            break
    os_s = d.get("os-wall-ms", 0) / 1000
    rows = d["grand-total"].get("rows", 0)
    return pgloader_s, copy_wall_s, os_s, rows


def parse_v3(d):
    # yason encodes CL slot names as uppercase strings
    pgloader_s = d.get("SECS") or d.get("secs") or 0.0
    os_s = d.get("os-wall-ms", 0) / 1000
    # DATA is a list of groups; each group is a list of per-table dicts.
    # v3 batches concurrent tables into sub-lists and may emit JSON null
    # for empty sections (e.g. SQLite loads with no DATA tables tracked).
    data = d.get("DATA") or d.get("data") or []
    rows = sum(
        entry.get("ROWS") or entry.get("rows") or 0
        for group in data
        if group is not None
        for entry in (group if isinstance(group, list) else [group])
        if isinstance(entry, dict)
    )
    return float(pgloader_s), None, os_s, rows


def collect(suite, version):
    runs = []
    for n in range(1, 100):
        p = SUMMARY_DIR / f"{suite}-{version}-{n}.json"
        if not p.exists():
            break
        with open(p) as f:
            d = json.load(f)
        if version == "v4":
            runs.append(parse_v4(d))
        else:
            runs.append(parse_v3(d))
    return runs


def fmt_s(s, width=8):
    if s is None:
        return " " * (width - 2) + "—" + " "
    return f"{s:{width}.3f}s"


def fmt_rows(n):
    if not n:
        return ""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}k"
    return str(n)


def build_table(suites, versions):
    hdr = (f"{'dataset':<12} {'ver':<4} {'run':<5}  "
           f"{'pgloader':>9}  {'COPY wall':>10}  {'OS wall':>9}  {'rows':>7}")
    sep = "-" * len(hdr)
    rows = [hdr, sep]

    for suite in suites:
        for ver in versions:
            run_data = collect(suite, ver)
            if not run_data:
                continue
            for n, (pg, cp, os_, r) in enumerate(run_data, 1):
                rows.append(
                    f"{suite:<12} {ver:<4} {n:<5}  "
                    f"{fmt_s(pg)}  {fmt_s(cp)}  {fmt_s(os_)}  {fmt_rows(r):>7}"
                )
            pg_med   = median(r[0] for r in run_data)
            cp_vals  = [r[1] for r in run_data if r[1] is not None]
            cp_med   = median(cp_vals) if cp_vals else None
            os_med   = median(r[2] for r in run_data)
            row_vals = [r[3] for r in run_data if r[3]]
            rows_med = int(median(row_vals)) if row_vals else 0
            rows.append(
                f"{suite:<12} {ver:<4} {'med':<5}  "
                f"{fmt_s(pg_med)}  {fmt_s(cp_med)}  {fmt_s(os_med)}  {fmt_rows(rows_med):>7}"
            )

        v4 = collect(suite, "v4")
        v3 = collect(suite, "v3")
        if v4 and v3:
            v4_med = median(r[0] for r in v4)
            v3_med = median(r[0] for r in v3)
            ratio  = v3_med / v4_med if v4_med else 0
            os_v4  = median(r[2] for r in v4)
            os_v3  = median(r[2] for r in v3)
            os_ratio = os_v3 / os_v4 if os_v4 else 0
            rows.append(
                f"{suite:<12} {'—':<4} {'v3÷v4':<5}  "
                f"{ratio:>8.2f}×  {'—':>10}  {os_ratio:>8.2f}×"
            )
        rows.append("")

    return "\n".join(rows)


def as_markdown(table_str):
    lines = ["## pgloader bench results", "", "```", table_str, "```", ""]
    return "\n".join(lines)


def main():
    suites   = ["employees", "lahman"]
    versions = ["v4", "v3"]

    table = build_table(suites, versions)
    print(table)

    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a") as f:
            f.write(as_markdown(table))


if __name__ == "__main__":
    main()
