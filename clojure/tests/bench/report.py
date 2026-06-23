#!/usr/bin/env python3
"""
pgloader bench timing report — PR-description format.

Rows are (dataset, version, run); columns are timing metrics.
A v3÷v4 ratio row follows each dataset block.

  dataset      ver   run │  pgloader │ COPY wall │   OS wall │    rows │  bytes │  MB/s
  ─────────────────────────────────────────────────────────────────────────────────────
  employees     v4     1 │    7.061s │    5.436s │    7.229s │   3.92M │  135MB │  24.8
                v4     2 │    7.168s │    5.519s │    7.306s │   3.92M │  135MB │  24.4
                v4   med │    7.115s │    5.478s │    7.268s │   3.92M │  135MB │  24.6
                v3     1 │    7.061s │    5.127s │    7.220s │   3.92M │  135MB │  26.4
                v3   med │    7.061s │    5.127s │    7.220s │   3.92M │  135MB │  26.4
                 ─  ratio│    1.00× │    1.06× │         — │       — │      — │     —
  ─────────────────────────────────────────────────────────────────────────────────────
  lahman        v4     1 │   ...

v4 JSON: {"grand-total": {"total-nanos": N, "bytes": N, "rows": N},
          "phases": {"post": {"tables": [{"label": "COPY Wall-Clock Time",
                                          "total-time": N}]}},
          "os-wall-ms": N}

v3 JSON: {"SECS": N, "BYTES": N, "DATA": [[{..., "ROWS": N}, ...], ...],
          "POSTLOAD": [[{"NAME": "COPY Threads Completion", "SECS": N}, ...]],
          "os-wall-ms": N}
"""

import json
import os
import sys
from pathlib import Path
from statistics import median

SUMMARY_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("summaries")


# ── parsers ───────────────────────────────────────────────────────────────────

# Each parser returns a 5-tuple: (pgloader_s, copy_wall_s, os_s, bytes_, rows_)

def parse_v4(d):
    gt = d["grand-total"]
    pgloader_s  = gt["total-nanos"] / 1e9
    bytes_      = int(gt.get("bytes") or 0)
    rows_       = int(gt.get("rows")  or 0)
    copy_wall_s = None
    for t in d.get("phases", {}).get("post", {}).get("tables", []):
        if t.get("label") == "COPY Wall-Clock Time":
            copy_wall_s = t["total-time"] / 1e9
            break
    os_s = d.get("os-wall-ms", 0) / 1000
    return pgloader_s, copy_wall_s, os_s, bytes_, rows_


def parse_v3(d):
    pgloader_s = float(d.get("SECS") or d.get("secs") or 0.0)
    os_s       = d.get("os-wall-ms", 0) / 1000
    bytes_     = int(d.get("BYTES") or d.get("bytes") or 0)
    rows_      = 0
    for group in (d.get("DATA") or []):
        if not group:
            continue
        for entry in (group if isinstance(group, list) else [group]):
            if isinstance(entry, dict):
                rows_ += int(entry.get("ROWS") or 0)
    copy_wall_s = None
    for group in (d.get("POSTLOAD") or d.get("postload") or []):
        if not group:
            continue
        for entry in (group if isinstance(group, list) else [group]):
            if isinstance(entry, dict) and entry.get("NAME") == "COPY Threads Completion":
                secs = entry.get("SECS") or entry.get("secs")
                if secs:
                    copy_wall_s = float(secs)
                break
    return pgloader_s, copy_wall_s, os_s, bytes_, rows_


def collect(suite, version):
    runs = []
    for n in range(1, 100):
        p = SUMMARY_DIR / f"{suite}-{version}-{n}.json"
        if not p.exists():
            break
        with open(p) as f:
            d = json.load(f)
        runs.append(parse_v4(d) if version == "v4" else parse_v3(d))
    return runs


# ── field accessors (index into 5-tuple) ──────────────────────────────────────

F_PG   = 0   # pgloader total time
F_COPY = 1   # COPY wall-clock time
F_OS   = 2   # OS wall time
F_BYTES = 3  # bytes transferred
F_ROWS  = 4  # rows transferred


# ── formatting helpers ─────────────────────────────────────────────────────────

def fmt_time(s, w):
    if s is None:
        return "—".rjust(w)
    return f"{s:.3f}s".rjust(w)


def fmt_rows(n, w):
    if not n:
        return "—".rjust(w)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M".rjust(w)
    if n >= 1_000:
        return f"{n / 1_000:.0f}k".rjust(w)
    return str(int(n)).rjust(w)


def fmt_bytes(b, w):
    if not b:
        return "—".rjust(w)
    if b >= 1_073_741_824:
        return f"{b / 1_073_741_824:.1f}G".rjust(w)
    if b >= 1_048_576:
        return f"{b / 1_048_576:.0f}M".rjust(w)
    if b >= 1_024:
        return f"{b / 1_024:.0f}K".rjust(w)
    return str(int(b)).rjust(w)


def fmt_mbps(bytes_, copy_s, w):
    if not bytes_ or not copy_s:
        return "—".rjust(w)
    return f"{bytes_ / 1_048_576 / copy_s:.1f}".rjust(w)


def fmt_ratio(v3, v4, w):
    if v3 is None or v4 is None or v4 == 0:
        return "—".rjust(w)
    return f"{v3 / v4:.2f}×".rjust(w)


def _med(lst, field):
    vals = [r[field] for r in lst if r is not None and r[field]]
    return median(vals) if vals else None


# ── table builder ─────────────────────────────────────────────────────────────

# Column widths
DST_W  = 0   # computed from suite names
VER_W  = 5   # "   v4" / "ratio"
RUN_W  = 5   # "  med" / "    1"
TIME_W = 10  # "   7.061s"
ROW_W  = 7   # "  3.92M"
BYT_W  = 7   # "   135M"
MBS_W  = 7   # "   24.8"
SEP    = " │ "


def build_table(suites, versions=("v4", "v3")):
    all_data = {s: {v: collect(s, v) for v in versions} for s in suites}
    max_runs = max(
        (len(all_data[s][v]) for s in suites for v in versions),
        default=0,
    )

    global DST_W
    DST_W = max(len(s) for s in suites) + 2   # +2 for padding

    # ── header ────────────────────────────────────────────────────────────────
    hdr = SEP.join([
        "dataset".rjust(DST_W),
        "ver".rjust(VER_W),
        "run".rjust(RUN_W),
        "pgloader".rjust(TIME_W),
        "COPY wall".rjust(TIME_W),
        "OS wall".rjust(TIME_W),
        "rows".rjust(ROW_W),
        "bytes".rjust(BYT_W),
        "MB/s".rjust(MBS_W),
    ])
    bar = "─" * len(hdr)
    lines = [hdr, bar]

    # ── per-dataset blocks ────────────────────────────────────────────────────
    for s_idx, suite in enumerate(suites):
        if s_idx > 0:
            lines.append(bar)

        for v_idx, ver in enumerate(versions):
            runs = all_data[suite][ver]

            # per-run rows
            for run_idx in range(max_runs):
                r = runs[run_idx] if run_idx < len(runs) else None
                pg   = r[F_PG]    if r else None
                copy = r[F_COPY]  if r else None
                os_s = r[F_OS]    if r else None
                byt  = r[F_BYTES] if r else None
                row  = r[F_ROWS]  if r else None

                dataset_lbl = suite.rjust(DST_W) if (v_idx == 0 and run_idx == 0) else " " * DST_W
                lines.append(SEP.join([
                    dataset_lbl,
                    ver.rjust(VER_W),
                    str(run_idx + 1).rjust(RUN_W),
                    fmt_time(pg,   TIME_W),
                    fmt_time(copy, TIME_W),
                    fmt_time(os_s, TIME_W),
                    fmt_rows(row,  ROW_W),
                    fmt_bytes(byt, BYT_W),
                    fmt_mbps(byt, copy, MBS_W),
                ]))

            # median row
            med_pg   = _med(runs, F_PG)
            med_copy = _med(runs, F_COPY)
            med_os   = _med(runs, F_OS)
            med_byt  = _med(runs, F_BYTES)
            med_row  = _med(runs, F_ROWS)
            lines.append(SEP.join([
                " " * DST_W,
                ver.rjust(VER_W),
                "med".rjust(RUN_W),
                fmt_time(med_pg,   TIME_W),
                fmt_time(med_copy, TIME_W),
                fmt_time(med_os,   TIME_W),
                fmt_rows(med_row,  ROW_W),
                fmt_bytes(med_byt, BYT_W),
                fmt_mbps(med_byt, med_copy, MBS_W),
            ]))

        # ratio row: always v3 ÷ v4 on pgloader + COPY wall; — elsewhere
        v4r = all_data[suite]["v4"]
        v3r = all_data[suite]["v3"]
        m4_pg   = _med(v4r, F_PG)
        m3_pg   = _med(v3r, F_PG)
        m4_copy = _med(v4r, F_COPY)
        m3_copy = _med(v3r, F_COPY)
        lines.append(SEP.join([
            " " * DST_W,
            "─".rjust(VER_W),
            "ratio".rjust(RUN_W),
            fmt_ratio(m3_pg,   m4_pg,   TIME_W),
            fmt_ratio(m3_copy, m4_copy, TIME_W),
            "—".rjust(TIME_W),
            "—".rjust(ROW_W),
            "—".rjust(BYT_W),
            "—".rjust(MBS_W),
        ]))

    lines.append(bar)
    return "\n".join(lines)


# ── entry point ───────────────────────────────────────────────────────────────

def as_markdown(table_str):
    return "\n".join(["## pgloader bench results", "", "```", table_str, "```", ""])


def main():
    # discover suites from available JSON files
    suites = []
    for name in ("employees", "lahman", "divvy"):
        if any(SUMMARY_DIR.glob(f"{name}-v4-*.json")) or \
           any(SUMMARY_DIR.glob(f"{name}-v3-*.json")):
            suites.append(name)
    if not suites:
        print("No bench summary files found in", SUMMARY_DIR)
        sys.exit(1)

    table = build_table(suites)
    print(table)

    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a") as f:
            f.write(as_markdown(table))


if __name__ == "__main__":
    main()
