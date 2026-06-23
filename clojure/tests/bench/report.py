#!/usr/bin/env python3
"""
pgloader bench timing report.

Pivoted layout: rows are (run, step), columns are (dataset × version) + ratio.

  run │      step │ employees v3 │ employees v4 │ v3÷v4 │ lahman v3 │ lahman v4 │ v3÷v4
    1 │  pgloader │       x.xxxs │       x.xxxs │ x.xx× │    x.xxxs │    x.xxxs │ x.xx×
    1 │ COPY wall │            — │       x.xxxs │     — │         — │    x.xxxs │     —
    1 │   OS wall │       x.xxxs │       x.xxxs │ x.xx× │    x.xxxs │    x.xxxs │ x.xx×
  ...
  med │  pgloader │         ...

v4 JSON:  {"grand-total": {"total-nanos": N},
           "phases": {"post": {"tables": [{"label": "COPY Wall-Clock Time",
                                           "total-time": N}]}},
           "os-wall-ms": N}

v3 JSON:  {"SECS": N, "DATA": [[{table…}, …], …], "os-wall-ms": N}
          DATA is a list of concurrent-batch groups; some may be JSON null.
"""

import json
import os
import sys
from pathlib import Path
from statistics import median

SUMMARY_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("summaries")


# ── parsers ───────────────────────────────────────────────────────────────────

def parse_v4(d):
    pgloader_s  = d["grand-total"]["total-nanos"] / 1e9
    copy_wall_s = None
    for t in d.get("phases", {}).get("post", {}).get("tables", []):
        if t.get("label") == "COPY Wall-Clock Time":
            copy_wall_s = t["total-time"] / 1e9
            break
    os_s = d.get("os-wall-ms", 0) / 1000
    return pgloader_s, copy_wall_s, os_s


def parse_v3(d):
    pgloader_s = float(d.get("SECS") or d.get("secs") or 0.0)
    os_s       = d.get("os-wall-ms", 0) / 1000
    # "COPY Threads Completion" in POSTLOAD is the v3 equivalent of v4's
    # "COPY Wall-Clock Time": elapsed seconds until all copy threads finished.
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
    return pgloader_s, copy_wall_s, os_s


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


# ── formatting ────────────────────────────────────────────────────────────────

def _dat_w(suites, versions):
    """Minimum column width to fit the widest suite+version header."""
    return max(len(f"{s} {v}") for s in suites for v in versions) + 1

SEP   = " │ "
RAT_W = 6   # "v3÷v4" / "0.87×"
RUN_W = 3   # "med"
STP_W = 9   # "COPY wall"


def fmt_time(s, w):
    if s is None:
        return "—".rjust(w)
    return f"{s:>{w-1}.3f}s"


def fmt_ratio(v3, v4):
    if v3 is None or v4 is None or v4 == 0:
        return "—".rjust(RAT_W)
    return f"{v3 / v4:.2f}×".rjust(RAT_W)


# ── table builder ─────────────────────────────────────────────────────────────

def build_table(suites, versions):
    all_data = {s: {v: collect(s, v) for v in versions} for s in suites}
    max_runs = max(
        (len(all_data[s][v]) for s in suites for v in versions),
        default=0,
    )

    DW = _dat_w(suites, versions)

    # header
    parts = ["run".rjust(RUN_W), "step".rjust(STP_W)]
    for suite in suites:
        for ver in versions:
            parts.append(f"{suite} {ver}".rjust(DW))
        parts.append("v3÷v4".rjust(RAT_W))
    hdr = SEP.join(parts)
    bar = "─" * len(hdr)

    lines = [hdr, bar]

    steps = [
        ("pgloader",  lambda r: r[0]),
        ("COPY wall", lambda r: r[1]),
        ("OS wall",   lambda r: r[2]),
    ]

    def data_cols(suite, run_idx):
        """Return (v3_val, v4_val) for a given run index (0-based)."""
        cols = {}
        for ver in ("v3", "v4"):
            lst = all_data[suite][ver]
            cols[ver] = None
        for ver in versions:
            lst = all_data[suite][ver]
            cols[ver] = lst[run_idx] if run_idx < len(lst) else None
        return cols

    pg_getter = steps[0][1]   # pgloader-time accessor — the comparison metric

    # per-run blocks
    for run_n in range(1, max_runs + 1):
        for s_idx, (step_name, getter) in enumerate(steps):
            run_lbl    = str(run_n) if s_idx == 0 else ""
            show_ratio = s_idx == 0   # ratio only on the pgloader row
            parts      = [run_lbl.rjust(RUN_W), step_name.rjust(STP_W)]
            for suite in suites:
                cols  = data_cols(suite, run_n - 1)
                pg_v3 = pg_getter(cols["v3"]) if cols["v3"] is not None else None
                pg_v4 = pg_getter(cols["v4"]) if cols["v4"] is not None else None
                for ver in versions:
                    val = getter(cols[ver]) if cols[ver] is not None else None
                    parts.append(fmt_time(val, DW))
                parts.append(fmt_ratio(pg_v3, pg_v4) if show_ratio
                             else "—".rjust(RAT_W))
            lines.append(SEP.join(parts))
        lines.append(bar)

    # median block (bar from last run already printed)
    for s_idx, (step_name, getter) in enumerate(steps):
        run_lbl    = "med" if s_idx == 0 else ""
        show_ratio = s_idx == 0
        parts      = [run_lbl.rjust(RUN_W), step_name.rjust(STP_W)]
        for suite in suites:
            meds = {}
            for ver in versions:
                vals = [getter(r) for r in all_data[suite][ver]
                        if getter(r) is not None]
                meds[ver] = median(vals) if vals else None
            pg_v3_vals = [pg_getter(r) for r in all_data[suite]["v3"]
                          if pg_getter(r) is not None]
            pg_v4_vals = [pg_getter(r) for r in all_data[suite]["v4"]
                          if pg_getter(r) is not None]
            pg_v3m = median(pg_v3_vals) if pg_v3_vals else None
            pg_v4m = median(pg_v4_vals) if pg_v4_vals else None
            for ver in versions:
                parts.append(fmt_time(meds[ver], DW))
            parts.append(fmt_ratio(pg_v3m, pg_v4m) if show_ratio
                         else "—".rjust(RAT_W))
        lines.append(SEP.join(parts))

    return "\n".join(lines)


# ── entry point ───────────────────────────────────────────────────────────────

def as_markdown(table_str):
    return "\n".join(["## pgloader bench results", "", "```", table_str, "```", ""])


def main():
    suites   = ["employees", "lahman"]
    versions = ["v3", "v4"]   # v3 first → ratio is v3÷v4

    table = build_table(suites, versions)
    print(table)

    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a") as f:
            f.write(as_markdown(table))


if __name__ == "__main__":
    main()
