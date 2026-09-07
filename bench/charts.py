"""Render benchmark results as standalone SVG.

Hand-rolled rather than matplotlib: the project is standard-library only and a
chart is a few dozen lines of geometry.

    python -m bench.charts
"""

from __future__ import annotations

import json
import math
import os
import sys
from typing import List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RESULTS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")

INK = "#12161f"
DIM = "#6b7688"
GRID = "#e3e7ee"
SERIES = ["#2563eb", "#dc2626", "#059669", "#d97706"]


def _fmt(v: float) -> str:
    if v >= 1e9:
        return "%.0fG" % (v / 1e9)
    if v >= 1e6:
        return "%.0fM" % (v / 1e6)
    if v >= 1e3:
        return "%.0fk" % (v / 1e3)
    if v >= 10:
        return "%.0f" % v
    return "%.2g" % v


def line_chart(path: str, title: str, subtitle: str, xs: Sequence[float],
               series: List[Tuple[str, Sequence[Optional[float]]]],
               x_label: str, y_label: str, log_y: bool = False,
               log_x: bool = False, annotate: str = "") -> None:
    W, H = 760, 430
    L, R, T, B = 78, 30, 74, 66
    pw, ph = W - L - R, H - T - B

    flat = [v for _, ys in series for v in ys if v is not None and (not log_y or v > 0)]
    if not flat:
        return
    ymax = max(flat) * 1.12
    ymin = min(flat) * 0.85 if log_y else 0.0
    if log_y:
        ymin = max(ymin, min(flat) / 3.0)

    def sx(x: float) -> float:
        if log_x:
            lo, hi = math.log10(min(xs)), math.log10(max(xs))
            return L + pw * ((math.log10(x) - lo) / (hi - lo) if hi > lo else 0.5)
        lo, hi = min(xs), max(xs)
        return L + pw * ((x - lo) / (hi - lo) if hi > lo else 0.5)

    def sy(y: float) -> float:
        if log_y:
            lo, hi = math.log10(ymin), math.log10(ymax)
            return T + ph - ph * ((math.log10(max(y, ymin)) - lo) / (hi - lo))
        return T + ph - ph * ((y - ymin) / (ymax - ymin) if ymax > ymin else 0.5)

    p = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" '
         'font-family="Inter,Segoe UI,Helvetica,Arial,sans-serif">' % (W, H, W, H)]
    p.append('<rect width="%d" height="%d" fill="#fff"/>' % (W, H))
    p.append('<text x="%d" y="30" font-size="17" font-weight="600" fill="%s">%s</text>'
             % (L - 44, INK, title))
    p.append('<text x="%d" y="50" font-size="12" fill="%s">%s</text>' % (L - 44, DIM, subtitle))

    # y grid
    ticks = 5
    for i in range(ticks + 1):
        if log_y:
            lo, hi = math.log10(ymin), math.log10(ymax)
            v = 10 ** (lo + (hi - lo) * i / ticks)
        else:
            v = ymin + (ymax - ymin) * i / ticks
        y = sy(v)
        p.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s"/>'
                 % (L, y, L + pw, y, GRID))
        p.append('<text x="%d" y="%.1f" font-size="11" fill="%s" text-anchor="end">%s</text>'
                 % (L - 9, y + 3.5, DIM, _fmt(v)))

    for x in xs:
        p.append('<text x="%.1f" y="%d" font-size="11" fill="%s" text-anchor="middle">%s</text>'
                 % (sx(x), T + ph + 20, DIM, _fmt(x)))

    p.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s"/>'
             % (L, T + ph, L + pw, T + ph, "#c3cad6"))
    p.append('<text x="%.1f" y="%d" font-size="11.5" fill="%s" text-anchor="middle">%s</text>'
             % (L + pw / 2, H - 24, INK, x_label))
    p.append('<text transform="translate(20,%.1f) rotate(-90)" font-size="11.5" fill="%s" '
             'text-anchor="middle">%s</text>' % (T + ph / 2, INK, y_label))

    for i, (name, ys) in enumerate(series):
        col = SERIES[i % len(SERIES)]
        pts = [(sx(x), sy(y)) for x, y in zip(xs, ys) if y is not None]
        if not pts:
            continue
        p.append('<polyline fill="none" stroke="%s" stroke-width="2.4" '
                 'stroke-linejoin="round" points="%s"/>'
                 % (col, " ".join("%.1f,%.1f" % q for q in pts)))
        for qx, qy in pts:
            p.append('<circle cx="%.1f" cy="%.1f" r="3.6" fill="#fff" stroke="%s" '
                     'stroke-width="2.2"/>' % (qx, qy, col))
        lx, ly = pts[-1]
        p.append('<text x="%.1f" y="%.1f" font-size="11.5" font-weight="600" fill="%s" '
                 'text-anchor="end">%s</text>' % (lx - 6, ly - 11, col, name))

    if annotate:
        p.append('<text x="%d" y="%d" font-size="11" fill="%s">%s</text>'
                 % (L, H - 6, DIM, annotate))
    p.append("</svg>")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(p))
    print("  wrote %s" % os.path.relpath(path))


def bar_chart(path: str, title: str, subtitle: str,
              labels: Sequence[str], values: Sequence[float],
              value_fmt=lambda v: "%.0f" % v, y_label: str = "") -> None:
    W, H = 760, 400
    L, R, T, B = 78, 30, 74, 74
    pw, ph = W - L - R, H - T - B
    vmax = max(values) * 1.15 or 1
    bw = pw / (len(values) * 1.6)

    p = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" '
         'font-family="Inter,Segoe UI,Helvetica,Arial,sans-serif">' % (W, H, W, H)]
    p.append('<rect width="%d" height="%d" fill="#fff"/>' % (W, H))
    p.append('<text x="%d" y="30" font-size="17" font-weight="600" fill="%s">%s</text>'
             % (L - 44, INK, title))
    p.append('<text x="%d" y="50" font-size="12" fill="%s">%s</text>' % (L - 44, DIM, subtitle))
    for i in range(6):
        v = vmax * i / 5
        y = T + ph - ph * (v / vmax)
        p.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s"/>' % (L, y, L + pw, y, GRID))
        p.append('<text x="%d" y="%.1f" font-size="11" fill="%s" text-anchor="end">%s</text>'
                 % (L - 9, y + 3.5, DIM, _fmt(v)))
    for i, (lab, v) in enumerate(zip(labels, values)):
        x = L + pw * (i + 0.5) / len(values) - bw / 2
        h = ph * (v / vmax)
        p.append('<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="3" fill="%s"/>'
                 % (x, T + ph - h, bw, h, SERIES[i % len(SERIES)]))
        p.append('<text x="%.1f" y="%.1f" font-size="11.5" font-weight="600" fill="%s" '
                 'text-anchor="middle">%s</text>' % (x + bw / 2, T + ph - h - 7, INK, value_fmt(v)))
        p.append('<text x="%.1f" y="%d" font-size="11" fill="%s" text-anchor="middle">%s</text>'
                 % (x + bw / 2, T + ph + 20, DIM, lab))
    if y_label:
        p.append('<text transform="translate(20,%.1f) rotate(-90)" font-size="11.5" fill="%s" '
                 'text-anchor="middle">%s</text>' % (T + ph / 2, INK, y_label))
    p.append("</svg>")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(p))
    print("  wrote %s" % os.path.relpath(path))


def before_after(path: str, title: str, subtitle: str, before_label: str,
                 before: float, after_label: str, after: float,
                 fmt=lambda v: "%.1f" % v, y_label: str = "",
                 caption: str = "") -> None:
    """Two bars, one number moving."""
    W, H = 700, 400
    L, R, T, B = 96, 210, 82, 66
    pw, ph = W - L - R, H - T - B
    vmax = max(before, after) * 1.25 or 1
    bw = pw / 3.2

    down = before and after and after < before
    pct = (100.0 * (before - after) / before) if before else 0

    p = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" '
         'font-family="Inter,Segoe UI,Helvetica,Arial,sans-serif">' % (W, H, W, H)]
    p.append('<rect width="%d" height="%d" fill="#fff"/>' % (W, H))
    p.append('<text x="34" y="34" font-size="18" font-weight="600" fill="%s">%s</text>'
             % (INK, title))
    p.append('<text x="34" y="55" font-size="12" fill="%s">%s</text>' % (DIM, subtitle))

    for i, (lab, val, col) in enumerate(((before_label, before, "#dc2626"),
                                         (after_label, after, "#059669"))):
        x = L + pw * (i + 0.5) / 2 - bw / 2
        h = ph * (val / vmax)
        p.append('<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="4" fill="%s"/>'
                 % (x, T + ph - h, bw, h, col))
        p.append('<text x="%.1f" y="%.1f" font-size="21" font-weight="700" fill="%s" '
                 'text-anchor="middle">%s</text>'
                 % (x + bw / 2, T + ph - h - 12, col, fmt(val)))
        p.append('<text x="%.1f" y="%d" font-size="12.5" fill="%s" text-anchor="middle">%s</text>'
                 % (x + bw / 2, T + ph + 22, INK, lab))

    p.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#c3cad6"/>'
             % (L, T + ph, L + pw, T + ph, ))
    # the arrow and the headline delta
    ax = L + pw + 26
    p.append('<text x="%d" y="%d" font-size="40" font-weight="700" fill="%s">%s</text>'
             % (ax, T + ph / 2 + 2, "#059669" if down else "#dc2626", "&#8595;" if down else "&#8593;"))
    p.append('<text x="%d" y="%d" font-size="27" font-weight="700" fill="%s">%.0f%%</text>'
             % (ax + 34, T + ph / 2, "#059669" if down else "#dc2626", abs(pct)))
    p.append('<text x="%d" y="%d" font-size="12" fill="%s">%s</text>'
             % (ax + 34, T + ph / 2 + 20, DIM, "reduction" if down else "increase"))
    if y_label:
        p.append('<text transform="translate(24,%.1f) rotate(-90)" font-size="11.5" '
                 'fill="%s" text-anchor="middle">%s</text>' % (T + ph / 2, INK, y_label))
    if caption:
        p.append('<text x="34" y="%d" font-size="11" fill="%s">%s</text>'
                 % (H - 14, DIM, caption))
    p.append("</svg>")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(p))
    print("  wrote %s" % os.path.relpath(path))


def load(name: str):
    p = os.path.join(RESULTS, name)
    if not os.path.exists(p):
        return None
    with open(p) as fh:
        return json.load(fh)


def main() -> int:
    os.makedirs(RESULTS, exist_ok=True)
    print("rendering charts from results/ ...")

    bench = load("benchmarks.json") or {}
    sim = load("sim_swarm.json")
    val = load("validation.json")

    # 1. THE money chart -- source egress, measured, ripple vs star.
    rows = bench.get("source_egress")
    if rows:
        xs = [r["nodes"] for r in rows]
        line_chart(
            os.path.join(RESULTS, "chart_egress.svg"),
            "Bytes sent by the publisher as the cluster grows",
            "Measured on a real cluster. Star topology sends the file once per node; "
            "Ripple lets every node that holds a chunk serve it.",
            xs,
            [("Ripple (swarm)", [r["ripple_egress"] for r in rows]),
             ("Star (naive)", [r["star_egress"] for r in rows])],
            "nodes in cluster", "bytes out of the source",
            annotate="File size: %.0f MB. Lower is better." % (rows[0]["file_bytes"] / 1e6))

        line_chart(
            os.path.join(RESULTS, "chart_convergence.svg"),
            "Time for every node to hold every byte",
            "Same runs. The swarm gets faster relative to the star as the cluster grows.",
            xs,
            [("Ripple (swarm)", [r["ripple_convergence_s"] for r in rows]),
             ("Star (naive)", [r["star_convergence_s"] for r in rows])],
            "nodes in cluster", "seconds to full convergence")

    # 2. Manifest vs bytes -- the thesis.
    rows = bench.get("manifest_vs_bytes")
    if rows:
        ok = [r for r in rows if r["full_sync_s"] == r["full_sync_s"]]
        if ok:
            line_chart(
                os.path.join(RESULTS, "chart_manifest_vs_bytes.svg"),
                "The file exists everywhere long before the bytes arrive",
                "Manifest propagation versus full materialisation, same cluster, same file.",
                [r["file_bytes"] for r in ok],
                [("manifest everywhere", [r["manifest_sync_s"] for r in ok]),
                 ("every byte everywhere", [r["full_sync_s"] for r in ok])],
                "file size (bytes)", "seconds", log_y=True, log_x=True,
                annotate="Log scale on both axes. The gap is what lazy materialisation buys.")

    # 3. Simulator projection.
    if sim:
        xs = [r["nodes"] for r in sim]
        line_chart(
            os.path.join(RESULTS, "chart_sim_scale.svg"),
            "Projected to 1,000 nodes",
            "Discrete-event model of the same protocol, calibrated against the real "
            "cluster (see chart_validation.svg).",
            xs,
            [("convergence (s)", [r["convergence_s"] for r in sim]),
             ("manifest everywhere (s)", [r["manifest_all_known_s"] for r in sim])],
            "nodes in cluster", "seconds", log_x=True,
            annotate="100x the nodes costs ~3x the time: gossip spreads in O(log N) rounds.")

        bar_chart(
            os.path.join(RESULTS, "chart_sim_egress.svg"),
            "Source egress stays flat while a star topology grows linearly",
            "Simulated. A star at 1,000 nodes would send 1,000x the file; Ripple sends ~7x.",
            ["%d" % r["nodes"] for r in sim],
            [r["source_egress_ratio"] for r in sim],
            value_fmt=lambda v: "%.1fx" % v,
            y_label="publisher egress (multiples of file size)")

    # 4. Validation: does the model track reality?
    if val:
        comps = val["comparisons"]
        line_chart(
            os.path.join(RESULTS, "chart_validation.svg"),
            "Simulator vs reality",
            "Model calibrated on N=%d only; every larger point is a prediction. "
            "Convergence error %.1f%%, egress error %.1f%%."
            % (val["calibrated_on"], val["mean_convergence_error_pct"],
               val.get("mean_egress_error_pct", float("nan"))),
            [c["nodes"] for c in comps],
            [("measured", [c["real"]["convergence_s"] for c in comps]),
             ("simulated", [c["sim"]["convergence_s"] for c in comps])],
            "nodes in cluster", "seconds to full convergence",
            annotate=val["verdict"])

    # 4b. Why the real cluster's wall-clock is linear: the harness saturates.
    if val:
        comps = val["comparisons"]
        thr = [c["real"].get("aggregate_throughput") for c in comps]
        if all(thr):
            line_chart(
                os.path.join(RESULTS, "chart_harness_limit.svg"),
                "The test harness is the bottleneck, not the protocol",
                "Aggregate throughput across the whole cluster, measured. It does not "
                "rise with node count, because every node is a thread on one machine.",
                [c["nodes"] for c in comps],
                [("aggregate throughput (B/s)", thr)],
                "nodes in cluster", "bytes/s across the whole cluster",
                annotate="Constant rate + linearly growing work = linearly growing "
                         "wall-clock. That is arithmetic about this laptop, not the design.")

    # 4c. The three findings, each as one number moving. Measured by the
    # ablation in benchmark.py experiment 7 and by validate.py, not remembered.
    abl = bench.get("ablation")
    if abl and len(abl) >= 2:
        # Compare against the configuration that won, not the last one tried:
        # super-seeding is listed last and did not help.
        naive = abl[0]
        fixed = min(abl, key=lambda r: r["publisher_share_pct"])
        before_after(
            os.path.join(RESULTS, "chart_fix_publisher_share.svg"),
            "The swarm was secretly a star",
            "Share of all chunks served by the publisher, %d-node cluster."
            % (bench.get("ablation_nodes") or 10),
            "rarest-first alone", naive["publisher_share_pct"],
            fixed["config"], fixed["publisher_share_pct"],
            fmt=lambda v: "%.0f%%" % v, y_label="% of chunks served by publisher",
            caption="Same code, same hardware, same run: two scheduling flags toggled.")
        before_after(
            os.path.join(RESULTS, "chart_fix_egress.svg"),
            "What that cost the publisher",
            "Bytes out of the source, as a multiple of the file.",
            "rarest-first alone", naive["source_egress_ratio"],
            fixed["config"], fixed["source_egress_ratio"],
            fmt=lambda v: "%.2fx" % v, y_label="publisher egress (x file size)",
            caption="Convergence got faster too, not slower.")

    if val and val.get("naive_convergence_error_pct"):
        before_after(
            os.path.join(RESULTS, "chart_fix_simulator.svg"),
            "Making the simulator honest",
            "Mean convergence error against the real cluster, on node counts the "
            "model was never fitted to.",
            "before modelling the rig", val["naive_convergence_error_pct"],
            "after", val["mean_convergence_error_pct"],
            fmt=lambda v: "%.0f%%" % v, y_label="mean prediction error",
            caption="Both models calibrated identically, on the smallest run only.")

    # 5. Metadata scaling.
    rows = bench.get("metadata_scaling")
    if rows:
        line_chart(
            os.path.join(RESULTS, "chart_metadata.svg"),
            "Availability metadata per gossip round",
            "A Bloom summary answers 'which chunks do you have?' in a fixed budget; "
            "a raw ID list does not.",
            [r["chunks"] for r in rows],
            [("raw chunk-ID list", [r["raw_id_bytes"] for r in rows]),
             ("Bloom summary", [r["bloom_bytes"] for r in rows])],
            "chunks held by the node", "bytes per round", log_x=True, log_y=True)

    # 6. Incremental edit.
    row = bench.get("incremental_edit")
    if row:
        bar_chart(
            os.path.join(RESULTS, "chart_edit.svg"),
            "Cost of a one-line change to a large file",
            "Content-defined chunking means only the chunks containing the edit are new.",
            ["whole file", "actually transferred"],
            [row["file_bytes"], row["bytes_moved"]],
            value_fmt=lambda v: "%.1f MB" % (v / 1e6), y_label="bytes")

    print("done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
