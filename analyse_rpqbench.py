#!/usr/bin/env python3

import argparse
import csv
import math
import re
import statistics as st
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


COMPETITORS = ["LARPQ", "MillenniumDB", "FalkorDB"]
DEFAULT_VARIANT_IDS = list(range(1, 11))
EDGE_ABBREVIATIONS = {
    "references": "R",
    "cite": "C",
    "creator": "K",
    "coauthor": "A",
    "predecessor": "P",
    "editor": "E",
    "partOf": "PO",
}


def competitor_total_runs(competitor: str) -> int:
    if competitor == "FalkorDB":
        return 5
    return 5


def competitor_time_scale_to_us(competitor: str) -> float:
    if competitor == "FalkorDB":
        return 1.0 / 1000.0
    return 1.0


def competitor_uses_all_runs_after_warmup(competitor: str) -> bool:
    return competitor == "FalkorDB"


@dataclass(frozen=True)
class VariantStats:
    variant_id: int
    raw_times_ms: list[float]
    used_times_ms: list[float]
    outlier_times_ms: list[float]
    mean_ms: float
    median_ms: float
    rsd_pct: float
    answer: Optional[str]


@dataclass(frozen=True)
class QueryTypeMeta:
    query_type_id: int
    label: str
    raw_expression: str
    variant_queries: dict[int, str]


def remove_outliers_20pct(values: list[float]) -> tuple[list[float], list[float]]:
    if len(values) < 3:
        return values[:], []

    used: list[float] = []
    outliers: list[float] = []

    for i, value in enumerate(values):
        others = values[:i] + values[i + 1 :]
        ref = st.median(others)

        if ref == 0:
            is_outlier = value != 0
        else:
            is_outlier = abs(value - ref) / abs(ref) > 0.20

        if is_outlier:
            outliers.append(value)
        else:
            used.append(value)

    if len(used) < 2:
        return values[:], []

    return used, outliers


def relative_stddev_pct(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0

    mean = st.mean(values)
    if mean == 0:
        return 0.0

    return st.pstdev(values) / mean * 100.0


def fmt_ms(x: Optional[float], digits: int = 1) -> str:
    if x is None or not math.isfinite(x):
        return "-"
    return f"{x:.{digits}f}"


def us_to_ms(x: float) -> float:
    return x / 1000.0


def fmt_time_ms(x: Optional[float], digits: int = 1) -> str:
    if x is None or not math.isfinite(x):
        return "-"
    return f"{us_to_ms(x):.{digits}f}"


def fmt_time_list_ms(values: list[float]) -> str:
    if not values:
        return ""
    return ";".join(f"{us_to_ms(x):.6f}" for x in values)


def slugify_label(label: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", label).strip("_")
    return slug or "query"


def normalize_expression(expr: str) -> str:
    def replace_edge(match: re.Match[str]) -> str:
        edge = match.group(1)
        return EDGE_ABBREVIATIONS.get(edge, edge)

    return re.sub(r"<([^>]+)>", replace_edge, expr)


def parse_strict_integer_answer(answer: Optional[str]) -> Optional[str]:
    if answer is None:
        return None
    answer = answer.strip()
    if not answer:
        return None
    if answer.lstrip("-").isdigit():
        return answer
    return None


def load_query_type_meta(query_dir: Path, query_type_id: int) -> QueryTypeMeta:
    path = query_dir / f"{query_type_id}.txt"
    if not path.exists():
        return QueryTypeMeta(
            query_type_id=query_type_id,
            label=str(query_type_id),
            raw_expression=str(query_type_id),
            variant_queries={},
        )

    variant_queries: dict[int, str] = {}
    raw_expression = str(query_type_id)

    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, 1):
            line = raw_line.strip()
            if not line:
                continue

            row = next(csv.reader([line]))
            if len(row) != 2:
                raise ValueError(
                    f"{path}: line {line_no}: expected 2 CSV fields, got {len(row)}"
                )

            variant_id = int(row[0].strip())
            query_text = row[1].strip()
            variant_queries[variant_id] = query_text

            parts = query_text.split()
            if len(parts) != 3:
                raise ValueError(
                    f"{path}: line {line_no}: expected 3 tokens in query body, got {len(parts)}"
                )

            raw_expression = parts[1]

    return QueryTypeMeta(
        query_type_id=query_type_id,
        label=normalize_expression(raw_expression),
        raw_expression=raw_expression,
        variant_queries=variant_queries,
    )


def finalize_variant_stats(
    competitor: str,
    query_type_id: int,
    variant_id: int,
    times_ms: list[float],
    answers: list[Optional[str]],
    warnings: list[str],
) -> Optional[VariantStats]:
    if not times_ms:
        return None

    if competitor_uses_all_runs_after_warmup(competitor):
        if len(times_ms) < 2:
            warnings.append(
                f"{competitor} query-type {query_type_id} variant {variant_id}: "
                f"only {len(times_ms)} run(s); skipping"
            )
            return None

        final_times = times_ms[1:]
        final_answers = answers[1:]

        if len(times_ms) not in (4, 5):
            warnings.append(
                f"{competitor} query-type {query_type_id} variant {variant_id}: "
                f"expected 4 or 5 runs, got {len(times_ms)}; using all runs after warm-up"
            )

        used_times, outliers = remove_outliers_20pct(final_times)
        if not used_times:
            warnings.append(
                f"{competitor} query-type {query_type_id} variant {variant_id}: "
                "no usable times after filtering"
            )
            return None

        chosen_answer = next((ans for ans in final_answers if ans not in (None, "")), None)

        return VariantStats(
            variant_id=variant_id,
            raw_times_ms=final_times,
            used_times_ms=used_times,
            outlier_times_ms=outliers,
            mean_ms=st.mean(used_times),
            median_ms=st.median(used_times),
            rsd_pct=relative_stddev_pct(used_times),
            answer=chosen_answer,
        )

    expected_runs = competitor_total_runs(competitor)

    if len(times_ms) >= expected_runs:
        final_times = times_ms[1:expected_runs]
        final_answers = answers[1:expected_runs]
        if len(times_ms) > expected_runs:
            warnings.append(
                f"{competitor} query-type {query_type_id} variant {variant_id}: "
                f"expected {expected_runs} runs, got {len(times_ms)}; using occurrences 2..{expected_runs}"
            )
    elif len(times_ms) == expected_runs - 1:
        final_times = times_ms[:]
        final_answers = answers[:]
        warnings.append(
            f"{competitor} query-type {query_type_id} variant {variant_id}: "
            f"got {len(times_ms)} runs; assuming warm-up is already absent"
        )
    elif len(times_ms) >= 2:
        final_times = times_ms[1:]
        final_answers = answers[1:]
        warnings.append(
            f"{competitor} query-type {query_type_id} variant {variant_id}: "
            f"expected {expected_runs} runs, got {len(times_ms)}; using available runs after warm-up"
        )
    else:
        warnings.append(
            f"{competitor} query-type {query_type_id} variant {variant_id}: "
            f"only {len(times_ms)} run(s); skipping"
        )
        return None

    used_times, outliers = remove_outliers_20pct(final_times)
    if not used_times:
        warnings.append(
            f"{competitor} query-type {query_type_id} variant {variant_id}: "
            "no usable times after filtering"
        )
        return None

    chosen_answer = next((ans for ans in final_answers if ans not in (None, "")), None)

    return VariantStats(
        variant_id=variant_id,
        raw_times_ms=final_times,
        used_times_ms=used_times,
        outlier_times_ms=outliers,
        mean_ms=st.mean(used_times),
        median_ms=st.median(used_times),
        rsd_pct=relative_stddev_pct(used_times),
        answer=chosen_answer,
    )


def parse_larpq_type_dir(
    type_dir: Path,
    query_type_id: int,
    warnings: list[str],
) -> dict[int, VariantStats]:
    if not type_dir.exists():
        warnings.append(f"LARPQ query-type {query_type_id}: directory is missing: {type_dir}")
        return {}

    all_txt = type_dir / "all.txt"
    if all_txt.exists():
        grouped_times: dict[int, list[float]] = defaultdict(list)
        grouped_answers: dict[int, list[Optional[str]]] = defaultdict(list)

        with all_txt.open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, 1):
                line = raw_line.strip()
                if not line:
                    continue

                row = next(csv.reader([line]))
                if len(row) < 2:
                    warnings.append(
                        f"LARPQ query-type {query_type_id}: skip malformed line {line_no} in all.txt: {line!r}"
                    )
                    continue

                try:
                    variant_id = int(row[0].strip())
                    time_ms = float(row[1].strip())
                except ValueError:
                    warnings.append(
                        f"LARPQ query-type {query_type_id}: skip malformed line {line_no} in all.txt: {line!r}"
                    )
                    continue

                if len(row) >= 5:
                    rc_s = row[3].strip()
                    err_s = row[4].strip()
                    try:
                        rc = int(rc_s)
                    except ValueError:
                        warnings.append(
                            f"LARPQ query-type {query_type_id}: skip malformed line {line_no} in all.txt: {line!r}"
                        )
                        continue
                    if rc != 0 or err_s not in ("", '""'):
                        continue

                answer = parse_strict_integer_answer(row[2].strip()) if len(row) >= 3 else None
                grouped_times[variant_id].append(time_ms)
                grouped_answers[variant_id].append(answer)

        result: dict[int, VariantStats] = {}
        for variant_id in sorted(grouped_times):
            stats = finalize_variant_stats(
                "LARPQ",
                query_type_id,
                variant_id,
                grouped_times[variant_id],
                grouped_answers[variant_id],
                warnings,
            )
            if stats is not None:
                result[variant_id] = stats
        return result

    result: dict[int, VariantStats] = {}
    for variant_id in DEFAULT_VARIANT_IDS:
        file_path = type_dir / f"{variant_id}.txt"
        if not file_path.exists():
            continue

        times_ms: list[float] = []
        answers: list[Optional[str]] = []

        with file_path.open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, 1):
                line = raw_line.strip()
                if not line:
                    continue

                parts = line.split()
                if len(parts) < 1:
                    continue

                try:
                    time_ms = float(parts[0])
                except ValueError:
                    warnings.append(
                        f"LARPQ query-type {query_type_id} variant {variant_id}: "
                        f"skip malformed line {line_no} in {file_path.name}: {line!r}"
                    )
                    continue

                answer = parts[1] if len(parts) >= 2 else None
                times_ms.append(time_ms)
                answers.append(answer)

        stats = finalize_variant_stats(
            "LARPQ",
            query_type_id,
            variant_id,
            times_ms,
            answers,
            warnings,
        )
        if stats is not None:
            result[variant_id] = stats

    return result


def find_csv_file(type_dir: Path) -> Optional[Path]:
    exact = type_dir / f"{type_dir.name}.csv"
    if exact.exists():
        return exact

    csv_files = sorted(type_dir.glob("*.csv"))
    if csv_files:
        return csv_files[0]

    return None


def parse_csv_type_dir(
    type_dir: Path,
    competitor: str,
    query_type_id: int,
    warnings: list[str],
) -> dict[int, VariantStats]:
    if not type_dir.exists():
        warnings.append(f"{competitor} query-type {query_type_id}: directory is missing: {type_dir}")
        return {}

    csv_path = find_csv_file(type_dir)
    if csv_path is None:
        warnings.append(f"{competitor} query-type {query_type_id}: no CSV file found in {type_dir}")
        return {}

    grouped_times: dict[int, list[float]] = defaultdict(list)
    grouped_answers: dict[int, list[Optional[str]]] = defaultdict(list)
    errored_variants: set[int] = set()
    time_scale = competitor_time_scale_to_us(competitor)

    with csv_path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, 1):
            line = raw_line.strip()
            if not line:
                continue

            row = next(csv.reader([line]))
            if not row:
                continue

            try:
                variant_id = int(row[0].strip())
            except ValueError:
                warnings.append(
                    f"{competitor} query-type {query_type_id}: skip malformed line {line_no} in {csv_path.name}: {line!r}"
                )
                continue

            lowered = [cell.strip().lower() for cell in row]
            if any("errored" in cell or "error" == cell for cell in lowered):
                errored_variants.add(variant_id)
                continue

            if len(row) != 3:
                warnings.append(
                    f"{competitor} query-type {query_type_id}: skip malformed line {line_no} in {csv_path.name}: {line!r}"
                )
                continue

            try:
                time_ms = float(row[1].strip())
            except ValueError:
                warnings.append(
                    f"{competitor} query-type {query_type_id}: skip malformed line {line_no} in {csv_path.name}: {line!r}"
                )
                continue

            answer = parse_strict_integer_answer(row[2].strip())
            if answer is None:
                warnings.append(
                    f"{competitor} query-type {query_type_id}: skip malformed line {line_no} in {csv_path.name}: {line!r}"
                )
                continue
            grouped_times[variant_id].append(time_ms * time_scale)
            grouped_answers[variant_id].append(answer)

    result: dict[int, VariantStats] = {}
    variant_ids = sorted(set(grouped_times) | errored_variants)
    for variant_id in variant_ids:
        if variant_id in errored_variants and variant_id not in grouped_times:
            warnings.append(
                f"{competitor} query-type {query_type_id} variant {variant_id}: marked as errored"
            )
            continue

        stats = finalize_variant_stats(
            competitor,
            query_type_id,
            variant_id,
            grouped_times[variant_id],
            grouped_answers[variant_id],
            warnings,
        )
        if stats is not None:
            result[variant_id] = stats

    return result


def parse_falkor_flat_file(
    file_path: Path,
    query_type_id: int,
    warnings: list[str],
) -> dict[int, VariantStats]:
    if not file_path.exists():
        warnings.append(f"FalkorDB query-type {query_type_id}: file is missing: {file_path}")
        return {}

    grouped_times: dict[int, list[float]] = defaultdict(list)
    grouped_answers: dict[int, list[Optional[str]]] = defaultdict(list)
    errored_variants: set[int] = set()
    time_scale = competitor_time_scale_to_us("FalkorDB")

    with file_path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, 1):
            line = raw_line.strip()
            if not line:
                continue

            row = next(csv.reader([line]))
            if not row:
                continue

            try:
                variant_id = int(row[0].strip())
            except ValueError:
                warnings.append(
                    f"FalkorDB query-type {query_type_id}: skip malformed line {line_no} in {file_path.name}: {line!r}"
                )
                continue

            lowered = [cell.strip().lower() for cell in row]
            if any("errored" in cell or cell == "error" for cell in lowered):
                errored_variants.add(variant_id)
                continue

            if len(row) != 3:
                warnings.append(
                    f"FalkorDB query-type {query_type_id}: skip malformed line {line_no} in {file_path.name}: {line!r}"
                )
                continue

            try:
                time_ms = float(row[1].strip())
            except ValueError:
                warnings.append(
                    f"FalkorDB query-type {query_type_id}: skip malformed line {line_no} in {file_path.name}: {line!r}"
                )
                continue

            answer = parse_strict_integer_answer(row[2].strip())
            if answer is None:
                warnings.append(
                    f"FalkorDB query-type {query_type_id}: skip malformed line {line_no} in {file_path.name}: {line!r}"
                )
                continue

            grouped_times[variant_id].append(time_ms * time_scale)
            grouped_answers[variant_id].append(answer)

    result: dict[int, VariantStats] = {}
    variant_ids = sorted(set(grouped_times) | errored_variants)
    for variant_id in variant_ids:
        if variant_id in errored_variants and variant_id not in grouped_times:
            warnings.append(
                f"FalkorDB query-type {query_type_id} variant {variant_id}: marked as errored"
            )
            continue

        stats = finalize_variant_stats(
            "FalkorDB",
            query_type_id,
            variant_id,
            grouped_times[variant_id],
            grouped_answers[variant_id],
            warnings,
        )
        if stats is not None:
            result[variant_id] = stats

    return result


def parse_competitor_root(
    root: Optional[Path],
    competitor: str,
    query_type_ids: list[int],
) -> tuple[dict[int, dict[int, VariantStats]], list[str]]:
    warnings: list[str] = []
    stats_by_type: dict[int, dict[int, VariantStats]] = {}

    if root is None:
        warnings.append(f"{competitor}: root directory is not provided; all cells will be dashes")
        return stats_by_type, warnings

    if not root.exists():
        warnings.append(f"{competitor}: root directory does not exist: {root}")
        return stats_by_type, warnings

    for query_type_id in query_type_ids:
        type_dir = root / f"{query_type_id}.txt"
        if competitor == "LARPQ":
            stats_by_type[query_type_id] = parse_larpq_type_dir(type_dir, query_type_id, warnings)
        elif competitor == "FalkorDB" and type_dir.is_file():
            stats_by_type[query_type_id] = parse_falkor_flat_file(type_dir, query_type_id, warnings)
        else:
            stats_by_type[query_type_id] = parse_csv_type_dir(type_dir, competitor, query_type_id, warnings)

    return stats_by_type, warnings


def discover_query_type_ids(
    query_dir: Optional[Path],
    larpq_dir: Optional[Path],
    millenniumdb_dir: Optional[Path],
    falkordb_dir: Optional[Path],
) -> list[int]:
    found: set[int] = set()

    def collect_from_dir(root: Optional[Path]) -> None:
        if root is None or not root.exists():
            return
        for child in root.iterdir():
            if child.name.endswith(".txt"):
                stem = child.name[:-4]
                if stem.isdigit():
                    found.add(int(stem))

    if query_dir is not None and query_dir.exists():
        for path in query_dir.glob("*.txt"):
            if path.stem.isdigit():
                found.add(int(path.stem))

    collect_from_dir(larpq_dir)
    collect_from_dir(millenniumdb_dir)
    collect_from_dir(falkordb_dir)

    return sorted(found)


def query_type_mean(stats: dict[int, VariantStats]) -> Optional[float]:
    if not stats:
        return None
    return st.mean(item.mean_ms for item in stats.values())


def active_competitors(args: argparse.Namespace) -> list[str]:
    competitors = ["LARPQ", "MillenniumDB"]
    if args.falkordb_dir is not None:
        competitors.append("FalkorDB")
    return competitors


def build_summary_rows(
    query_type_ids: list[int],
    metas: dict[int, QueryTypeMeta],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
) -> list[list[str]]:
    rows: list[list[str]] = []

    for query_type_id in query_type_ids:
        row = [metas[query_type_id].label]
        numeric_values: dict[str, float] = {}

        for competitor in competitors:
            stats = stats_by_competitor.get(competitor, {}).get(query_type_id, {})
            mean_value = query_type_mean(stats)
            if mean_value is not None:
                numeric_values[competitor] = mean_value

        best_value = min(numeric_values.values()) if numeric_values else None

        for competitor in competitors:
            value = numeric_values.get(competitor)
            rendered = fmt_time_ms(value)
            if value is not None and best_value is not None and abs(value - best_value) <= 1e-9:
                rendered = rendered + " *"
            row.append(rendered)

        rows.append(row)

    return rows


def rpqbench_speedups_vs_larpq(
    query_type_ids: list[int],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitor: str,
) -> list[float]:
    if competitor == "LARPQ":
        return [1.0]

    result: list[float] = []
    for query_type_id in query_type_ids:
        larpq_val = query_type_mean(stats_by_competitor.get("LARPQ", {}).get(query_type_id, {}))
        other_val = query_type_mean(stats_by_competitor.get(competitor, {}).get(query_type_id, {}))
        if larpq_val is None or other_val is None or other_val <= 0:
            continue
        result.append(larpq_val / other_val)
    return result


def build_overall_stats_rows(
    query_type_ids: list[int],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
) -> list[list[str]]:
    totals: dict[str, Optional[float]] = {}
    means: dict[str, Optional[float]] = {}
    medians: dict[str, Optional[float]] = {}
    mean_speedups: dict[str, Optional[float]] = {}
    median_speedups: dict[str, Optional[float]] = {}

    for competitor in competitors:
        vals = []
        for query_type_id in query_type_ids:
            value = query_type_mean(stats_by_competitor.get(competitor, {}).get(query_type_id, {}))
            if value is not None:
                vals.append(value)

        if vals:
            totals[competitor] = sum(vals)
            means[competitor] = st.mean(vals)
            medians[competitor] = st.median(vals)
        else:
            totals[competitor] = None
            means[competitor] = None
            medians[competitor] = None

        sp = rpqbench_speedups_vs_larpq(query_type_ids, stats_by_competitor, competitor)
        if sp:
            mean_speedups[competitor] = st.mean(sp)
            median_speedups[competitor] = st.median(sp)
        else:
            mean_speedups[competitor] = None
            median_speedups[competitor] = None

    return [
        ["Total, ms"] + [fmt_time_ms(totals[c]) for c in competitors],
        ["Mean, ms"] + [fmt_time_ms(means[c]) for c in competitors],
        ["Median, ms"] + [fmt_time_ms(medians[c]) for c in competitors],
        ["Mean speedup"] + [fmt_ms(mean_speedups[c], 2) for c in competitors],
        ["Median speedup"] + [fmt_ms(median_speedups[c], 2) for c in competitors],
    ]


def query_type_ids_common_to_all(
    query_type_ids: list[int],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
) -> list[int]:
    result: list[int] = []
    for query_type_id in query_type_ids:
        if all(
            query_type_mean(stats_by_competitor.get(competitor, {}).get(query_type_id, {})) is not None
            for competitor in competitors
        ):
            result.append(query_type_id)
    return result


def query_type_ids_missing_on_falkor(
    query_type_ids: list[int],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
) -> list[int]:
    result: list[int] = []
    for query_type_id in query_type_ids:
        larpq_val = query_type_mean(stats_by_competitor.get("LARPQ", {}).get(query_type_id, {}))
        mdb_val = query_type_mean(stats_by_competitor.get("MillenniumDB", {}).get(query_type_id, {}))
        falkor_val = query_type_mean(stats_by_competitor.get("FalkorDB", {}).get(query_type_id, {}))
        if larpq_val is not None and mdb_val is not None and falkor_val is None:
            result.append(query_type_id)
    return result


def render_text_table(headers: list[str], rows: list[list[str]], title: Optional[str] = None) -> str:
    table = [headers] + rows
    widths = [
        max(len(str(row[i])) for row in table)
        for i in range(len(headers))
    ]

    def render_row(row: list[str]) -> str:
        return " | ".join(
            str(cell).ljust(widths[i])
            for i, cell in enumerate(row)
        )

    sep = "-+-".join("-" * widths[i] for i in range(len(headers)))
    out: list[str] = []

    if title:
        out.append(title)
        out.append("=" * len(title))
        out.append("")

    out.append("All execution times below are in milliseconds (ms).")
    out.append("")

    out.append(render_row(headers))
    out.append(sep)
    out.extend(render_row(row) for row in rows)
    out.append("")
    out.append("* = best result in the row")
    return "\n".join(out) + "\n"


def build_detail_rows(
    query_type_id: int,
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    variant_ids: list[int],
    competitors: list[str],
) -> list[list[str]]:
    rows: list[list[str]] = []

    for variant_id in variant_ids:
        row = [str(variant_id)]
        numeric_values: dict[str, float] = {}

        for competitor in competitors:
            stats = stats_by_competitor.get(competitor, {}).get(query_type_id, {})
            variant_stats = stats.get(variant_id)
            if variant_stats is not None:
                numeric_values[competitor] = variant_stats.mean_ms

        best_value = min(numeric_values.values()) if numeric_values else None

        for competitor in competitors:
            value = numeric_values.get(competitor)
            rendered = fmt_time_ms(value)
            if value is not None and best_value is not None and abs(value - best_value) <= 1e-9:
                rendered = rendered + " *"
            row.append(rendered)

        rows.append(row)

    return rows


def classify_larpq_variant_groups(
    query_type_ids: list[int],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
) -> tuple[dict[int, list[int]], dict[int, list[int]]]:
    better_by_type: dict[int, list[int]] = {}
    worse_by_type: dict[int, list[int]] = {}

    for query_type_id in query_type_ids:
        variant_ids = sorted(
            set().union(
                *[
                    set(stats_by_competitor.get(competitor, {}).get(query_type_id, {}))
                    for competitor in competitors
                ]
            )
        )

        better: list[int] = []
        worse: list[int] = []

        for variant_id in variant_ids:
            larpq_stats = stats_by_competitor.get("LARPQ", {}).get(query_type_id, {}).get(variant_id)
            if larpq_stats is None:
                continue

            values: dict[str, float] = {}
            for competitor in competitors:
                variant_stats = stats_by_competitor.get(competitor, {}).get(query_type_id, {}).get(variant_id)
                if variant_stats is not None:
                    values[competitor] = variant_stats.mean_ms

            if len(values) <= 1:
                continue

            best_value = min(values.values())
            if abs(values["LARPQ"] - best_value) <= 1e-9:
                better.append(variant_id)
            else:
                worse.append(variant_id)

        better_by_type[query_type_id] = better
        worse_by_type[query_type_id] = worse

    return better_by_type, worse_by_type


def query_type_mean_for_variants(
    stats: dict[int, VariantStats],
    variant_ids: list[int],
) -> Optional[float]:
    values = [stats[variant_id].mean_ms for variant_id in variant_ids if variant_id in stats]
    if not values:
        return None
    return st.mean(values)


def build_group_summary_rows(
    query_type_ids: list[int],
    metas: dict[int, QueryTypeMeta],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
    variant_ids_by_type: dict[int, list[int]],
) -> list[list[str]]:
    rows: list[list[str]] = []

    for query_type_id in query_type_ids:
        row = [metas[query_type_id].label]
        selected_variant_ids = variant_ids_by_type.get(query_type_id, [])
        numeric_values: dict[str, float] = {}

        for competitor in competitors:
            stats = stats_by_competitor.get(competitor, {}).get(query_type_id, {})
            mean_value = query_type_mean_for_variants(stats, selected_variant_ids)
            if mean_value is not None:
                numeric_values[competitor] = mean_value

        if not numeric_values:
            row.extend("-" for _ in competitors)
            rows.append(row)
            continue

        best_value = min(numeric_values.values())
        for competitor in competitors:
            value = numeric_values.get(competitor)
            rendered = fmt_time_ms(value)
            if value is not None and abs(value - best_value) <= 1e-9:
                rendered = rendered + " *"
            row.append(rendered)

        rows.append(row)

    return rows


def group_speedups_vs_larpq(
    query_type_ids: list[int],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
    competitor: str,
    variant_ids_by_type: dict[int, list[int]],
) -> list[float]:
    if competitor == "LARPQ":
        return [1.0]

    result: list[float] = []
    for query_type_id in query_type_ids:
        selected_variant_ids = variant_ids_by_type.get(query_type_id, [])
        if not selected_variant_ids:
            continue

        larpq_val = query_type_mean_for_variants(
            stats_by_competitor.get("LARPQ", {}).get(query_type_id, {}),
            selected_variant_ids,
        )
        other_val = query_type_mean_for_variants(
            stats_by_competitor.get(competitor, {}).get(query_type_id, {}),
            selected_variant_ids,
        )
        if larpq_val is None or other_val is None or other_val <= 0:
            continue
        result.append(larpq_val / other_val)
    return result


def build_group_overall_stats_rows(
    query_type_ids: list[int],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
    variant_ids_by_type: dict[int, list[int]],
) -> list[list[str]]:
    totals: dict[str, Optional[float]] = {}
    means: dict[str, Optional[float]] = {}
    medians: dict[str, Optional[float]] = {}
    mean_speedups: dict[str, Optional[float]] = {}
    median_speedups: dict[str, Optional[float]] = {}

    for competitor in competitors:
        vals: list[float] = []
        for query_type_id in query_type_ids:
            selected_variant_ids = variant_ids_by_type.get(query_type_id, [])
            if not selected_variant_ids:
                continue
            value = query_type_mean_for_variants(
                stats_by_competitor.get(competitor, {}).get(query_type_id, {}),
                selected_variant_ids,
            )
            if value is not None:
                vals.append(value)

        if vals:
            totals[competitor] = sum(vals)
            means[competitor] = st.mean(vals)
            medians[competitor] = st.median(vals)
        else:
            totals[competitor] = None
            means[competitor] = None
            medians[competitor] = None

        sp = group_speedups_vs_larpq(
            query_type_ids,
            stats_by_competitor,
            competitors,
            competitor,
            variant_ids_by_type,
        )
        if sp:
            mean_speedups[competitor] = st.mean(sp)
            median_speedups[competitor] = st.median(sp)
        else:
            mean_speedups[competitor] = None
            median_speedups[competitor] = None

    return [
        ["Total, ms"] + [fmt_time_ms(totals[c]) for c in competitors],
        ["Mean, ms"] + [fmt_time_ms(means[c]) for c in competitors],
        ["Median, ms"] + [fmt_time_ms(medians[c]) for c in competitors],
        ["Mean speedup"] + [fmt_ms(mean_speedups[c], 2) for c in competitors],
        ["Median speedup"] + [fmt_ms(median_speedups[c], 2) for c in competitors],
    ]


def write_summary_csv(
    path: Path,
    query_type_ids: list[int],
    metas: dict[int, QueryTypeMeta],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
) -> None:
    headers = ["query_type_id", "query_label", "raw_expression"]
    for competitor in competitors:
        headers.extend(
            [
                f"{competitor}_mean_ms",
                f"{competitor}_variants_present",
            ]
        )

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)

        for query_type_id in query_type_ids:
            meta = metas[query_type_id]
            row: list[str] = [
                str(query_type_id),
                meta.label,
                meta.raw_expression,
            ]

            for competitor in competitors:
                stats = stats_by_competitor.get(competitor, {}).get(query_type_id, {})
                mean_value = query_type_mean(stats)
                row.append("" if mean_value is None else f"{us_to_ms(mean_value):.6f}")
                row.append(str(len(stats)))

            writer.writerow(row)


def write_variant_csv(
    path: Path,
    query_type_ids: list[int],
    metas: dict[int, QueryTypeMeta],
    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]],
    competitors: list[str],
) -> None:
    headers = [
        "query_type_id",
        "query_label",
        "variant_id",
        "query_text",
    ]
    for competitor in competitors:
        headers.extend(
            [
                f"{competitor}_mean_ms",
                f"{competitor}_median_ms",
                f"{competitor}_rsd_pct",
                f"{competitor}_used_runs_ms",
                f"{competitor}_outliers_ms",
            ]
        )

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)

        for query_type_id in query_type_ids:
            meta = metas[query_type_id]
            variant_ids = sorted(
                set(DEFAULT_VARIANT_IDS)
                | set(meta.variant_queries)
                | set().union(
                    *[
                        set(stats_by_competitor.get(competitor, {}).get(query_type_id, {}))
                        for competitor in competitors
                    ]
                )
            )

            for variant_id in variant_ids:
                row: list[str] = [
                    str(query_type_id),
                    meta.label,
                    str(variant_id),
                    meta.variant_queries.get(variant_id, ""),
                ]

                for competitor in competitors:
                    stats = stats_by_competitor.get(competitor, {}).get(query_type_id, {}).get(variant_id)
                    if stats is None:
                        row.extend(["", "", "", "", ""])
                    else:
                        row.extend(
                            [
                                f"{us_to_ms(stats.mean_ms):.6f}",
                                f"{us_to_ms(stats.median_ms):.6f}",
                                f"{stats.rsd_pct:.6f}",
                                fmt_time_list_ms(stats.used_times_ms),
                                fmt_time_list_ms(stats.outlier_times_ms),
                            ]
                        )

                writer.writerow(row)


def write_warnings(path: Path, warnings: list[str]) -> None:
    if not warnings:
        path.write_text("No warnings.\n", encoding="utf-8")
        return
    path.write_text("\n".join(warnings) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze RPQBench results for LARPQ, MillenniumDB and FalkorDB. "
            "Each root directory must contain query-type subdirectories like 1.txt, 2.txt, ..., 20.txt."
        )
    )

    parser.add_argument("--larpq-dir", type=Path, required=True, help="LARPQ root results directory")
    parser.add_argument(
        "--millenniumdb-dir",
        type=Path,
        required=True,
        help="MillenniumDB root results directory",
    )
    parser.add_argument(
        "--falkordb-dir",
        type=Path,
        default=None,
        help="FalkorDB root results directory. Optional; missing values become dashes.",
    )
    parser.add_argument(
        "--query-dir",
        type=Path,
        default=Path("mdb/sparql-adbis-queries"),
        help="Directory with RPQBench query templates",
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        type=Path,
        default=Path("rpqbench_analysis"),
        help="Output directory",
    )
    parser.add_argument(
        "--prefix",
        default="rpqbench",
        help="Prefix for generated files",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    query_type_ids = discover_query_type_ids(
        args.query_dir,
        args.larpq_dir,
        args.millenniumdb_dir,
        args.falkordb_dir,
    )
    if not query_type_ids:
        print("No query types discovered.", file=sys.stderr)
        return 1

    metas = {
        query_type_id: load_query_type_meta(args.query_dir, query_type_id)
        for query_type_id in query_type_ids
    }

    stats_by_competitor: dict[str, dict[int, dict[int, VariantStats]]] = {}
    all_warnings: list[str] = []

    competitors = active_competitors(args)

    roots = {
        "LARPQ": args.larpq_dir,
        "MillenniumDB": args.millenniumdb_dir,
    }
    if "FalkorDB" in competitors:
        roots["FalkorDB"] = args.falkordb_dir

    for competitor, root in roots.items():
        stats, warnings = parse_competitor_root(root, competitor, query_type_ids)
        stats_by_competitor[competitor] = stats
        all_warnings.extend(warnings)

    overall_sections: list[str] = []

    overall_rows = build_overall_stats_rows(query_type_ids, stats_by_competitor, competitors)
    overall_sections.append(
        render_text_table(["metric"] + competitors, overall_rows, title="RPQBench Overall Stats")
    )

    if "FalkorDB" in competitors:
        common_query_type_ids = query_type_ids_common_to_all(query_type_ids, stats_by_competitor, competitors)
        if common_query_type_ids:
            overall_sections.append(
                render_text_table(
                    ["metric"] + competitors,
                    build_overall_stats_rows(common_query_type_ids, stats_by_competitor, competitors),
                    title="RPQBench Overall Stats: Common To All",
                )
            )

        missing_on_falkor_query_type_ids = query_type_ids_missing_on_falkor(query_type_ids, stats_by_competitor)
        if missing_on_falkor_query_type_ids:
            overall_sections.append(
                render_text_table(
                    ["metric", "LARPQ", "MillenniumDB"],
                    build_overall_stats_rows(
                        missing_on_falkor_query_type_ids,
                        stats_by_competitor,
                        ["LARPQ", "MillenniumDB"],
                    ),
                    title="RPQBench Overall Stats: Missing On FalkorDB",
                )
            )

    overall_text = "\n".join(overall_sections)

    summary_rows = build_summary_rows(query_type_ids, metas, stats_by_competitor, competitors)
    competitor_headers = ["Query"] + [f"{comp}, ms" for comp in competitors]
    summary_text = overall_text + "\n" + render_text_table(
        competitor_headers,
        summary_rows,
        title="RPQBench Summary By Query Type",
    )

    summary_text_path = args.out_dir / f"{args.prefix}_summary.txt"
    summary_text_path.write_text(summary_text, encoding="utf-8")

    larpq_better_by_type, larpq_worse_by_type = classify_larpq_variant_groups(
        query_type_ids,
        stats_by_competitor,
        competitors,
    )

    better_summary_text = render_text_table(
        competitor_headers,
        build_group_summary_rows(
            query_type_ids,
            metas,
            stats_by_competitor,
            competitors,
            larpq_better_by_type,
        ),
        title="RPQBench Summary By Query Type: LARPQ Better",
    )
    better_summary_path = args.out_dir / f"{args.prefix}_larpq_better_by_query_type.txt"
    better_summary_path.write_text(better_summary_text, encoding="utf-8")

    better_overall_text = render_text_table(
        ["metric"] + competitors,
        build_group_overall_stats_rows(
            query_type_ids,
            stats_by_competitor,
            competitors,
            larpq_better_by_type,
        ),
        title="RPQBench Overall Stats: LARPQ Better",
    )
    better_overall_path = args.out_dir / f"{args.prefix}_larpq_better_overall.txt"
    better_overall_path.write_text(better_overall_text, encoding="utf-8")

    worse_summary_text = render_text_table(
        competitor_headers,
        build_group_summary_rows(
            query_type_ids,
            metas,
            stats_by_competitor,
            competitors,
            larpq_worse_by_type,
        ),
        title="RPQBench Summary By Query Type: LARPQ Worse",
    )
    worse_summary_path = args.out_dir / f"{args.prefix}_larpq_worse_by_query_type.txt"
    worse_summary_path.write_text(worse_summary_text, encoding="utf-8")

    worse_overall_text = render_text_table(
        ["metric"] + competitors,
        build_group_overall_stats_rows(
            query_type_ids,
            stats_by_competitor,
            competitors,
            larpq_worse_by_type,
        ),
        title="RPQBench Overall Stats: LARPQ Worse",
    )
    worse_overall_path = args.out_dir / f"{args.prefix}_larpq_worse_overall.txt"
    worse_overall_path.write_text(worse_overall_text, encoding="utf-8")

    details_dir = args.out_dir / f"{args.prefix}_details"
    details_dir.mkdir(parents=True, exist_ok=True)

    combined_parts: list[str] = []
    for query_type_id in query_type_ids:
        meta = metas[query_type_id]
        variant_ids = sorted(
            set(DEFAULT_VARIANT_IDS)
            | set(meta.variant_queries)
            | set().union(
                *[
                    set(stats_by_competitor.get(competitor, {}).get(query_type_id, {}))
                    for competitor in competitors
                ]
            )
        )
        detail_rows = build_detail_rows(query_type_id, stats_by_competitor, variant_ids, competitors)
        detail_text = render_text_table(
            ["Variant"] + [f"{comp}, ms" for comp in competitors],
            detail_rows,
            title=f"Query type {query_type_id}: {meta.label}",
        )

        file_name = f"{query_type_id:02d}_{slugify_label(meta.label)}.txt"
        detail_path = details_dir / file_name
        detail_path.write_text(detail_text, encoding="utf-8")

        combined_parts.append(detail_text)
        combined_parts.append("\n")

    combined_details_path = args.out_dir / f"{args.prefix}_details_all.txt"
    combined_details_path.write_text("".join(combined_parts), encoding="utf-8")

    warnings_path = args.out_dir / f"{args.prefix}_warnings.log"
    write_warnings(warnings_path, all_warnings)

    print("Generated files:")
    print(f"  {summary_text_path}")
    print(f"  {better_summary_path}")
    print(f"  {better_overall_path}")
    print(f"  {worse_summary_path}")
    print(f"  {worse_overall_path}")
    print(f"  {combined_details_path}")
    print(f"  {details_dir}")
    print(f"  {warnings_path}")
    print("\nSummary preview:\n")
    print(summary_text)

    if all_warnings:
        print(f"Warnings: {len(all_warnings)} entries, see {warnings_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
