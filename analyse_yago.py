#!/usr/bin/env python3

import argparse
import csv
import math
import statistics as st
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# ============================================================
# Data model
# ============================================================

@dataclass(frozen=True)
class Measurement:
    query_id: int
    time_ms: float
    answer: str
    line_no: int


@dataclass(frozen=True)
class QueryStats:
    query_id: int
    raw_times_ms: list[float]
    used_times_ms: list[float]
    outlier_times_ms: list[float]
    mean_ms: float
    answer: str


COMPETITORS = ["LARPQ", "MillenniumDB"]


# ============================================================
# Parsing result files
# ============================================================

def parse_answer(s: str) -> str:
    s = s.strip()

    if not s:
        raise ValueError("empty answer")

    if s.lstrip("-").isdigit():
        return s

    raise ValueError(f"non-integer answer: {s!r}")


def parse_measurement_line(line: str, line_no: int) -> Measurement:
    row = next(csv.reader([line]))

    if len(row) < 3:
        raise ValueError(f"expected at least 3 CSV fields, got {len(row)}")
    if len(row) not in (3, 5):
        raise ValueError(f"expected either 3 or 5 CSV fields, got {len(row)}")

    qid_s, time_s, ans_s = [x.strip() for x in row[:3]]

    if len(row) >= 5:
        rc_s = row[3].strip()
        err_s = row[4].strip()

        try:
            rc = int(rc_s)
        except ValueError as e:
            raise ValueError(f"bad return code: {rc_s!r}") from e

        if rc != 0 or err_s not in ("", '""'):
            raise ValueError(f"execution failed: return_code={rc}, error={err_s!r}")

    query_id = int(qid_s)
    time_ms = float(time_s)
    answer = parse_answer(ans_s)

    if not math.isfinite(time_ms):
        raise ValueError(f"non-finite time: {time_ms}")

    if time_ms < 0:
        raise ValueError(f"negative time: {time_ms}")

    return Measurement(
        query_id=query_id,
        time_ms=time_ms,
        answer=answer,
        line_no=line_no,
    )


def parse_result_file(
    path: Path,
    competitor: str,
    strict: bool,
    filter_outliers: bool,
) -> tuple[dict[int, QueryStats], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"{competitor}: file does not exist: {path}")

    occurrences: dict[int, list[Measurement]] = defaultdict(list)
    warnings: list[str] = []

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()

            if not line:
                continue

            try:
                m = parse_measurement_line(line, line_no)
            except Exception as e:
                msg = f"{competitor}: skip malformed line {line_no}: {e}; line={line!r}"

                if strict:
                    raise ValueError(msg) from e

                warnings.append(msg)
                continue

            occurrences[m.query_id].append(m)

    result: dict[int, QueryStats] = {}

    for query_id, measurements in sorted(occurrences.items()):
        if len(measurements) < 2:
            warnings.append(
                f"{competitor}: query {query_id}: only {len(measurements)} run(s), "
                f"nothing remains after warm-up skip"
            )
            continue

        if len(measurements) < 5:
            warnings.append(
                f"{competitor}: query {query_id}: expected 5 runs, got {len(measurements)}; "
                f"will use available runs after warm-up"
            )

        if len(measurements) > 5:
            warnings.append(
                f"{competitor}: query {query_id}: expected 5 runs, got {len(measurements)}; "
                f"will use occurrences 2..5 only"
            )

        # Первый прогон — warm-up.
        # В финальный зачет идут следующие 4.
        final_measurements = measurements[1:5]

        raw_times = [m.time_ms for m in final_measurements]

        if filter_outliers:
            used_times, outliers = remove_outliers_20pct(raw_times)
        else:
            used_times, outliers = raw_times[:], []

        if not used_times:
            warnings.append(
                f"{competitor}: query {query_id}: no usable runs after filtering"
            )
            continue

        answers = [m.answer for m in final_measurements]
        answer = most_common(answers)

        if len(set(answers)) != 1:
            warnings.append(
                f"{competitor}: query {query_id}: inconsistent answers after warm-up: "
                f"{answers}; using most common answer={answer}"
            )

        result[query_id] = QueryStats(
            query_id=query_id,
            raw_times_ms=raw_times,
            used_times_ms=used_times,
            outlier_times_ms=outliers,
            mean_ms=st.mean(used_times),
            answer=answer,
        )

    return result, warnings


def most_common(xs: list[str]) -> str:
    counts: dict[str, int] = defaultdict(int)

    for x in xs:
        counts[x] += 1

    return max(counts.items(), key=lambda kv: kv[1])[0]


def remove_outliers_20pct(values: list[float]) -> tuple[list[float], list[float]]:
    """
    Выброс: значение отличается больше чем на 20% от медианы остальных значений.

    Для 4 запусков это нормальная эвристика:
    - один сильно выбившийся запуск будет удален;
    - если фильтр оставляет меньше двух значений, откатываемся к исходным значениям.
    """
    if len(values) < 3:
        return values[:], []

    used: list[float] = []
    outliers: list[float] = []

    for i, v in enumerate(values):
        others = values[:i] + values[i + 1:]
        ref = st.median(others)

        if ref == 0:
            is_outlier = v != 0
        else:
            is_outlier = abs(v - ref) / abs(ref) > 0.20

        if is_outlier:
            outliers.append(v)
        else:
            used.append(v)

    if len(used) < 2:
        return values[:], []

    return used, outliers


# ============================================================
# Parsing query file and minimizing regexes
# ============================================================

def label_name(index: int) -> str:
    """
    0 -> A
    1 -> B
    ...
    25 -> Z
    26 -> AA
    27 -> AB
    """
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    index += 1

    result = ""

    while index > 0:
        index -= 1
        result = alphabet[index % 26] + result
        index //= 26

    return result


def parse_queries(path: Path) -> tuple[dict[int, str], dict[str, str]]:
    """
    Вход:
        1,?x <isMarriedTo>/<livesIn>/(<isLocatedIn>)+/(<dealsWith>)+ <Argentina>

    Извлекается только регулярка:
        <isMarriedTo>/<livesIn>/(<isLocatedIn>)+/(<dealsWith>)+

    Потом предикаты заменяются на глобальные метки:
        A/B/(C)+/(D)+
    """
    if not path.exists():
        raise FileNotFoundError(f"queries file does not exist: {path}")

    label_by_predicate: dict[str, str] = {}
    minimized_by_qid: dict[int, str] = {}

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()

            if not line:
                continue

            try:
                qid_s, query = line.split(",", maxsplit=1)
                query_id = int(qid_s.strip())
            except Exception as e:
                raise ValueError(
                    f"bad query line {line_no}: expected 'query_id,query', got {line!r}"
                ) from e

            regex_body = extract_regex_body(query)
            minimized = minimize_regex(regex_body, label_by_predicate)

            minimized_by_qid[query_id] = minimized

    return minimized_by_qid, label_by_predicate


def extract_regex_body(query: str) -> str:
    """
    Берет середину запроса между стартовой и финальной вершиной.

    Было:
        ?x <a>/<b> <C>

    Стало:
        <a>/<b>
    """
    parts = query.strip().split()

    if len(parts) < 3:
        raise ValueError(f"cannot extract regex from query: {query!r}")

    return " ".join(parts[1:-1]).strip()


def minimize_regex(regex_body: str, label_by_predicate: dict[str, str]) -> str:
    result: list[str] = []
    i = 0

    while i < len(regex_body):
        if regex_body[i] != "<":
            result.append(regex_body[i])
            i += 1
            continue

        j = regex_body.find(">", i + 1)

        if j == -1:
            raise ValueError(f"bad predicate token in regex: {regex_body!r}")

        predicate = regex_body[i + 1:j]

        if predicate not in label_by_predicate:
            label_by_predicate[predicate] = label_name(len(label_by_predicate))

        result.append(label_by_predicate[predicate])
        i = j + 1

    return "".join(result)


# ============================================================
# Output
# ============================================================

def fmt_time(x: Optional[float], digits: int = 3) -> str:
    if x is None:
        return "-"

    if not math.isfinite(x):
        return "-"

    return f"{x:,.{digits}f}".replace(",", " ")


def us_to_ms(x: float) -> float:
    return x / 1000.0


def fmt_time_ms(x: Optional[float], digits: int = 3) -> str:
    if x is None:
        return "-"
    if not math.isfinite(x):
        return "-"
    return fmt_time(us_to_ms(x), digits)


def fmt_time_list_ms(values: list[float]) -> str:
    if not values:
        return "-"
    return ";".join(f"{us_to_ms(x):.6f}" for x in values)


def render_text_table(headers: list[str], rows: list[list[str]]) -> str:
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

    out = [render_row(headers), sep]
    out.extend(render_row(row) for row in rows)

    return "\n".join(out)


def common_query_ids(
    minimized_queries: dict[int, str],
    larpq_stats: dict[int, QueryStats],
    mdb_stats: dict[int, QueryStats],
) -> list[int]:
    return [
        query_id
        for query_id in sorted(minimized_queries)
        if query_id in larpq_stats and query_id in mdb_stats
    ]


def build_summary_rows(
    query_ids: list[int],
    larpq_stats: dict[int, QueryStats],
    mdb_stats: dict[int, QueryStats],
) -> list[list[str]]:
    larpq_vals = [larpq_stats[qid].mean_ms for qid in query_ids]
    mdb_vals = [mdb_stats[qid].mean_ms for qid in query_ids]

    def speedups() -> list[float]:
        values: list[float] = []
        for qid in query_ids:
            larpq = larpq_stats[qid].mean_ms
            mdb = mdb_stats[qid].mean_ms
            if mdb > 0:
                values.append(larpq / mdb)
        return values

    mdb_speedups = speedups()

    return [
        ["Total, ms", fmt_time_ms(sum(larpq_vals)), fmt_time_ms(sum(mdb_vals))],
        ["Mean, ms", fmt_time_ms(st.mean(larpq_vals)), fmt_time_ms(st.mean(mdb_vals))],
        ["Median, ms", fmt_time_ms(st.median(larpq_vals)), fmt_time_ms(st.median(mdb_vals))],
        ["Mean speedup", "1.00", fmt_time(st.mean(mdb_speedups), 2) if mdb_speedups else "-"],
        ["Median speedup", "1.00", fmt_time(st.median(mdb_speedups), 2) if mdb_speedups else "-"],
    ]


def build_rows(
    minimized_queries: dict[int, str],
    larpq_stats: dict[int, QueryStats],
    mdb_stats: dict[int, QueryStats],
) -> list[list[str]]:
    rows: list[list[str]] = []

    for query_id in sorted(minimized_queries):
        larpq = larpq_stats.get(query_id)
        mdb = mdb_stats.get(query_id)

        rows.append([
            minimized_queries[query_id],
            fmt_time_ms(larpq.mean_ms if larpq else None),
            fmt_time_ms(mdb.mean_ms if mdb else None),
        ])

    return rows


def write_csv(
    path: Path,
    minimized_queries: dict[int, str],
    larpq_stats: dict[int, QueryStats],
    mdb_stats: dict[int, QueryStats],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            "query_id",
            "query",
            "LARPQ_ms",
            "MillenniumDB_ms",
            "LARPQ_used_runs_ms",
            "MillenniumDB_used_runs_ms",
            "LARPQ_outliers_ms",
            "MillenniumDB_outliers_ms",
            "LARPQ_answer",
            "MillenniumDB_answer",
        ])

        for query_id in sorted(minimized_queries):
            larpq = larpq_stats.get(query_id)
            mdb = mdb_stats.get(query_id)

            writer.writerow([
                query_id,
                minimized_queries[query_id],
                f"{us_to_ms(larpq.mean_ms):.6f}" if larpq else "-",
                f"{us_to_ms(mdb.mean_ms):.6f}" if mdb else "-",
                fmt_time_list_ms(larpq.used_times_ms) if larpq else "-",
                fmt_time_list_ms(mdb.used_times_ms) if mdb else "-",
                fmt_time_list_ms(larpq.outlier_times_ms) if larpq else "-",
                fmt_time_list_ms(mdb.outlier_times_ms) if mdb else "-",
                larpq.answer if larpq else "-",
                mdb.answer if mdb else "-",
            ])


def write_label_map(path: Path, label_by_predicate: dict[str, str]) -> None:
    rows = sorted(
        ((label, predicate) for predicate, label in label_by_predicate.items()),
        key=lambda x: (len(x[0]), x[0]),
    )

    with path.open("w", encoding="utf-8") as f:
        f.write("label | predicate\n")
        f.write("------+----------------\n")

        for label, predicate in rows:
            f.write(f"{label:<5} | {predicate}\n")


def write_warnings(path: Path, warnings: list[str]) -> None:
    if not warnings:
        path.write_text("No warnings.\n", encoding="utf-8")
        return

    path.write_text("\n".join(warnings) + "\n", encoding="utf-8")


# ============================================================
# Main
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Analyze YAGO benchmark results for LARPQ and MillenniumDB. "
            "Input result format: query_id,time_in_source_units,answer. "
            "Output times are reported in milliseconds (ms). "
            "Each result file contains 5 runs; first run is skipped."
        )
    )

    p.add_argument("larpq", type=Path, help="LARPQ result file")
    p.add_argument("millenniumdb", type=Path, help="MillenniumDB result file")
    p.add_argument("queries", type=Path, help="YAGO queries file")

    p.add_argument(
        "-o",
        "--out-dir",
        type=Path,
        default=Path("yago_analysis"),
        help="Output directory",
    )

    p.add_argument(
        "--prefix",
        default="yago",
        help="Output file prefix",
    )

    p.add_argument(
        "--strict",
        action="store_true",
        help="Fail on malformed result lines instead of skipping them",
    )

    p.add_argument(
        "--no-outlier-filter",
        action="store_true",
        help="Do not remove >20%% outliers before averaging",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    filter_outliers = not args.no_outlier_filter

    minimized_queries, label_by_predicate = parse_queries(args.queries)

    larpq_stats, larpq_warnings = parse_result_file(
        args.larpq,
        competitor="LARPQ",
        strict=args.strict,
        filter_outliers=filter_outliers,
    )

    mdb_stats, mdb_warnings = parse_result_file(
        args.millenniumdb,
        competitor="MillenniumDB",
        strict=args.strict,
        filter_outliers=filter_outliers,
    )

    rows = build_rows(
        minimized_queries=minimized_queries,
        larpq_stats=larpq_stats,
        mdb_stats=mdb_stats,
    )

    common_ids = common_query_ids(
        minimized_queries=minimized_queries,
        larpq_stats=larpq_stats,
        mdb_stats=mdb_stats,
    )

    summary_rows = build_summary_rows(
        query_ids=common_ids,
        larpq_stats=larpq_stats,
        mdb_stats=mdb_stats,
    )

    summary_table = render_text_table(
        headers=["metric", "LARPQ", "MillenniumDB"],
        rows=summary_rows,
    )

    table = render_text_table(
        headers=["query", "LARPQ, ms", "MillenniumDB, ms"],
        rows=rows,
    )

    summary_path = args.out_dir / f"{args.prefix}_summary.txt"
    table_path = args.out_dir / f"{args.prefix}_table.txt"
    labels_path = args.out_dir / f"{args.prefix}_labels.txt"
    warnings_path = args.out_dir / f"{args.prefix}_warnings.log"

    summary_text = (
        "Common successful queries\n"
        "=========================\n\n"
        "All execution times below are in milliseconds (ms).\n\n"
        f"Query ids count: {len(common_ids)}\n\n"
        f"{summary_table}\n"
    )
    summary_path.write_text(summary_text, encoding="utf-8")

    table_path.write_text(table + "\n", encoding="utf-8")

    write_label_map(labels_path, label_by_predicate)
    write_warnings(warnings_path, larpq_warnings + mdb_warnings)

    print(summary_text)
    print(table)

    print("\nOutput files:")
    print(f"  {summary_path}")
    print(f"  {table_path}")
    print(f"  {labels_path}")
    print(f"  {warnings_path}")

    warnings_count = len(larpq_warnings) + len(mdb_warnings)

    if warnings_count:
        print(f"\nWarnings: {warnings_count}, see {warnings_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
