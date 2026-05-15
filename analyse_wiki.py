#!/usr/bin/env python3

import argparse
import csv
import math
import statistics as st
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# ============================================================
# PLACEHOLDERS: сюда руками вставить номера запросов
# ============================================================

# Запросы, которые успешно выполнились у всех конкурентов.
# Пример: COMMON_SUCCESS_QUERY_IDS = [1, 2, 5, 6, 7, 8]
COMMON_SUCCESS_QUERY_IDS = [
    1, 2, 3, 4, 5, 6, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 39, 40, 41, 42,
    43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
    61, 64, 65, 66, 68, 69, 70, 71, 73, 74, 75, 76, 77, 78, 79, 80, 82, 83,
    84, 85, 86, 87, 88, 89, 90, 91, 92, 96, 99, 100, 103, 106, 107, 108, 109,
    110, 111, 112, 113, 125, 126, 127, 141, 145, 146, 147, 149, 150, 158, 159,
    160, 161, 162, 163, 164, 165, 166, 167, 168, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 190, 191, 192, 193, 194, 200, 203, 204, 205, 206,
    210, 217, 218, 219, 220, 221, 223, 224, 225, 226, 227, 228, 229, 230, 231,
    233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247,
    248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262,
    263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277,
    278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 291, 292, 293,
    294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 309, 310,
    311, 312, 313, 314, 315, 316, 318, 323, 324, 325, 326, 327, 328, 329, 330,
    331, 332, 333, 334, 335, 336, 337, 340, 342, 343, 344, 345, 348, 350, 351,
    353, 354, 355, 356, 357, 359, 360, 361, 362, 365, 367, 368, 370, 371, 372,
    373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387,
    388, 389, 390, 391, 393, 394, 396, 397, 398, 399, 400, 401, 402, 403, 404,
    407, 409, 410, 412, 414, 415, 416, 417, 418, 421, 423, 424, 425, 426, 427,
    428, 429, 430, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443,
    444, 445, 448, 449, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463,
    465, 466, 467, 468, 469, 470, 471, 473, 474, 475, 476, 477, 478, 479, 480,
    482, 483, 484, 485, 486, 487, 488, 489, 490, 492, 493, 495, 496, 497, 498,
    499, 500, 501, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 516,
    517, 518, 519, 521, 523, 524, 525, 526, 527, 528, 529, 531, 532, 533, 534,
    535, 536, 538, 539, 541, 542, 543, 544, 546, 547, 548, 549, 550, 551, 553,
    554, 555, 556, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569,
    571, 572, 574, 577, 578, 579, 580, 581, 582, 583, 586, 587, 588, 589, 590,
    591, 592, 593, 594, 595, 596, 597, 598, 599, 603, 604, 607, 608, 609, 610,
    611, 612, 613, 614, 616, 617, 618, 619, 620, 628, 629, 631, 632, 634, 637,
    638, 639, 641, 649, 651, 652,
]

# Запросы, на которых FalkorDB не справился,
# но остальные интересующие системы имеют результат.
# Пример: FALKOR_FAILED_QUERY_IDS = [3, 4, 11, 12]
FALKOR_FAILED_QUERY_IDS = [
    7, 21, 93, 94, 95, 101, 115, 128, 130, 136, 137, 138, 140, 154, 157, 181,
    182, 183, 184, 185, 187, 189, 195, 196, 198, 199, 209, 216, 624, 625, 626,
    627,
]


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
    raw_times_ms: list[float]          # после пропуска первого прогона
    used_times_ms: list[float]         # после удаления выбросов
    outlier_times_ms: list[float]
    mean_ms: float
    median_ms: float
    rsd_pct: float                     # relative standard deviation, %
    answer: str
    answer_consistent: bool


COMPETITORS = ["LARPQ", "MillenniumDB", "FalkorDB"]
PLOT_COLORS = [
    "#b565b5",  # purple
    "#d95757",  # red
    "#f0b33e",  # amber
    "#6b8ecf",  # blue
    "#57b26f",  # green
    "#8aa9a8",  # teal
]
PLOT_DARK_COLORS = [
    "#6f2f6f",
    "#8d2020",
    "#986300",
    "#1f3f7a",
    "#165b2d",
    "#355b5a",
]


def competitor_total_runs(competitor: str) -> int:
    if competitor == "FalkorDB":
        return 3
    return 5


def competitor_time_scale_to_us(competitor: str) -> float:
    if competitor == "FalkorDB":
        return 1.0 / 1000.0
    return 1.0


def is_falkordb_error_row(line: str) -> bool:
    row = next(csv.reader([line]))
    return len(row) == 2 and row[1].strip().lower() == "errored"


# ============================================================
# Parsing
# ============================================================

def parse_answer(s: str) -> str:
    s = s.strip()
    if not s:
        raise ValueError("empty answer")

    # Ответ ожидается числовой.
    # Если у MDB будет что-то вроде "8957779 results before",
    # такая строка будет отброшена как невалидная.
    if not s.lstrip("-").isdigit():
        raise ValueError(f"non-integer answer: {s!r}")

    return s


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


def parse_file(path: Optional[Path], competitor: str, strict: bool) -> tuple[dict[int, QueryStats], list[str]]:
    if path is None:
        return {}, [f"{competitor}: file is not provided, all values will be dashes"]

    if not path.exists():
        msg = f"{competitor}: file does not exist: {path}"
        if strict:
            raise FileNotFoundError(msg)
        return {}, [msg]

    occurrences: dict[int, list[Measurement]] = defaultdict(list)
    warnings: list[str] = []
    expected_runs = competitor_total_runs(competitor)
    time_scale = competitor_time_scale_to_us(competitor)

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()

            if not line:
                continue

            if competitor == "FalkorDB" and is_falkordb_error_row(line):
                continue

            try:
                m = parse_measurement_line(line, line_no)
            except Exception as e:
                msg = f"{competitor}: skip malformed line {line_no}: {e}; line={line!r}"
                if strict:
                    raise ValueError(msg) from e
                warnings.append(msg)
                continue

            if time_scale != 1.0:
                m = Measurement(
                    query_id=m.query_id,
                    time_ms=m.time_ms * time_scale,
                    answer=m.answer,
                    line_no=m.line_no,
                )

            occurrences[m.query_id].append(m)

    stats: dict[int, QueryStats] = {}

    for query_id, ms in sorted(occurrences.items()):
        if len(ms) < 2:
            warnings.append(
                f"{competitor}: query {query_id}: only {len(ms)} occurrence(s), "
                f"nothing remains after warm-up skip"
            )
            continue

        if competitor == "FalkorDB":
            if len(ms) not in (3, 4):
                warnings.append(
                    f"{competitor}: query {query_id}: expected 3 or 4 runs, got {len(ms)}; "
                    f"will use all runs after warm-up"
                )

            final_ms = ms[1:]
            raw_times = [x.time_ms for x in final_ms]
            used_times, outliers = remove_outliers_20pct(raw_times)

            if not used_times:
                warnings.append(
                    f"{competitor}: query {query_id}: no usable times after filtering"
                )
                continue

            mean_ms = st.mean(used_times)
            median_ms = st.median(used_times)
            rsd_pct = relative_stddev_pct(used_times)

            answers = [x.answer for x in final_ms]
            answer_counts = Counter(answers)
            answer, _ = answer_counts.most_common(1)[0]
            answer_consistent = len(answer_counts) == 1

            if not answer_consistent:
                warnings.append(
                    f"{competitor}: query {query_id}: inconsistent answers after warm-up: "
                    f"{dict(answer_counts)}; using most common answer={answer}"
                )

            stats[query_id] = QueryStats(
                query_id=query_id,
                raw_times_ms=raw_times,
                used_times_ms=used_times,
                outlier_times_ms=outliers,
                mean_ms=mean_ms,
                median_ms=median_ms,
                rsd_pct=rsd_pct,
                answer=answer,
                answer_consistent=answer_consistent,
            )
            continue

        if len(ms) < expected_runs:
            warnings.append(
                f"{competitor}: query {query_id}: expected {expected_runs} runs, got {len(ms)}; "
                f"will use available runs after warm-up"
            )

        if len(ms) > expected_runs:
            warnings.append(
                f"{competitor}: query {query_id}: expected {expected_runs} runs, got {len(ms)}; "
                f"will use occurrences 2..{expected_runs} only"
            )

        # Первый прогон — warm-up, его скипаем.
        # Финальный зачет — остальные прогоны.
        final_ms = ms[1:expected_runs]

        raw_times = [x.time_ms for x in final_ms]
        used_times, outliers = remove_outliers_20pct(raw_times)

        if not used_times:
            warnings.append(
                f"{competitor}: query {query_id}: no usable times after filtering"
            )
            continue

        mean_ms = st.mean(used_times)
        median_ms = st.median(used_times)
        rsd_pct = relative_stddev_pct(used_times)

        answers = [x.answer for x in final_ms]
        answer_counts = Counter(answers)
        answer, _ = answer_counts.most_common(1)[0]
        answer_consistent = len(answer_counts) == 1

        if not answer_consistent:
            warnings.append(
                f"{competitor}: query {query_id}: inconsistent answers after warm-up: "
                f"{dict(answer_counts)}; using most common answer={answer}"
            )

        stats[query_id] = QueryStats(
            query_id=query_id,
            raw_times_ms=raw_times,
            used_times_ms=used_times,
            outlier_times_ms=outliers,
            mean_ms=mean_ms,
            median_ms=median_ms,
            rsd_pct=rsd_pct,
            answer=answer,
            answer_consistent=answer_consistent,
        )

    return stats, warnings


# ============================================================
# Statistics
# ============================================================

def remove_outliers_20pct(values: list[float]) -> tuple[list[float], list[float]]:
    """
    Выброс: значение отличается больше чем на 20% от медианы остальных значений.

    Почему так:
    - всего 4 финальных запуска;
    - среднее легко ломается одним большим выбросом;
    - медиана остальных устойчива для случая 3 нормальных значений + 1 выброс.

    Если фильтрация оставляет меньше 2 значений, считаем, что надежно выделить выброс нельзя,
    и возвращаем исходные значения.
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


def relative_stddev_pct(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0

    mean = st.mean(values)
    if mean == 0:
        return 0.0

    # population stddev: у нас есть все 4 запуска, а не выборка из бесконечной серии.
    return st.pstdev(values) / mean * 100.0


def fmt_num(x: Optional[float], digits: int = 3) -> str:
    if x is None:
        return "-"

    if not math.isfinite(x):
        return "-"

    return f"{x:,.{digits}f}".replace(",", " ")


def fmt_speedup(x: Optional[float]) -> str:
    if x is None:
        return "-"
    if not math.isfinite(x):
        return "-"
    return f"{x:.2f}"


def us_to_ms(x: float) -> float:
    return x / 1000.0


def fmt_time_ms(x: Optional[float], digits: int = 3) -> str:
    if x is None:
        return "-"
    if not math.isfinite(x):
        return "-"
    return fmt_num(us_to_ms(x), digits)


def fmt_time_list_ms(values: list[float]) -> str:
    if not values:
        return "-"
    return ";".join(f"{us_to_ms(x):.6f}" for x in values)


def competitor_values(
    query_ids: list[int],
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    competitor: str,
) -> list[float]:
    stats = stats_by_competitor.get(competitor, {})
    return [
        stats[qid].mean_ms
        for qid in query_ids
        if qid in stats
    ]


def speedups_vs_larpq(
    query_ids: list[int],
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    competitor: str,
) -> list[float]:
    """
    speedup считается относительно LARPQ.

    Для конкурента X:
        speedup_X = time_LARPQ / time_X

    То есть:
    - LARPQ = 1.00
    - MillenniumDB = 5.00 значит MillenniumDB быстрее LARPQ в 5 раз
    - MillenniumDB = 0.50 значит MillenniumDB медленнее LARPQ в 2 раза
    """
    larpq_stats = stats_by_competitor.get("LARPQ", {})
    comp_stats = stats_by_competitor.get(competitor, {})

    result: list[float] = []

    for qid in query_ids:
        if qid not in larpq_stats:
            continue

        if qid not in comp_stats:
            continue

        base = larpq_stats[qid].mean_ms
        cur = comp_stats[qid].mean_ms

        if base <= 0 or cur <= 0:
            continue

        result.append(base / cur)

    return result


def build_summary_rows(
    query_ids: list[int],
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    competitors: list[str],
) -> list[list[str]]:
    rows: list[list[str]] = []

    totals: dict[str, Optional[float]] = {}
    means: dict[str, Optional[float]] = {}
    medians: dict[str, Optional[float]] = {}
    mean_speedups: dict[str, Optional[float]] = {}
    median_speedups: dict[str, Optional[float]] = {}

    for comp in competitors:
        vals = competitor_values(query_ids, stats_by_competitor, comp)

        if vals:
            totals[comp] = sum(vals)
            means[comp] = st.mean(vals)
            medians[comp] = st.median(vals)
        else:
            totals[comp] = None
            means[comp] = None
            medians[comp] = None

        sp = speedups_vs_larpq(query_ids, stats_by_competitor, comp)

        if sp:
            mean_speedups[comp] = st.mean(sp)
            median_speedups[comp] = st.median(sp)
        else:
            mean_speedups[comp] = None
            median_speedups[comp] = None

    rows.append(["Total, ms"] + [fmt_time_ms(totals[c]) for c in competitors])
    rows.append(["Mean, ms"] + [fmt_time_ms(means[c]) for c in competitors])
    rows.append(["Median, ms"] + [fmt_time_ms(medians[c]) for c in competitors])
    rows.append(["Mean speedup"] + [fmt_speedup(mean_speedups[c]) for c in competitors])
    rows.append(["Median speedup"] + [fmt_speedup(median_speedups[c]) for c in competitors])

    return rows


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


# ============================================================
# Details CSV
# ============================================================

def stat_cell(
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    comp: str,
    qid: int,
) -> Optional[QueryStats]:
    return stats_by_competitor.get(comp, {}).get(qid)


def write_details_csv(
    path: Path,
    query_ids: list[int],
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    competitors: list[str],
) -> None:
    headers = ["query_id"]

    for comp in competitors:
        headers.extend([
            f"{comp}_mean_ms",
            f"{comp}_rsd_pct",
            f"{comp}_answer",
            f"{comp}_runs_used_ms",
            f"{comp}_outliers_ms",
            f"{comp}_answer_consistent",
        ])

    for comp in competitors:
        if comp != "LARPQ":
            headers.append(f"{comp}_speedup")

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for qid in query_ids:
            row: list[str] = [str(qid)]

            for comp in competitors:
                s = stat_cell(stats_by_competitor, comp, qid)

                if s is None:
                    row.extend(["-", "-", "-", "-", "-", "-"])
                else:
                    row.extend([
                        f"{us_to_ms(s.mean_ms):.6f}",
                        f"{s.rsd_pct:.6f}",
                        s.answer,
                        fmt_time_list_ms(s.used_times_ms),
                        fmt_time_list_ms(s.outlier_times_ms),
                        str(s.answer_consistent),
                    ])

            for comp in competitors:
                if comp != "LARPQ":
                    row.append(speedup_cell(stats_by_competitor, comp, qid))

            writer.writerow(row)


def speedup_cell(
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    comp: str,
    qid: int,
) -> str:
    larpq = stat_cell(stats_by_competitor, "LARPQ", qid)
    other = stat_cell(stats_by_competitor, comp, qid)

    if larpq is None or other is None:
        return "-"

    if larpq.mean_ms <= 0:
        return "-"

    if other.mean_ms <= 0:
        return "-"

    return f"{larpq.mean_ms / other.mean_ms:.6f}"


# ============================================================
# Boxplots
# ============================================================

def write_boxplot(
    path: Path,
    title: str,
    query_ids: list[int],
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    log_scale: bool,
    competitors: list[str],
) -> Optional[str]:
    try:
        import matplotlib.pyplot as plt
    except Exception as e:
        return f"matplotlib is not available, skip boxplot {path.name}: {e}"

    data: list[list[float]] = []
    labels: list[str] = []

    for comp in competitors:
        vals = [us_to_ms(v) for v in competitor_values(query_ids, stats_by_competitor, comp)]
        if vals:
            data.append(vals)
            labels.append(comp)

    if not data:
        return f"no data for boxplot {path.name}"

    colors = [PLOT_COLORS[i % len(PLOT_COLORS)] for i in range(len(labels))]
    dark_colors = [PLOT_DARK_COLORS[i % len(PLOT_DARK_COLORS)] for i in range(len(labels))]

    fig, ax = plt.subplots(figsize=(5.3, 7.6))
    bp = ax.boxplot(
        data,
        tick_labels=labels,
        showmeans=True,
        meanline=True,
        patch_artist=True,
        widths=0.42,
        showfliers=False,
        medianprops={"color": "#111111", "linewidth": 2.4},
        meanprops={"color": "#111111", "linewidth": 1.8, "linestyle": ":"},
        whiskerprops={"color": "#5a5a5a", "linewidth": 1.2},
        capprops={"color": "#5a5a5a", "linewidth": 1.2},
        boxprops={"edgecolor": "#333333", "linewidth": 1.0},
    )

    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.42)

    for whisker, color in zip(bp["whiskers"], [c for c in dark_colors for _ in (0, 1)]):
        whisker.set_color(color)

    for cap, color in zip(bp["caps"], [c for c in dark_colors for _ in (0, 1)]):
        cap.set_color(color)

    ax.set_facecolor("#fbfbfb")
    ax.set_ylabel("Execution time, ms")
    ax.tick_params(axis="x", rotation=45)
    ax.set_xlim(0.5, len(labels) + 0.5)

    if log_scale:
        ax.set_yscale("log")

    ax.grid(True, axis="y", which="major", alpha=0.28, linewidth=0.8)
    ax.grid(True, axis="y", which="minor", alpha=0.08, linewidth=0.6)
    fig.tight_layout()
    fig.savefig(path, dpi=220)
    plt.close(fig)

    return None


def write_scatter_plot(
    path: Path,
    title: str,
    query_ids: list[int],
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    log_scale: bool,
    competitors: list[str],
) -> Optional[str]:
    try:
        import matplotlib.pyplot as plt
    except Exception as e:
        return f"matplotlib is not available, skip scatter plot {path.name}: {e}"

    plotted = False
    fig, ax = plt.subplots(figsize=(5.5, 8.4))
    ax.set_facecolor("#fbfbfb")
    plotted_positions: list[int] = []
    plotted_labels: list[str] = []

    for idx, comp in enumerate(competitors, 1):
        vals = [us_to_ms(v) for v in competitor_values(query_ids, stats_by_competitor, comp)]
        if not vals:
            continue

        plotted = True
        plotted_positions.append(idx)
        plotted_labels.append(comp)
        xs = []
        jitter_pattern = [
            -0.09, -0.075, -0.06, -0.045, -0.03, -0.015, 0.0,
            0.015, 0.03, 0.045, 0.06, 0.075, 0.09,
        ]
        for pos in range(len(vals)):
            xs.append(idx + jitter_pattern[pos % len(jitter_pattern)])

        color = PLOT_COLORS[(idx - 1) % len(PLOT_COLORS)]
        dark_color = PLOT_DARK_COLORS[(idx - 1) % len(PLOT_DARK_COLORS)]
        ax.scatter(xs, vals, alpha=0.78, s=22, color=color)

        mean_val = st.mean(vals)
        median_val = st.median(vals)
        ax.hlines(
            mean_val,
            idx - 0.24,
            idx + 0.24,
            colors=dark_color,
            linewidth=2.2,
            linestyles=":",
        )
        ax.hlines(
            median_val,
            idx - 0.28,
            idx + 0.28,
            colors=dark_color,
            linewidth=3.0,
        )

    if not plotted:
        plt.close(fig)
        return f"no data for scatter plot {path.name}"

    ax.plot([], [], color="#222222", linewidth=2.2, linestyle=":", label="Mean")
    ax.plot([], [], color="#222222", linewidth=3.0, linestyle="-", label="Median")

    ax.set_xticks(plotted_positions, plotted_labels)
    ax.tick_params(axis="x", rotation=45)
    ax.set_xlim(min(plotted_positions) - 0.5, max(plotted_positions) + 0.5)
    ax.set_ylabel("Execution time, ms")

    if log_scale:
        ax.set_yscale("log")

    ax.grid(True, axis="y", which="major", alpha=0.28, linewidth=0.8)
    ax.grid(True, axis="y", which="minor", alpha=0.08, linewidth=0.6)
    ax.legend(loc="lower right", frameon=True, framealpha=0.92, facecolor="white")
    fig.tight_layout()
    fig.savefig(path, dpi=220)
    plt.close(fig)

    return None


# ============================================================
# Reports
# ============================================================

def write_summary_report(
    path: Path,
    title: str,
    query_ids: list[int],
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    competitors: list[str],
) -> str:
    rows = build_summary_rows(query_ids, stats_by_competitor, competitors)
    table = render_text_table(
        ["metric"] + competitors,
        rows,
    )

    text = (
        f"{title}\n"
        f"{'=' * len(title)}\n\n"
        f"All execution times below are in milliseconds (ms).\n\n"
        f"Query ids count: {len(query_ids)}\n\n"
        f"{table}\n"
    )

    path.write_text(text, encoding="utf-8")
    return table


def write_warnings(path: Path, warnings: list[str]) -> None:
    if not warnings:
        path.write_text("No warnings.\n", encoding="utf-8")
        return

    path.write_text("\n".join(warnings) + "\n", encoding="utf-8")


def normalize_query_ids(xs: list[int]) -> list[int]:
    return sorted(set(int(x) for x in xs))


def print_missing_info(
    title: str,
    query_ids: list[int],
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    competitors: list[str],
) -> None:
    print(f"\n[{title}] missing data:")

    for comp in competitors:
        stats = stats_by_competitor.get(comp, {})
        missing = [qid for qid in query_ids if qid not in stats]

        if missing:
            print(f"  {comp}: {len(missing)} missing")
        else:
            print(f"  {comp}: 0 missing")


def active_competitors(args: argparse.Namespace) -> list[str]:
    competitors = ["LARPQ", "MillenniumDB"]
    if args.falkordb is not None:
        competitors.append("FalkorDB")
    return competitors


def auto_common_query_ids(
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    competitors: list[str],
) -> list[int]:
    if not competitors:
        return []
    query_sets = [set(stats_by_competitor.get(comp, {})) for comp in competitors]
    common = set.intersection(*query_sets) if query_sets else set()
    return sorted(common)


def auto_falkor_failed_query_ids(
    stats_by_competitor: dict[str, dict[int, QueryStats]],
    competitors: list[str],
) -> list[int]:
    if "FalkorDB" not in competitors:
        return []

    larpq = set(stats_by_competitor.get("LARPQ", {}))
    mdb = set(stats_by_competitor.get("MillenniumDB", {}))
    falkor = set(stats_by_competitor.get("FalkorDB", {}))
    return sorted((larpq & mdb) - falkor)


def infer_semantic(args: argparse.Namespace) -> str:
    haystacks = [
        args.prefix,
        str(args.out_dir),
        str(args.larpq),
        str(args.millenniumdb),
    ]
    if args.falkordb is not None:
        haystacks.append(str(args.falkordb))

    joined = " ".join(haystacks).lower().replace("_", "-")

    if "all-shortest" in joined:
        return "all-shortest"
    if "trails" in joined:
        return "trails"
    if "simple" in joined:
        return "simple"
    return "unknown"


def cleanup_optional_outputs(args: argparse.Namespace, include_falkor_failed: bool) -> None:
    if include_falkor_failed:
        return

    stale_paths = [
        args.out_dir / f"{args.prefix}_falkor_failed_summary.txt",
        args.out_dir / f"{args.prefix}_falkor_failed_scatter.png",
    ]

    for path in stale_paths:
        if path.exists():
            path.unlink()


# ============================================================
# Main
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Analyze Wikidata benchmark results for LARPQ, MillenniumDB and FalkorDB. "
            "Input format: query_id,time_in_source_units,answer. "
            "Output times are reported in milliseconds (ms). "
            "Each file contains 5 runs; first run is skipped."
        )
    )

    p.add_argument("larpq", type=Path, help="LARPQ result file")
    p.add_argument("millenniumdb", type=Path, help="MillenniumDB result file")
    p.add_argument(
        "falkordb",
        type=Path,
        nargs="?",
        default=None,
        help="FalkorDB result file. Optional; if absent, FalkorDB columns will be dashes.",
    )

    p.add_argument(
        "-o",
        "--out-dir",
        type=Path,
        default=Path("wikidata_analysis"),
        help="Output directory",
    )

    p.add_argument(
        "--prefix",
        default="wikidata",
        help="Prefix for output files",
    )

    p.add_argument(
        "--strict",
        action="store_true",
        help="Fail on malformed lines instead of skipping them",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    competitors = active_competitors(args)
    semantic = infer_semantic(args)

    stats_by_competitor: dict[str, dict[int, QueryStats]] = {}
    all_warnings: list[str] = []

    files = {
        "LARPQ": args.larpq,
        "MillenniumDB": args.millenniumdb,
    }
    if "FalkorDB" in competitors:
        files["FalkorDB"] = args.falkordb

    for comp, path in files.items():
        stats, warnings = parse_file(path, comp, strict=args.strict)
        stats_by_competitor[comp] = stats
        all_warnings.extend(warnings)

    if semantic in ("simple", "all-shortest"):
        common_query_ids = auto_common_query_ids(
            stats_by_competitor,
            ["LARPQ", "MillenniumDB"],
        )
        falkor_failed_query_ids: list[int] = []
        print(
            "INFO: For Wikidata simple/all-shortest, using auto-detected queries "
            "that succeeded on both LARPQ and MillenniumDB only.",
            file=sys.stderr,
        )
    else:
        common_query_ids = normalize_query_ids(COMMON_SUCCESS_QUERY_IDS)
        if "FalkorDB" not in competitors:
            common_query_ids = auto_common_query_ids(stats_by_competitor, competitors)
            print(
                "INFO: FalkorDB is not provided. Using auto-detected intersection of successful queries "
                "for active competitors only.",
                file=sys.stderr,
            )
        elif not common_query_ids:
            common_query_ids = auto_common_query_ids(stats_by_competitor, competitors)
            print(
                "WARNING: COMMON_SUCCESS_QUERY_IDS is empty. "
                "Using auto-detected intersection of successful queries.",
                file=sys.stderr,
            )

        falkor_failed_query_ids = normalize_query_ids(FALKOR_FAILED_QUERY_IDS)
        if "FalkorDB" in competitors and not falkor_failed_query_ids:
            falkor_failed_query_ids = auto_falkor_failed_query_ids(stats_by_competitor, competitors)
            print(
                "WARNING: FALKOR_FAILED_QUERY_IDS is empty. "
                "Using auto-detected queries that succeeded on LARPQ and MillenniumDB but not on FalkorDB.",
                file=sys.stderr,
            )

    cleanup_optional_outputs(args, include_falkor_failed=bool(falkor_failed_query_ids))

    # ----------------------------
    # Common successful queries
    # ----------------------------

    common_summary_path = args.out_dir / f"{args.prefix}_common_summary.txt"
    common_scatter_path = args.out_dir / f"{args.prefix}_common_scatter.png"

    common_table = write_summary_report(
        common_summary_path,
        "Common successful queries",
        common_query_ids,
        stats_by_competitor,
        competitors,
    )

    scatter_warning = write_scatter_plot(
        common_scatter_path,
        "Common successful queries",
        common_query_ids,
        stats_by_competitor,
        log_scale=True,
        competitors=competitors,
    )

    if scatter_warning:
        all_warnings.append(scatter_warning)

    # ----------------------------
    # FalkorDB failed queries
    # ----------------------------

    falkor_failed_summary_path = None
    falkor_failed_scatter_path = None
    falkor_failed_table = None

    if falkor_failed_query_ids:
        falkor_failed_summary_path = args.out_dir / f"{args.prefix}_falkor_failed_summary.txt"
        falkor_failed_scatter_path = args.out_dir / f"{args.prefix}_falkor_failed_scatter.png"

        falkor_failed_table = write_summary_report(
            falkor_failed_summary_path,
            "Queries failed on FalkorDB",
            falkor_failed_query_ids,
            stats_by_competitor,
            competitors,
        )

        scatter_warning = write_scatter_plot(
            falkor_failed_scatter_path,
            "Queries failed on FalkorDB",
            falkor_failed_query_ids,
            stats_by_competitor,
            log_scale=True,
            competitors=competitors,
        )

        if scatter_warning:
            all_warnings.append(scatter_warning)

    warnings_path = args.out_dir / f"{args.prefix}_warnings.log"
    write_warnings(warnings_path, all_warnings)

    # ----------------------------
    # Console output
    # ----------------------------

    print("\n=== Common successful queries ===")
    print(common_table)

    print_missing_info(
        "Common successful queries",
        common_query_ids,
        stats_by_competitor,
        competitors,
    )

    if falkor_failed_table is not None:
        print("\n=== Queries failed on FalkorDB ===")
        print(falkor_failed_table)

        print_missing_info(
            "Queries failed on FalkorDB",
            falkor_failed_query_ids,
            stats_by_competitor,
            competitors,
        )

    print("\nOutput files:")
    print(f"  {common_summary_path}")
    print(f"  {common_scatter_path}")
    if falkor_failed_summary_path is not None:
        print(f"  {falkor_failed_summary_path}")
        print(f"  {falkor_failed_scatter_path}")
    print(f"  {warnings_path}")

    if all_warnings:
        print(f"\nWarnings: {len(all_warnings)} warning(s), see {warnings_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
