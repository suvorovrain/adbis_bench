#!/usr/bin/env python3

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


SEMANTIC_ORDER = ["all-shortest", "simple", "trails"]


@dataclass(frozen=True)
class ResultBeforeStats:
    dataset: str
    semantic: str
    file_path: Path
    results_before_rows: int
    unique_query_ids: int
    unique_query_id_values: list[int]


def semantic_sort_key(name: str) -> tuple[int, str]:
    try:
        return (SEMANTIC_ORDER.index(name), name)
    except ValueError:
        return (len(SEMANTIC_ORDER), name)


def normalize_semantic(raw_name: str) -> str:
    if raw_name == "all":
        return "all-shortest"
    return raw_name


def normalize_dataset(raw_name: str) -> str:
    if raw_name == "wiki":
        return "wikidata"
    return raw_name


def discover_mdb_result_files(mdb_root: Path) -> list[tuple[str, str, Path]]:
    candidates: list[tuple[str, str, Path]] = []

    for path in sorted(mdb_root.glob("*-results-*")):
        if not path.is_dir():
            continue

        parts = path.name.split("-results-", maxsplit=1)
        if len(parts) != 2:
            continue

        dataset_raw, semantic_raw = parts
        dataset = normalize_dataset(dataset_raw)
        semantic = normalize_semantic(semantic_raw)

        csv_files = sorted(path.glob("*.csv"))
        for csv_path in csv_files:
            candidates.append((dataset, semantic, csv_path))

    return candidates


def count_results_before(path: Path, dataset: str, semantic: str) -> ResultBeforeStats:
    rows = 0
    query_ids: set[int] = set()

    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, 1):
            line = raw_line.strip()
            if not line:
                continue

            row = next(csv.reader([line]))
            if len(row) < 3:
                continue

            answer = row[2].strip().lower()
            if "results before" not in answer:
                continue

            rows += 1

            try:
                query_ids.add(int(row[0].strip()))
            except ValueError:
                # Для этой сводки достаточно посчитать строку даже если query_id битый.
                continue

    return ResultBeforeStats(
        dataset=dataset,
        semantic=semantic,
        file_path=path,
        results_before_rows=rows,
        unique_query_ids=len(query_ids),
        unique_query_id_values=sorted(query_ids),
    )


def render_text_table(headers: list[str], rows: list[list[str]]) -> str:
    table = [headers] + rows
    widths = [max(len(str(row[i])) for row in table) for i in range(len(headers))]

    def render_row(row: list[str]) -> str:
        return " | ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))

    sep = "-+-".join("-" * widths[i] for i in range(len(headers)))
    out = [render_row(headers), sep]
    out.extend(render_row(row) for row in rows)
    return "\n".join(out)


def write_csv(path: Path, stats: list[ResultBeforeStats]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset",
                "semantic",
                "results_before_rows",
                "unique_query_ids",
                "query_ids",
                "file",
            ]
        )

        for item in stats:
            writer.writerow(
                [
                    item.dataset,
                    item.semantic,
                    item.results_before_rows,
                    item.unique_query_ids,
                    ";".join(str(x) for x in item.unique_query_id_values),
                    str(item.file_path),
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Count MillenniumDB result lines that contain 'results before' "
            "and summarize them by dataset and semantic."
        )
    )

    parser.add_argument(
        "--mdb-root",
        type=Path,
        default=Path("mdb"),
        help="Root directory with MillenniumDB result folders",
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        type=Path,
        default=Path("Results/mdb-results-before"),
        help="Output directory for the generated report",
    )
    parser.add_argument(
        "--prefix",
        default="mdb_results_before",
        help="Prefix for generated files",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    discovered = discover_mdb_result_files(args.mdb_root)
    if not discovered:
        print(f"No MillenniumDB result files found under {args.mdb_root}")
        return 1

    stats = [
        count_results_before(path, dataset, semantic)
        for dataset, semantic, path in discovered
    ]

    stats.sort(key=lambda item: (item.dataset, semantic_sort_key(item.semantic)))

    rows = [
        [
            item.dataset,
            item.semantic,
            str(item.results_before_rows),
            str(item.unique_query_ids),
        ]
        for item in stats
    ]

    table = render_text_table(
        ["dataset", "semantic", "results before rows", "unique query ids"],
        rows,
    )

    text = (
        "MillenniumDB 'results before' summary\n"
        "===================================\n\n"
        f"{table}\n"
    )

    text_path = args.out_dir / f"{args.prefix}.txt"
    text_path.write_text(text, encoding="utf-8")

    print(text)
    print("Output files:")
    print(f"  {text_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
