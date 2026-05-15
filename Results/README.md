# Results Description

## Semantics
- `all-simple` (LARPQ, MillenniumDB)
- `all-trails` (LARPQ, MillenniumDB, FalkorDB). FalkorDB is used only for trails.
- `all-shortest` (LARPQ, MillenniumDB)

## General
- [Memory usage](../memory_stats.txt)
- [Query stats](../query_stats.txt)
- [MillenniumDB timeouts](mdb-results-before/mdb_results_before.txt)
- Falkor timeouts: only on trails, `13`

## Datasets

### Wikidata
Detailed query information is available [here](../query_stats.txt) and [here](mdb-results-before/mdb_results_before.txt).

#### all-simple
- [Scatterplot](wikidata/simple/wikidata_simple_common_scatter.png)
- [Overall stats](wikidata/simple/wikidata_simple_common_summary.txt)

#### all-trails
- [Scatterplot](wikidata/trails/wikidata_trails_common_scatter.png)
- [Overall stats](wikidata/trails/wikidata_trails_common_summary.txt)
- [Scatterplot for queries where FalkorDB failed](wikidata/trails/wikidata_trails_falkor_failed_scatter.png)
- [Overall stats for queries where FalkorDB failed](wikidata/trails/wikidata_trails_falkor_failed_summary.txt)

#### all-shortest
- [Scatterplot](wikidata/all-shortest/wikidata_all_shortest_common_scatter.png)
- [Overall stats](wikidata/all-shortest/wikidata_all_shortest_common_summary.txt)

### RPQBench
- 20 different query types
- 10 queries of each type
- FalkorDB does not support queries `3`, `4`, `11`, `12`

#### all-simple
- Statistics for queries where LARPQ is [better](rpqbench/simple/rpqbench_simple_larpq_better_worse_counts.txt)
- Table with results only for queries where LARPQ is [better](rpqbench/simple/rpqbench_simple_larpq_better_overall.txt)
- Table with results only for queries where LARPQ is [worse](rpqbench/simple/rpqbench_simple_larpq_worse_overall.txt)
- Table with results for all query types (mean of all 10 queries for each type) [here](rpqbench/simple/rpqbench_simple_summary.txt)

#### all-trails
- Statistics for queries where LARPQ is [better](rpqbench/trails/rpqbench_trails_larpq_better_worse_counts.txt)
- Table with results only for queries where LARPQ is [better](rpqbench/trails/rpqbench_trails_larpq_better_overall.txt)
- Table with results only for queries where LARPQ is [worse](rpqbench/trails/rpqbench_trails_larpq_worse_overall.txt)
- Table with results for all query types (mean of all 10 queries for each type) [here](rpqbench/trails/rpqbench_trails_summary.txt)

#### all-shortest
- Statistics for queries where LARPQ is [better](rpqbench/all-shortest/rpqbench_all_shortest_larpq_better_worse_counts.txt)
- Table with results only for queries where LARPQ is [better](rpqbench/all-shortest/rpqbench_all_shortest_larpq_better_overall.txt)
- Table with results only for queries where LARPQ is [worse](rpqbench/all-shortest/rpqbench_all_shortest_larpq_worse_overall.txt)
- Table with results for all query types (mean of all 10 queries for each type) [here](rpqbench/all-shortest/rpqbench_all_shortest_summary.txt)

### YAGO
Only LARPQ and MillenniumDB.

- 7 queries

#### all-simple
- Summary [statistics](yago/simple/yago_simple_summary.txt)
- Per-query [statistics](yago/simple/yago_simple_table.txt)

#### all-trails
- Summary [statistics](yago/trails/yago_trails_summary.txt)
- Per-query [statistics](yago/trails/yago_trails_table.txt)

#### all-shortest
- Summary [statistics](yago/all-shortest/yago_all_shortest_summary.txt)
- Per-query [statistics](yago/all-shortest/yago_all_shortest_table.txt)
