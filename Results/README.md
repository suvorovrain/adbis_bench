Results description:

#### Semantics:
- all-simple (LARPQ, MillenniumDB)
- all-trails (LARPQ, MillenniumDB, FalkorDB) FALKOR DB ONLY FOR TRAILS!
- all-shortest (LARPQ, MillenniumDB)

#### general
    [memory usage](memory_stats.txt)
    [query stats](query_stats.txt)
    [mdb timeouts](Results/mdb-results-before/mdb_results_before.txt)
    [falkor timouts] -- only on trails: 13 
#### Datasets:


### wikidata
    detailed info about queries [there](query_stats.txt)  and [there](Results/mdb-results-before/mdb_results_before.txt)
    ## all-simple
        - [scatterplot](Results/wikidata/simple/wikidata_simple_common_scatter.png)
        - [overall stats](Results/wikidata/simple/wikidata_simple_common_summary.txt)
    ## all-trails
        - [scatterplot](Results/wikidata/trails/wikidata_trails_common_scatter.png)
        - [overall stats](Results/wikidata/trails/wikidata_trails_common_summary.txt)
        - [scatterplot for queries which falkor have failed on](Results/wikidata/trails/wikidata_trails_falkor_failed_scatter.png)
        - [overall for queries which falkor have failed on](Results/wikidata/trails/wikidata_trails_falkor_failed_summary.txt)
    ## all-shortest
        - [scatterplot](Results/wikidata/all-shortest/wikidata_all_shortest_common_scatter.png)
        - [overall stats](Results/wikidata/all-shortest/wikidata_all_shortest_common_summary.txt)
### rpqbench
    - 20 different types of query
    - 10 different query of each type
    - falkor db doesn't support querires 3,4,11,12
    ## all-simple
        - statistics for query on which we are [better](Results/rpqbench/simple/rpqbench_simple_larpq_better_worse_counts.txt)
        - table with results only on queries which we are [better](Results/rpqbench/simple/rpqbench_simple_larpq_better_overall.txt)
        - table with results only on queries which we are [worse](Results/rpqbench/simple/rpqbench_simple_larpq_worse_overall.txt)
        - table with results on all (mean of all 10 queries for each type) [queries](Results/rpqbench/simple/rpqbench_simple_summary.txt)
    ## all-trails
        - statistics for query on which we are [better](Results/rpqbench/trails/rpqbench_trails_larpq_better_worse_counts.txt)
        - table with results only on queries which we are [better](Results/rpqbench/trails/rpqbench_trails_larpq_better_overall.txt)
        - table with results only on queries which we are [worse](Results/rpqbench/trails/rpqbench_trails_larpq_worse_overall.txt)
        - table with results on all (mean of all 10 queries for each type) [queries](Results/rpqbench/trails/rpqbench_trails_summary.txt)
    ## all-shortest
        - statistics for query on which we are [better](Results/rpqbench/all-shortest/rpqbench_all_shortest_larpq_better_worse_counts.txt)
        - table with results only on queries which we are [better](Results/rpqbench/all-shortest/rpqbench_all_shortest_larpq_better_overall.txt)
        - table with results only on queries which we are [worse](Results/rpqbench/all-shortest/rpqbench_all_shortest_larpq_worse_overall.txt)
        - table with results on all (mean of all 10 queries for each type) [queries](Results/rpqbench/all-shortest/rpqbench_all_shortest_summary.txt)
### yago (Only LARPQ and MillenniumDB)
    - 7 queries
    ## all-simple
        - summary [statistics](Results/yago/simple/yago_simple_summary.txt)
        - per-query [statistics](Results/yago/simple/yago_simple_table.txt)
    ## all-trails
        - summary [statistics](Results/yago/trails/yago_trails_summary.txt)
        - per-query [statistics](Results/yago/trails/yago_trails_table.txt)
    ## all-shortest
        - summary [statistics](Results/yago/all-shortest/yago_all_shortest_summary.txt)
        - per-query [statistics](Results/yago/all-shortest/yago_all_shortest_table.txt)

