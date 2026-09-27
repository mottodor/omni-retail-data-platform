# Phase 6 benchmark: Trino vs ClickHouse

Generated at: `2026-09-27T08:28:54.842364+00:00`  
Query: aggregate `mart_daily_sales` by `order_date` and `region`  
Repetitions: `5`; first sample is the cold-start proxy, remaining samples are warm runs.

## Query

```sql
Trino:
SELECT order_date, region,
       sum(revenue_eur) AS revenue_eur,
       sum(margin_eur) AS margin_eur,
       sum(orders_count) AS orders_count
FROM iceberg.analytics.mart_daily_sales
GROUP BY order_date, region
ORDER BY order_date, region

ClickHouse:
SELECT order_date, region,
       sum(revenue_eur) AS revenue_eur,
       sum(margin_eur) AS margin_eur,
       sum(orders_count) AS orders_count
FROM analytics.mart_daily_sales
GROUP BY order_date, region
ORDER BY order_date, region
```

The query is semantically identical; only the fully qualified table name differs:
`iceberg.analytics.mart_daily_sales` for Trino and `analytics.mart_daily_sales` for
ClickHouse.

## Results (milliseconds)

| Engine | Result rows | First/cold | Min | Median | P95 | Max |
|---|---:|---:|---:|---:|---:|---:|
| Trino -> Iceberg | 2 | 118.78 | 49.57 | 55.60 | 73.36 | 118.78 |
| ClickHouse | 2 | 35.90 | 2.67 | 3.83 | 3.85 | 35.90 |

## Plans

### Trino -> Iceberg

#### EXPLAIN

```text
Trino version: 483
Fragment 0 [SINGLE]
    Output layout: [order_date, region, sum, sum_0, sum_1]
    Output partitioning: SINGLE []
    Output[columnNames = [order_date, region, revenue_eur, margin_eur, orders_count]]
    │   Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
    │   Estimates: {rows: 2 (248B), cpu: 0, memory: 0B, network: 0B}
    │   revenue_eur := sum
    │   margin_eur := sum_0
    │   orders_count := sum_1
    └─ RemoteMerge[sourceFragmentIds = [1]]
           Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]

Fragment 1 [ROUND_ROBIN]
    Output layout: [order_date, region, sum, sum_0, sum_1]
    Output partitioning: SINGLE []
    LocalMerge[orderBy = [order_date ASC NULLS LAST, region ASC NULLS LAST]]
    │   Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
    │   Estimates: {rows: 2 (248B), cpu: 0, memory: 0B, network: 0B}
    └─ PartialSort[orderBy = [order_date ASC NULLS LAST, region ASC NULLS LAST]]
       │   Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
       │   Estimates: {rows: 2 (248B), cpu: ?, memory: ?, network: ?}
       └─ RemoteSource[sourceFragmentIds = [2]]
              Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]

Fragment 2 [HASH]
    Output layout: [order_date, region, sum, sum_0, sum_1]
    Output partitioning: ROUND_ROBIN []
    Aggregate[type = FINAL, keys = [order_date, region]]
    │   Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
    │   Estimates: {rows: 2 (248B), cpu: 600, memory: 248B, network: 0B}
    │   sum := sum(sum_2)
    │   sum_0 := sum(sum_3)
    │   sum_1 := sum(sum_4)
    └─ LocalExchange[partitioning = HASH, arguments = [order_date::date, region::varchar]]
       │   Layout: [order_date:date, region:varchar, sum_2:varbinary, sum_3:varbinary, sum_4:bigint]
       │   Estimates: {rows: 3 (600B), cpu: 600, memory: 0B, network: 0B}
       └─ RemoteSource[sourceFragmentIds = [3]]
              Layout: [order_date:date, region:varchar, sum_2:varbinary, sum_3:varbinary, sum_4:bigint]

Fragment 3 [SOURCE]
    Output layout: [order_date, region, sum_2, sum_3, sum_4]
    Output partitioning: HASH [order_date, region]
    Aggregate[type = PARTIAL, keys = [order_date, region]]
    │   Layout: [order_date:date, region:varchar, sum_2:varbinary, sum_3:varbinary, sum_4:bigint]
    │   Estimates: {rows: 3 (600B), cpu: ?, memory: ?, network: ?}
    │   sum_2 := sum(revenue_eur)
    │   sum_3 := sum(margin_eur)
    │   sum_4 := sum(orders_count)
    └─ TableScan[table = iceberg:analytics.mart_daily_sales$data@4160093371695357939]
           Layout: [order_date:date, region:varchar, orders_count:bigint, revenue_eur:decimal(38,21), margin_eur:decimal(38,6)]
           Estimates: {rows: 3 (372B), cpu: 372, memory: 0B, network: 0B}
           orders_count := 5:orders_count:bigint
           revenue_eur := 7:revenue_eur:decimal(38,21)
           order_date := 2:order_date:date
           margin_eur := 8:margin_eur:decimal(38,6)
           region := 4:region:varchar


```

#### EXPLAIN ANALYZE / PIPELINE

```text
Trino version: 483
Queued: 102.83us, Analysis: 16.42ms, Planning: 7.60ms, Execution: 135.34ms, Finishing: 0.00ns
Fragment 1 [SINGLE]
    CPU: 200.84us, Scheduled: 211.66us, Blocked 16.01ms (Input: 16.02ms, Output: 0.00ns), Input: 2 rows (126B); per task: avg.: 2.00 std.dev.: 0.00, Output: 2 rows (126B)
    Peak Memory: 376B, Tasks count: 1; per task: max: 376B
    Output layout: [order_date, region, sum, sum_0, sum_1]
    Output partitioning: SINGLE []
    RemoteMerge[sourceFragmentIds = [2]]
        Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
        CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 16.00ms (2.18%), Output: 2 rows (126B)
        Input avg.: 2.00 rows, Input std.dev.: 0.00%

Fragment 2 [ROUND_ROBIN]
    CPU: 1.32ms, Scheduled: 1.43ms, Blocked 266.12ms (Input: 251.55ms, Output: 0.00ns), Input: 2 rows (126B); per task: avg.: 2.00 std.dev.: 0.00, Output: 2 rows (126B)
    Peak Memory: 376B, Tasks count: 1; per task: max: 376B
    Output layout: [order_date, region, sum, sum_0, sum_1]
    Output partitioning: SINGLE []
    LocalMerge[orderBy = [order_date ASC NULLS LAST, region ASC NULLS LAST]]
    │   Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
    │   Estimates: {rows: 2 (248B), cpu: 0, memory: 0B, network: 0B}
    │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 15.00ms (2.04%), Output: 2 rows (126B)
    │   Input avg.: 0.13 rows, Input std.dev.: 264.58%
    └─ PartialSort[orderBy = [order_date ASC NULLS LAST, region ASC NULLS LAST]]
       │   Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
       │   Estimates: {rows: 2 (248B), cpu: ?, memory: ?, network: ?}
       │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 0.00ns (0.00%), Output: 2 rows (126B)
       │   Input avg.: 0.13 rows, Input std.dev.: 264.58%
       └─ RemoteSource[sourceFragmentIds = [3]]
              Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
              CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 252.00ms (34.33%), Output: 2 rows (126B)
              Input avg.: 0.13 rows, Input std.dev.: 264.58%

Fragment 3 [HASH]
    CPU: 1.88ms, Scheduled: 2.09ms, Blocked 450.68ms (Input: 223.10ms, Output: 0.00ns), Input: 2 rows (118B); per task: avg.: 2.00 std.dev.: 0.00, Output: 2 rows (126B)
    Peak Memory: 318.19kB, Tasks count: 1; per task: max: 318.19kB
    Output layout: [order_date, region, sum, sum_0, sum_1]
    Output partitioning: ROUND_ROBIN []
    Aggregate[type = FINAL, keys = [order_date, region]]
    │   Layout: [order_date:date, region:varchar, sum:decimal(38,21), sum_0:decimal(38,6), sum_1:bigint]
    │   Estimates: {rows: 2 (248B), cpu: 600, memory: 248B, network: 0B}
    │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 0.00ns (0.00%), Output: 2 rows (126B)
    │   Input avg.: 0.13 rows, Input std.dev.: 264.58%
    │   sum := sum(sum_2)
    │   sum_0 := sum(sum_3)
    │   sum_1 := sum(sum_4)
    └─ LocalExchange[partitioning = HASH, arguments = [order_date::date, region::varchar]]
       │   Layout: [order_date:date, region:varchar, sum_2:varbinary, sum_3:varbinary, sum_4:bigint]
       │   Estimates: {rows: 3 (600B), cpu: 600, memory: 0B, network: 0B}
       │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 228.00ms (31.06%), Output: 2 rows (118B)
       │   Input avg.: 0.13 rows, Input std.dev.: 387.30%
       └─ RemoteSource[sourceFragmentIds = [4]]
              Layout: [order_date:date, region:varchar, sum_2:varbinary, sum_3:varbinary, sum_4:bigint]
              CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 223.00ms (30.38%), Output: 2 rows (118B)
              Input avg.: 0.13 rows, Input std.dev.: 387.30%

Fragment 4 [SOURCE]
    CPU: 850.52us, Scheduled: 884.92us, Blocked 0.00ns (Input: 0.00ns, Output: 0.00ns), Input: 3 rows (204B); per task: avg.: 3.00 std.dev.: 0.00, Output: 2 rows (118B)
    Peak Memory: 360B, Tasks count: 1; per task: max: 360B
    Output layout: [order_date, region, sum_2, sum_3, sum_4]
    Output partitioning: HASH [order_date, region]
    Aggregate[type = PARTIAL, keys = [order_date, region]]
    │   Layout: [order_date:date, region:varchar, sum_2:varbinary, sum_3:varbinary, sum_4:bigint]
    │   Estimates: {rows: 3 (600B), cpu: ?, memory: ?, network: ?}
    │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 0.00ns (0.00%), Output: 2 rows (118B)
    │   Input avg.: 3.00 rows, Input std.dev.: 0.00%
    │   sum_2 := sum(revenue_eur)
    │   sum_3 := sum(margin_eur)
    │   sum_4 := sum(orders_count)
    └─ TableScan[table = iceberg:analytics.mart_daily_sales$data@4160093371695357939]
           Layout: [order_date:date, region:varchar, orders_count:bigint, revenue_eur:decimal(38,21), margin_eur:decimal(38,6)]
           Estimates: {rows: 3 (372B), cpu: 372, memory: 0B, network: 0B}
           CPU: 1.00ms (100.00%), Scheduled: 1.00ms (100.00%), Blocked: 0.00ns (0.00%), Output: 3 rows (204B)
           Input avg.: 3.00 rows, Input std.dev.: 0.00%
           orders_count := 5:orders_count:bigint
           revenue_eur := 7:revenue_eur:decimal(38,21)
           order_date := 2:order_date:date
           margin_eur := 8:margin_eur:decimal(38,6)
           region := 4:region:varchar
           Input: 3 rows (204B), Physical input: 1.47kB, Physical input time: 2.90us, Splits: 1, Splits generation wait time: 11.96ms


```

### ClickHouse

#### EXPLAIN

```text
Output: order_date, region, sum(revenue_eur), sum(margin_eur), sum(orders_count)

Sorting (Sorting for ORDER BY)
│  Sort description: order_date ASC, region ASC
└──Aggregating
   │  Keys: order_date, region
   │  Aggregates: sum(revenue_eur), sum(margin_eur), sum(orders_count)
   │  Skip merging: 0
   └──ReadFromMergeTree (analytics.mart_daily_sales)
         Read type: Default
         Parts: 1 | Granules: 1
         Output: order_date, region, sum(revenue_eur), sum(margin_eur), sum(orders_count)
```

#### EXPLAIN ANALYZE / PIPELINE

```text
(Expression)
ExpressionTransform
  (Sorting)
  MergeSortingTransform
    LimitsCheckingTransform
      PartialSortingTransform
        (Expression)
        ExpressionTransform
          (Aggregating)
          AggregatingTransform
            (Expression)
            ExpressionTransform
              (ReadFromMergeTree)
              MergeTreeSelect(pool: ReadPoolInOrder, algorithm: InOrder) 0 → 1
```

## Interpretation

These are local-workstation observations for the recorded dataset and stack
configuration, not a general performance claim. Repeat after changing data
volume or physical design; retain the query equivalence and plan output when
comparing runs.
