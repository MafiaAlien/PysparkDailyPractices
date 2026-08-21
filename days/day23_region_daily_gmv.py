"""
=====================================================================
PySpark Daily Practice — ETL Scenario Template (Day 22 onward)
=====================================================================
Day 23 — agg_region_daily  (Medium-Hard)

ETL layer : L3 serving
Domain    : e-commerce (marketplace) orders

BUSINESS CONTEXT
----------------
The marketplace finance dashboard reads one daily aggregate table: gross and
net GMV per region per order date. A nightly job rebuilds it from the order
feed, the refund feed, and the seller-to-region assignment config that the
regional ops team maintains by hand.

INPUT TABLES
------------
orders(order_id: string, seller_id: string, order_date: string,
       qty: int, unit_price: double, status: string)
    -- append-only order feed, one row per order

seller_region(seller_id: string, region: string, warehouse: string,
              valid_from: string, valid_to: string)
    -- hand-maintained config table. One row per seller assignment. When a
       seller is reassigned, ops appends a new row and is supposed to close
       the previous one by setting its `valid_to`.

refunds(refund_id: string, order_id: string, refund_amount: double,
        refund_date: string)
    -- append-only refund feed, one row per refund

Dates are ISO-8601 STRINGS, not the DATE type. They are fixed-width, so
lexicographic order equals chronological order and you can compare them
directly. This day is about the pipeline, not about date parsing.

orders:

    +----------+-----------+------------+-----+------------+-----------+
    | order_id | seller_id | order_date | qty | unit_price | status    |
    +----------+-----------+------------+-----+------------+-----------+
    | O1       | S1        | 2026-06-14 |   2 |       50.0 | DELIVERED |
    | O2       | S2        | 2026-06-14 |   1 |      120.0 | SHIPPED   |
    | O3       | S3        | 2026-06-14 |   3 |       20.0 | DELIVERED |
    | O4       | S3        | 2026-05-20 |   1 |       80.0 | DELIVERED |
    | O5       | S4        | 2026-06-15 |   2 |       45.0 | PLACED    |
    | O6       | S1        | 2026-06-15 |   1 |      200.0 | CANCELLED |
    | O7       | S3        | 2026-06-15 |   5 |       12.0 | SHIPPED   |
    | O8       | S1        | 2026-06-14 |   1 |       75.0 | DELIVERED |
    +----------+-----------+------------+-----+------------+-----------+

    Note: O6 is CANCELLED. A cancelled order contributes to nothing, and
    neither does any refund that references it.

seller_region:

    +-----------+---------+-----------+------------+------------+
    | seller_id | region  | warehouse | valid_from | valid_to   |
    +-----------+---------+-----------+------------+------------+
    | S1        | CENTRAL | WH_C1     | 2026-01-01 | NULL       |
    | S2        | EAST    | WH_E1     | 2026-01-01 | 2026-06-01 |
    | S2        | EAST    | WH_E2     | 2026-06-01 | NULL       |
    | S3        | CENTRAL | WH_C7     | 2026-05-01 | NULL       |
    | S3        | CENTRAL | WH_C9     | 2026-06-01 | NULL       |
    | S4        | SOUTH   | WH_S1     | 2026-07-01 | NULL       |
    +-----------+---------+-----------+------------+------------+

    Note: validity is half-open — an assignment covers a date d when
    valid_from <= d < valid_to. A NULL `valid_to` means the assignment is
    still open. `warehouse` is an operational column; downstream finance
    has no use for it.

refunds:

    +-----------+----------+---------------+-------------+
    | refund_id | order_id | refund_amount | refund_date |
    +-----------+----------+---------------+-------------+
    | R1        | O2       |          40.0 | 2026-06-16  |
    | R2        | O3       |          15.0 | 2026-06-20  |
    | R3        | O6       |         200.0 | 2026-06-18  |
    | R4        | O8       |          25.0 | 2026-06-16  |
    +-----------+----------+---------------+-------------+

    Note: a refund is reported on the ORDER's date, never on `refund_date`.
    Finance compares gross and net GMV on the same date line.

OUTPUT CONTRACT
---------------
Table    : agg_region_daily
Grain    : one row per (order_date, region), over non-cancelled orders only
Columns  : order_date: string, region: string, n_orders: bigint,
           gross_gmv: double, refund_amount: double, net_gmv: double
           (this exact order)
Ordering : irrelevant — check() sorts both sides

Column semantics:
  n_orders       number of DISTINCT orders in the group
  gross_gmv      SUM(qty * unit_price) over those orders
  refund_amount  total refunded against those orders; 0.0, never NULL,
                 when the group had no refunds
  net_gmv        gross_gmv - refund_amount

Region attribution: an order belongs to the region of the assignment in
effect on that order's `order_date`. An order whose seller has NO assignment
in effect on that date is attributed to region 'UNKNOWN'. No non-cancelled
order may be dropped from the report.

Expected:

    +------------+---------+----------+-----------+---------------+---------+
    | order_date | region  | n_orders | gross_gmv | refund_amount | net_gmv |
    +------------+---------+----------+-----------+---------------+---------+
    | 2026-05-20 | CENTRAL |        1 |      80.0 |           0.0 |    80.0 |
    | 2026-06-14 | CENTRAL |        3 |     235.0 |          40.0 |   195.0 |
    | 2026-06-14 | EAST    |        1 |     120.0 |          40.0 |    80.0 |
    | 2026-06-15 | CENTRAL |        1 |      60.0 |           0.0 |    60.0 |
    | 2026-06-15 | UNKNOWN |        1 |      90.0 |           0.0 |    90.0 |
    +------------+---------+----------+-----------+---------------+---------+

PRODUCTION CONSTRAINTS
----------------------
P1. Every metric is defined once. `net_gmv` must be derived from the same
    `gross_gmv` and `refund_amount` expressions the output reports — no
    branch may recompute a metric its own way.
P2. Before returning, the job must assert its own output: unique on the
    declared grain, and `order_date` / `region` never NULL. A silent bad
    publish is worse than a failed job.
P3. Read only the columns this job actually needs from `seller_region`. The
    config carries operational columns that must not enter the pipeline.

These are requirements, not hints. Missing one fails the tests.

WORKFLOW (v2)
-------------
Stage 1  SOLVE   : Implement the two Part 1 functions. Run this file.
Stage 2  GENERATE: Run /genprompt and paste the emitted prompt into a
                   SEPARATE incognito conversation. Never ask for the
                   solution inside this repo.
Stage 3  REVIEW  : Paste the AI answer into the Part 3 zone, UNMODIFIED.
                   Review by reading only. Fill REVIEW_NOTES and commit a
                   VERDICT BEFORE running anything.
Stage 4  VERIFY  : Un-comment the Stage-4 lines in Part 2 and run.
Stage 5  DIGEST  : Fill Part 5, then run /review and /digest.

Reference answers and concept takeaways live in
refs/day23_region_daily_gmv_ref.md. Do not open it before Stage 5.
=====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# #####################################################################
# ##                                                                 ##
# ##   PART 1 — YOUR JOB                                             ##
# ##                                                                 ##
# ##   Stage 1 touches this block and nothing else in this file.     ##
# ##                                                                 ##
# #####################################################################

# ------------------------------ 1a. DataFrame API --------------------
def build_agg_region_daily_dsl(
    orders: DataFrame,
    seller_region: DataFrame,
    refunds: DataFrame,
) -> DataFrame:
    """Daily gross / net GMV per region for the finance dashboard.

    Hint: validity is half-open — valid_from <= order_date < valid_to,
    with a NULL valid_to meaning open-ended.
    """
    # TODO: implement

    seller_region = (
        seller_region.withColumn(
            'valid_to',
            F.coalesce(F.col('valid_to'), F.lit('9999-12-31'))
        )
        .select(
            'seller_id',
            'region',
            'valid_from',
            'valid_to'
        )
    )

    filter_out_canceled = (orders.filter(F.col('status')!='CANCELLED'))


    merge_order_refunds = (
        filter_out_canceled
        .join(
            refunds,
            on='order_id',
            how='left'
        )
        .withColumn(
            'refund_amount',
            F.coalesce(F.col('refund_amount'), F.lit(0.0))
        )
        .select(
            'order_id',
            'seller_id',
            'order_date',
            'qty',
            'unit_price',
            'refund_amount'
        )
    )

    order_seller_join_cond = (merge_order_refunds.seller_id == seller_region.seller_id) & (merge_order_refunds.order_date.between(seller_region.valid_from, seller_region.valid_to)) 

    merge_region_daily = (
        merge_order_refunds.join(
            seller_region,
            on=order_seller_join_cond,
            how='left'
        )
        .withColumn(
            'region',
            F.coalesce(F.col('region'), F.lit('UNKNOWN'))
        )
        .drop(seller_region.seller_id))

    window_most_recent = (
        Window.partitionBy('seller_id', 'order_date', 'order_id').orderBy(F.col('valid_from').desc())
        )

    agg_region_daily = (
        merge_region_daily.withColumn(
            'rn',
            F.row_number().over(window_most_recent)
        )
        .filter(F.col('rn') == 1)
        .groupBy('order_date', 'region')
        .agg(
            F.count_distinct(F.col('order_id')).cast('int').alias('n_orders'),
            F.sum(F.col('qty') * F.col('unit_price')).cast('double').alias('gross_gmv'),
            F.sum(F.coalesce(F.col('refund_amount'), F.lit(0.0))).cast('double').alias('refund_amount')
        )
        .withColumn(
            'net_gmv',
            F.col('gross_gmv') - F.col('refund_amount'), 
        )
        .select(
            'order_date',
            'region',
            'n_orders',
            'gross_gmv',
            'refund_amount',
            'net_gmv'
        )
    )

    return agg_region_daily


# ------------------------------ 1b. Spark SQL ------------------------
def build_agg_region_daily_sql(
    spark: SparkSession,
    orders: DataFrame,
    seller_region: DataFrame,
    refunds: DataFrame,
) -> DataFrame:
    """Same job, Spark SQL.

    Hint: one CTE per pipeline stage keeps a job this long readable.
    """
    orders.createOrReplaceTempView("orders")
    seller_region.createOrReplaceTempView("seller_region")
    refunds.createOrReplaceTempView("refunds")

    sql = """
        -- write your SQL here
        WITH fill_null_valid_to AS (
            SELECT 
                seller_id,
                region,
                valid_from,
                COALESCE(valid_to, '9999-12-31') AS valid_to
            FROM
                seller_region
        ),
        merge_order_refunds AS (
            SELECT 
            o.order_id,
            seller_id,
            order_date,
            qty,
            unit_price,
            COALESCE(refund_amount, 0.0) AS refund_amount
        FROM orders o LEFT JOIN refunds r ON o.order_id = r.order_id
        WHERE status != 'CANCELLED'
        ),

        merge_region_daily AS (
            SELECT 
                m.seller_id,
                order_id,
                order_date,
                qty,
                unit_price,
                refund_amount,
                valid_from,
                valid_to,
                COALESCE(region, 'UNKNOWN') AS region
            FROM 
                merge_order_refunds m LEFT JOIN fill_null_valid_to f 
                ON (m.seller_id = f.seller_id) AND (m.order_date BETWEEN f.valid_from AND f.valid_to)
        ),

        agg_region_daily AS (
                SELECT 
                    order_date,
                    region,
                    COUNT(DISTINCT order_id) AS n_orders,
                    CAST(SUM(qty * unit_price) AS DOUBLE) AS gross_gmv,
                    SUM(COALESCE(refund_amount, 0.0)) AS refund_amount
                FROM (SELECT 
                        *, 
                        ROW_NUMBER()OVER(PARTITION BY seller_id, order_date, order_id ORDER BY valid_from DESC) AS rn 
                    FROM merge_region_daily ) tb
                WHERE rn = 1
                GROUP BY order_date, region
        )

        SELECT 
            *,
            gross_gmv - refund_amount AS net_gmv
        FROM 
            agg_region_daily
    """
    return spark.sql(sql)


# #####################################################################
# ##   END OF PART 1                                                 ##
# #####################################################################


# #####################################################################
# ##                                                                 ##
# ##   PART 3 — PASTE THE INCOGNITO AI ANSWER BELOW                  ##
# ##                                                                 ##
# ##   UNMODIFIED. Two functions, `ai_` prefix, signatures identical ##
# ##   to Part 1. Do not reformat it. Do not fix anything you spot — ##
# ##   spotting it is exactly what Stage 3 measures.                 ##
# ##                                                                 ##
# #####################################################################

# >>> PASTE BEGIN
def ai_build_agg_region_daily_dsl(
    orders: DataFrame,
    seller_region: DataFrame,
    refunds: DataFrame,
) -> DataFrame:
    live_orders = orders.filter(F.col("status") != "CANCELLED").select(
        "order_id", "seller_id", "order_date", "qty", "unit_price"
    )
 
    assignments = seller_region.select("seller_id", "region", "valid_from", "valid_to")
 
    joined = live_orders.alias("o").join(
        assignments.alias("sr"),
        (F.col("o.seller_id") == F.col("sr.seller_id"))
        & (F.col("sr.valid_from") <= F.col("o.order_date"))
        & (
            F.col("sr.valid_to").isNull()
            | (F.col("o.order_date") < F.col("sr.valid_to"))
        ),
        "left",
    )
 
    pick = Window.partitionBy(F.col("o.order_id")).orderBy(
        F.col("sr.valid_from").desc_nulls_last(),
        F.col("sr.valid_to").asc_nulls_last(),
        F.col("sr.region").asc_nulls_last(),
    )
 
    attributed = (
        joined.withColumn("_rn", F.row_number().over(pick))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("o.order_id").alias("order_id"),
            F.col("o.order_date").alias("order_date"),
            F.coalesce(F.col("sr.region"), F.lit("UNKNOWN")).alias("region"),
            (F.col("o.qty") * F.col("o.unit_price")).cast("double").alias("order_gmv"),
        )
    )
 
    refunds_per_order = (
        refunds.join(live_orders.select("order_id"), on="order_id", how="inner")
        .groupBy("order_id")
        .agg(F.sum("refund_amount").cast("double").alias("order_refund"))
    )
 
    aggregated = (
        attributed.join(refunds_per_order, on="order_id", how="left")
        .groupBy("order_date", "region")
        .agg(
            F.countDistinct("order_id").cast("bigint").alias("n_orders"),
            F.sum("order_gmv").cast("double").alias("gross_gmv"),
            F.coalesce(F.sum("order_refund"), F.lit(0.0))
            .cast("double")
            .alias("refund_amount"),
        )
    )
 
    result = aggregated.withColumn(
        "net_gmv", F.col("gross_gmv") - F.col("refund_amount")
    ).select(
        "order_date", "region", "n_orders", "gross_gmv", "refund_amount", "net_gmv"
    )
 
    result.cache()
 
    null_keys = result.filter(
        F.col("order_date").isNull() | F.col("region").isNull()
    ).count()
    if null_keys:
        raise AssertionError(
            f"agg_region_daily: {null_keys} row(s) with NULL order_date/region"
        )
 
    duplicate_keys = (
        result.groupBy("order_date", "region")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_keys:
        raise AssertionError(
            f"agg_region_daily: {duplicate_keys} duplicated (order_date, region) key(s)"
        )
 
    return result
 
 
def ai_build_agg_region_daily_sql(
    spark: SparkSession,
    orders: DataFrame,
    seller_region: DataFrame,
    refunds: DataFrame,
) -> DataFrame:
    orders.createOrReplaceTempView("ai_src_orders")
    seller_region.createOrReplaceTempView("ai_src_seller_region")
    refunds.createOrReplaceTempView("ai_src_refunds")
 
    result = spark.sql(
        """
        WITH live_orders AS (
            SELECT order_id, seller_id, order_date, qty, unit_price
            FROM ai_src_orders
            WHERE status <> 'CANCELLED'
        ),
        assignments AS (
            SELECT seller_id, region, valid_from, valid_to
            FROM ai_src_seller_region
        ),
        ranked AS (
            SELECT
                o.order_id                                  AS order_id,
                o.order_date                                AS order_date,
                sr.region                                   AS region,
                CAST(o.qty * o.unit_price AS DOUBLE)        AS order_gmv,
                ROW_NUMBER() OVER (
                    PARTITION BY o.order_id
                    ORDER BY sr.valid_from DESC NULLS LAST,
                             sr.valid_to   ASC  NULLS LAST,
                             sr.region     ASC  NULLS LAST
                )                                           AS rn
            FROM live_orders o
            LEFT JOIN assignments sr
                   ON o.seller_id = sr.seller_id
                  AND sr.valid_from <= o.order_date
                  AND (sr.valid_to IS NULL OR o.order_date < sr.valid_to)
        ),
        attributed AS (
            SELECT
                order_id,
                order_date,
                COALESCE(region, 'UNKNOWN') AS region,
                order_gmv
            FROM ranked
            WHERE rn = 1
        ),
        refunds_per_order AS (
            SELECT
                r.order_id                                  AS order_id,
                CAST(SUM(r.refund_amount) AS DOUBLE)        AS order_refund
            FROM ai_src_refunds r
            JOIN live_orders o
              ON o.order_id = r.order_id
            GROUP BY r.order_id
        ),
        aggregated AS (
            SELECT
                a.order_date                                        AS order_date,
                a.region                                            AS region,
                CAST(COUNT(DISTINCT a.order_id) AS BIGINT)          AS n_orders,
                CAST(SUM(a.order_gmv) AS DOUBLE)                    AS gross_gmv,
                CAST(COALESCE(SUM(rp.order_refund), 0.0) AS DOUBLE) AS refund_amount
            FROM attributed a
            LEFT JOIN refunds_per_order rp
                   ON rp.order_id = a.order_id
            GROUP BY a.order_date, a.region
        )
        SELECT
            order_date,
            region,
            n_orders,
            gross_gmv,
            refund_amount,
            gross_gmv - refund_amount AS net_gmv
        FROM aggregated
        """
    )
 
    result.cache()
 
    null_keys = result.filter(
        F.col("order_date").isNull() | F.col("region").isNull()
    ).count()
    if null_keys:
        raise AssertionError(
            f"agg_region_daily: {null_keys} row(s) with NULL order_date/region"
        )
 
    duplicate_keys = (
        result.groupBy("order_date", "region")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_keys:
        raise AssertionError(
            f"agg_region_daily: {duplicate_keys} duplicated (order_date, region) key(s)"
        )
 
    return result
# >>> PASTE END

# ---------------------------------------------------------------------
# REVIEW_NOTES — fill in BEFORE running anything.
# Severity order: runtime break > wrong results on dirty data
#               > production robustness > performance > portability > style
# ---------------------------------------------------------------------
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes:LGTM, AI also handle tie-break via orderBy region in window func
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes: LGTM
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (cast, division, array index, element_at)?
#     notes:cast correctly in final query to ensure ANSI compliance
# [ ] Production — walk P1/P2/P3 one at a time, each on its own line.
#     Every metric defined once? Output asserted before returning? Only the
#     needed columns read from the config?
#     notes: LGTM, P1/P2/P3 are all implemented, but I forgot to implement P2
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (reading-stage hypothesis only — verified with .explain() later,
#     never asserted from memory)
#     notes: AI calculate refund_amount in refunds table, this will lead to another shuffle, but it is correct because it can ensure the grain of refund amount before joining
# [ ] Robustness — hardcoded values, assumptions not in the output contract?
#     notes: LGTM, no hardcodes, assertions are all implemented
# [ ] Style/clarity — would you approve this in a real code review?
#     notes: Yes, no obvious error of codes 
#
# VERDICT (commit before running): PASS / FAIL — because:Pass, logics are as same as mine, and even better because AI'solution for to calculate grain of refund amount is more rigurous
# ACTUAL RESULT (after Stage 4 run): All passed
# GAP ANALYSIS: did the run reveal anything the reading missed? No


# #####################################################################
# ##   PART 2 — HARNESS.  Do not edit.                               ##
# #####################################################################
def check(actual: DataFrame, expected_rows: list, label: str) -> None:
    """Order-insensitive comparison; sorted() absorbs tie-order instability."""
    actual_rows = sorted([tuple(r) for r in actual.collect()])
    expected = sorted(expected_rows)
    status = "PASS" if actual_rows == expected else "FAIL"
    print(f"[{status}] {label}")
    if status == "FAIL":
        print("  expected:", expected)
        print("  actual  :", actual_rows)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    orders = spark.createDataFrame(
        [
            ("O1", "S1", "2026-06-14", 2, 50.0, "DELIVERED"),
            ("O2", "S2", "2026-06-14", 1, 120.0, "SHIPPED"),
            ("O3", "S3", "2026-06-14", 3, 20.0, "DELIVERED"),
            ("O4", "S3", "2026-05-20", 1, 80.0, "DELIVERED"),
            ("O5", "S4", "2026-06-15", 2, 45.0, "PLACED"),
            ("O6", "S1", "2026-06-15", 1, 200.0, "CANCELLED"),
            ("O7", "S3", "2026-06-15", 5, 12.0, "SHIPPED"),
            ("O8", "S1", "2026-06-14", 1, 75.0, "DELIVERED"),
        ],
        schema=(
            "order_id string, seller_id string, order_date string, "
            "qty int, unit_price double, status string"
        ),
    )
    seller_region = spark.createDataFrame(
        [
            ("S1", "CENTRAL", "WH_C1", "2026-01-01", None),
            ("S2", "EAST", "WH_E1", "2026-01-01", "2026-06-01"),
            ("S2", "EAST", "WH_E2", "2026-06-01", None),
            ("S3", "CENTRAL", "WH_C7", "2026-05-01", None),
            ("S3", "CENTRAL", "WH_C9", "2026-06-01", None),
            ("S4", "SOUTH", "WH_S1", "2026-07-01", None),
        ],
        schema=(
            "seller_id string, region string, warehouse string, "
            "valid_from string, valid_to string"
        ),
    )
    refunds = spark.createDataFrame(
        [
            ("R1", "O2", 40.0, "2026-06-16"),
            ("R2", "O3", 15.0, "2026-06-20"),
            ("R3", "O6", 200.0, "2026-06-18"),
            ("R4", "O8", 25.0, "2026-06-16"),
        ],
        schema=(
            "refund_id string, order_id string, refund_amount double, "
            "refund_date string"
        ),
    )

    expected = [
        ("2026-05-20", "CENTRAL", 1, 80.0, 0.0, 80.0),
        ("2026-06-14", "CENTRAL", 3, 235.0, 40.0, 195.0),
        ("2026-06-14", "EAST", 1, 120.0, 40.0, 80.0),
        ("2026-06-15", "CENTRAL", 1, 60.0, 0.0, 60.0),
        ("2026-06-15", "UNKNOWN", 1, 90.0, 0.0, 90.0),
    ]

    check(build_agg_region_daily_dsl(orders, seller_region, refunds),
          expected, "DSL")
    check(build_agg_region_daily_sql(spark, orders, seller_region, refunds),
          expected, "SQL")

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    check(ai_build_agg_region_daily_dsl(orders, seller_region, refunds),
          expected, "AI-DSL (post-review verification)")
    check(ai_build_agg_region_daily_sql(spark, orders, seller_region, refunds),
          expected, "AI-SQL (post-review verification)")

    spark.stop()


# #####################################################################
# ##   PART 5 — Review takeaways (fill at Stage 5, before /review)   ##
# #####################################################################
# - What did the AI get wrong, or suspiciously right?
# - Which production constraint (P#) did either side miss, and why was it
#   missable by reading?
#  I missed P2;
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem:
# see refs/day23_region_daily_gmv_ref.md.
