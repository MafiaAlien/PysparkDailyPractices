"""
=====================================================================
PySpark Daily Practice — Template v2 (Claude Code edition)
=====================================================================
Day 17 — Incremental patterns: SCD2 history build from a change feed
         (Medium-Hard)

PROBLEM
-------
`product_feed` is an append-only feed of product attribute snapshots.
Every time an upstream system touches a product it appends a row carrying
that product's attribute values as of `changed_at`. The feed is not
guaranteed to be meaningful: an upstream job may re-emit a product whose
tracked attributes did not actually change.

Build the Slowly-Changing-Dimension type 2 (SCD2) history table.

Rules:

1.  Tracked attributes are `category` and `price`. A version is a maximal
    run of consecutive feed rows (ordered by `changed_at` within a
    product) that carry the same tracked attribute values.
2.  A feed row that leaves every tracked attribute unchanged relative to
    the immediately preceding feed row of that product opens no new
    version.
3.  `effective_from` is the `changed_at` at which the version's attribute
    values started being true for that product.
4.  Intervals are half-open `[effective_from, effective_to)`:
    `effective_to` equals the NEXT version's `effective_from`. The latest
    version of a product has `effective_to = NULL`.
5.  `is_current` is `true` for exactly the latest version of each product,
    `false` otherwise.

Attribute values are never NULL in this feed, and no product has two feed
rows with the same `changed_at`.

INPUT SCHEMA
------------
product_feed(product_id: string, changed_at: date,
             category: string, price: double)

EXPECTED OUTPUT
---------------
Columns, in this order:

    product_id     string
    effective_from date
    effective_to   date     (NULL for the current version)
    category       string
    price          double
    is_current     boolean

One row per (product, version). Row order does not matter — `check()`
compares order-insensitively.

EXAMPLE
-------
Input (one product):

    P9  2026-02-01  'a'  1.0
    P9  2026-02-04  'a'  1.0
    P9  2026-02-08  'b'  1.0

Expected:

    P9  2026-02-01  2026-02-08  'a'  1.0  false
    P9  2026-02-08  NULL        'b'  1.0  true

WORKFLOW (v2)
-------------
Stage 1  SOLVE   : Implement solve_dsl() and solve_sql(). Run this file.
Stage 2  GENERATE: Run /genprompt and paste the emitted prompt into a
                   SEPARATE incognito conversation. Never ask for the
                   solution inside this repo.
Stage 3  REVIEW  : Paste the AI's code into Part 3, UNMODIFIED. Review by
                   reading only. Fill REVIEW_NOTES and commit a VERDICT
                   BEFORE running anything.
Stage 4  VERIFY  : Un-comment the Stage-4 lines below and run.
Stage 5  DIGEST  : Fill "Review takeaways" in Part 5, then run /review
                   and /digest.

Reference answers and concept takeaways live in the sibling *_ref.md file.
Do not open it before Stage 5.
=====================================================================
"""

from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =====================================================================
# Part 1 — Solution (yours)
# =====================================================================
class Solution:
    def solve_dsl(self, df: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: everything here is a window over (product_id ordered by
        changed_at); the hard part is deciding what counts as a new version.
        """
        # TODO: implement
        window_date = Window.partitionBy('product_id').orderBy('changed_at')

        add_prev = (
            df.withColumn(
                'prev_category',
                F.lag(F.col('category'), 1).over(window_date)
            )
            .withColumn(
                'prev_price',
                F.lag(F.col('price'), 1).over(window_date)
            )
        )

        change_cond = (F.struct(F.col('category'), F.col('price')) != F.struct(F.col('prev_category'), F.col('prev_price')))

        add_flag = (
            add_prev.withColumn(
                'is_changed',
                F.when(
                    change_cond, 
                    1).otherwise(0)
            )
        )

        add_boundary = (
            add_flag.withColumn(
                'boundary',
                F.sum('is_changed').over(window_date)
            )
        )

        window_end = Window.partitionBy('product_id').orderBy('boundary')



        get_updates = (
            add_boundary.groupBy(
                'product_id',
                'boundary'
            )
            .agg(
                F.min('changed_at').alias('effective_from'),
                F.max('category').alias('category'),
                F.max('price').alias('price')
            )
            .withColumn(
                'next_date',
                F.lead(F.col('effective_from')).over(window_end)
            )
            .withColumn(
                'effective_to',
                F.when(F.col('next_date').isNotNull(), F.col('next_date'))
            )
            .withColumn(
                'is_current',
                F.when(F.col('effective_to').isNotNull(), False).otherwise(True)
            )
            .select(
                'product_id',
                'effective_from',
                'effective_to',
                'category',
                'price',
                'is_current'
            )
        )

        return get_updates



    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """Spark SQL approach.

        Hint: same shape in SQL — window functions inside a CTE, then
        assemble the intervals.
        """
        df.createOrReplaceTempView("product_feed")

        sql = """
            -- write your SQL here
            WITH 
            get_prev AS (
                SELECT 
                    product_id,
                    changed_at,
                    category,
                    price,
                    LAG(category)OVER(partition by product_id order by changed_at) AS prev_category,
                    LAG(price)OVER(partition by product_id order by changed_at) AS prev_price
                FROM 
                    product_feed
            ),

            flag_change AS (
                SELECT
                    product_id,
                    changed_at,
                    category,
                    price,
                    CASE WHEN struct(category, price) <> struct(prev_category, prev_price) THEN 1 ELSE 0 END AS is_changed
                FROM 
                    get_prev
            ),

            set_boundary AS (
                SELECT 
                    product_id,
                    changed_at,
                    category,
                    price,
                    SUM(is_changed)OVER(partition by product_id order by changed_at) AS boundary
                FROM 
                    flag_change),

            agg_effective_date AS (
                SELECT 
                    product_id,
                    boundary,
                    MIN(changed_at) AS effective_from,
                    MAX(category) AS category,
                    MAX(price) AS price
                FROM 
                    set_boundary
                GROUP BY product_id, boundary),

            get_next AS (
                SELECT 
                    product_id,
                    effective_from,
                    LEAD(effective_from)OVER(partition by product_id order by boundary) AS next_date,
                    category,
                    price,
                    CASE WHEN 
                        LEAD(effective_from)OVER(partition by product_id order by boundary) IS NULL THEN TRUE
                        ELSE FALSE END AS is_current
                FROM 
                    agg_effective_date                
            )

            SELECT * FROM get_next;
        """
        return spark.sql(sql)


def ai_solve_dsl(df: DataFrame) -> DataFrame:
    w_feed = Window.partitionBy("product_id").orderBy("changed_at")
    w_run = w_feed.rowsBetween(Window.unboundedPreceding, Window.currentRow)
 
    prev_category = F.lag("category").over(w_feed)
    prev_price = F.lag("price").over(w_feed)
 
    is_new_version = (
        prev_category.isNull()
        | (prev_category != F.col("category"))
        | (prev_price != F.col("price"))
    )
 
    flagged = df.withColumn("_is_new", is_new_version.cast("int"))
    grouped = flagged.withColumn("_version", F.sum("_is_new").over(w_run))
 
    versions = grouped.groupBy("product_id", "_version").agg(
        F.min("changed_at").alias("effective_from"),
        F.min("category").alias("category"),
        F.min("price").alias("price"),
    )
 
    w_version = Window.partitionBy("product_id").orderBy("effective_from")
    result = versions.withColumn(
        "effective_to", F.lead("effective_from").over(w_version)
    ).withColumn("is_current", F.col("effective_to").isNull())
 
    return result.select(
        "product_id",
        "effective_from",
        "effective_to",
        "category",
        "price",
        "is_current",
    )
 
 
def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("_scd2_product_feed")
 
    return spark.sql(
        """
        WITH flagged AS (
            SELECT
                product_id,
                changed_at,
                category,
                price,
                CASE
                    WHEN LAG(category) OVER (
                             PARTITION BY product_id ORDER BY changed_at
                         ) IS NULL
                      OR LAG(category) OVER (
                             PARTITION BY product_id ORDER BY changed_at
                         ) <> category
                      OR LAG(price) OVER (
                             PARTITION BY product_id ORDER BY changed_at
                         ) <> price
                    THEN 1
                    ELSE 0
                END AS is_new
            FROM _scd2_product_feed
        ),
        runs AS (
            SELECT
                product_id,
                changed_at,
                category,
                price,
                SUM(is_new) OVER (
                    PARTITION BY product_id
                    ORDER BY changed_at
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS version_no
            FROM flagged
        ),
        versions AS (
            SELECT
                product_id,
                version_no,
                MIN(changed_at) AS effective_from,
                MIN(category)   AS category,
                MIN(price)      AS price
            FROM runs
            GROUP BY product_id, version_no
        ),
        bounded AS (
            SELECT
                product_id,
                effective_from,
                LEAD(effective_from) OVER (
                    PARTITION BY product_id ORDER BY effective_from
                ) AS effective_to,
                category,
                price
            FROM versions
        )
        SELECT
            product_id,
            effective_from,
            effective_to,
            category,
            price,
            effective_to IS NULL AS is_current
        FROM bounded
        """
    )

# =====================================================================
# Part 2 — Tests
# =====================================================================
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

    data = [
        ("P1", date(2026, 1, 1), "toys", 10.0),
        ("P1", date(2026, 1, 5), "toys", 10.0),
        ("P1", date(2026, 1, 10), "toys", 12.0),

        ("P2", date(2026, 1, 2), "books", 20.0),
        ("P2", date(2026, 1, 6), "books", 25.0),
        ("P2", date(2026, 1, 9), "media", 25.0),

        ("P3", date(2026, 1, 3), "games", 30.0),

        ("P4", date(2026, 1, 4), "tools", 40.0),
        ("P4", date(2026, 1, 7), "tools", 45.0),
        ("P4", date(2026, 1, 11), "tools", 40.0),
    ]
    df = spark.createDataFrame(
        data, schema="product_id string, changed_at date, category string, price double"
    )

    expected = [
        ("P1", date(2026, 1, 1), date(2026, 1, 10), "toys", 10.0, False),
        ("P1", date(2026, 1, 10), None, "toys", 12.0, True),

        ("P2", date(2026, 1, 2), date(2026, 1, 6), "books", 20.0, False),
        ("P2", date(2026, 1, 6), date(2026, 1, 9), "books", 25.0, False),
        ("P2", date(2026, 1, 9), None, "media", 25.0, True),

        ("P3", date(2026, 1, 3), None, "games", 30.0, True),

        ("P4", date(2026, 1, 4), date(2026, 1, 7), "tools", 40.0, False),
        ("P4", date(2026, 1, 7), date(2026, 1, 11), "tools", 45.0, False),
        ("P4", date(2026, 1, 11), None, "tools", 40.0, True),
    ]

    s = Solution()
    check(s.solve_dsl(df), expected, "DSL")
    check(s.solve_sql(spark, df), expected, "SQL")

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # ------------------------------------------------------------------
    check(ai_solve_dsl(df), expected, "AI-DSL (post-review verification)")
    check(ai_solve_sql(spark, df), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
#
# def ai_solve_dsl(df: DataFrame) -> DataFrame:
#     ...
#
# def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
#     ...
#
# REVIEW_NOTES (fill in BEFORE running):
# ----------------------------------------------------------------------
# [pass] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes:
# [pass] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes:
# [pass] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (element_at, array index, cast, division)?
#     notes:
# [pass ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (do NOT guess — this is a reading-stage hypothesis, verified later)
#     notes: because AI did not use F.when, so AI added prev.isNull() in conditions
# [pass] Robustness — hardcoded values, assumptions not in the problem statement?
#     notes:
# [pass] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS — because: same logic as my solution
# ACTUAL RESULT (after Stage 4 run): passed
# GAP ANALYSIS: did the run reveal anything the reading missed? No gap


# =====================================================================
# Part 5 — Review takeaways (fill in at Stage 5, before /review)
# =====================================================================
# - What did the AI get wrong, or suspiciously right?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem: see the sibling *_ref.md file.
