"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 15 — Window frames: trailing moving average + period-over-period growth
         (Medium)

PROBLEM
-------
A retail platform stores one row per store per day of trading. Days on which
a store did not trade have NO row at all (the table is sparse, not dense).

For EVERY row in the input, produce a trend report:
  - revenue          : that day's revenue, unchanged
  - ma7              : average daily revenue over the trailing 7 CALENDAR
                       days — the current day plus the 6 days before it.
                       A calendar day with no row counts as a 0-revenue day,
                       so the denominator is ALWAYS 7 (even at the very
                       start of a store's history, where the window reaches
                       back before the first row).
  - prev_day_revenue : revenue on the immediately preceding CALENDAR day
                       (sale_date - 1). 0.0 if that day has no row.
  - dod_growth_pct   : (revenue - prev_day_revenue) / prev_day_revenue * 100.
                       NULL when prev_day_revenue is 0.

Rounding: ma7 and dod_growth_pct to 2 decimal places.
The output has exactly as many rows as the input — do not densify the
output, do not drop the first days of a store.

INPUT SCHEMA
------------
daily_sales(store_id: string, sale_date: date, revenue: double)

At most one row per (store_id, sale_date). revenue is never NULL.

EXPECTED OUTPUT
---------------
Columns, in this exact order:
  store_id (string), sale_date (date), revenue (double),
  ma7 (double), prev_day_revenue (double), dod_growth_pct (double, nullable)
Row order does not matter (check() sorts).

EXAMPLE
-------
("S2", 2026-06-01, 10.0)
("S2", 2026-06-02, 20.0)
("S2", 2026-06-08, 30.0)      <- five calendar days with no row in between

  ("S2", 2026-06-01, 10.0, 1.43, 0.0,  NULL)
  ("S2", 2026-06-02, 20.0, 4.29, 10.0, 100.0)
  ("S2", 2026-06-08, 30.0, 7.14, 0.0,  NULL)

WORKFLOW (v2)
-------------
Stage 1  SOLVE   : Implement solve_dsl() and solve_sql() yourself. Run tests.
Stage 2  GENERATE: Only AFTER your solutions pass, ask an AI to solve the
                   same problem. IMPORTANT: paste only the PROBLEM + SCHEMA
                   sections into the AI — never your own solution — so its
                   answer is independent and your review is a blind review.
Stage 3  REVIEW  : Paste the AI's code into Part 3 below. Review it by
                   reading only (no running). Fill in REVIEW_NOTES and
                   commit to a VERDICT before executing anything.
Stage 4  VERIFY  : Un-comment and run the AI code against the same check().
                   Compare the actual result with your verdict.
Stage 5  DIGEST  : Fill in "Review takeaways" in Part 5 — what the AI got
                   wrong/right, and whether YOU caught it by reading.
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

        Hint: everything here is one window spec plus a frame. Ask what the
        frame bounds are counted in before you write them.
        """
        # TODO: implement
        df = df.withColumn("day_num", F.datediff("sale_date", F.lit(date(1970, 1, 1))).cast("long"))
        base_partition = Window.partitionBy('store_id').orderBy('day_num')
        window_spec_7 = base_partition.rangeBetween(-6, Window.currentRow)
        window_spec_1 = base_partition.rangeBetween(-1, -1)
        col_revenue = F.col('revenue')
        col_prev_revenue = F.col('prev_day_revenue')

        ma7 = (
            df.withColumn(
            'ma7',
            F.round((F.sum('revenue').over(window_spec_7) / 7 ), 2)
            )
        )

        res_row_between = (
            ma7.withColumn(
                'prev_day_revenue',
                F.coalesce(F.sum('revenue').over(window_spec_1), F.lit(0.0))
            )
            .withColumn(
                'dod_growth_pct',
                F.when(col_prev_revenue > 0.0, F.round(
                    ((col_revenue - col_prev_revenue) / col_prev_revenue * 100), 2)
                )
            )
            .select(
                'store_id',
                'sale_date',
                'revenue',
                'ma7',
                'prev_day_revenue',
                'dod_growth_pct')
        )

        prev_revenue = F.lag("revenue", 1).over(base_partition)
        prev_date = F.lag('sale_date', 1).over(base_partition)
        prev_day_revenue = F.when(
            F.datediff(F.col("sale_date"), prev_date) == 1, prev_revenue
        ).otherwise(F.lit(0.0))

        res_date_diff = (
            ma7.
            withColumn(
                'prev_day_revenue',
                prev_day_revenue)
            .withColumn(
                'dod_growth_pct',
                F.when(col_prev_revenue > 0.0, F.round(
                    ((col_revenue - col_prev_revenue) / col_prev_revenue * 100), 2)
                )
            )
            .select(
                'store_id',
                'sale_date',
                'revenue',
                'ma7',
                'prev_day_revenue',
                'dod_growth_pct')
        )

        return res_row_between, res_date_diff

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: OVER (PARTITION BY ... ORDER BY ... <frame>). The frame clause
        is where the whole problem lives.
        """
        df.createOrReplaceTempView("daily_sales")

        sql = """
            -- write your SQL here
            WITH
            get_prev_day_revenue AS (
                SELECT
                    store_id,
                    sale_date,
                    revenue,
                    SUM(revenue)OVER(partition by store_id order by sale_date RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW) AS ma7,
                    COALESCE(SUM(revenue)OVER(partition by store_id order by sale_date RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 1 DAY PRECEDING), 0) as prev_day_revenue
                FROM
                    daily_sales
            )

            SELECT store_id,
                sale_date,
                revenue,
                Round(ma7 / 7, 2) AS ma7,
                prev_day_revenue,
                CASE
                    WHEN prev_day_revenue > 0 THEN Round((revenue - prev_day_revenue) / prev_day_revenue * 100, 2)
                    ELSE NULL
                END AS dod_growth_pct
            FROM   get_prev_day_revenue;
        """
        return spark.sql(sql)

_EPOCH = "1970-01-01"
_VIEW = "ai_daily_sales"
 
 
def ai_solve_dsl(df: DataFrame) -> DataFrame:
    # Convert the date to an integer day number so window frames can be
    # RANGE-based (calendar-aware) rather than ROW-based (gap-blind).
    base = df.withColumn(
        "_d", F.datediff(F.col("sale_date"), F.to_date(F.lit(_EPOCH))).cast("long")
    )
 
    w7 = Window.partitionBy("store_id").orderBy("_d").rangeBetween(-6, 0)
    w_prev = Window.partitionBy("store_id").orderBy("_d").rangeBetween(-1, -1)
 
    prev = F.coalesce(F.sum("revenue").over(w_prev), F.lit(0.0)).cast("double")
    sum7 = F.coalesce(F.sum("revenue").over(w7), F.lit(0.0))
 
    return (
        base
        .withColumn("ma7", F.round(sum7 / F.lit(7.0), 2).cast("double"))
        .withColumn("prev_day_revenue", prev)
        .withColumn(
            "dod_growth_pct",
            F.when(F.col("prev_day_revenue") == F.lit(0.0), F.lit(None).cast("double"))
            .otherwise(
                F.round(
                    (F.col("revenue") - F.col("prev_day_revenue"))
                    / F.col("prev_day_revenue")
                    * F.lit(100.0),
                    2,
                ).cast("double")
            ),
        )
        .select(
            "store_id",
            "sale_date",
            "revenue",
            "ma7",
            "prev_day_revenue",
            "dod_growth_pct",
        )
    )
 
 
def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView(_VIEW)
 
    query = f"""
        WITH base AS (
            SELECT
                store_id,
                sale_date,
                revenue,
                CAST(datediff(sale_date, DATE'{_EPOCH}') AS BIGINT) AS d
            FROM {_VIEW}
        ),
        windowed AS (
            SELECT
                store_id,
                sale_date,
                revenue,
                COALESCE(
                    SUM(revenue) OVER (
                        PARTITION BY store_id ORDER BY d
                        RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
                    ), 0.0D
                ) AS sum7,
                CAST(COALESCE(
                    SUM(revenue) OVER (
                        PARTITION BY store_id ORDER BY d
                        RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING
                    ), 0.0D
                ) AS DOUBLE) AS prev_day_revenue
            FROM base
        )
        SELECT
            store_id,
            sale_date,
            revenue,
            CAST(ROUND(sum7 / 7.0D, 2) AS DOUBLE)              AS ma7,
            prev_day_revenue,
            CASE
                WHEN prev_day_revenue = 0.0D THEN CAST(NULL AS DOUBLE)
                ELSE CAST(ROUND(
                    (revenue - prev_day_revenue) / prev_day_revenue * 100.0D, 2
                ) AS DOUBLE)
            END                                                AS dod_growth_pct
        FROM windowed
    """
    return spark.sql(query)


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
        SparkSession.builder.appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    data = [
        # S1 — five contiguous days, then a four-day trading gap, then two more
        ("S1", date(2026, 6, 1), 100.0),
        ("S1", date(2026, 6, 2), 200.0),
        ("S1", date(2026, 6, 3), 300.0),
        ("S1", date(2026, 6, 4), 400.0),
        ("S1", date(2026, 6, 5), 500.0),
        ("S1", date(2026, 6, 10), 600.0),
        ("S1", date(2026, 6, 11), 700.0),
        # S2 — sparse from the start
        ("S2", date(2026, 6, 1), 10.0),
        ("S2", date(2026, 6, 2), 20.0),
        ("S2", date(2026, 6, 8), 30.0),
    ]
    df = spark.createDataFrame(
        data, schema="store_id string, sale_date date, revenue double"
    )

    expected = [
        ("S1", date(2026, 6, 1), 100.0, 14.29, 0.0, None),
        ("S1", date(2026, 6, 2), 200.0, 42.86, 100.0, 100.0),
        ("S1", date(2026, 6, 3), 300.0, 85.71, 200.0, 50.0),
        ("S1", date(2026, 6, 4), 400.0, 142.86, 300.0, 33.33),
        ("S1", date(2026, 6, 5), 500.0, 214.29, 400.0, 25.0),
        ("S1", date(2026, 6, 10), 600.0, 214.29, 0.0, None),
        ("S1", date(2026, 6, 11), 700.0, 257.14, 600.0, 16.67),
        ("S2", date(2026, 6, 1), 10.0, 1.43, 0.0, None),
        ("S2", date(2026, 6, 2), 20.0, 4.29, 10.0, 100.0),
        ("S2", date(2026, 6, 8), 30.0, 7.14, 0.0, None),
    ]

    s = Solution()
    res_row_between, res_date_diff = s.solve_dsl(df)
    check(res_row_between, expected, "DSL_row_between")
    check(res_date_diff, expected, "DSL_date_diff")
    print("#" * 50, "row between explain:", sep='\n')
    # res_row_between.explain()

    print("#" * 50, "datediff explain:", sep="\n")
    # res_date_diff.explain()
    check(s.solve_sql(spark, df), expected, "SQL")

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    check(ai_solve_dsl(df), expected, "AI-DSL (post-review verification)")
    check(ai_solve_sql(spark, df), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review (NEW in v2)
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
# [pass] Correctness — edge cases: ties? nulls? empty groups? duplicate keys?
#     notes:
# [pass] API usage — wrong signatures, deprecated calls, ambiguous column
#     references in joins, SQL syntax slips (trailing commas, quoting)?
#     notes:
# [fail] Performance — unnecessary shuffles, window over the whole table
#     without partitionBy, join strategy, redundant actions/collects?
#     notes: it is not necessary to transfer day into integer or bigint in sql statement, just order by date instead, which will be more clear
# [pass] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes:
# [pass] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS  — because:
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# ---- Route A (DSL): one window spec, two frames ----------------------
# def ref_solve_dsl(df: DataFrame) -> DataFrame:
#     base = Window.partitionBy("store_id").orderBy("sale_date")
#     w_trailing = base.rangeBetween(-6, Window.currentRow)
#     w_prev_day = base.rangeBetween(-1, -1)
#
#     prev = F.coalesce(F.sum("revenue").over(w_prev_day), F.lit(0.0))
#
#     return (
#         df.withColumn("ma7", F.round(F.sum("revenue").over(w_trailing) / 7, 2))
#         .withColumn("prev_day_revenue", prev)
#         .withColumn(
#             "dod_growth_pct",
#             F.when(
#                 F.col("prev_day_revenue") > 0,
#                 F.round(
#                     (F.col("revenue") - F.col("prev_day_revenue"))
#                     / F.col("prev_day_revenue")
#                     * 100,
#                     2,
#                 ),
#             ),
#         )
#         .select(
#             "store_id", "sale_date", "revenue",
#             "ma7", "prev_day_revenue", "dod_growth_pct",
#         )
#     )
#
# ---- Route A (SQL) ---------------------------------------------------
# def ref_solve_sql(spark, df):
#     df.createOrReplaceTempView("daily_sales")
#     sql = """
#         WITH w AS (
#             SELECT
#                 store_id,
#                 sale_date,
#                 revenue,
#                 ROUND(SUM(revenue) OVER (
#                     PARTITION BY store_id ORDER BY sale_date
#                     RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
#                 ) / 7, 2) AS ma7,
#                 COALESCE(SUM(revenue) OVER (
#                     PARTITION BY store_id ORDER BY sale_date
#                     RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING
#                 ), 0.0) AS prev_day_revenue
#             FROM daily_sales
#         )
#         SELECT store_id, sale_date, revenue, ma7, prev_day_revenue,
#                CASE WHEN prev_day_revenue > 0
#                     THEN ROUND((revenue - prev_day_revenue) / prev_day_revenue * 100, 2)
#                END AS dod_growth_pct
#         FROM w
#     """
#     return spark.sql(sql)
#
# ---- Route B: densify first, then plain ROWS / LAG --------------------
# Build the full store x date axis (explode(sequence(min_d, max_d, 1 day))
# crossJoin the store list), LEFT JOIN the sparse table onto it, COALESCE
# revenue to 0. On a DENSE series, rowsBetween(-6, 0) and LAG(revenue, 1)
# are then correct by construction — one physical row per calendar day is
# exactly the assumption a ROWS frame makes. Finally semi-join back to the
# original keys so the output keeps only real trading days.
# This is the Day 11 spine in a new costume. It works, but costs a
# crossJoin + a join + a filter to buy something the RANGE frame gives for
# free. Prefer Route A here; reach for the spine when the report itself
# must be dense.
#
# ---- Route C (rejected): self-join on sale_date - 1 -------------------
# LEFT JOIN daily_sales b ON a.store_id = b.store_id
#                        AND b.sale_date = DATE_SUB(a.sale_date, 1)
# gives prev_day_revenue correctly, but it is a second shuffle to fetch a
# value the window already has in its partition, and it does nothing for
# ma7 (a 7-way self-join is not a serious answer). Named here only so the
# comparison is on the record.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - ROWS vs RANGE is not a style choice on a sparse time series, it is the
#   definition of the metric. ROWS counts PHYSICAL ROWS; RANGE counts
#   VALUES of the orderBy column. "Trailing 7 calendar days" is a statement
#   about values, so it is a RANGE frame. rowsBetween(-6, 0) silently
#   reaches further back than 7 days whenever a day is missing.
# - The bug is invisible on contiguous data: on a store with no gaps the
#   two frames agree row for row. Only the rows adjacent to a gap disagree
#   — same coincidentally-correct camouflage family as the Day 11 tz
#   direction and the Day 13 gap-direction traps.
# - rangeBetween bounds on a DATE orderBy are counted in DAYS (integers,
#   pure DSL, no expr needed). On a TIMESTAMP orderBy the same integers are
#   SECONDS. Same literal, different span — check the order column's type
#   before reading a frame.
# - A frame is also a value-offset LOOKUP: rangeBetween(-1, -1) is exactly
#   "the previous calendar day", and yields NULL (empty frame) when that
#   day has no row. lag(revenue, 1) is the previous ROW, which after a gap
#   is a different day entirely — the same ROWS-vs-RANGE distinction, worn
#   as a positional function. lag has no frame to fix (Day 13: positional
#   functions ignore frames), so on sparse data the fix is a range-framed
#   aggregate or a densified spine, never a frame on the lag.
# - Empty frame -> NULL, not 0. The COALESCE on prev_day_revenue is
#   load-bearing, unlike the many defensive COALESCEs the aggregate-NULL
#   rule lets you delete.
# - Guarding the division explicitly (WHEN prev > 0) rather than relying on
#   x/0 returning NULL: Spark returns NULL for division by zero only with
#   ANSI off; under ANSI it raises DIVIDE_BY_ZERO. Same Day 14 rule — a
#   NULL that comes from a failure mode is a session config, not a
#   guarantee.
# - Cost: one partitionBy expression -> one Exchange, shared by every
#   window in the query regardless of how many different frames sit on top
#   of it. Frames are evaluated inside the already-sorted partition; they
#   are free. Confirm by counting Exchange nodes in .explain().
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
