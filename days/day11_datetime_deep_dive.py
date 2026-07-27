"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 11 — Date/time deep dive: date-dimension left join + gap-filling  (Medium-Hard)

PROBLEM
-------
You run a small chain of stores. `sales` holds daily gross revenue per
store, recorded as event TIMESTAMPS in UTC. Management wants a DENSE
daily report per store: every calendar date in the reporting window
[2024-03-01, 2024-03-07] must appear for every store, INCLUDING days on
which a store made no sale (revenue 0). Revenue must be bucketed by the
store's LOCAL calendar date, not the UTC date.

Timestamps are stored in UTC. Each store has its own timezone (see the
`stores` dimension). "Which day a sale belongs to" is decided by the
store's LOCAL date after converting the UTC timestamp to that store's
timezone. A sale at 2024-03-02 02:30:00 UTC in a store at
'America/Los_Angeles' (UTC-8) belongs to LOCAL date 2024-03-01.

Produce, for every (store_id, date d in the 7-day window):
  - store_id
  - d                (the local calendar date, one row per store per day)
  - revenue          (SUM of amount for sales whose LOCAL date == d;
                      0.0 if no sale that local day)
  - n_txn            (number of sales that local day; 0 if none)

INPUT SCHEMA
------------
sales(txn_id: int, store_id: int, ts_utc: timestamp, amount: double)
stores(store_id: int, tz: string)

The reporting window is the 7 dates 2024-03-01 .. 2024-03-07 inclusive.
You may hardcode/derive the window bounds from these two literal dates.

EXPECTED OUTPUT
---------------
One row per (store_id, local_date) for EVERY store in `stores` and EVERY
date in the window — 7 rows per store. Columns in order:
    (store_id, d, revenue, n_txn)
where d is a date, revenue is double, n_txn is a bigint/long.
Order-insensitive (check() sorts).

EXAMPLE
-------
stores: (1, 'America/Los_Angeles'), (2, 'UTC')

A UTC sale for store 1 at 2024-03-02 02:30:00 converts to local
2024-03-01 18:30 -> counts on 2024-03-01. The same wall-clock UTC sale
for store 2 (tz UTC) counts on 2024-03-02.

Store 1's output still contains a row for 2024-03-05 with revenue 0.0
and n_txn 0 even if no sale landed on that local day.

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

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =====================================================================
# Part 1 — Solution (yours)
# =====================================================================
class Solution:
    def solve_dsl(self, sales: DataFrame, stores: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: build a store x date spine first (sequence of dates x stores),
              LEFT join the aggregated local-date sales onto it, then
              zero-fill. Convert ts_utc -> local date with from_utc_timestamp
              (ts, tz) then to_date. Watch which side the LEFT join keeps.
        """
        # TODO: implement
        # create date in row by sequence(date'2024-03-01', date'2024-03-07, interval 1 day)
        # stores left join sales on store_id

        # create date*store spine
        target_dates = F.sequence(
            F.lit('2024-03-01').cast('date'),
            F.lit('2024-03-07').cast('date'),
            F.expr('interval 1 day'))
        target_dates_with_store = (
            stores.select('store_id')
            .withColumn('local_date', F.explode(target_dates))
            .select(
                'store_id',
                'local_date'
            )
        )

        agg_daily_store = (
            stores.join(
                sales,
                on='store_id',
                how='inner')
            .withColumn('local_date',
                        F.to_date(
                            F.from_utc_timestamp(F.col('ts_utc'), F.col('tz')))
                        )
            .filter(
                (F.col('local_date') >= F.lit('2024-03-01')) & (F.col('local_date') <= F.lit("2024-03-07"))
                )
            .groupBy(
                'store_id',
                'local_date')
            .agg(
                F.sum('amount').alias('revenue'),
                F.count(F.lit(1)).alias('n_txn'))
        )

        res_df = (
            target_dates_with_store.join(
                agg_daily_store,
                on=['store_id', 'local_date'],
                how='left')
            .select(
                'store_id',
                'local_date',
                F.coalesce(F.col('revenue'), F.lit(0)).cast('double').alias('revenue'),
                F.coalesce(F.col('n_txn'), F.lit(0)).cast('int').alias('n_txn'),
            )
        )
        return res_df

    def solve_sql(self, spark: SparkSession, sales: DataFrame, stores: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: sequence(date'2024-03-01', date'2024-03-07', interval 1 day)
              + explode gives the date axis; CROSS JOIN stores for the spine.
              from_utc_timestamp(ts_utc, tz) then to_date for the local date.
        """
        sales.createOrReplaceTempView("sales")
        stores.createOrReplaceTempView("stores")

        sql = """
            -- write your SQL here
            WITH target_dates_with_store AS (
                SELECT
                    store_id,
                    local_date
                FROM
                    stores s
                LATERAL VIEW explode(sequence(DATE('2024-03-01'), DATE('2024-03-07'), interval 1 day)) tb AS local_date
                ),

            agg_daily_store AS (
                SELECT
                    sa.store_id,
                    TO_DATE(FROM_UTC_TIMESTAMP(sa.ts_utc, st.tz)) AS local_date,
                    SUM(amount) AS revenue,
                    COUNT(*) AS n_txn
                FROM
                    sales sa JOIN stores st ON sa.store_id = st.store_id
                WHERE 
                    TO_DATE(FROM_UTC_TIMESTAMP(sa.ts_utc, st.tz)) BETWEEN '2024-03-01' AND '2024-03-07'
                GROUP BY
                    sa.store_id,
                    TO_DATE(FROM_UTC_TIMESTAMP(sa.ts_utc, st.tz))
            ),

            res AS (
                SELECT
                    t.store_id,
                    t.local_date,
                    CAST(COALESCE(revenue, 0) AS DOUBLE) AS revenue,
                    CAST(COALESCE(n_txn, 0) AS BIGINT) AS n_txn
                FROM
                    target_dates_with_store t LEFT JOIN agg_daily_store a
                    ON t.store_id = a.store_id AND t.local_date = a.local_date

            )
            SELECT * FROM res;
        """
        return spark.sql(sql)
    
def ai_solve_dsl(sales: DataFrame, stores: DataFrame) -> DataFrame:
    dates = spark_range_dates(stores.sparkSession)

    scaffold = stores.select("store_id", "tz").crossJoin(dates)

    agg = (
        sales.join(stores, "store_id")
        .withColumn(
            "d",
            F.to_date(F.from_utc_timestamp(F.col("ts_utc"), F.col("tz")))
        )
        .filter((F.col("d") >= F.lit("2024-03-01")) & (F.col("d") <= F.lit("2024-03-07")))
        .groupBy("store_id", "d")
        .agg(
            F.sum("amount").alias("revenue"),
            F.count(F.lit(1)).alias("n_txn"),
        )
    )

    out = (
        scaffold.join(agg, ["store_id", "d"], "left")
        .select(
            "store_id",
            "d",
            F.coalesce(F.col("revenue"), F.lit(0.0)).cast("double").alias("revenue"),
            F.coalesce(F.col("n_txn"), F.lit(0)).cast("bigint").alias("n_txn"),
        )
    )
    return out


def spark_range_dates(spark: SparkSession) -> DataFrame:
    return (
        spark.range(0, 7)
        .select(F.date_add(F.lit("2024-03-01").cast("date"), F.col("id").cast("int")).alias("d"))
    )


def ai_solve_sql(spark: SparkSession, sales: DataFrame, stores: DataFrame) -> DataFrame:
    sales.createOrReplaceTempView("sales")
    stores.createOrReplaceTempView("stores")

    return spark.sql("""
        WITH dates AS (
            SELECT date_add(DATE'2024-03-01', CAST(i AS INT)) AS d
            FROM (SELECT explode(sequence(0, 6)) AS i)
        ),
        scaffold AS (
            SELECT s.store_id, s.tz, dt.d
            FROM stores s
            CROSS JOIN dates dt
        ),
        agg AS (
            SELECT
                sa.store_id,
                to_date(from_utc_timestamp(sa.ts_utc, st.tz)) AS d,
                SUM(sa.amount) AS revenue,
                COUNT(1) AS n_txn
            FROM sales sa
            JOIN stores st ON sa.store_id = st.store_id
            GROUP BY sa.store_id, to_date(from_utc_timestamp(sa.ts_utc, st.tz))
            HAVING d BETWEEN DATE'2024-03-01' AND DATE'2024-03-07'
        )
        SELECT
            sc.store_id,
            sc.d AS d,
            CAST(COALESCE(a.revenue, 0.0) AS DOUBLE) AS revenue,
            CAST(COALESCE(a.n_txn, 0) AS BIGINT) AS n_txn
        FROM scaffold sc
        LEFT JOIN agg a
            ON sc.store_id = a.store_id AND sc.d = a.d
    """)

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
    import datetime as dt

    spark = (
        SparkSession.builder.appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")  # pin session tz
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    def ts(s):
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

    def d(s):
        return dt.date.fromisoformat(s)

    # stores: 1 = LA (UTC-8, no DST inside window), 2 = UTC
    stores = spark.createDataFrame(
        [(1, "America/Los_Angeles"), (2, "UTC")],
        schema="store_id int, tz string",
    )

    # NOTE: ts_utc values are UTC wall-clock instants.
    sales_rows = [
        # store 1 (LA, UTC-8):
        # 02:30Z on 03-02 -> local 03-01 18:30  -> LOCAL 03-01   (TRAP: crosses day boundary)
        (101, 1, ts("2024-03-02 02:30:00"), 100.0),
        # 09:00Z on 03-01 -> local 03-01 01:00  -> LOCAL 03-01
        (102, 1, ts("2024-03-01 09:00:00"), 50.0),
        # 05:00Z on 03-04 -> local 03-03 21:00  -> LOCAL 03-03   (TRAP: back a day again)
        (103, 1, ts("2024-03-04 05:00:00"), 20.0),
        # store 2 (UTC): same wall clock as txn 101 but stays on its UTC date
        # 02:30Z on 03-02 -> local 03-02        -> LOCAL 03-02
        (201, 2, ts("2024-03-02 02:30:00"), 200.0),
        # 23:59Z on 03-07 -> local 03-07 (last day, in window)
        (202, 2, ts("2024-03-07 23:59:00"), 5.0),
        # out-of-window guard: 03-08 must NOT appear
        (203, 2, ts("2024-03-08 00:30:00"), 999.0),
    ]
    sales = spark.createDataFrame(
        sales_rows, schema="txn_id int, store_id int, ts_utc timestamp, amount double"
    )

    # Expected: 2 stores x 7 days = 14 rows. Zero-filled where no sale.
    # Store 1 (LA):
    #   03-01: txn101 (100) + txn102 (50) = 150.0, 2 txns
    #   03-03: txn103 (20) = 20.0, 1 txn
    #   others: 0
    # Store 2 (UTC):
    #   03-02: txn201 = 200.0, 1 txn
    #   03-07: txn202 = 5.0, 1 txn
    #   txn203 (03-08) out of window -> dropped
    #   others: 0
    expected = [
        (1, d("2024-03-01"), 150.0, 2),
        (1, d("2024-03-02"), 0.0, 0),
        (1, d("2024-03-03"), 20.0, 1),
        (1, d("2024-03-04"), 0.0, 0),
        (1, d("2024-03-05"), 0.0, 0),
        (1, d("2024-03-06"), 0.0, 0),
        (1, d("2024-03-07"), 0.0, 0),
        (2, d("2024-03-01"), 0.0, 0),
        (2, d("2024-03-02"), 200.0, 1),
        (2, d("2024-03-03"), 0.0, 0),
        (2, d("2024-03-04"), 0.0, 0),
        (2, d("2024-03-05"), 0.0, 0),
        (2, d("2024-03-06"), 0.0, 0),
        (2, d("2024-03-07"), 5.0, 1),
    ]

    s = Solution()
    check(s.solve_dsl(sales, stores), expected, "DSL")
    check(s.solve_sql(spark, sales, stores), expected, "SQL")

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    check(ai_solve_dsl(sales, stores), expected, "AI-DSL (post-review verification)")
    check(ai_solve_sql(spark, sales, stores), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review (NEW in v2)
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
#
# def ai_solve_dsl(sales: DataFrame, stores: DataFrame) -> DataFrame:
#     ...
#
# def ai_solve_sql(spark: SparkSession, sales: DataFrame, stores: DataFrame) -> DataFrame:
#     ...
#
# REVIEW_NOTES (fill in BEFORE running):
# ----------------------------------------------------------------------
# [pass] Correctness — edge cases: local-date conversion direction (UTC->local,
#     NOT local->UTC)? day-boundary crossing sales? zero-fill on no-sale days?
#     out-of-window sale excluded? window endpoints inclusive on both ends?
#     notes:
# [pass] API usage — from_utc_timestamp vs to_utc_timestamp direction; session
#     timeZone dependence of to_date/cast(date); sequence() bounds inclusive?
#     notes:
# [pass] Performance — spine built via explode(sequence) (narrow) vs a shuffle-y
#     cross join; is the sales aggregation done BEFORE the join to the spine?
#     notes:
# [pass] Robustness — DST assumptions; hardcoded window; what if a store has
#     zero sales at all (does it still get 7 rows)?
#     notes:
# [pass] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS / FAIL — because: PASS, no fatal issues here
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# ---- DSL route: explode(sequence) spine -> crossJoin stores -> LEFT join agg ----
# def ref_solve_dsl(sales, stores):
#     # 1) local date per sale
#     local = (
#         sales
#         .join(stores, "store_id")                         # attach tz (inner OK: every sale has a store)
#         .withColumn("d", F.to_date(F.from_utc_timestamp(F.col("ts_utc"), F.col("tz"))))
#     )
#     # 2) aggregate to (store_id, d) grain, window-filtered
#     agg = (
#         local
#         .filter((F.col("d") >= F.lit("2024-03-01")) & (F.col("d") <= F.lit("2024-03-07")))
#         .groupBy("store_id", "d")
#         .agg(F.sum("amount").alias("revenue"),
#              F.count(F.lit(1)).alias("n_txn"))
#     )
#     # 3) dense spine: dates x stores
#     dates = (
#         spark.range(1).select(
#             F.explode(F.sequence(F.lit("2024-03-01").cast("date"),
#                                  F.lit("2024-03-07").cast("date"),
#                                  F.expr("interval 1 day"))).alias("d")
#         )
#     )
#     spine = stores.select("store_id").crossJoin(dates)     # 2 x 7 = 14 rows
#     # 4) LEFT join agg onto spine (spine is LEFT so all 14 survive), zero-fill
#     out = (
#         spine.join(agg, ["store_id", "d"], "left")
#         .select(
#             "store_id", "d",
#             F.coalesce("revenue", F.lit(0.0)).alias("revenue"),
#             F.coalesce("n_txn", F.lit(0).cast("long")).alias("n_txn"),
#         )
#     )
#     return out
#
# ---- SQL route ----
# def ref_solve_sql(spark, sales, stores):
#     sales.createOrReplaceTempView("sales")
#     stores.createOrReplaceTempView("stores")
#     return spark.sql("""
#         WITH local AS (
#           SELECT s.store_id,
#                  to_date(from_utc_timestamp(s.ts_utc, st.tz)) AS d,
#                  s.amount
#           FROM sales s
#           JOIN stores st USING (store_id)
#         ),
#         agg AS (
#           SELECT store_id, d,
#                  SUM(amount) AS revenue,
#                  COUNT(*)    AS n_txn
#           FROM local
#           WHERE d BETWEEN date'2024-03-01' AND date'2024-03-07'
#           GROUP BY store_id, d
#         ),
#         spine AS (
#           SELECT st.store_id, d
#           FROM stores st
#           CROSS JOIN (
#             SELECT explode(sequence(date'2024-03-01', date'2024-03-07',
#                                     interval 1 day)) AS d
#           )
#         )
#         SELECT sp.store_id, sp.d,
#                COALESCE(a.revenue, 0.0)      AS revenue,
#                COALESCE(a.n_txn, CAST(0 AS BIGINT)) AS n_txn
#         FROM spine sp
#         LEFT JOIN agg a
#           ON sp.store_id = a.store_id AND sp.d = a.d
#     """)
#
# Note on the trap (see Part 5): the direction of conversion is the whole
# game. from_utc_timestamp(ts, tz) reads "ts is UTC, give me the wall clock
# in tz" — correct here. to_utc_timestamp goes the WRONG way and shifts LA
# sales +8h instead of -8h, silently moving txn101 to 03-02.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - from_utc_timestamp(ts, tz): interprets the instant as UTC and returns the
#   wall-clock time in `tz`. to_utc_timestamp is the inverse. For "UTC stored,
#   report in local", from_utc_timestamp is the direction. Getting it backwards
#   is a silent day-shift (passes on rows that don't straddle midnight).
# - to_date / cast(date) on a timestamp depends on spark.sql.session.timeZone.
#   Pin the session tz (here UTC) so the local date you already computed via
#   from_utc_timestamp isn't re-shifted a second time. Double-conversion is a
#   classic date-bucketing bug.
# - Gap-filling = build a dense SPINE (all keys x all dates), LEFT join the
#   sparse aggregate onto it, COALESCE the nulls to 0. The dimension-table
#   cousin of Day 2 gaps-and-islands: there you detected gaps, here you
#   manufacture the full axis so gaps can't hide.
# - sequence(start, stop, interval) is INCLUSIVE on both ends; explode turns
#   the array into rows. Narrow generation (no shuffle) — cheaper than a
#   range-join or a hand-unioned calendar.
# - crossJoin(stores, dates) makes the spine; keep it small (|stores| x |days|).
#   Aggregate sales to (store, day) BEFORE joining the spine so the join is
#   spine(small) LEFT agg(small) — never join raw sales to the spine.
# - Zero-fill both measures: revenue -> 0.0 (double), n_txn -> CAST(0 AS BIGINT)
#   to match COUNT's bigint type (schema hygiene, per API-style notes).
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
