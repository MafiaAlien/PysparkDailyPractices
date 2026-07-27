"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 5 — Join Strategies & Broadcast Join  (Difficulty: Medium)

PROBLEM
-------
A large transaction fact table must be enriched with a small store
dimension, then aggregated to region level.

  1. Compute total transaction amount PER REGION.
  2. A transaction whose store_id does not exist in the dimension
     must NOT be dropped — report it under region 'UNKNOWN'.
  3. Regions with no transactions must NOT appear.
  4. The dimension table is tiny (a few hundred rows in production,
     while transactions are billions of rows). Your join MUST use
     the BROADCAST strategy, explicitly requested in code — do not
     rely on the optimizer choosing it by luck:
       - DSL: use F.broadcast(...) on the dimension side
       - SQL: use the /*+ BROADCAST(alias) */ hint

OBSERVATION TASK (part of the exercise, answer in comments):
  After your tests pass, call .explain() on your DSL result and
  find the join node in the physical plan.
  (a) What join strategy does the plan show?
  (b) Remove F.broadcast() and explain() again — does the strategy
      change? Why might it NOT change on this tiny local dataset
      even without the hint? (hint: autoBroadcastJoinThreshold)

INPUT SCHEMA
------------
transactions(txn_id: int, store_id: int, amount: int)
stores(store_id: int, region: string)

EXPECTED OUTPUT
---------------
One row per region present in the transactions (plus 'UNKNOWN' if
any orphan transactions exist):
    region: string | total_amount: int
Row order does not matter (the test compares order-insensitively).

EXAMPLE
-------
Input transactions:
    (1, 10, 100)
    (2, 10,  50)
    (3, 20,  80)
    (4, 99,  40)     <- store 99 missing from the dimension
Input stores:
    (10, 'West')
    (20, 'East')
    (30, 'North')    <- no transactions -> must not appear

Output:
    region  | total_amount
    West    | 150
    East    | 80
    UNKNOWN | 40

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


# =====================================================================
# Part 1 — Solution (yours)
# =====================================================================
class Solution:
    def solve_dsl(self, txns: DataFrame, stores: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: which join type preserves the orphan transactions?
        Where does the NULL region come from, and where do you turn
        it into 'UNKNOWN'? Wrap the dimension in F.broadcast().
        """
        # TODO: implement
        df_res = txns.join(
            F.broadcast(stores),
            # stores,
            on='store_id',
            how='left'
        ).select(
            F.coalesce(
                F.col('region'),
                F.lit('UNKNOWN'))
            .alias('region'),
            'amount'
        )\
            .groupBy('region').agg(F.sum('amount').alias('total_amount'))

        return df_res

    def solve_sql(self, spark: SparkSession,
                  txns: DataFrame, stores: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: the hint comment goes right after SELECT:
        SELECT /*+ BROADCAST(s) */ ...  — the alias must match the
        one you give the dimension table in FROM/JOIN.
        """
        txns.createOrReplaceTempView("transactions")
        stores.createOrReplaceTempView("stores")

        sql = """
            -- write your SQL here
            WITH joint_tables AS(
                SELECT
                    /*+ BROADCAST(s) */
                    t.store_id AS store_id,
                    COALESCE(region, 'UNKNOWN') AS region,
                    amount
                FROM
                    transactions t LEFT JOIN stores s ON t.store_id = s.store_id
                )

            SELECT
                region,
                SUM(amount) AS total_amount
            FROM
                joint_tables
            GROUP BY
                region;
        """
        return spark.sql(sql)
    def ai_solve_dsl(self, txns: DataFrame, stores: DataFrame) -> DataFrame:
      return (
        txns.join(F.broadcast(stores), on="store_id", how="left")
        .withColumn("region", F.coalesce(F.col("region"), F.lit("UNKNOWN")))
        .groupBy("region")
        .agg(F.sum("amount").alias("total_amount"))
        .select("region", "total_amount")
    )

    def ai_solve_sql(self, spark: SparkSession, txns: DataFrame, stores: DataFrame) -> DataFrame:
        txns.createOrReplaceTempView("transactions")
        stores.createOrReplaceTempView("stores")
        return spark.sql("""
            SELECT /*+ BROADCAST(s) */
                COALESCE(s.region, 'UNKNOWN') AS region,
                SUM(t.amount) AS total_amount
            FROM transactions t
            LEFT JOIN stores s
            ON t.store_id = s.store_id
            GROUP BY COALESCE(s.region, 'UNKNOWN')
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
    spark = SparkSession.builder.appName("daily-practice").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    txns = spark.createDataFrame(
        [
            (1, 10, 100),
            (2, 10,  50),
            (3, 20,  80),
            (4, 99,  40),   # orphan store_id
        ],
        schema="txn_id INT, store_id INT, amount INT",
    )

    stores = spark.createDataFrame(
        [
            (10, "West"),
            (20, "East"),
            (30, "North"),  # region with no transactions
        ],
        schema="store_id INT, region STRING",
    )

    expected = [
        ("West",    150),
        ("East",     80),
        ("UNKNOWN",  40),
    ]

    s = Solution()
    result = s.solve_dsl(txns, stores)
    check(result, expected, "DSL")
    result.explain()
    check(s.solve_sql(spark, txns, stores), expected, "SQL")
    
    def ai_solve_dsl(txns: DataFrame, stores: DataFrame) -> DataFrame:
      return (
        txns.join(F.broadcast(stores), on="store_id", how="left")
        .withColumn("region", F.coalesce(F.col("region"), F.lit("UNKNOWN")))
        .groupBy("region")
        .agg(F.sum("amount").alias("total_amount"))
        .select("region", "total_amount")
    )

    def ai_solve_sql(spark: SparkSession, txns: DataFrame, stores: DataFrame) -> DataFrame:
        txns.createOrReplaceTempView("transactions")
        stores.createOrReplaceTempView("stores")
        return spark.sql("""
            SELECT /*+ BROADCAST(s) */
                COALESCE(s.region, 'UNKNOWN') AS region,
                SUM(t.amount) AS total_amount
            FROM transactions t
            LEFT JOIN stores s
            ON t.store_id = s.store_id
            GROUP BY COALESCE(s.region, 'UNKNOWN')
        """)

    # ------------------------------------------------------------------
    # OBSERVATION TASK — after tests pass:
    # s.solve_dsl(txns, stores).explain()
    #     == Physical Plan ==
    # AdaptiveSparkPlan isFinalPlan=false
    # +- HashAggregate(keys=[region#5], functions=[sum(amount#2)])
    #    +- Exchange hashpartitioning(region#5, 200), ENSURE_REQUIREMENTS, [plan_id=36]
    #       +- HashAggregate(keys=[region#5], functions=[partial_sum(amount#2)])
    #          +- Project [coalesce(region#4, UNKNOWN) AS region#5, amount#2]
    #             +- BroadcastHashJoin [store_id#1], [store_id#3], LeftOuter, BuildRight, false
    #                :- Project [store_id#1, amount#2]
    #                :  +- Scan ExistingRDD[txn_id#0,store_id#1,amount#2]
    #                +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=31]
    #                   +- Filter isnotnull(store_id#3)
    #                      +- Scan ExistingRDD[store_id#3,region#4]
    # ANSWER (a): BroadcastHashJoin instead of SortMergeJoin, with a BroadcastExchange on the stores side.
    # ANSWER (b): it will use SortMergeJoin, but I believe I did not enable AQE, if AQE enabled, it will use BroadcastHashJoin if partition number is not beyond the threshold.
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    check(ai_solve_dsl(txns, stores), expected, "AI-DSL (post-review verification)")
    check(ai_solve_sql(spark, txns, stores), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review (NEW in v2)
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
#
def ai_solve_dsl(txns: DataFrame, stores: DataFrame) -> DataFrame:
      return (
        txns.join(F.broadcast(stores), on="store_id", how="left")
        .withColumn("region", F.coalesce(F.col("region"), F.lit("UNKNOWN")))
        .groupBy("region")
        .agg(F.sum("amount").alias("total_amount"))
        .select("region", "total_amount")
    )

def ai_solve_sql(spark: SparkSession, txns: DataFrame, stores: DataFrame) -> DataFrame:
    txns.createOrReplaceTempView("transactions")
    stores.createOrReplaceTempView("stores")
    return spark.sql("""
        SELECT /*+ BROADCAST(s) */
               COALESCE(s.region, 'UNKNOWN') AS region,
               SUM(t.amount) AS total_amount
        FROM transactions t
        LEFT JOIN stores s
          ON t.store_id = s.store_id
        GROUP BY COALESCE(s.region, 'UNKNOWN')
    """)
# REVIEW_NOTES (fill in BEFORE running):
# ----------------------------------------------------------------------
# [pass] Correctness — edge cases: ties? nulls? empty groups? duplicate keys?
#     notes: COALESCE handles NULLs, and LEFT JOIN keep ensure all records will not be filtered out; 
# [pass] API usage — wrong signatures, deprecated calls, ambiguous column
#     references in joins, SQL syntax slips (trailing commas, quoting)?
#     notes: LGTM
# [pass] Performance — unnecessary shuffles, window over the whole table
#     without partitionBy, join strategy, redundant actions/collects?
#     notes: pass, the AI enable broadcast both in DSL and SQL, which is the correct approach for this problem.
#     Or AQE will automatically use broadcast join but need one more shuffle;
# [pass] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes: no hardecoded values, the AI solution is robust and will work for any input data.
# [pass] Style/clarity — would you approve this in a real code review?
#     notes: LGTM
#
# VERDICT (commit before running): PASS — because: same logic as my solution;
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# --- DataFrame API (DSL) ---
#
# def ref_solve_dsl(txns, stores):
#     return (
#         txns.join(F.broadcast(stores), on="store_id", how="left")
#             .withColumn("region", F.coalesce("region", F.lit("UNKNOWN")))
#             .groupBy("region")
#             .agg(F.sum("amount").cast("int").alias("total_amount"))
#     )
#
# Notes:
#   * LEFT join keeps orphan transactions (region comes back NULL);
#     INNER would silently drop store 99's 40 dollars — a classic
#     "revenue went missing" production bug.
#   * coalesce AFTER the join, BEFORE the groupBy: the NULL region
#     must become 'UNKNOWN' before it is used as a grouping key,
#     otherwise you get a NULL-keyed group (works, but then the
#     rename happens post-aggregation — also fine, just choose one).
#   * F.broadcast() wraps the DIMENSION (small side), never the fact.
#
# --- SparkSQL ---
#
# sql = """
#     SELECT /*+ BROADCAST(s) */
#            COALESCE(s.region, 'UNKNOWN') AS region,
#            CAST(SUM(t.amount) AS INT)    AS total_amount
#     FROM transactions t
#     LEFT JOIN stores s
#       ON t.store_id = s.store_id
#     GROUP BY COALESCE(s.region, 'UNKNOWN')
# """
#
# The hint takes the TABLE ALIAS (s), not the table name, when an
# alias exists. GROUP BY repeats the COALESCE expression (or group
# by 1 / use a subquery) — grouping by raw s.region would create a
# separate NULL group and the COALESCE in SELECT would then be
# applied per-group, which happens to work here but reads badly.
#
# --- Observation task answers ---
# (a) The plan shows BroadcastHashJoin, with a BroadcastExchange on
#     the stores side.
# (b) On this dataset the strategy usually does NOT change even
#     without the hint: both tables are far below
#     spark.sql.autoBroadcastJoinThreshold (default 10MB), so the
#     optimizer auto-broadcasts. The hint matters in production when
#     the dimension exceeds the threshold (or stats are missing/
#     stale, e.g. right after a table is written without ANALYZE),
#     where the planner would otherwise fall back to SortMergeJoin.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - Spark's three main join strategies:
#     BroadcastHashJoin — ship the small side to every executor;
#       no shuffle of the big side; needs the small side to fit in
#       memory (driver + executors).
#     SortMergeJoin — shuffle BOTH sides by key, sort, merge; the
#       default for two large tables; robust but shuffle-heavy.
#     ShuffleHashJoin — shuffle both sides but hash instead of sort;
#       chosen when one side is much smaller per-partition.
# - autoBroadcastJoinThreshold (default 10MB) controls automatic
#   broadcasting; -1 disables it. AQE (3.x) can also convert a
#   sort-merge join to broadcast AT RUNTIME once it sees actual
#   sizes. Explicit F.broadcast()/hints document intent and protect
#   against missing/stale statistics.
# - Broadcasting the WRONG (large) side is a real failure mode:
#   driver OOM or "Broadcast exceeds spark.sql.maxBroadcastTableSize".
#   The hint is a promise you make about data size — own it.
# - LEFT join + COALESCE(dim_col, 'UNKNOWN') is the standard
#   "unmatched fact rows must survive enrichment" pattern; inner
#   join here silently loses revenue.
# - Reading explain(): find the join node name and which side has
#   the BroadcastExchange. Being able to narrate a physical plan is
#   a strong interview signal.
#
# Review takeaways (Day 5):
# - AI solution vs mine: functionally identical. Differences are
#   structural — AI repeated COALESCE in GROUP BY (single-block form),
#   I isolated the transform in a CTE so GROUP BY references the
#   already-coalesced column. Both correct; CTE form eliminates the
#   expression-drift risk (SELECT/GROUP BY copies diverging) by
#   construction.
# - GROUP BY name resolution: input columns take precedence over
#   SELECT aliases (spark.sql.groupByAliases). "GROUP BY region" in a
#   single block silently binds to raw s.region, not the coalesced
#   alias — happens to work here, semantic trap in general. Alias-in-
#   GROUP-BY is also non-portable (SQL Server/Oracle reject it).
# - Caught by reading: broadcast wraps the dimension side in both AI
#   functions; hint alias matches. Verified physically, not assumed:
#   explain() on the executed DataFrame.
# - Run revealed what reading couldn't: auto-broadcast NEVER fired in
#   the static plan because Scan ExistingRDD carries no size stats
#   (missing stats = treated as infinitely large); AQE converted
#   SMJ -> BHJ at runtime, visible only in isFinalPlan=true after an
#   action on the SAME DataFrame object. Cost order confirmed:
#   explicit broadcast < AQE conversion < full SMJ.
# - New review heuristics: (1) for any SQL with expressions in both
#   SELECT and GROUP BY, diff the two copies character-by-character;
#   (2) physical-plan claims require an executed plan, not explain()
#   on a fresh lazy DataFrame.
