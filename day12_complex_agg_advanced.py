"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 12 — Complex aggregation, advanced: GROUPING SETS precise control
         + conditional aggregation  (Medium-Hard)

PROBLEM
-------
You are given a flat table of orders. Produce a multi-grain summary in a
SINGLE pass over the source, at EXACTLY these three grains (and no others):

    1. (region, category)   -- the base grain
    2. (region)             -- category rolled up
    3. (category)           -- region rolled up

Do NOT emit a grand-total (both dimensions rolled up) row. Emit exactly
those three grains.

For every output row compute two measures:
    - total_amount : SUM(amount) over the rows in that grain
    - big_orders   : the NUMBER OF ORDERS whose amount >= 1000
                     (a threshold count, evaluated per grain)

Rolled-up dimensions must be labelled with the literal string 'ALL'
(not NULL). Add a `level` column identifying the grain of each row with
one of: 'region_category', 'region', 'category'. The level MUST be
derived from the grouping metadata, not from a bare IS NULL test on the
dimension column.

INPUT SCHEMA
------------
orders(region: string, category: string, amount: double)

EXPECTED OUTPUT
---------------
Columns, in this exact order:
    level (string), region (string), category (string),
    total_amount (double), big_orders (bigint)

Row order is not significant (check() sorts). Rolled-up dims are 'ALL'.

EXAMPLE
-------
For grain (region='East', category='A') with amounts [1200.0, 300.0]:
    level='region_category', region='East', category='A',
    total_amount=1500.0, big_orders=1   (only 1200 clears the threshold)

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

TRAP
----
One deliberate trap is planted in this problem. A naive-but-plausible
formulation of `big_orders` produces the RIGHT number on some grains and
the WRONG number on others — it passes casual inspection on the group
where the answer happens to coincide, and only the mismatched groups in
the test data expose it. (Not telling you which formulation.)
=====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =====================================================================
# Part 1 — Solution (yours)
# =====================================================================
class Solution:
    def solve_dsl(self, df: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: there is no df.grouping_sets() helper. Options: cube-then-
        filter the unwanted grand-total grain, or union three explicit
        groupBy branches. Rebuild rolled-up dims from grouping_id /
        grouping bits, NOT from IS NULL. Watch how you count big_orders.
        """
        # TODO: implement

        GROUPING = ['region', 'category']
        res_df = (
            df.cube(*GROUPING)
            .agg(
                # 0:region+cate, 1:region, 2:cate, 3:all(will deprecate)
                F.grouping_id(*GROUPING).alias('gid'),
                F.sum('amount').cast('long').alias('total_amount'),
                F.sum(
                    F.when(F.col('amount') >= 1000, F.lit(1)).otherwise(
                        F.lit(0))).cast('int').alias('big_orders')
            )
            .filter(~(F.col('gid') == 3))
            .select(
                F.when(F.col('gid') == 0, 'region_category')
                .when(F.col('gid') == 1, 'region')
                .otherwise('category').alias('level'),
                F.when(F.col('gid').isin(2), 'ALL').otherwise(
                    F.col('region')).alias('region'),
                F.when(F.col('gid').isin(1), 'ALL').otherwise(
                    F.col('category')).alias('category'),
                'total_amount',
                'big_orders'
            )
        )

        return res_df

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: GROUP BY GROUPING SETS ((region, category), (region),
        (category)) names exactly the grains you want. GROUPING(col) / a
        CASE on the grouping bits gives you both the 'ALL' label and the
        `level`. Threshold count is a conditional aggregate.
        """
        df.createOrReplaceTempView("orders")

        sql = """
            -- write your SQL here
            WITH grouping AS (
                SELECT
                    grouping_id(region, category) AS gid,
                    region,
                    category,
                    CAST(SUM(amount) AS DOUBLE) AS total_amount,
                    CAST(SUM(CASE WHEN amount >= 1000 THEN 1 ELSE 0 END) AS BIGINT) AS big_orders
                FROM
                    orders
                GROUP BY
                    region, category
                GROUPING SETS (
                    (region, category),
                    (region),
                    (category)
                )
            ),

            set_levels AS (
                SELECT
                    CASE
                        WHEN gid = 0 THEN 'region_category'
                        WHEN gid = 1 THEN 'region'
                        ELSE 'category' END AS level,
                    CASE WHEN gid = 2 THEN 'ALL' ELSE region END AS region, -- gid = 1 means region level, if region is NULL but gid = 2, which means it is ALL level in category col
                    CASE WHEN gid = 1 THEN 'ALL' ELSE category END AS category,  -- gid = 2 means cate level
                    total_amount,
                    big_orders
                FROM
                    grouping
            )

            SELECT * FROM set_levels;
        """
        return spark.sql(sql)
    
def ai_solve_dsl(df: DataFrame) -> DataFrame:
    grouped = df.groupBy(
        F.grouping_sets(
            [["region", "category"], ["region"], ["category"]],
            "region", "category"
        )
    ).agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("amount") >= 1000, 1).otherwise(0)).cast("bigint").alias("big_orders"),
        F.grouping("region").alias("gr_region"),
        F.grouping("category").alias("gr_category"),
    )

    return grouped.select(
        F.when((F.col("gr_region") == 0) & (F.col("gr_category") == 0), F.lit("region_category"))
         .when((F.col("gr_region") == 0) & (F.col("gr_category") == 1), F.lit("region"))
         .otherwise(F.lit("category")).alias("level"),
        F.when(F.col("gr_region") == 1, F.lit("ALL")).otherwise(F.col("region")).alias("region"),
        F.when(F.col("gr_category") == 1, F.lit("ALL")).otherwise(F.col("category")).alias("category"),
        F.col("total_amount"),
        F.col("big_orders"),
    )


def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("orders")
    return spark.sql("""
        SELECT
            CASE
                WHEN grouping(region) = 0 AND grouping(category) = 0 THEN 'region_category'
                WHEN grouping(region) = 0 AND grouping(category) = 1 THEN 'region'
                ELSE 'category'
            END AS level,
            CASE WHEN grouping(region) = 1 THEN 'ALL' ELSE region END AS region,
            CASE WHEN grouping(category) = 1 THEN 'ALL' ELSE category END AS category,
            SUM(amount) AS total_amount,
            CAST(SUM(CASE WHEN amount >= 1000 THEN 1 ELSE 0 END) AS BIGINT) AS big_orders
        FROM orders
        GROUP BY GROUPING SETS (
            (region, category),
            (region),
            (category)
        )
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
    spark = (SparkSession.builder
             .appName("daily-practice")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    data = [
        ("East", "A", 1200.0),
        ("East", "A",  300.0),
        ("East", "B", 1000.0),   # boundary: amount >= 1000 is a big order
        ("West", "A",  500.0),
        ("West", "B", 2000.0),
        ("West", "B",  800.0),
    ]
    df = spark.createDataFrame(
        data, schema="region string, category string, amount double")

    expected = [
        # level,              region, category, total_amount, big_orders
        ("region_category",  "East", "A",   1500.0, 1),
        ("region_category",  "East", "B",   1000.0, 1),
        ("region_category",  "West", "A",    500.0, 0),
        ("region_category",  "West", "B",   2800.0, 1),
        ("region",           "East", "ALL", 2500.0, 2),
        ("region",           "West", "ALL", 3300.0, 1),
        ("category",         "ALL",  "A",   2000.0, 1),
        ("category",         "ALL",  "B",   3800.0, 2),
    ]

    s = Solution()
    check(s.solve_dsl(df), expected, "DSL")
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
# [Pass] Correctness — threshold count formulation (COUNT vs SUM, ELSE 0 or 
#     no ELSE)? does it emit exactly 3 grains (no grand total, no missing
#     grain)? are rolled-up dims labelled from grouping bits, not IS NULL?
#     notes: logic is corrected 
# [Fail] API usage — grouping_id arg order vs the level/'ALL' mapping;
#     df.grouping_sets doesn't exist (DSL); trailing commas / quoting in SQL?
#     notes: syntax is not correct, groupingSet is new API from Spark 4.0 +, I did not think it will work here, and if groupingSets works 
#           it does not need groupBy here
# [Pass] Performance — single Expand + single Exchange, or multiple scans /
#     union branches / a needless grand-total-then-filter?
#     notes:
# [Pass] Robustness — behaviour if a source dim were genuinely NULL; bit-order
#     assumption baked into grouping_id.isin(...)?
#     notes:
# [Fail] Style/clarity — would you approve this in a real code review?
#     notes: if groupingSets works, it does not need grouping in SQL and DSL, level col can be generated against 
#           NULL logic of region and category this redundant part
#
# VERDICT (commit before running): FAIL — because: syntax error, grouping id does not work
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# --- DSL route: cube-then-filter (no grouping_sets helper in DSL) ------
# def ref_solve_dsl(df):
#     agg = (df.cube("region", "category")
#              .agg(
#                  F.sum("amount").alias("total_amount"),
#                  # big_orders: threshold count. Correct forms:
#                  #   SUM(when(cond,1).otherwise(0))   -- count the 1s
#                  #   COUNT(when(cond,1))              -- NO otherwise: NULL is skipped
#                  # WRONG: COUNT(when(cond,1).otherwise(0)) counts EVERY row
#                  #        (0 is non-NULL) -> returns the group size.
#                  F.sum(F.when(F.col("amount") >= 1000, 1).otherwise(0))
#                   .cast("long").alias("big_orders"),
#                  F.grouping_id("region", "category").alias("gid"),
#              )
#              # gid bits: region = high bit (2), category = low bit (1).
#              # 0=(region,category) 1=(region) 2=(category) 3=grand total.
#              .filter(F.col("gid") != 3))          # drop grand total grain
#     return (agg
#             # rebuild dims from grouping bits, NOT from IS NULL:
#             .withColumn("region",
#                         F.when(F.col("gid") == 2, F.lit("ALL")).otherwise(F.col("region")))
#             .withColumn("category",
#                         F.when(F.col("gid") == 1, F.lit("ALL")).otherwise(F.col("category")))
#             .withColumn("level",
#                         F.when(F.col("gid") == 0, F.lit("region_category"))
#                          .when(F.col("gid") == 1, F.lit("region"))
#                          .otherwise(F.lit("category")))
#             .select("level", "region", "category", "total_amount", "big_orders"))
#
# --- DSL route B: explicit union (fully precise, no wasted grain) ------
# def ref_solve_dsl_union(df):
#     big = F.sum(F.when(F.col("amount") >= 1000, 1).otherwise(0)).cast("long").alias("big_orders")
#     tot = F.sum("amount").alias("total_amount")
#     g_rc = (df.groupBy("region", "category").agg(tot, big)
#               .select(F.lit("region_category").alias("level"), "region", "category",
#                       "total_amount", "big_orders"))
#     g_r  = (df.groupBy("region").agg(tot, big)
#               .select(F.lit("region").alias("level"), "region",
#                       F.lit("ALL").alias("category"), "total_amount", "big_orders"))
#     g_c  = (df.groupBy("category").agg(tot, big)
#               .select(F.lit("category").alias("level"), F.lit("ALL").alias("region"),
#                       "category", "total_amount", "big_orders"))
#     return g_rc.unionByName(g_r).unionByName(g_c)
#     # Note: 3 scans + 3 Exchanges. The cube route scans ONCE (one Expand +
#     # one Exchange) but computes a 4th grain it throws away. On this tiny
#     # domain both are fine; at scale prefer the single-scan grouping-sets.
#
# --- SQL route: GROUPING SETS names exactly the wanted grains ----------
# def ref_solve_sql(spark, df):
#     df.createOrReplaceTempView("orders")
#     return spark.sql("""
#         SELECT
#             CASE grouping_id(region, category)
#                  WHEN 0 THEN 'region_category'
#                  WHEN 1 THEN 'region'
#                  WHEN 2 THEN 'category'
#             END                                              AS level,
#             CASE WHEN grouping(region)   = 1 THEN 'ALL' ELSE region   END AS region,
#             CASE WHEN grouping(category) = 1 THEN 'ALL' ELSE category END AS category,
#             SUM(amount)                                      AS total_amount,
#             CAST(SUM(CASE WHEN amount >= 1000 THEN 1 ELSE 0 END) AS BIGINT) AS big_orders
#         FROM orders
#         GROUP BY GROUPING SETS ((region, category), (region), (category))
#     """)
#     # grouping_id(region, category): region is the LEFT/high bit.
#     #   gid 0 -> base, 1 -> category rolled up (=> 'region' level),
#     #   2 -> region rolled up (=> 'category' level). No grand-total grain
#     #   is generated at all, so no filter is needed (vs the cube route).
#     # COUNT(CASE WHEN amount>=1000 THEN 1 END) (no ELSE) is an equally
#     #   correct big_orders; SUM(... ELSE 0) is shown for symmetry.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - GROUPING SETS names the exact grains -> no grand-total row is produced,
#   so no post-filter is needed. CUBE would emit the () grain you then have
#   to drop; ROLLUP(region,category) would give {(r,c),(r),()} — WRONG set
#   (has grand total, MISSING the (category) grain). Only GROUPING SETS (or
#   cube-then-filter, or a union) yields exactly {(r,c),(r),(c)}.
# - Threshold / conditional count trap: big_orders counts rows meeting a
#   predicate. COUNT(when(cond,1)) (no otherwise) works — the else-branch
#   NULL is skipped by COUNT. SUM(when(cond,1).otherwise(0)) works — sums
#   the 1s. But COUNT(when(cond,1).otherwise(0)) counts EVERY row (0 is
#   non-NULL) => returns the group SIZE. Same aggregate-NULL rule as the
#   COUNT(device.os) family: the 0-vs-NULL else-branch decides everything.
# - Rebuild rolled-up dims and the level label from GROUPING()/GROUPING_ID,
#   never from a bare IS NULL (would misread a genuinely-NULL source dim as
#   a subtotal). grouping_id arg order fixes the bit weights: leftmost =
#   high bit; pin the CASE map to the ARG order, not table column order.
# - Single-scan: grouping sets / cube -> one Expand + one Exchange +
#   HashAggregate pair. The 3-way union scans the source 3x with 3
#   Exchanges. Verify by counting Expand/Exchange in .explain().
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
#   API hallucination——AI 会发明和 SQL 关键字对仗的假 DSL 函数(F.grouping_sets),读代码时对"没见过但名字太顺"的 F.* 调用要查证,别默认存在。
