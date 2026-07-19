"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 10 — Complex aggregation: GROUPING SETS / ROLLUP / CUBE  (Medium)

PROBLEM
-------
A retailer logs unit sales per (region, category) line item. Build a
single multi-grain summary in ONE pass that reports total units at
four grains at once:

  1. per (region, category)          -- the base grain
  2. per region      (all categories subtotal)
  3. per category    (all regions subtotal)
  4. grand total     (all regions, all categories)

Emit one output table containing all four grains stacked together.
For every subtotal / grand-total row, the "rolled-up" dimension must
be reported as the literal string 'ALL' (not NULL), so a downstream
consumer can read the report without guessing which NULLs are
subtotals. A dimension value that is genuinely missing in the source
data must remain reported as its own distinct group, NOT collapsed
into 'ALL'.

Also emit a `level` column describing the grain of each row, one of:
  'region+category' | 'region' | 'category' | 'grand_total'

INPUT SCHEMA
------------
sales(region: string, category: string, units: int)
  -- region may be NULL for some rows (unlabeled/legacy source).
  -- category is always non-NULL in this dataset.

EXPECTED OUTPUT
---------------
summary(region: string, category: string, level: string, total_units: long)

  - region / category hold the actual dimension value at that grain,
    or the literal 'ALL' when that dimension is rolled up.
  - A genuinely-NULL source region stays a distinct group at the
    base and region grains (reported however you choose to surface a
    real NULL — see EXAMPLE), and must be DISTINGUISHABLE from the
    'ALL' subtotal rows.
  - total_units = SUM(units) at that grain.
  - Row order is not significant (check() sorts).

EXAMPLE
-------
Input:
  ('west', 'toys',   10)
  ('west', 'toys',    5)
  ('west', 'books',   3)
  ('east', 'toys',    7)
  (None,   'books',   4)   # genuinely missing region

Selected expected rows (base + subtotals + grand total):
  ('west', 'toys',   'region+category', 15)
  ('west', 'books',  'region+category',  3)
  ('east', 'toys',   'region+category',  7)
  (NULL_R, 'books',  'region+category',  4)   # real-NULL region group
  ('west', 'ALL',    'region',          18)
  ('east', 'ALL',    'region',           7)
  (NULL_R, 'ALL',    'region',           4)   # real-NULL region subtotal
  ('ALL',  'toys',   'category',         22)
  ('ALL',  'books',  'category',          7)
  ('ALL',  'ALL',    'grand_total',      29)

  NULL_R = how you choose to surface a genuinely-NULL region distinctly
  from 'ALL' (e.g. keep it NULL, or map to a sentinel like '<null>').
  The ONLY hard requirement: the real-NULL region rows must not be
  confusable with the 'ALL' subtotal rows.

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
    def solve_dsl(self, df: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: F.cube(...) / F.rollup(...) / GROUPING SETS via
              df.groupBy(...).agg(...) — but DSL has no grouping_sets
              helper; cube gives all 4 grains here, then you FILTER out
              the combinations you don't want if using cube.
              Hint: grouping(col) returns 1 when that col is rolled up
              in this row, 0 otherwise — use it to (a) build 'level',
              (b) replace rolled-up dims with 'ALL', and (c) keep a
              genuinely-NULL region distinct from a subtotal NULL.
        """
        # TODO: implement
        GROUPING_ORDER = ['region', 'category']
        res_df = (
            df.withColumn(
                'region',
                F.coalesce(
                    F.col('region'),
                    F.lit('<null>')))
            .cube('region', 'category')
            .agg(
                F.grouping_id(*GROUPING_ORDER).alias("gid"),
                F.sum('units').cast('long').alias('total_units')
            ).select(
                F.when(
                    F.col('gid').isin(2, 3), # gid 0 = region&category, gid 1 = region, gid 2 = category, gid 3 = ALL
                    F.lit('ALL'))
                .otherwise(F.col('region')).alias('region'),
                F.when(
                    F.col('gid').isin(1, 3), F.lit('ALL'))
                .otherwise(F.col('category')).alias('category'),
                F.when(F.col('gid') == 0, F.lit('region+category'))
                .when(F.col('gid') == 1, F.lit('region'))
                .when(F.col('gid') == 2, F.lit('category'))
                .otherwise(F.lit('grand_total')).alias('level'),
                'total_units',
            )
        )

        return res_df

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: GROUP BY GROUPING SETS ( (region,category), (region),
              (category), () ) gives EXACTLY the four grains with no
              extra filtering. GROUPING(col) / GROUPING_ID(...) tell a
              subtotal NULL from a real NULL.
        """
        df.createOrReplaceTempView("sales")

        sql = """
            -- write your SQL here
            WITH fill_na_region AS (
              SELECT
                COALESCE(region, '<null>') AS region,
                category,
                units
              FROM
                sales
            ),

          cube_units AS (
            SELECT
              region,
              category,
              GROUPING_ID(region, category) AS gid,
              SUM(units) AS total_units
            FROM
              fill_na_region
            GROUP BY CUBE(region, category)
          )

          SELECT
            CASE WHEN gid IN (2, 3) THEN 'ALL' ELSE region END AS region,
            CASE WHEN gid IN (1, 3) THEN 'ALL' ELSE category END AS category,
            CASE
              WHEN gid = 0 THEN 'region+category'
              WHEN gid = 1 THEN 'region'
              WHEN gid = 2 THEN 'category'
              ELSE 'grand_total' END AS level,
            total_units
          FROM
            cube_units;
        """
        return spark.sql(sql)
    
def ai_solve_dsl(df: DataFrame) -> DataFrame:
    g = df.groupBy(
        F.grouping_id().alias("gid"),
        F.grouping("region").alias("gr"),
        F.grouping("category").alias("gc"),
    )
    grouped = (
        df.cube("region", "category")
        .agg(
            F.sum("units").cast("long").alias("total_units"),
            F.grouping("region").alias("gr"),
            F.grouping("category").alias("gc"),
        )
    )
    region_out = F.when(F.col("gr") == 1, F.lit("ALL")).otherwise(F.col("region"))
    category_out = F.when(F.col("gc") == 1, F.lit("ALL")).otherwise(F.col("category"))
    level = (
        F.when((F.col("gr") == 0) & (F.col("gc") == 0), F.lit("region+category"))
        .when((F.col("gr") == 0) & (F.col("gc") == 1), F.lit("region"))
        .when((F.col("gr") == 1) & (F.col("gc") == 0), F.lit("category"))
        .otherwise(F.lit("grand_total"))
    )
    return grouped.select(
        region_out.alias("region"),
        category_out.alias("category"),
        level.alias("level"),
        F.col("total_units"),
    )


def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("sales")
    return spark.sql("""
        SELECT
            CASE WHEN GROUPING(region) = 1 THEN 'ALL' ELSE region END AS region,
            CASE WHEN GROUPING(category) = 1 THEN 'ALL' ELSE category END AS category,
            CASE
                WHEN GROUPING(region) = 0 AND GROUPING(category) = 0 THEN 'region+category'
                WHEN GROUPING(region) = 0 AND GROUPING(category) = 1 THEN 'region'
                WHEN GROUPING(region) = 1 AND GROUPING(category) = 0 THEN 'category'
                ELSE 'grand_total'
            END AS level,
            CAST(SUM(units) AS BIGINT) AS total_units
        FROM sales
        GROUP BY CUBE(region, category)
    """)


# =====================================================================
# Part 2 — Tests
# =====================================================================
def check(actual: DataFrame, expected_rows: list, label: str) -> None:
    """Order-insensitive comparison; sorted() absorbs tie-order instability."""
    actual_rows = sorted([tuple(r) for r in actual.collect()],
                         key=lambda t: tuple((x is None, x) for x in t))
    expected = sorted(expected_rows,
                      key=lambda t: tuple((x is None, x) for x in t))
    status = "PASS" if actual_rows == expected else "FAIL"
    print(f"[{status}] {label}")
    if status == "FAIL":
        print("  expected:", expected)
        print("  actual  :", actual_rows)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("daily-practice").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data = [
        ("west", "toys",  10),
        ("west", "toys",   5),
        ("west", "books",  3),
        ("east", "toys",   7),
        (None,   "books",  4),   # trap: genuinely-NULL region
    ]
    df = spark.createDataFrame(
        data, schema="region string, category string, units int")

    # Expected output. Convention chosen here: a genuinely-NULL region is
    # surfaced as the literal '<null>' so it is distinguishable from 'ALL'.
    # (If you keep it as NULL instead, adjust these expected tuples.)
    expected = [
        # base grain: region+category
        ("west",   "toys",  "region+category", 15),
        ("west",   "books", "region+category",  3),
        ("east",   "toys",  "region+category",  7),
        ("<null>", "books", "region+category",  4),
        # region subtotal
        ("west",   "ALL",   "region",          18),
        ("east",   "ALL",   "region",           7),
        ("<null>", "ALL",   "region",           4),
        # category subtotal
        ("ALL",    "toys",  "category",         22),
        ("ALL",    "books", "category",          7),
        # grand total
        ("ALL",    "ALL",   "grand_total",      29),
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
# [pass] Correctness — edge cases: ties? nulls? empty groups? duplicate keys?
#     notes:
# [pass] API usage — wrong signatures, deprecated calls, ambiguous column
#     references in joins, SQL syntax slips (trailing commas, quoting)?
#     notes:
# [pass] Performance — unnecessary shuffles, window over the whole table
#     without partitionBy, join strategy, redundant actions/collects?
#     notes:
# [pass] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes:
# [ ] Style/clarity — would you approve this in a real code review?
#     notes:I may deprecate first g = df.groupby snippet, I think this part is redundant, because the following codes just use result from GROUPING to handle 'region' and 'category' here, 
#     without result generated from GROUPING_ID
#
# VERDICT (commit before running): PASS — because: no mistakes
# ACTUAL RESULT (after Stage 4 run): FAILED because it does not handle Null in region, to replace by str '<null>'
# GAP ANALYSIS: did the run reveal anything your reading missed?: no, because the error is due to str but not code logic


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# ---- Route A: SQL GROUPING SETS (most precise — exactly 4 grains) ----
# def ref_solve_sql(spark, df):
#     df.createOrReplaceTempView("sales")
#     return spark.sql("""
#         SELECT
#             CASE WHEN GROUPING(region) = 1 THEN 'ALL'
#                  WHEN region IS NULL      THEN '<null>'   -- real NULL, kept distinct
#                  ELSE region END                                   AS region,
#             CASE WHEN GROUPING(category) = 1 THEN 'ALL'
#                  ELSE category END                                 AS category,
#             CASE GROUPING_ID(region, category)
#                  WHEN 0 THEN 'region+category'   -- 00: neither rolled up
#                  WHEN 1 THEN 'region'            -- 01: category rolled up
#                  WHEN 2 THEN 'category'          -- 10: region rolled up
#                  WHEN 3 THEN 'grand_total'       -- 11: both rolled up
#             END                                                    AS level,
#             CAST(SUM(units) AS BIGINT)                             AS total_units
#         FROM sales
#         GROUP BY GROUPING SETS (
#             (region, category),
#             (region),
#             (category),
#             ()
#         )
#     """)
#     # Key point: GROUPING(region)=1 is the ONLY thing that means "rolled
#     # up". region IS NULL alone is AMBIGUOUS on subtotal rows because the
#     # cube/rollup machinery also emits NULL there — GROUPING() disambiguates.
#     # GROUPING_ID bit order: leftmost arg = highest bit. Here region is bit1
#     # (value 2), category is bit0 (value 1).
#
# ---- Route B: DSL cube(), then filter to the 4 wanted grains ----
# def ref_solve_dsl(df):
#     # cube(region, category) emits ALL 2^2 = 4 combinations here, which
#     # happens to be exactly what we want, so no filtering needed. (With
#     # 3+ dims, cube over-generates and you'd filter, or use rollup, or
#     # do multiple groupBy+union — DSL has no grouping_sets helper.)
#     agg = (df.cube("region", "category")
#              .agg(F.sum("units").cast("long").alias("total_units"),
#                   F.grouping("region").alias("g_region"),
#                   F.grouping("category").alias("g_category")))
#     return (agg.select(
#         F.when(F.col("g_region") == 1, F.lit("ALL"))
#          .when(F.col("region").isNull(), F.lit("<null>"))
#          .otherwise(F.col("region")).alias("region"),
#         F.when(F.col("g_category") == 1, F.lit("ALL"))
#          .otherwise(F.col("category")).alias("category"),
#         F.when((F.col("g_region") == 0) & (F.col("g_category") == 0), F.lit("region+category"))
#          .when((F.col("g_region") == 0) & (F.col("g_category") == 1), F.lit("region"))
#          .when((F.col("g_region") == 1) & (F.col("g_category") == 0), F.lit("category"))
#          .otherwise(F.lit("grand_total")).alias("level"),
#         F.col("total_units")))
#     # NOTE: F.grouping(col) is the DSL equivalent of SQL GROUPING(col).
#     # F.grouping_id(*cols) also exists if you prefer the single-int form.
#     # Order of when() branches matters: test g_region==1 BEFORE the
#     # isNull() branch, else real-NULL and subtotal-NULL collapse together.
#
# ---- Route C (portable, no grouping sets): 4 explicit groupBy + union ----
# def ref_solve_dsl_union(df):
#     base = (df.groupBy("region", "category")
#               .agg(F.sum("units").cast("long").alias("total_units"))
#               .withColumn("level", F.lit("region+category")))
#     by_r = (df.groupBy("region")
#               .agg(F.sum("units").cast("long").alias("total_units"))
#               .withColumn("category", F.lit("ALL"))
#               .withColumn("level", F.lit("region")))
#     by_c = (df.groupBy("category")
#               .agg(F.sum("units").cast("long").alias("total_units"))
#               .withColumn("region", F.lit("ALL"))
#               .withColumn("level", F.lit("category")))
#     tot  = (df.agg(F.sum("units").cast("long").alias("total_units"))
#               .withColumn("region", F.lit("ALL"))
#               .withColumn("category", F.lit("ALL"))
#               .withColumn("level", F.lit("grand_total")))
#     # still need the '<null>' mapping on the real-NULL region here.
#     # Downside: scans the source 4x (no single-pass sharing); grouping
#     # sets / cube read the source ONCE and expand internally.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - GROUPING SETS is the precise tool: you list EXACTLY the grains you
#   want. ROLLUP (a,b) = hierarchical subset { (a,b),(a),() } — assumes a
#   drill-down hierarchy. CUBE (a,b) = ALL 2^n subsets. With 2 dims cube
#   == the 4 grains we want; with n dims cube = 2^n rows (combinatorial
#   blow-up) — prefer GROUPING SETS when you want specific grains only.
# - Single source scan: grouping sets / rollup / cube read the table ONCE
#   and expand the grouping internally; the 4-groupBy-union route scans
#   4x. Verify with .explain() (one Expand + one Exchange vs four scans).
# - GROUPING(col) = 1 iff col is rolled up (aggregated away) in this row,
#   else 0. This is the ONLY reliable "is this a subtotal" signal — a bare
#   `col IS NULL` cannot tell a real-NULL source value from the NULL the
#   rollup machinery injects on subtotal rows (echoes Day 7: once you
#   project a NULL, the origin is lost — here GROUPING() recovers it).
# - GROUPING_ID(c1,...,cn) packs the per-column grouping bits into one
#   int: leftmost arg is the HIGH bit. 0 = base grain, all-ones = grand
#   total. Handy for a single CASE mapping to a 'level' label.
# - DSL: F.cube / F.rollup + F.grouping / F.grouping_id. There is NO
#   df.grouping_sets() helper — reach for SQL when you need precise
#   grain control, or cube+filter, or explicit union.
# - Branch ORDER when reconstructing dims: test GROUPING()==1 (-> 'ALL')
#   BEFORE testing IS NULL (-> real-null sentinel), or the two NULL
#   sources collapse.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
