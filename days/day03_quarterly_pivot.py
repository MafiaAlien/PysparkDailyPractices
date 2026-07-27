"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 3 — Pivot (Rows to Columns) & Conditional Aggregation  (Difficulty: Medium)

PROBLEM
-------
Given a sales fact table at the grain of one row per sales record,
produce a quarterly revenue report with ONE ROW PER PRODUCT and one
column per quarter.

- Multiple records may exist for the same (product, quarter);
  their revenue must be SUMMED.
- A product may have no sales in some quarter; report 0 for that
  quarter (not NULL).
- Quarters are exactly 'Q1', 'Q2', 'Q3', 'Q4' — no other values
  appear in the data, and all four columns must be present in the
  output even if no product sold in that quarter.

INPUT SCHEMA
------------
sales(product: string, quarter: string, revenue: int)

EXPECTED OUTPUT
---------------
One row per product:
    product: string | Q1: int | Q2: int | Q3: int | Q4: int
Row order does not matter (the test compares order-insensitively).
Column order matters: product, Q1, Q2, Q3, Q4.

EXAMPLE
-------
Input sales:
    ('Widget', 'Q1', 100)
    ('Widget', 'Q1',  50)    <- same product+quarter, sum to 150
    ('Widget', 'Q2', 200)
    ('Widget', 'Q3',  80)
    ('Widget', 'Q4', 120)
    ('Gadget', 'Q1', 300)
    ('Gadget', 'Q3',  90)    <- Gadget has no Q2/Q4 -> report 0
    ('Gizmo',  'Q2',  60)    <- Gizmo only sold in Q2

Output:
    product | Q1  | Q2  | Q3 | Q4
    Widget  | 150 | 200 | 80 | 120
    Gadget  | 300 | 0   | 90 | 0
    Gizmo   | 0   | 60  | 0  | 0

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
    def solve_dsl(self, df: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: groupBy(...).pivot(...).agg(...). The pivot() call can
        take an explicit list of values — think about why you'd want
        to pass it. Then deal with the NULLs.
        """
        # TODO: implement
        df_res = df\
            .groupBy(F.col('product'))\
            .agg(
            F.sum(F.when(F.col('quarter') == 'Q1', F.col('revenue')).otherwise(0)).alias('Q1'),
            F.sum(F.when(F.col('quarter') == 'Q2', F.col('revenue')).otherwise(0)).alias('Q2'),
            F.sum(F.when(F.col('quarter') == 'Q3', F.col('revenue')).otherwise(0)).alias('Q3'),
            F.sum(F.when(F.col('quarter') == 'Q4', F.col('revenue')).otherwise(0)).alias('Q4'),
        )
            

        return df_res

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: two idioms exist — Spark SQL's PIVOT clause, or the
        classic conditional aggregation SUM(CASE WHEN ... THEN ... END)
        that works on every SQL engine. Pick one (or try both and
        compare their plans).
        """
        df.createOrReplaceTempView("sales")

        sql = """
            -- write your SQL here
            SELECT
                product,
                SUM(CASE WHEN quarter = 'Q1' THEN revenue ELSE 0 END) AS Q1,
                SUM(CASE WHEN quarter = 'Q2' THEN revenue ELSE 0 END) AS Q2,
                SUM(CASE WHEN quarter = 'Q3' THEN revenue ELSE 0 END) AS Q3,
                SUM(CASE WHEN quarter = 'Q4' THEN revenue ELSE 0 END) AS Q4
            FROM
                sales
            GROUP BY
                product;
        """
        return spark.sql(sql)
    QUARTERS = ["Q1", "Q2", "Q3", "Q4"]


    def ai_solve_dsl(self, df: DataFrame) -> DataFrame:
        QUARTERS = ["Q1", "Q2", "Q3", "Q4"]

        pivoted = (
            df.groupBy("product")
            .pivot("quarter", QUARTERS)          # explicit values => all 4 cols always exist
            .agg(F.sum("revenue"))
        )
        return pivoted.select(
            "product",
            *[F.coalesce(F.col(q), F.lit(0)).cast("int").alias(q) for q in QUARTERS],
        )


    def ai_solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView("sales")
        return spark.sql("""
            SELECT
                product,
                CAST(COALESCE(SUM(CASE WHEN quarter = 'Q1' THEN revenue END), 0) AS INT) AS Q1,
                CAST(COALESCE(SUM(CASE WHEN quarter = 'Q2' THEN revenue END), 0) AS INT) AS Q2,
                CAST(COALESCE(SUM(CASE WHEN quarter = 'Q3' THEN revenue END), 0) AS INT) AS Q3,
                CAST(COALESCE(SUM(CASE WHEN quarter = 'Q4' THEN revenue END), 0) AS INT) AS Q4
            FROM sales
            GROUP BY product
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

    data = [
        ("Widget", "Q1", 100),
        ("Widget", "Q1",  50),   # same product+quarter duplicate record
        ("Widget", "Q2", 200),
        ("Widget", "Q3",  80),
        ("Widget", "Q4", 120),
        ("Gadget", "Q1", 300),
        ("Gadget", "Q3",  90),
        ("Gizmo",  "Q2",  60),
    ]
    df = spark.createDataFrame(
        data, schema="product STRING, quarter STRING, revenue INT")

    expected = [
        ("Widget", 150, 200, 80, 120),
        ("Gadget", 300,   0, 90,   0),
        ("Gizmo",    0,  60,  0,   0),
    ]

    s = Solution()
    check(s.solve_dsl(df), expected, "DSL")
    check(s.solve_sql(spark, df), expected, "SQL")
    check(s.ai_solve_dsl(df), expected, "AI-DSL")
    check(s.ai_solve_sql(spark, df), expected, "AI-SQL")

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    # check(ai_solve_dsl(df), expected, "AI-DSL (post-review verification)")
    # check(ai_solve_sql(spark, df), expected, "AI-SQL (post-review verification)")

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
#     notes: no comments
# [pass] API usage — wrong signatures, deprecated calls, ambiguous column
#     references in joins, SQL syntax slips (trailing commas, quoting)?
#     notes: no comments
# [pass] Performance — unnecessary shuffles, window over the whole table
#     without partitionBy, join strategy, redundant actions/collects?
#     notes: no comments
# [fail] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes: both DSL and AI parts handle null values by coalescing for pivoted columns, which is diffrent from references solution here
# [pass] Style/clarity — would you approve this in a real code review?
#     notes: no comments
#
# VERDICT (commit before running): PASS — because:
# ACTUAL RESULT (after Stage 4 run): both AI solutions pass the same tests as the human solution.
# GAP ANALYSIS: did the run reveal anything your reading missed? no


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# --- DataFrame API (DSL) ---
#
# def ref_solve_dsl(df):
#     return (
#         df.groupBy("product")
#           .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"])
#           .agg(F.sum("revenue"))
#           .na.fill(0)
#           .select("product", "Q1", "Q2", "Q3", "Q4")
#     )
#
# --- SparkSQL, idiom A: PIVOT clause ---
#
# sql = """
#     SELECT product,
#            COALESCE(Q1, 0) AS Q1,
#            COALESCE(Q2, 0) AS Q2,
#            COALESCE(Q3, 0) AS Q3,
#            COALESCE(Q4, 0) AS Q4
#     FROM (
#         SELECT product, quarter, revenue FROM sales
#     )
#     PIVOT (
#         SUM(revenue)
#         FOR quarter IN ('Q1' AS Q1, 'Q2' AS Q2, 'Q3' AS Q3, 'Q4' AS Q4)
#     )
# """
#
# --- SparkSQL, idiom B: conditional aggregation (portable everywhere) ---
#
# sql = """
#     SELECT product,
#            SUM(CASE WHEN quarter = 'Q1' THEN revenue ELSE 0 END) AS Q1,
#            SUM(CASE WHEN quarter = 'Q2' THEN revenue ELSE 0 END) AS Q2,
#            SUM(CASE WHEN quarter = 'Q3' THEN revenue ELSE 0 END) AS Q3,
#            SUM(CASE WHEN quarter = 'Q4' THEN revenue ELSE 0 END) AS Q4
#     FROM sales
#     GROUP BY product
# """


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - pivot(col) vs pivot(col, values): without the explicit values list,
#   Spark must launch an EXTRA job first just to collect the distinct
#   pivot values before it can plan the aggregation. Passing the list
#   skips that job AND guarantees stable output columns (a missing
#   quarter still gets its column). In production, always pass it when
#   the domain is known and bounded.
# - Pivot produces NULL (not 0) for empty cells; decide explicitly with
#   na.fill / COALESCE. In idiom B, `ELSE 0` handles it inside the CASE
#   -- but note the subtle difference: CASE-with-ELSE-0 also turns a
#   product with NULL revenue into 0, while SUM alone would propagate
#   differently. Know which one your business logic wants.
# - Conditional aggregation (idiom B) is the portable fallback: PIVOT
#   syntax varies across engines (Spark/Oracle/MSSQL have it with
#   different flavors; PostgreSQL/MySQL don't), but SUM(CASE WHEN ...)
#   runs everywhere and its plan is a plain single-pass aggregation.
# - Both idioms need exactly one shuffle (by the groupBy key). Pivot is
#   not inherently more expensive than conditional aggregation once the
#   values list is explicit -- they compile to nearly the same plan.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
#  the AI solutions's DSL will have better extension and robustness to more values for pivot, e.g. if we handle other values but not quarters here,(quarter just have 4 fix max values),
#  however, if we need to process more values, what we need to do is adding values into the list and did not need to change other codes here
# - Did I catch it by reading, or only by running?
#  by reading, it is pretty obvious 
# - What review heuristic should I add to my checklist next time?
# - Other takeaways from this review?
#  1.chekc unessary code in my solution, e.g. in my dsl solution it does not need coalesce if I fill null value by .otherwise(0)
#  2. for DSL, using count(F.lit(1)) is better than count("*") because:
    # DSL 里的 F.count("*") 才是要避开的。 原因是这里的 "*" 不是 SQL 语法，而是一个字符串参数，Python API 要把它翻译成 Catalyst 表达式——大多数上下文翻译得很好，但你昨天撞上的 pivot 聚合就是翻译不了的角落。
    # F.count(F.lit(1)) 语义完全等价（每行贡献一个非 NULL 常量，即数行数），且在所有上下文都能解析,所以在 DSL 里统一用它，一劳永逸。
    # 总结成三行规则：

    # SQL：COUNT(*) ✓（标准、通用、无坑）
    # DSL：F.count(F.lit(1)) ✓（全上下文安全）
    # 两边都要警惕的是 count(某具体列)——它数的是该列非 NULL 的行数，和数行数是两个不同的业务含义，混用是隐性 bug 的常见来源。
#  3. "在非常规上下文（pivot/自定义聚合）里，对星号和通配表达式多留一个心眼"。
