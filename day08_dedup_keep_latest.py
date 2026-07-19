"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 8 — Deduplication patterns: keep latest per key (Medium-Hard)

PROBLEM
-------
A product catalog receives update records from multiple upstream feeds.
The same product_id may appear many times. Build the CURRENT catalog:
for each product, keep exactly ONE row — the most recent update.

Rules:
  1. "Most recent" = highest updated_at.
  2. If two updates share the same updated_at, the one with the HIGHER
     source_seq wins (source_seq is a feed sequence number).
  3. The output must also report n_versions = the TOTAL number of
     update records received for that product (all rows, including
     the discarded ones and any exact-duplicate replays).

INPUT SCHEMA
------------
updates(product_id: int, product_name: string, price: double,
        updated_at: string,  -- 'yyyy-MM-dd HH:mm', lexicographic ==
                             -- chronological order
        source_seq: int)

EXPECTED OUTPUT
---------------
One row per product_id:
  (product_id, product_name, price, updated_at, n_versions)
where product_name / price / updated_at come from the SURVIVING row.
Row order does not matter (check() sorts); column order DOES matter.

EXAMPLE
-------
Input:
  (1, "Widget",  9.99, "2026-07-01 10:00", 101)
  (1, "Widget", 10.49, "2026-07-05 09:30", 102)
Output:
  (1, "Widget", 10.49, "2026-07-05 09:30", 2)

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

        Hint: "keep a SPECIFIC row per key" is the window row_number()=1
        pattern (see Day 1 / dedup takeaways). n_versions is a per-key
        TOTAL — think about which window it needs.
        """
        # TODO: implement
        window_updated_spec = Window.partitionBy('product_id')\
            .orderBy(F.col('updated_at').desc(), F.col('source_seq').desc())

        updated_df = (
            df.withColumn(
                'update_rk',
                F.row_number().over(window_updated_spec)
            )            
            .filter(F.col('update_rk') == 1)
            .select(
                'product_id',
                'product_name',
                'price',
                'updated_at',
                'update_rk',
            )
        )

        cnt_updates_df = (
            df.groupBy('product_id')
            .agg(F.count(F.lit(1)).alias('n_versions'))
        )

        res_df = (
            updated_df
            .join(
                cnt_updates_df,
                on='product_id',
                how='inner'
            )
            .select(
                'product_id',
                'product_name',
                'price',
                'updated_at',
                'n_versions'
            )
        )

        return res_df

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: window in a subquery/CTE, then filter. (Spark SQL has no
        QUALIFY clause — Databricks SQL does; know both for the exam.)
        """
        df.createOrReplaceTempView("updates")

        sql = """
            -- write your SQL here
            WITH updated_rk AS (
                SELECT
                    product_id,
                    product_name,
                    price,
                    updated_at,
                    ROW_NUMBER()OVER(partition by product_id order by updated_at DESC, source_seq DESC) as rk
                FROM
                    updates
                ),

            cnt_updated AS (
                SELECT
                    product_id,
                    COUNT(*) AS n_versions
                FROM
                    updates
                GROUP BY
                    product_id
            )

            SELECT
                u.product_id AS product_id,
                product_name,
                price,
                updated_at,
                n_versions
            FROM
                updated_rk u JOIN cnt_updated c ON u.product_id = c.product_id
            WHERE
                rk = 1
        """
        return spark.sql(sql)
    
def ai_solve_dsl(df: DataFrame) -> DataFrame:
    order_win = Window.partitionBy("product_id").orderBy(
        F.col("updated_at").desc(), F.col("source_seq").desc()
    )
    count_win = Window.partitionBy("product_id")

    return (
        df.withColumn("rn", F.row_number().over(order_win))
          .withColumn("n_versions", F.count(F.lit(1)).over(count_win).cast("int"))
          .filter(F.col("rn") == 1)
          .select("product_id", "product_name", "price", "updated_at", "n_versions")
    )


def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("updates")
    return spark.sql("""
        SELECT product_id,
               product_name,
               price,
               updated_at,
               n_versions
        FROM (
            SELECT product_id,
                   product_name,
                   price,
                   updated_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY product_id
                       ORDER BY updated_at DESC, source_seq DESC
                   ) AS rn,
                   CAST(COUNT(*) OVER (PARTITION BY product_id) AS INT) AS n_versions
            FROM updates
        )
        WHERE rn = 1
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
    spark = (
        SparkSession.builder.appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    data = [
        # P1: clean case — three straightforward versions
        (1, "Widget",     9.99, "2026-07-01 10:00", 101),
        (1, "Widget",    10.49, "2026-07-05 09:30", 102),
        (1, "Widget Pro", 10.99, "2026-07-10 14:00", 103),
        # P2: two updates share the SAME updated_at
        (2, "Gadget",     5.25, "2026-07-02 08:00", 200),
        (2, "Gadget",     5.00, "2026-07-08 12:00", 201),
        (2, "Gadget",     4.50, "2026-07-08 12:00", 205),
        # P3: single-version product
        (3, "Doohickey", 19.99, "2026-07-03 16:45", 300),
        # P4: exact duplicate replay (same feed record delivered twice)
        (4, "Gizmo",      7.75, "2026-07-06 11:20", 400),
        (4, "Gizmo",      7.75, "2026-07-06 11:20", 400),
    ]
    df = spark.createDataFrame(
        data,
        schema="product_id int, product_name string, price double, "
               "updated_at string, source_seq int",
    )

    expected = [
        (1, "Widget Pro", 10.99, "2026-07-10 14:00", 3),
        (2, "Gadget",      4.50, "2026-07-08 12:00", 3),
        (3, "Doohickey",  19.99, "2026-07-03 16:45", 1),
        (4, "Gizmo",       7.75, "2026-07-06 11:20", 2),
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
#     order_win = Window.partitionBy("product_id").orderBy(
    #     F.col("updated_at").desc(), F.col("source_seq").desc()
    # )
    # count_win = Window.partitionBy("product_id")

    # return (
    #     df.withColumn("rn", F.row_number().over(order_win))
    #       .withColumn("n_versions", F.count(F.lit(1)).over(count_win).cast("int"))
    #       .filter(F.col("rn") == 1)
    #       .select("product_id", "product_name", "price", "updated_at", "n_versions")
    # )
#
# def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
#     df.createOrReplaceTempView("updates")
    # return spark.sql("""
    #     SELECT product_id,
    #            product_name,
    #            price,
    #            updated_at,
    #            n_versions
    #     FROM (
    #         SELECT product_id,
    #                product_name,
    #                price,
    #                updated_at,
    #                ROW_NUMBER() OVER (
    #                    PARTITION BY product_id
    #                    ORDER BY updated_at DESC, source_seq DESC
    #                ) AS rn,
    #                CAST(COUNT(*) OVER (PARTITION BY product_id) AS INT) AS n_versions
    #         FROM updates
    #     )
    #     WHERE rn = 1
    # """)
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
# [pass] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS / FAIL — because: PASS
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# Route A — window row_number + unordered count window (most general):
#
# def ref_solve_dsl(df):
#     w_rank = Window.partitionBy("product_id").orderBy(
#         F.col("updated_at").desc(), F.col("source_seq").desc()
#     )
#     w_all = Window.partitionBy("product_id")   # NO orderBy: frame = whole
#                                                # partition -> true total
#     return (
#         df.withColumn("rn", F.row_number().over(w_rank))
#           .withColumn("n_versions", F.count(F.lit(1)).over(w_all))
#           .filter(F.col("rn") == 1)
#           .select("product_id", "product_name", "price",
#                   "updated_at", "n_versions")
#     )
#
# Route B — struct-argmax (Day 7 route b) + groupBy, no window at all:
#
# def ref_solve_dsl_b(df):
#     return (
#         df.groupBy("product_id")
#           .agg(
#               F.max(F.struct("updated_at", "source_seq",
#                              "product_name", "price")).alias("best"),
#               F.count(F.lit(1)).alias("n_versions"),
#           )
#           .select("product_id",
#                   F.col("best.product_name").alias("product_name"),
#                   F.col("best.price").alias("price"),
#                   F.col("best.updated_at").alias("updated_at"),
#                   F.col("n_versions").cast("int"))
#     )
#     # Both order keys are DESC ("take large"), so max(struct(...)) works
#     # with no negation; payload fields ride along AFTER the keys and are
#     # only compared if (updated_at, source_seq) fully ties — which for
#     # P4 is an exact-duplicate row, so any winner is identical.
#
# def ref_solve_sql(spark, df):
#     sql = """
#         WITH ranked AS (
#             SELECT *,
#                    ROW_NUMBER() OVER (
#                        PARTITION BY product_id
#                        ORDER BY updated_at DESC, source_seq DESC
#                    ) AS rn,
#                    COUNT(*) OVER (PARTITION BY product_id) AS n_versions
#             FROM updates
#         )
#         SELECT product_id, product_name, price, updated_at,
#                CAST(n_versions AS INT) AS n_versions
#         FROM ranked
#         WHERE rn = 1
#     """
#     return spark.sql(sql)


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - "Keep latest per key" REQUIRES the window route (or struct-argmax):
#   dropDuplicates(["product_id"]) keeps a NONDETERMINISTIC row, and
#   orderBy(...).dropDuplicates(...) is NOT a guaranteed contract —
#   it may appear to work locally and break on a real cluster.
# - A window with partitionBy but NO orderBy has a whole-partition
#   default frame; ADDING orderBy silently switches the default frame
#   to RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW — an
#   aggregate over an ORDERED window becomes a RUNNING aggregate.
#   Same function, different window spec, different semantics.
# - ROW_NUMBER's determinism is only as strong as its ORDER BY: every
#   tie left unresolved by the sort keys is an arbitrary choice.
#   A complete tie-break chain is part of the CONTRACT, not style.
# - Spark SQL has no QUALIFY; Databricks SQL supports it (exam-relevant
#   platform difference). Portable form: subquery/CTE + WHERE rn = 1.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
