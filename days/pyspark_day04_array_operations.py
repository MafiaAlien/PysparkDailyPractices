"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 4 — Array Columns: explode, collect, array functions  (Difficulty: Easy-Medium)

PROBLEM
-------
Each order carries an ARRAY of item names. Produce a per-customer
summary with:

  1. distinct_items : all DISTINCT items the customer has ever
     ordered, as an array sorted alphabetically (ascending);
  2. total_items    : the TOTAL number of items across all their
     orders, counting duplicates (an order of ['apple','apple']
     contributes 2).

- An order may contain duplicate items; duplicates count toward
  total_items but appear once in distinct_items.
- An order's items array may be EMPTY. A customer whose only
  order(s) have empty arrays must still appear in the result,
  with distinct_items = [] and total_items = 0.

INPUT SCHEMA
------------
orders(order_id: int, customer: string, items: array<string>)

EXPECTED OUTPUT
---------------
One row per customer:
    customer: string | distinct_items: array<string> | total_items: int
Row order does not matter (the test compares order-insensitively).

EXAMPLE
-------
Input orders:
    (1, 'alice', ['apple', 'banana'])
    (2, 'alice', ['banana', 'cherry'])
    (3, 'bob',   ['apple'])
    (4, 'bob',   ['apple', 'apple'])     <- duplicates within one order
    (5, 'carol', [])                     <- empty array!

Output:
    customer | distinct_items              | total_items
    alice    | [apple, banana, cherry]     | 4
    bob      | [apple]                     | 3
    carol    | []                          | 0

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

        Hint: two viable routes — (a) explode the array then aggregate,
        or (b) aggregate the arrays themselves with collect_list and
        the array_* function family, never exploding at all.
        If you pick (a), think hard about what explode() does to a
        row whose array is empty.
        """
        # TODO: implement
        df_res = df\
            .select(
                'order_id',
                'customer',
                F.explode_outer(F.col('items')).alias('item')
            ).groupBy(
                'customer'
            ).agg(
                F.sort_array(F.collect_set('item')).alias('distinct_items'),
                F.count(F.col('item')).alias('total_items')
            ).select(
                'customer',
                'distinct_items',
                'total_items'
            )

        return df_res

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: the same two routes exist in SQL. Functions worth
        knowing: EXPLODE / LATERAL VIEW, FLATTEN, ARRAY_DISTINCT,
        ARRAY_SORT, SIZE.
        """
        df.createOrReplaceTempView("orders")

        sql = """
            -- write your SQL here
            WITH exploded_item AS (
                SELECT
                    customer,
                    item
                FROM
                    orders o
                LATERAL VIEW OUTER EXPLODE(items) tb AS item
            )

            SELECT
                customer,
                SORT_ARRAY(COLLECT_SET(item)) AS distinct_items,
                COUNT(item) AS total_items
            FROM
                exploded_item e
            GROUP BY
                customer
        """

        return spark.sql(sql)
    def ai_solve_dsl(self, df: DataFrame) -> DataFrame:
    
        exploded = df.select("customer", F.explode_outer("items").alias("item"))
        return (
            exploded.groupBy("customer")
            .agg(
                F.array_sort(
                    F.collect_set(F.col("item"))  # collect_set 自动去重并忽略 null
                ).alias("distinct_items"),
                F.count(F.col("item")).cast("int").alias("total_items"),  # count 忽略 null
            )
            .select("customer", "distinct_items", "total_items")
        )


    def ai_solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView("orders")
        return spark.sql(
            """
            SELECT
                customer,
                array_sort(collect_set(item))        AS distinct_items,
                CAST(count(item) AS INT)             AS total_items
            FROM orders
            LATERAL VIEW OUTER explode(items) AS item
            GROUP BY customer
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
    spark = SparkSession.builder.appName("daily-practice").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data = [
        (1, "alice", ["apple", "banana"]),
        (2, "alice", ["banana", "cherry"]),
        (3, "bob",   ["apple"]),
        (4, "bob",   ["apple", "apple"]),   # duplicates within one order
        (5, "carol", []),                   # empty array
    ]
    df = spark.createDataFrame(
        data, schema="order_id INT, customer STRING, items ARRAY<STRING>"
    )

    expected = [
        ("alice", ["apple", "banana", "cherry"], 4),
        ("bob",   ["apple"],                     3),
        ("carol", [],                            0),
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
#     notes: using outer explode to handle NULL if there's no item in items, if business side needs to keep NULL records explode outer will be better
# [pass] API usage — wrong signatures, deprecated calls, ambiguous column
#     references in joins, SQL syntax slips (trailing commas, quoting)?
#     notes: LGTM
# [pass] Performance — unnecessary shuffles, window over the whole table
#     without partitionBy, join strategy, redundant actions/collects?
#     notes: LGTM
# [pass] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes: the AI solution used cast after count, I have no idea why 
# [pass] Style/clarity — would you approve this in a real code review?
#     notes: LGTM
#
# VERDICT (commit before running): PASS  — because: 
# ACTUAL RESULT (after Stage 4 run): passed
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# --- DataFrame API (DSL), route (b): no explode at all ---
#
# def ref_solve_dsl(df):
#     return (
#         df.groupBy("customer")
#           .agg(
#               F.array_sort(
#                   F.array_distinct(F.flatten(F.collect_list("items")))
#               ).alias("distinct_items"),
#               F.sum(F.size("items")).cast("int").alias("total_items"),
#           )
#     )
#
# Why this works for carol: collect_list gathers [[]] -> flatten -> []
# -> array_distinct/sort keep []; size([]) = 0 so the sum is 0.
# The row never disappears because we never exploded it away.
#
# --- DataFrame API (DSL), route (a): explode_outer ---
#
# def ref_solve_dsl_explode(df):
#     exploded = df.select(
#         "customer", F.explode_outer("items").alias("item")
#     )
#     return (
#         exploded.groupBy("customer")
#         .agg(
#             F.array_sort(
#                 F.filter(F.collect_set("item"), lambda x: x.isNotNull())
#             ).alias("distinct_items"),
#             F.count("item").cast("int").alias("total_items"),
#         )
#     )
#
# Two subtleties in route (a):
#   * plain explode() DROPS rows with empty (or NULL) arrays — carol
#     would vanish. explode_outer() keeps her with item = NULL.
#   * that NULL then has to be excluded from the distinct list
#     (hence the filter), while F.count("item") skips NULLs
#     automatically — for once, count-a-column's NULL-skipping is
#     exactly what we want, giving carol total_items = 0.
#
# --- SparkSQL, route (b) ---
#
# sql = """
#     SELECT customer,
#            ARRAY_SORT(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(items))))
#                AS distinct_items,
#            CAST(SUM(SIZE(items)) AS INT) AS total_items
#     FROM orders
#     GROUP BY customer
# """
#
# Caveat worth knowing: SIZE(NULL) returns -1 (legacy Hive behavior,
# controlled by spark.sql.legacy.sizeOfNull in older versions). No
# NULL arrays exist in this dataset, but in production guard with
# COALESCE(SIZE(items), 0) if the column is nullable.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - explode() silently DROPS rows whose array is empty or NULL — the
#   single most common bug with array columns. explode_outer() is the
#   row-preserving variant (emits one row with NULL).
# - You often don't need explode at all: collect_list of arrays +
#   flatten + array_distinct/array_sort/size does group-level array
#   math without multiplying the row count. Fewer rows through the
#   shuffle = cheaper, and no empty-array trap.
# - collect_list vs collect_set: list preserves duplicates and (within
#   a partition) order of arrival; set dedupes but the element order
#   is nondeterministic — always array_sort before comparing or
#   displaying.
# - SIZE(NULL) = -1, not 0 nor NULL (legacy quirk). COALESCE-guard
#   nullable array columns before summing sizes.
# - F.count(column) skips NULLs; usually a footgun (Day 3), but with
#   explode_outer it is precisely the tool that zeroes out the
#   empty-array customer.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
#  "聚合函数（count/sum/collect_list/collect_set/avg...）一律忽略 NULL 输入"是一条可以推平很多防御代码的通用规则
# 这里的 AI 方案在 DSL 中使用了 count(item).cast("int")的目的是，因为output schema要求了total_items是int类型，而count返回的是long类型，所以需要cast成int类型。
# 虽然在这个问题中没有影响，但在严格要求类型的场景下，这个cast是有必要的。