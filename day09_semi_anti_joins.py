"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 9 — Semi / Anti Joins as Filter Idioms  (Difficulty: Easy-Medium)

PROBLEM
-------
Marketing wants a clean outreach list. From the customers table,
return every customer who:

  (a) has placed AT LEAST ONE order, AND
  (b) has NEVER filed a complaint.

Return one row per qualifying customer with their cust_id and country.
No columns from orders or complaints may appear in the output, and the
result must contain exactly one row per qualifying customer no matter
how many orders that customer has placed.

INPUT SCHEMA
------------
customers(cust_id: int, country: string)
orders(order_id: int, cust_id: int)
complaints(complaint_id: int, cust_id: int)   -- cust_id is nullable
                                                 (anonymous complaints)

EXPECTED OUTPUT
---------------
Columns, in this exact order:
  cust_id: int
  country: string
One row per qualifying customer. Output order does not matter.

EXAMPLE
-------
customers: (1,'US') (2,'CA') (3,'US')
orders:    (101,1) (102,1) (103,2)
complaints:(201,2)

-> customer 1 qualifies (has orders, no complaint) -> (1,'US')
   customer 2 is excluded (filed a complaint)
   customer 3 is excluded (never ordered)

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
    def solve_dsl(
        self, customers: DataFrame, orders: DataFrame, complaints: DataFrame
    ) -> DataFrame:
        """DataFrame API approach.

        Hint: two join types exist whose ONLY job is filtering the left
        side — neither ever emits right-side columns nor multiplies rows.
        """
        # TODO: 1
        # customer id exist in order table: inner join
        # customer id does not exist in complaint table: left_ant
        res_df = (
            customers.join(
                orders.select('cust_id'),
                on=(customers['cust_id'] == orders['cust_id']),
                how='left_semi'
            )
            .join(
                complaints.select('cust_id'),
                on='cust_id',
                how='left_anti'
            )
            .select(
                F.col('cust_id'),
                F.col('country')
            )
        )

        return res_df

    def solve_sql(
        self,
        spark: SparkSession,
        customers: DataFrame,
        orders: DataFrame,
        complaints: DataFrame,
    ) -> DataFrame:
        """SparkSQL approach.

        Hint: more than one SQL spelling exists for each half of the
        condition (join syntax vs subquery predicates). Pick one — but
        know what the alternatives do on THIS data before trusting them.
        """
        # implement using groupBy + having to retrieve customer who has at least one order and left anti join complaint；

        customers.createOrReplaceTempView("customers")
        orders.createOrReplaceTempView("orders")
        complaints.createOrReplaceTempView("complaints")

        sql = """
            -- write your SQL here
            SELECT
                /*+ BROADCAST(c) */
                cust_id,
                country
            FROM
                customers c
            WHERE NOT EXISTS (SELECT * FROM complaints cp WHERE c.cust_id = cp.cust_id )
                AND cust_id IN (SELECT cust_id FROM orders)
        """

        return spark.sql(sql)

def ai_solve_dsl(customers: DataFrame, orders: DataFrame,
                 complaints: DataFrame) -> DataFrame:
    has_order = orders.select("cust_id").distinct()
    complained = complaints.where(F.col("cust_id").isNotNull()) \
                           .select("cust_id").distinct()
    return (customers
            .join(has_order, "cust_id", "left_semi")
            .join(complained, "cust_id", "left_anti")
            .select("cust_id", "country"))


def ai_solve_sql(spark: SparkSession, customers: DataFrame,
                 orders: DataFrame, complaints: DataFrame) -> DataFrame:
    customers.createOrReplaceTempView("customers")
    orders.createOrReplaceTempView("orders")
    complaints.createOrReplaceTempView("complaints")
    return spark.sql("""
        SELECT c.cust_id, c.country
        FROM customers c
        WHERE c.cust_id IN (SELECT cust_id FROM orders)
          AND c.cust_id NOT IN (
              SELECT cust_id FROM complaints WHERE cust_id IS NOT NULL
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
    spark = (
        SparkSession.builder.appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    customers = spark.createDataFrame(
        [
            (1, "US"),
            (2, "CA"),
            (3, "US"),
            (4, "MX"),
            (5, "CA"),
        ],
        schema="cust_id int, country string",
    )

    orders = spark.createDataFrame(
        [
            (101, 1),
            (102, 1),          # customer 1: multiple orders
            (103, 2),
            (104, 3),
            (105, 3),
            (106, 3),          # customer 3: three orders
            # customers 4 and 5: never ordered
        ],
        schema="order_id int, cust_id int",
    )

    complaints = spark.createDataFrame(
        [
            (201, 2),          # customer 2 complained
            (202, None),       # anonymous complaint (cust_id IS NULL)
        ],
        schema="complaint_id int, cust_id int",
    )

    expected = [
        (1, "US"),
        (3, "US"),
    ]

    s = Solution()
    check(s.solve_dsl(customers, orders, complaints), expected, "DSL")
    sql_result = s.solve_sql(spark, customers, orders, complaints)
    check(sql_result, expected, "SQL")
    sql_result.explain()

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    check(ai_solve_dsl(customers, orders, complaints), expected,
          "AI-DSL (post-review verification)")
    check(ai_solve_sql(spark, customers, orders, complaints), expected,
          "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review (NEW in v2)
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
#
# def ai_solve_dsl(customers, orders, complaints) -> DataFrame:
#     ...
#
# def ai_solve_sql(spark, customers, orders, complaints) -> DataFrame:
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
#     notes: there are two distinct in DSL part, which I suppose is unnecessary shuffle, but join strategy is excellent, 
#       BTW, AI used isNotNull to avoid NULL-aware in NOT IN statement, this avoid null-check branch in Spark
# [pass] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes:
# [pass ] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS  — because: same logic as my solution but has a few differences 
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# --- Route A (DSL): the idiomatic two-step filter ---------------------
# def ref_solve_dsl(customers, orders, complaints):
#     return (
#         customers
#         .join(orders, on="cust_id", how="left_semi")
#         .join(complaints, on="cust_id", how="left_anti")
#         .select("cust_id", "country")
#     )
#
# Notes:
# - left_semi: keeps a customers row iff >=1 match in orders; never
#   duplicates rows, never emits orders columns. It is EXISTS, not a
#   join-then-distinct.
# - left_anti: keeps a customers row iff NO match in complaints.
#   The NULL cust_id row in complaints matches nothing (NULL = x is
#   never true), so it simply filters no one out — anti join is
#   NULL-safe by construction.
#
# --- Route B (SQL): EXISTS / NOT EXISTS (correlated subqueries) -------
# def ref_solve_sql(spark, customers, orders, complaints):
#     ... createOrReplaceTempView as in the scaffold ...
#     sql = """
#         SELECT c.cust_id, c.country
#         FROM customers c
#         WHERE EXISTS (
#                   SELECT 1 FROM orders o
#                   WHERE o.cust_id = c.cust_id
#               )
#           AND NOT EXISTS (
#                   SELECT 1 FROM complaints x
#                   WHERE x.cust_id = c.cust_id
#               )
#     """
#     return spark.sql(sql)
#
# --- Route C (SQL): explicit SEMI/ANTI join syntax --------------------
#     SELECT c.cust_id, c.country
#     FROM customers c
#     LEFT SEMI JOIN orders o     ON c.cust_id = o.cust_id
#     LEFT ANTI JOIN complaints x ON c.cust_id = x.cust_id
#   (Spark SQL supports LEFT SEMI / LEFT ANTI join syntax directly;
#    Catalyst compiles Routes B and C to the same LeftSemi/LeftAnti
#    logical operators — verify with explain().)
#
# --- The tempting-but-wrong spellings (study, do not use) -------------
# 1) cust_id NOT IN (SELECT cust_id FROM complaints)
#    Three-valued logic: if the subquery returns ANY NULL, `x NOT IN
#    (..., NULL)` is never TRUE (it is FALSE or UNKNOWN for every x),
#    so the whole query returns ZERO rows. IN/EXISTS for the positive
#    half are both fine; NOT IN vs NOT EXISTS are NOT equivalent under
#    NULLs.
# 2) inner join customers-orders, then filter:
#    duplicates customer 1 (x2) and customer 3 (x3); needs distinct/
#    dropDuplicates afterwards — extra aggregate, and easy to forget.
# 3) left join complaints + WHERE x.cust_id IS NULL:
#    correct here, and a portable classic (many engines lack anti join
#    syntax) — but it drags right-side columns through the plan and
#    the IS NULL filter must reference the RIGHT side's key, an easy
#    slip after Day 5's on="key" column-merging lesson.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - left_semi / left_anti are FILTERS wearing join syntax: left columns
#   only, no row multiplication, complementary semantics (EXISTS /
#   NOT EXISTS).
# - NOT IN + nullable subquery column is the classic three-valued-logic
#   footgun; NOT EXISTS / anti join are the NULL-safe spellings.
# - inner-join-as-existence-test changes the row grain (Day 4/6 lesson
#   in join clothing): every downstream count is suspect until you
#   dedupe back.
# - EXISTS subqueries, SEMI/ANTI join syntax, and DSL left_semi /
#   left_anti all compile to the same logical operators — check
#   explain() to confirm.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
