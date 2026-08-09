"""
=====================================================================
PySpark Daily Practice — Template v2 (Claude Code edition)
=====================================================================
Day 19 — Higher-order array functions  (Medium)

PROBLEM
-------
An order service stores each order's line items denormalized, as an
ARRAY of STRUCTs on the order row. Every element carries the SKU, the
quantity, the unit price, a line status ('ACTIVE' or 'CANCELLED') and an
availability flag.

Produce one summary row per order. A line item counts toward the summary
only if its status is 'ACTIVE'; CANCELLED lines are invisible to every
measure below.

For each order emit, in this column order:

  order_id       the order key, unchanged
  n_active       how many ACTIVE line items the order has
  active_total   SUM(qty * unit_price) over the ACTIVE line items;
                 an order with no ACTIVE line items reports 0.0
  has_bulk       TRUE if AT LEAST ONE ACTIVE line item has qty >= 10,
                 otherwise FALSE
  all_in_stock   TRUE only if the order has AT LEAST ONE ACTIVE line item
                 AND every one of its ACTIVE line items has in_stock = true.
                 In every other case, FALSE.

Constraint for this exercise: solve it with higher-order array functions
operating on the array in place (transform / filter / exists / forall /
aggregate / size ...). Do not explode into one row per line item and
group back up — that route is the subject of the reference discussion,
not of your solution.

Every order has at least one line item; `items` is never NULL and never
an empty array; no struct field is ever NULL.

INPUT SCHEMA
------------
orders(
    order_id: string, 
    items: array<struct<
        sku: string,
        qty: int,
        unit_price: double,
        status: string,      -- 'ACTIVE' | 'CANCELLED'
        in_stock: boolean
    >>
)

EXPECTED OUTPUT
---------------
order_id: string, n_active: int, active_total: double,
has_bulk: boolean, all_in_stock: boolean

One row per input order. Row order does not matter — check() sorts.

EXAMPLE
-------
orders  (as df.show(truncate=False) renders it):

    +--------+---------------------------------------------------------+
    |order_id|items                                                    |
    +--------+---------------------------------------------------------+
    |O1      |[{A1, 2, 10.0, ACTIVE, true}, {A2, 3, 5.0, ACTIVE, true}]|
    |O5      |[{E1, 10, 7.5, ACTIVE, false}]                           |
    +--------+---------------------------------------------------------+

Same two rows, with the array expanded one line per element so the
struct fields line up under their names (this is a reading aid — the
table really is one row per order):

    +----------+-----+-----+------------+-----------+----------+
    | order_id | sku | qty | unit_price | status    | in_stock |
    +----------+-----+-----+------------+-----------+----------+
    | O1       | A1  | 2   | 10.0       | ACTIVE    | true     |
    | O1       | A2  | 3   | 5.0        | ACTIVE    | true     |
    | O5       | E1  | 10  | 7.5        | ACTIVE    | false    |
    +----------+-----+-----+------------+-----------+----------+

Expected for those two orders:

    +----------+----------+--------------+----------+--------------+
    | order_id | n_active | active_total | has_bulk | all_in_stock |
    +----------+----------+--------------+----------+--------------+
    | O1       | 2        | 35.0         | false    | true         |
    | O5       | 1        | 75.0         | true     | false        |
    +----------+----------+--------------+----------+--------------+

Notes:
  - O1: 2*10.0 + 3*5.0 = 35.0; neither line reaches qty 10.
  - O5: qty 10 is "at least 10", so has_bulk is true; the single ACTIVE
    line is out of stock, so all_in_stock is false.

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

Reference answers and concept takeaways live in
refs/day19_higher_order_array_functions_ref.md.
Do not open it before Stage 5.
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

        Hint: narrow the array once, then ask four different questions of
        the narrowed array.
        """
        # TODO: implement
        active_cond = F.filter(F.col('items'), lambda x: x.status == 'ACTIVE')
        res = (
            df.withColumn(
                'active',
                active_cond
            )
            .select(
                'order_id',
                F.size(F.col('active')).cast('int').alias('n_active'),
                F.aggregate(
                    F.transform(
                        F.col('active'), 
                        lambda x: x.qty * x.unit_price
                        ), 
                        F.lit(0.0), 
                        lambda acc, x: acc + x).cast('double').alias('active_total'),
                F.exists(F.col('active'), lambda x: x.qty >= 10).alias('has_bulk'),
                ((F.size('active') > 0) & (F.forall(F.col('active'), lambda x: x.in_stock == True))).alias('all_in_stock')
            )
        )

        return (res.select(
            'order_id',
            'n_active',
            'active_total',
            'has_bulk',
            'all_in_stock'
        ))

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """Spark SQL approach.

        Hint: the same higher-order functions exist in SQL with
        `x -> ...` lambda syntax.
        """
        df.createOrReplaceTempView("orders")

        sql = """
            -- write your SQL here
            WITH active_cond AS (
                SELECT 
                    order_id,
                    FILTER(items, x -> x.status = 'ACTIVE') AS active
                FROM 
                    orders
            ),

            hof AS (
                SELECT 
                    order_id,
                    SIZE(active) AS n_active,
                    AGGREGATE(TRANSFORM(active, x -> x.qty * x.unit_price), 0.0D, (acc, x) -> acc + x) AS active_total,
                    EXISTS(active, x -> x.qty >= 10) AS has_bulk,
                    ((SIZE(active) > 0) AND (FORALL(active, x -> x.in_stock = True))) AS all_in_stock
                FROM 
                    active_cond
            )

            SELECT * FROM hof
        """
        return spark.sql(sql)

def ai_solve_dsl(df: DataFrame) -> DataFrame:
    active = F.filter("items", lambda x: x["status"] == F.lit("ACTIVE"))
    return df.select(
        F.col("order_id"),
        F.size(active).alias("n_active"),
        F.aggregate(
            active,
            F.lit(0.0),
            lambda acc, x: acc + x["qty"] * x["unit_price"],
        ).alias("active_total"),
        F.exists(active, lambda x: x["qty"] >= F.lit(10)).alias("has_bulk"),
        (
            (F.size(active) > F.lit(0))
            & F.forall(active, lambda x: x["in_stock"])
        ).alias("all_in_stock"),
    )


def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("orders_src")
    return spark.sql(
        """
        SELECT
            order_id,
            size(active) AS n_active,
            aggregate(
                active,
                CAST(0.0 AS DOUBLE),
                (acc, x) -> acc + x.qty * x.unit_price
            ) AS active_total,
            exists(active, x -> x.qty >= 10) AS has_bulk,
            (size(active) > 0 AND forall(active, x -> x.in_stock)) AS all_in_stock
        FROM (
            SELECT
                order_id,
                filter(items, x -> x.status = 'ACTIVE') AS active
            FROM orders_src
        )
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

    SCHEMA = (
        "order_id string, "
        "items array<struct<sku:string,qty:int,unit_price:double,"
        "status:string,in_stock:boolean>>"
    )

    data = [
        # (order_id, [(sku, qty, unit_price, status, in_stock), ...])
        ("O1", [("A1", 2, 10.0, "ACTIVE", True),
                ("A2", 3, 5.0, "ACTIVE", True)]),
        ("O2", [("B1", 12, 4.0, "ACTIVE", True),
                ("B2", 20, 100.0, "CANCELLED", False)]),
        ("O3", [("C1", 1, 50.0, "ACTIVE", True),
                ("C2", 4, 25.0, "ACTIVE", False)]),
        ("O4", [("D1", 5, 20.0, "CANCELLED", True),
                ("D2", 15, 3.0, "CANCELLED", True)]),
        ("O5", [("E1", 10, 7.5, "ACTIVE", False)]),
    ]
    df = spark.createDataFrame(data, schema=SCHEMA)

    expected = [
        # (order_id, n_active, active_total, has_bulk, all_in_stock)
        ("O1", 2, 35.0, False, True),
        ("O2", 1, 48.0, True, True),
        ("O3", 2, 150.0, False, False),
        ("O4", 0, 0.0, False, False),
        ("O5", 1, 75.0, True, False),
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
# [pass ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes: LGTM
# [pass ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes: LGTM
# [pass ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (element_at, array index, cast, division)?
#     notes: LGTM
# [pass ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (do NOT guess — this is a reading-stage hypothesis, verified later)
#     notes: AI does not use transform but aggregate for qty and price 
# [pass ] Robustness — hardcoded values, assumptions not in the problem statement?
#     notes: AI used F.lit to transform constant to col type, which will be robuster than my solution using constant here
# [pass ] Style/clarity — would you approve this in a real code review?
#     notes: LGTM
#
# VERDICT (commit before running): PASS — because: same thoughts as mine with out any other obvisou fault
# ACTUAL RESULT (after Stage 4 run): all passed
# GAP ANALYSIS: did the run reveal anything the reading missed? yes, the transform can be bypassed and implement aggregate directly


# =====================================================================
# Part 5 — Review takeaways (fill in at Stage 5, before /review)
# =====================================================================
# - What did the AI get wrong, or suspiciously right?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem:
# see refs/day19_higher_order_array_functions_ref.md.
