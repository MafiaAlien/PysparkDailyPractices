"""
=====================================================================
PySpark Daily Practice — Template v2 (Claude Code edition)
=====================================================================
Day 18 — Skew handling: salted join + two-phase aggregation
         (Medium-Hard)

PROBLEM
-------
`orders` is a fact table whose `customer_id` distribution is heavily
skewed: a handful of enterprise accounts carry most of the volume while
the long tail carries one or two rows each. `customers` is the dimension
holding each account's segment.

Produce the per-customer summary, but produce it the way you would have
to on a table where one key does not fit in a single task: **salt the
skewed key**.

Rules:

1.  `N_SALT = 3`. The salt of an order row is `pmod(order_id, N_SALT)`.
    It is derived from a stable column on purpose — do NOT use `rand()`
    or `monotonically_increasing_id()` here (Part 2 asks you why).
2.  The join to `customers` must be a **salted join**: replicate every
    dimension row across all salt values `0 .. N_SALT - 1` and join on
    the composite key `(customer_id, salt)`. A plain join on
    `customer_id` alone does not count as a solution.
3.  The aggregation must be **two-phase**: a partial aggregation keyed by
    something that includes `salt`, then a final aggregation keyed by
    `customer_id`. A single `groupBy("customer_id")` does not count.
4.  Output one row per customer that has at least one order. A customer
    present in `customers` with no orders is excluded (inner semantics).
5.  `total_amount` is the sum of `amount` over the customer's orders.
    `n_products` is the number of DISTINCT `product` values the customer
    ordered.

`customer_id` is non-NULL in both tables and unique in `customers`.
`product` and `amount` are never NULL.

Note that rules 2 and 3 constrain the *route*, not the *result*: a
correct salted solution must return exactly what an unsalted one would.
`check()` cannot see which route you took — Part 2 asks you to prove it
from the physical plan.

INPUT SCHEMA
------------
orders(order_id: int, customer_id: string, product: string, amount: double)
customers(customer_id: string, segment: string)

EXPECTED OUTPUT
---------------
Columns, in this order:

    customer_id  string
    segment      string
    total_amount double
    n_products   int/long

One row per customer with >= 1 order. Row order does not matter —
`check()` compares order-insensitively.

EXAMPLE
-------
orders:

    +----------+-------------+---------+--------+
    | order_id | customer_id | product | amount |
    +----------+-------------+---------+--------+
    |      101 | X           | p       |   10.0 |
    |      102 | X           | q       |   20.0 |
    |      103 | X           | p       |   30.0 |
    +----------+-------------+---------+--------+

    salt = pmod(order_id, 3):  101 -> 2,  102 -> 0,  103 -> 1

customers:

    +-------------+---------+
    | customer_id | segment |
    +-------------+---------+
    | X           | SMB     |
    +-------------+---------+

Expected:

    +-------------+---------+--------------+------------+
    | customer_id | segment | total_amount | n_products |
    +-------------+---------+--------------+------------+
    | X           | SMB     |         60.0 |          2 |
    +-------------+---------+--------------+------------+

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
refs/day18_skew_salting_ref.md. Do not open it before Stage 5.
=====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

N_SALT = 3


# =====================================================================
# Part 1 — Solution (yours)
# =====================================================================
class Solution:
    def solve_dsl(self, orders: DataFrame, customers: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: derive the salt column once — the salted join and the
        partial aggregation both key on the same column.
        """
        # TODO: implement
        salt_orders = F.pmod(F.col('order_id'), F.lit(N_SALT))
        salt_cust = F.explode(F.sequence(F.lit(0), F.lit(N_SALT - 1)))
        orders = (orders.withColumn(
            'salt',
            salt_orders
        )
        )

        customers = (
            customers.withColumn(
                'salt',
                salt_cust
            )
        )

        table_join = (
            customers.join(
                orders,
                on=['customer_id', 'salt'],
                how='inner'
            )
            .groupBy(
                'customer_id', 
                'segment', 
                'product',
                'salt')
            .agg(
                F.sum('amount').cast('double').alias('partial_amount')
            )
            .drop('salt')
            .groupBy(
                'customer_id',
                'segment')
            .agg(
                F.sum(F.col('partial_amount')).alias('total_amount'),
                F.count_distinct(F.col('product')).cast('int').alias('n_products')
            )
            .select(
                'customer_id',
                'segment',
                F.col('total_amount'),
                F.col('n_products')
            )
        )
        return table_join

    def solve_sql(
        self, spark: SparkSession, orders: DataFrame, customers: DataFrame
    ) -> DataFrame:
        """Spark SQL approach.

        Hint: the replicated dimension is an explode over
        sequence(0, N_SALT - 1); N_SALT is a literal you inline yourself.
        """
        orders.createOrReplaceTempView("orders")
        customers.createOrReplaceTempView("customers")

        sql = f"""
            -- write your SQL here
            WITH 
            salted_cust AS (
                SELECT 
                    customer_id,
                    segment,
                    salt
                FROM 
                    customers
                LATERAL VIEW explode(sequence(0, {N_SALT - 1})) tb AS salt
            ),

            salted_orders AS (
                SELECT 
                    customer_id,
                    amount,
                    product,
                    pmod((order_id), {N_SALT}) AS salt 
                FROM 
                    orders 
            ),

            table_join AS (
                SELECT 
                    c.customer_id AS customer_id,
                    c.segment AS segment,
                    product,
                    CAST(SUM(amount) AS DOUBLE) AS partial_amount
                FROM salted_cust c JOIN salted_orders o ON c.customer_id = o.customer_id AND c.salt = o.salt
                GROUP BY 
                    c.customer_id,
                    segment,
                    c.salt,
                    product
            ),

            agg_total AS (
                SELECT 
                    customer_id,
                    segment,
                    SUM(partial_amount) AS total_amount,
                    COUNT(DISTINCT product) AS n_products
                FROM 
                    table_join
                GROUP BY 
                    customer_id,
                    segment

            )
            SELECT * FROM agg_total
        """
        return spark.sql(sql)
    
def ai_solve_dsl(orders: DataFrame, customers: DataFrame) -> DataFrame:

    o = orders.withColumn("salt", F.expr(f"pmod(order_id, {N_SALT})"))

    c = customers.withColumn(
        "salt", F.explode(F.array(*[F.lit(i) for i in range(N_SALT)]))
    )

    joined = o.join(c, on=["customer_id", "salt"], how="inner")

    partial = joined.groupBy("customer_id", "segment", "salt", "product").agg(
        F.sum("amount").alias("part_amount")
    )

    final = partial.groupBy("customer_id", "segment").agg(
        F.sum("part_amount").alias("total_amount"),
        F.countDistinct("product").alias("n_products"),
    )

    return final.select("customer_id", "segment", "total_amount", "n_products")


def ai_solve_sql(spark: SparkSession, orders: DataFrame, customers: DataFrame) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    sql = """
        WITH salted_orders AS (
            SELECT
                order_id,
                customer_id,
                product,
                amount,
                pmod(order_id, 3) AS salt
            FROM orders
        ),
        salted_customers AS (
            SELECT
                c.customer_id,
                c.segment,
                s.salt AS salt
            FROM customers c
            LATERAL VIEW explode(sequence(0, 2)) s AS salt
        ),
        joined AS (
            SELECT
                o.customer_id AS customer_id,
                c.segment     AS segment,
                o.salt        AS salt,
                o.product     AS product,
                o.amount      AS amount
            FROM salted_orders o
            JOIN salted_customers c
              ON o.customer_id = c.customer_id
             AND o.salt = c.salt
        ),
        partial AS (
            SELECT
                customer_id,
                segment,
                salt,
                product,
                SUM(amount) AS part_amount
            FROM joined
            GROUP BY customer_id, segment, salt, product
        )
        SELECT
            customer_id,
            segment,
            SUM(part_amount)        AS total_amount,
            COUNT(DISTINCT product) AS n_products
        FROM partial
        GROUP BY customer_id, segment
    """
    return spark.sql(sql)


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

    orders_data = [
        # order_id, customer_id, product, amount     (salt = order_id % 3)
        (1,  "C_HOT", "P_A", 100.0),
        (2,  "C_HOT", "P_A",  50.0),
        (3,  "C_HOT", "P_B", 200.0),
        (4,  "C_HOT", "P_B",  25.0),
        (5,  "C_HOT", "P_C",  75.0),
        (6,  "C_HOT", "P_A",  10.0),
        (7,  "C_HOT", "P_C",  40.0),
        (9,  "C_HOT", "P_D",   5.0),
        (10, "C1",    "Q_A",  60.0),
        (13, "C1",    "Q_B",  30.0),
        (16, "C1",    "Q_A",  20.0),
        (11, "C2",    "R_A",  15.0),
        (12, "C2",    "R_B",  45.0),
        (14, "C3",    "S_A",  90.0),
    ]
    orders = spark.createDataFrame(
        orders_data,
        schema="order_id int, customer_id string, product string, amount double",
    )

    customers_data = [
        ("C_HOT", "ENTERPRISE"),
        ("C1",    "SMB"),
        ("C2",    "SMB"),
        ("C3",    "ENTERPRISE"),
        ("C4",    "SMB"),  # no orders — must not appear in the output
    ]
    customers = spark.createDataFrame(
        customers_data, schema="customer_id string, segment string"
    )

    expected = [
        # customer_id, segment, total_amount, n_products
        ("C_HOT", "ENTERPRISE", 505.0, 4),
        ("C1",    "SMB",        110.0, 2),
        ("C2",    "SMB",         60.0, 2),
        ("C3",    "ENTERPRISE",  90.0, 1),
    ]

    s = Solution()
    check(s.solve_dsl(orders, customers), expected, "DSL")
    check(s.solve_sql(spark, orders, customers), expected, "SQL")

    # ------------------------------------------------------------------
    # Observation task — answer these in Part 5, from an ACTUALLY RUN
    # .explain(), never from memory.
    #
    #  (a) 14 rows means the dimension gets broadcast and the join
    #      shuffle disappears. Re-run with
    #          spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    #      and count Exchange nodes in your salted plan.
    #  (b) Compare your two-phase aggregation against the single
    #      groupBy("customer_id") baseline. How many Exchanges does each
    #      have, and how many HashAggregate/SortAggregate nodes?
    #  (c) Look up spark.sql.adaptive.skewJoin.enabled,
    #      skewedPartitionFactor and skewedPartitionThresholdInBytes.
    #      Which physical operator does AQE's skew handling attach to,
    #      and which join strategies can it NOT rescue? Does it do
    #      anything at all for a skewed groupBy?
    #  (d) Rule 1 forbids rand() for the salt. Name the concrete failure
    #      mode, not the slogan.
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # ------------------------------------------------------------------
    check(ai_solve_dsl(orders, customers), expected, "AI-DSL (post-review verification)")
    check(ai_solve_sql(spark, orders, customers), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
#
# def ai_solve_dsl(orders: DataFrame, customers: DataFrame) -> DataFrame:
#     ...
#
# def ai_solve_sql(spark: SparkSession, orders: DataFrame, customers: DataFrame) -> DataFrame:
#     ...
#
# REVIEW_NOTES (fill in BEFORE running):
# ----------------------------------------------------------------------
# [pass] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes:
# [pass] Salting invariants — does the salt range on the fact side match the
#     replication range on the dimension side, exactly? Is every measure it
#     recombines actually decomposable over the salt buckets?
#     notes:
# [pass] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes:
# [pass] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (element_at, array index, cast, division)?
#     notes:
# [pass] Performance — extra Exchange? extra scan? window without partitionBy?
#     (do NOT guess — this is a reading-stage hypothesis, verified later)
#     notes:
# [pass] Robustness — hardcoded values, assumptions not in the problem statement?
#     notes:
# [pass] Style/clarity — would you approve this in a real code review?
#     notes: AI ust for loop to generate salt tail for customer but still use sequence method in SQL query, 
#            and AI used F.expr,I think this is redundant and not very readable
#
# VERDICT (commit before running): PASS  — because: some syntax is redundant but corrent, and AI also consider partial groupBy, which is better and rigorious than mine
# ACTUAL RESULT (after Stage 4 run): ALL PASSED
# GAP ANALYSIS: did the run reveal anything the reading missed?
# I did not consider the partial groupby because the salt col should be deprecate after joining, 
# and need to group by again to aggregate values under same customer_id with 
# different salt before, but Both incognito and I did not filter out hot key and normal key, we explode all keys, this may inflate data in prod. 
# for better solution, we just need to salt hot keys here, and add fixed salt key(such as constant 0) for normal keys existing in both customers table and orders table 
# to ensure consistency.

# =====================================================================
# Part 5 — Review takeaways (fill in at Stage 5, before /review)
# =====================================================================
# - What did the AI get wrong, or suspiciously right?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
# - Observation task answers (a)–(d), with the plan fragments you actually saw:
#
# Concept takeaways for this problem: see refs/day18_skew_salting_ref.md.
