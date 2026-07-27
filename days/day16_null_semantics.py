"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 16 — NULL semantics special: null-safe equality, NULL in joins,
         NULL ordering in windows  (Medium)

PROBLEM
-------
An e-commerce catalog team reconciles a product catalog against a
marketplace offer feed.

A product is identified by (sku, variant). `variant` is NULLABLE and a
NULL variant is MEANINGFUL: it identifies the base product (the product
sold with no variant at all). It is not missing data — it is a real,
first-class key value, and it appears in BOTH tables.

`price` in the offer feed is also nullable: a seller may list a product
with "price on request", in which case the offer exists but its price is
unknown.

Produce one row per CATALOG entry (every catalog row must survive, even
with zero offers) with:

  * n_offers    — how many offers match this catalog entry, counting
                  offers whose price is unknown. 0 if there are none.
  * best_seller — the seller with the LOWEST KNOWN price for this
                  catalog entry. An offer with an unknown price can
                  never be the best seller. Ties on price are broken by
                  seller name ascending. If no offer with a known price
                  exists, emit the string 'NONE'.
  * best_price  — the price of that best seller, or NULL if best_seller
                  is 'NONE'.

Offers that match no catalog entry are ignored.

INPUT SCHEMA
------------
catalog(sku: string, variant: string NULLABLE, product_name: string)
offers (sku: string, variant: string NULLABLE, seller: string,
        price: double NULLABLE)

catalog is unique on (sku, variant). offers is not.

EXPECTED OUTPUT
---------------
Columns, in this exact order:

    sku (string)
    variant_label (string)   -- the variant, or 'BASE' when it is NULL
    product_name (string)
    n_offers (bigint)
    best_seller (string)     -- 'NONE' when no known-price offer exists
    best_price (double)      -- NULL when best_seller is 'NONE'

Row order does not matter (the harness sorts).

EXAMPLE
-------
catalog:
    ('S1', 'RED',  'Widget Red')
    ('S1', None,   'Widget Base')
    ('S9', 'BLUE', 'Never Offered')

offers:
    ('S1', 'RED',  'alpha', 10.0)
    ('S1', 'RED',  'bravo',  9.5)
    ('S1', None,   'delta', 12.0)

expected:
    ('S1', 'RED',  'Widget Red',    2, 'bravo', 9.5)
    ('S1', 'BASE', 'Widget Base',   1, 'delta', 12.0)
    ('S9', 'BLUE', 'Never Offered', 0, 'NONE',  None)

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
    def solve_dsl(self, catalog: DataFrame, offers: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: two tables, two nullable key columns, one LEFT join —
        then decide at what grain each of the two measures lives.
        """
        # TODO: implement
        fill_variant = F.coalesce(F.col('variant'), F.lit('BASE'))
        window_best = Window.partitionBy(
            'sku', 'variant').orderBy(F.col("price").asc(), F.col("seller").asc())

        catalog = (catalog.withColumn(
            'variant',
            fill_variant)
        )

        offers = (offers.withColumn(
            'variant',
            fill_variant)
        )

        best_df = (
            offers.filter(F.col('price').isNotNull())
            .withColumn(
                'price_rk',
                F.row_number().over(window_best))
            .filter(
                F.col('price_rk') == 1)
            .select(
                'sku',
                'variant',
                'seller',
                'price'
            )
        )

        cnt = (
        offers.groupBy(
            'sku',
            'variant',
        )
            .agg(
            F.sum(
                F.when(F.col('seller').isNotNull(), 1)
                .otherwise(0)).alias('n_offers'))
            .select(
                'sku',
                'variant',
                'n_offers'
            )
        )

        res_df = (
            catalog.join(
                cnt,
                on=['sku', 'variant'],
                how='left'
            ).join(
                best_df,
                on=['sku', 'variant'],
                how='left')
            .select(
                'sku',
                F.col('variant').alias('variant_label'),
                'product_name',
                F.coalesce(F.col('n_offers'), F.lit(0)).alias('n_offers'),
                F.coalesce(F.col('seller'),
                           F.lit('NONE')).alias('best_seller'),
                F.col('price').alias('best_price')

            )
            )
        

        return res_df

    def solve_sql(
        self, spark: SparkSession, catalog: DataFrame, offers: DataFrame
    ) -> DataFrame:
        """SparkSQL approach.

        Hint: the join predicate needs an operator that is not `=`.
        """
        catalog.createOrReplaceTempView("catalog")
        offers.createOrReplaceTempView("offers")

        sql = """
            -- write your SQL here
            WITH 
            agg_offers AS (
                SELECT 
                    sku,
                    variant,
                    COUNT(*) AS n_offers,
                    MIN(CASE WHEN price IS NOT NULL THEN struct(price, seller) END) AS best
                FROM    
                    offers 
                GROUP BY 
                    sku, 
                    variant
            ),



            merge_res AS (
                SELECT 
                    c.sku,
                    COALESCE(c.variant, 'BASE') AS variant_label,
                    product_name,
                    COALESCE(n_offers, 0) AS n_offers,
                    COALESCE(a.best.seller ,'NONE') AS best_seller,
                    a.best.price AS best_price
                FROM catalog c LEFT JOIN agg_offers a ON c.sku = a.sku AND c.variant <=> a.variant
            )

            SELECT * FROM merge_res;
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
        SparkSession.builder.appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    catalog_data = [
        ("S1", "RED", "Widget Red"),
        ("S1", None, "Widget Base"),
        ("S2", None, "Gadget"),
        ("S3", "BLUE", "Doohickey Blue"),
        ("S4", None, "Thingamajig"),
    ]
    catalog = spark.createDataFrame(
        catalog_data, schema="sku string, variant string, product_name string"
    )

    offers_data = [
        ("S1", "RED", "alpha", 10.0),
        ("S1", "RED", "bravo", 9.5),
        ("S1", "RED", "cosmo", None),   # price on request
        ("S1", None, "delta", 12.0),
        ("S1", None, "echo", 11.0),
        ("S2", None, "foxtrot", None),  # the only offer, price unknown
        ("S4", None, "golf", 20.0),
        ("S4", None, "hotel", 20.0),    # price tie with golf
        ("S4", None, "india", None),
        ("S5", "X", "juliet", 5.0),     # offer with no catalog entry
    ]
    offers = spark.createDataFrame(
        offers_data,
        schema="sku string, variant string, seller string, price double",
    )

    expected = [
        ("S1", "BASE", "Widget Base", 2, "echo", 11.0),
        ("S1", "RED", "Widget Red", 3, "bravo", 9.5),
        ("S2", "BASE", "Gadget", 1, "NONE", None),
        ("S3", "BLUE", "Doohickey Blue", 0, "NONE", None),
        ("S4", "BASE", "Thingamajig", 3, "golf", 20.0),
    ]

    s = Solution()
    res_dsl = s.solve_dsl(catalog, offers)
    res_sql = s.solve_sql(spark, catalog, offers)
    check(res_dsl, expected, "DSL")
    print('#'*100, 'DSL EXPLAIN PLAN:', sep='\n')
    res_dsl.explain()
    check(res_sql, expected, "SQL")

    print('#'*100, 'SQL EXPLAIN PLAN:', sep='\n')
    res_sql.explain()

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    # check(ai_solve_dsl(catalog, offers), expected, "AI-DSL (post-review verification)")
    # check(ai_solve_sql(spark, catalog, offers), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review (NEW in v2)
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
#
# def ai_solve_dsl(catalog: DataFrame, offers: DataFrame) -> DataFrame:
#     ...
#
# def ai_solve_sql(spark: SparkSession, catalog: DataFrame, offers: DataFrame) -> DataFrame:
#     ...
#
# REVIEW_NOTES (fill in BEFORE running):
# ----------------------------------------------------------------------
# [ ] Correctness — edge cases: ties? nulls? empty groups? duplicate keys?
#     notes:
# [ ] API usage — wrong signatures, deprecated calls, ambiguous column
#     references in joins, SQL syntax slips (trailing commas, quoting)?
#     notes:
# [ ] Performance — unnecessary shuffles, window over the whole table
#     without partitionBy, join strategy, redundant actions/collects?
#     notes:
# [ ] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes:
# [ ] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS / FAIL — because:
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# ---------- Route A (DSL): null-safe join + filtered ranking window ----
# def ref_solve_dsl_a(catalog, offers):
#     j = (
#         catalog.alias("c")
#         .join(
#             offers.alias("o"),
#             (F.col("c.sku") == F.col("o.sku"))
#             & F.col("c.variant").eqNullSafe(F.col("o.variant")),
#             "left",
#         )
#         .select(
#             F.col("c.sku").alias("sku"),
#             F.col("c.variant").alias("variant"),
#             F.col("c.product_name").alias("product_name"),
#             F.col("o.seller").alias("seller"),
#             F.col("o.price").alias("price"),
#         )
#     )
#
#     # measure 1 lives at the ALL-offers grain; count(seller) skips the
#     # null-padded rows produced by the LEFT join (count("*") would say 1).
#     counts = j.groupBy("sku", "variant", "product_name").agg(
#         F.count("seller").alias("n_offers")
#     )
#
#     # measure 2 lives at the KNOWN-PRICE grain -> filter FIRST, then rank.
#     w = Window.partitionBy("sku", "variant").orderBy(
#         F.col("price").asc(), F.col("seller").asc()
#     )
#     best = (
#         j.filter(F.col("price").isNotNull())
#         .withColumn("rn", F.row_number().over(w))
#         .filter(F.col("rn") == 1)
#         .select(
#             F.col("sku").alias("b_sku"),
#             F.col("variant").alias("b_variant"),
#             F.col("seller").alias("best_seller"),
#             F.col("price").alias("best_price"),
#         )
#     )
#
#     # the second join is on the SAME nullable key -> null-safe again.
#     return (
#         counts.join(
#             best,
#             (F.col("sku") == F.col("b_sku"))
#             & F.col("variant").eqNullSafe(F.col("b_variant")),
#             "left",
#         )
#         .select(
#             "sku",
#             F.coalesce(F.col("variant"), F.lit("BASE")).alias("variant_label"),
#             "product_name",
#             "n_offers",
#             F.coalesce(F.col("best_seller"), F.lit("NONE")).alias("best_seller"),
#             "best_price",
#         )
#     )
#
# ---------- Route B (DSL): sentinel pre-fill (Day 10 echo) -------------
# Kill the NULL in the key BEFORE any join, then plain `=` works
# everywhere and no null-safe operator is needed at all.
#
# def ref_solve_dsl_b(catalog, offers):
#     c = catalog.withColumn("vkey", F.coalesce("variant", F.lit("BASE")))
#     o = offers.withColumn("vkey", F.coalesce("variant", F.lit("BASE")))
#     ...  # join on ["sku", "vkey"], rest identical to Route A
#
# Trade-off (identical to Day 10's sentinel-vs-discriminate pair):
# the sentinel is the most robust once chosen, but it silently collides
# if 'BASE' is ever a legitimate variant value. eqNullSafe has no
# collision surface but must be repeated at EVERY join on that key —
# one forgotten `=` and the base products go quiet.
#
# ---------- SQL ---------------------------------------------------------
# WITH j AS (
#     SELECT c.sku, c.variant, c.product_name, o.seller, o.price
#     FROM catalog c
#     LEFT JOIN offers o
#            ON c.sku = o.sku
#           AND c.variant <=> o.variant          -- IS NOT DISTINCT FROM
# ),
# counts AS (
#     SELECT sku, variant, product_name, COUNT(seller) AS n_offers
#     FROM j
#     GROUP BY sku, variant, product_name        -- NULL is ONE group here
# ),
# ranked AS (
#     SELECT sku, variant, seller, price,
#            ROW_NUMBER() OVER (PARTITION BY sku, variant
#                               ORDER BY price ASC, seller ASC) AS rn
#     FROM j
#     WHERE price IS NOT NULL
# )
# SELECT c.sku,
#        COALESCE(c.variant, 'BASE')  AS variant_label,
#        c.product_name,
#        c.n_offers,
#        COALESCE(r.seller, 'NONE')   AS best_seller,
#        r.price                      AS best_price
# FROM counts c
# LEFT JOIN ranked r
#        ON c.sku = r.sku AND c.variant <=> r.variant AND r.rn = 1
#
# ---------- SQL variant: struct-argmax instead of the window ------------
# MIN(struct(price, seller)) over the price-IS-NOT-NULL subset gives the
# same answer in one aggregation (both sort keys point the same way, so
# no negation is needed — Day 8 shape, not Day 7 shape). It is only
# correct AFTER the NULL prices are gone: a NULL first field sorts
# smallest, so an unknown price would win the MIN outright.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - `=` is NOT reflexive on NULL: NULL = NULL is UNKNOWN, not TRUE. Any
#   join whose key column is nullable silently loses those rows.
#   Null-safe equality: DSL a.eqNullSafe(b) / SQL a <=> b (equivalently
#   a IS NOT DISTINCT FROM b). It returns TRUE for NULL-NULL and FALSE
#   for NULL-value; it never returns NULL.
# - The three "equality-ish" mechanisms do NOT agree on NULL, and this is
#   the whole reason a nullable key is dangerous:
#       join ON a = b        -> NULL never matches (row dropped/padded)
#       GROUP BY col         -> all NULLs form ONE group
#       Window.partitionBy   -> all NULLs form ONE partition
#       DISTINCT / dropDup   -> NULLs are equal to each other
#   Grouping treats NULL as a value; joining treats it as unknown.
# - Default NULL ordering in Spark: ASC -> NULLS FIRST, DESC -> NULLS
#   LAST. So an ordinary `orderBy(col.asc())` ranking puts the rows with
#   the *missing* value at the top. Overrides: asc_nulls_last() /
#   desc_nulls_first() in DSL, NULLS FIRST|LAST in SQL.
# - "Filter the NULLs out" and "order NULLS LAST" are NOT interchangeable
#   fixes; decide which one the spec actually asks for, then check
#   whether the other has become dead code.
# - LEFT join + COUNT(*) vs COUNT(col): after a LEFT join, a non-matching
#   left row still produces one physical row, so COUNT(*) reports 1 for
#   an empty group. COUNT(a_right_column) reports 0 (aggregate-NULL
#   rule, Day 7 / Day 12 family).
# - Two measures at two different grains (all offers vs known-price
#   offers) means two branches over the same joined relation; check with
#   .explain() how many Exchanges that actually costs and whether the
#   window and the groupBy can share one.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
