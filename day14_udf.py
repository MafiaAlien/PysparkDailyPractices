"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 14 — UDFs: python UDF vs pandas_udf vs native expressions  (Medium)

PROBLEM
-------
A logging pipeline stores each request's raw query-string payload as a
single text column. Product wants a normalized report.

For every row in `requests`, produce:

  1. `req_id`      — passthrough.
  2. `n_params`    — the number of key=value pairs present in `payload`.
  3. `user_tier`   — the value of the `tier` key inside `payload`,
                     uppercased; if the `tier` key is absent, use the
                     string 'UNKNOWN'.
  4. `latency_bucket` — bucket `latency_ms` into:
                     'FAST'   for latency_ms < 100
                     'NORMAL' for 100 <= latency_ms < 500
                     'SLOW'   for latency_ms >= 500

`payload` format: pairs separated by '&', key and value separated by
'=', e.g. "tier=gold&region=us&lang=en". The payload may be an empty
string (zero pairs) or NULL.

Return ALL rows of the input — no filtering.

Implement it TWICE inside solve_dsl:
  - the columns above must be produced by a **Python UDF** (at least
    one `@F.udf` you write yourself) for the payload parsing part,
  - and `latency_bucket` must be produced with **native expressions**
    (no UDF).
Then in Part 5, answer the perf question with `.explain()` evidence.

INPUT SCHEMA
------------
requests(req_id: int, payload: string, latency_ms: int)

EXPECTED OUTPUT
---------------
Columns, in this exact order:
    req_id: int, n_params: int, user_tier: string, latency_bucket: string
One row per input row. Order-insensitive (harness sorts).

EXAMPLE
-------
Input:
    (1, "tier=gold&region=us", 42)
    (2, "region=eu",           250)
Output:
    (1, 2, 'GOLD',    'FAST')
    (2, 1, 'UNKNOWN', 'NORMAL')

NOTE: this problem contains one deliberate trap in the test data. It
passes on naive clean-data assumptions and fails on the rows provided.

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window


# =====================================================================
# Part 1 — Solution (yours)
# =====================================================================
class Solution:

    @staticmethod
    @F.udf(IntegerType())
    def cnt_param(text:str):
        if not text:
            return 0
        return len(text.split('&'))

    @staticmethod
    @F.udf(StringType())
    def retrieve_tier(text:str):
        if not text:
            return "UNKNOWN"

        for item in text.split('&'):
            if '=' in item:
                item_name, _, user_tier = item.partition('=')
                if item_name == 'tier':
                    return user_tier.upper()
        return "UNKNOWN"
    
    def solve_dsl(self, df: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: a Python UDF receives Python objects, not Columns — what
        arrives when the column value is NULL? Decorate with an explicit
        return type. latency_bucket stays native (F.when chain).
        """
        # TODO: implement

        res_df = (
            df.withColumn(
                'n_params',
                self.cnt_param(F.col('payload')))
            .withColumn(
                'user_tier',
                self.retrieve_tier(F.col('payload'))
            )
            .withColumn(
                'latency_bucket',
                F.when(F.col('latency_ms') < 100, F.lit('FAST'))
                .when(F.col('latency_ms') >= 500, F.lit('SLOW'))
                .otherwise(F.lit('NORMAL'))
            )
            .select(
                'req_id',
                'n_params',
                'user_tier',
                'latency_bucket')
        )
        return res_df

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: register the same Python function with
        spark.udf.register(name, fn, returnType) to call it from SQL,
        OR express the parsing natively (split / size / regexp_extract /
        str_to_map) and compare the plans.
        """
        df.createOrReplaceTempView("requests")
            
        spark.udf.register('cnt_param', self.cnt_param)
        spark.udf.register('retrieve_tier', self.retrieve_tier)

        sql = """
            -- write your SQL here
            WITH
            transform AS(
                SELECT
                    req_id,
                    cnt_param(payload) AS n_params,
                    retrieve_tier(payload) AS user_tier,
                    CASE
                        WHEN latency_ms < 100 THEN 'FAST'
                        WHEN latency_ms >= 500 THEN 'SLOW'
                        ELSE 'NORMAL' END AS latency_bucket
                FROM
                    requests
            )

            SELECT * FROM transform;
        """
        return spark.sql(sql)
    
def _parse_payload(payload):
    if payload is None or payload == "":
        return (0, "UNKNOWN")
    pairs = [p for p in payload.split("&") if p != ""]
    tier = "UNKNOWN"
    for p in pairs:
        k, sep, v = p.partition("=")
        if k == "tier" and sep:
            tier = v.upper()
    return (len(pairs), tier)


_parse_schema = StructType([
    StructField("n_params", IntegerType(), False),
    StructField("user_tier", StringType(), False),
])


def ai_solve_dsl(df: DataFrame) -> DataFrame:
    parse_udf = F.udf(_parse_payload, _parse_schema)
    return (
        df.withColumn("_p", parse_udf(F.col("payload")))
          .select(
              F.col("req_id").cast("int").alias("req_id"),
              F.col("_p.n_params").cast("int").alias("n_params"),
              F.col("_p.user_tier").alias("user_tier"),
              F.when(F.col("latency_ms") < 100, F.lit("FAST"))
               .when(F.col("latency_ms") < 500, F.lit("NORMAL"))
               .otherwise(F.lit("SLOW")).alias("latency_bucket"),
          )
    )


def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("requests")
    return spark.sql("""
        SELECT
            CAST(req_id AS INT) AS req_id,
            CAST(
                CASE
                    WHEN payload IS NULL OR payload = '' THEN 0
                    ELSE size(filter(split(payload, '&'), x -> x <> ''))
                END AS INT
            ) AS n_params,
            COALESCE(
                UPPER(
                    element_at( -- need try_element_at here
                        filter(
                            transform(
                                split(COALESCE(payload, ''), '&'),
                                x -> split(x, '=', 2)
                            ),
                            kv -> size(kv) = 2 AND kv[0] = 'tier'
                        ),
                        1
                    )[1]
                ),
                'UNKNOWN'
            ) AS user_tier,
            CASE
                WHEN latency_ms < 100 THEN 'FAST'
                WHEN latency_ms < 500 THEN 'NORMAL'
                ELSE 'SLOW'
            END AS latency_bucket
        FROM requests
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
        (1, "tier=gold&region=us&lang=en", 42),
        (2, "region=eu&lang=fr", 250),
        (3, "tier=silver", 500),
        (4, "", 99),
        (5, None, 1200),
        (6, "tier=Gold&region=us", 100),
        (7, "tier=&region=jp", 499),
    ]
    df = spark.createDataFrame(
        data, schema="req_id int, payload string, latency_ms int"
    )

    expected = [
        (1, 3, "GOLD", "FAST"),
        (2, 2, "UNKNOWN", "NORMAL"),
        (3, 1, "SILVER", "SLOW"),
        (4, 0, "UNKNOWN", "FAST"),
        (5, 0, "UNKNOWN", "SLOW"),
        (6, 2, "GOLD", "NORMAL"),
        (7, 2, "", "NORMAL"),
    ]

    s = Solution()
    res_dsl = s.solve_dsl(df)
    check(res_dsl, expected, "DSL")
    res_sql = s.solve_sql(spark, df)
    check(res_sql, expected, "SQL")

    res_dsl.explain()
    res_sql.explain()

    # ------------------------------------------------------------------
    # Observation task (do this after both PASS):
    #   s.solve_dsl(df).explain(True)
    #   s.solve_sql(spark, df).explain(True)
    # Look for BatchEvalPython / ArrowEvalPython nodes. How many are
    # there? Does adding a second UDF add a second node, or do they
    # fuse? Where does the native latency_bucket land in the plan
    # relative to the Python node?
    # ------------------------------------------------------------------

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
# [pass ] Correctness — edge cases: NULL payload? empty payload? key present
#     but value empty? case of the value? missing key vs empty value?
#     notes:
# [pass ] API usage — UDF return type declared? default type if omitted?
#     does the UDF body defend against None input? deprecated calls?
#     notes:
# [pass ] Performance — how many Python eval nodes? could any UDF have been
#     a native expression? is the UDF applied before or after any filter?
#     notes:
# [pass ] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior on malformed pairs?
#     notes:
# [pass ] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS / FAIL — because:
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# ---- Route A: Python UDF (as the problem requires) -------------------
# @F.udf(returnType=IntegerType())
# def count_params(payload):
#     if payload is None or payload == "":
#         return 0
#     return len(payload.split("&"))
#
# @F.udf(returnType=StringType())
# def extract_tier(payload):
#     if payload is None:
#         return "UNKNOWN"
#     for pair in payload.split("&"):
#         if "=" in pair:
#             k, _, v = pair.partition("=")
#             if k == "tier":
#                 return v.upper()          # empty value stays "" — NOT UNKNOWN
#     return "UNKNOWN"
#
# def ref_solve_dsl(df):
#     return df.select(
#         "req_id",
#         count_params("payload").alias("n_params"),
#         extract_tier("payload").alias("user_tier"),
#         F.when(F.col("latency_ms") < 100, F.lit("FAST"))
#          .when(F.col("latency_ms") < 500, F.lit("NORMAL"))
#          .otherwise(F.lit("SLOW")).alias("latency_bucket"),
#     )
#
# ---- Route B: fully native (no UDF) — the plan to compare against ----
# def ref_solve_dsl_native(df):
#     pmap = F.when(
#         F.col("payload").isNull() | (F.col("payload") == ""), F.create_map()
#     ).otherwise(F.str_to_map(F.col("payload"), F.lit("&"), F.lit("=")))
#     return df.select(
#         "req_id",
#         F.when(F.col("payload").isNull() | (F.col("payload") == ""), F.lit(0))
#          .otherwise(F.size(F.split(F.col("payload"), "&")))
#          .cast("int").alias("n_params"),
#         F.coalesce(F.upper(F.element_at(pmap, F.lit("tier"))), F.lit("UNKNOWN"))
#          .alias("user_tier"),
#         F.when(F.col("latency_ms") < 100, F.lit("FAST"))
#          .when(F.col("latency_ms") < 500, F.lit("NORMAL"))
#          .otherwise(F.lit("SLOW")).alias("latency_bucket"),
#     )
#
# ---- SQL: registered UDF route ---------------------------------------
# def ref_solve_sql(spark, df):
#     df.createOrReplaceTempView("requests")
#     spark.udf.register("count_params", count_params.func, IntegerType())
#     spark.udf.register("extract_tier", extract_tier.func, StringType())
#     return spark.sql("""
#         SELECT req_id,
#                count_params(payload) AS n_params,
#                extract_tier(payload) AS user_tier,
#                CASE WHEN latency_ms < 100 THEN 'FAST'
#                     WHEN latency_ms < 500 THEN 'NORMAL'
#                     ELSE 'SLOW' END      AS latency_bucket
#         FROM requests
#     """)
#
# ---- SQL: native route (str_to_map) ----------------------------------
# def ref_solve_sql_native(spark, df):
#     df.createOrReplaceTempView("requests")
#     return spark.sql("""
#         WITH m AS (
#           SELECT req_id, payload, latency_ms,
#                  CASE WHEN payload IS NULL OR payload = '' THEN map()
#                       ELSE str_to_map(payload, '&', '=') END AS p
#           FROM requests
#         )
#         SELECT req_id,
#                CASE WHEN payload IS NULL OR payload = '' THEN 0
#                     ELSE SIZE(SPLIT(payload, '&')) END        AS n_params,
#                COALESCE(UPPER(p['tier']), 'UNKNOWN')          AS user_tier,
#                CASE WHEN latency_ms < 100 THEN 'FAST'
#                     WHEN latency_ms < 500 THEN 'NORMAL'
#                     ELSE 'SLOW' END                           AS latency_bucket
#         FROM m
#     """)


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - Python UDF default return type is StringType() when omitted — an int
#   UDF without returnType silently yields strings and the tuple compare
#   fails on type, not value. Always declare returnType.
# - A Python UDF is NOT NULL-aware: NULL arrives as Python None and the
#   body runs anyway. Native expressions propagate NULL for free; a UDF
#   must defend explicitly or raise inside the executor.
# - Cost model: BatchEvalPython serializes rows out to a Python worker
#   and back (pickle round trip), is a black box to Catalyst (no
#   pushdown through it, no constant folding), and blocks whole-stage
#   codegen for that column. pandas_udf uses ArrowEvalPython — vectorized
#   batches, far less per-row overhead, same optimizer opacity.
# - Rule: reach for a UDF only when no native expression exists. Here
#   str_to_map / split / size / element_at cover the whole task natively.
# - Observation: count Python eval nodes in .explain(); adjacent UDFs on
#   the same input can share ONE BatchEvalPython node, but the node is
#   still an optimizer barrier.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
