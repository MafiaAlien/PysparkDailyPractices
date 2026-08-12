"""
=====================================================================
PySpark Daily Practice — Template v2 (Claude Code edition)
=====================================================================
Day 20 — ANSI mode & the try_* family, on dirty-string ingestion
         (Medium-Hard)

PROBLEM
-------
Three upstream systems land their sales records in one raw table. Every
value arrives as a STRING, formatted however that system felt like.
Build the per-source ingestion report.

This session runs with ANSI mode ON — the Spark 4.x default.

Parsing rules
~~~~~~~~~~~~~
amount_raw — a money amount as a human writes it. It may be padded with
    whitespace, may carry a leading '$', may use ',' as a thousands
    separator, and is negative (a refund) when it carries a leading '-'.
    Once the currency symbol and the thousands separators are removed,
    what is left must be a valid decimal number. If it is not — or if
    amount_raw is NULL or blank — the record's amount is UNPARSEABLE:
    it contributes nothing to net_amount and it counts toward
    n_bad_amount.

units_raw — the number of units on the record, as a whole count. The
    upstream systems disagree on formatting: most emit a plain integer
    string ('5'), one of them serializes its internal float and emits
    '12.0'. Both forms denote a whole count and are usable. The target
    type for a usable count is INT.
    If units_raw does not denote a usable whole count (letters, blank,
    NULL), the record's units are UNKNOWN and the record contributes
    nothing to total_units.

The two fields are parsed independently: an unparseable amount does not
invalidate the record's units, and unknown units do not invalidate its
amount.

Produce one row per source. For each source emit, in this column order:

  source          the source key, unchanged
  n_records       how many input rows this source contributed
  n_bad_amount    how many of those rows have an UNPARSEABLE amount
  net_amount      SUM of this source's parsed amounts (refunds subtract)
  total_units     SUM of this source's usable unit counts
  avg_unit_price  net_amount / total_units, rounded to 2 decimal places;
                  NULL when total_units is 0

INPUT SCHEMA
------------
raw_sales(
    record_id: string,
    source: string,
    amount_raw: string,
    units_raw: string
)

EXPECTED OUTPUT
---------------
source: string, n_records: int, n_bad_amount: int, net_amount: double,
total_units: int, avg_unit_price: double

One row per distinct source. Row order does not matter — check() sorts.

EXAMPLE
-------
raw_sales (the full test table):

    +-----------+--------+-------------+-----------+
    | record_id | source | amount_raw  | units_raw |
    +-----------+--------+-------------+-----------+
    | R01       | SRC_A  | $1,250.00   | 5         |
    | R02       | SRC_A  |   850.50    | 3         |
    | R03       | SRC_A  | -$120.00    | 2         |
    | R04       | SRC_A  | N/A         | 7         |
    | R05       | SRC_B  | 420.00      | 12.0      |
    | R06       | SRC_B  | $99.50      | 4         |
    | R07       | SRC_B  | 1,000.00    | 0         |
    | R08       | SRC_C  | $300.00     | 0         |
    | R09       | SRC_C  | NULL        | 0         |
    | R10       | SRC_C  | $200.00     | 0         |
    +-----------+--------+-------------+-----------+

Notes on the input:
  - R02's amount is padded with two spaces on each side.
  - R03 is a refund: it parses to -120.00.
  - R04's amount is the literal text 'N/A' — unparseable.
  - R05's units come from the float-serializing system.
  - R09's amount_raw is a genuine NULL, not the string 'NULL'.

Expected:

    +--------+-----------+--------------+------------+-------------+----------------+
    | source | n_records | n_bad_amount | net_amount | total_units | avg_unit_price |
    +--------+-----------+--------------+------------+-------------+----------------+
    | SRC_A  | 4         | 1            | 1980.5     | 17          | 116.5          |
    | SRC_B  | 3         | 0            | 1519.5     | 16          | 94.97          |
    | SRC_C  | 3         | 1            | 500.0      | 0           | NULL           |
    +--------+-----------+--------------+------------+-------------+----------------+

Notes on the expected output:
  - SRC_A: 1250.00 + 850.50 - 120.00 = 1980.50 over three parsed rows;
    R04 is the one bad amount, but its 7 units still count, so
    total_units = 5 + 3 + 2 + 7 = 17, and 1980.50 / 17 = 116.50 exactly.
  - SRC_B: 420.00 + 99.50 + 1000.00 = 1519.50; units 12 + 4 + 0 = 16;
    1519.50 / 16 = 94.96875, which rounds to 94.97.
  - SRC_C: every unit count is a usable zero, so total_units is 0 — not
    unknown, not missing — and avg_unit_price is NULL.

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
refs/day20_ansi_try_functions_ref.md.
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

        Hint: clean each string into a per-record parsed value first, then
        let that parsed value be the single source of truth for whether
        the record is usable.
        """
        # TODO: implement
        cleaned = (
            df.withColumn(
                'cleaned_amount',
                F.coalesce(
                    F.try_to_number(F.col('amount_raw'), F.lit('S$999,999,999.99')),
                    F.try_to_number(F.col('amount_raw'), F.lit('S999,999,999.99')),
                    )
            )
            .withColumn(
                'cleaned_units',
                F.col('units_raw').try_cast('double').try_cast('int')
            )
        )

        agg = (
            cleaned.groupBy('source')
            .agg(
                F.count(F.lit(1)).alias('n_records'),
                F.count_if(F.col('cleaned_amount').isNull()).alias('n_bad_amount'),
                F.try_sum(F.col('cleaned_amount')).try_cast('double').alias('net_amount'),
                F.try_sum(F.col('cleaned_units')).alias('total_units')
            ).withColumn(
                'avg_unit_price',
               F.round( F.try_divide(F.col('net_amount'), F.col('total_units')), 2)
            )
            )

        return ( agg.select(
            'source',
            'n_records',
            'n_bad_amount',
            'net_amount',
            'total_units',
            'avg_unit_price'
        )
        )

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """Spark SQL approach.

        Hint: the try_* family has the same names in SQL; note that
        try_divide is a function, not an operator.
        """
        df.createOrReplaceTempView("raw_sales")

        sql = """
            -- write your SQL here
            with cleaned as (
                select 
                    source,
                    coalesce(try_to_number(amount_raw, 'S$999,999,999.99'), try_to_number(amount_raw, 'S999,999,999.99')) as cleaned_amount,
                    try_cast(try_cast(units_raw as DOUBLE) as BIGINT) as cleaned_units
                from 
                    raw_sales
            )
            select 
                *,
                round(try_divide(net_amount, total_units), 2) as avg_unit_price
            from
            (select 
                source,
                count(*) as n_records,
                count_if(cleaned_amount is null) as n_bad_amount,
                try_cast(try_sum(cleaned_amount) as double) net_amount,
                try_sum(cleaned_units) as total_units
            from 
                cleaned
            group by source) tb1
        """
        return spark.sql(sql)
    
def ai_solve_dsl(df: DataFrame) -> DataFrame:
    amt_clean = F.regexp_replace(F.trim(F.col("amount_raw")), r"[$,]", "")
    amt = F.when(
        amt_clean.rlike(r"^-?([0-9]+(\.[0-9]*)?|\.[0-9]+)$"), amt_clean
    ).try_cast("double")

    units_clean = F.trim(F.col("units_raw"))
    units = F.when(
        units_clean.rlike(r"^[+-]?[0-9]+(\.0+)?$"),
        F.regexp_extract(units_clean, r"^([+-]?[0-9]+)", 1),
    ).try_cast("int")

    parsed = df.select(
        F.col("source"),
        amt.alias("amt"),
        units.alias("units"),
    )

    agg = parsed.groupBy("source").agg(
        F.count(F.lit(1)).cast("int").alias("n_records"),
        F.count(F.when(F.col("amt").isNull(), F.lit(1))).cast("int").alias("n_bad_amount"),
        F.coalesce(F.sum("amt"), F.lit(0.0)).cast("double").alias("net_amount"),
        F.coalesce(F.sum("units"), F.lit(0)).cast("int").alias("total_units"),
    )

    return agg.select(
        F.col("source"),
        F.col("n_records"),
        F.col("n_bad_amount"),
        F.col("net_amount"),
        F.col("total_units"),
        F.when(
            F.col("total_units") != F.lit(0),
            F.round(F.try_divide(F.col("net_amount"), F.col("total_units")), 2),
        ).cast("double").alias("avg_unit_price"),
    )


def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("raw_sales")
    return spark.sql(
        r"""
        WITH parsed AS (
            SELECT
                source,
                TRY_CAST(
                    CASE
                        WHEN regexp_replace(trim(amount_raw), '[$,]', '')
                             RLIKE '^-?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)$'
                        THEN regexp_replace(trim(amount_raw), '[$,]', '')
                    END AS DOUBLE
                ) AS amt,
                TRY_CAST(
                    CASE
                        WHEN trim(units_raw) RLIKE '^[+-]?[0-9]+(\\.0+)?$'
                        THEN regexp_extract(trim(units_raw), '^([+-]?[0-9]+)', 1)
                    END AS INT
                ) AS units
            FROM raw_sales
        ),
        agg AS (
            SELECT
                source,
                CAST(count(1) AS INT) AS n_records,
                CAST(count(CASE WHEN amt IS NULL THEN 1 END) AS INT) AS n_bad_amount,
                CAST(coalesce(sum(amt), 0.0) AS DOUBLE) AS net_amount,
                CAST(coalesce(sum(units), 0) AS INT) AS total_units
            FROM parsed
            GROUP BY source
        )
        SELECT
            source,
            n_records,
            n_bad_amount,
            net_amount,
            total_units,
            CAST(
                CASE
                    WHEN total_units <> 0
                    THEN round(try_divide(net_amount, total_units), 2)
                END AS DOUBLE
            ) AS avg_unit_price
        FROM agg
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

    SCHEMA = "record_id string, source string, amount_raw string, units_raw string"

    data = [
        # (record_id, source, amount_raw, units_raw)
        ("R01", "SRC_A", "$1,250.00", "5"),
        ("R02", "SRC_A", "  850.50  ", "3"),
        ("R03", "SRC_A", "-$120.00", "2"),
        ("R04", "SRC_A", "N/A", "7"),
        ("R05", "SRC_B", "420.00", "12.0"),
        ("R06", "SRC_B", "$99.50", "4"),
        ("R07", "SRC_B", "1,000.00", "0"),
        ("R08", "SRC_C", "$300.00", "0"),
        ("R09", "SRC_C", None, "0"),
        ("R10", "SRC_C", "$200.00", "0"),
    ]
    df = spark.createDataFrame(data, schema=SCHEMA)

    expected = [
        # (source, n_records, n_bad_amount, net_amount, total_units, avg_unit_price)
        ("SRC_A", 4, 1, 1980.5, 17, 116.5),
        ("SRC_B", 3, 0, 1519.5, 16, 94.97),
        ("SRC_C", 3, 1, 500.0, 0, None),
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
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes:not sure if AI is correct because I am not familiar with regex
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes: besides regex part, other syntax LGTM
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (element_at, array index, cast, division)?
#     notes: LGTM
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (do NOT guess — this is a reading-stage hypothesis, verified later)
#     notes: no extra shuffle, just format transformation, LGTM
# [ ] Robustness — hardcoded values, assumptions not in the problem statement?
#     notes: LGTM
# [ ] Style/clarity — would you approve this in a real code review?
#     notes: LGTM
#
# VERDICT (commit before running): PASS / FAIL — because: not sure, because I can not evaluate if correction of regex
# ACTUAL RESULT (after Stage 4 run): PASS
# GAP ANALYSIS: did the run reveal anything the reading missed? no, besides regex part


# =====================================================================
# Part 5 — Review takeaways (fill in at Stage 5, before /review)
# =====================================================================
# - What did the AI get wrong, or suspiciously right?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem:
# see refs/day20_ansi_try_functions_ref.md.
