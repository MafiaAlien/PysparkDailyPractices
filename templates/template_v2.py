"""
=====================================================================
PySpark Daily Practice — Template v2 (Claude Code edition)
=====================================================================
Day N — <Topic>  (<Difficulty: Easy / Medium / Medium-Hard / Hard>)

PROBLEM
-------
<Problem statement goes here.>

INPUT SCHEMA
------------
<table_name>(col_a: type, col_b: type, ...)

EXPECTED OUTPUT
---------------
<Describe expected columns, in order, and ordering semantics.>

EXAMPLE
-------
<Input rows and expected output rows.>

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

Reference answers and concept takeaways live in the sibling *_ref.md file.
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

        Hint: <one short hint>
        """
        # TODO: implement
        pass

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """Spark SQL approach.

        Hint: <one short hint>
        """
        df.createOrReplaceTempView("<view_name>")

        sql = """
            -- write your SQL here
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

    data = [
        # TODO: test rows (include the trap rows)
    ]
    df = spark.createDataFrame(data, schema="<schema string>")

    expected = [
        # TODO: expected tuples, hand-derived
    ]

    s = Solution()
    check(s.solve_dsl(df), expected, "DSL")
    check(s.solve_sql(spark, df), expected, "SQL")

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # ------------------------------------------------------------------
    # check(ai_solve_dsl(df), expected, "AI-DSL (post-review verification)")
    # check(ai_solve_sql(spark, df), expected, "AI-SQL (post-review verification)")

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
#     notes:
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes:
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (element_at, array index, cast, division)?
#     notes:
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (do NOT guess — this is a reading-stage hypothesis, verified later)
#     notes:
# [ ] Robustness — hardcoded values, assumptions not in the problem statement?
#     notes:
# [ ] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS / FAIL — because:
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything the reading missed?


# =====================================================================
# Part 5 — Review takeaways (fill in at Stage 5, before /review)
# =====================================================================
# - What did the AI get wrong, or suspiciously right?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem: see the sibling *_ref.md file.
