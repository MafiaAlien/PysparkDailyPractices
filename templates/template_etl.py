"""
=====================================================================
PySpark Daily Practice — ETL Scenario Template (Day 22 onward)
=====================================================================
Day N — <output_table_name>  (<Medium / Medium-Hard / Hard>)

ETL layer : <L1 landing cleanup | L2 detail modeling | L3 serving | L4 incremental>
Domain    : <e-commerce orders | ad delivery | payment reconciliation |
             clickstream | logistics | content | subscription billing>

BUSINESS CONTEXT
----------------
<1-2 sentences: who consumes this table, in which system, how often the job
runs, and what upstream feeds it.>

INPUT TABLES
------------
<table_a>(col: type, ...)   -- append-only event feed; upstream may replay
<table_b>(col: type, ...)   -- daily snapshot dimension
<table_c>(col: type, ...)   -- hand-maintained config table

Render each input as a df.show()-style ASCII box with a header row.
These boxes are the FULL test data, not a sample: every box must match the
harness spark.createDataFrame rows exactly, and the Expected box below must
match the `expected` list exactly. /genprompt ships the boxes and nothing
else to the incognito AI — if they drift, the AI solves a different dataset
and the Stage-4 AI checks fail for a reason unrelated to its solution.

<table_a>:

    +-------+-------+-------+
    | col_a | col_b | col_c |
    +-------+-------+-------+
    | ...   | ...   | ...   |
    +-------+-------+-------+

Per-row annotations go UNDER the box as notes, never inside the cells.

OUTPUT CONTRACT
---------------
Table    : <output_table_name>
Grain    : one row per <what>
Columns  : <name: type>, <name: type>, ...   (this exact order)
Ordering : irrelevant — check() sorts both sides

Expected:

    +-------+-------+
    | out_a | out_b |
    +-------+-------+
    | ...   | ...   |
    +-------+-------+

PRODUCTION CONSTRAINTS
----------------------
P1. <e.g. Re-running the same batch twice must produce byte-identical rows.>
P2. <e.g. An event arriving up to 3 days late belongs to its EVENT date,
     not its arrival date.>
P3. <optional third>

These are requirements, not hints. Missing one fails the tests.

WORKFLOW (v2)
-------------
Stage 1  SOLVE   : Implement the two Part 1 functions. Run this file.
Stage 2  GENERATE: Run /genprompt and paste the emitted prompt into a
                   SEPARATE incognito conversation. Never ask for the
                   solution inside this repo.
Stage 3  REVIEW  : Paste the AI answer into the Part 3 zone, UNMODIFIED.
                   Review by reading only. Fill REVIEW_NOTES and commit a
                   VERDICT BEFORE running anything.
Stage 4  VERIFY  : Un-comment the Stage-4 lines in Part 2 and run.
Stage 5  DIGEST  : Fill Part 5, then run /review and /digest.

Reference answers and concept takeaways live in refs/<this-file>_ref.md.
Do not open it before Stage 5.
=====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# #####################################################################
# ##                                                                 ##
# ##   PART 1 — YOUR JOB                                             ##
# ##                                                                 ##
# ##   Stage 1 touches this block and nothing else in this file.     ##
# ##                                                                 ##
# #####################################################################

# ------------------------------ 1a. DataFrame API --------------------
def build_output_table_dsl(
    table_a: DataFrame,
    table_b: DataFrame,
) -> DataFrame:
    """<one line: what this job produces>

    Hint: <one short hint>
    """
    # TODO: implement
    pass


# ------------------------------ 1b. Spark SQL ------------------------
def build_output_table_sql(
    spark: SparkSession,
    table_a: DataFrame,
    table_b: DataFrame,
) -> DataFrame:
    """<same job, Spark SQL>

    Hint: <one short hint>
    """
    table_a.createOrReplaceTempView("table_a")
    table_b.createOrReplaceTempView("table_b")

    sql = """
        -- write your SQL here
    """
    return spark.sql(sql)


# #####################################################################
# ##   END OF PART 1                                                 ##
# #####################################################################


# #####################################################################
# ##                                                                 ##
# ##   PART 3 — PASTE THE INCOGNITO AI ANSWER BELOW                  ##
# ##                                                                 ##
# ##   UNMODIFIED. Two functions, `ai_` prefix, signatures identical ##
# ##   to Part 1. Do not reformat it. Do not fix anything you spot — ##
# ##   spotting it is exactly what Stage 3 measures.                 ##
# ##                                                                 ##
# #####################################################################

# >>> PASTE BEGIN

# >>> PASTE END

# ---------------------------------------------------------------------
# REVIEW_NOTES — fill in BEFORE running anything.
# Severity order: runtime break > wrong results on dirty data
#               > production robustness > performance > portability > style
# ---------------------------------------------------------------------
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes:
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes:
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (cast, division, array index, element_at)?
#     notes:
# [ ] Production — walk P1/P2/P3 one at a time, each on its own line.
#     Re-run idempotent? Late data attributed to the event date? The same
#     metric computed the same way in every branch? Quality assertions there?
#     notes:
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (reading-stage hypothesis only — verified with .explain() later,
#     never asserted from memory)
#     notes:
# [ ] Robustness — hardcoded values, assumptions not in the output contract?
#     notes:
# [ ] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS / FAIL — because:
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything the reading missed?


# #####################################################################
# ##   PART 2 — HARNESS.  Do not edit.                               ##
# #####################################################################
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

    # Input tables: <= 8 rows each, <= 25 rows across ALL inputs.
    table_a = spark.createDataFrame(
        [
            # TODO: rows (include the trap rows)
        ],
        schema="<schema string>",
    )
    table_b = spark.createDataFrame(
        [
            # TODO: rows
        ],
        schema="<schema string>",
    )

    expected = [
        # TODO: hand-derived tuples, one per output row
    ]

    check(build_output_table_dsl(table_a, table_b), expected, "DSL")
    check(build_output_table_sql(spark, table_a, table_b), expected, "SQL")

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    # check(ai_build_output_table_dsl(table_a, table_b),
    #       expected, "AI-DSL (post-review verification)")
    # check(ai_build_output_table_sql(spark, table_a, table_b),
    #       expected, "AI-SQL (post-review verification)")

    spark.stop()


# #####################################################################
# ##   PART 5 — Review takeaways (fill at Stage 5, before /review)   ##
# #####################################################################
# - What did the AI get wrong, or suspiciously right?
# - Which production constraint (P#) did either side miss, and why was it
#   missable by reading?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem: see refs/<this-file>_ref.md.
