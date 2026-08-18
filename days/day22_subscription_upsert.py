"""
=====================================================================
PySpark Daily Practice — ETL Scenario Template (Day 22 onward)
=====================================================================
Day 22 — dim_subscription  (Medium-Hard)

ETL layer : L4 incremental
Domain    : subscription billing

BUSINESS CONTEXT
----------------
Billing keeps a subscription dimension that downstream revenue reporting
reads every morning. A nightly job applies one batch of CDC changes to
yesterday's snapshot of that dimension and publishes the new current state.

INPUT TABLES
------------
dim_subscription_current(subscription_id: string, plan: string,
                         status: string, mrr: double, updated_at: string)
    -- daily snapshot of the TARGET dimension, as published yesterday

subscription_changes(subscription_id: string, op: string, plan: string,
                     status: string, mrr: double, change_ts: string)
    -- append-only CDC feed for today's batch. `op` is 'I' (insert),
       'U' (update) or 'D' (delete). The feed MAY carry more than one
       change for the same subscription in a single batch.

plan_catalog(plan: string, mrr: double)
    -- hand-maintained config table: the authoritative price per plan

Timestamps are ISO-8601 STRINGS, not the TIMESTAMP type. They are
fixed-width, so lexicographic order equals chronological order and you can
compare them directly. This day is about the merge, not about timestamp
parsing.

dim_subscription_current:

    +-----------------+------------+--------+-------+---------------------+
    | subscription_id | plan       | status | mrr   | updated_at          |
    +-----------------+------------+--------+-------+---------------------+
    | SUB1            | BASIC      | ACTIVE |  10.0 | 2026-08-15 09:00:00 |
    | SUB2            | PRO        | ACTIVE |  30.0 | 2026-08-17 18:30:00 |
    | SUB3            | PRO        | ACTIVE |  30.0 | 2026-08-10 12:00:00 |
    | SUB4            | ENTERPRISE | ACTIVE | 100.0 | 2026-08-16 08:00:00 |
    | SUB5            | BASIC      | PAUSED |  10.0 | 2026-08-14 11:00:00 |
    +-----------------+------------+--------+-------+---------------------+

subscription_changes:

    +-----------------+----+------------+--------+------+---------------------+
    | subscription_id | op | plan       | status | mrr  | change_ts           |
    +-----------------+----+------------+--------+------+---------------------+
    | SUB1            | U  | PRO        | ACTIVE | 30.0 | 2026-08-18 09:15:00 |
    | SUB2            | U  | BASIC      | ACTIVE | 10.0 | 2026-08-17 10:00:00 |
    | SUB3            | U  | ENTERPRISE | ACTIVE | 55.0 | 2026-08-18 11:00:00 |
    | SUB4            | D  | NULL       | NULL   | NULL | 2026-08-18 07:45:00 |
    | SUB5            | U  | BASIC      | ACTIVE | 10.0 | 2026-08-18 08:00:00 |
    | SUB5            | U  | PRO        | ACTIVE | 30.0 | 2026-08-18 16:00:00 |
    | SUB6            | I  | BASIC      | ACTIVE | 10.0 | 2026-08-18 13:20:00 |
    +-----------------+----+------------+--------+------+---------------------+

    Note: a 'D' row carries no attribute values — only the key and the
    timestamp are meaningful on a tombstone.

plan_catalog:

    +------------+-------+
    | plan       | mrr   |
    +------------+-------+
    | BASIC      |  10.0 |
    | PRO        |  30.0 |
    | ENTERPRISE | 100.0 |
    +------------+-------+

OUTPUT CONTRACT
---------------
Table    : dim_subscription
Grain    : one row per subscription_id — the state after this batch is
           applied. A subscription must never appear twice, including when
           the feed carried several changes for it (the latest change in the
           batch wins).
Columns  : subscription_id: string, plan: string, status: string,
           mrr: double, updated_at: string   (this exact order)
Ordering : irrelevant — check() sorts both sides

`updated_at` is the timestamp of the change actually applied to that
subscription; a subscription this batch did not change keeps the value it
already had.

Expected:

    +-----------------+------------+--------+-------+---------------------+
    | subscription_id | plan       | status | mrr   | updated_at          |
    +-----------------+------------+--------+-------+---------------------+
    | SUB1            | PRO        | ACTIVE |  30.0 | 2026-08-18 09:15:00 |
    | SUB2            | PRO        | ACTIVE |  30.0 | 2026-08-17 18:30:00 |
    | SUB3            | ENTERPRISE | ACTIVE | 100.0 | 2026-08-18 11:00:00 |
    | SUB5            | PRO        | ACTIVE |  30.0 | 2026-08-18 16:00:00 |
    | SUB6            | BASIC      | ACTIVE |  10.0 | 2026-08-18 13:20:00 |
    +-----------------+------------+--------+-------+---------------------+

PRODUCTION CONSTRAINTS
----------------------
P1. Re-running this job on the same two inputs must produce byte-identical
    output. The batch is replayed whenever the scheduler retries.
P2. Every output row's `mrr` must come from `plan_catalog`. The `mrr` the
    change feed carries is advisory — upstream computes it independently and
    it is not the system of record.
P3. A subscription deleted in this batch (`op = 'D'`) must not appear in the
    output at all.

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

Reference answers and concept takeaways live in
refs/day22_subscription_upsert_ref.md. Do not open it before Stage 5.
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
def build_dim_subscription_dsl(
    dim_subscription_current: DataFrame,
    subscription_changes: DataFrame,
    plan_catalog: DataFrame,
) -> DataFrame:
    """Apply today's CDC batch to the subscription dimension.

    Hint: decide what a change is allowed to do to the row it lands on.
    """
    # TODO: implement
    pass


# ------------------------------ 1b. Spark SQL ------------------------
def build_dim_subscription_sql(
    spark: SparkSession,
    dim_subscription_current: DataFrame,
    subscription_changes: DataFrame,
    plan_catalog: DataFrame,
) -> DataFrame:
    """Same job, Spark SQL.

    Hint: decide what a change is allowed to do to the row it lands on.
    """
    dim_subscription_current.createOrReplaceTempView("dim_subscription_current")
    subscription_changes.createOrReplaceTempView("subscription_changes")
    plan_catalog.createOrReplaceTempView("plan_catalog")

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
    dim_subscription_current = spark.createDataFrame(
        [
            ("SUB1", "BASIC", "ACTIVE", 10.0, "2026-08-15 09:00:00"),
            ("SUB2", "PRO", "ACTIVE", 30.0, "2026-08-17 18:30:00"),
            ("SUB3", "PRO", "ACTIVE", 30.0, "2026-08-10 12:00:00"),
            ("SUB4", "ENTERPRISE", "ACTIVE", 100.0, "2026-08-16 08:00:00"),
            ("SUB5", "BASIC", "PAUSED", 10.0, "2026-08-14 11:00:00"),
        ],
        schema=(
            "subscription_id STRING, plan STRING, status STRING, "
            "mrr DOUBLE, updated_at STRING"
        ),
    )
    subscription_changes = spark.createDataFrame(
        [
            ("SUB1", "U", "PRO", "ACTIVE", 30.0, "2026-08-18 09:15:00"),
            ("SUB2", "U", "BASIC", "ACTIVE", 10.0, "2026-08-17 10:00:00"),
            ("SUB3", "U", "ENTERPRISE", "ACTIVE", 55.0, "2026-08-18 11:00:00"),
            ("SUB4", "D", None, None, None, "2026-08-18 07:45:00"),
            ("SUB5", "U", "BASIC", "ACTIVE", 10.0, "2026-08-18 08:00:00"),
            ("SUB5", "U", "PRO", "ACTIVE", 30.0, "2026-08-18 16:00:00"),
            ("SUB6", "I", "BASIC", "ACTIVE", 10.0, "2026-08-18 13:20:00"),
        ],
        schema=(
            "subscription_id STRING, op STRING, plan STRING, status STRING, "
            "mrr DOUBLE, change_ts STRING"
        ),
    )
    plan_catalog = spark.createDataFrame(
        [
            ("BASIC", 10.0),
            ("PRO", 30.0),
            ("ENTERPRISE", 100.0),
        ],
        schema="plan STRING, mrr DOUBLE",
    )

    expected = [
        ("SUB1", "PRO", "ACTIVE", 30.0, "2026-08-18 09:15:00"),
        ("SUB2", "PRO", "ACTIVE", 30.0, "2026-08-17 18:30:00"),
        ("SUB3", "ENTERPRISE", "ACTIVE", 100.0, "2026-08-18 11:00:00"),
        ("SUB5", "PRO", "ACTIVE", 30.0, "2026-08-18 16:00:00"),
        ("SUB6", "BASIC", "ACTIVE", 10.0, "2026-08-18 13:20:00"),
    ]

    check(
        build_dim_subscription_dsl(
            dim_subscription_current, subscription_changes, plan_catalog
        ),
        expected,
        "DSL",
    )
    check(
        build_dim_subscription_sql(
            spark, dim_subscription_current, subscription_changes, plan_catalog
        ),
        expected,
        "SQL",
    )

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    # check(ai_build_dim_subscription_dsl(
    #           dim_subscription_current, subscription_changes, plan_catalog),
    #       expected, "AI-DSL (post-review verification)")
    # check(ai_build_dim_subscription_sql(
    #           spark, dim_subscription_current, subscription_changes, plan_catalog),
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
# Concept takeaways: see refs/day22_subscription_upsert_ref.md.
