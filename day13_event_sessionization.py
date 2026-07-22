"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 13 — Event sessionization (gap-threshold sessions)  (Medium-Hard)

PROBLEM
-------
You receive a raw clickstream of user events. Business defines a
SESSION as follows:

  * Events belong to the same session when the gap between an event
    and the PREVIOUS event of the SAME user is AT MOST 30 minutes
    (gap <= 30 minutes  =>  same session).
  * A gap STRICTLY GREATER than 30 minutes starts a new session.
  * A session's start is the timestamp of its FIRST event; a session's
    end is the timestamp of its LAST event (NOT last event + gap).

For every session of every user, report:
  user_id, session_start, session_end, n_events

Events may arrive in the DataFrame in ANY order. Multiple events may
share the exact same timestamp (double-click / replay); they belong to
the same session and each counts as one event.

INPUT SCHEMA
------------
events(user_id: string, event_ts: timestamp)

EXPECTED OUTPUT
---------------
Columns, in this exact order:
  user_id: string
  session_start: timestamp   -- min event_ts of the session
  session_end: timestamp     -- max event_ts of the session
  n_events: bigint           -- number of events in the session

One row per (user, session). Output order does not matter
(the test harness sorts).

EXAMPLE
-------
Input (user u9):
  u9  2026-03-01 08:00:00
  u9  2026-03-01 08:25:00      -- 25 min gap  -> same session
  u9  2026-03-01 09:10:00      -- 45 min gap  -> NEW session

Output:
  (u9, 2026-03-01 08:00:00, 2026-03-01 08:25:00, 2)
  (u9, 2026-03-01 09:10:00, 2026-03-01 09:10:00, 1)

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
    def solve_dsl(self, df: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: this is gaps-and-islands wearing a timestamp costume.
        Think in three moves: (1) look at the previous event of the
        same user, (2) mark session boundaries, (3) turn the marks
        into a session key you can group by. Re-read your Day 8 note
        about what an orderBy does to a window's default frame —
        this time that behavior is not the bug, it is the tool.
        """
        # TODO: implement
        window_spec = Window.partitionBy('user_id').orderBy('event_ts')

        get_prev = (
            df.select(
              'user_id',
              'event_ts',
              F.lag('event_ts', 1).over(window_spec).alias('prev_event_ts')
        ))

        time_diff_with_flag = (
            get_prev.select(
                'user_id',
                'event_ts',
                'prev_event_ts',
                F.timestamp_diff(
                    'SECOND', 
                    F.col('prev_event_ts'), 
                    F.col('event_ts')).alias('time_span')
            )
            .withColumn(
                'is_new',
                 F.when(
                     (
                    F.col('time_span') > 1800) | (F.col('prev_event_ts').isNull()), 
                    F.lit(1)).otherwise(F.lit(0))        
                 )
        )

        set_boundary = (
            time_diff_with_flag.select(
                'user_id',
                'event_ts',
                'is_new',
                F.sum(F.col('is_new')).over(window_spec).alias('boundary')
            )
            .groupBy('user_id', F.col('boundary'))
            .agg(
                F.min('event_ts').alias('session_start'),
                F.max('event_ts').alias('session_end'),
                F.count(F.lit(1)).alias('n_events')
            )
        )

        res_df = (set_boundary.select(
            'user_id',
            'session_start',
            'session_end',
            'n_events'
        ))
        return res_df

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: two stacked CTEs mirror the DSL moves; LAG lives in the
        first, the session key in the second, GROUP BY at the end.
        """
        df.createOrReplaceTempView("events")

        sql = """
            -- write your SQL here
        with get_prev as (
        select 
          user_id,
          event_ts,
          lag(event_ts, 1)over(partition by user_id order by event_ts) as prev_event_ts
        from
          events
        ),

        ts_diff as (
          select 
            user_id,
            event_ts,
            prev_event_ts,
            round((unix_timestamp(event_ts)- unix_timestamp(prev_event_ts)) / 60.0, 2) as diff
          from 
            get_prev
        ),

        flag as (
          select 
            user_id,
            event_ts,
            prev_event_ts,
            case when (diff > 30) or (prev_event_ts is null) then 1 else 0 end as is_new
          from 
            ts_diff
        ),

        set_boundary as (
          select 
            user_id,
            event_ts,
            prev_event_ts,
            sum(is_new)over(partition by user_id order by event_ts) as boundary
          from
            flag
        )

        select 
          user_id,
          min(event_ts) as session_start,
          max(event_ts) as session_end,
          count(*) as n_events
        from 
          set_boundary
        group by 
          user_id,
          boundary;
        """
        return spark.sql(sql)

def ai_solve_dsl(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("user_id").orderBy("event_ts")
    prev = F.lag("event_ts").over(w)
    gap_min = (F.col("event_ts").cast("long") - prev.cast("long")) / 60.0
    is_new = F.when(prev.isNull() | (gap_min > 30), 1).otherwise(0)
    sid = F.sum("is_new").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow))

    marked = df.withColumn("is_new", is_new).withColumn("session_id", sid)

    return (
        marked.groupBy("user_id", "session_id")
        .agg(
            F.min("event_ts").alias("session_start"),
            F.max("event_ts").alias("session_end"),
            F.count(F.lit(1)).alias("n_events"),
        )
        .select("user_id", "session_start", "session_end", "n_events")
    )


def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("events")
    return spark.sql(
        """
        WITH flagged AS (
            SELECT
                user_id,
                event_ts,
                CASE
                    WHEN LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) IS NULL
                         OR (CAST(event_ts AS LONG) -
                             CAST(LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS LONG)) / 60.0 > 30
                    THEN 1 ELSE 0
                END AS is_new
            FROM events
        ),
        sessioned AS (
            SELECT
                user_id,
                event_ts,
                SUM(is_new) OVER (
                    PARTITION BY user_id ORDER BY event_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS session_id
            FROM flagged
        )
        SELECT
            user_id,
            MIN(event_ts) AS session_start,
            MAX(event_ts) AS session_end,
            COUNT(*) AS n_events
        FROM sessioned
        GROUP BY user_id, session_id
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
    from datetime import datetime as dt

    spark = (
        SparkSession.builder.appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    data = [
        # u1 — deliberately inserted OUT of chronological order
        ("u1", dt(2026, 3, 1, 10, 20, 0)),
        ("u1", dt(2026, 3, 1, 10, 0, 0)),
        ("u1", dt(2026, 3, 1, 10, 50, 0)),   # gap from 10:20 = exactly 30 min
        ("u1", dt(2026, 3, 1, 11, 45, 0)),   # gap 55 min
        # u2 — duplicate timestamp (double-click replay)
        ("u2", dt(2026, 3, 1, 9, 0, 0)),
        ("u2", dt(2026, 3, 1, 9, 29, 0)),
        ("u2", dt(2026, 3, 1, 9, 0, 0)),
        ("u2", dt(2026, 3, 1, 10, 10, 0)),   # gap 41 min
        # u3 — single-event user
        ("u3", dt(2026, 3, 1, 12, 0, 0)),
    ]
    df = spark.createDataFrame(data, schema="user_id string, event_ts timestamp")

    expected = [
        ("u1", dt(2026, 3, 1, 10, 0, 0), dt(2026, 3, 1, 10, 50, 0), 3),
        ("u1", dt(2026, 3, 1, 11, 45, 0), dt(2026, 3, 1, 11, 45, 0), 1),
        ("u2", dt(2026, 3, 1, 9, 0, 0), dt(2026, 3, 1, 9, 29, 0), 3),
        ("u2", dt(2026, 3, 1, 10, 10, 0), dt(2026, 3, 1, 10, 10, 0), 1),
        ("u3", dt(2026, 3, 1, 12, 0, 0), dt(2026, 3, 1, 12, 0, 0), 1),
    ]

    s = Solution()
    check(s.solve_dsl(df), expected, "DSL")
    check(s.solve_sql(spark, df), expected, "SQL")

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
# [pass] Correctness — edge cases: ties? nulls? empty groups? duplicate keys?
#     notes:
# [pass] API usage — wrong signatures, deprecated calls, ambiguous column
#     references in joins, SQL syntax slips (trailing commas, quoting)?
#     notes:
# [pass] Performance — unnecessary shuffles, window over the whole table
#     without partitionBy, join strategy, redundant actions/collects?
#     notes:
# [fail] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes:the time span does not use second but use minute instead, this will violate coincidentally-correct
# [pass] Style/clarity — would you approve this in a real code review?
#     notes: 
#
# VERDICT (commit before running): PASS  — because: use minute to divide time gap will be misleading, but it does not block whole logic of DSL and SQL
# ACTUAL RESULT (after Stage 4 run): passed
# GAP ANALYSIS: did the run reveal anything your reading missed?
#  No


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# Route A — lag + boundary flag + running sum (the canonical idiom):
#
# def ref_solve_dsl(df):
#     w_ord = Window.partitionBy("user_id").orderBy("event_ts")
#     prev = F.lag("event_ts").over(w_ord)
#     gap_sec = F.unix_timestamp("event_ts") - F.unix_timestamp(prev)
#     # boundary: STRICTLY greater than 30 min starts a new session;
#     # gap == 1800 sec stays in the SAME session.  `>= 1800` is wrong.
#     is_new = F.when(prev.isNull() | (gap_sec > 1800), 1).otherwise(0)
#     # Running sum: the ordered window's default frame
#     # (RANGE UNBOUNDED PRECEDING .. CURRENT ROW) is exactly what we
#     # want here — the Day 8 footgun used deliberately. Note RANGE
#     # frames treat duplicate event_ts as PEERS: two flag rows sharing
#     # a ts would both see each other's flag. Safe here because a
#     # duplicate ts always has gap 0 -> flag 0, so peers contribute 0.
#     # If you want peer-independence on principle, make it explicit:
#     # .rowsBetween(Window.unboundedPreceding, Window.currentRow).
#     sess = df.withColumn("is_new", is_new) \
#              .withColumn("session_id", F.sum("is_new").over(w_ord))
#     return (sess.groupBy("user_id", "session_id")
#                 .agg(F.min("event_ts").alias("session_start"),
#                      F.max("event_ts").alias("session_end"),
#                      F.count(F.lit(1)).alias("n_events"))
#                 .select("user_id", "session_start", "session_end",
#                         "n_events"))
#
# def ref_solve_sql(spark, df):
#     df.createOrReplaceTempView("events")
#     return spark.sql("""
#         WITH lagged AS (
#             SELECT user_id, event_ts,
#                    LAG(event_ts) OVER
#                        (PARTITION BY user_id ORDER BY event_ts) AS prev_ts
#             FROM events
#         ),
#         flagged AS (
#             SELECT user_id, event_ts,
#                    CASE WHEN prev_ts IS NULL
#                          OR (UNIX_TIMESTAMP(event_ts)
#                              - UNIX_TIMESTAMP(prev_ts)) > 1800
#                         THEN 1 ELSE 0 END AS is_new
#             FROM lagged
#         ),
#         keyed AS (
#             SELECT user_id, event_ts,
#                    SUM(is_new) OVER
#                        (PARTITION BY user_id ORDER BY event_ts)
#                        AS session_id
#             FROM flagged
#         )
#         SELECT user_id,
#                MIN(event_ts) AS session_start,
#                MAX(event_ts) AS session_end,
#                COUNT(*)      AS n_events
#         FROM keyed
#         GROUP BY user_id, session_id
#     """)
#
# Route B — built-in session_window (WHY IT FAILS THIS SPEC):
#
#     df.groupBy("user_id", F.session_window("event_ts", "30 minutes"))
#       .agg(...)
#
#   Two mismatches against this problem's contract:
#   1. Boundary semantics: session_window closes the session at
#      last_ts + gap and merges a new event only if new_ts < that end
#      (STRICT <). An event arriving at EXACTLY +30:00 does NOT merge —
#      but our spec says gap <= 30 min is the same session. u1's
#      10:20 -> 10:50 row splits under session_window, stays merged
#      under the spec.
#   2. End semantics: session_window.end = last event + 30 min, not the
#      last event's timestamp. Every session_end would be shifted +30
#      min (start is fine).
#   session_window is the right tool when the SPEC is written in its
#   semantics (esp. streaming); here the spec is written in lag terms.
#
# All-window plan note: lag-window and sum-window share the SAME
# partitionBy expression (user_id) -> ONE Exchange for both (Day 8
# spec-not-function rule); the final groupBy(user_id, session_id) is a
# subset->superset mismatch in the other direction — (user_id) covers
# (user_id, session_id)? No: partitioning by user_id ALSO satisfies a
# groupBy on (user_id, session_id)'s correctness only via a fresh
# hash — check explain(): whether ENSURE_REQUIREMENTS reuses the
# user_id partitioning (all rows of one user are co-located, which is
# sufficient) or inserts a new Exchange. Verify, don't assert.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - Sessionization = gaps-and-islands with a THRESHOLD instead of
#   strict +1: lag -> boundary flag -> running SUM of flags as island
#   key. Day 2 used (value - row_number) constant-difference; a gap
#   threshold breaks that arithmetic, so the flag+running-sum idiom is
#   the general form.
# - The ordered-window default frame (RANGE UNBOUNDED PRECEDING ..
#   CURRENT ROW) that was the Day 8 BUG (accidental running count) is
#   the TOOL here (deliberate running sum). Same mechanism, opposite
#   verdict — judge the frame against intent, not by reflex.
# - RANGE vs ROWS on duplicate orderBy values: RANGE pools peers.
#   Where duplicates are possible, decide whether peer-pooling is
#   harmless (flag 0 here) or corrupting, and pin ROWS if in doubt.
# - session_window's boundary (<, end = last + gap) vs a spec's
#   boundary (<=, end = last event): built-ins carry their OWN
#   contract; match it to the written spec before reaching for them.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
# - Frame set on a ranking/positional function is a no-op smell: if you
  # see rowsBetween/rangeBetween attached to row_number/rank/lag/lead,
  # the author likely misunderstands frames — those functions ignore it.
  # Frames matter only for sum/count/avg/max/min/collect_list windows.
