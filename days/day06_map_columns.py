"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 6 — Map Columns (map access, map_keys, explode on maps)  (Medium)

PROBLEM
-------
An analytics system stores raw events with a free-form property bag:
each event carries a map<string,string> of optional properties (keys
like "device", "os", "browser" may or may not be present).

For EACH user, produce a usage report with:
  1. total_events   : total number of events by that user
  2. mobile_events  : number of events where the property "device"
                      equals 'mobile'
  3. distinct_keys  : the number of DISTINCT property keys that appear
                      across ALL of that user's events

Every user present in the events table must appear in the output,
even if none of their events carry any properties.

INPUT SCHEMA
------------
events(event_id: int, user_id: int, props: map<string,string>)

Notes:
- props may be NULL (event logged with no property bag at all).
- props may be an EMPTY map (property bag present but empty).

EXPECTED OUTPUT
---------------
Columns, in this exact order:
    user_id (int), total_events (long), mobile_events (long),
    distinct_keys (int)
One row per user. Output order does not matter (test is
order-insensitive).

EXAMPLE
-------
Input:
    event_id | user_id | props
    ---------+---------+--------------------------------
    101      | 1       | {device: mobile, os: ios}
    102      | 1       | {device: desktop}
    103      | 1       | {device: mobile, browser: chrome}
    104      | 2       | NULL
    105      | 2       | {os: android}
    106      | 2       | {}
    107      | 3       | NULL

Expected output:
    user_id | total_events | mobile_events | distinct_keys
    --------+--------------+---------------+--------------
    1       | 3            | 2             | 3     (device, os, browser)
    2       | 3            | 0             | 1     (os)
    3       | 1            | 0             | 0

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

        Hint: There are (at least) two routes — an explode-based route
        and a pure map/array-function route (map_keys + collect +
        flatten). Before choosing, ask yourself what each route does to
        the rows you start with.
        """
        # TODO: implement
        df_res = df\
        .select(
            'user_id',
            'event_id',
            F.explode_outer(F.col('props')).alias('devices', 'platform')
            )\
            .groupBy('user_id')\
            .agg(
                F.countDistinct(F.col('event_id')).alias('total_events'),
                F.sum(F.when(F.col('platform') == 'mobile', 1).otherwise(0)).alias('mobile_events'),
                F.array_size(F.collect_set(F.col('devices'))).alias('distinct_keys'),
            ).orderBy('user_id')

        return df_res

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: Accessing a map key in SQL is props['device']. Think about
        what that expression evaluates to when the key is absent — and
        when the whole map is NULL.
        """
        df.createOrReplaceTempView("events")

        sql = """
            -- write your SQL here
            WITH exploded AS (
            SELECT 
                user_id,
                event_id,
                device,
                platform
            FROM events
            LATERAL VIEW OUTER explode(props) tb AS device, platform 
            )
        
            SELECT 
                user_id,
                COUNT(DISTINCT event_id) AS total_events,
                SUM(CASE WHEN platform = 'mobile' THEN 1 ELSE 0 END) AS mobile_events,
                COUNT(DISTINCT device) AS distinct_keys
            FROM 
                exploded 
            GROUP BY 
                user_id;
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

    data = [
        (101, 1, {"device": "mobile", "os": "ios"}),
        (102, 1, {"device": "desktop"}),
        (103, 1, {"device": "mobile", "browser": "chrome"}),
        (104, 2, None),
        (105, 2, {"os": "android"}),
        (106, 2, {}),
        (107, 3, None),
    ]
    df = spark.createDataFrame(
        data, schema="event_id INT, user_id INT, props MAP<STRING,STRING>"
    )

    expected = [
        (1, 3, 2, 3),
        (2, 3, 0, 1),
        (3, 1, 0, 0),
    ]

    s = Solution()
    check(s.solve_dsl(df), expected, "DSL")
    check(s.solve_sql(spark, df), expected, "SQL")

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    def ai_solve_dsl(df: DataFrame) -> DataFrame:
        return (
            df.groupBy("user_id")
            .agg(
                F.count("*").alias("total_events"),
                F.sum(
                    F.when(F.col("props")["device"] == "mobile", 1).otherwise(0)
                ).cast("long").alias("mobile_events"),
                F.size(
                    F.array_distinct(F.flatten(F.collect_list(F.map_keys(F.col("props")))))
                ).alias("distinct_keys"),
            )
            .select("user_id", "total_events", "mobile_events", "distinct_keys")
        )
    
    def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView("events")
        return spark.sql("""
            SELECT
                user_id,
                COUNT(*) AS total_events,
                CAST(SUM(CASE WHEN props['device'] = 'mobile' THEN 1 ELSE 0 END) AS BIGINT) AS mobile_events,
                SIZE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(MAP_KEYS(props))))) AS distinct_keys
            FROM events
            GROUP BY user_id
        """)

    check(ai_solve_dsl(df), expected, "AI-DSL (post-review verification)")
    check(ai_solve_sql(spark, df), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review (NEW in v2)
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
# def ai_solve_dsl(df: DataFrame) -> DataFrame:
#     return (
#         df.groupBy("user_id")
#         .agg(
#             F.count("*").alias("total_events"),
#             F.sum(
#                 F.when(F.col("props")["device"] == "mobile", 1).otherwise(0)
#             ).cast("long").alias("mobile_events"),
#             F.size(
#                 F.array_distinct(F.flatten(F.collect_list(F.map_keys(F.col("props")))))
#             ).alias("distinct_keys"),
#         )
#         .select("user_id", "total_events", "mobile_events", "distinct_keys")
#     )


# def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
#     df.createOrReplaceTempView("events")
#     return spark.sql("""
#         SELECT
#             user_id,
#             COUNT(*) AS total_events,
#             CAST(SUM(CASE WHEN props['device'] = 'mobile' THEN 1 ELSE 0 END) AS BIGINT) AS mobile_events,
#             SIZE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(MAP_KEYS(props))))) AS distinct_keys
#         FROM events
#         GROUP BY user_id
#     """)

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
# [pass] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes:
# [pass] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS — because:
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# Route A — no explosion: stay at one-row-per-event grain and do the
# key math with map/array functions. Single groupBy, single shuffle.
#
# def ref_solve_dsl(df):
#     return (
#         df.groupBy("user_id")
#         .agg(
#             F.count(F.lit(1)).alias("total_events"),
#             F.count(
#                 F.when(F.col("props")["device"] == "mobile", 1)
#             ).alias("mobile_events"),
#             F.size(
#                 F.array_distinct(
#                     F.flatten(F.collect_list(F.map_keys(F.col("props"))))
#                 )
#             ).alias("distinct_keys"),
#         )
#         .select("user_id", "total_events", "mobile_events", "distinct_keys")
#     )
#
#   Why this survives the dirty rows:
#   - props['device'] on a NULL map or a missing key evaluates to NULL;
#     the when() then yields NULL and count() skips it (aggregate-NULL
#     rule) — mobile_events is correct with zero defensive code.
#   - map_keys(NULL) is NULL (not an empty array!), but collect_list
#     IGNORES NULL inputs, so an all-NULL user collapses to an empty
#     list; flatten([]) = [] and size([]) = 0. The empty map contributes
#     an empty key array, which flatten absorbs harmlessly.
#
# def ref_solve_sql(spark, df):
#     df.createOrReplaceTempView("events")
#     return spark.sql("""
#         SELECT user_id,
#                COUNT(*)                                        AS total_events,
#                COUNT(CASE WHEN props['device'] = 'mobile'
#                           THEN 1 END)                          AS mobile_events,
#                SIZE(ARRAY_DISTINCT(
#                     FLATTEN(COLLECT_LIST(MAP_KEYS(props)))))   AS distinct_keys
#         FROM events
#         GROUP BY user_id
#     """)
#
# Route B — explode-based (meaningfully different, shown for contrast):
#
#     exploded = df.select(
#         "event_id", "user_id",
#         F.explode_outer(F.map_keys("props")).alias("k"),
#     )
#     result = exploded.groupBy("user_id").agg(
#         F.countDistinct("event_id").alias("total_events"),
#         F.countDistinct("k").alias("distinct_keys"),
#     )
#     # ...then mobile_events must be computed on the ORIGINAL df and
#     # joined back, because the exploded grain has lost the map values.
#
#   Notes on Route B:
#   - It MUST be explode_outer: map_keys(NULL) is NULL and explode()
#     would drop those rows entirely.
#   - total_events must become countDistinct(event_id) because one
#     event now spans multiple rows (one per key).
#   - countDistinct ignores NULL, so the outer-explode NULL key does
#     not pollute distinct_keys.
#   - Net cost: an extra aggregate + a join back. Route A is the
#     cleaner production answer; Route B is worth knowing because
#     explode on maps (as F.explode(map_col) -> key, value TWO columns)
#     is the standard tool when you need per-key VALUES, not just keys.


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - Map access: props['k'], F.col("props")["k"], .getItem("k"), and
#   element_at(props, 'k') all return NULL for a missing key AND for a
#   NULL map — no error is thrown. Safe for filtering, silent for
#   debugging.
# - map_keys(NULL) / map_values(NULL) return NULL, not an empty array.
#   The NULL-map vs empty-map distinction ripples through everything
#   downstream.
# - explode() on a map emits TWO columns (key, value); explode on
#   map_keys(...) emits one. Both drop NULL/empty inputs; the _outer
#   variants keep the row with NULLs — same rule as arrays (Day 4).
# - collect_list ignores NULL elements (aggregate-NULL rule), which is
#   exactly what bridges "some users have only NULL maps" into "empty
#   array, size 0" without any coalesce.
# - Grain discipline: exploding changes the row grain; every COUNT
#   downstream must be re-examined (count -> countDistinct(id)).
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
# - AI chose the no-explode route (Route A): fewer steps, no grain
#   change, no countDistinct. Nothing wrong found; verdict PASS
#   confirmed by the run.
# - My own solution exploded first — correct but one extra step and a
#   pricier aggregation; the AI's route was the cleaner production
#   answer this time. "Suspiciously right" check: its count(when(...))
#   with no otherwise() relies on the aggregate-NULL rule, which I
#   verified rather than assumed.
