"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 7 — Struct columns (dot-path access, struct(), nested field
        extraction, struct(array)/array(struct) shapes)  (Medium)

PROBLEM
-------
You are given a table of user events. Each row carries a nested
`device` struct describing the device that produced the event, and a
top-level event type. The `device` struct has fields `os` (string) and
`version` (string). Some rows have a NULL `device` struct entirely
(the event came from an unknown/server-side source), which is DISTINCT
from a device whose `os` sub-field happens to be NULL.

Produce, per user, a compact device-profile report:
  - total_events        : total number of event rows for the user
  - mobile_events       : number of events whose device.os is one of
                          {'iOS', 'Android'}
  - distinct_os         : number of DISTINCT non-null device.os values
                          the user was seen on
  - primary_os          : the device.os the user has the MOST events on;
                          break ties by os string ascending. Rows with a
                          NULL device struct or NULL os do NOT count
                          toward primary_os. If the user has NO usable
                          os at all, primary_os is the string 'UNKNOWN'.

INPUT SCHEMA
------------
events(
    user_id: int,
    event_type: string,
    device: struct<os: string, version: string>   -- the struct itself may be NULL
)

EXPECTED OUTPUT
---------------
Columns, in THIS exact order:
    user_id, total_events, mobile_events, distinct_os, primary_os
One row per user_id. Order-insensitive (the harness sorts).
total_events / mobile_events / distinct_os are plain ints;
primary_os is a string ('UNKNOWN' when no usable os).

EXAMPLE
-------
Input (user 1):
    (1, 'click',  {os:'iOS',     version:'17'})
    (1, 'click',  {os:'iOS',     version:'16'})
    (1, 'view',   {os:'Android', version:'14'})
    (1, 'view',   NULL)                          -- whole struct NULL
Output for user 1:
    total_events=4, mobile_events=3, distinct_os=2, primary_os='iOS'

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

        Hint: access nested fields with df["device"]["os"] or
        F.col("device.os"). Think about what device.os returns when the
        whole struct is NULL vs when only os is NULL — and whether your
        primary_os logic distinguishes them correctly. A groupBy per
        (user, os) to rank, plus a separate per-user aggregate, then
        join — or a window over the per-os counts.
        """
        # TODO: implement
        mobile_os = F.array(F.lit("iOS"), F.lit('Android'))
        cnt_df = (
            df.groupby('user_id')
            .agg(
                F.count(F.lit(1)).alias("total_events"),
                F.sum(F.when(F.array_contains(mobile_os, F.col('device.os')), F.lit(1))
                      .otherwise(F.lit(0)))
                      .alias("mobile_events"),
                F.countDistinct(F.col('device.os')).alias("distinct_os")
            )
            .select(
                'user_id',
                'total_events',
                'mobile_events',
                'distinct_os'
            )
        )
        window_distinct_os_spec = Window.partitionBy('user_id').orderBy(F.col('os_cnt').desc())

        primary_os_df = (
            df.groupBy(
                'user_id',
                F.col('device.os').alias('os'),
            )
            .agg(F.count(F.lit(1)).alias('os_cnt'))
            .withColumn('rank', F.row_number().over(window_distinct_os_spec))
            .filter(F.col('rank') == 1)
            .drop('rank')
            .select(
                'user_id',
                F.col('os').alias('primary_os')
            )
        )
        

        res_df =  (
            cnt_df.join(
                primary_os_df,
                on='user_id',
                how='left')
            .withColumn(
                'primary_os',
                F.coalesce(F.col('primary_os'), F.lit("UNKNOWN"))
            )
        )
        return res_df

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: dot access device.os works in SQL too. COUNT(device.os)
        skips NULL; for primary_os a per-(user,os) count ranked with a
        window, then pick rank 1, then LEFT JOIN back so users with no
        usable os still get a row (COALESCE to 'UNKNOWN').
        """
        df.createOrReplaceTempView("events")
        MOBILES = ("iOS", "Android")
        sql = f"""
            -- write your SQL here
            WITH cnt_cte AS (
                SELECT 
                    user_id,
                    COUNT(*) AS total_events,
                    SUM(CASE WHEN device.os IN ('iOS', 'Android') THEN 1 ELSE 0 END) AS mobile_events,
                    COUNT(DISTINCT device.os) AS distinct_os
                FROM 
                    events 
                GROUP BY 
                    user_id
            ),

            rk_primary_os AS (
                SELECT 
                    user_id,
                    os AS primary_os
                FROM
                (SELECT 
                    user_id,
                    os,
                    ROW_NUMBER()OVER(partition by user_id order by cnt_os DESC) AS rk_os
                FROM 
                (SELECT 
                    user_id,
                    device.os AS os,
                    COUNT(device.os) AS cnt_os
                FROM 
                    events
                GROUP BY
                    user_id,
                    device.os) tb) tb1
                WHERE 
                    rk_os = 1
            )
            
            SELECT 
                c.user_id,
                total_events,
                mobile_events,
                distinct_os,
                COALESCE(primary_os, 'UNKNOWN') AS primary_os
            FROM 
                cnt_cte c LEFT JOIN rk_primary_os r ON c.user_id = r.user_id
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
    spark = SparkSession.builder.appName("daily-practice").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = "user_id int, event_type string, device struct<os: string, version: string>"

    data = [
        # user 1: iOS x2, Android x1, one NULL struct -> total 4, mobile 3, distinct 2, primary iOS
        (1, "click", ("iOS", "17")),
        (1, "click", ("iOS", "16")),
        (1, "view",  ("Android", "14")),
        (1, "view",  None),
        # user 2: tie between iOS and Android (1 each) -> primary = 'Android' (asc)
        (2, "click", ("Android", "14")),
        (2, "view",  ("iOS", "17")),
        # user 3: desktop only (macOS), plus a struct with NULL os sub-field
        #   total 3, mobile 0, distinct 1 (macOS; NULL os not counted), primary macOS
        (3, "click", ("macOS", "15")),
        (3, "click", ("macOS", "15")),
        (3, "view",  (None, "0")),          # struct present, os sub-field NULL
        # user 4: every event has a NULL struct OR NULL os -> no usable os
        #   total 3, mobile 0, distinct 0, primary 'UNKNOWN'
        (4, "click", None),
        (4, "view",  (None, "9")),
        (4, "view",  None),
        # user 5: single mobile event
        (5, "click", ("iOS", "18")),
    ]
    df = spark.createDataFrame(data, schema=schema)

    expected = [
        (1, 4, 3, 2, "iOS"),
        (2, 2, 2, 2, "Android"),
        (3, 3, 0, 1, "macOS"),
        (4, 3, 0, 0, "UNKNOWN"),
        (5, 1, 1, 1, "iOS"),
    ]

    s = Solution()
    check(s.solve_dsl(df), expected, "DSL")
    # s.solve_dsl(df).explain()
    check(s.solve_sql(spark, df), expected, "SQL")

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    def ai_solve_dsl(df: DataFrame) -> DataFrame:
    # Accessing device.os on a NULL struct yields NULL, so both "NULL struct"
    # and "NULL os sub-field" collapse into the same NULL check — which is
    # exactly the semantics the problem wants.
        base = df.groupBy("user_id").agg(
            F.count(F.lit(1)).alias("total_events"),
            F.count(
                F.when(F.col("device.os").isin("iOS", "Android"), F.lit(1))
            ).alias("mobile_events"),
            F.countDistinct(F.col("device.os")).alias("distinct_os"),  # ignores NULLs
        )

        # Per-(user, os) counts over usable os only.
        os_counts = (
            df.filter(F.col("device.os").isNotNull())
            .groupBy("user_id", F.col("device.os").alias("os"))
            .agg(F.count(F.lit(1)).alias("cnt"))
        )

        # min(struct(-cnt, os)) = highest count, ties broken by ascending os.
        prim = (
            os_counts.groupBy("user_id")
                    .agg(
                        F.min(
                            F.struct(
                                (-F.col("cnt")).alias("neg_cnt"),
                                F.col("os").alias("os"),
                            )
                        ).alias("best")
                    )
                    .select("user_id", F.col("best.os").alias("primary_os"))
        )

        return (
            base.join(prim, on="user_id", how="left")
                .select(
                    "user_id",
                    "total_events",
                    "mobile_events",
                    "distinct_os",
                    F.coalesce(F.col("primary_os"), F.lit("UNKNOWN")).alias("primary_os"),
                )
        )


    def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView("events")
        return spark.sql("""
            WITH base AS (
                SELECT
                    user_id,
                    COUNT(*) AS total_events,
                    COUNT(CASE WHEN device.os IN ('iOS', 'Android') THEN 1 END) AS mobile_events,
                    COUNT(DISTINCT device.os) AS distinct_os
                FROM events
                GROUP BY user_id
            ),
            os_counts AS (
                SELECT user_id, device.os AS os, COUNT(*) AS cnt
                FROM events
                WHERE device.os IS NOT NULL
                GROUP BY user_id, device.os
            ),
            prim AS (
                SELECT user_id, os AS primary_os
                FROM (
                    SELECT
                        user_id,
                        os,
                        ROW_NUMBER() OVER (
                            PARTITION BY user_id
                            ORDER BY cnt DESC, os ASC
                        ) AS rn
                    FROM os_counts
                )
                WHERE rn = 1
            )
            SELECT
                b.user_id,
                b.total_events,
                b.mobile_events,
                b.distinct_os,
                COALESCE(p.primary_os, 'UNKNOWN') AS primary_os
            FROM base b
            LEFT JOIN prim p ON b.user_id = p.user_id
        """)

    check(ai_solve_dsl(df), expected, "AI-DSL (post-review verification)")
    check(ai_solve_sql(spark, df), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review (NEW in v2)
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
#
# def ai_solve_dsl(df: DataFrame) -> DataFrame:
#     # Accessing device.os on a NULL struct yields NULL, so both "NULL struct"
#     # and "NULL os sub-field" collapse into the same NULL check — which is
#     # exactly the semantics the problem wants.
#     base = df.groupBy("user_id").agg(
#         F.count(F.lit(1)).alias("total_events"),
#         F.count(
#             F.when(F.col("device.os").isin("iOS", "Android"), F.lit(1))
#         ).alias("mobile_events"),
#         F.countDistinct(F.col("device.os")).alias("distinct_os"),  # ignores NULLs
#     )

#     # Per-(user, os) counts over usable os only.
#     os_counts = (
#         df.filter(F.col("device.os").isNotNull())
#           .groupBy("user_id", F.col("device.os").alias("os"))
#           .agg(F.count(F.lit(1)).alias("cnt"))
#     )

#     # min(struct(-cnt, os)) = highest count, ties broken by ascending os.
#     prim = (
#         os_counts.groupBy("user_id")
#                  .agg(
#                      F.min(
#                          F.struct(
#                              (-F.col("cnt")).alias("neg_cnt"),
#                              F.col("os").alias("os"),
#                          )
#                      ).alias("best")
#                  )
#                  .select("user_id", F.col("best.os").alias("primary_os"))
#     )

#     return (
#         base.join(prim, on="user_id", how="left")
#             .select(
#                 "user_id",
#                 "total_events",
#                 "mobile_events",
#                 "distinct_os",
#                 F.coalesce(F.col("primary_os"), F.lit("UNKNOWN")).alias("primary_os"),
#             )
#     )


# def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
#     df.createOrReplaceTempView("events")
#     return spark.sql("""
#         WITH base AS (
#             SELECT
#                 user_id,
#                 COUNT(*) AS total_events,
#                 COUNT(CASE WHEN device.os IN ('iOS', 'Android') THEN 1 END) AS mobile_events,
#                 COUNT(DISTINCT device.os) AS distinct_os
#             FROM events
#             GROUP BY user_id
#         ),
#         os_counts AS (
#             SELECT user_id, device.os AS os, COUNT(*) AS cnt
#             FROM events
#             WHERE device.os IS NOT NULL
#             GROUP BY user_id, device.os
#         ),
#         prim AS (
#             SELECT user_id, os AS primary_os
#             FROM (
#                 SELECT
#                     user_id,
#                     os,
#                     ROW_NUMBER() OVER (
#                         PARTITION BY user_id
#                         ORDER BY cnt DESC, os ASC
#                     ) AS rn
#                 FROM os_counts
#             )
#             WHERE rn = 1
#         )
#         SELECT
#             b.user_id,
#             b.total_events,
#             b.mobile_events,
#             b.distinct_os,
#             COALESCE(p.primary_os, 'UNKNOWN') AS primary_os
#         FROM base b
#         LEFT JOIN prim p ON b.user_id = p.user_id
#     """)
#
# REVIEW_NOTES (fill in BEFORE running):
# ----------------------------------------------------------------------
# [pass] Correctness — edge cases: NULL struct vs NULL os sub-field treated
#     differently? tie in primary_os broken by os asc? user with zero
#     usable os still emitted (LEFT JOIN, not inner)? distinct_os counts
#     non-null only?
#     notes:
# [pass] API usage — nested access via device.os / col("device.os") /
#     df["device"]["os"] correct? struct field name typos silently
#     resolving? SQL dot-path quoting?
#     notes:
# [pass] Performance — how many shuffles? per-(user,os) count + per-user
#     agg + join = separate Exchanges; could a single window pass do it?
#     window without partitionBy anywhere?
#     notes: AI used MIN to get the largest cnt of primary os, which will reduce one shuffle from window funcs, so it should performance better than window I suppose
# [pass] Robustness — hardcoded os set? assumption that struct is never
#     NULL? behavior if version is used where os was meant?
#     notes:
# [pass] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS / FAIL — because: it will pass 
# ACTUAL RESULT (after Stage 4 run): all passed
# GAP ANALYSIS: did the run reveal anything your reading missed?: use MIN to replace window func to reduce shuffle, I am not sure if this logic is correct


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# def ref_solve_dsl(df: DataFrame) -> DataFrame:
#     mobile = F.array(F.lit("iOS"), F.lit("Android"))
#     os_col = F.col("device.os")   # NULL if struct is NULL OR os is NULL
#
#     # per-user base aggregates
#     base = (df.groupBy("user_id")
#               .agg(F.count(F.lit(1)).alias("total_events"),
#                    F.count(F.when(F.array_contains(mobile, os_col), F.lit(1)))
#                       .alias("mobile_events"),
#                    F.countDistinct(os_col).alias("distinct_os")))
#     # countDistinct ignores NULL -> NULL struct and NULL os both excluded.
#
#     # per-(user, os) counts, only usable os rows
#     per_os = (df.where(os_col.isNotNull())
#                 .groupBy("user_id", os_col.alias("os"))
#                 .agg(F.count(F.lit(1)).alias("os_cnt")))
#     w = Window.partitionBy("user_id").orderBy(F.col("os_cnt").desc(),
#                                                F.col("os").asc())
#     primary = (per_os.withColumn("rn", F.row_number().over(w))
#                      .where(F.col("rn") == 1)
#                      .select("user_id", F.col("os").alias("primary_os")))
#
#     return (base.join(primary, "user_id", "left")
#                 .withColumn("primary_os",
#                             F.coalesce(F.col("primary_os"), F.lit("UNKNOWN")))
#                 .select("user_id", "total_events", "mobile_events",
#                         "distinct_os", "primary_os"))
#
# def ref_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
#     df.createOrReplaceTempView("events")
#     return spark.sql("""
#         WITH base AS (
#             SELECT user_id,
#                    COUNT(*)                                       AS total_events,
#                    COUNT(CASE WHEN device.os IN ('iOS','Android')
#                               THEN 1 END)                         AS mobile_events,
#                    COUNT(DISTINCT device.os)                      AS distinct_os
#             FROM events
#             GROUP BY user_id
#         ),
#         per_os AS (
#             SELECT user_id, device.os AS os, COUNT(*) AS os_cnt
#             FROM events
#             WHERE device.os IS NOT NULL
#             GROUP BY user_id, device.os
#         ),
#         ranked AS (
#             SELECT user_id, os,
#                    ROW_NUMBER() OVER (PARTITION BY user_id
#                                       ORDER BY os_cnt DESC, os ASC) AS rn
#             FROM per_os
#         )
#         SELECT b.user_id, b.total_events, b.mobile_events, b.distinct_os,
#                COALESCE(r.primary_os, 'UNKNOWN') AS primary_os
#         FROM base b
#         LEFT JOIN (SELECT user_id, os AS primary_os FROM ranked WHERE rn = 1) r
#           ON b.user_id = r.user_id
#     """)


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - Nested field access: device.os / F.col("device.os") /
#   df["device"]["os"] are equivalent. When the PARENT struct is NULL,
#   the sub-field access returns NULL (no error) — same NULL as an
#   explicitly-NULL os sub-field. The two are INDISTINGUISHABLE once you
#   project device.os, which is exactly why the aggregate-NULL rule
#   makes COUNT(device.os) / countDistinct(device.os) "just work" for
#   both mobile_events and distinct_os.
# - Struct is a fixed-schema record (dot path, positional), unlike a map
#   (dynamic keys, element_at). No explode on a plain struct — you
#   access fields by name; explode is for the array(struct<...>) shape.
# - primary_os is the classic "argmax per group" pattern: per-(key,dim)
#   count -> row_number over count desc + tiebreaker -> pick rn=1 ->
#   LEFT JOIN back so groups with no qualifying dim still emit a row.
#   Inner join here would silently drop user 4 (all-NULL os) entirely.
# - COALESCE to 'UNKNOWN' is the struct analog of the LEFT-join +
#   COALESCE enrichment idiom from Day 5.
#
# Review takeaways (NEW in v2):
# - What did the AI get wrong (or suspiciously right)?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to my checklist next time?
