"""
=====================================================================
PySpark Daily Practice — ETL Scenario
=====================================================================
Day 27 — fct_app_session  (Medium-Hard)

ETL layer : L2 detail modeling
Domain    : mobile app clickstream

BUSINESS CONTEXT
----------------
The product analytics team builds its "sessions per day" and "average
session length" dashboards, and the retention model, on top of a session
fact table. This job turns each landing batch of raw app events into that
table: one row per session, enriched with the device platform.

INPUT TABLES
------------
raw_app_event(event_id: string, device_id: string, event_ts: string,
              event_name: string)
    -- append-only event feed from the mobile SDK collector.
       event_ts is UTC, format 'yyyy-MM-dd HH:mm:ss', never NULL.
       Upstream guarantees: event_id is unique, every event_name exists
       in event_catalog.

event_catalog(event_name: string, is_engagement: boolean, owner_team: string)
    -- hand-maintained config. is_engagement marks events that a user
       actively triggered. owner_team is operational metadata.

device_registry(device_id: string, platform: string, push_token: string)
    -- daily snapshot dimension, one row per registered device.
       push_token is operational metadata.

raw_app_event:

    +----------+-----------+---------------------+---------------+
    | event_id | device_id | event_ts            | event_name    |
    +----------+-----------+---------------------+---------------+
    | E01      | D1        | 2026-10-07 23:48:00 | app_open      |
    | E02      | D1        | 2026-10-07 23:51:00 | push_received |
    | E03      | D1        | 2026-10-07 23:55:00 | screen_view   |
    | E04      | D1        | 2026-10-08 00:09:00 | screen_view   |
    | E05      | D2        | 2026-10-07 23:20:00 | app_open      |
    | E06      | D2        | 2026-10-08 00:05:00 | app_open      |
    | E07      | D2        | 2026-10-08 00:12:00 | screen_view   |
    | E08      | D3        | 2026-10-08 08:00:00 | app_open      |
    +----------+-----------+---------------------+---------------+

    notes: E02 is a push-notification receipt logged by the SDK, not a
           user action. D3 has no row in device_registry.

event_catalog:

    +----------------+---------------+------------+
    | event_name     | is_engagement | owner_team |
    +----------------+---------------+------------+
    | app_open       | true          | growth     |
    | screen_view    | true          | content    |
    | push_received  | false         | messaging  |
    | app_background | false         | platform   |
    +----------------+---------------+------------+

device_registry:

    +-----------+----------+------------+
    | device_id | platform | push_token |
    +-----------+----------+------------+
    | D1        | iOS      | apns-7f3a  |
    | D2        | Android  | fcm-91c2   |
    | D4        | iOS      | apns-02bd  |
    +-----------+----------+------------+

    notes: D4 is registered but has no events in this batch.

OUTPUT CONTRACT
---------------
Table    : fct_app_session
Grain    : one row per session of one device
Columns  : session_id: string, device_id: string, platform: string,
           session_date: string, session_start_ts: string,
           session_end_ts: string, n_events: bigint, duration_sec: bigint
           (this exact order)
Ordering : irrelevant — check() sorts both sides

Session definition:
  Only engagement events (event_catalog.is_engagement = true) take part.
  Non-engagement events are ignored entirely: they never start, extend,
  or count toward a session.
  A session is a maximal run of one device's engagement events, ordered
  by event_ts, in which each event follows the previous one by at most
  30 minutes. A gap of MORE than 30 minutes starts a new session.

Column definitions:
  session_id       : device_id || '-' || session_start_ts formatted as
                     'yyyyMMddHHmmss'
  platform         : device_registry.platform; 'UNKNOWN' when the device
                     has no registry row
  session_date     : UTC calendar date of session_start_ts, 'yyyy-MM-dd'
  session_start_ts : event_ts of the session's first engagement event,
                     'yyyy-MM-dd HH:mm:ss'
  session_end_ts   : event_ts of the session's last engagement event,
                     'yyyy-MM-dd HH:mm:ss'
  n_events         : number of engagement events in the session
  duration_sec     : session_end_ts - session_start_ts in seconds
                     (0 for a single-event session)

Expected:

    +-------------------+-----------+----------+--------------+---------------------+---------------------+----------+--------------+
    | session_id        | device_id | platform | session_date | session_start_ts    | session_end_ts      | n_events | duration_sec |
    +-------------------+-----------+----------+--------------+---------------------+---------------------+----------+--------------+
    | D1-20261007234800 | D1        | iOS      | 2026-10-07   | 2026-10-07 23:48:00 | 2026-10-08 00:09:00 | 3        | 1260         |
    | D2-20261007232000 | D2        | Android  | 2026-10-07   | 2026-10-07 23:20:00 | 2026-10-07 23:20:00 | 1        | 0            |
    | D2-20261008000500 | D2        | Android  | 2026-10-08   | 2026-10-08 00:05:00 | 2026-10-08 00:12:00 | 2        | 420          |
    | D3-20261008080000 | D3        | UNKNOWN  | 2026-10-08   | 2026-10-08 08:00:00 | 2026-10-08 08:00:00 | 1        | 0            |
    +-------------------+-----------+----------+--------------+---------------------+---------------------+----------+--------------+

PRODUCTION CONSTRAINTS
----------------------
P1. Re-running this job on the same three inputs must produce
    byte-identical output. session_id is a pure function of the session's
    own events — never of run time, row order, or a generated sequence.
P2. Before returning, the job must assert its own output: session_id is
    unique; sum(n_events) equals the number of engagement events in
    raw_app_event; and session_id / device_id / platform / session_date
    are never NULL. A silent bad publish is worse than a failed job.
P3. Read only the columns this job needs from the two config tables.
    owner_team and push_token are operational metadata and must not enter
    the pipeline.

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
refs/day27_app_session_ref.md. Do not open it before Stage 5.
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
def build_fct_app_session_dsl(
    raw_app_event: DataFrame,
    event_catalog: DataFrame,
    device_registry: DataFrame,
) -> DataFrame:
    """Build fct_app_session: one row per device session, with platform.

    Hint: filter -> enrich -> sessionize -> roll up -> assert, then return.
    """
    # TODO: implement
    events_engagements = event_catalog.select('event_name', 'is_engagement')
    devices = device_registry.select('device_id', 'platform')
    
    true_events = (raw_app_event.join(
            F.broadcast(events_engagements),
            on='event_name',
            how='left_outer'
    ).filter(F.col('is_engagement') == 'true')
    )

    window_spec = Window.partitionBy('device_id').orderBy(F.col('event_ts'), F.col('event_id'))

    session_diff = (
        true_events.withColumn(
            '_prev_ts',
            F.lag(F.col('event_ts'), 1).over(window_spec)
        )
        .withColumn(
            '_time_diff',
            F.timestamp_diff('SECOND', F.col('_prev_ts'), F.col('event_ts'))
        )
        .withColumn(
            '_is_new_session',
            F.when((F.col('_time_diff') * 1.0 / 60) > 30.0, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            '_sessionize',
            F.sum(F.col('_is_new_session')).over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )
    )

    agg_events = (
        session_diff.groupBy('device_id','_sessionize').agg(
            F.min('event_ts').alias('session_start_ts'),
            F.max('event_ts').alias('session_end_ts'),
            F.count(F.lit(1)).cast('int').alias('n_events'),
        ).withColumn(
            'duration_sec',
            F.timestamp_diff('second', F.col('session_start_ts'), F.col('session_end_ts'))
        ).join(
            F.broadcast(devices),
            'device_id',
            'left_outer'
        )
        .select(
            F.concat_ws(
                '-', 
                F.col('device_id'), 
                F.date_format(F.col('session_start_ts'), 'yyyyMMddHHmmss')
                ).alias('session_id'),
            'device_id',
            F.coalesce('platform', F.lit('UNKNOWN')).alias('platform'),
            F.date_format(F.col('session_start_ts'), 'yyyy-MM-dd').alias('session_date'),
            F.col('session_start_ts'),
            F.col('session_end_ts'),
            F.coalesce(F.col('n_events'), F.lit(0)).alias('n_events'),
            F.coalesce(F.col('duration_sec'), F.lit(0)).alias('duration_sec')
        )
    )

    # RODUCTION CONSTRAINTS
    # P1. Re-running this job on the same three inputs must produce
    #     byte-identical output. session_id is a pure function of the session's
    #     own events — never of run time, row order, or a generated sequence.
    

    # P2. Before returning, the job must assert its own output: session_id is
    #     unique; sum(n_events) equals the number of engagement events in
    #     raw_app_event; and session_id / device_id / platform / session_date
    #     are never NULL. A silent bad publish is worse than a failed job.

    unique_cnt = agg_events.groupBy('session_id').agg(
        F.count(F.lit(1)).alias('uni_cnt')
    ).first()['uni_cnt']
    assert unique_cnt == 1, f"the sessions_id is not unique, cnt:{unique_cnt}"

    raw_n_events = true_events.agg(F.count('event_id').alias("raw_events_cnt")).first()['raw_events_cnt']
    n_events = agg_events.agg(F.sum(F.col('n_events')).alias('total_n_events')).first()['total_n_events']
    assert raw_n_events == n_events, f'numbers of raw and result events are different, raw:{raw_n_events}, result:{n_events}'

    null_cnt = agg_events.filter(F.col('session_id').isNull() | F.col('device_id').isNull() | F.col('platform').isNull() | F.col('session_date').isNull()).count()
    assert null_cnt == 0, f'null_cnt of session_id / device_id / platform / session_date is not 0, null_cnt: {null_cnt}'
    # P3. Read only the columns this job needs from the two config tables.
    #     owner_team and push_token are operational metadata and must not enter
    #     the pipeline.

    return agg_events


# ------------------------------ 1b. Spark SQL ------------------------
def build_fct_app_session_sql(
    spark: SparkSession,
    raw_app_event: DataFrame,
    event_catalog: DataFrame,
    device_registry: DataFrame,
) -> DataFrame:
    """Same job, Spark SQL.

    Hint: the P2 assertions may run as separate queries after the main one.
    """
    raw_app_event.createOrReplaceTempView("raw_app_event")
    event_catalog.createOrReplaceTempView("event_catalog")
    device_registry.createOrReplaceTempView("device_registry")

    sql = """
        -- write your SQL here
        WITH events_engagements AS (
            SELECT 
                event_name,
                is_engagement
            FROM 
                event_catalog
        ),

        devices AS (
            SELECT 
                device_id,
                platform
            FROM 
                device_registry
        ),

        true_events AS (
            SELECT 
                /*+ BROADCAST(ee) */
                event_id,
                device_id,
                event_ts,
                r.event_name,
                is_engagement
            FROM 
                raw_app_event r LEFT JOIN events_engagements ee ON r.event_name = ee.event_name
            WHERE 
                is_engagement = true
        ),

        prev_ts AS (
            SELECT 
                event_id,
                device_id,
                event_ts,
                event_name,
                is_engagement,
                LAG(event_ts)OVER(partition by device_id order by event_ts, event_id) AS _prev_ts
            FROM 
                true_events
        ),

        time_diff AS (
            SELECT 
                event_id,
                device_id,
                event_ts,
                event_name,
                is_engagement,
                TIMESTAMPDIFF(second, _prev_ts, event_ts) AS _time_diff
            FROM 
                prev_ts
        ),

        new_session_flag AS (
            SELECT 
                event_id,
                device_id,
                event_ts,
                event_name,
                is_engagement,
                CASE 
                    WHEN (_time_diff * 1.0 / 60) > 30 THEN 1 ELSE 0 END AS _is_new_session
            FROM 
                time_diff
        ),

        sessionize AS (
            SELECT 
                event_id,
                device_id,
                event_ts,
                event_name,
                is_engagement,
                SUM(_is_new_session)OVER(partition by device_id order by event_ts, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _sessionize
            FROM 
                new_session_flag
        ),

        agg AS (
            SELECT 
                device_id,
                MIN(event_ts) AS session_start_ts,
                MAX(event_ts) AS session_end_ts,
                COUNT(*) AS n_events
            FROM 
                sessionize
            GROUP BY 
                device_id, _sessionize
        ),

        duration AS (
            SELECT 
                *,
                TIMESTAMPDIFF(SECOND, session_start_ts, session_end_ts) AS duration_sec
            FROM 
                agg
        ),

        get_platform AS (
            SELECT 
                /*+ BROADCAST(dr) */
                CONCAT_WS('-', d.device_id, DATE_FORMAT(session_start_ts, 'yyyyMMddHHmmss')) AS session_id,
                d.device_id,
                COALESCE(dr.platform, 'UNKNOWN') AS platform,
                DATE_FORMAT(session_start_ts, 'yyyy-MM-dd') AS session_date,
                session_start_ts, 
                session_end_ts,
                CAST(COALESCE(n_events, 0) AS INTEGER) AS n_events,
                CAST(COALESCE(duration_sec, 0) AS BIGINT) AS duration_sec
            FROM 
                duration d LEFT JOIN device_registry dr ON d.device_id = dr.device_id
        )

        SELECT 
            *
        FROM 
            get_platform
    """
    output = spark.sql(sql)

    unique_cnt = output.groupBy('session_id').agg(
            F.count(F.lit(1)).alias('uni_cnt')
        ).first()['uni_cnt']
    assert unique_cnt == 1, f"the sessions_id is not unique, cnt:{unique_cnt}"

    raw_n_events = (
        raw_app_event.join(
            event_catalog, 
            'event_name', 
            'left_outer').filter(
                F.col('is_engagement')== 'true'
                ).agg(
                    F.count('event_id').alias("raw_events_cnt")
                ).first()['raw_events_cnt']
        )
    n_events = output.agg(F.sum(F.col('n_events')).alias('total_n_events')).first()['total_n_events']
    assert raw_n_events == n_events, f'numbers of raw and result events are different, raw:{raw_n_events}, result:{n_events}'

    null_cnt = output.filter(F.col('session_id').isNull() | F.col('device_id').isNull() | F.col('platform').isNull() | F.col('session_date').isNull()).count()
    assert null_cnt == 0, f'null_cnt of session_id / device_id / platform / session_date is not 0, null_cnt: {null_cnt}'


    return output


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
def ai_build_fct_app_session_dsl(
    raw_app_event: DataFrame,
    event_catalog: DataFrame,
    device_registry: DataFrame,
) -> DataFrame:
    engagement_names = (
        event_catalog
        .select("event_name", "is_engagement")
        .where(F.coalesce(F.col("is_engagement"), F.lit(False)))
        .select("event_name")
        .distinct()
    )

    platforms = device_registry.select("device_id", "platform")

    engagement_events = (
        raw_app_event
        .select("event_id", "device_id", "event_ts", "event_name")
        .join(engagement_names, on="event_name", how="left_semi")
        .select(
            "event_id",
            "device_id",
            F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss").alias("ts"),
        )
        .withColumn("epoch", F.unix_timestamp(F.col("ts")))
    )

    w = Window.partitionBy("device_id").orderBy("epoch", "event_id")
    w_running = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    prev_epoch = F.lag("epoch").over(w)

    sessions = (
        engagement_events
        .withColumn(
            "is_new",
            F.when(
                prev_epoch.isNull() | ((F.col("epoch") - prev_epoch) > F.lit(1800)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("session_seq", F.sum("is_new").over(w_running))
        .groupBy("device_id", "session_seq")
        .agg(
            F.min("ts").alias("start_ts"),
            F.max("ts").alias("end_ts"),
            F.count(F.lit(1)).alias("n_events"),
            (F.max("epoch") - F.min("epoch")).alias("duration_sec"),
        )
    )

    result = (
        sessions
        .join(platforms, on="device_id", how="left")
        .select(
            F.concat(
                F.col("device_id"),
                F.lit("-"),
                F.date_format(F.col("start_ts"), "yyyyMMddHHmmss"),
            ).alias("session_id"),
            F.col("device_id"),
            F.coalesce(F.col("platform"), F.lit("UNKNOWN")).alias("platform"),
            F.date_format(F.col("start_ts"), "yyyy-MM-dd").alias("session_date"),
            F.date_format(F.col("start_ts"), "yyyy-MM-dd HH:mm:ss").alias("session_start_ts"),
            F.date_format(F.col("end_ts"), "yyyy-MM-dd HH:mm:ss").alias("session_end_ts"),
            F.col("n_events").cast("bigint").alias("n_events"),
            F.col("duration_sec").cast("bigint").alias("duration_sec"),
        )
    )

    expected_events = engagement_events.count()

    stats = result.agg(
        F.count(F.lit(1)).alias("n_rows"),
        F.count_distinct(F.col("session_id")).alias("n_ids"),
        F.coalesce(F.sum("n_events"), F.lit(0)).alias("n_events"),
        F.coalesce(
            F.sum(
                F.when(
                    F.col("session_id").isNull()
                    | F.col("device_id").isNull()
                    | F.col("platform").isNull()
                    | F.col("session_date").isNull(),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ),
            F.lit(0),
        ).alias("n_null"),
    ).first()

    failures = []
    if stats["n_null"] != 0:
        failures.append(
            f"{stats['n_null']} rows with NULL session_id/device_id/platform/session_date"
        )
    if stats["n_ids"] != stats["n_rows"]:
        failures.append(
            f"session_id not unique: {stats['n_rows']} rows, {stats['n_ids']} distinct ids"
        )
    if stats["n_events"] != expected_events:
        failures.append(
            f"sum(n_events)={stats['n_events']} != engagement events={expected_events}"
        )
    if failures:
        raise RuntimeError("fct_app_session validation failed: " + "; ".join(failures))

    return result


def ai_build_fct_app_session_sql(
    spark: SparkSession,
    raw_app_event: DataFrame,
    event_catalog: DataFrame,
    device_registry: DataFrame,
) -> DataFrame:
    raw_app_event.createOrReplaceTempView("fas_raw_app_event")
    event_catalog.createOrReplaceTempView("fas_event_catalog")
    device_registry.createOrReplaceTempView("fas_device_registry")

    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW fas_engagement_event AS
        SELECT
            e.event_id,
            e.device_id,
            TO_TIMESTAMP(e.event_ts, 'yyyy-MM-dd HH:mm:ss') AS ts
        FROM (
            SELECT event_id, device_id, event_ts, event_name
            FROM fas_raw_app_event
        ) e
        LEFT SEMI JOIN (
            SELECT DISTINCT event_name
            FROM (
                SELECT event_name, is_engagement
                FROM fas_event_catalog
            )
            WHERE COALESCE(is_engagement, FALSE)
        ) c
            ON e.event_name = c.event_name
    """)

    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW fas_fct_app_session AS
        WITH ev AS (
            SELECT
                event_id,
                device_id,
                ts,
                UNIX_TIMESTAMP(ts) AS epoch
            FROM fas_engagement_event
        ),
        flagged AS (
            SELECT
                event_id,
                device_id,
                ts,
                epoch,
                CASE
                    WHEN LAG(epoch) OVER w IS NULL
                      OR epoch - LAG(epoch) OVER w > 1800
                    THEN 1 ELSE 0
                END AS is_new
            FROM ev
            WINDOW w AS (PARTITION BY device_id ORDER BY epoch, event_id)
        ),
        numbered AS (
            SELECT
                device_id,
                ts,
                epoch,
                SUM(is_new) OVER (
                    PARTITION BY device_id
                    ORDER BY epoch, event_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS session_seq
            FROM flagged
        ),
        sessions AS (
            SELECT
                device_id,
                MIN(ts)                 AS start_ts,
                MAX(ts)                 AS end_ts,
                COUNT(1)                AS n_events,
                MAX(epoch) - MIN(epoch) AS duration_sec
            FROM numbered
            GROUP BY device_id, session_seq
        ),
        platforms AS (
            SELECT device_id, platform
            FROM fas_device_registry
        )
        SELECT
            CONCAT(s.device_id, '-', DATE_FORMAT(s.start_ts, 'yyyyMMddHHmmss')) AS session_id,
            s.device_id                                                          AS device_id,
            COALESCE(p.platform, 'UNKNOWN')                                      AS platform,
            DATE_FORMAT(s.start_ts, 'yyyy-MM-dd')                                AS session_date,
            DATE_FORMAT(s.start_ts, 'yyyy-MM-dd HH:mm:ss')                       AS session_start_ts,
            DATE_FORMAT(s.end_ts, 'yyyy-MM-dd HH:mm:ss')                         AS session_end_ts,
            CAST(s.n_events AS BIGINT)                                           AS n_events,
            CAST(s.duration_sec AS BIGINT)                                       AS duration_sec
        FROM sessions s
        LEFT JOIN platforms p
            ON s.device_id = p.device_id
    """)

    result = spark.sql("""
        SELECT
            session_id,
            device_id,
            platform,
            session_date,
            session_start_ts,
            session_end_ts,
            n_events,
            duration_sec
        FROM fas_fct_app_session
    """)

    stats = spark.sql("""
        SELECT
            a.n_rows,
            a.n_ids,
            a.n_events,
            a.n_null,
            b.expected_events
        FROM (
            SELECT
                COUNT(1)                                AS n_rows,
                COUNT(DISTINCT session_id)              AS n_ids,
                COALESCE(SUM(n_events), 0)              AS n_events,
                COALESCE(SUM(
                    CASE
                        WHEN session_id IS NULL
                          OR device_id IS NULL
                          OR platform IS NULL
                          OR session_date IS NULL
                        THEN 1 ELSE 0
                    END
                ), 0)                                   AS n_null
            FROM fas_fct_app_session
        ) a
        CROSS JOIN (
            SELECT COUNT(1) AS expected_events
            FROM fas_engagement_event
        ) b
    """).first()

    failures = []
    if stats["n_null"] != 0:
        failures.append(
            f"{stats['n_null']} rows with NULL session_id/device_id/platform/session_date"
        )
    if stats["n_ids"] != stats["n_rows"]:
        failures.append(
            f"session_id not unique: {stats['n_rows']} rows, {stats['n_ids']} distinct ids"
        )
    if stats["n_events"] != stats["expected_events"]:
        failures.append(
            f"sum(n_events)={stats['n_events']} != engagement events={stats['expected_events']}"
        )
    if failures:
        raise RuntimeError("fct_app_session validation failed: " + "; ".join(failures))

    return result
# >>> PASTE END

# ---------------------------------------------------------------------
# REVIEW_NOTES — fill in BEFORE running anything.
# Severity order: runtime break > wrong results on dirty data
#               > production robustness > performance > portability > style
# ---------------------------------------------------------------------
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes: correct, nothing wrong
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes: correct 
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (cast, division, array index, element_at)?
#     notes: everything is ok
# [ ] Production — walk P1/P2/P3 one at a time, each on its own line.
#     Re-run idempotent? Late data attributed to the event date? The same
#     metric computed the same way in every branch? Quality assertions there?
#     notes: LGTM
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (reading-stage hypothesis only — verified with .explain() later,
#     never asserted from memory)
#     notes: no 
# [ ] Robustness — hardcoded values, assumptions not in the output contract?
#     notes: no, AI also fiter prev_ts by NULL value which I missed here
# [ ] Style/clarity — would you approve this in a real code review?
#     notes: yes I will approve
#
# VERDICT (commit before running): PASS  — because: no obvious error, the whole logic is correct except AI did not broadcast small table
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything the reading missed? None


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
    raw_app_event = spark.createDataFrame(
        [
            ("E01", "D1", "2026-10-07 23:48:00", "app_open"),
            ("E02", "D1", "2026-10-07 23:51:00", "push_received"),
            ("E03", "D1", "2026-10-07 23:55:00", "screen_view"),
            ("E04", "D1", "2026-10-08 00:09:00", "screen_view"),
            ("E05", "D2", "2026-10-07 23:20:00", "app_open"),
            ("E06", "D2", "2026-10-08 00:05:00", "app_open"),
            ("E07", "D2", "2026-10-08 00:12:00", "screen_view"),
            ("E08", "D3", "2026-10-08 08:00:00", "app_open"),
        ],
        schema=(
            "event_id string, device_id string, event_ts string, "
            "event_name string"
        ),
    )
    event_catalog = spark.createDataFrame(
        [
            ("app_open", True, "growth"),
            ("screen_view", True, "content"),
            ("push_received", False, "messaging"),
            ("app_background", False, "platform"),
        ],
        schema="event_name string, is_engagement boolean, owner_team string",
    )
    device_registry = spark.createDataFrame(
        [
            ("D1", "iOS", "apns-7f3a"),
            ("D2", "Android", "fcm-91c2"),
            ("D4", "iOS", "apns-02bd"),
        ],
        schema="device_id string, platform string, push_token string",
    )

    expected = [
        ("D1-20261007234800", "D1", "iOS", "2026-10-07",
         "2026-10-07 23:48:00", "2026-10-08 00:09:00", 3, 1260),
        ("D2-20261007232000", "D2", "Android", "2026-10-07",
         "2026-10-07 23:20:00", "2026-10-07 23:20:00", 1, 0),
        ("D2-20261008000500", "D2", "Android", "2026-10-08",
         "2026-10-08 00:05:00", "2026-10-08 00:12:00", 2, 420),
        ("D3-20261008080000", "D3", "UNKNOWN", "2026-10-08",
         "2026-10-08 08:00:00", "2026-10-08 08:00:00", 1, 0),
    ]

    check(build_fct_app_session_dsl(
              raw_app_event, event_catalog, device_registry),
          expected, "DSL")
    check(build_fct_app_session_sql(
              spark, raw_app_event, event_catalog, device_registry),
          expected, "SQL")

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    check(ai_build_fct_app_session_dsl(
              raw_app_event, event_catalog, device_registry),
          expected, "AI-DSL (post-review verification)")
    check(ai_build_fct_app_session_sql(
              spark, raw_app_event, event_catalog, device_registry),
          expected, "AI-SQL (post-review verification)")

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
# Concept takeaways for this problem: see refs/day27_app_session_ref.md.
