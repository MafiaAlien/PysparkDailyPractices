"""
=====================================================================
PySpark Daily Practice — ETL Scenario Template (Day 22 onward)
=====================================================================
Day 24 — fct_device_hour  (Medium-Hard)

ETL layer : L2 detail modeling
Domain    : logistics — cold-chain warehouse telemetry

BUSINESS CONTEXT
----------------
Cold-chain warehouses are covered by temperature sensors. Every sensor pushes
its readings to a site gateway, the gateway forwards them to the central
collector, and the collector appends whatever it receives into a landing
table. This job runs hourly and turns that landing table into the modeled
telemetry fact every downstream job reads: one conformed row per device per
hour. Nothing downstream ever touches the landing table again.

INPUT TABLES
------------
raw_readings(event_id: string, device_id: string, reading_ts: string,
             temp_c: double, ingest_ts: string)
    -- append-only landing table. Delivery from gateway to collector is
       at-least-once: the gateway holds a message until the collector acks
       it, and sends it again when the ack does not arrive in time.
       `event_id` is stamped by the gateway when it transmits a message;
       `ingest_ts` is stamped by the collector when it writes the row.

device_registry(device_id: string, site_id: string, model: string,
                installed_on: string, decommissioned_on: string)
    -- hand-maintained config table, one row per device. Facilities keeps it
       up to date when hardware is installed or pulled from service.

maintenance_windows(ticket_id: string, device_id: string,
                    window_start: string, window_end: string)
    -- hand-maintained config table. One row per maintenance ticket. While a
       technician is working on a device, whatever the sensor reports is an
       artifact of the maintenance, not the temperature of the warehouse.

Timestamps are ISO-8601 STRINGS ('yyyy-MM-dd HH:mm:ss'), dates are
'yyyy-MM-dd' STRINGS. They are fixed-width, so lexicographic order equals
chronological order and you can compare them directly. This day is about the
pipeline, not about date parsing.

raw_readings:

    +----------+-----------+---------------------+--------+---------------------+
    | event_id | device_id | reading_ts          | temp_c | ingest_ts           |
    +----------+-----------+---------------------+--------+---------------------+
    | E01      | D1        | 2026-08-25 09:05:00 |   22.0 | 2026-08-25 09:06:12 |
    | E02      | D1        | 2026-08-25 09:20:00 |   22.0 | 2026-08-25 09:21:03 |
    | E03      | D2        | 2026-08-25 09:10:00 |   30.5 | 2026-08-25 09:26:40 |
    | E04      | D3        | 2026-08-25 09:30:00 |   15.0 | 2026-08-25 09:31:15 |
    | E05      | D1        | 2026-08-25 10:10:00 |   25.0 | 2026-08-25 10:11:07 |
    | E06      | D2        | 2026-08-25 10:00:00 |   31.0 | 2026-08-25 10:12:55 |
    | E06      | D2        | 2026-08-25 10:00:00 |   31.0 | 2026-08-25 10:12:55 |
    | E07      | D1        | 2026-08-25 09:20:00 |   22.0 | 2026-08-25 10:18:22 |
    +----------+-----------+---------------------+--------+---------------------+

    Note: the table is shown in `ingest_ts` order, which is the order the
    collector appended the rows. `ingest_ts` carries no business meaning —
    it records arrival, not measurement.

device_registry:

    +-----------+---------+--------+--------------+-------------------+
    | device_id | site_id | model  | installed_on | decommissioned_on |
    +-----------+---------+--------+--------------+-------------------+
    | D1        | SITE_A  | TH-200 | 2026-01-10   | NULL              |
    | D2        | SITE_B  | TH-200 | 2026-02-01   | NULL              |
    | D3        | SITE_A  | TH-100 | 2026-03-01   | 2026-08-20        |
    | D4        | SITE_C  | TH-300 | 2026-08-01   | NULL              |
    +-----------+---------+--------+--------------+-------------------+

    Note: a NULL `decommissioned_on` means the device is still in service.
    `model` is an asset-management column; telemetry consumers have no use
    for it.

maintenance_windows:

    +-----------+-----------+---------------------+---------------------+
    | ticket_id | device_id | window_start        | window_end          |
    +-----------+-----------+---------------------+---------------------+
    | MT-101    | D2        | 2026-08-25 09:00:00 | 2026-08-25 09:30:00 |
    | MT-102    | D1        | 2026-08-24 08:00:00 | 2026-08-24 12:00:00 |
    | MT-103    | D4        | 2026-08-25 00:00:00 | 2026-08-26 00:00:00 |
    +-----------+-----------+---------------------+---------------------+

    Note: a device may have any number of tickets, including none.
    `ticket_id` is a work-order reference; telemetry consumers have no use
    for it.

OUTPUT CONTRACT
---------------
Table    : fct_device_hour
Grain    : one row per (device_id, reading_hour), over surviving readings only
Columns  : device_id: string, reading_hour: string, site_id: string,
           n_readings: bigint, avg_temp_c: double, max_temp_c: double
           (this exact order)
Ordering : irrelevant — check() sorts both sides

Cleansing rules — a reading survives only if all of these hold:
  C1  its `device_id` appears in `device_registry`. A reading from hardware
      the registry does not know about is not telemetry and is dropped.
  C2  the device was in service on the calendar date of `reading_ts`:
      installed_on <= date(reading_ts) < decommissioned_on, where a NULL
      `decommissioned_on` means open-ended.
  C3  the reading was not taken inside one of ITS OWN device's maintenance
      windows. A window covers a timestamp t when
      window_start <= t < window_end (half-open). Excluding maintenance
      readings is a filter: it must never duplicate a reading that survives.

Column semantics:
  reading_hour   the hour bucket of `reading_ts`, formatted
                 'yyyy-MM-dd HH:00:00'
  site_id        the device's site, from the registry
  n_readings     how many readings the device took in that hour
  avg_temp_c     the mean temperature over those readings
  max_temp_c     the highest temperature among those readings

A (device_id, reading_hour) with no surviving reading produces no row — this
job does not gap-fill.

Expected:

    +-----------+---------------------+---------+------------+------------+------------+
    | device_id | reading_hour        | site_id | n_readings | avg_temp_c | max_temp_c |
    +-----------+---------------------+---------+------------+------------+------------+
    | D1        | 2026-08-25 09:00:00 | SITE_A  |          2 |       22.0 |       22.0 |
    | D1        | 2026-08-25 10:00:00 | SITE_A  |          1 |       25.0 |       25.0 |
    | D2        | 2026-08-25 10:00:00 | SITE_B  |          1 |       31.0 |       31.0 |
    +-----------+---------------------+---------+------------+------------+------------+

PRODUCTION CONSTRAINTS
----------------------
P1. Every reading is attributed to the hour of its own `reading_ts`.
    `ingest_ts` records when the collector received the message and must
    never determine which hour a reading falls in.
P2. Before returning, the job must assert its own output: exactly one row
    per (device_id, reading_hour), and no NULL in `device_id` /
    `reading_hour` / `site_id`. A silent bad publish is worse than a failed
    job.
P3. Read only the columns this job needs from the two config tables.
    `model` and `ticket_id` are operational metadata and must not enter the
    pipeline.

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
refs/day24_device_hour_ref.md. Do not open it before Stage 5.
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
def build_fct_device_hour_dsl(
    raw_readings: DataFrame,
    device_registry: DataFrame,
    maintenance_windows: DataFrame,
) -> DataFrame:
    """Conformed hourly telemetry fact, one row per device per hour.

    Hint: both validity checks are half-open — start <= t < end, with a
    NULL end meaning open-ended.
    """
    # TODO: implement
    dedup_window = Window.partitionBy('device_id', 'reading_ts').orderBy(F.col('reading_ts').asc_nulls_first())
    join_cond_filter_maintenance_window = (raw_readings.device_id == maintenance_windows.device_id) & ((maintenance_windows.window_start <= raw_readings.reading_ts) & (raw_readings.reading_ts < maintenance_windows.window_end))

    filter_maintenance_windows = (
        raw_readings
        .withColumn(
            "_rn",
            F.row_number().over(dedup_window)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .join(
            maintenance_windows,
            on=join_cond_filter_maintenance_window,
            how='left_anti'
        )

        )
    reading_entrich_date_hour = (
        filter_maintenance_windows
    .withColumn(
        'reading_hour',
        F.concat(F.substring(F.col('reading_ts'), 1, 13), F.lit(":00:00"))
    )
    .groupBy('device_id', "reading_hour")
    .agg(
        F.count(F.lit(1)).alias('n_readings'),
        F.avg(F.col('temp_c')).cast('double').alias("avg_temp_c"),
        F.max(F.col('temp_c')).cast('double').alias('max_temp_c')
    ).withColumn(
        '_reading_date',
        F.to_date(F.col('reading_hour'))
    )
    )

    device_registry = (
        device_registry.withColumn(
            'decommissioned_on',
            F.coalesce(F.col('decommissioned_on'), F.lit('9999-12-31'))
        )
    )

    join_cond_device_readings = (device_registry.device_id == reading_entrich_date_hour.device_id) & ((device_registry.installed_on <= reading_entrich_date_hour._reading_date) & (reading_entrich_date_hour._reading_date < device_registry.decommissioned_on))

    agg_device_reading_hours = (
        device_registry.join(
            reading_entrich_date_hour,
            on=join_cond_device_readings,
            how='left'
    )
    .filter(F.col('reading_hour').isNotNull())
    .select(
        device_registry.device_id,
        'reading_hour',
        'site_id',
        F.coalesce(F.col('n_readings'), F.lit(0)).cast('int').alias('n_readings'),
        F.coalesce('avg_temp_c', F.lit(0.0)).cast('double').alias('avg_temp_c'),
        F.coalesce('max_temp_c', F.lit(0.0)).cast('double').alias('max_temp_c')
    )
    )

    null_keys = agg_device_reading_hours.filter(
        F.col("device_id").isNull() | F.col("site_id").isNull() | F.col('reading_hour').isNull()
    ).count()
    if null_keys:
        raise AssertionError(
            f"agg_device_reading_hours: {null_keys} row(s) with NULL device_id/reading_hour/site_id"
        )

    return agg_device_reading_hours




# ------------------------------ 1b. Spark SQL ------------------------
def build_fct_device_hour_sql(
    spark: SparkSession,
    raw_readings: DataFrame,
    device_registry: DataFrame,
    maintenance_windows: DataFrame,
) -> DataFrame:
    """Same job, Spark SQL.

    Hint: both validity checks are half-open — start <= t < end, with a
    NULL end meaning open-ended.
    """
    raw_readings.createOrReplaceTempView("raw_readings")
    device_registry.createOrReplaceTempView("device_registry")
    maintenance_windows.createOrReplaceTempView("maintenance_windows")

    sql = """
        -- write your SQL here
        WITH dedup_reading_ts AS (
            SELECT 
                *,
                ROW_NUMBER()OVER(PARTITION BY device_id, reading_ts ORDER BY reading_ts ASC) as _rn
            FROM 
                raw_readings
        ),

        filter_maitain_devices AS (
            SELECT 
                d.device_id,
                d.reading_ts,
                d.temp_c
            FROM 
                dedup_reading_ts  d
            WHERE _rn = 1 AND NOT EXISTS (
                SELECT 
                    * 
                FROM 
                    maintenance_windows m 
                WHERE d.device_id = m.device_id AND ((m.window_start <= d.reading_ts) AND (d.reading_ts < m.window_end))
            )
        ),

        add_reading_hour AS (
            SELECT 
                device_id,
                CONCAT(SUBSTRING(reading_ts, 1, 13), ':00:00') AS reading_hour,
                temp_c
            FROM 
                filter_maitain_devices
        ),

        agg_avg_max AS (
            SELECT 
                device_id,
                reading_hour,
                COUNT(*) AS n_readings,
                AVG(temp_c) AS avg_temp_c,
                MAX(temp_c) AS max_temp_c
            FROM 
                add_reading_hour
            GROUP BY 
                device_id,
                reading_hour
        ),

        add_reading_date AS (
            SELECT 
                device_id,
                reading_hour,
                TO_DATE(reading_hour) AS reading_date,
                n_readings,
                avg_temp_c,
                max_temp_c
            FROM 
                agg_avg_max
        ),

        fill_null_decommissioned_on AS (
            SELECT 
                device_id,
                site_id,
                installed_on,
                COALESCE(decommissioned_on, '9999-12-31') AS decommissioned_on
            FROM 
                device_registry
        ),

        join_readings_registry AS(
            SELECT 
                d.device_id,
                reading_hour,
                site_id,
                CAST(COALESCE(n_readings, 0) AS INTEGER) AS n_readings,
                CAST(COALESCE(avg_temp_c, 0.0) AS DOUBLE) AS avg_temp_c,
                CAST(COALESCE(max_temp_c, 0.0) AS DOUBLE) AS max_temp_c
            FROM fill_null_decommissioned_on d LEFT JOIN add_reading_date a ON d.device_id = a.device_id AND ((d.installed_on <= a.reading_date) AND (a.reading_date < d.decommissioned_on))
            WHERE reading_hour IS NOT NULL
        )

        SELECT * FROM join_readings_registry
    """
    res_df = spark.sql(sql)

    null_keys = res_df.filter(
        F.col("device_id").isNull() | F.col("site_id").isNull() | F.col('reading_hour').isNull()
    ).count()
    if null_keys:
        raise AssertionError(
            f"res_df: {null_keys} row(s) with NULL device_id/reading_hour/site_id"
        )

    return res_df


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
def ai_build_fct_device_hour_dsl(
    raw_readings: DataFrame,
    device_registry: DataFrame,
    maintenance_windows: DataFrame,
) -> DataFrame:
    registry = device_registry.select(
        "device_id", "site_id", "installed_on", "decommissioned_on"
    )
    windows = maintenance_windows.select(
        F.col("device_id").alias("mw_device_id"),
        F.col("window_start"),
        F.col("window_end"),
    )

    deduped = raw_readings.select("device_id", "reading_ts", "temp_c").distinct()

    reading_date = F.substring(F.col("reading_ts"), 1, 10)

    in_service = (
        deduped.join(registry, on="device_id", how="inner")
        .where(F.col("installed_on") <= reading_date)
        .where(
            F.col("decommissioned_on").isNull()
            | (reading_date < F.col("decommissioned_on"))
        )
    )

    surviving = in_service.join(
        windows,
        (F.col("device_id") == F.col("mw_device_id"))
        & (F.col("window_start") <= F.col("reading_ts"))
        & (F.col("reading_ts") < F.col("window_end")),
        "left_anti",
    )

    result = (
        surviving.withColumn(
            "reading_hour",
            F.concat(F.substring(F.col("reading_ts"), 1, 13), F.lit(":00:00")),
        )
        .groupBy("device_id", "reading_hour", "site_id")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("n_readings"),
            F.avg("temp_c").cast("double").alias("avg_temp_c"),
            F.max("temp_c").cast("double").alias("max_temp_c"),
        )
        .select(
            "device_id",
            "reading_hour",
            "site_id",
            "n_readings",
            "avg_temp_c",
            "max_temp_c",
        )
    ).cache()

    total_rows = result.count()
    distinct_keys = result.select("device_id", "reading_hour").distinct().count()
    if total_rows != distinct_keys:
        raise ValueError(
            "fct_device_hour grain violated: "
            f"{total_rows} rows for {distinct_keys} (device_id, reading_hour) keys"
        )
    null_rows = result.where(
        F.col("device_id").isNull()
        | F.col("reading_hour").isNull()
        | F.col("site_id").isNull()
    ).count()
    if null_rows:
        raise ValueError(
            f"fct_device_hour has {null_rows} row(s) with NULL device_id/reading_hour/site_id"
        )

    return result


def ai_build_fct_device_hour_sql(
    spark: SparkSession,
    raw_readings: DataFrame,
    device_registry: DataFrame,
    maintenance_windows: DataFrame,
) -> DataFrame:
    raw_readings.createOrReplaceTempView("ai_raw_readings_src")
    device_registry.select(
        "device_id", "site_id", "installed_on", "decommissioned_on"
    ).createOrReplaceTempView("ai_device_registry_src")
    maintenance_windows.select(
        "device_id", "window_start", "window_end"
    ).createOrReplaceTempView("ai_maintenance_windows_src")

    result = spark.sql(
        """
        WITH deduped AS (
            SELECT DISTINCT device_id, reading_ts, temp_c
            FROM ai_raw_readings_src
        ),
        in_service AS (
            SELECT d.device_id, d.reading_ts, d.temp_c, r.site_id
            FROM deduped d
            JOIN ai_device_registry_src r
              ON d.device_id = r.device_id
            WHERE r.installed_on <= substring(d.reading_ts, 1, 10)
              AND (
                    r.decommissioned_on IS NULL
                 OR substring(d.reading_ts, 1, 10) < r.decommissioned_on
              )
        ),
        surviving AS (
            SELECT s.device_id, s.reading_ts, s.temp_c, s.site_id
            FROM in_service s
            WHERE NOT EXISTS (
                SELECT 1
                FROM ai_maintenance_windows_src m
                WHERE m.device_id = s.device_id
                  AND m.window_start <= s.reading_ts
                  AND s.reading_ts < m.window_end
            )
        )
        SELECT
            device_id,
            concat(substring(reading_ts, 1, 13), ':00:00') AS reading_hour,
            site_id,
            CAST(count(1) AS BIGINT)      AS n_readings,
            CAST(avg(temp_c) AS DOUBLE)   AS avg_temp_c,
            CAST(max(temp_c) AS DOUBLE)   AS max_temp_c
        FROM surviving
        GROUP BY
            device_id,
            concat(substring(reading_ts, 1, 13), ':00:00'),
            site_id
        """
    ).cache()

    total_rows = result.count()
    distinct_keys = result.select("device_id", "reading_hour").distinct().count()
    if total_rows != distinct_keys:
        raise ValueError(
            "fct_device_hour grain violated: "
            f"{total_rows} rows for {distinct_keys} (device_id, reading_hour) keys"
        )
    null_rows = result.where(
        F.col("device_id").isNull()
        | F.col("reading_hour").isNull()
        | F.col("site_id").isNull()
    ).count()
    if null_rows:
        raise ValueError(
            f"fct_device_hour has {null_rows} row(s) with NULL device_id/reading_hour/site_id"
        )

    return result
# >>> PASTE END

# ---------------------------------------------------------------------
# REVIEW_NOTES — fill in BEFORE running anything.
# Severity order: runtime break > wrong results on dirty data
#               > production robustness > performance > portability > style
# ---------------------------------------------------------------------
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes: LGTM, but Incognito aggregate result after joining with registry table by inner join, I got no idea if this will have fan-out effect
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes: LGTM
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (cast, division, array index, element_at)?
#     notes: no coalesce, becasue  AI use inner join ,which will filter out device that neither exists in reading table nor registry table, so it does not exist NULL 
# [ ] Production — walk P1/P2/P3 one at a time, each on its own line.
#     Re-run idempotent? Late data attributed to the event date? The same
#     metric computed the same way in every branch? Quality assertions there?
#     notes: the site id is as a key of table joining, which does not follow P2 here, and I bypass row count reconciliation between distinct rows and output rows
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (reading-stage hypothesis only — verified with .explain() later,
#     never asserted from memory)
#     notes: LGTM
# [ ] Robustness — hardcoded values, assumptions not in the output contract?
#     notes: LGTM
# [ ] Style/clarity — would you approve this in a real code review?
#     notes: LGTM
#
# VERDICT (commit before running): PASS — because: follow the logic as same as mine but be different from groupby step
# ACTUAL RESULT (after Stage 4 run): passed
# GAP ANALYSIS: did the run reveal anything the reading missed? inner join with registry table still work here


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

    raw_readings = spark.createDataFrame(
        [
            ("E01", "D1", "2026-08-25 09:05:00", 22.0, "2026-08-25 09:06:12"),
            ("E02", "D1", "2026-08-25 09:20:00", 22.0, "2026-08-25 09:21:03"),
            ("E03", "D2", "2026-08-25 09:10:00", 30.5, "2026-08-25 09:26:40"),
            ("E04", "D3", "2026-08-25 09:30:00", 15.0, "2026-08-25 09:31:15"),
            ("E05", "D1", "2026-08-25 10:10:00", 25.0, "2026-08-25 10:11:07"),
            ("E06", "D2", "2026-08-25 10:00:00", 31.0, "2026-08-25 10:12:55"),
            ("E06", "D2", "2026-08-25 10:00:00", 31.0, "2026-08-25 10:12:55"),
            ("E07", "D1", "2026-08-25 09:20:00", 22.0, "2026-08-25 10:18:22"),
        ],
        schema=(
            "event_id string, device_id string, reading_ts string, "
            "temp_c double, ingest_ts string"
        ),
    )
    device_registry = spark.createDataFrame(
        [
            ("D1", "SITE_A", "TH-200", "2026-01-10", None),
            ("D2", "SITE_B", "TH-200", "2026-02-01", None),
            ("D3", "SITE_A", "TH-100", "2026-03-01", "2026-08-20"),
            ("D4", "SITE_C", "TH-300", "2026-08-01", None),
        ],
        schema=(
            "device_id string, site_id string, model string, "
            "installed_on string, decommissioned_on string"
        ),
    )
    maintenance_windows = spark.createDataFrame(
        [
            ("MT-101", "D2", "2026-08-25 09:00:00", "2026-08-25 09:30:00"),
            ("MT-102", "D1", "2026-08-24 08:00:00", "2026-08-24 12:00:00"),
            ("MT-103", "D4", "2026-08-25 00:00:00", "2026-08-26 00:00:00"),
        ],
        schema=(
            "ticket_id string, device_id string, window_start string, "
            "window_end string"
        ),
    )

    expected = [
        ("D1", "2026-08-25 09:00:00", "SITE_A", 2, 22.0, 22.0),
        ("D1", "2026-08-25 10:00:00", "SITE_A", 1, 25.0, 25.0),
        ("D2", "2026-08-25 10:00:00", "SITE_B", 1, 31.0, 31.0),
    ]

    check(build_fct_device_hour_dsl(
              raw_readings, device_registry, maintenance_windows),
          expected, "DSL")
    check(build_fct_device_hour_sql(
              spark, raw_readings, device_registry, maintenance_windows),
          expected, "SQL")

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    check(ai_build_fct_device_hour_dsl(
              raw_readings, device_registry, maintenance_windows),
          expected, "AI-DSL (post-review verification)")
    check(ai_build_fct_device_hour_sql(
              spark, raw_readings, device_registry, maintenance_windows),
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
# Concept takeaways for this problem: see refs/day24_device_hour_ref.md.
