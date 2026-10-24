"""
=====================================================================
PySpark Daily Practice — ETL Scenario
=====================================================================
Day 28 — trip_payout_resolution  (Medium-Hard)

ETL layer : L4 incremental
Domain    : ride-hailing driver payout

BUSINESS CONTEXT
----------------
Finance pays drivers every morning from the trips this nightly job marks
PUBLISHED. Each run takes the night's landing batch of completed trips plus
every trip an earlier run had to hold back, decides which of them can be
paid now, and holds back the rest for the next run. Drivers who onboard
during the day often complete trips before the nightly dim_driver snapshot
includes them.

INPUT TABLES
------------
raw_trip(trip_id: string, driver_id: string, completed_ts: string,
         fare_cents: bigint, landing_date: string)
    -- append-only landing batch of trips completed since the last run.
       completed_ts is UTC, format 'yyyy-MM-dd HH:mm:ss'. landing_date is
       the date of the run that landed the row; every row in one batch
       carries the same value.

pending_trip(trip_id: string, driver_id: string, completed_ts: string,
             fare_cents: bigint, first_seen_date: string)
    -- trips held back by earlier runs, carried into this run.
       first_seen_date is the landing_date of the batch that first brought
       the trip in.

dim_driver(driver_id: string, fleet_id: string)
    -- daily snapshot dimension, cut before this run. driver_id is unique
       and never NULL.

fleet_commission(fleet_id: string, commission_pct: int)
    -- hand-maintained config: the platform's commission per fleet, in
       whole percent. fleet_id is unique and never NULL.

Upstream guarantees on the two trip feeds: trip_id is unique across
raw_trip and pending_trip together; driver_id, completed_ts and fare_cents
are never NULL.

raw_trip:

    +---------+-----------+---------------------+------------+--------------+
    | trip_id | driver_id | completed_ts        | fare_cents | landing_date |
    +---------+-----------+---------------------+------------+--------------+
    | T101    | D1        | 2026-10-20 08:15:00 | 2500       | 2026-10-20   |
    | T102    | D2        | 2026-10-20 09:40:00 | 4000       | 2026-10-20   |
    | T103    | D3        | 2026-10-20 11:05:00 | 1800       | 2026-10-20   |
    | T104    | D9        | 2026-10-20 12:30:00 | 3000       | 2026-10-20   |
    | T105    | D5        | 2026-10-20 14:10:00 | 2200       | 2026-10-20   |
    | T106    | D1        | 2026-10-20 23:50:00 | 1500       | 2026-10-20   |
    +---------+-----------+---------------------+------------+--------------+

pending_trip:

    +---------+-----------+---------------------+------------+-----------------+
    | trip_id | driver_id | completed_ts        | fare_cents | first_seen_date |
    +---------+-----------+---------------------+------------+-----------------+
    | T085    | D8        | 2026-10-18 17:20:00 | 2000       | 2026-10-18      |
    | T090    | D4        | 2026-10-19 21:00:00 | 3500       | 2026-10-19      |
    +---------+-----------+---------------------+------------+-----------------+

dim_driver:

    +-----------+----------+
    | driver_id | fleet_id |
    +-----------+----------+
    | D1        | F1       |
    | D2        | F2       |
    | D3        | F9       |
    | D4        | F1       |
    | D5        | NULL     |
    +-----------+----------+

    notes: D4 onboarded on 2026-10-19 and first appears in today's
           snapshot. D9 onboarded on 2026-10-20, after the snapshot was
           cut.

fleet_commission:

    +----------+----------------+
    | fleet_id | commission_pct |
    +----------+----------------+
    | F1       | 20             |
    | F2       | 25             |
    | F3       | 15             |
    +----------+----------------+

OUTPUT CONTRACT
---------------
Table    : trip_payout_resolution
Grain    : one row per trip_id in raw_trip UNION pending_trip
Columns  : trip_id: string, driver_id: string, fleet_id: string,
           trip_date: string, fare_cents: bigint,
           driver_earning_cents: bigint, first_seen_date: string,
           load_status: string
           (this exact order)
Ordering : irrelevant — check() sorts both sides

Payability rule:
  A trip can be paid only when its driver_id is in dim_driver AND that
  driver's fleet_id has a row in fleet_commission. A trip that can be paid
  is PUBLISHED; any other trip is PENDING. The publisher writes PENDING
  rows back into pending_trip, so a PENDING trip is offered again on every
  later run until it can be paid.

Column definitions:
  fleet_id             : dim_driver.fleet_id of the trip's driver; NULL
                         when the driver is not in dim_driver
  trip_date            : UTC calendar date of completed_ts, 'yyyy-MM-dd'
  driver_earning_cents : PUBLISHED -> fare_cents
                                      - fare_cents * commission_pct / 100
                         (integer cents; every test value divides exactly)
                         PENDING   -> NULL
  first_seen_date      : raw_trip.landing_date for a trip from raw_trip;
                         pending_trip.first_seen_date for a carried trip
  load_status          : 'PUBLISHED' or 'PENDING'

Expected:

    +---------+-----------+----------+------------+------------+----------------------+-----------------+-------------+
    | trip_id | driver_id | fleet_id | trip_date  | fare_cents | driver_earning_cents | first_seen_date | load_status |
    +---------+-----------+----------+------------+------------+----------------------+-----------------+-------------+
    | T085    | D8        | NULL     | 2026-10-18 | 2000       | NULL                 | 2026-10-18      | PENDING     |
    | T090    | D4        | F1       | 2026-10-19 | 3500       | 2800                 | 2026-10-19      | PUBLISHED   |
    | T101    | D1        | F1       | 2026-10-20 | 2500       | 2000                 | 2026-10-20      | PUBLISHED   |
    | T102    | D2        | F2       | 2026-10-20 | 4000       | 3000                 | 2026-10-20      | PUBLISHED   |
    | T103    | D3        | F9       | 2026-10-20 | 1800       | NULL                 | 2026-10-20      | PENDING     |
    | T104    | D9        | NULL     | 2026-10-20 | 3000       | NULL                 | 2026-10-20      | PENDING     |
    | T105    | D5        | NULL     | 2026-10-20 | 2200       | NULL                 | 2026-10-20      | PENDING     |
    | T106    | D1        | F1       | 2026-10-20 | 1500       | 1200                 | 2026-10-20      | PUBLISHED   |
    +---------+-----------+----------+------------+------------+----------------------+-----------------+-------------+

PRODUCTION CONSTRAINTS
----------------------
P1. Every candidate trip comes out of the job exactly once — PUBLISHED or
    PENDING, never dropped, never duplicated. Before returning, the job
    must assert it: the output row count equals count(raw_trip) +
    count(pending_trip), and trip_id is unique in the output.
P2. A payout is never published incomplete. Before returning, the job
    must assert that driver_earning_cents is non-NULL on every PUBLISHED
    row and NULL on every PENDING row. A silent bad publish is worse than
    a failed job.
P3. A night with no completed trips is a normal night. With raw_trip empty
    the job must still succeed — every assertion included — and return
    exactly the carried pending trips' rows; with raw_trip and
    pending_trip both empty it must return 0 rows.

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
refs/day28_trip_payout_ref.md. Do not open it before Stage 5.
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
def build_trip_payout_resolution_dsl(
    raw_trip: DataFrame,
    pending_trip: DataFrame,
    dim_driver: DataFrame,
    fleet_commission: DataFrame,
) -> DataFrame:
    """Resolve tonight's and carried trips into PUBLISHED / PENDING payouts.

    Hint: gather candidates -> enrich -> classify and pay -> assert, then return.
    """
    # TODO: implement
    completes = (
        raw_trip.select(
            'trip_id',
            'driver_id',
            'completed_ts',
            'fare_cents',
            F.col('landing_date').alias('first_seen_date')
        )
    )

    pendings = (
        pending_trip.select(
            'trip_id', 
            'driver_id', 
            'completed_ts',
            'fare_cents', 
            'first_seen_date' 
        )
    )

    all_trips = (
        completes.unionByName(pendings)
    )

    trips_with_fleet = (
        all_trips.join(
            dim_driver.alias('d'),
            'driver_id',
            'left'
        ).join(
            F.broadcast(fleet_commission.alias('f')),
            'fleet_id',
            'left'
        ).select(
            'trip_id',
            'driver_id',
            'd.fleet_id',
            F.col('f.fleet_id').alias('commission_fleet_id'),
            F.date_format('completed_ts', 'yyyy-MM-dd').alias('trip_date'),
            'fare_cents', 
            'first_seen_date',
            'commission_pct'
        )
    )

    fare_cents = F.col('fare_cents')
    commission_pct = F.col('commission_pct')
    trips_with_earnings = (
        trips_with_fleet.withColumn(
            'load_status',
            F.when(F.col('commission_fleet_id').isNotNull(), F.lit('PUBLISHED')).otherwise('PENDING')
        )
        .drop('commission_fleet_id')
        .withColumn(
            'driver_earning_cents',
            F.when(
                (F.col('load_status') == 'PUBLISHED'),
                (fare_cents - fare_cents * commission_pct / 100)
            ).otherwise(F.lit(None))
        )
    )

    res = trips_with_earnings.select(
        'trip_id',
        'driver_id',
        'fleet_id',
        'trip_date',
        'fare_cents',
        F.col('driver_earning_cents').cast('bigint'), 
        'first_seen_date', 
        F.col('load_status').cast('string').alias('load_status')
    )

    # P1. Every candidate trip comes out of the job exactly once — PUBLISHED or
    # PENDING, never dropped, never duplicated. Before returning, the job
    # must assert it: the output row count equals count(raw_trip) +
    # count(pending_trip), and trip_id is unique in the output.
    unique = res.agg(F.count('trip_id').alias('cnt')).first()['cnt']
    pend_trips_cnt = pending_trip.agg(F.count('trip_id').alias('pending_cnt')).first()['pending_cnt']
    completed_trips_cnt = raw_trip.agg(F.count('trip_id').alias('completed_cnt')).first()['completed_cnt']

    assert pend_trips_cnt + completed_trips_cnt == unique, f'dup trip exits, pending:{pend_trips_cnt}, completed:{completed_trips_cnt}, result:{unique}'


    # P2. A payout is never published incomplete. Before returning, the job
    # must assert that driver_earning_cents is non-NULL on every PUBLISHED
    # row and NULL on every PENDING row. A silent bad publish is worse than
    # a failed job.
    published_null_cnt = (
        res.filter(F.col('load_status') == 'PUBLISHED')
        .agg(
            F.sum(F.when(F.col('driver_earning_cents').isNull(), F.lit(1)).otherwise(0)).alias('published_null')
        ).first()['published_null']
    )

    pending_not_null_cnt = (
        res.filter(F.col('load_status') == 'PENDING')
        .agg(
            F.sum(F.when(F.col('driver_earning_cents').isNotNull(), F.lit(1)).otherwise(0)).alias('pending_not_null')
        ).first()['pending_not_null']
    )
    assert published_null_cnt is not None or pending_not_null_cnt is not None, f'payout is published incomplete'


    # P3. A night with no completed trips is a normal night. With raw_trip empty
    # the job must still succeed — every assertion included — and return
    # exactly the carried pending trips' rows; with raw_trip and
    # pending_trip both empty it must return 0 rows.

    return res 




# ------------------------------ 1b. Spark SQL ------------------------
def build_trip_payout_resolution_sql(
    spark: SparkSession,
    raw_trip: DataFrame,
    pending_trip: DataFrame,
    dim_driver: DataFrame,
    fleet_commission: DataFrame,
) -> DataFrame:
    """Same job, Spark SQL.

    Hint: one statement can carry the whole job; the assertions run on its result.
    """
    raw_trip.createOrReplaceTempView("raw_trip")
    pending_trip.createOrReplaceTempView("pending_trip")
    dim_driver.createOrReplaceTempView("dim_driver")
    fleet_commission.createOrReplaceTempView("fleet_commission")

    sql = """
        -- write your SQL here
        WITH completes AS (
            SELECT 
                trip_id,
                driver_id,
                completed_ts,
                fare_cents,
                landing_date AS first_seen_date
            FROM 
                raw_trip
        ),

        union_trips AS (
            SELECT 
                *
            FROM 
                completes
            UNION ALL 
            SELECT 
                *
            FROM 
                pending_trip
        ),

        trips_with_fleet AS (
            SELECT 
                trip_id,
                ut.driver_id,
                d.fleet_id AS fleet_id,
                f.fleet_id AS f_fleet_id,
                DATE_FORMAT(completed_ts, 'yyyy-MM-dd') AS trip_date,
                fare_cents,
                first_seen_date,
                commission_pct
            FROM
            union_trips ut LEFT JOIN dim_driver d ON ut.driver_id = d.driver_id
            LEFT JOIN fleet_commission f ON d.fleet_id = f.fleet_id
        ),

        trips_with_loads AS (
            SELECT 
                trip_id,
                driver_id,
                fleet_id,
                trip_date,
                fare_cents,
                first_seen_date,
                commission_pct,
                CASE 
                    WHEN f_fleet_id IS NOT NULL THEN 'PUBLISHED'
                    ELSE 'PENDING' END AS load_status
            FROM 
                trips_with_fleet
        ),

        trips_with_earnings AS (
            SELECT 
                trip_id,
                driver_id,
                fleet_id,
                trip_date,
                fare_cents,
                CASE 
                    WHEN load_status = 'PUBLISHED' THEN fare_cents - fare_cents * commission_pct / 100
                    ELSE NULL END AS driver_earning_cents,
                first_seen_date,
                CAST(load_status AS STRING) AS load_status
            FROM
                trips_with_loads
        )

        SELECT 
            trip_id,
            driver_id,
            fleet_id,
            trip_date,
            fare_cents,
            CAST(driver_earning_cents AS BIGINT) AS driver_earning_cents,
            first_seen_date,
            load_status
        FROM 
            trips_with_earnings
    """

    res_sql = spark.sql(sql)
    unique = res_sql.agg(F.count('trip_id').alias('cnt')).first()['cnt']
    pend_trips_cnt = pending_trip.agg(F.count('trip_id').alias('pending_cnt')).first()['pending_cnt']
    completed_trips_cnt = raw_trip.agg(F.count('trip_id').alias('completed_cnt')).first()['completed_cnt']

    assert pend_trips_cnt + completed_trips_cnt == unique, f'dup trip exits, pending:{pend_trips_cnt}, completed:{completed_trips_cnt}, result:{unique}'

    published_null_cnt = (
        res_sql.filter(F.col('load_status') == 'PUBLISHED')
        .agg(
            F.sum(F.when(F.col('driver_earning_cents').isNull(), F.lit(1)).otherwise(0)).alias('published_null')
        ).first()['published_null']
    )

    pending_not_null_cnt = (
        res_sql.filter(F.col('load_status') == 'PENDING')
        .agg(
            F.sum(F.when(F.col('driver_earning_cents').isNotNull(), F.lit(1)).otherwise(0)).alias('pending_not_null')
        ).first()['pending_not_null']
    )
    assert published_null_cnt is not None or pending_not_null_cnt is not None, f'payout is published incomplete'

    return res_sql


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
def ai_build_trip_payout_resolution_dsl(
    raw_trip: DataFrame,
    pending_trip: DataFrame,
    dim_driver: DataFrame,
    fleet_commission: DataFrame,
) -> DataFrame:
    raw_c = raw_trip.select(
        F.col("trip_id").cast("string").alias("trip_id"),
        F.col("driver_id").cast("string").alias("driver_id"),
        F.col("completed_ts").cast("string").alias("completed_ts"),
        F.col("fare_cents").cast("bigint").alias("fare_cents"),
        F.col("landing_date").cast("string").alias("first_seen_date"),
    )
    pend_c = pending_trip.select(
        F.col("trip_id").cast("string").alias("trip_id"),
        F.col("driver_id").cast("string").alias("driver_id"),
        F.col("completed_ts").cast("string").alias("completed_ts"),
        F.col("fare_cents").cast("bigint").alias("fare_cents"),
        F.col("first_seen_date").cast("string").alias("first_seen_date"),
    )
    candidates = raw_c.unionByName(pend_c)

    d = dim_driver.select(
        F.col("driver_id").cast("string").alias("d_driver_id"),
        F.col("fleet_id").cast("string").alias("d_fleet_id"),
    )
    c = fleet_commission.select(
        F.col("fleet_id").cast("string").alias("c_fleet_id"),
        F.col("commission_pct").cast("bigint").alias("c_commission_pct"),
    )

    joined = (
        candidates
        .join(d, F.col("driver_id") == F.col("d_driver_id"), "left")
        .join(c, F.col("d_fleet_id") == F.col("c_fleet_id"), "left")
    )

    payable = F.col("d_driver_id").isNotNull() & F.col("c_fleet_id").isNotNull()

    result = joined.select(
        F.col("trip_id"),
        F.col("driver_id"),
        F.col("d_fleet_id").alias("fleet_id"),
        F.date_format(
            F.to_date(F.col("completed_ts"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"
        ).alias("trip_date"),
        F.col("fare_cents"),
        F.when(
            payable,
            F.expr("fare_cents - ((fare_cents * c_commission_pct) DIV 100)"),
        ).cast("bigint").alias("driver_earning_cents"),
        F.col("first_seen_date"),
        F.when(payable, F.lit("PUBLISHED")).otherwise(F.lit("PENDING")).alias("load_status"),
    )

    expected_rows = raw_trip.count() + pending_trip.count()

    stats = result.agg(
        F.count(F.lit(1)).alias("n_rows"),
        F.countDistinct("trip_id").alias("n_distinct"),
        F.count(F.when(F.col("trip_id").isNull(), 1)).alias("n_null_id"),
        F.count(
            F.when(
                (F.col("load_status") == "PUBLISHED") & F.col("driver_earning_cents").isNull(), 1
            )
        ).alias("bad_published"),
        F.count(
            F.when(
                (F.col("load_status") == "PENDING") & F.col("driver_earning_cents").isNotNull(), 1
            )
        ).alias("bad_pending"),
        F.count(
            F.when(
                F.col("load_status").isNull()
                | ~F.col("load_status").isin("PUBLISHED", "PENDING"),
                1,
            )
        ).alias("bad_status"),
    ).first()

    if stats["n_rows"] != expected_rows:
        raise AssertionError(
            f"P1 violated: output has {stats['n_rows']} rows, expected {expected_rows}"
        )
    if stats["n_null_id"] != 0 or stats["n_distinct"] != stats["n_rows"]:
        raise AssertionError("P1 violated: trip_id is not unique in the output")
    if stats["bad_published"] != 0:
        raise AssertionError(
            f"P2 violated: {stats['bad_published']} PUBLISHED rows have NULL driver_earning_cents"
        )
    if stats["bad_pending"] != 0:
        raise AssertionError(
            f"P2 violated: {stats['bad_pending']} PENDING rows have non-NULL driver_earning_cents"
        )
    if stats["bad_status"] != 0:
        raise AssertionError("Invalid load_status values in output")

    return result


def ai_build_trip_payout_resolution_sql(
    spark: SparkSession,
    raw_trip: DataFrame,
    pending_trip: DataFrame,
    dim_driver: DataFrame,
    fleet_commission: DataFrame,
) -> DataFrame:
    raw_trip.createOrReplaceTempView("raw_trip")
    pending_trip.createOrReplaceTempView("pending_trip")
    dim_driver.createOrReplaceTempView("dim_driver")
    fleet_commission.createOrReplaceTempView("fleet_commission")

    result = spark.sql("""
        WITH candidates AS (
            SELECT CAST(trip_id AS STRING)       AS trip_id,
                   CAST(driver_id AS STRING)     AS driver_id,
                   CAST(completed_ts AS STRING)  AS completed_ts,
                   CAST(fare_cents AS BIGINT)    AS fare_cents,
                   CAST(landing_date AS STRING)  AS first_seen_date
            FROM raw_trip
            UNION ALL
            SELECT CAST(trip_id AS STRING)         AS trip_id,
                   CAST(driver_id AS STRING)       AS driver_id,
                   CAST(completed_ts AS STRING)    AS completed_ts,
                   CAST(fare_cents AS BIGINT)      AS fare_cents,
                   CAST(first_seen_date AS STRING) AS first_seen_date
            FROM pending_trip
        ),
        resolved AS (
            SELECT t.trip_id,
                   t.driver_id,
                   CAST(d.fleet_id AS STRING) AS fleet_id,
                   t.completed_ts,
                   t.fare_cents,
                   t.first_seen_date,
                   CAST(c.commission_pct AS BIGINT) AS commission_pct,
                   (d.driver_id IS NOT NULL AND c.fleet_id IS NOT NULL) AS payable
            FROM candidates t
            LEFT JOIN dim_driver d
                   ON t.driver_id = d.driver_id
            LEFT JOIN fleet_commission c
                   ON d.fleet_id = c.fleet_id
        )
        SELECT trip_id,
               driver_id,
               fleet_id,
               date_format(to_date(completed_ts, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS trip_date,
               fare_cents,
               CAST(CASE WHEN payable
                         THEN fare_cents - ((fare_cents * commission_pct) DIV 100)
                    END AS BIGINT) AS driver_earning_cents,
               first_seen_date,
               CASE WHEN payable THEN 'PUBLISHED' ELSE 'PENDING' END AS load_status
        FROM resolved
    """)

    result.createOrReplaceTempView("trip_payout_resolution_candidate")

    stats = spark.sql("""
        SELECT
            (SELECT COUNT(1) FROM raw_trip) + (SELECT COUNT(1) FROM pending_trip) AS expected_rows,
            COUNT(1)                                                              AS n_rows,
            COUNT(DISTINCT trip_id)                                               AS n_distinct,
            COUNT(CASE WHEN trip_id IS NULL THEN 1 END)                           AS n_null_id,
            COUNT(CASE WHEN load_status = 'PUBLISHED'
                        AND driver_earning_cents IS NULL THEN 1 END)              AS bad_published,
            COUNT(CASE WHEN load_status = 'PENDING'
                        AND driver_earning_cents IS NOT NULL THEN 1 END)          AS bad_pending,
            COUNT(CASE WHEN load_status IS NULL
                         OR load_status NOT IN ('PUBLISHED', 'PENDING') THEN 1 END) AS bad_status
        FROM trip_payout_resolution_candidate
    """).first()

    if stats["n_rows"] != stats["expected_rows"]:
        raise AssertionError(
            f"P1 violated: output has {stats['n_rows']} rows, expected {stats['expected_rows']}"
        )
    if stats["n_null_id"] != 0 or stats["n_distinct"] != stats["n_rows"]:
        raise AssertionError("P1 violated: trip_id is not unique in the output")
    if stats["bad_published"] != 0:
        raise AssertionError(
            f"P2 violated: {stats['bad_published']} PUBLISHED rows have NULL driver_earning_cents"
        )
    if stats["bad_pending"] != 0:
        raise AssertionError(
            f"P2 violated: {stats['bad_pending']} PENDING rows have non-NULL driver_earning_cents"
        )
    if stats["bad_status"] != 0:
        raise AssertionError("Invalid load_status values in output")

    return result
# >>> PASTE END

# ---------------------------------------------------------------------
# REVIEW_NOTES — fill in BEFORE running anything.
# Severity order: runtime break > wrong results on dirty data
#               > production robustness > performance > portability > style
# ---------------------------------------------------------------------
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes: Correct, everything is ok
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes: line 499 using date_format and to_date is redundant here
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (cast, division, array index, element_at)?
#     notes: Line 604, I am not sure if DIV can prevent 0 divisor
# [ ] Production — walk P1/P2/P3 one at a time, each on its own line.
#     Re-run idempotent? Late data attributed to the event date? The same
#     metric computed the same way in every branch? Quality assertions there?
#     notes: LGTM 
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (reading-stage hypothesis only — verified with .explain() later,
#     never asserted from memory)
#     notes: LGTM
# [ ] Robustness — hardcoded values, assumptions not in the output contract?
#     notes: no
# [ ] Style/clarity — would you approve this in a real code review?
#     notes: yes
#
# VERDICT (commit before running): PASS  — because: no critical mistake here, everything looks good
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
    raw_trip = spark.createDataFrame(
        [
            ("T101", "D1", "2026-10-20 08:15:00", 2500, "2026-10-20"),
            ("T102", "D2", "2026-10-20 09:40:00", 4000, "2026-10-20"),
            ("T103", "D3", "2026-10-20 11:05:00", 1800, "2026-10-20"),
            ("T104", "D9", "2026-10-20 12:30:00", 3000, "2026-10-20"),
            ("T105", "D5", "2026-10-20 14:10:00", 2200, "2026-10-20"),
            ("T106", "D1", "2026-10-20 23:50:00", 1500, "2026-10-20"),
        ],
        schema="trip_id string, driver_id string, completed_ts string, "
               "fare_cents bigint, landing_date string",
    )
    pending_trip = spark.createDataFrame(
        [
            ("T085", "D8", "2026-10-18 17:20:00", 2000, "2026-10-18"),
            ("T090", "D4", "2026-10-19 21:00:00", 3500, "2026-10-19"),
        ],
        schema="trip_id string, driver_id string, completed_ts string, "
               "fare_cents bigint, first_seen_date string",
    )
    dim_driver = spark.createDataFrame(
        [
            ("D1", "F1"),
            ("D2", "F2"),
            ("D3", "F9"),
            ("D4", "F1"),
            ("D5", None),
        ],
        schema="driver_id string, fleet_id string",
    )
    fleet_commission = spark.createDataFrame(
        [
            ("F1", 20),
            ("F2", 25),
            ("F3", 15),
        ],
        schema="fleet_id string, commission_pct int",
    )

    expected = [
        ("T085", "D8", None, "2026-10-18", 2000, None, "2026-10-18", "PENDING"),
        ("T090", "D4", "F1", "2026-10-19", 3500, 2800, "2026-10-19", "PUBLISHED"),
        ("T101", "D1", "F1", "2026-10-20", 2500, 2000, "2026-10-20", "PUBLISHED"),
        ("T102", "D2", "F2", "2026-10-20", 4000, 3000, "2026-10-20", "PUBLISHED"),
        ("T103", "D3", "F9", "2026-10-20", 1800, None, "2026-10-20", "PENDING"),
        ("T104", "D9", None, "2026-10-20", 3000, None, "2026-10-20", "PENDING"),
        ("T105", "D5", None, "2026-10-20", 2200, None, "2026-10-20", "PENDING"),
        ("T106", "D1", "F1", "2026-10-20", 1500, 1200, "2026-10-20", "PUBLISHED"),
    ]

    check(build_trip_payout_resolution_dsl(
              raw_trip, pending_trip, dim_driver, fleet_commission),
          expected, "DSL")
    check(build_trip_payout_resolution_sql(
              spark, raw_trip, pending_trip, dim_driver, fleet_commission),
          expected, "SQL")

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    check(ai_build_trip_payout_resolution_dsl(
              raw_trip, pending_trip, dim_driver, fleet_commission),
          expected, "AI-DSL (post-review verification)")
    check(ai_build_trip_payout_resolution_sql(
              spark, raw_trip, pending_trip, dim_driver, fleet_commission),
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
# Concept takeaways for this problem: see refs/day28_trip_payout_ref.md.
