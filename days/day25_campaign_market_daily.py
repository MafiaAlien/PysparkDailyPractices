"""
=====================================================================
PySpark Daily Practice — ETL Scenario Template (Day 22 onward)
=====================================================================
Day 25 — agg_campaign_market_daily  (Medium-Hard)

ETL layer : L3 serving (aggregation)
Domain    : ad delivery

BUSINESS CONTEXT
----------------
The ad server delivers a campaign's creatives through placements, and each
placement sits in exactly one delivery market. Every impression and every
click is logged with a UTC timestamp. Advertisers, however, buy and read
their campaigns on the calendar of the market they are delivering into: a
Tokyo advertiser's "September 10" is not a London advertiser's "September
10". This job runs daily and publishes the campaign reporting table the
advertiser-facing dashboard reads — one row per campaign, market and local
delivery day. Nothing downstream re-reads the raw event feed.

INPUT TABLES
------------
ad_events(event_id: string, campaign_id: string, placement_id: string,
          event_type: string, event_ts_utc: string, spend_usd: double)
    -- append-only event feed from the ad server. `event_type` is either
       'IMPRESSION' or 'CLICK'. `event_ts_utc` is the moment the ad server
       served the impression or received the click, in UTC. `spend_usd` is
       the cost the ad server booked for that event; clicks are not billed
       separately and carry 0.0.

placement_market(placement_id: string, market_code: string, channel: string)
    -- hand-maintained config table, one row per placement. Ad ops keeps it
       up to date when a placement is set up. `channel` is a trafficking
       attribute; reporting consumers have no use for it.

market_config(market_code: string, tz_name: string,
              reporting_currency: string)
    -- hand-maintained config table, one row per market. `tz_name` is the
       IANA timezone the market reports on. `reporting_currency` is a
       billing attribute; reporting consumers have no use for it.

campaign_flight(campaign_id: string, market_code: string,
                flight_start_date: string, flight_end_date: string)
    -- hand-maintained config table, one row per (campaign, market). The
       flight is the window the advertiser bought. **flight_start_date and
       flight_end_date are calendar dates in that market's own timezone**,
       both ends inclusive — they are what the advertiser signed, and the
       advertiser signed them in local time.

Timestamps are ISO-8601 STRINGS ('yyyy-MM-dd HH:mm:ss') and dates are
'yyyy-MM-dd' STRINGS. They are fixed-width, so lexicographic order equals
chronological order within one clock. The harness pins
spark.sql.session.timeZone to UTC, so `event_ts_utc` reads back as the UTC
wall clock it claims to be. This day is about the pipeline, not about date
parsing.

ad_events:

    +----------+-------------+--------------+------------+---------------------+-----------+
    | event_id | campaign_id | placement_id | event_type | event_ts_utc        | spend_usd |
    +----------+-------------+--------------+------------+---------------------+-----------+
    | E01      | C1          | P_JP_1       | IMPRESSION | 2026-09-09 15:30:00 |      12.0 |
    | E02      | C1          | P_JP_2       | IMPRESSION | 2026-09-10 03:00:00 |       8.0 |
    | E03      | C1          | P_JP_1       | CLICK      | 2026-09-10 03:05:00 |       0.0 |
    | E04      | C1          | P_US_1       | IMPRESSION | 2026-09-10 05:00:00 |      15.0 |
    | E05      | C1          | P_US_1       | CLICK      | 2026-09-10 05:05:00 |       0.0 |
    | E06      | C2          | P_DE_1       | IMPRESSION | 2026-09-10 10:00:00 |       5.0 |
    | E07      | C2          | P_DE_1       | CLICK      | 2026-09-10 22:30:00 |       0.0 |
    | E08      | C1          | P_JP_2       | IMPRESSION | 2026-09-10 20:00:00 |      10.0 |
    +----------+-------------+--------------+------------+---------------------+-----------+

    Note: the feed is shown in the order the ad server appended it. Every
    `event_ts_utc` in this table is UTC; not one of them is a local time.

placement_market:

    +--------------+-------------+----------+
    | placement_id | market_code | channel  |
    +--------------+-------------+----------+
    | P_JP_1       | JP          | VIDEO    |
    | P_JP_2       | JP          | DISPLAY  |
    | P_US_1       | US_W        | VIDEO    |
    | P_DE_1       | DE          | DISPLAY  |
    +--------------+-------------+----------+

market_config:

    +-------------+---------------------+--------------------+
    | market_code | tz_name             | reporting_currency |
    +-------------+---------------------+--------------------+
    | JP          | Asia/Tokyo          | JPY                |
    | US_W        | America/Los_Angeles | USD                |
    | DE          | Europe/Berlin       | EUR                |
    +-------------+---------------------+--------------------+

campaign_flight:

    +-------------+-------------+-------------------+-----------------+
    | campaign_id | market_code | flight_start_date | flight_end_date |
    +-------------+-------------+-------------------+-----------------+
    | C1          | JP          | 2026-09-10        | 2026-09-11      |
    | C1          | US_W        | 2026-09-09        | 2026-09-11      |
    | C2          | DE          | 2026-09-10        | 2026-09-11      |
    | C2          | JP          | 2026-09-01        | 2026-09-05      |
    +-------------+-------------+-------------------+-----------------+

    Note: a (campaign, market) pair the advertiser bought may have delivered
    nothing at all. Both flight dates are in the market's own timezone, and
    both ends are inclusive.

OUTPUT CONTRACT
---------------
Table    : agg_campaign_market_daily
Grain    : one row per (campaign_id, market_code, local_date), over in-scope
           events only
Columns  : campaign_id: string, market_code: string, local_date: string,
           n_impressions: bigint, n_clicks: bigint, spend_usd: double
           (this exact order)
Ordering : irrelevant — check() sorts both sides

Attribution and scoping rules:
  A1  An event's `market_code` is the market of its placement, from
      `placement_market`. Every placement in the feed appears there.
  A2  An event's `local_date` is the calendar date of `event_ts_utc` read on
      the clock of that market — i.e. `local_date` is determined by exactly
      two columns, the event's own `event_ts_utc` and its market's
      `tz_name`, and by nothing else.
  A3  An event is in scope only if its `local_date` falls inside the flight
      the advertiser bought for that (campaign, market):
      flight_start_date <= local_date <= flight_end_date, both ends
      inclusive. Out-of-scope events are dropped; they are delivery the
      advertiser did not buy and must never reach the dashboard.

Column semantics:
  local_date      the market-local calendar date, formatted 'yyyy-MM-dd'
  n_impressions   how many in-scope events of type 'IMPRESSION' the campaign
                  had in that market on that local date
  n_clicks        how many in-scope events of type 'CLICK'
  spend_usd       the total `spend_usd` over all in-scope events of that
                  (campaign, market, local_date), impressions and clicks
                  alike

A (campaign_id, market_code, local_date) with no in-scope event produces no
row — this job does not gap-fill the flight window. A row that does exist may
legitimately carry 0 impressions or 0 clicks.

Expected:

    +-------------+-------------+------------+---------------+----------+-----------+
    | campaign_id | market_code | local_date | n_impressions | n_clicks | spend_usd |
    +-------------+-------------+------------+---------------+----------+-----------+
    | C1          | JP          | 2026-09-10 |             2 |        1 |      20.0 |
    | C1          | JP          | 2026-09-11 |             1 |        0 |      10.0 |
    | C1          | US_W        | 2026-09-09 |             1 |        1 |      15.0 |
    | C2          | DE          | 2026-09-10 |             1 |        0 |       5.0 |
    | C2          | DE          | 2026-09-11 |             0 |        1 |       0.0 |
    +-------------+-------------+------------+---------------+----------+-----------+

PRODUCTION CONSTRAINTS
----------------------
P1. Re-running this job on the same four inputs must produce byte-identical
    output. The batch is replayed whenever the scheduler retries.
P2. Before returning, the job must assert its own output: exactly one row per
    (campaign_id, market_code, local_date), and no NULL in `campaign_id` /
    `market_code` / `local_date`. A silent bad publish is worse than a
    failed job.
P3. Read only the columns this job needs from the two config tables.
    `channel` and `reporting_currency` are operational metadata and must not
    enter the pipeline.

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
refs/day25_campaign_market_daily_ref.md. Do not open it before Stage 5.
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
def build_agg_campaign_market_daily_dsl(
    ad_events: DataFrame,
    placement_market: DataFrame,
    market_config: DataFrame,
    campaign_flight: DataFrame,
) -> DataFrame:
    """Daily campaign delivery report, bucketed on each market's own calendar.

    Hint: every row of the feed carries one clock; the contract is written on
    another.
    """
    # TODO: implement
    df_ad_local_time =(
        ad_events.join(
            placement_market,
            "placement_id",
            "left"
        ).join(
            market_config,
            "market_code",
            "left"
        ).withColumn(
            'local_date',
            F.date_format(F.from_utc_timestamp(F.col("event_ts_utc"), F.col("tz_name")), "yyyy-MM-dd")
        ).select(
            "campaign_id",
            "market_code",
            "event_type",
            F.col("local_date"),
            "spend_usd"
        )
    )

    join_cond = (df_ad_local_time.campaign_id == campaign_flight.campaign_id ) & (df_ad_local_time.market_code == campaign_flight.market_code) 

    agg_campaign_market_daily = (
        df_ad_local_time.join(
            campaign_flight,
            join_cond,
            "left"
        ).drop(
            "campaign_flight.campaign_id", 
            "campaign_flight.market_code"
        ).filter(
            df_ad_local_time.local_date.between(F.col("flight_start_date"), F.col("flight_end_date")) 
        ).groupBy(
            df_ad_local_time.campaign_id,
            df_ad_local_time.market_code,
            "local_date"
        ).agg(
            F.count(F.when(F.col("event_type") == "IMPRESSION", F.lit(1))).cast("int").alias("n_impressions"),
            F.count(F.when(F.col("event_type") == "CLICK", F.lit(1))).cast("int").alias("n_clicks"),
            F.sum(F.coalesce(F.col("spend_usd"), F.lit(0.0))).cast("double").alias("spend_usd")
        ).select(
            "campaign_id",
            "market_code",
            "local_date",
            "n_impressions",
            "n_clicks",
            "spend_usd"
        )
    )
    # Constraits
    # exactly once execution
    exactly_once_cnt = agg_campaign_market_daily.groupBy(
        "campaign_id",
        "market_code",
        "local_date",
    ).agg(
        F.count(F.lit(1)).alias("dup_cnt")
    ).filter(F.col("dup_cnt") > 1)
    assert (exactly_once_cnt is not None), f"agg_campaign_market_daily has duplicated rows per (campaign_id, market_code, local_date)"

    # check null key 
    null_cnt = agg_campaign_market_daily.filter(
        F.col("campaign_id").isNull() | F.col("market_code").isNull() | F.col("local_date").isNull()
    ).count()
    assert (null_cnt == 0), f"agg_campaign_market_daily: {null_cnt} row(s) with NULL campaign_id/market_code/local_date"

    return agg_campaign_market_daily

# ------------------------------ 1b. Spark SQL ------------------------
def build_agg_campaign_market_daily_sql(
    spark: SparkSession,
    ad_events: DataFrame,
    placement_market: DataFrame,
    market_config: DataFrame,
    campaign_flight: DataFrame,
) -> DataFrame:
    """Same job, Spark SQL.

    Hint: every row of the feed carries one clock; the contract is written on
    another.
    """
    ad_events.createOrReplaceTempView("ad_events")
    placement_market.createOrReplaceTempView("placement_market")
    market_config.createOrReplaceTempView("market_config")
    campaign_flight.createOrReplaceTempView("campaign_flight")

    sql = """
        -- write your SQL here
        WITH join_campaign_placement_market AS (
            SELECT 
                ae.campaign_id,
                pm.market_code,
                event_type,
                date_format(from_utc_timestamp(event_ts_utc, tz_name),'yyyy-MM-dd') AS local_date,
                spend_usd
            FROM 
                ad_events ae LEFT JOIN placement_market pm ON ae.placement_id = pm.placement_id 
                LEFT JOIN market_config mc ON pm.market_code = mc.market_code
        ),
        
        join_campaign_flight AS (
            SELECT 
                j.campaign_id,
                j.market_code,
                j.local_date,
                event_type,
                spend_usd
            FROM 
                join_campaign_placement_market j LEFT JOIN campaign_flight cl ON j.campaign_id = cl.campaign_id AND j.market_code = cl.market_code
            WHERE 
                j.local_date BETWEEN cl.flight_start_date AND flight_end_date
        )

        SELECT 
            campaign_id,
            market_code,
            local_date,
            CAST(COUNT(CASE WHEN event_type = 'IMPRESSION' THEN 1 END) AS BIGINT)AS n_impressions,
            CAST(COUNT(CASE WHEN event_type = 'CLICK' THEN 1 END) AS BIGINT) AS n_clicks,
            CAST(SUM(COALESCE(spend_usd, 0)) AS DOUBLE) AS spend_usd
        FROM 
            join_campaign_flight 
        GROUP BY 
            campaign_id,
            market_code,
            local_date

    """
    agg_campaign_market_daily = spark.sql(sql)

    # Constraits
    # exactly once execution
    exactly_once_cnt = agg_campaign_market_daily.groupBy(
        "campaign_id",
        "market_code",
        "local_date",
    ).agg(
        F.count(F.lit(1)).alias("dup_cnt")
    ).filter(F.col("dup_cnt") > 1)
    assert (exactly_once_cnt is not None), f"agg_campaign_market_daily has duplicated rows per (campaign_id, market_code, local_date)"

    # check null key 
    null_cnt = agg_campaign_market_daily.filter(
        F.col("campaign_id").isNull() | F.col("market_code").isNull() | F.col("local_date").isNull()
    ).count()
    assert (null_cnt == 0), f"agg_campaign_market_daily: {null_cnt} row(s) with NULL campaign_id/market_code/local_date"

    return agg_campaign_market_daily

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
def ai_build_agg_campaign_market_daily_dsl(
    ad_events: DataFrame,
    placement_market: DataFrame,
    market_config: DataFrame,
    campaign_flight: DataFrame,
) -> DataFrame:
    pm = placement_market.select("placement_id", "market_code")
    mc = market_config.select("market_code", "tz_name")
    fl = campaign_flight.select(
        "campaign_id", "market_code", "flight_start_date", "flight_end_date"
    )

    events = (
        ad_events.select(
            "campaign_id", "placement_id", "event_type", "event_ts_utc", "spend_usd"
        )
        .join(F.broadcast(pm), on="placement_id", how="inner")
        .join(F.broadcast(mc), on="market_code", how="inner")
        .withColumn(
            "local_date",
            F.date_format(
                F.from_utc_timestamp(
                    F.to_timestamp(F.col("event_ts_utc"), "yyyy-MM-dd HH:mm:ss"),
                    F.col("tz_name"),
                ),
                "yyyy-MM-dd",
            ),
        )
        .select("campaign_id", "market_code", "local_date", "event_type", "spend_usd")
    )

    scoped = events.join(
        F.broadcast(fl), on=["campaign_id", "market_code"], how="inner"
    ).filter(
        (F.col("local_date") >= F.col("flight_start_date"))
        & (F.col("local_date") <= F.col("flight_end_date"))
    )

    result = (
        scoped.groupBy("campaign_id", "market_code", "local_date")
        .agg(
            F.sum(F.when(F.col("event_type") == F.lit("IMPRESSION"), F.lit(1)).otherwise(F.lit(0)))
            .cast("bigint")
            .alias("n_impressions"),
            F.sum(F.when(F.col("event_type") == F.lit("CLICK"), F.lit(1)).otherwise(F.lit(0)))
            .cast("bigint")
            .alias("n_clicks"),
            F.coalesce(F.sum(F.col("spend_usd")), F.lit(0.0))
            .cast("double")
            .alias("spend_usd"),
        )
        .select(
            "campaign_id",
            "market_code",
            "local_date",
            "n_impressions",
            "n_clicks",
            "spend_usd",
        )
    )

    result = result.cache()

    null_keys = result.filter(
        F.col("campaign_id").isNull()
        | F.col("market_code").isNull()
        | F.col("local_date").isNull()
    ).count()
    if null_keys != 0:
        raise ValueError(
            "agg_campaign_market_daily: NULL grain key in output rows: %d" % null_keys
        )

    dup_keys = (
        result.groupBy("campaign_id", "market_code", "local_date")
        .count()
        .filter(F.col("count") > F.lit(1))
        .count()
    )
    if dup_keys != 0:
        raise ValueError(
            "agg_campaign_market_daily: duplicate (campaign_id, market_code, local_date) groups: %d"
            % dup_keys
        )

    return result


def ai_build_agg_campaign_market_daily_sql(
    spark: SparkSession,
    ad_events: DataFrame,
    placement_market: DataFrame,
    market_config: DataFrame,
    campaign_flight: DataFrame,
) -> DataFrame:
    ad_events.createOrReplaceTempView("ai_src_ad_events")
    placement_market.createOrReplaceTempView("ai_src_placement_market")
    market_config.createOrReplaceTempView("ai_src_market_config")
    campaign_flight.createOrReplaceTempView("ai_src_campaign_flight")

    result = spark.sql(
        """
        WITH pm AS (
            SELECT placement_id, market_code
            FROM ai_src_placement_market
        ),
        mc AS (
            SELECT market_code, tz_name
            FROM ai_src_market_config
        ),
        fl AS (
            SELECT campaign_id, market_code, flight_start_date, flight_end_date
            FROM ai_src_campaign_flight
        ),
        ev AS (
            SELECT
                e.campaign_id                                  AS campaign_id,
                pm.market_code                                 AS market_code,
                e.event_type                                   AS event_type,
                e.spend_usd                                    AS spend_usd,
                date_format(
                    from_utc_timestamp(
                        to_timestamp(e.event_ts_utc, 'yyyy-MM-dd HH:mm:ss'),
                        mc.tz_name
                    ),
                    'yyyy-MM-dd'
                )                                              AS local_date
            FROM ai_src_ad_events e
            JOIN pm ON e.placement_id = pm.placement_id
            JOIN mc ON pm.market_code = mc.market_code
        ),
        scoped AS (
            SELECT
                ev.campaign_id,
                ev.market_code,
                ev.local_date,
                ev.event_type,
                ev.spend_usd
            FROM ev
            JOIN fl
              ON ev.campaign_id = fl.campaign_id
             AND ev.market_code = fl.market_code
            WHERE ev.local_date >= fl.flight_start_date
              AND ev.local_date <= fl.flight_end_date
        )
        SELECT
            campaign_id,
            market_code,
            local_date,
            CAST(SUM(CASE WHEN event_type = 'IMPRESSION' THEN 1 ELSE 0 END) AS BIGINT) AS n_impressions,
            CAST(SUM(CASE WHEN event_type = 'CLICK' THEN 1 ELSE 0 END) AS BIGINT)      AS n_clicks,
            CAST(COALESCE(SUM(spend_usd), 0.0D) AS DOUBLE)                             AS spend_usd
        FROM scoped
        GROUP BY campaign_id, market_code, local_date
        """
    )

    result = result.cache()

    null_keys = result.filter(
        F.col("campaign_id").isNull()
        | F.col("market_code").isNull()
        | F.col("local_date").isNull()
    ).count()
    if null_keys != 0:
        raise ValueError(
            "agg_campaign_market_daily: NULL grain key in output rows: %d" % null_keys
        )

    dup_keys = (
        result.groupBy("campaign_id", "market_code", "local_date")
        .count()
        .filter(F.col("count") > F.lit(1))
        .count()
    )
    if dup_keys != 0:
        raise ValueError(
            "agg_campaign_market_daily: duplicate (campaign_id, market_code, local_date) groups: %d"
            % dup_keys
        )

    return result

# >>> PASTE END

# ---------------------------------------------------------------------
# REVIEW_NOTES — fill in BEFORE running anything.
# Severity order: runtime break > wrong results on dirty data
#               > production robustness > performance > portability > style
# ---------------------------------------------------------------------
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes:correct my solutions and AI's are almost same logic
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes: No, every API used correctly
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (cast, division, array index, element_at)?
#     notes: no, just coalesce for final sum, but I used coalesce here to avoid if all spend usd are null 
# [ ] Production — walk P1/P2/P3 one at a time, each on its own line.
#     Re-run idempotent? The same metric computed the same way in every
#     branch? Quality assertions there? Only the needed columns read?
#     notes: I am not pretty sure if it is idempotent, quality assertions are correct here and only needed columns read, 
#     this part AI's code is better than mine, because I did not select needed cols at begining 
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (reading-stage hypothesis only — verified with .explain() later,
#     never asserted from memory)
#     notes: no, AI's codes are pretty here because it knows to use broadcast for mannual config table or small table, which I ignored here
# [ ] Robustness — hardcoded values, assumptions not in the output contract?
#     notes: No LGTM and no any other comments for this part
# [ ] Style/clarity — would you approve this in a real code review?
#     notes: there's one code I want to say: for line 400 -407, AI transfer UTC time to stamp then to date, but I transfer to date directly, 
#     these codes are littl bit redundant I think
#
# VERDICT (commit before running): PASS / FAIL — because: I believe it will pass, no critical error and logic here
# ACTUAL RESULT (after Stage 4 run): all passed
# GAP ANALYSIS: did the run reveal anything the reading missed? no 


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

    ad_events = spark.createDataFrame(
        [
            ("E01", "C1", "P_JP_1", "IMPRESSION", "2026-09-09 15:30:00", 12.0),
            ("E02", "C1", "P_JP_2", "IMPRESSION", "2026-09-10 03:00:00", 8.0),
            ("E03", "C1", "P_JP_1", "CLICK", "2026-09-10 03:05:00", 0.0),
            ("E04", "C1", "P_US_1", "IMPRESSION", "2026-09-10 05:00:00", 15.0),
            ("E05", "C1", "P_US_1", "CLICK", "2026-09-10 05:05:00", 0.0),
            ("E06", "C2", "P_DE_1", "IMPRESSION", "2026-09-10 10:00:00", 5.0),
            ("E07", "C2", "P_DE_1", "CLICK", "2026-09-10 22:30:00", 0.0),
            ("E08", "C1", "P_JP_2", "IMPRESSION", "2026-09-10 20:00:00", 10.0),
        ],
        schema=(
            "event_id string, campaign_id string, placement_id string, "
            "event_type string, event_ts_utc string, spend_usd double"
        ),
    )
    placement_market = spark.createDataFrame(
        [
            ("P_JP_1", "JP", "VIDEO"),
            ("P_JP_2", "JP", "DISPLAY"),
            ("P_US_1", "US_W", "VIDEO"),
            ("P_DE_1", "DE", "DISPLAY"),
        ],
        schema="placement_id string, market_code string, channel string",
    )
    market_config = spark.createDataFrame(
        [
            ("JP", "Asia/Tokyo", "JPY"),
            ("US_W", "America/Los_Angeles", "USD"),
            ("DE", "Europe/Berlin", "EUR"),
        ],
        schema="market_code string, tz_name string, reporting_currency string",
    )
    campaign_flight = spark.createDataFrame(
        [
            ("C1", "JP", "2026-09-10", "2026-09-11"),
            ("C1", "US_W", "2026-09-09", "2026-09-11"),
            ("C2", "DE", "2026-09-10", "2026-09-11"),
            ("C2", "JP", "2026-09-01", "2026-09-05"),
        ],
        schema=(
            "campaign_id string, market_code string, "
            "flight_start_date string, flight_end_date string"
        ),
    )

    expected = [
        ("C1", "JP", "2026-09-10", 2, 1, 20.0),
        ("C1", "JP", "2026-09-11", 1, 0, 10.0),
        ("C1", "US_W", "2026-09-09", 1, 1, 15.0),
        ("C2", "DE", "2026-09-10", 1, 0, 5.0),
        ("C2", "DE", "2026-09-11", 0, 1, 0.0),
    ]

    check(
        build_agg_campaign_market_daily_dsl(
            ad_events, placement_market, market_config, campaign_flight
        ),
        expected,
        "DSL",
    )
    check(
        build_agg_campaign_market_daily_sql(
            spark, ad_events, placement_market, market_config, campaign_flight
        ),
        expected,
        "SQL",
    )

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    check(ai_build_agg_campaign_market_daily_dsl(
              ad_events, placement_market, market_config, campaign_flight),
          expected, "AI-DSL (post-review verification)")
    check(ai_build_agg_campaign_market_daily_sql(
              spark, ad_events, placement_market, market_config,
              campaign_flight),
          expected, "AI-SQL (post-review verification)")

    spark.stop()


# #####################################################################
# ##   PART 5 — Review takeaways (fill at Stage 5, before /review)   ##
# #####################################################################
# - What did the AI get wrong, or suspiciously right?: No
# - Which production constraint (P#) did either side miss, and why was it
#   missable by reading?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem:
# see refs/day25_campaign_market_daily_ref.md.
