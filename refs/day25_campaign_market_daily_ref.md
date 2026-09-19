# Day 25 — agg_campaign_market_daily — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

ETL layer : L3 serving (aggregation)
Domain    : ad delivery
Failure mode (the trap axis — never stated in the day file):
            **boundary closure** — timezone attribution across the local
            midnight, applied to the grain but not to the scope filter

## The trap

**What it is:** `local_date` and the flight window live on the **same clock**
(the market's), but `event_ts_utc` lives on another. The grain forces you to
convert; the *scope filter* does not force anything, and a filter written
against the raw UTC timestamp — or against `to_date(event_ts_utc)` — silently
uses a different day boundary than the one the contract defines. A3 is
written in local time; the feed is written in UTC; nothing in the code
complains when you compare them.

**Why a naive solution passes anyway:** every event whose UTC date happens to
equal its market-local date is scoped identically under both clocks, and that
is 7 of the 8 feed rows. More importantly, the two events that *do* cross
local midnight forward (E07 DE 22:30Z → local 09-11, E08 JP 20:00Z → local
09-11) are **inside** their flight window on both clocks, so they produce a
visible, correct-looking demonstration that "the timezone conversion works" —
their `local_date` is right in the output, and they were never at risk. The
grain column is converted correctly in every wrong solution; only the
predicate is on the wrong clock.

**Which row exposes it:** `ad_events.E01` — C1 / P_JP_1 / IMPRESSION /
`2026-09-09 15:30:00` / 12.0. Asia/Tokyo is UTC+9, so its local time is
2026-09-10 00:30 and its `local_date` is **2026-09-10** — the first day of
the C1/JP flight (`2026-09-10` .. `2026-09-11`). Its UTC date, 2026-09-09, is
one day *before* the flight starts, so any predicate built on the UTC value
drops it.

Failure shape: exactly one output row moves, and only in two of its six
cells. `(C1, JP, 2026-09-10)` reads `n_impressions = 1, spend_usd = 8.0`
instead of `2 / 20.0`. The row still exists (E02 keeps it alive), the row
**count stays 5**, the grain stays unique, there is no NULL anywhere, and
`n_clicks` on that same row is correct. 1 impression at $8.00 is a
perfectly plausible ad-delivery day; nothing in the output's shape says
anything is missing.

Note the asymmetry that was designed in: the mirror-image error
(over-inclusion — keeping an event whose local date falls *outside* the
flight) would create an extra output row and be caught by row count. That
variant was deliberately kept out of the data, so the only surviving symptom
is a number getting smaller.

**Reading-time tell:** it is on the two columns the day file explicitly tells
the reader are on different clocks, and it needs no scanning for a
coincidence:

- A2 says `local_date` is determined by `event_ts_utc` + `tz_name` **and
  nothing else**.
- `campaign_flight`'s note says both flight dates are calendar dates **in the
  market's own timezone**.
- A3 compares `local_date` against those flight dates.

So the reviewer's question is a single one, answerable without running:
*which column does the scope predicate read?* If it reads `event_ts_utc` (raw
or `to_date`-ed), the predicate compares a UTC date to a local date. That is
a type error the type system cannot see. The confirming data point is one
row: E01 is the only feed row whose UTC date differs from its market-local
date **and** whose flight boundary sits between the two.

**Which pipeline stage it lives in:** stage 3, **scope to flight window** —
not stage 2 (local-date attribution), which every wrong solution gets right.

---

## Production constraints — audit

| # | Constraint | Satisfied by | How it fails if ignored |
|---|---|---|---|
| P1 | Re-running this job on the same four inputs must produce byte-identical output. | The whole job is join + deterministic expression + `groupBy` aggregation — no `row_number` over a tie, no `first`/`last` without an order, no `collect_list` fed into an order-dependent expression. Idempotency here is achieved by *not introducing* nondeterminism, which is why it deserves an explicit review line rather than a code line. | Any `dropDuplicates` / `first()` introduced "to be safe" on a feed with no duplicates would make the survivor arbitrary. The tests can never show it (each output cell is an order-insensitive `count`/`sum`), so P1 is graded by reading only. |
| P2 | Assert exactly one row per (campaign_id, market_code, local_date), and no NULL in `campaign_id` / `market_code` / `local_date`. | Two assertions after the final `groupBy`: `out.count() == out.select(keys).distinct().count()`, and `out.filter(key.isNull() for any key).count() == 0`. | **Both assertions pass under the trap.** The grain is constructively unique because it is the `groupBy` key, and the three key columns come from inner joins so they cannot be NULL. Grade P2 on *whether the assertion exists*, never on whether it caught anything — it structurally cannot catch this (same finding as Day 23 and Day 24). |
| P3 | Read only the columns this job needs from the two config tables: `channel` and `reporting_currency` must not enter the pipeline. | An explicit `.select("placement_id", "market_code")` and `.select("market_code", "tz_name")` before the joins; in SQL, a CTE that projects those columns, or a named column list instead of `SELECT *`. | No observable symptom on 19 rows. Classify a violation as **production robustness** (and specifically as an inconsistency when one of the two branches satisfies it and the other does not — the Day 24 shape), never as performance, unless an `.explain()` actually shows a wider scan mattering. |

---

## Pipeline stages

1. **Resolve market** — join `ad_events` to `placement_market` on
   `placement_id` (broadcast-sized), projecting only `placement_id` and
   `market_code`.
2. **Attribute to local date** — join to `market_config` on `market_code` for
   `tz_name`, then
   `date_format(from_utc_timestamp(event_ts_utc, tz_name), 'yyyy-MM-dd')`.
3. **Scope to the flight window** — join to `campaign_flight` on
   `(campaign_id, market_code)` and keep rows where
   `flight_start_date <= local_date <= flight_end_date`. **The trap lives
   here.**
4. **Aggregate** — `groupBy(campaign_id, market_code, local_date)` with
   conditional aggregation: `count(when(type='IMPRESSION'))`,
   `count(when(type='CLICK'))`, `sum(spend_usd)`.
5. **Assert and project** — the two P2 assertions, then the six contract
   columns in order with `n_impressions` / `n_clicks` as `bigint`.

---

## Part 4 — Reference answers

### Route A — convert the event, filter in local time

The direct reading of the contract: move the event onto the market's clock,
then compare like with like.

```python
def ref_build_agg_campaign_market_daily_dsl_a(
    ad_events, placement_market, market_config, campaign_flight
):
    pm = placement_market.select("placement_id", "market_code")
    mc = market_config.select("market_code", "tz_name")

    scoped = (
        ad_events
        .join(F.broadcast(pm), "placement_id", "inner")
        .join(F.broadcast(mc), "market_code", "inner")
        .withColumn(
            "local_date",
            F.date_format(
                F.from_utc_timestamp(F.col("event_ts_utc"), F.col("tz_name")),
                "yyyy-MM-dd",
            ),
        )
        .join(F.broadcast(campaign_flight), ["campaign_id", "market_code"], "inner")
        .filter(
            (F.col("local_date") >= F.col("flight_start_date"))
            & (F.col("local_date") <= F.col("flight_end_date"))
        )
    )

    out = (
        scoped
        .groupBy("campaign_id", "market_code", "local_date")
        .agg(
            F.count(F.when(F.col("event_type") == "IMPRESSION", F.lit(1)))
             .cast("bigint").alias("n_impressions"),
            F.count(F.when(F.col("event_type") == "CLICK", F.lit(1)))
             .cast("bigint").alias("n_clicks"),
            F.sum("spend_usd").alias("spend_usd"),
        )
        .select(
            "campaign_id", "market_code", "local_date",
            "n_impressions", "n_clicks", "spend_usd",
        )
    )

    # P2 — assert before publishing.
    keys = ["campaign_id", "market_code", "local_date"]
    out.cache()
    n = out.count()
    assert n == out.select(*keys).distinct().count(), "grain not unique"
    assert out.filter(
        F.col("campaign_id").isNull()
        | F.col("market_code").isNull()
        | F.col("local_date").isNull()
    ).count() == 0, "NULL in grain key"
    return out
```

```sql
-- ref_build_agg_campaign_market_daily_sql_a
WITH pm AS (
    SELECT placement_id, market_code FROM placement_market
),
mc AS (
    SELECT market_code, tz_name FROM market_config
),
attributed AS (
    SELECT
        e.campaign_id,
        pm.market_code,
        e.event_type,
        e.spend_usd,
        date_format(
            from_utc_timestamp(e.event_ts_utc, mc.tz_name), 'yyyy-MM-dd'
        ) AS local_date
    FROM ad_events e
    JOIN pm ON e.placement_id = pm.placement_id
    JOIN mc ON pm.market_code = mc.market_code
),
scoped AS (
    SELECT a.*
    FROM attributed a
    JOIN campaign_flight f
      ON a.campaign_id = f.campaign_id
     AND a.market_code = f.market_code
    WHERE a.local_date >= f.flight_start_date
      AND a.local_date <= f.flight_end_date
)
SELECT
    campaign_id,
    market_code,
    local_date,
    CAST(count_if(event_type = 'IMPRESSION') AS BIGINT) AS n_impressions,
    CAST(count_if(event_type = 'CLICK')      AS BIGINT) AS n_clicks,
    sum(spend_usd)                                      AS spend_usd
FROM scoped
GROUP BY campaign_id, market_code, local_date
```

### Route B — convert the window, filter in UTC

The pushdown-friendly shape, and the one worth knowing because a real job
reads a UTC-partitioned feed. It is correct **only** if the window is made
half-open on the far end: the flight ends on the last *local* day, so the
scope ends at local `flight_end_date + 1 day` at 00:00:00, exclusive.

```python
    flight_utc = (
        campaign_flight
        .join(F.broadcast(mc), "market_code", "inner")
        .withColumn(
            "scope_from_utc",
            F.to_utc_timestamp(
                F.concat_ws(" ", F.col("flight_start_date"), F.lit("00:00:00")),
                F.col("tz_name"),
            ),
        )
        .withColumn(
            "scope_to_utc",                       # exclusive
            F.to_utc_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format(
                        F.date_add(F.to_date("flight_end_date"), 1), "yyyy-MM-dd"
                    ),
                    F.lit("00:00:00"),
                ),
                F.col("tz_name"),
            ),
        )
    )
    # ... join on (campaign_id, market_code) and filter
    #     event_ts_utc >= scope_from_utc AND event_ts_utc < scope_to_utc
    # `local_date` is still computed per event for the grain.
```

**The tradeoff is not a shuffle story.** Both routes join the same four
relations on the same keys and aggregate on the same grain; the flight table
is 4 rows either way. What actually differs:

- Route A compares two `string` dates; Route B compares `timestamp` to
  `timestamp`, which is what lets a real reader skip UTC partitions. On this
  in-memory harness that advantage is worth exactly nothing.
- Route B has **two** places to get the boundary wrong instead of one: the
  `+1 day` and the strictness of `<`. Writing `<=` on `scope_to_utc` admits
  an event at exactly local midnight of the day after the flight; writing
  `<= to_utc_timestamp(flight_end_date 00:00)` silently drops the entire
  last flight day. Neither is visible in this test data — no event sits on a
  C1/JP or C2/DE end-of-flight boundary.
- Route B also needs `market_config` joined into the **dimension** rather
  than only into the fact stream, which is the structural reason a "push the
  filter down" refactor tends to leak: the window is not a constant, it is a
  per-market value.

Route A is the reference. Route B is the one to be able to defend in an
interview, with the half-open boundary named out loud.

### The wrong route, for Stage 5 comparison

```python
    # stage 3, written against the wrong clock — this is the trap
    .filter(
        (F.to_date("event_ts_utc") >= F.col("flight_start_date"))
        & (F.to_date("event_ts_utc") <= F.col("flight_end_date"))
    )
```

Both naive variants — `to_date(event_ts_utc)` and the bare string comparison
`event_ts_utc >= flight_start_date` — drop **E01 and only E01** on this data,
and produce the identical wrong output. The bare-string variant is the more
interesting one: `'2026-09-10 10:00:00' <= '2026-09-10'` is **false**
lexicographically (the shorter string is the smaller one), so that variant
would also silently delete every event on the last flight day. It does not
here only because no kept event's UTC date equals its flight's end date.
That is data luck, not correctness — say so if it shows up.

---

## Part 5 — Concept takeaways

- **A timezone conversion has two customers, and only one of them is loud.**
  The grain column forces the conversion — you cannot emit `local_date`
  without it. Every predicate, join key and window boundary that mentions a
  date is a second, silent customer, and nothing in Spark's type system
  connects the two. The review question is not "did they convert?" but "did
  they convert *everywhere the contract's clock is assumed*?"
- **A date is a clock-relative label, not a point in time.** UTC date and
  local date agree for `24 - |offset|` hours out of every 24, which is why
  attribution bugs are ~70–90% coincidentally correct and always look like
  an unlucky rounding rather than a systematic error.
- **A local calendar day is a half-open UTC interval**, `[local 00:00,
  local+1d 00:00)`, and its *width* is 24h only when no DST transition falls
  inside it. Pushing a local-date filter down to UTC therefore needs
  `to_utc_timestamp(end_date + 1 day)` with a strict `<` — the inclusive
  local `<=` end has no inclusive UTC equivalent.
- `from_utc_timestamp(ts, tz)` and `to_utc_timestamp(ts, tz)` are inverses,
  and both accept a **Column** for `tz` in DSL and SQL alike — the per-row
  timezone is the normal case, not an exotic one. A solution that hardcodes
  one zone string is not merely inflexible, it is wrong the moment a second
  market exists.
- **String timestamps compare correctly to string timestamps and incorrectly
  to string dates.** `'2026-09-10 10:00:00' <= '2026-09-10'` is false. Fixed
  width buys lexicographic ordering *within one format*, and crossing formats
  is where it silently stops holding.
- **Conditional `count` counts non-NULL**, so
  `count(when(cond, 1))` / `count_if(cond)` yield 0 for a group with no
  matching row, while `sum(when(cond, 1))` yields NULL. The expected output
  has one row with `n_impressions = 0` and one with `n_clicks = 0`
  specifically to exercise this (re-confirms Day 10 / Day 12).
- **A self-assertion on the grain cannot see a filter bug.** The grain is the
  `groupBy` key, so uniqueness is constructive and the non-NULL check only
  restates the inner joins. P2 is satisfied and useless here — the third
  consecutive day (23, 24, 25) where the job's own quality gate publishes the
  wrong number. The general rule: an assertion can only observe what survived
  into the output, never what was dropped on the way in. A row-count or
  input-conservation assertion is a *different* class of check, and no side
  has written one yet.
- Whitelist techniques exercised: UTC→local bucketing, event-date
  attribution, broadcast joins against hand-maintained config, conditional
  aggregation, `COUNT(*)` vs `COUNT(col)`, column pruning.

## Physical plan notes

**NOT MEASURED.** Run `.explain()` at Stage 5 before making any claim.
Hypotheses to test, not to assert:

- Route A and Route B are expected to have the **same** Exchange count — four
  broadcast-eligible dimension joins and one final aggregation. If they
  differ, the difference is in where `market_config` is joined (fact stream
  vs flight dimension), not in the filter.
- With all three config tables well under the broadcast threshold, expect
  `BroadcastHashJoin` throughout and a single Exchange for the final
  `HashAggregate`. Confirm with an actual run before writing anything down —
  and if Exchange counts match, name the real difference instead of inventing
  a shuffle gap.
- `count_if` vs `count(when(...))` should plan identically; check rather than
  assume.

## Echoes

- **Day 11** — UTC→local bucketing and date-dimension work. This day reuses
  the primitive on a serving-layer job instead of teaching it, and adds the
  half-open UTC interval that Day 11 never needed.
- **Day 23 / Day 24** — third instance of "the job's P2 assertions are all
  green while the numbers are wrong". Day 23: uniqueness assertion on a
  `groupBy` product is tautological. Day 24: a complete P2 still cannot see
  duplicates the final `groupBy` already folded. Day 25 extends the rule to
  *dropped* rows. This is now a standing review heuristic, not a per-day
  observation.
- **Day 17 / Day 23** — half-open interval closing. Day 17 had `[from, to)`
  on SCD2 versions, Day 23 on an effective-dated dimension; here the
  half-open interval appears as the *UTC image of a local calendar day*,
  which is the same idea with the boundary generated rather than stored.
- **Day 20** — the string-vs-parsed comparison hazard, in a new place:
  Day 20 was about parse paths (`'12.0'` → INT), this is about comparing two
  string formats that are individually well-ordered.
- **Day 23 / Day 24 contract drift** — `n_impressions` / `n_clicks` are
  declared `bigint`. The user has cast counts to `int` against a `bigint`
  contract two days running and `check()` cannot see it. Grade it explicitly
  this time.
