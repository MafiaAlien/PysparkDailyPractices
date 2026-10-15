# Day 27 — fct_app_session — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

ETL layer : L2 detail modeling
Domain    : mobile app clickstream
Failure mode (the trap axis — never stated in the day file):
            boundary closure — session boundary: **calendar midnight is not a
            session boundary.** A session is closed only by a > 30 min gap;
            the date is an attribute of the session, never part of its key.

## The trap

**What it is:** D1's session runs 23:48 → 23:55 → 00:09 across UTC midnight,
with 7- and 14-minute gaps. It is one session, dated 2026-10-07, with
`n_events = 3` and `duration_sec = 1260`. Any route that lets an
**event-level date** (`to_date(event_ts)`) into the session key splits it in
two. Three ways that happens:
- the window is `partitionBy(device_id, event_date)`, so `lag` returns NULL
  at 00:09 and a new session starts;
- the rollup is `groupBy(device_id, session_seq, to_date(event_ts))`, because
  "every output dimension goes into the groupBy";
- `session_id` is built from the event's own date and then used as the group
  key.

**Why a naive solution passes anyway:** every session that does not cross
midnight comes out right under all three variants. The decoy is **D2**:
23:20 then 00:05. Its gap is 45 minutes, so it is genuinely **two** sessions.
A midnight cut produces exactly the correct answer for D2. A reader who
notices "D2 straddles midnight and came out right" gets visible evidence that
midnight is handled. D2 is right because the gap closed it, not because
midnight did. E02 (`push_received`) sits *inside* D1's session, so filtering
before or after sessionizing gives the same boundaries. Only `n_events`
depends on the filter, and that rule is stated openly.

**Which row exposes it:** `raw_app_event` **E04** (D1, `2026-10-08 00:09:00`).
It is the only engagement event whose UTC date differs from its session's
start date. Measured naive output (both the groupBy and the window variants):

```
('D1-20261007234800', 'D1', 'iOS', '2026-10-07', '2026-10-07 23:48:00', '2026-10-07 23:55:00', 2, 420)
('D1-20261008000900', 'D1', 'iOS', '2026-10-08', '2026-10-08 00:09:00', '2026-10-08 00:09:00', 1, 0)
... D2 x2, D3 identical to expected
```

That is 5 rows instead of 4. The row count catches it in `check()`.

**P2 sees it only in one variant** (an important sub-finding):
- **Measured.** When `session_id` is built *after* the rollup from
  `min(ts)`, the two fragments get **different** ids. Uniqueness passes.
  Conservation also passes (2 + 1 = 3, so `sum(n_events)` is still 7). All P2
  assertions are green while the job publishes the wrong rows. This repeats
  Days 23/24/25: an assertion only sees what survives into the output.
- **Derived, not executed.** When `session_id` is computed *per event before*
  the rollup (e.g. `first(ts) OVER (device, session_seq)` with correct
  sessionization) and the event date then joins the groupBy keys, both
  fragments carry **the same** id. The uniqueness assertion fires.

So P2 is a real gate on this day. It catches the inner-join-to-registry
route (D3 vanishes: 6 ≠ 7) and one of the trap variants, but not the most
common trap variant.

**Reading-time tell:** both places are stated in the problem text, and
neither depends on scanning the data:
1. The session definition has exactly one closing condition, "a gap of MORE
   than 30 minutes". Nothing in it says "or a new day".
2. `session_date` is defined as the date of `session_start_ts`. That makes it
   a **function of the session**, so it can only be computed after the
   session exists.

The review question: **do the window partition keys and the groupBy keys
contain anything other than `device_id` and the session label?** If a date
appears in either, the job cuts sessions at midnight.

**Which pipeline stage it lives in:** **stage 3 (sessionize)** for the window
variant, **stage 4 (session rollup)** for the groupBy and session_id
variants. Stages 1, 2 and 5 cannot cause it.

---

## Production constraints — audit

| # | Constraint | Satisfied by | How it fails if ignored |
|---|---|---|---|
| P1 | Re-running this job on the same three inputs must produce byte-identical output. session_id is a pure function of the session's own events — never of run time, row order, or a generated sequence. | `session_id = concat(device_id, '-', from_unixtime(start_sec, 'yyyyMMddHHmmss'))`. The window order is `(ts_sec, event_id)`, a total order, because event_id is unique. | `monotonically_increasing_id()` / `uuid()` / `current_timestamp()` in the id gives different ids on every run, and a downstream join on session_id breaks silently. On this data, `orderBy(ts_sec)` alone is still deterministic: there are no tied timestamps per device, and with ties `lag` would give a gap of 0 either way. The `event_id` tiebreak is cheap insurance, not a fix for a measured bug. **The test data cannot tell the two apart. Don't credit it.** |
| P2 | Assert: session_id unique; sum(n_events) = engagement events in raw_app_event; no NULL in session_id / device_id / platform / session_date. | `_assert_fct_app_session` below. A single `agg` collects the stats; `n_engagement` is counted **from the input side** (raw ⋈ catalog filtered), never from the output. | **Inner join to `device_registry`** drops D3: 3 rows, `sum = 6 ≠ 7`, so the **conservation assertion fires**. This is the naive route the day was built to make the gate catch. **An inert gate**: `assert df` / `assert x is not None` / an assert on an un-`collect`ed DataFrame is always true (the Day 25/26 pattern). Feed it a violating row to prove it fires. Without `coalesce(platform, 'UNKNOWN')`, D3 has platform NULL and the NULL assertion fires. |
| P3 | Read only the columns this job needs from the two config tables. owner_team and push_token are operational metadata and must not enter the pipeline. | `event_catalog.select("event_name", "is_engagement")`, `device_registry.select("device_id", "platform")`. The SQL side uses subquery projections. | Measured plan: Catalyst already prunes `owner_team` / `push_token` above the Scan (`Project [event_name#4]`, `Project [device_id#7, platform#8]`). **No plan-level symptom.** Grade it as production robustness / hygiene (both branches should be consistent), not performance. Same ruling as Days 24/25. |

---

## Pipeline stages

1. **engagement filter** — project raw to `(event_id, device_id, event_ts,
   event_name)`, broadcast-join the catalog's `(event_name, is_engagement)`,
   keep `is_engagement`, and parse `ts_sec = unix_timestamp(event_ts)`.
2. **device enrich** — left-join `device_registry(device_id, platform)` and
   apply `coalesce(platform, 'UNKNOWN')`. Placement is free: before
   sessionizing or after the rollup. The reference does it after, because
   the dimension then joins 4 rows instead of 7.
3. **sessionize** — `lag(ts_sec)` over `(device_id ORDER BY ts_sec,
   event_id)`, with `is_new = prev IS NULL OR gap > 1800`. Then label each
   event with a running `sum(is_new)` (Route A) or a running
   `max(start_mark)` (Route B). **Trap lives here (window variant).**
4. **session rollup** — `groupBy(device_id, session_label)`: compute
   `min / max / count`, then derive `session_id`, `session_date`,
   formatted timestamps and `duration_sec` **from `start_sec`**. **Trap
   lives here (groupBy / id variants).**
5. **assert & publish** — the P2 assertions, then return.

---

## Part 4 — Reference answers

All three routes below were executed against the harness data and returned
PASS with schema
`session_id string, device_id string, platform string, session_date string,
session_start_ts string, session_end_ts string, n_events bigint,
duration_sec bigint`.

### Shared assertion helper (P2)

```python
def _assert_fct_app_session(out, n_engagement):
    s = out.agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("session_id").alias("n_ids"),
        F.coalesce(F.sum("n_events"), F.lit(0)).alias("n_ev"),
        F.sum(F.when(F.col("session_id").isNull() | F.col("device_id").isNull()
                     | F.col("platform").isNull() | F.col("session_date").isNull(), 1)
               .otherwise(0)).alias("n_null"),
    ).first()
    assert s["n_rows"] == s["n_ids"], \
        f"P2: session_id not unique ({s['n_rows']} rows, {s['n_ids']} ids)"
    assert s["n_ev"] == n_engagement, \
        f"P2: {s['n_ev']} events in sessions vs {n_engagement} engagement events in input"
    assert (s["n_null"] or 0) == 0, f"P2: {s['n_null']} rows with NULL key columns"
```

`countDistinct` ignores NULL. A NULL `session_id` would therefore also show up
as `n_ids < n_rows`, so the uniqueness check doubles as part of the NULL
check. The `coalesce(sum, 0)` handles an empty batch, where `sum` over zero
rows is NULL and `NULL == 0` is false.

### Route A — lag + running-sum session counter (Day 13 idiom)

```python
GAP_SEC = 30 * 60

def ref_build_fct_app_session_dsl_a(raw_app_event, event_catalog, device_registry):
    catalog = event_catalog.select("event_name", "is_engagement")       # P3
    registry = device_registry.select("device_id", "platform")          # P3

    # 1. engagement filter
    events = (
        raw_app_event.select("event_id", "device_id", "event_ts", "event_name")
        .join(F.broadcast(catalog), "event_name", "inner")
        .filter(F.col("is_engagement"))
        .select("event_id", "device_id",
                F.unix_timestamp("event_ts").alias("ts_sec"))
    )

    # 3. sessionize — partition by device ONLY; no date anywhere in the key
    w = Window.partitionBy("device_id").orderBy("ts_sec", "event_id")
    sessionized = (
        events
        .withColumn("prev_sec", F.lag("ts_sec").over(w))
        .withColumn("is_new",
                    F.when(F.col("prev_sec").isNull()
                           | (F.col("ts_sec") - F.col("prev_sec") > GAP_SEC), 1)
                     .otherwise(0))
        .withColumn("session_seq", F.sum("is_new").over(
            w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    )

    # 4. rollup, then 2. enrich — every date/id column derives from start_sec
    out = (
        sessionized.groupBy("device_id", "session_seq")
        .agg(F.min("ts_sec").alias("start_sec"),
             F.max("ts_sec").alias("end_sec"),
             F.count("*").alias("n_events"))
        .join(F.broadcast(registry), "device_id", "left")
        .select(
            F.concat("device_id", F.lit("-"),
                     F.from_unixtime("start_sec", "yyyyMMddHHmmss")).alias("session_id"),
            "device_id",
            F.coalesce("platform", F.lit("UNKNOWN")).alias("platform"),
            F.from_unixtime("start_sec", "yyyy-MM-dd").alias("session_date"),
            F.from_unixtime("start_sec").alias("session_start_ts"),
            F.from_unixtime("end_sec").alias("session_end_ts"),
            "n_events",
            (F.col("end_sec") - F.col("start_sec")).alias("duration_sec"),
        )
    )

    # 5. assert & publish
    _assert_fct_app_session(out, events.count())
    return out
```

```sql
-- ref_build_fct_app_session_sql_a
-- (P2 runs afterwards in Python: n_engagement = SELECT COUNT(*) FROM raw_app_event r
--  JOIN event_catalog c ON r.event_name = c.event_name WHERE c.is_engagement)
WITH events AS (
    SELECT /*+ BROADCAST(c) */
           r.event_id, r.device_id, unix_timestamp(r.event_ts) AS ts_sec
    FROM raw_app_event r
    JOIN (SELECT event_name, is_engagement FROM event_catalog) c
      ON r.event_name = c.event_name
    WHERE c.is_engagement
),
flagged AS (
    SELECT event_id, device_id, ts_sec,
           CASE WHEN LAG(ts_sec) OVER w IS NULL
                  OR ts_sec - LAG(ts_sec) OVER w > 1800
                THEN 1 ELSE 0 END AS is_new
    FROM events
    WINDOW w AS (PARTITION BY device_id ORDER BY ts_sec, event_id)
),
numbered AS (
    SELECT device_id, ts_sec,
           SUM(is_new) OVER (PARTITION BY device_id ORDER BY ts_sec, event_id
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_seq
    FROM flagged
),
sessions AS (
    SELECT device_id, session_seq,
           MIN(ts_sec) AS start_sec, MAX(ts_sec) AS end_sec, COUNT(*) AS n_events
    FROM numbered
    GROUP BY device_id, session_seq
)
SELECT /*+ BROADCAST(d) */
       concat(s.device_id, '-', from_unixtime(s.start_sec, 'yyyyMMddHHmmss')) AS session_id,
       s.device_id,
       COALESCE(d.platform, 'UNKNOWN')          AS platform,
       from_unixtime(s.start_sec, 'yyyy-MM-dd') AS session_date,
       from_unixtime(s.start_sec)               AS session_start_ts,
       from_unixtime(s.end_sec)                 AS session_end_ts,
       s.n_events,
       s.end_sec - s.start_sec                  AS duration_sec
FROM sessions s
LEFT JOIN (SELECT device_id, platform FROM device_registry) d
  ON s.device_id = d.device_id
```

### Route B — running max of the start timestamp as the session label

Replace stage 3's counter with the session's own start time. Each event
carries its session's `start_sec` directly. The rollup groups by
`(device_id, start_sec)`, which is also the natural key that `session_id`
encodes.

```python
    labelled = (
        events
        .withColumn("prev_sec", F.lag("ts_sec").over(w))
        .withColumn("start_mark",
                    F.when(F.col("prev_sec").isNull()
                           | (F.col("ts_sec") - F.col("prev_sec") > GAP_SEC),
                           F.col("ts_sec")))                    # NULL otherwise
        .withColumn("start_sec", F.max("start_mark").over(
            w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    )
    out = (labelled.groupBy("device_id", "start_sec")
           .agg(F.max("ts_sec").alias("end_sec"), F.count("*").alias("n_events"))
           ...)   # same enrich / select / assert as Route A
```

Route B only works because `ts_sec` is monotone within a device under the
window order. A running `max` of the start marks is therefore the *latest*
start so far. A running `last(start_mark, ignorenulls=True)` is equivalent
here.

**Tradeoff: none at plan level.** Routes A and B are **node-level
isomorphic** (measured, see below). The difference is readability.
Route B's label already *is* the session's identity, so it is harder to
accidentally group by something else. Route A's `session_seq` is
meaningless outside the batch, so it is easy to "enrich" the key with a
date. **No performance story here, none should be invented.**

---

## Part 5 — Concept takeaways

- **A date is an attribute of a session, never part of its key.** Session
  boundaries come from the gap rule alone. Any `to_date(event_ts)` in a
  window `partitionBy` or a rollup `groupBy` re-introduces midnight as a
  second, undeclared closing condition. The general form: **an attribute
  derived from the entity (start date, start ts, id) is computed from the
  entity's aggregate (`min(ts)`), never from its members.**
- **"Every output dimension goes into the groupBy" is the instinct that
  breaks this.** It is safe only when the dimension is functionally
  dependent on the group key at the member level. `session_date` depends on
  the session, but `to_date(event_ts)` does not. A functional-dependency
  check must be done on the member column actually used, not the output
  column name.
- **Decoys that cross midnight and come out right are not evidence.** D2's
  correctness comes from the 45-minute gap. A midnight cut and a gap cut
  coincide there. The only discriminating row is a session crossing midnight
  with **short** gaps.
- **Assertion reach depends on *where* the id is minted.** When the id is
  minted after the rollup, the split fragments get distinct ids and the
  uniqueness check is blind. When it is minted per event before a bad
  rollup, the fragments collide and the check fires. The conservation check
  is blind to *any* re-partitioning of events between sessions: it counts
  events, not boundaries. **Conservation catches loss, uniqueness catches
  duplication, and neither catches mis-segmentation.**
- **Total order for determinism.** `orderBy(ts_sec, event_id)` makes the
  window order total, since event_id is unique by upstream contract. For a
  `lag` gap rule specifically, ties are harmless (gap 0 either way). The
  tiebreak matters as soon as a positional function (`row_number`, `first`)
  reads the order. Cheap insurance, not a measured bug.
- **ANSI:** `unix_timestamp(string)` on a malformed `event_ts` **throws**
  under ANSI (Spark 4 default) rather than returning NULL. The contract says
  "never NULL, fixed format", so no fallback is needed. Any
  `COALESCE(unix_timestamp(...), ...)` in a review is dead code that would
  never receive a NULL.
- **Whitelist techniques exercised:** gap-threshold sessionization
  (`lag` + running sum, or running max), ROWS frame, broadcast join, left
  join + `'UNKNOWN'` default (orphan dimension key), conditional
  aggregation in the assertion, and input-side conservation.

## Physical plan notes

Measured: Spark 4.1.1, `local[2]`, `shuffle.partitions = 4`,
`spark.sql.adaptive.enabled = false`, `executedPlan` string, node counts by
regex.

| Route | shuffle Exchange | BroadcastExchange | Sort | Window | HashAggregate | BHJ | SMJ |
|---|---|---|---|---|---|---|---|
| ref-A DSL | 1 | 2 | 1 | 2 | 2 | 2 | 0 |
| ref-B DSL | 1 | 2 | 1 | 2 | 2 | 2 | 0 |
| ref-A SQL | 1 | 2 | 1 | 2 | 2 | 2 | 0 |

ref-A DSL fragment:

```
+- *(6) HashAggregate(keys=[device_id#1, session_seq#13L], functions=[min, max, count(1)])
   +- *(6) HashAggregate(keys=[device_id#1, session_seq#13L], functions=[partial_min, partial_max, partial_count(1)])
      +- Window [sum(is_new) ... RowFrame, unboundedpreceding, currentrow ...], [device_id#1], [ts_sec ASC, event_id ASC]
         +- Window [lag(ts_sec, -1, null) ...], [device_id#1], [ts_sec ASC, event_id ASC]
            +- *(3) Sort [device_id ASC, ts_sec ASC, event_id ASC]
               +- Exchange hashpartitioning(device_id#1, 4)
```

- **One shuffle for the whole job.** The two `Window` nodes share one
  `Sort`, because the second window's spec is identical and its child is
  already sorted. The partial and final `HashAggregate` have **no Exchange
  between them**. The window has already hash-partitioned by `device_id`,
  which is a subset of the groupBy keys `(device_id, session_seq)`. That is
  the **partition-key subset rule, eighth confirmation** (Days
  16/17/18/23/24/25).
- **A date in the key changes this.** A naive
  `groupBy(device_id, session_seq, event_date)` still has `device_id` as a
  subset, so its Exchange count is likely unchanged. A naive
  `partitionBy(device_id, event_date)` window would shuffle on
  `(device_id, event_date)`, and the later `groupBy(device_id, ...)` is no
  longer a superset of that partitioning. **NOT MEASURED** — do not assert a
  count without running it.
- Both dimension joins are BHJ. They come from the explicit hints: `Scan
  ExistingRDD` has no size statistics, so without a hint they would be SMJ
  under AQE off (the Day 26 finding).

## Echoes

- **Day 13** (gap-threshold sessionization via `lag` + running sum): the same
  idiom, placed inside a full job for the first time. The new piece is
  **what the partition key is allowed to contain**.
- **Days 23 / 24 / 25** ("an assertion only sees what survives into the
  output"): refined. This is the first day where P2 has a naive route it
  **does** catch (inner join drops D3, so conservation fires). The trap
  still shows that conservation is blind to mis-segmentation.
- **Days 25 / 26** (inert quality gates: `is not None`, bare `assert df`):
  P2 here is checkable by perturbation. Join D3 inner and the conservation
  assertion must raise. If it doesn't, the gate is inert.
- **Day 25** (two clocks): the domain here is single-clock UTC on purpose.
  The attribution question is *which member's* date, not *which clock*.
