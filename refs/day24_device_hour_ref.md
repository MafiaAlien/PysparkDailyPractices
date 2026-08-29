# Day 24 — fct_device_hour — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

ETL layer : L2 detail modeling
Domain    : logistics — cold-chain warehouse telemetry
Failure mode (the trap axis — never stated in the day file): **replay**

## The trap

**What it is:** the landing feed is at-least-once, and a *retransmission is a
new transmission*: the gateway stamps it with a fresh `event_id` and the
collector stamps it with a fresh `ingest_ts`. `E07` is `E02` sent again. The
only identity that survives a retransmission is the physical reading's
business key `(device_id, reading_ts)`. Deduping on `event_id`, or on the
whole row, does not collapse it — and the job must collapse it **at the
entry**, before anything counts rows.

**Why a naive solution passes anyway:** the feed *also* contains a
byte-identical pair (`E06` appears twice — the collector wrote the same
message twice). A whole-row `dropDuplicates()` removes that one, D2's
`n_readings` comes out 1, and the solver now has visible evidence that
"duplicates are handled". Everything else in the output is right too:

- the grain stays unique — the final `groupBy` guarantees it, so the
  duplicate can never show up as an extra row;
- no NULL appears anywhere;
- `avg_temp_c` and `max_temp_c` on the affected row are **both unchanged**:
  D1's two surviving 09-hour readings are both `22.0`, and
  `mean{22,22} = mean{22,22,22} = 22.0`, `max` likewise. AVG is only
  duplicate-sensitive when the duplicated value differs from the group mean,
  and here it does not.

Exactly **one integer cell** moves:
`D1 / 2026-08-25 09:00:00 / n_readings = 3` instead of `2`.
Three is a plausible number of readings in an hour. There is no shape signal
anywhere in the output that says "duplicate".

**Which row exposes it:** `raw_readings` row
`E07 | D1 | 2026-08-25 09:20:00 | 22.0 | 2026-08-25 10:18:22`
— same `(device_id, reading_ts)` as `E02`, different `event_id`.

**Reading-time tell:** two of them, both one column-scan away.

1. Scan `(device_id, reading_ts)` for repeats. `E02` and `E07` collide on it
   while every other column of the envelope differs. This is the general
   rule, not a rule about this dataset: **when a feed is at-least-once, the
   only key you may dedup on is the one the payload carries, never the one
   the envelope carries.**
2. `E07` is the only row in the table whose `ingest_ts` falls in a *later
   hour* than its `reading_ts` (09:20 measured, 10:18 arrived — a 58-minute
   lag against a 13–17 minute worst case for every other row). P1 points a
   reader straight at this column pair.

The exact-duplicate pair `E06`/`E06` is a decoy: it is caught by every dedup
route, including the two wrong ones, which is precisely what makes them feel
validated.

**Which pipeline stage it lives in:** **Stage 1, the entry dedup.** Nothing
downstream can repair it. Patching the symptom at Stage 4 with
`COUNT(DISTINCT reading_ts)` fixes `n_readings` and leaves `avg_temp_c`
wrong — a metric-by-metric patch that stops holding the moment a fifth
column is added to the contract.

---

## Production constraints — audit

| # | Constraint | Satisfied by | How it fails if ignored |
|---|---|---|---|
| P1 | Every reading is attributed to the hour of its own `reading_ts`; `ingest_ts` must never determine the hour | `concat(substring(reading_ts, 1, 13), ':00:00')` as the bucket; `ingest_ts` appears **only** inside the dedup `ORDER BY`, never as a key | bucket by `ingest_ts` and `E07`'s reading lands in the 10:00 bucket: D1 shows three hours instead of two, and the 09:00 count drops to 1 while a phantom 10:00 reading appears |
| P2 | Assert the output before returning: one row per (device_id, reading_hour); no NULL in `device_id` / `reading_hour` / `site_id` | `out.count() == out.select("device_id","reading_hour").distinct().count()`, plus a zero-count filter on the three NULL checks | a `left` registry join instead of `inner` lets D3's decommissioned reading through with `site_id = NULL`, and it publishes silently. **Measured: the naive route passes all three assertions.** Score P2 on presence, never on whether it caught anything — an output-grain assertion is structurally incapable of seeing a duplicate that the final `groupBy` already collapsed |
| P3 | Read only the needed columns from the two config tables | `.select("device_id","site_id","installed_on","decommissioned_on")` on the registry; `.select("device_id","window_start","window_end")` on maintenance | `model` and `ticket_id` ride through both shuffles; on the real table `ticket_id` fans out into a wide free-text work-order record |

---

## Pipeline stages

1. **Entry dedup** — collapse the landing feed to one row per physical
   reading, keyed on `(device_id, reading_ts)`. **The trap lives here.**
2. **Registry enrich + in-service filter** — inner join to `device_registry`
   (drops hardware the registry does not know), keep only readings whose
   `date(reading_ts)` satisfies `installed_on <= d < decommissioned_on`,
   with NULL open-ended. Drops `E04` (D3, decommissioned 2026-08-20).
3. **Maintenance exclusion** — `left_anti` / `NOT EXISTS` against
   `maintenance_windows` on the half-open `[window_start, window_end)`.
   Drops `E03`. It must be an anti-join, not a left join plus an
   `IS NULL` filter, because a device may hold several tickets.
4. **Hour bucket, aggregate, assert** — bucket on `reading_ts`, group to
   `(device_id, reading_hour, site_id)`, then run the P2 assertions.

`installed_on` is dead code on this dataset — no reading sits near an
install boundary. It is load-bearing in production. Same for the second and
third maintenance rows (`MT-102` is a different day, `MT-103` is a device
with no readings): both are controls, and `MT-103` is what breaks an
implementation that inner-joins the maintenance table instead of anti-joining
it.

---

## Part 4 — Reference answers

### Route A — window `row_number` dedup (the auditable one)

```python
def _assert_contract(out: DataFrame) -> DataFrame:
    n = out.count()
    n_keys = out.select("device_id", "reading_hour").distinct().count()
    assert n == n_keys, f"grain violated: {n} rows, {n_keys} keys"
    bad = out.filter(
        F.col("device_id").isNull()
        | F.col("reading_hour").isNull()
        | F.col("site_id").isNull()
    ).count()
    assert bad == 0, f"{bad} rows with NULL in a key column"
    return out


def ref_build_fct_device_hour_dsl_a(
    raw_readings, device_registry, maintenance_windows
):
    # 1. entry dedup — the business key is the payload's, not the envelope's
    w = Window.partitionBy("device_id", "reading_ts").orderBy(
        F.col("ingest_ts").desc(), F.col("event_id").desc()
    )
    readings = (
        raw_readings
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("device_id", "reading_ts", "temp_c")
    )

    # 2. enrich + in-service filter  (P3: four columns, not five)
    reg = device_registry.select(
        "device_id", "site_id", "installed_on", "decommissioned_on"
    )
    d = F.substring(F.col("reading_ts"), 1, 10)
    enriched = (
        readings.join(reg, on="device_id", how="inner")
        .filter(
            (d >= F.col("installed_on"))
            & (F.col("decommissioned_on").isNull()
               | (d < F.col("decommissioned_on")))
        )
        .select("device_id", "reading_ts", "temp_c", "site_id")
    )

    # 3. maintenance exclusion — anti-join, so it can never duplicate a row
    mw = maintenance_windows.select("device_id", "window_start", "window_end")
    cond = (
        (enriched["device_id"] == mw["device_id"])
        & (enriched["reading_ts"] >= mw["window_start"])
        & (enriched["reading_ts"] < mw["window_end"])
    )
    clean = enriched.join(mw, cond, "left_anti")

    # 4. bucket + aggregate  (P1: reading_ts, never ingest_ts)
    out = (
        clean
        .withColumn(
            "reading_hour",
            F.concat(F.substring(F.col("reading_ts"), 1, 13), F.lit(":00:00")),
        )
        .groupBy("device_id", "reading_hour", "site_id")
        .agg(
            F.count(F.lit(1)).alias("n_readings"),
            F.avg("temp_c").alias("avg_temp_c"),
            F.max("temp_c").alias("max_temp_c"),
        )
        .select("device_id", "reading_hour", "site_id",
                "n_readings", "avg_temp_c", "max_temp_c")
    )
    return _assert_contract(out)
```

```sql
-- ref_build_fct_device_hour_sql_a
WITH deduped AS (
    SELECT device_id, reading_ts, temp_c
    FROM (
        SELECT device_id, reading_ts, temp_c,
               ROW_NUMBER() OVER (
                   PARTITION BY device_id, reading_ts
                   ORDER BY ingest_ts DESC, event_id DESC
               ) AS rn
        FROM raw_readings
    )
    WHERE rn = 1
),
in_service AS (
    SELECT r.device_id, r.reading_ts, r.temp_c, g.site_id
    FROM deduped r
    JOIN (SELECT device_id, site_id, installed_on, decommissioned_on
          FROM device_registry) g
      ON r.device_id = g.device_id
    WHERE substring(r.reading_ts, 1, 10) >= g.installed_on
      AND (g.decommissioned_on IS NULL
           OR substring(r.reading_ts, 1, 10) < g.decommissioned_on)
),
clean AS (
    SELECT s.*
    FROM in_service s
    WHERE NOT EXISTS (
        SELECT 1
        FROM maintenance_windows m
        WHERE m.device_id  = s.device_id
          AND s.reading_ts >= m.window_start
          AND s.reading_ts <  m.window_end
    )
)
SELECT device_id,
       concat(substring(reading_ts, 1, 13), ':00:00') AS reading_hour,
       site_id,
       COUNT(*)    AS n_readings,
       AVG(temp_c) AS avg_temp_c,
       MAX(temp_c) AS max_temp_c
FROM clean
GROUP BY device_id, concat(substring(reading_ts, 1, 13), ':00:00'), site_id
```

The SQL still needs the P2 assertions applied to the returned DataFrame; the
`spark.sql(...)` result is wrapped in the same `_assert_contract`.

### Route B — `dropDuplicates` on the business key

```python
readings = (
    raw_readings
    .select("device_id", "reading_ts", "temp_c")
    .dropDuplicates(["device_id", "reading_ts"])
)
```

Everything downstream is identical to Route A. **It passes on this dataset,
and it is not a contract.** `dropDuplicates(subset)` keeps an *arbitrary*
survivor — Spark gives no guarantee about which one, and there is no
`orderBy` that makes it deterministic (Day 8 recorded this). It happens to be
safe here for a reason that is a property of the data, not of the code: a
retransmission carries the *same payload*, so every survivor is
interchangeable. The moment upstream sends a correction under the same
`(device_id, reading_ts)` — the normal way a bad reading gets fixed — Route B
silently starts picking at random and Route A's
`ORDER BY ingest_ts DESC` keeps meaning what it says. Route A is also the
only one of the two that leaves an auditable statement of *which* copy wins.

Node-level the two are not isomorphic, but the difference is not a shuffle
(see below): Route B trades the `Window` for two more `HashAggregate` nodes
at the same Exchange count.

---

## Part 5 — Concept takeaways

- **At-least-once means the envelope repeats, not the payload.** The message
  id (`event_id`) and the write timestamp (`ingest_ts`) are properties of a
  *transmission*; a retransmission is a different transmission. Dedup keys
  must come from the payload — here `(device_id, reading_ts)` — and choosing
  that key is a modeling decision that has to be made explicitly, never
  defaulted to "whatever looks like a primary key".
- **The three dedup strengths, in order.** `dropDuplicates()` (whole row)
  only catches a byte-identical write and is the weakest thing that still
  looks like dedup. `dropDuplicates(subset)` catches the right rows but picks
  an arbitrary survivor. `row_number() OVER (PARTITION BY key ORDER BY ...)`
  is the only one that states which copy wins.
- **Dedup is a placement decision, not just a key decision.** It belongs at
  the entry, before any join or aggregate. Repairing it downstream works
  per-metric — `COUNT(DISTINCT reading_ts)` fixes the count and leaves `AVG`
  broken — so the repair has to be re-derived for every column that is ever
  added.
- **Duplicate-idempotent aggregates hide duplicates.** `MAX`/`MIN` are immune
  to repeated rows; `COUNT`/`SUM` are not; `AVG` is immune only when the
  duplicated value equals the group mean, which is exactly the arrangement on
  this dataset. The immune aggregates are disproportionately the ones a human
  spot-checks first.
- **An output-grain assertion cannot see a collapsed duplicate.** The final
  `groupBy` makes the grain unique by construction, so uniqueness-on-grain is
  true whether or not the input was deduped. P2 was satisfied and useless
  simultaneously — measured, not argued. This is the second consecutive day
  where the quality gate passed on wrong numbers (Day 23 P2).
- **Excluding by interval is an anti-join, not a left join.** A device can
  hold several maintenance tickets; `left join` + `IS NULL` gives the same
  answer only while the right side is unique on the join key, and nothing
  enforces that. `left_anti` / `NOT EXISTS` is fan-out-proof by construction.
- **DSL/SQL contract note:** the same anti-join written as `left_anti` and as
  `NOT EXISTS` produced *different physical plans* here, and the SQL form was
  the cheaper one. Measured below.
- **ANSI:** nothing in this day throws. Every comparison is string↔string,
  there is no `cast`, no division, no array index, no `element_at`. The
  correct answer in the REVIEW_NOTES ANSI row is "no item to flag".
- **Whitelist coverage exercised:** `row_number` dedup; "`dropDuplicates` is
  not a contract"; `left_anti` / `NOT EXISTS` as a filter idiom; half-open
  interval semantics with NULL open ends; inner join as an existence filter;
  `COUNT(*)` on a deduped stream; the partition-key subset rule.

## Physical plan notes

Measured with `.explain(mode="formatted")`, Spark 4.1.1,
`spark.sql.adaptive.enabled=false`, `spark.sql.shuffle.partitions=4`.
Counts are `(n) NodeName` headers in the physical plan.

| Route | Exchange | Sort | Window | HashAggregate | SortMergeJoin |
|---|---|---|---|---|---|
| Route A DSL (`row_number`) | 4 | 5 | 1 | 2 | 2 |
| Route A SQL (`NOT EXISTS`) | **3** | 4 | 1 | 2 | 2 |
| Route B DSL (`dropDuplicates` on key) | 4 | 3 | 0 | 4 | 2 |
| naive (whole-row `dropDuplicates`) | 4 | 3 | 0 | 4 | 2 |

Re-measured with `spark.sql.autoBroadcastJoinThreshold` at its default and at
`-1`: **identical in both settings.** No `BroadcastHashJoin` is chosen for
these `LocalRelation`s, so there is no broadcast story on this data.

**Where the DSL/SQL gap comes from.** Isolated to the anti-join step with a
two-table repro (window + exclusion only, everything else removed):

```
DSL: window + left_anti      Exchange=3  parts=[(device_id, reading_ts), device_id, device_id]
SQL: window + NOT EXISTS     Exchange=2  parts=[device_id, device_id]
```

The SQL plan shuffles the feed on `device_id` **alone** and lets the Window
run on top of it. That is legal because a `Window`'s
`requiredChildDistribution` is `ClusteredDistribution(device_id, reading_ts)`,
and `HashPartitioning(device_id)` *satisfies* a clustered distribution whose
expression set it is a subset of — the **partition-key subset rule, fifth
confirmation** (Day 16 / 17 / 18 / 23). One shuffle then serves both the
window and the join. The DSL's explicit `left_anti` plan shuffles on the full
`(device_id, reading_ts)` for the window and shuffles again on `device_id`
for the join.

**Why Catalyst takes the smaller key set for the `NOT EXISTS` rewrite and not
for an explicit `left_anti` join is not established here** — it is measured,
not derived. What *was* ruled out: reordering the DSL's two joins
(anti-join before the registry join) changes nothing — still 4 Exchanges,
still partitioned `[(device_id, reading_ts), device_id, device_id, device_id]`.

Route A vs Route B is **not** a shuffle story — both are 4. The real trade is
`Window` + 2 `Sort` (Route A) against 2 extra `HashAggregate` nodes
(Route B, where `dropDuplicates` plans as a partial+final aggregate). Do not
invent a shuffle gap between them.

## Echoes

- **Day 8** (dedup: keep latest per key) — direct sequel. Day 8 recorded
  "exact-duplicate replay must still count in `n_versions`" and
  "`dropDuplicates` after `orderBy` is not a contract". Day 24 moves both
  from a single-table exercise into a four-stage job and adds the part Day 8
  did not have: *where in the pipeline* the dedup must sit.
- **Day 9 / Day 23** (semi/anti joins as filter idioms) — reinforces the
  anti-join-as-exclusion idiom. Unlike Day 23's effective-dated case, the
  anti-join here is the plainly correct tool: exclusion, not attribution.
- **Day 23 P2** (assertions) — refines it. Day 23 showed the reference's
  uniqueness assertion was tautological; Day 24 shows the same assertion set
  is *structurally* blind to an entry-stage duplicate, and the user wrote no
  assertions at all last time.
- **Day 16 / 17 / 18 / 23** (partition-key subset rule) — fifth measured
  confirmation, this time as a *DSL/SQL divergence* rather than a route
  choice.
- **Day 15** (a plan claim overturned by running it) — the DSL/SQL Exchange
  gap here would have been asserted backwards from memory; it only shows up
  in the plan.
