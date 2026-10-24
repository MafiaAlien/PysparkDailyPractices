# Day 28 — trip_payout_resolution — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

ETL layer : L4 incremental
Domain    : ride-hailing driver payout
Failure mode (the trap axis — never stated in the day file):
            orphan keys — **the orphan is on the second lookup hop, not the
            first.** The business context sells the late-arriving *driver*
            (D9, D4). The rows that break naive jobs have a driver that
            resolves and a fleet that does not: a real orphan key (`F9`, no
            config row yet) and an unassigned driver (`fleet_id` NULL), which
            is the outer-side NULL that collapses `NOT IN`.

## The trap

**What it is:** payability is decided by the **last** hop of the lookup
chain (driver → fleet → commission). It is not decided by the first hop.
T103 (driver D3 → fleet F9, no row in `fleet_commission`) and T105 (driver
D5 → `fleet_id` NULL) both have a driver in `dim_driver`, and neither can be
paid. Both must come out `PENDING` with `driver_earning_cents` NULL. T103
keeps `fleet_id = 'F9'`. T105 has `fleet_id` NULL.

**Why a naive solution passes anyway:** the business context and the
`dim_driver` notes are about drivers who onboard late. A solver who builds
the job around that story writes the status gate on the driver hop:
`CASE WHEN d.driver_id IS NULL THEN 'PENDING'`, or `left_anti dim_driver`
for the pending branch. On clean data every known driver has a configured
fleet, so "driver found ⇒ payable" is coincidentally correct. In the test
data it is right for six of eight rows: T101, T102, T106 and T090 are
PUBLISHED, and T104 and T085 are PENDING. T090 is the late-arriving driver
that has now arrived. It resolves cleanly and *rewards* the driver-hop
mental model.

**Which row exposes it:** `raw_trip` **T103** (D3 → F9) and **T105**
(D5 → NULL fleet). Measured output of four naive variants on the day's data
(the scratch script ran each without and with the reference assertions):

| Variant | Output | What goes wrong | Which P# fires |
|---|---|---|---|
| Status gate on the driver hop, left-join chain | 8 rows | T103 and T105 come out `PUBLISHED` with `driver_earning_cents` NULL | **P2** (2 PUBLISHED without earning) |
| Split route: inner join driver + fleet → PUBLISHED; `LEFT ANTI JOIN dim_driver` → PENDING | 6 rows | T103 and T105 vanish: inner-joined out of PUBLISHED, anti-joined out of PENDING | **P1** (6 out, 8 in) |
| Split route with `e.fleet_id NOT IN (SELECT fleet_id FROM fleet_commission)` → PENDING | 5 rows | T105, T104 and T085 vanish (see below) | **P1** (5 out, 8 in) |
| `COALESCE(commission_pct, 0)` "safe default" | 8 rows | Unresolved trips get earning = full fare: T103 PUBLISHED with 1800; T085/T104/T105 PENDING but carry an earning | **P2** (3 PENDING with earning) |

The `NOT IN` row deserves the detail. T104 and T085 have no driver row, so
the job's **own left join** to `dim_driver` hands them `fleet_id` NULL.
T105's NULL comes straight from the dimension. `NULL NOT IN ('F1','F2','F3')`
is UNKNOWN, so the filter drops all three, and the inner join on
`fleet_id` dropped them from PUBLISHED as well. The subquery side has **no**
NULL here. This is the *outer-column* half of the `NOT IN` safety rule. The
NULL was manufactured by an earlier stage of the same job, the way Day 26's
`trim` manufactured its sentinel collision. The measured plan shows the
mechanism directly:
`BroadcastHashJoin [fleet_id], [fleet_id], LeftAnti, BuildRight, true`. The
trailing `true` is `isNullAwareAntiJoin`.

So P1 and P2 are both **real gates** on this day. Every naive variant above
trips one of them, and that was the design requirement inherited from
Day 27 (at least one P# must fire under a naive route). The day only
punishes a gate that is *written but not evaluated*. That failure mode
carries over from Days 25–27 and is not specific to this day.

**Reading-time tell:** look at the predicate that decides `load_status`
and ask which column it tests. A correct gate tests the column the payout
is **computed from** (`commission_pct`, or the computed earning itself).
A gate on `d.driver_id`, `fleet_id IS NOT NULL`, or an anti-join against
`dim_driver` tests an intermediate hop. In a split route there is a second
tell: the PENDING branch must be the complement of the PUBLISHED branch,
which means an anti-join on **`trip_id` against the published set**. An
anti-join against a dimension is a different predicate. When the two
branches are not complements by construction, some row belongs to neither.

**Which pipeline stage it lives in:** stage 4, **classify and pay**. Stages
1–3 can be identical to the reference. The left joins are right, and the
enriched rows carry `fleet_id = 'F9'` and `commission_pct` NULL correctly.
The divergence happens only where the status is decided. In the split
variants it happens one step earlier: the inner join in stage 3 deletes
information the classify step needs.

---

## Production constraints — audit

| # | Constraint | Satisfied by | How it fails if ignored |
|---|---|---|---|
| P1 | Every candidate trip comes out of the job exactly once — PUBLISHED or PENDING, never dropped, never duplicated. Before returning, the job must assert it: the output row count equals count(raw_trip) + count(pending_trip), and trip_id is unique in the output. | One global `agg` over the output: `count(1)` vs `raw_trip.count() + pending_trip.count()`, `countDistinct(trip_id)` vs `count(1)`, then explicit `raise`. Route A gets conservation by construction (left joins on unique dimension keys, no filter). | Split routes with a non-complementary PENDING branch lose rows silently (6 or 5 out of 8, measured). Fan-out would duplicate: if `dim_driver` ever carried two rows for a driver, the left join would double that driver's trips, and the uniqueness half catches it. |
| P2 | A payout is never published incomplete. Before returning, the job must assert that driver_earning_cents is non-NULL on every PUBLISHED row and NULL on every PENDING row. A silent bad publish is worse than a failed job. | In the same `agg`: `count(when(PUBLISHED & earning IS NULL, 1))` and `count(when(PENDING & earning IS NOT NULL, 1))` must both be 0. Route A also satisfies it by construction, because one predicate (`commission_pct IS NOT NULL`) drives **both** `load_status` and the earning `CASE`. | A driver-hop gate publishes T103/T105 with NULL earning. `COALESCE(rate, 0)` pays the full fare. A gate written as `assert df` / `is not None` / `.first()` on a filtered frame is green either way (Days 25–27). |
| P3 | A night with no completed trips is a normal night. With raw_trip empty the job must still succeed — every assertion included — and return exactly the carried pending trips' rows; with raw_trip and pending_trip both empty it must return 0 rows. | A global (no `groupBy`) aggregate always returns exactly one row, even on empty input, and `count(...)` returns 0 rather than NULL on empty input. So `.collect()[0]` is safe and every comparison is an int comparison. **Measured:** empty `raw_trip` returns exactly T085 (PENDING) and T090 (PUBLISHED); both feeds empty returns `[]` for Route A DSL, Route A SQL and Route B. | `.first()[...]` on a `groupBy` result returns `None` on empty input → `TypeError` (Day 27 B1). `sum(when(...))` returns NULL on empty input: `None > 0` raises `TypeError` in Python, and `None == 0` is `False`, so an "== 0" style gate *raises* on a valid empty night. `max(landing_date)`, used to derive the run date, is NULL on an empty batch. |

The harness does not call the job on an empty batch: the two `check()` call
sites are fixed. P3 is graded at review, by calling the job with
`spark.createDataFrame([], schema)` for `raw_trip` (and for both feeds).

---

## Pipeline stages

1. **Gather candidates** — project `raw_trip` (renaming `landing_date` →
   `first_seen_date`) and `pending_trip` to one schema, then `unionByName`
   / `UNION ALL`. There is no dedup: `trip_id` is unique across both feeds
   by contract.
2. **Resolve driver** — `LEFT` join `dim_driver` on `driver_id` (broadcast).
   It carries `fleet_id`, which is NULL for D9/D8 (no row) and for D5
   (unassigned).
3. **Resolve commission** — `LEFT` join `fleet_commission` on the
   *driver's* `fleet_id` (broadcast). `commission_pct` is NULL for every
   trip whose chain broke at either hop.
4. **Classify and pay** — **the trap lives here.** One predicate,
   `commission_pct IS NOT NULL`, drives both `load_status` and
   `driver_earning_cents`. `trip_date = substring(completed_ts, 1, 10)`.
5. **Assert, then return** — one global `agg` covering P1 and P2, explicit
   `raise`, empty-safe for P3.

---

## Part 4 — Reference answers

All routes below were executed against the day's harness data and match
`expected` exactly (scratch validation, 2026-10-21). Output dtypes match the
contract (`fare_cents` / `driver_earning_cents` are `bigint`).

### Shared assertion (DSL and SQL both call it)

```python
def _assert_resolution(out, raw_trip, pending_trip):
    n_in = raw_trip.count() + pending_trip.count()
    s = out.agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct("trip_id").alias("n_ids"),
        F.count(F.when((F.col("load_status") == "PUBLISHED")
                       & F.col("driver_earning_cents").isNull(), 1)).alias("bad_pub"),
        F.count(F.when((F.col("load_status") == "PENDING")
                       & F.col("driver_earning_cents").isNotNull(), 1)).alias("bad_pen"),
        F.count(F.when(~F.col("load_status").isin("PUBLISHED", "PENDING")
                       | F.col("load_status").isNull(), 1)).alias("bad_status"),
    ).collect()[0]          # global agg: exactly one row, even on empty input
    if s["n"] != n_in:
        raise ValueError(f"P1 conservation: {s['n']} rows out, {n_in} in")
    if s["n_ids"] != s["n"]:
        raise ValueError(f"P1 uniqueness: {s['n'] - s['n_ids']} duplicate trip_id")
    if s["bad_pub"] or s["bad_pen"] or s["bad_status"]:
        raise ValueError(
            f"P2 completeness: {s['bad_pub']} PUBLISHED without earning, "
            f"{s['bad_pen']} PENDING with earning, {s['bad_status']} bad status")
```

`count(when(cond, 1))` is used instead of `sum(when(cond, 1).otherwise(0))`
because `count` is 0 on empty input and `sum` is NULL. Explicit `raise`
is used instead of `assert` because `python -O` strips `assert` (log/04,
Day 27).

### Route A — left-join chain, one predicate drives status and payout

```python
def ref_build_trip_payout_resolution_dsl(raw_trip, pending_trip,
                                         dim_driver, fleet_commission):
    candidates = (
        raw_trip.select("trip_id", "driver_id", "completed_ts", "fare_cents",
                        F.col("landing_date").alias("first_seen_date"))
        .unionByName(pending_trip.select("trip_id", "driver_id", "completed_ts",
                                         "fare_cents", "first_seen_date"))
    )
    enriched = (
        candidates
        .join(F.broadcast(dim_driver.select("driver_id", "fleet_id")),
              "driver_id", "left")
        .join(F.broadcast(fleet_commission.select("fleet_id", "commission_pct")),
              "fleet_id", "left")
    )
    payable = F.col("commission_pct").isNotNull()      # the LAST hop decides
    out = enriched.select(
        "trip_id", "driver_id", "fleet_id",
        F.substring("completed_ts", 1, 10).alias("trip_date"),
        "fare_cents",
        F.when(payable,
               F.col("fare_cents") - F.expr("fare_cents * commission_pct div 100"))
         .cast("bigint").alias("driver_earning_cents"),
        "first_seen_date",
        F.when(payable, "PUBLISHED").otherwise("PENDING").alias("load_status"),
    )
    _assert_resolution(out, raw_trip, pending_trip)
    return out
```

```sql
-- ref_build_trip_payout_resolution_sql  (then _assert_resolution on the result)
WITH candidates AS (
    SELECT trip_id, driver_id, completed_ts, fare_cents,
           landing_date AS first_seen_date
    FROM raw_trip
    UNION ALL
    SELECT trip_id, driver_id, completed_ts, fare_cents, first_seen_date
    FROM pending_trip
),
enriched AS (
    SELECT /*+ BROADCAST(d, f) */
           c.*, d.fleet_id, f.commission_pct
    FROM candidates c
    LEFT JOIN dim_driver d       ON c.driver_id = d.driver_id
    LEFT JOIN fleet_commission f ON d.fleet_id  = f.fleet_id
)
SELECT trip_id, driver_id, fleet_id,
       substring(completed_ts, 1, 10)                      AS trip_date,
       fare_cents,
       CASE WHEN commission_pct IS NOT NULL
            THEN fare_cents - fare_cents * commission_pct div 100
       END                                                 AS driver_earning_cents,
       first_seen_date,
       CASE WHEN commission_pct IS NOT NULL
            THEN 'PUBLISHED' ELSE 'PENDING' END            AS load_status
FROM enriched
```

Notes:
- The second join is `d.fleet_id = f.fleet_id`: the **driver's** fleet.
  The trip has no fleet of its own.
- `UNION ALL` in SQL, positional; `unionByName` in DSL. Positional `union`
  in DSL works here only because both selects list the columns in the same
  order.
- `div` keeps the arithmetic integral. `/` returns `double`, and a trailing
  `CAST(... AS BIGINT)` would also be exact on these values. That form is
  fine, but it is a rounding decision in disguise once a fare stops
  dividing evenly.
- ANSI: no exposure. `NULL * int` is NULL, not an error, and no cast of a
  string happens. The only ANSI-throwing case would be bigint overflow.

### Route B — split, with PENDING as the complement on `trip_id`

```python
def ref_build_trip_payout_resolution_dsl_b(raw_trip, pending_trip,
                                           dim_driver, fleet_commission):
    candidates = ...                                   # as Route A
    resolved = candidates.join(F.broadcast(dim_driver), "driver_id", "left")
    published = (
        resolved.join(F.broadcast(fleet_commission), "fleet_id", "inner")
        .select("trip_id", "driver_id", "fleet_id",
                F.substring("completed_ts", 1, 10).alias("trip_date"),
                "fare_cents",
                (F.col("fare_cents") - F.expr("fare_cents * commission_pct div 100"))
                    .cast("bigint").alias("driver_earning_cents"),
                "first_seen_date", F.lit("PUBLISHED").alias("load_status"))
    )
    pending = (
        resolved.join(published.select("trip_id"), "trip_id", "left_anti")
        .select("trip_id", "driver_id", "fleet_id",
                F.substring("completed_ts", 1, 10).alias("trip_date"),
                "fare_cents",
                F.lit(None).cast("bigint").alias("driver_earning_cents"),
                "first_seen_date", F.lit("PENDING").alias("load_status"))
    )
    out = published.unionByName(pending)
    _assert_resolution(out, raw_trip, pending_trip)
    return out
```

This route is correct **only because** the anti-join key is `trip_id` (never
NULL, by contract) and the right side is the published result itself. That
makes the two branches complements by construction. Swap the right side
for `dim_driver`, or swap `left_anti` for `NOT IN` on any nullable column,
and you get the Route X variants in the trap table.

Route B is not a style alternative to Route A. The published subtree
appears **three times** in its plan: once as output, and once as the right
side of each of the two anti-joins (the optimizer pushes the anti-join
through the candidates `Union` into each branch). It also adds shuffles
(below). Its only advantage is that a reviewer can see the two output
populations as two named frames. A `status` column does that more cheaply.

---

## Part 5 — Concept takeaways

- **Payability is a property of the last hop.** In a lookup *chain*
  (fact → dim → config), a key can resolve at hop 1 and orphan at hop 2.
  A status gate must test the column the payload is computed from, not
  the first join's key. "Driver found" and "driver payable" are different
  predicates that agree on clean data.
- **One predicate, two consumers.** When the same fact (here
  `commission_pct IS NOT NULL`) decides both a status and a nullable
  payload, derive both from that one expression. Two independently
  written conditions can disagree, and P2 is exactly the assertion that
  catches the disagreement.
- **Split-and-union routes need complementary predicates by
  construction.** `A` and `NOT A` are only complements in two-valued
  logic. `IN` / `NOT IN` over a nullable column, or an inner join paired
  with an anti-join against a *different* table, leave a third population
  that lands in neither branch. Anti-joining against the published
  `trip_id`s makes the complement structural.
- **`NOT IN` has two NULL sides, and a job can manufacture the outer one.**
  log/04 already records that a NULL in the *subquery* empties the result.
  Day 28 hits the other side: `NULL NOT IN (non-empty list)` is UNKNOWN
  and the row is filtered. Here the NULL was not in any input column the
  solver inspected. The job's own left join produced it for every
  unresolved driver. Spark plans this as a null-aware anti join
  (`BroadcastHashJoin ... LeftAnti, BuildRight, true`).
- **Late-arriving dimension pattern (park and replay).** An L4 job that
  cannot resolve a fact either drops it (silent loss), publishes a
  placeholder (an `UNKNOWN` member, as on Day 27), or parks it and retries.
  Parking makes `pending ∪ new` the job's real input, and conservation
  must be stated against that union, not against the landing batch.
- **Empty-safe assertion shape:** a global `agg` (no `groupBy`) always
  yields exactly one row, and `count(when(...))` is 0 on empty input. Both
  facts are what make `.collect()[0]` and integer comparisons safe on an
  empty night.
- **Whitelist techniques exercised:** `unionByName` / `UNION ALL` of two
  feeds, left join chain with `broadcast`, orphan keys (true orphan + NULL
  key), `left_anti` vs `NOT IN` three-valued logic, conditional expression
  driving status and payload, `COUNT(*)` vs `COUNT(col)` in assertions.

## Physical plan notes

Measured with `.explain()` on the day's data, Spark 4.1.1,
`shuffle.partitions = 4`, initial (pre-AQE) plan. Counts are taken
node-by-node from the printed plans (Route B's seven `BroadcastExchange`
plan_ids were enumerated individually):

| Route | Exchange (shuffle) | BroadcastExchange | Joins | Other |
|---|---|---|---|---|
| A — DSL | **0** | 2 | 2 × `BroadcastHashJoin LeftOuter` | `Union` of two scans feeds the first BHJ |
| A — SQL (with `BROADCAST(d, f)` hint) | **0** | 2 | 2 × `BroadcastHashJoin LeftOuter` | node-for-node identical to A-DSL |
| B — DSL | **4** | **7** | **6** × BHJ Inner + 1 × BHJ LeftOuter + 2 × `SortMergeJoin LeftAnti` on `trip_id` | the published subtree (2 BHJ Inner + 2 BroadcastExchange) appears 3 times: once as output, once as the right side of each anti-join; the anti-join is pushed through the candidates `Union` into each branch, so 2 SMJs × 2 Exchanges; the 7th broadcast is `dim_driver` for the PENDING branch's left join |
| Naive `NOT IN`, no hints | 6 | 1 | 2 × SMJ Inner, 1 × SMJ LeftOuter, 1 × BHJ `LeftAnti ... true` | null-aware anti join. The subquery side is broadcast even with no hint, while every other join in the same plan fell back to SMJ. So the broadcast came from the null-aware anti-join planning path, not from size-based auto-broadcast. The exact planner rule was not isolated |

- In this harness the Route A broadcasts come from the **hints**. A
  `createDataFrame` from local rows plans as `Scan ExistingRDD` with no
  size estimate that triggers auto-broadcast, so an unhinted equi-join
  plans as `SortMergeJoin` (visible in the naive `NOT IN` plan). Do not read
  "Route A has 0 Exchange" as a property of left joins. It is a property
  of the hint plus a small dimension.
- A vs B is a real difference, not an invented one: 0 vs 4 shuffle
  Exchanges, and the published subtree evaluated 1 vs 3 times. The cause
  is the anti-join on `trip_id` against a derived frame that has no
  broadcast hint, not "splitting" as such. With
  `F.broadcast(published.select("trip_id"))` the SMJs would likely become
  BHJs (NOT MEASURED), but the subtree would still be recomputed.
- The assertions in every route trigger extra jobs: `raw_trip.count()`,
  `pending_trip.count()`, and the `agg` over the full output, which
  recomputes the DAG once more before the caller's own action. On a real
  batch that is one extra full evaluation of the job. Caching or
  persisting `out` before asserting is the standard fix. It is not
  measured here and should not be claimed as a speedup without a run.

## Echoes

- **log/04 "NOT IN vs NOT EXISTS — 三值逻辑陷阱" (Day 9 / Day 16).** Reinforced,
  and the half that had only been stated is now demonstrated:
  "NOT IN 的安全性要求**两侧**都没有 NULL ... 外层列". Day 16 drilled the
  subquery side. Day 28 is the outer side, with the NULL produced by the
  job's own left join.
- **Day 26 Route X** (split + `left_anti` + union, rows vanish; `NOT IN`
  named as the sibling and never measured). Now measured: 6/8 and 5/8 rows
  out, P1 fires. This closes the `orphan keys` axis that Day 26 left half
  consumed (false orphans there, true orphans here).
- **Day 27 inner-join drop of D3 / UNKNOWN member.** Day 27 handled an
  unresolved dimension with a placeholder. Day 28 is the other standard
  answer (park and replay). The choice between them is a contract
  decision.
- **Days 25–27 "gate written but not evaluated".** P1 and P2 are designed
  to fire under every naive variant measured above. If a submitted
  solution's assertions stay green on the driver-hop gate, the gate is not
  being evaluated, and that is the fourth consecutive day of the pattern.
- **Day 27 B1 (empty-batch `.first()[...]` crash).** Promoted from review
  finding to public contract as P3. Global-agg shape documented above.
