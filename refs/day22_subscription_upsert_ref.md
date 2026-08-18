# Day 22 — dim_subscription — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

ETL layer : L4 incremental
Domain    : subscription billing
Failure mode (the trap axis — never stated in the day file):
            **late data** — a change that arrives in today's batch but is
            older than the state already applied to the target row.

## The trap

**What it is:** a change row is not automatically newer than the target row
it lands on. `SUB2`'s change carries `change_ts = 2026-08-17 10:00:00`, while
the target row it matches already sits at `updated_at = 2026-08-17 18:30:00`.
The change is a straggler from before the last published state. Applying it
silently **reverts** the subscription from PRO to BASIC. A correct merge
applies a change only when it is strictly newer than the row it overwrites.

**Why a naive solution passes anyway:** every other subscription's change is
genuinely newer than its target row, so the staleness guard is a no-op on
them. Measured: the guard-less version produces **4 of 5 expected rows**
correctly and gets exactly one row wrong. Nothing about the wrong row looks
wrong — a downgrade from PRO to BASIC is an ordinary business event, the mrr
10.0 is internally consistent with the plan, and the row count is right.
There is no dropped row, no NULL, no duplicate: the output is the right
shape with one subscription quietly rolled back.

**Which row exposes it:** `subscription_changes` row 2 —
`("SUB2", "U", "BASIC", "ACTIVE", 10.0, "2026-08-17 10:00:00")` — against
`dim_subscription_current` row 2 —
`("SUB2", "PRO", "ACTIVE", 30.0, "2026-08-17 18:30:00")`. Note both are
dated 2026-08-17: the staleness is a matter of hours, not days, so a
date-level eyeball comparison does not catch it. Every other change is dated
2026-08-18.

**Reading-time tell:** `updated_at` runs **backwards**. SUB2's output
timestamp under the naive solution is `2026-08-17 10:00:00`, earlier than
the `2026-08-17 18:30:00` the dimension already carried. A dimension whose
last-updated column can decrease is the signature of an unguarded merge, and
it is visible without running anything: the target table has an `updated_at`
and the feed has a `change_ts`, and if the solution never compares the two,
nothing is guarding the direction of time.

**Which pipeline stage it lives in:** stage 2, "apply the staleness guard".
Stages 1, 3, 4 and 5 are identical in the trapped and untrapped versions.

---

## Production constraints — audit

| # | Constraint | Satisfied by | How it fails if ignored |
|---|---|---|---|
| P1 | Re-running on the same inputs is byte-identical | Pure function of the two inputs; the guard uses **strict** `>` so a re-applied change is rejected the second time | Not distinguishable on this test data — see the note below. Both `>` and `>=` produce the correct output here |
| P2 | `mrr` comes from `plan_catalog` | Inner join to `plan_catalog` on `plan`, projecting `plan_catalog.mrr` and never the feed's | SUB3 emits **55.0** instead of **100.0**. The feed's advisory value is wrong on exactly this row |
| P3 | `op = 'D'` must not appear | Tombstone keys removed from both the upsert set and the surviving-target set | SUB4 survives as `ENTERPRISE / ACTIVE / 100.0`, a cancelled subscription still billing |

**P1 is satisfied by construction, and no test row distinguishes `>` from
`>=`.** Say this out loud at Stage 5 rather than crediting the solution for
a strictness choice the data never tested. The distinction bites only when
the job's own output is fed back as tomorrow's `dim_subscription_current`
and the same batch is replayed: with `>=`, a change whose `change_ts` now
equals the row's `updated_at` is re-applied. It still lands on the same
values here, so even then the output is unchanged — the strictness matters
for cost and for audit trails, not for correctness on this shape.

---

## Pipeline stages

1. **Collapse the feed** — one change per `subscription_id`, latest
   `change_ts` wins (`row_number` over a descending window). SUB5 carries two
   changes in this batch; the 16:00 PRO row is the survivor.
2. **Apply the staleness guard** — keep a collapsed change only if its key is
   absent from the target, or its `change_ts` is strictly greater than the
   target's `updated_at`. **The trap lives here.**
3. **Split tombstones from upserts** — `op = 'D'` becomes a delete key set,
   everything else an upsert candidate.
4. **Price the upserts** — inner join to `plan_catalog` on `plan`, taking the
   catalog's `mrr`.
5. **Recombine** — target rows whose key was neither upserted nor deleted
   (`left_anti`) union the priced upserts.

---

## Part 4 — Reference answers

### Route A — collapse, guard, anti-join, union

The literal MERGE shape: work out what happens to each incoming change, then
stitch the untouched target rows back on.

```python
def ref_build_dim_subscription_dsl_a(
    dim_subscription_current, subscription_changes, plan_catalog
):
    cols = ["subscription_id", "plan", "status", "mrr", "updated_at"]
    w = Window.partitionBy("subscription_id").orderBy(F.col("change_ts").desc())
    collapsed = (
        subscription_changes.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1").drop("rn")
    )
    j = collapsed.alias("c").join(
        dim_subscription_current.alias("t"), "subscription_id", "left"
    )
    applied = j.filter(
        F.col("t.updated_at").isNull() | (F.col("c.change_ts") > F.col("t.updated_at"))
    )
    deleted = applied.filter("c.op = 'D'").select("subscription_id")
    upserts = (
        applied.filter("c.op <> 'D'")
        .join(F.broadcast(plan_catalog.alias("p")), F.col("c.plan") == F.col("p.plan"))
        .select(
            F.col("subscription_id"),
            F.col("c.plan").alias("plan"),
            F.col("c.status").alias("status"),
            F.col("p.mrr").alias("mrr"),
            F.col("c.change_ts").alias("updated_at"),
        )
    )
    touched = upserts.select("subscription_id").union(deleted)
    survivors = dim_subscription_current.join(
        touched, "subscription_id", "left_anti"
    ).select(*cols)
    return survivors.unionByName(upserts.select(*cols))
```

```sql
-- ref_build_dim_subscription_sql_a
WITH collapsed AS (
  SELECT * FROM (
    SELECT c.*, ROW_NUMBER() OVER (
             PARTITION BY subscription_id ORDER BY change_ts DESC) AS rn
    FROM subscription_changes c
  ) WHERE rn = 1
),
applied AS (
  SELECT c.*, t.updated_at AS target_updated_at
  FROM collapsed c
  LEFT JOIN dim_subscription_current t USING (subscription_id)
  WHERE t.updated_at IS NULL OR c.change_ts > t.updated_at
),
upserts AS (
  SELECT a.subscription_id, a.plan, a.status, p.mrr, a.change_ts AS updated_at
  FROM applied a JOIN plan_catalog p ON a.plan = p.plan
  WHERE a.op <> 'D'
),
touched AS (SELECT subscription_id FROM applied)
SELECT t.subscription_id, t.plan, t.status, t.mrr, t.updated_at
FROM dim_subscription_current t
WHERE t.subscription_id NOT IN (SELECT subscription_id FROM touched)
UNION ALL
SELECT subscription_id, plan, status, mrr, updated_at FROM upserts
```

Note the SQL half uses `NOT IN` against `touched`. `subscription_id` is
non-nullable in both tables, so the Day 9 three-valued-logic hazard does not
fire — but `NOT IN` on a nullable key would return zero rows, and the DSL
half's `left_anti` has no such failure mode. Prefer `NOT EXISTS` or
`left_anti` as a habit.

### Route B — normalise, union, one ranking

Put the target row and the change rows into one shape and let a single
window pick the winner per key.

```python
def ref_build_dim_subscription_dsl_b(
    dim_subscription_current, subscription_changes, plan_catalog
):
    t_norm = dim_subscription_current.select(
        "subscription_id", F.lit("T").alias("op"), "plan", "status",
        F.col("updated_at").alias("ts"),
    )
    c_norm = subscription_changes.select(
        "subscription_id", "op", "plan", "status", F.col("change_ts").alias("ts")
    )
    w = Window.partitionBy("subscription_id").orderBy(F.col("ts").desc())
    winner = (
        t_norm.unionByName(c_norm).withColumn("rn", F.row_number().over(w))
        .filter("rn = 1").drop("rn")
    )
    return (
        winner.filter("op <> 'D'")
        .join(F.broadcast(plan_catalog.alias("p")), winner["plan"] == F.col("p.plan"))
        .select(
            winner["subscription_id"], winner["plan"], winner["status"],
            F.col("p.mrr").alias("mrr"), winner["ts"].alias("updated_at"),
        )
    )
```

**Route B is immune to the trap, and the reason is worth naming.** It never
compares a change against a target; it makes the target row *compete* in the
same ranking as the changes. A stale change simply loses to the target row on
`ts`, so the guard falls out of the ordering for free. There is no explicit
staleness predicate anywhere in Route B — which means a reviewer cannot find
one, and must reason about why its absence is fine here.

The corollary is the useful half: **if you write Route B and later add a
target row that has no timestamp, or unify on the wrong column, the
protection disappears silently** — there is no line of code to notice
missing. Route A's guard is explicit and auditable; Route B's is emergent.
That is a real engineering trade, not a style preference.

Route B also collapses stages 1 and 2 into one window, which is why it is
dramatically cheaper — see below.

---

## Part 5 — Concept takeaways

- **A change is not automatically newer than the row it updates.** Every
  upsert against a table that carries its own `updated_at` needs an explicit
  answer to "what if this change is older?", and the two answers — guard it,
  or make the target compete in the ranking — produce identical output with
  completely different auditability.
- **Collapsing the source batch to one row per key is not optional.** It is
  what Delta's `MERGE` enforces by throwing on multiple source matches; a
  hand-rolled merge has no such guard and will happily multiply rows or pick
  an arbitrary change.
- **A tombstone must be removed from two places**, not one: the upsert set
  and the surviving-target set. Filtering `op <> 'D'` early deletes the
  tombstone but leaves the target row alive — the subscription resurrects.
- The "system of record" for a derived measure is a **contract question, not
  a data question**. The feed's `mrr` and the catalog's `mrr` agree on four
  of five rows; agreeing most of the time is exactly what makes the wrong
  source survive code review.
- ISO-8601 fixed-width strings compare lexicographically in chronological
  order, which is why this day uses STRING rather than TIMESTAMP. That is a
  harness decision, not a modelling recommendation: a naive Python
  `datetime` handed to `createDataFrame` is interpreted in the **driver's
  local** zone, so `show()` and the ASCII boxes drift apart under a pinned
  `spark.sql.session.timeZone` while `collect()` round-trips. Measured on
  this machine: `datetime(2026,8,15,9,0)` displayed as `16:00:00` under
  session tz UTC.

## Physical plan notes

Measured with `df.explain()` (default physical plan, one line per node),
broadcast enabled, on the harness data:

| Route | Exchange | Scan | Window | SortMergeJoin | BroadcastHashJoin | Sort |
|---|---|---|---|---|---|---|
| A — collapse + guard + anti-join/union | **10** | 9 | 3 | 4 | 2 | 11 |
| B — union + one ranking | **2** | 3 | 1 | 0 | 1 | 2 |

The gap is **branch reuse, not the merge idea**. Route A derives
`collapsed` → `applied` and then forks it four ways (`deleted`, `upserts`,
`touched`, `survivors`); nothing is cached, so each branch recomputes the
whole windowed lineage — 9 scans of 3 input tables. Route B has a single
linear chain: union, one window, one broadcast join.

Do not conclude "anti-join is expensive". Caching or materialising `applied`
collapses most of Route A's gap; the honest statement is that a fork over an
un-cached windowed intermediate pays for that window once per branch. If
Stage 5 wants a fair comparison, re-measure Route A with `applied.cache()`
before drawing the performance conclusion.

## Echoes

- **Day 17** built SCD2 history from a feed but explicitly never merged into
  an existing target. This closes that gap and reuses its `row_number`
  run-collapsing idiom in stage 1.
- **Day 8** — keep-latest-per-key, and the reminder that `dropDuplicates`
  after an `orderBy` is not a contract. Stage 1 is that pattern verbatim.
- **Day 9** — `NOT IN` three-valued logic, live in the SQL half of Route A.
- **Day 16** — `COUNT(*)` vs `COUNT(col)` after a LEFT join, and the
  structure-dictates-defensive-code point: Route A needs an explicit guard
  where Route B needs none, purely because of the shape each chose.
- **Day 5** — `broadcast` on the small config dimension.
