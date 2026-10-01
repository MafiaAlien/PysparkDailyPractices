# Day 26 — stg_replenishment_request — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

ETL layer : L1 staging / cleansing
Domain    : inventory replenishment
Failure mode (the trap axis — never stated in the day file):
            **nullable key** — a nullable column inside a composite lookup
            key, where NULL on both sides means the same thing ("single pack
            configuration") and `=` refuses to match it. The row it breaks is
            a **false orphan**: every key it carries is valid, and the lookup
            still comes back empty.

## The trap

**What it is:** `units_per_case` is looked up on the composite key
`(sku, pack_code)`, and `pack_code` is NULL on **both** sides for a
single-pack SKU. `NULL = NULL` is UNKNOWN, so an ordinary equi-join never
matches that row: the request's store is known, its sku is known and ACTIVE,
the pack row it names exists in `sku_pack`, and the lookup still misses. The
join has to be null-safe on `pack_code` (`eqNullSafe` / `<=>`) or both sides
have to be pre-filled with the same collision-free sentinel.

**Why a naive solution passes anyway:** on every row whose `pack_code` is
non-NULL, `=` and `<=>` are the same predicate. Three of the four ACCEPTED
requests (R01, R02, R07) carry a pack code, and so do all the multi-pack
SKUs, so the naive join resolves them all correctly. On top of that, **R06 is
the decoy**: it is the other NULL-pack request (SKU-103, single pack), its
pack lookup misses under `=` just like R03's does, and its output row is
still **exactly right**. R06 is DISCONTINUED, and the contract NULLs out
`qty_units` on every QUARANTINED row, so the missing lookup is invisible.
It looks as though the job has already survived a NULL pack code. It has
not; that row never needed the lookup.

**Which row exposes it:** `raw_replenishment_request.R03`: S03 / SKU-102 /
`pack_code = NULL` / 3 cases. `sku_pack` holds `(SKU-102, NULL, 12)`, so the
correct output is ACCEPTED, DC-WEST, `qty_units = 36`. R03 is the only
request that reaches the ACCEPTED branch with a NULL pack code.

**Failure shape depends on the route, and that is the point of the day.**
All three naive routes below were checked or derived against this data:

| Naive route | What happens to R03 | Caught by |
|---|---|---|
| **X — split + union**: ACCEPTED = inner joins through `sku_pack`; QUARANTINED = anti-joins for the three R1 reasons; union | Matches no inner join and fires no R1 rule, so it **vanishes**. Output has 7 rows. | **P1** conservation: executed, `in=8 out=7` |
| **Y — left-join chain + CASE**: `=` on `pack_code` | Comes out ACCEPTED / DC-WEST with **`qty_units = NULL`** (`3 * NULL`). Row count stays 8. | **P2** consistency (ACCEPTED with NULL `qty_units`): executed, 1 bad row |
| **Y′ — UNKNOWN_SKU derived from the composite pack join** (`p.sku IS NULL`) instead of `sku_master` | Comes out QUARANTINED / `UNKNOWN_SKU`, with `dc_code` and `qty_units` NULL as a quarantined row should have. | **Nothing.** P1 and P2 both pass and only `check()` sees it. Derived by reading, not executed. |

Route Y′ is the reason not to over-credit the assertions. P1 catches a row
that is *lost*, and P2 catches a row that is *internally inconsistent*. A
row that is **consistently misrouted**, with a plausible reason and the
right NULLs, passes every shape check the contract can state. Store ops
would open a ticket saying "SKU-102 is unknown", find it in `sku_master`,
and have no idea why.

**Reading-time tell:** the day file states it outright, spread over three
places:

- the `raw_replenishment_request` note: `pack_code` is NULL for a
  single-pack SKU
- the `sku_pack` note: a single-pack SKU has exactly one row, with
  `pack_code` NULL
- the upstream guarantee: every request whose sku exists names a
  `(sku, pack_code)` that `sku_pack` holds

Put the three together and a NULL pack code is a **valid key value**, and
the join has to match it. The review question is one line: **what does the
pack join do when `pack_code` is NULL on both sides?** Two boxes show NULL
in that column, one in each table.

**Which pipeline stage it lives in:** stage 2, **resolve**, in the pack
lookup only. Stages 1, 3 and 4 are identical between the naive and correct
solutions. The route decides whether stage 4 reports the damage (X, Y) or
publishes it (Y′).

---

## Production constraints — audit

| # | Constraint | Satisfied by | How it fails if ignored |
|---|---|---|---|
| P1 | Every request in the landing batch must come out of the job exactly once — ACCEPTED or QUARANTINED, never dropped, never duplicated. Before returning, the job must assert it: count(ACCEPTED) + count(QUARANTINED) equals the row count of raw_replenishment_request, and `request_id` is unique in the output. | `raw.count()` taken on the **raw input** (not on a normalized or joined intermediate), compared with `out.count()` and `out.select("request_id").distinct().count()`. An `if … raise`, never a bare `assert` on a DataFrame object (Day 25). | Route X silently publishes 7 rows. Joining `sku_pack` on `sku` alone fans out SKU-101 and SKU-104 (two packs each), so R01, R02, R04 and R07 come out twice. The uniqueness half catches that. Counting the input *after* an inner join makes the assertion a tautology: it compares the output with something already filtered. |
| P2 | Before returning, the job must also assert its routing is consistent: every ACCEPTED row has a non-NULL `dc_code` and `qty_units` and a NULL `reject_reason`; every QUARANTINED row has a non-NULL `reject_reason` and a NULL `dc_code` and `qty_units`. | One filter that ORs the four violation shapes, plus `load_status IS NULL OR load_status NOT IN (...)`, then `.count()` must be 0. | Route Y publishes R03 as ACCEPTED with no quantity. Allocation reads it and sends zero units, or crashes on the NULL. |
| P3 | Read only the columns this job needs from the three reference tables. `store_name` and `buyer_id` are operational metadata and must not enter the pipeline. | An explicit projection per reference table before any join. | No observable symptom in output or plan: Catalyst's ColumnPruning removes both columns above the Scan either way (see plan notes; `store_name` and `buyer_id` never reach a `BroadcastExchange`). Grade this as **production robustness** (two branches with different hygiene), not performance. This is the same finding as Day 24/25. |

---

## Pipeline stages

1. **conform**: project the landing feed down to the five columns and apply
   N1 (`upper(trim(x))` on the three keys). `upper(trim(NULL))` is NULL.
2. **resolve**: three left lookups. `store_code` → `dc_code`,
   `sku` → `sku_status`, `(sku, pack_code)` → `units_per_case`, with
   **`pack_code` compared null-safe**. ← **the trap lives here**
3. **route**: the R1 first-match CASE decides `reject_reason`, then
   `load_status` and the masking of `dc_code` / `qty_units` are both derived
   from `reject_reason IS NULL`. There is one source of truth, so the two can
   never disagree.
4. **assert & publish**: P1 conservation + uniqueness, P2 consistency, then
   return.

A structural note on stage 3: `UNKNOWN_SKU` must come from `sku_master` (the
contract says so) and **never** from the pack lookup. Stage 2 resolves three
independent facts, and stage 3 may only read each fact from the table that
owns it. Route Y′ is exactly this rule broken.

---

## Part 4 — Reference answers

All three routes below were executed against the harness data (Spark 4.1.1,
local[2]) and PASS.

### Shared assertion (both routes, both languages)

```python
def _assert_contract(raw: DataFrame, out: DataFrame) -> DataFrame:
    out = out.cache()
    n_in = raw.count()                       # the RAW input, not an intermediate
    n_out = out.count()
    n_ids = out.select("request_id").distinct().count()
    if n_out != n_in or n_ids != n_in:
        raise ValueError(f"conservation broken: in={n_in} out={n_out} ids={n_ids}")
    bad = out.filter(
        ((F.col("load_status") == "ACCEPTED")
         & (F.col("dc_code").isNull() | F.col("qty_units").isNull()
            | F.col("reject_reason").isNotNull()))
        | ((F.col("load_status") == "QUARANTINED")
           & (F.col("reject_reason").isNull() | F.col("dc_code").isNotNull()
              | F.col("qty_units").isNotNull()))
        | F.col("load_status").isNull()
        | ~F.col("load_status").isin("ACCEPTED", "QUARANTINED")
    ).count()
    if bad:
        raise ValueError(f"status consistency broken: {bad} rows")
    return out
```

`n_out == n_in` together with `n_ids == n_in` is the full P1 contract: no row
lost, no row doubled, and no pair of errors that cancel out. Checking only
`n_out == n_in` misses a batch that drops one request and duplicates
another.

### Route A: left-join chain, null-safe pack key

```python
KEYS = ["store_code", "sku", "pack_code"]

def ref_build_stg_replenishment_request_dsl_a(
        raw_replenishment_request, store_dim, sku_master, sku_pack):
    req = raw_replenishment_request.select(
        "request_id",
        *[F.upper(F.trim(F.col(c))).alias(c) for c in KEYS],
        "qty_cases",
    )
    st = store_dim.select("store_code", F.col("dc_code").alias("st_dc_code"),
                          F.lit(True).alias("store_found"))
    sm = sku_master.select("sku", "sku_status")
    sp = sku_pack.select(F.col("sku").alias("p_sku"),
                         F.col("pack_code").alias("p_pack_code"),
                         "units_per_case")

    j = (req.join(F.broadcast(st), "store_code", "left")
            .join(F.broadcast(sm), "sku", "left")
            .join(F.broadcast(sp),
                  (req.sku == sp.p_sku)
                  & req.pack_code.eqNullSafe(sp.p_pack_code),   # <- the fix
                  "left"))

    reason = (F.when(F.col("store_found").isNull(), "UNKNOWN_STORE")
               .when(F.col("sku_status").isNull(), "UNKNOWN_SKU")
               .when(F.col("sku_status") == "DISCONTINUED", "DISCONTINUED_SKU"))
    j = j.withColumn("reject_reason", reason)
    acc = F.col("reject_reason").isNull()
    out = j.select(
        "request_id", "store_code", "sku", "pack_code",
        F.when(acc, F.col("st_dc_code")).alias("dc_code"),
        F.when(acc, F.col("qty_cases") * F.col("units_per_case")).alias("qty_units"),
        F.when(acc, F.lit("ACCEPTED")).otherwise(F.lit("QUARANTINED")).alias("load_status"),
        "reject_reason",
    )
    return _assert_contract(raw_replenishment_request, out)
```

`store_found` is a literal `true` carried through the join. It makes
"store unresolved" independent of whether `dc_code` happens to be nullable.
The contract says `dc_code` is never NULL, but the routing decision should
not depend on that claim.

```sql
-- ref_build_stg_replenishment_request_sql_a
WITH req AS (
    SELECT request_id,
           UPPER(TRIM(store_code)) AS store_code,
           UPPER(TRIM(sku))        AS sku,
           UPPER(TRIM(pack_code))  AS pack_code,
           qty_cases
    FROM raw_replenishment_request
),
resolved AS (
    SELECT /*+ BROADCAST(s, m, p) */
           r.request_id, r.store_code, r.sku, r.pack_code, r.qty_cases,
           s.store_code AS s_store_code, s.dc_code,
           m.sku_status,
           p.units_per_case
    FROM req r
    LEFT JOIN (SELECT store_code, dc_code FROM store_dim) s
           ON r.store_code = s.store_code
    LEFT JOIN (SELECT sku, sku_status FROM sku_master) m
           ON r.sku = m.sku
    LEFT JOIN (SELECT sku, pack_code, units_per_case FROM sku_pack) p
           ON r.sku = p.sku AND r.pack_code <=> p.pack_code      -- the fix
),
routed AS (
    SELECT *,
           CASE WHEN s_store_code IS NULL        THEN 'UNKNOWN_STORE'
                WHEN sku_status IS NULL          THEN 'UNKNOWN_SKU'
                WHEN sku_status = 'DISCONTINUED' THEN 'DISCONTINUED_SKU'
           END AS reject_reason
    FROM resolved
)
SELECT request_id, store_code, sku, pack_code,
       CASE WHEN reject_reason IS NULL THEN dc_code END                    AS dc_code,
       CASE WHEN reject_reason IS NULL THEN qty_cases * units_per_case END AS qty_units,
       CASE WHEN reject_reason IS NULL THEN 'ACCEPTED' ELSE 'QUARANTINED' END AS load_status,
       reject_reason
FROM routed
```

The SQL function returns `_assert_contract(raw_replenishment_request,
spark.sql(sql))`: SQL computes the table and Python asserts on it.

### Route B: sentinel pre-fill on both sides

```python
SENT = "<SINGLE>"   # written ONCE; both sides must use the identical literal

req = req.withColumn("pack_key", F.coalesce("pack_code", F.lit(SENT)))
sp  = sku_pack.select("sku",
                      F.coalesce("pack_code", F.lit(SENT)).alias("pack_key"),
                      "units_per_case")
... .join(F.broadcast(sp), ["sku", "pack_key"], "left") ...
```

Everything else is identical to Route A. The output still selects
`pack_code`, never `pack_key`, so the sentinel never leaks into the
published table. That is the Day 16 "key column doing two jobs" pitfall,
avoided structurally by giving the key its own name.

**The tradeoff here is correctness, not performance.** The routes are
node-level isomorphic on this data (see plan notes). Route B works only
while the sentinel cannot collide with a real value. **Today's N1 rule
creates a specific collision:** a pack code keyed as whitespace only
(`'  '`) becomes `''` after `trim`. Pick `''` as the sentinel, which is the
obvious choice, and that garbage request silently resolves to the
single-pack row. `<=>` cannot collide. Catalyst lowers it to the key pair
`coalesce(pack_code, '')` + `isnull(pack_code)` (visible in the plan below):
it does use `''` as a sentinel, but pairs it with a boolean flag, and the
flag is what separates `''` from NULL.

### Not a route: `NOT IN` for the orphan checks

Route X's anti-joins are NULL-correct: `left_anti` and `NOT EXISTS` treat a
NULL key as "no match". The SQL spelling
`sku NOT IN (SELECT sku FROM sku_master)` is safe **today only** because
neither `sku_master.sku` nor any normalized request `sku` is NULL. A single
NULL `sku` on either side changes the result (log/04 "NOT IN vs NOT EXISTS"):
the outer row, or every row, drops out of the reject set. Under Route X
that means the requests vanish, which is the same failure shape as today's
trap reached through a different operator. This data does not exercise it,
so it is not scored, but it belongs to the same family.

---

## Part 5 — Concept takeaways

- **A false orphan is an orphan-detection bug, not a data bug.** Every key
  R03 carries is valid; the *comparison* is what fails. The generic review
  question for any composite lookup is: *can any key part be NULL on both
  sides, and does NULL mean a real value there?* When the answer is yes,
  `=` is wrong and `<=>` / `eqNullSafe` is right. This is Day 16's
  `variant` finding moved from a report query to a staging job.
- **The shape of the pipeline decides the shape of the failure.**
  Split + union (Route X) turns the missed lookup into a *lost row*. A
  left-join chain (Route Y) turns it into a *NULL metric*. Reading an
  orphan status off the wrong table (Route Y′) turns it into a *wrong
  reason*. The bug is the same in all three and so is the stage; the
  symptoms fall in three different classes.
- **Conservation assertions finally have teeth, and their limit is now
  measurable.** P1 catches Route X, which no shape assertion from Day
  23/24/25 could have done. P2 catches Route Y. **Neither catches Route
  Y′**, because a consistently misrouted row passes every assertion that
  describes the output's own shape. Assertion coverage is a map: loss →
  count conservation; internal contradiction → consistency rules;
  plausible wrong classification → only a comparison against something
  outside the job (expected data, a reconciliation, a human).
- **Count the input where it enters, not where it has been cleaned.** A
  conservation check whose denominator is taken after a filter or an inner
  join is a tautology. It is the Day 25 `assert df is not None` problem one
  level up: the check runs, and it cannot fail.
- **Derive every routing column from one decision.** `load_status`,
  `dc_code` and `qty_units` all branch on `reject_reason IS NULL`. Three
  independent CASE expressions can disagree with each other, and P2 would
  then catch a bug the structure should have made impossible.
- **Sentinel vs `<=>`, with a new collision point**: a normalization step
  (`trim`) can *manufacture* the sentinel value from dirty input. Catalyst's
  own lowering of `<=>` (`coalesce(x,'') + isnull(x)`) shows the
  collision-free form of a sentinel: the fill value plus a flag.
- Whitelist techniques exercised: nullable keys (`<=>` vs sentinel), orphan
  keys, left / anti / semi joins, `broadcast`, conditional expressions
  routed by precedence, key normalization. No new API.

## Physical plan notes

Measured with Spark 4.1.1, local[2], AQE on (default), explicit broadcast
hints on all three reference tables:

| Route | shuffle Exchange | BroadcastExchange | BroadcastHashJoin | Sort | HashAggregate |
|---|---|---|---|---|---|
| A — DSL, `eqNullSafe` | 0 | 3 | 3 | 0 | 0 |
| A — SQL, `<=>` | 0 | 3 | 3 | 0 | 0 |
| B — DSL, sentinel | 0 | 3 | 3 | 0 | 0 |

All three are node-level isomorphic. **There is no performance story here,
and none should be invented.** The only visible difference is the pack
join's key list:

```
Route A:  BroadcastHashJoin [sku, coalesce(pack_code, ), isnull(pack_code)],
                            [p_sku, coalesce(p_pack_code, ), isnull(p_pack_code)], LeftOuter
Route B:  BroadcastHashJoin [sku, pack_key], [sku, pack_key], LeftOuter
```

That is three hash keys against two, both inside one broadcast hash lookup.

P3 is confirmed by the same plans: `store_dim` and `sku_master` are scanned
with `store_name` / `buyer_id` and projected away immediately
(`Project [store_code, dc_code …]` / `Project [sku, sku_status]`) before
their `BroadcastExchange`.

Day 16's partition-reuse argument for the sentinel (one Exchange fewer under
forced SMJ) **does not apply** to this job. There is no upstream `GROUP BY`
whose partitioning a join could reuse, and all three lookups are broadcast.
The no-hint / AQE-off variant is **NOT MEASURED**.

## Echoes

- **Day 16** — log/04 "NULL 语义总表" + "哨兵路线 vs `<=>` 路线":
  `= 在 NULL 上不自反`, and the four-dimension sentinel-vs-`<=>` tradeoff.
  Today reinforces it in an ETL frame and adds one refinement: the
  sentinel's collision risk can be *created by the job's own cleansing
  step*.
- **Day 16** — log/04 "null-safe join 的物理形态": `<=>` desugars to
  `coalesce(x,'')` + `isnull(x)`. Re-observed today on a BroadcastHashJoin,
  unchanged.
- **Day 23/24/25** — "断言只能观察活着进入输出的东西": today *refines*
  rather than repeats it. A conservation assertion is the first check in
  this project that observes a row which did **not** survive, and it
  catches Route X. The general rule becomes "each assertion class sees one
  failure class", and Route Y′ is the class no in-job assertion sees.
- **Day 24/25** — P3 has zero symptom under Catalyst ColumnPruning. Third
  confirmation.
- **Day 9 / log/04 "NOT IN vs NOT EXISTS"**: named as the sibling
  operator that would produce the same "vanishing row" shape. Not
  exercised.
