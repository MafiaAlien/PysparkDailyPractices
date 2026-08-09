# Day 19 — Higher-order array functions — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

## The trap

**What it is:** `forall` over an **empty** array returns **`true`**, not
`false` and not NULL — vacuous truth, the identity element of AND. Once the
`filter(items, x -> x.status = 'ACTIVE')` removes every element of an order,
`forall(<empty>, x -> x.in_stock)` reports `true`, so an order with **zero**
ACTIVE line items is declared fully in stock. The spec's clause "TRUE only if
the order has AT LEAST ONE ACTIVE line item AND ..." is exactly the guard that
suppresses it; `size(active) > 0 AND forall(...)` is **load-bearing**, not
defensive.

**Why a naive solution passes anyway:** on any order that still has at least
one ACTIVE line after the filter, the guard is a no-op — `size > 0` is
already true, so guarded and unguarded expressions are literally the same
value. Four of the five test orders (O1, O2, O3, O5) have ACTIVE lines. And
on the one order that doesn't, the *other three* measures come out right by
themselves, because their empty-array defaults happen to match the spec:

| measure | empty-array result | spec wants | verdict |
|---|---|---|---|
| `size(active)` | `0` | 0 | correct |
| `aggregate(active, 0.0D, ...)` | `0.0` (the zero value) | 0.0 | correct |
| `exists(active, qty>=10)` | `false` (OR identity) | false | correct |
| `forall(active, in_stock)` | **`true`** (AND identity) | **false** | **WRONG** |

So the failure is a **single cell** in a 5x5 output grid — one boolean in one
row, with the other four columns of that same row correct. Measured, naive vs
guarded differ *only* at `O4.all_in_stock`.

**Which row exposes it:** **O4** — `[{D1,5,20.0,CANCELLED,true},
{D2,15,3.0,CANCELLED,true}]`. Both lines are CANCELLED, so the filtered array
is empty. Both are also `in_stock = true`, which closes the back door: even a
solution that *forgot the filter* and ran `forall` over the raw array would
still say `true` there, so the row cannot be rescued by a different mistake.
O4's `D2` has `qty = 15`, so the same row double-checks that `has_bulk`
actually filters (unfiltered would give `true`); likewise `O2.B2` is a
CANCELLED line with `qty 20 @ 100.0` and `in_stock=false`, whose survival
would show up loudly in `active_total` (2048.0 instead of 48.0).

**Reading-time tell:** two sibling functions from the same family sit next to
each other in the code, applied to the same possibly-empty array, and only one
of them is guarded — or neither is. `exists` and `forall` are De Morgan duals
with **opposite** empty-collection identities (`false` / `true`); any code
that treats them as symmetric is asserting they behave the same on the empty
case. The other tell is textual: the spec spells out "at least one ACTIVE
line item AND ...", a two-clause predicate, and the code contains one clause.

---

## Part 4 — Reference answers

### Route A — filter once, then four questions (the intended route)

```python
def ref_solve_dsl_a(df):
    active = F.filter("items", lambda x: x["status"] == "ACTIVE")
    return df.select(
        "order_id",
        F.size(active).alias("n_active"),
        F.aggregate(
            active,
            F.lit(0.0),
            lambda acc, x: acc + x["qty"] * x["unit_price"],
        ).alias("active_total"),
        F.exists(active, lambda x: x["qty"] >= 10).alias("has_bulk"),
        ((F.size(active) > 0)
         & F.forall(active, lambda x: x["in_stock"])).alias("all_in_stock"),
    )
```

`active` is a Column expression, not a materialized column — reusing the
Python variable inlines the same `filter(...)` subtree four times. Catalyst
does **not** CSE it away in the physical plan (the executed plan below shows
`filter(items, ...)` written out five times, once per use plus the `size`
inside the guard). Assigning it with `withColumn("active", ...)` and dropping
it afterwards produces one evaluation instead of five and reads better; on 14
rows it is invisible, and either way the node count is identical.

```sql
-- ref_solve_sql_a
SELECT
    order_id,
    size(filter(items, x -> x.status = 'ACTIVE'))                     AS n_active,
    aggregate(filter(items, x -> x.status = 'ACTIVE'),
              CAST(0 AS DOUBLE),
              (acc, x) -> acc + x.qty * x.unit_price)                 AS active_total,
    exists(filter(items, x -> x.status = 'ACTIVE'), x -> x.qty >= 10) AS has_bulk,
    size(filter(items, x -> x.status = 'ACTIVE')) > 0
      AND forall(filter(items, x -> x.status = 'ACTIVE'),
                 x -> x.in_stock)                                     AS all_in_stock
FROM orders
```

A `CROSS JOIN LATERAL`-free way to name the filtered array once is a CTE with
`filter(...) AS active_items`, then referencing `active_items` four times —
same plan, much less repetition. Worth writing that way.

### Route B — explode, aggregate, join back (the route the spec forbids)

```python
def ref_solve_dsl_b(df):
    it = F.col("it")
    agg = (df.select("order_id", F.explode("items").alias("it"))
             .filter(it["status"] == "ACTIVE")
             .groupBy("order_id")
             .agg(F.count("*").alias("n_active"),
                  F.sum(it["qty"] * it["unit_price"]).alias("active_total"),
                  F.max(it["qty"] >= 10).alias("has_bulk"),
                  F.min(it["in_stock"]).alias("all_in_stock")))
    return (df.select("order_id").join(agg, "order_id", "left")
              .select("order_id",
                      F.coalesce("n_active", F.lit(0)).cast("int").alias("n_active"),
                      F.coalesce("active_total", F.lit(0.0)).alias("active_total"),
                      F.coalesce("has_bulk", F.lit(False)).alias("has_bulk"),
                      F.coalesce("all_in_stock", F.lit(False)).alias("all_in_stock")))
```

The same trap reappears wearing different clothes, and **inverted**: after
`explode` + `filter`, order O4 has no rows at all, so it vanishes from the
`groupBy` entirely (Day 4's disappearing-row failure). The LEFT join back to
the order list resurrects it with NULLs, and `COALESCE(all_in_stock, false)`
supplies the answer that Route A had to *suppress*. Note the direction: SQL's
`min(in_stock)` / `bool_and(in_stock)` over an **empty group** returns
**NULL**, whereas `forall` over an **empty array** returns **true**. Two
encodings of "for all x in S", opposite defaults on `S = {}`.

### Route B' — explode_outer + conditional aggregation, no join

```python
def ref_solve_dsl_bp(df):
    it = F.col("it")
    act = it["status"] == "ACTIVE"
    return (df.select("order_id", F.explode_outer("items").alias("it"))
              .groupBy("order_id")
              .agg(F.count(F.when(act, 1)).cast("int").alias("n_active"),
                   F.coalesce(F.sum(F.when(act, it["qty"] * it["unit_price"])),
                              F.lit(0.0)).alias("active_total"),
                   F.coalesce(F.max(F.when(act, it["qty"] >= 10)),
                              F.lit(False)).alias("has_bulk"),
                   F.coalesce(F.min(F.when(act, it["in_stock"])),
                              F.lit(False)).alias("all_in_stock")))
```

Strictly better than Route B: pushing the ACTIVE test from a `WHERE` into the
aggregates keeps every order in the group set, which deletes the join and one
of the two shuffles. `explode_outer` here guards the *empty items array* case
(not in this data, but free); with a plain `explode` plus conditional aggs the
result is identical on this input.

**Tradeoff, measured (see plan notes):** Route A does **0** shuffles — every
measure is a within-row expression, the whole query is one `Project` over the
scan. Route B' costs **1** Exchange, Route B costs **2** plus a broadcast.
This is the one topic where the HOF route wins on plan shape and not merely
on style: the array is already colocated with its key, so grouping it is
re-deriving a grouping that the storage layout already gave you for free.
That advantage is entirely a property of the *denormalized layout*, not of
higher-order functions as such — the moment the line items live in their own
table, Route B' is the only route there is.

---

## Part 5 — Concept takeaways

- **Empty-collection identities are the whole topic.** `forall([]) = true`
  (AND identity), `exists([]) = false` (OR identity), `aggregate([], z, ...)
  = z`, `size([]) = 0`. Three of the four match the "obvious" answer and one
  does not, which is precisely why `forall` is the one that gets shipped
  broken. Verified: `SELECT forall(array(), x -> x), exists(array(), x -> x)`
  → `true, false`.
- **Empty array and empty group are different objects with different
  defaults.** `forall` over `[]` is `true`; `bool_and` / `min` over a group
  with zero rows is `NULL` (and after a LEFT join, absent entirely). Same
  question, three different "no data" encodings — Day 16 logged the
  `COUNT(*)` vs `COUNT(col)` version of this; the HOF version flips the sign.
- **`aggregate`'s zero value fixes the accumulator type, and the merge
  lambda must return exactly that type.** No implicit widening. Both of these
  fail at analysis time with
  `AnalysisException [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]`:
  `F.aggregate(items, F.lit(0), lambda acc, x: acc + x.qty * x.unit_price)`
  ("the third parameter requires the `INT` type"), and in SQL the bare
  literal `0.0`, which is **`DECIMAL(1,1)`**, not a double ("requires the
  `DECIMAL(1,1)` type"). Write `F.lit(0.0)` in DSL and `CAST(0 AS DOUBLE)`
  (or `0.0D`) in SQL. A loud failure, not a silent one — but it is the single
  most common first-run error with `aggregate`.
- **DSL lambdas take Columns, and struct fields are subscripted, not
  attributes.** `lambda x: x["status"] == "ACTIVE"` — `x.status` works too via
  `Column.__getattr__`, but `x["qty"]` is the form that survives a field named
  like a Column method (`cast`, `name`, `alias`). The array argument may be a
  plain string column name: `F.filter("items", ...)`.
- **PySpark 4.1.1 does have the aggregate-side duals** (verified by
  `hasattr`): `F.bool_and` / `F.every`, `F.bool_or` / `F.some`, plus
  `F.reduce` as an alias of `F.aggregate`, `F.array_compact`, `F.zip_with`.
  Unlike Day 12's `F.grouping_sets`, none of these is a hallucination — but
  the DSL/SQL naming split still holds (`F.filter` is a *function* on arrays,
  `DataFrame.filter` is a row filter; they share a name and share nothing
  else).
- **The HOF route is shuffle-free by construction.** Any per-key measure that
  can be phrased as a function of one array in one row costs zero Exchanges,
  because the "grouping" was done by whoever wrote the array. Reaching for
  `explode` + `groupBy` on a denormalized array converts a free grouping into
  a paid one — and if you then need the empty groups back, into a join too.
- **ANSI note:** nothing here touches ANSI's failure surface —
  `filter`/`exists`/`forall`/`aggregate` never index the array, so no
  `element_at` / `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` exposure (contrast Day
  14's AI SQL). `size(NULL)` returns `-1` when
  `spark.sql.legacy.sizeOfNull=true` and `NULL` otherwise (4.x default:
  `NULL`); this data has no NULL arrays, but a `size(x) > 0` guard against a
  NULL array yields NULL, not false.

## Physical plan notes

Measured on Spark 4.1.1, `local[2]`, `shuffle.partitions=4`, AQE on (default),
`executedPlan` after an actual `.collect()`.

**Route A** — `Exchange=0  Generate=0  HashAggregate=0  Sort=0  Join=0`

```
Project [order_id#0, size(filter(items#1, ...)) AS n_active#31,
         aggregate(filter(items#1, ...), 0.0, ...) AS active_total#32,
         exists(filter(items#1, ...), ...) AS has_bulk#33,
         ((size(filter(items#1, ...)) > 0) AND forall(filter(items#1, ...), ...)) AS all_in_stock#34]
+- *(1) Scan ExistingRDD[order_id#0,items#1]
```

Two nodes total, no stage boundary. `filter(items#1, ...)` appears **5×** —
no common-subexpression elimination at the plan level, confirming the note in
Route A above.

**Route B'** (explode_outer + conditional agg) — final plan
`Exchange=1  Generate=1  HashAggregate=2 (partial+final)  Join=0`

```
*(2) HashAggregate(keys=[order_id#0], functions=[count(CASE WHEN ... END), sum(...), max(...), min(...)])
+- AQEShuffleRead coalesced
   +- ShuffleQueryStage 0
      +- Exchange hashpartitioning(order_id#0, 4), ENSURE_REQUIREMENTS
         +- *(1) HashAggregate(keys=[order_id#0], functions=[partial_count(...), ...])
            +- *(1) Project [order_id#0, it#3.status AS _extract_status#46, ...]
               +- *(1) Generate explode(items#1), [order_id#0], true, [it#3]
                  +- *(1) Scan ExistingRDD[order_id#0,items#1]
```

All four aggregates are partial-aggregable → `HashAggregate`, not
`SortAggregate` (contrast Day 17: `min`/`max` on a **string** forces
SortAggregate; here `min`/`max` are on a **boolean**, fixed-width buffer).
Note `Generate explode(items#1), ..., true` — the trailing `true` is the
`outer` flag.

**Route B** (explode + WHERE + groupBy + LEFT join back) — final plan
2 shuffle `Exchange` + 1 `BroadcastExchange`, 1 `Generate`, 1
`BroadcastHashJoin`. The initial plan is a `SortMergeJoin` with 2 Exchanges +
2 Sorts; AQE rewrites it to a broadcast join because the 5-row aggregate side
is tiny. The extra Exchange over Route B' is the `df.select("order_id")` probe
side of the join — the price of having deleted O4 and needing it back.

Ordering: **A (0) < B' (1) < B (2 + broadcast)**. The gap between B' and B is
not "one more shuffle for the join" in the abstract — it is that the `WHERE
status='ACTIVE'` destroyed a group, and re-materializing the full key set
costs a second scan of `orders` plus its own Exchange.

## Echoes

- **Day 4** (array columns) — direct descendant. Day 4's lesson was "the empty
  array row vanishes under `explode`"; Day 19's is "the empty array row
  *survives* under HOFs and lies to you." The failure mode inverts when you
  stop exploding: a missing row is loud, a wrong boolean is quiet.
- **Day 16** (NULL semantics) — "after a LEFT join `COUNT(*)` reports 1 for an
  empty group, `COUNT(col)` reports 0" is the same class of question: what
  does an aggregate mean when there is nothing to aggregate. Route B is
  literally that entry replayed with `bool_and`.
- **Day 15 / Day 16** (load-bearing vs dead code) — the `size(active) > 0`
  conjunct is **load-bearing** in Route A and **dead** in Route B' (where
  `COALESCE(..., false)` carries the same weight instead). Third instance of
  the pattern "whether a guard is required is dictated by the structure you
  chose, not by the spec."
- **Day 18** (`collect_set` → `flatten` → `array_distinct` → `size`) and
  **Day 14** (AI's hand-rolled 5-layer `transform`/`filter`/`element_at`
  nest) — both were HOF usage discovered sideways through AI review. This day
  is the direct drill those two kept pointing at.
- **Day 12** (`F.grouping_sets` does not exist) — the counter-example: here
  the plausible-sounding names (`F.bool_and`, `F.every`, `F.reduce`,
  `F.zip_with`) all really do exist in 4.1.1. Checked with `hasattr`, not
  from memory.
