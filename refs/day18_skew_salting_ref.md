# Day 18 — Skew handling: salted join + two-phase aggregation — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

## The trap

**What it is:** `n_products` is a DISTINCT count, and a distinct count is **not
decomposable over salt buckets**. The natural two-phase shape —
`partial: COUNT(DISTINCT product) per (customer, salt)` then
`final: SUM(part_n)` — double-counts every product that appears in more than
one bucket. `total_amount` in the very same query is fine, because `SUM` *is*
decomposable. So the salted route returns a row that is **half right**: correct
money, inflated cardinality.

**Why a naive solution passes anyway:** it is correct on 3 of the 4 output rows,
i.e. on every cold customer.

- **C1** — orders 10, 13, 16 all satisfy `id % 3 == 1`, so C1 lives entirely in
  **one bucket**. Per-bucket distinct == true distinct. C1 is also the only cold
  customer with a **repeated product** (`Q_A` twice), so a reviewer who
  explicitly asks "does this handle a customer ordering the same product twice?"
  tests it, sees `2`, and concludes yes.
- **C2** — spans buckets 2 and 0, but its two products differ, so no product
  crosses a bucket boundary. `1 + 1 = 2`. Correct.
- **C3** — a single order. Correct.

The clean-data assumption being smuggled in is *"a group's rows land in one
bucket, or its repeats land together"* — which is precisely what stops being
true once a key is big enough to need salting at all.

**Which row exposes it:** `C_HOT`, and only `C_HOT`. Its products are seeded to
straddle bucket boundaries in all three directions:

| product | orders (salt) | buckets |
|---|---|---|
| P_A | 1 (s1), 2 (s2), 6 (s0) | all three |
| P_B | 3 (s0), 4 (s1) | two |
| P_C | 5 (s2), 7 (s1) | two |
| P_D | 9 (s0) | one |

Per-bucket distinct counts are `bucket0 = {P_B,P_A,P_D} = 3`,
`bucket1 = {P_A,P_B,P_C} = 3`, `bucket2 = {P_A,P_C} = 2` -> `SUM = 8`.
True answer `4`. **Measured**, both DSL and SQL forms:
`('C_HOT','ENTERPRISE', 505.0, 8)` vs expected `('C_HOT','ENTERPRISE', 505.0, 4)`.
Note the amount is right — 505.0 either way.

**Reading-time tell:** a `SUM(...)` sitting on top of a partial
`COUNT(DISTINCT ...)`. The check is mechanical and needs no data: for every
measure, ask **"is `f(A U B)` computable from `f(A)` and `f(B)` alone?"**
`SUM` / `COUNT` / `MIN` / `MAX` / `collect_set` — yes. `COUNT(DISTINCT)`,
`AVG` recombined as an average-of-averages, `MEDIAN`, any percentile,
`FIRST`/`LAST` — no. A salt bucket is an arbitrary partition of the group, so
**every** measure in a two-phase aggregation has to clear that bar
independently. This is the same "which rows is this expression actually
ranging over" discipline as Day 12's `COUNT(when(...))` vs `SUM(when(...))`,
one level up: there the question was which rows the aggregate sees, here it is
whether the aggregate survives being split.

---

## Part 4 — Reference answers

All three routes below were executed against the Day 18 harness and **all three
PASS**. That is the point: rules 2 and 3 constrain the route, not the result.

### Shared salting step

```python
N_SALT = 3

def salted(orders, customers):
    so = orders.withColumn("salt", F.pmod(F.col("order_id"), F.lit(N_SALT)))
    sc = customers.withColumn(
        "salt", F.explode(F.sequence(F.lit(0), F.lit(N_SALT - 1)))
    )
    return so.join(sc, on=["customer_id", "salt"], how="inner")
```

Two invariants live in these four lines:

1. The fact-side salt **range** and the dim-side replication **range** must be
   the same set. `pmod(x, 3)` yields `{0,1,2}`; `sequence(0, 2)` yields
   `{0,1,2}`. `sequence(1, 3)` would drop every row of an inner join and NULL
   every row of a left join — loud, but only if some row actually mismatches.
2. Joining with `on=["customer_id", "salt"]` (list form) collapses the
   duplicate key columns, so there is no ambiguous-column trap afterwards —
   the Day 1 lesson, avoided structurally rather than by aliasing.

### Route A — collect the sets per bucket, union at the top

```python
def ref_solve_dsl_a(orders, customers):
    partial = salted(orders, customers).groupBy("customer_id", "segment", "salt").agg(
        F.sum("amount").alias("part_amount"),
        F.collect_set("product").alias("part_products"),
    )
    return (
        partial.groupBy("customer_id", "segment")
        .agg(
            F.sum("part_amount").alias("total_amount"),
            F.array_distinct(F.flatten(F.collect_list("part_products"))).alias("products"),
        )
        .select(
            "customer_id", "segment", "total_amount",
            F.size("products").alias("n_products"),
        )
    )
```

```sql
-- ref_solve_sql_a
WITH salted_orders AS (
    SELECT customer_id, product, amount, pmod(order_id, 3) AS salt
    FROM orders
),
salted_customers AS (
    SELECT c.customer_id, c.segment, e.salt
    FROM customers c
    LATERAL VIEW explode(sequence(0, 2)) e AS salt
),
joined AS (
    SELECT o.customer_id, c.segment, o.salt, o.product, o.amount
    FROM salted_orders o
    JOIN salted_customers c
      ON o.customer_id = c.customer_id AND o.salt = c.salt
),
partial AS (
    SELECT customer_id, segment, salt,
           SUM(amount)          AS part_amount,
           collect_set(product) AS part_products
    FROM joined
    GROUP BY customer_id, segment, salt
)
SELECT customer_id,
       segment,
       SUM(part_amount) AS total_amount,
       size(array_distinct(flatten(collect_list(part_products)))) AS n_products
FROM partial
GROUP BY customer_id, segment
```

`collect_set` is the fix because a **set union is decomposable** — that is the
whole content of the route. `flatten` turns `array<array<string>>` into
`array<string>`; `array_distinct` performs the union; `size` counts it. The
`array_distinct` is load-bearing, not defensive: `collect_set` dedupes only
*within* a bucket, and the trap is precisely that a product appears in two.

### Route B — put `product` in the partial key, defer the distinct

```python
def ref_solve_dsl_b(orders, customers):
    partial = salted(orders, customers).groupBy(
        "customer_id", "segment", "salt", "product"
    ).agg(F.sum("amount").alias("part_amount"))

    return partial.groupBy("customer_id", "segment").agg(
        F.sum("part_amount").alias("total_amount"),
        F.countDistinct("product").alias("n_products"),
    )
```

Also correct, and arguably the more idiomatic "two-phase distinct": rather than
recombining a distinct count, push `product` into the partial grouping key so
the partial stage emits **one row per (customer, salt, product)**, and let the
final stage do a genuine `COUNT(DISTINCT)` over an already-collapsed input.

**The tradeoff is real and it goes against Route B** — measured below: B costs
**one more Exchange than A and two more than the unsalted baseline**, because
Spark's `COUNT(DISTINCT x)` rewrite wants a stage partitioned by
`(group keys, x)`, and the salt-partitioned upstream does not satisfy that.
Route B's salt in the partial key ends up buying nothing: once `product` is in
the grouping key, the per-(customer,product) pre-aggregation is already doing
the row collapsing that the salt was supposed to do.

### Route C — the unsalted baseline (the contract, and the cheapest one here)

```python
def ref_solve_dsl_c(orders, customers):
    return (
        orders.join(customers, on="customer_id", how="inner")
        .groupBy("customer_id", "segment")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.countDistinct("product").alias("n_products"),
        )
    )
```

Keep this one around even though the problem statement forbids it as an answer.
It is the **oracle**: a salted implementation is correct iff it agrees with the
unsalted one on every row. Any "optimization" that changes the answer is not an
optimization. It is also, on this data and on any data without real skew,
strictly the fastest of the three.

---

## Part 5 — Concept takeaways

- **Salting is a physical-layout intervention with a semantic blast radius.**
  It changes nothing about *what* is computed and everything about *where*.
  The correctness burden it creates is entirely in the recombination step:
  every measure must be decomposable over an arbitrary partition of its group.
  Distinct counts, averages-of-averages and percentiles are not — and the
  failure lands exclusively on the hot key, i.e. the one you salted because it
  mattered.

- **Salting costs exactly one extra Exchange, and the reason is structural,
  not incidental.** The salted stages are hash-partitioned by
  `(customer_id, salt)`. The final aggregation groups by `(customer_id,
  segment)`. Spark can skip a shuffle only when the child's partitioning keys
  are a **subset** of the required clustering keys — and `salt` is in the
  partitioning but *not* in the final grouping key, so the subset test fails by
  construction. Every salting scheme pays this, because removing the salt is
  the entire point of the final stage. Measured: unsalted 2 Exchanges, salted 3.

- **The partial stage, by contrast, is free.** `groupBy(customer_id, segment,
  salt)` immediately after a join on `(customer_id, salt)` adds **no** Exchange:
  partition keys `{customer_id, salt}` subset of grouping keys
  `{customer_id, segment, salt}`. Third independent confirmation of the Day 16
  subset rule (Day 17 was the second) — and here it is what makes salting
  affordable at all.

- **`COUNT(DISTINCT x)` is not one aggregate, it is a two-stage plan.** Spark
  rewrites it into an aggregation at the `(group keys, x)` grain feeding an
  aggregation at the `(group keys)` grain. That rewrite has its own
  partitioning requirement, and whether it is free depends on what partitioning
  it inherits: free under a plain `customer_id`-partitioned join (Route C),
  one extra Exchange under a `(customer_id, salt)`-partitioned one (Route B).
  Reading `HashAggregate(... partial_count(distinct product))` in a plan and
  assuming it is a single cheap operator is the mistake.

- **`collect_set` / `collect_list` force `ObjectHashAggregate`, a third
  aggregate operator.** Day 17 established `min`/`max` on STRING -> SortAggregate
  (non-fixed-width buffer). The set-collecting aggregates take a different
  branch again: `ObjectHashAggregate`, which keeps arbitrary JVM objects in the
  buffer and falls back to sort-based spilling when it outgrows
  `spark.sql.objectHashAggregate.sortBased.fallbackThreshold`. Measured: Route A
  is `ObjectHashAggregate` throughout; Routes B and C are `HashAggregate`
  throughout. Route A trades the cheaper operator for one fewer Exchange —
  that is the actual A-vs-B tradeoff, not a shuffle-count story on its own.

- **A deterministic salt is a design choice, not pedantry — but the usual
  slogan about `rand()` is wrong.** Measured: an unseeded `F.rand()` salt is
  **stable across actions within one lineage** (the seed is fixed when the
  expression is constructed, then combined with partition index), and stable
  across a shuffle boundary too. What it is *not* stable across is **separate
  construction of the expression**: two independently built `rand()` columns
  never agree. So the real failure mode is not "task retry corrupts your join",
  it is (a) any scheme that salts *both* sides independently is broken from the
  start, (b) two runs of the same job are not reproducible, so a wrong result
  cannot be reproduced or diffed, and (c) the trap above becomes
  nondeterministically wrong rather than consistently wrong. `pmod(stable_id,
  N)` costs nothing and removes all three.

- **AQE's skew join and manual salting solve overlapping but different
  problems.** `spark.sql.adaptive.skewJoin.enabled` attaches to the shuffle
  read of a sort-merge (and shuffled-hash) join, splitting an oversized
  partition into pieces and replicating the matching partition of the other
  side — mechanically the same replicate-and-fan-out idea, decided at runtime
  from real partition sizes. What it cannot do: rescue a **broadcast** join
  (there is no shuffle to reread), and do anything whatsoever for a skewed
  **`groupBy`** — an aggregation has no other side to replicate. Manual salting
  remains the only tool for the aggregation case, which is exactly the case
  this problem is built on.

- **DSL/SQL contract note:** Route A's `n_products` comes back as `int` (`size`)
  while Routes B and C return `bigint` (`count`). `check()` compares Python
  ints, so both pass — but a schema-sensitive downstream consumer would notice,
  and it is worth casting deliberately rather than by accident.

## Physical plan notes

Measured on **Spark 4.1.1**, `local[2]`, `shuffle.partitions = 4`, with
`spark.sql.autoBroadcastJoinThreshold = -1` (otherwise the 5-row dimension is
broadcast and both join Exchanges vanish, hiding everything interesting).
Counts are from `queryExecution().executedPlan()`.

| Route | Exchanges | Join | Aggregate operator | Note |
|---|---|---|---|---|
| A — collect_set | **3** | SortMergeJoin on `(customer_id, salt)` | `ObjectHashAggregate` x4 | 2 join + 1 final |
| B — product in partial key | **4** | SortMergeJoin on `(customer_id, salt)` | `HashAggregate` x6 | 2 join + 1 for the distinct rewrite at `(cust, seg, product)` + 1 final |
| C — unsalted baseline | **2** | SortMergeJoin on `customer_id` | `HashAggregate` x4 | 2 join, **zero** aggregation Exchanges |

Route A, bottom-up (the two facts worth keeping):

```
ObjectHashAggregate(keys=[customer_id, segment], functions=[sum, collect_list])
+- Exchange hashpartitioning(customer_id, segment, 4)         <-- the salt tax
   +- ObjectHashAggregate(keys=[customer_id, segment], partial_...)
      +- ObjectHashAggregate(keys=[customer_id, segment, salt], ...)
         +- ObjectHashAggregate(keys=[customer_id, segment, salt], partial_...)
            +- SortMergeJoin [customer_id, salt], [customer_id, salt], Inner
```

No Exchange between the join and the `(customer_id, segment, salt)` aggregation
— subset rule satisfied. One Exchange before the `(customer_id, segment)`
aggregation — subset rule violated by `salt`.

Route C's aggregation, for contrast — the `COUNT(DISTINCT)` rewrite is visible
as two stacked grains with **no Exchange between them**, because the join's
`customer_id` partitioning is a subset of `(customer_id, segment, product)`:

```
HashAggregate(keys=[customer_id, segment], functions=[sum, count(distinct product)])
+- HashAggregate(keys=[customer_id, segment], merge_sum, partial_count(distinct product))
   +- HashAggregate(keys=[customer_id, segment, product], merge_sum)
      +- HashAggregate(keys=[customer_id, segment, product], partial_sum)
         +- SortMergeJoin [customer_id], [customer_id], Inner
```

**AQE skew handling — NOT OBSERVED, and not observable here.** Defaults measured
on this install: `adaptive.enabled = true`, `skewJoin.enabled = true`,
`skewedPartitionFactor = 5.0`, `skewedPartitionThresholdInBytes = 268435456b`
(256 MB), `advisoryPartitionSizeInBytes = 67108864b` (64 MB),
`autoBroadcastJoinThreshold = 10485760b` (10 MB). A partition must be **both**
larger than 256 MB **and** more than 5x the median partition before
`OptimizeSkewedJoin` fires. 14 rows clears neither bar, so the final plan shows
only `AQEShuffleRead coalesced` on all three Exchanges — the *coalescing* branch
of AQE, not the skew branch. No `OptimizeSkewedJoin` node and no
`AQEShuffleRead ... skewed=true` was produced on this data; do not claim to have
seen one. The two-bar rule is itself the useful takeaway: AQE deliberately
ignores skew that is merely *relative*, which is why a 5x imbalance on a
100 MB table gets no help at all.

## Echoes

- **Day 16 / Day 17 — the Exchange subset rule.** Third measurement, and the
  first where it is the *load-bearing* fact rather than an observation: it is
  what makes the salted partial aggregation free, and its failure on `salt` is
  what makes the final one cost. Refines the rule with the converse case —
  Day 16 showed derived expressions failing to match upstream partitioning,
  here an *extra* partitioning key fails the subset test in the other direction.
- **Day 17 — aggregate operator selection.** `min`/`max` on STRING ->
  SortAggregate; now `collect_set`/`collect_list` -> ObjectHashAggregate. Three
  operators, three buffer shapes. The generalisation: the aggregate *function
  list*, not the grouping keys, picks the operator.
- **Day 12 — aggregate formulation.** `COUNT(when(...))` vs `SUM(when(...))`
  asked which rows an aggregate ranges over. This day asks whether an aggregate
  survives being split across an arbitrary partition of its group. Same family
  of error, and both are invisible on data that is uniform enough.
- **Day 15 / Day 16 — load-bearing vs dead defensive code.** `array_distinct`
  in Route A is load-bearing (`collect_set` dedupes only within a bucket); the
  reflex to also wrap the final `size` in a `coalesce` would be dead code, since
  an inner join guarantees every surviving group has at least one row.
- **CLAUDE.md's "fewer shuffles is a tempting and frequently wrong story."**
  Confirmed hard here: the *unsalted* route has the fewest Exchanges of the
  three. Salting is a deliberate trade of one extra shuffle plus a wider
  intermediate for the ability to make progress on a key that would otherwise
  straggle or OOM a single task. On data small enough to test by hand, salting
  is pure overhead — which is exactly why its correctness cost has to be
  justified by a measured skew, never by reflex.
