# Day 17 — SCD2 history build from a change feed — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

## The trap

**What it is:** the feed contains a **no-op replay** — a row whose tracked
attributes are identical to the immediately preceding row of the same product.
A version is a *run* of feed rows, not a feed row. Any solution that maps one
feed row to one SCD2 version emits a spurious version and splits one interval
in two.

**Why a naive solution passes anyway:** `lead(changed_at) over (partition by
product_id order by changed_at)` **is** the correct interval-closing idiom —
it is only applied to the wrong row set. On every product whose consecutive
rows genuinely differ, one-feed-row-per-version and one-run-per-version are
the same thing, so the naive route is exactly right. 8 of the 9 expected rows
come out correct; three of the four products are fully correct.

**Which row exposes it:** `("P1", 2026-01-05, "toys", 10.0)` — same
`category`/`price` as the `2026-01-01` row. Naive output (verified by running
it):

```
P1  01-01 -> 01-05  toys 10.0 false      <- should close at 01-10
P1  01-05 -> 01-10  toys 10.0 false      <- should not exist
P1  01-10 -> NULL   toys 12.0 true       <- correct either way
```

Note the failure shape: the version count is wrong **and** the surviving
interval is truncated. `effective_from` of the run must be the **first**
timestamp of the run (01-01), not the last (01-05) — a `dropDuplicates`-style
collapse that keeps an arbitrary row inside the run gets the boundary wrong
even when it gets the count right.

**Reading-time tell:** the spec says a version is a *maximal run of consecutive
rows*. Grep the candidate solution for anything that compares a row to its
**predecessor** (`lag` / `lead` on the attributes). If the only window
functions are over `changed_at` and nothing ever touches `category`/`price`
in a comparison, the solution cannot possibly detect a no-op — it has no
expression capable of distinguishing P1's second row from a real change.

**Not the trap, but adjacent:** P4 goes `40.0 -> 45.0 -> 40.0`. Change
detection must be **positional** (compare with the previous row), never
value-set-based. `dropDuplicates(["product_id", "category", "price"])` or a
`DISTINCT` over the attributes collapses P4's two `40.0` rows into one version
and loses the third interval. Same root cause as the trap: a run is defined by
adjacency, not by value identity.

---

## Part 4 — Reference answers

Shared change-detection predicate — "this row differs from its predecessor":

```python
w = Window.partitionBy("product_id").orderBy("changed_at")
is_change = ~(
    F.col("category").eqNullSafe(F.lag("category").over(w))
    & F.col("price").eqNullSafe(F.lag("price").over(w))
)
```

`eqNullSafe` (`<=>`) does double duty: it handles a NULL attribute if the feed
ever grows one, **and** it makes the very first row of each product fall out
as a change for free (`lag` is NULL there, `<=>` is `false`, `~` is `true`).
With plain `!=` the first row yields NULL → filtered away → the product's
opening version disappears. That is a real bug waiting in any solution that
special-cases the first row with `row_number() == 1` instead.

### Route A — running-sum version key, then groupBy (the Day 13 idiom)

```python
def ref_solve_dsl_a(df):
    w = Window.partitionBy("product_id").orderBy("changed_at")
    versioned = df.withColumn("version_id", F.sum(is_change.cast("int")).over(w))
    agg = versioned.groupBy("product_id", "version_id", "category", "price").agg(
        F.min("changed_at").alias("effective_from")
    )
    wv = Window.partitionBy("product_id").orderBy("effective_from")
    return (
        agg.withColumn("effective_to", F.lead("effective_from").over(wv))
        .withColumn("is_current", F.col("effective_to").isNull())
        .select("product_id", "effective_from", "effective_to",
                "category", "price", "is_current")
    )
```

```sql
-- ref_solve_sql_a
WITH flagged AS (
    SELECT product_id, changed_at, category, price,
           CASE WHEN category <=> LAG(category) OVER w
                 AND price    <=> LAG(price)    OVER w
                THEN 0 ELSE 1 END AS is_change
    FROM product_feed
    WINDOW w AS (PARTITION BY product_id ORDER BY changed_at)
),
versioned AS (
    SELECT product_id, changed_at, category, price,
           SUM(is_change) OVER (PARTITION BY product_id ORDER BY changed_at) AS version_id
    FROM flagged
),
versions AS (
    SELECT product_id, version_id, category, price, MIN(changed_at) AS effective_from
    FROM versioned
    GROUP BY product_id, version_id, category, price
)
SELECT product_id,
       effective_from,
       LEAD(effective_from) OVER (PARTITION BY product_id ORDER BY effective_from) AS effective_to,
       category, price,
       LEAD(effective_from) OVER (PARTITION BY product_id ORDER BY effective_from) IS NULL AS is_current
FROM versions
```

Note the ordered `SUM(...) OVER (... ORDER BY ...)` — the default
`RANGE UNBOUNDED PRECEDING .. CURRENT ROW` frame is what makes it a *running*
sum. Day 8 logged that default frame as the bug; here, exactly as in Day 13,
it is the tool. (The `WINDOW w AS (...)` named-window clause is real Spark SQL
and does run — verified on 4.2.0.)

### Route B — filter to the version-start rows, then LEAD directly

```python
def ref_solve_dsl_b(df):
    w = Window.partitionBy("product_id").orderBy("changed_at")
    starts = df.withColumn("is_change", is_change).where("is_change")
    return (
        starts.select(
            "product_id",
            F.col("changed_at").alias("effective_from"),
            F.lead("changed_at").over(w).alias("effective_to"),
            "category", "price",
        )
        .withColumn("is_current", F.col("effective_to").isNull())
        .select("product_id", "effective_from", "effective_to",
                "category", "price", "is_current")
    )
```

```sql
-- ref_solve_sql_b
WITH flagged AS (
    SELECT product_id, changed_at, category, price,
           LAG(category) OVER w AS prev_category,
           LAG(price)    OVER w AS prev_price
    FROM product_feed
    WINDOW w AS (PARTITION BY product_id ORDER BY changed_at)
),
starts AS (
    SELECT product_id, changed_at, category, price
    FROM flagged
    WHERE NOT (category <=> prev_category AND price <=> prev_price)
)
SELECT product_id,
       changed_at AS effective_from,
       LEAD(changed_at) OVER (PARTITION BY product_id ORDER BY changed_at) AS effective_to,
       category, price,
       LEAD(changed_at) OVER (PARTITION BY product_id ORDER BY changed_at) IS NULL AS is_current
FROM starts
```

**Runtime break to know about:** `df.where(is_change)` — passing the window
expression straight into the filter — raises

```
AnalysisException: It is not allowed to use window functions inside WHERE clause.
```

The window must be materialized as a column first (`withColumn(...).where(...)`),
which is exactly why the SQL half needs the `flagged` CTE. Same rule in both
APIs; SQL just makes it structurally obvious.

### Which route, and why

The tempting story — "Route A groups by `(product_id, version_id, ...)`, so it
pays a second shuffle" — is **wrong**, and the plans below say so: both routes
have exactly **one Exchange**. The aggregate's required clustering is on
`product_id, version_id, category, price`, and the stream is already
`hashpartitioning(product_id)`; a partitioning on a *subset* of the grouping
keys already satisfies the requirement, so no re-Exchange is inserted.

The real difference is node count within that single stage: Route A pays an
extra `Window`, a partial/final `HashAggregate` pair, and — the part that
actually costs — a **second `Sort`**, because `HashAggregate` destroys the
`(product_id, changed_at)` ordering that the second `LEAD` needs. Route B's
`Filter` *preserves* that ordering, so its second `Window` reuses the first
sort and no second `Sort` node appears at all.

Route A is not wasted knowledge: it is the general form. The moment a version
needs an aggregate over the whole run (`n_feed_rows`, `max(price)` inside the
run) Route B cannot express it and Route A is the only route.

---

## Part 5 — Concept takeaways

- **A version is a run, not a row.** Every SCD2 build is gaps-and-islands in
  disguise: detect boundary → derive island key (or filter to boundaries) →
  close intervals. The feed-row grain and the version grain differ, and a
  re-emitted no-op is precisely the row where they diverge.
- **Boundary detection is positional, never value-based.** `lag`-vs-current is
  the contract. `distinct` / `dropDuplicates` over the attributes silently
  merges non-adjacent repeats (`A -> B -> A` becomes two versions instead of
  three) — a different bug from the no-op one, caught by the same discipline.
- **`<=>` / `eqNullSafe` also solves the first-row problem.** `lag` is NULL at
  a partition's first row; `!=` there is NULL → falsy → the opening version is
  dropped. `<=>` makes "no predecessor" evaluate to "different", which is what
  the semantics want anyway. Direct echo of Day 16.
- **Half-open `[from, to)` intervals need no arithmetic.** `effective_to =
  lead(effective_from)` — no `date_sub(..., 1)`, no `date_add`. Closed-interval
  specs (`to = next_from - 1 day`) are where off-by-one bugs live, and the
  grain of that `-1` (day? second?) is a spec question, not a code question.
  Pin which convention the spec wants before writing anything.
- **`is_current` is `effective_to IS NULL`, derived — not a second window.**
  `row_number() desc = 1` or `max(changed_at)` re-derives the same fact through
  a second sort. Once the interval is closed, the flag is free.
- **Window functions cannot appear in `WHERE` / `HAVING`** in either API.
  Materialize first (`withColumn` / a CTE), then filter. The optimizer will not
  rescue you; it is an analysis-time error.
- **An ordered `SUM(flag) OVER (...)` is a running sum only because of the
  default `RANGE UNBOUNDED PRECEDING .. CURRENT ROW` frame.** Day 8 logged that
  default as the source of a bug (a `COUNT OVER` that must not be ordered);
  Day 13 and this problem use the same default deliberately. The default is
  neither right nor wrong — knowing which one you want is the skill.
- **`groupBy` after a window does not automatically add an Exchange** when the
  window's `partitionBy` is a subset of the grouping keys. Reach for the plan
  before claiming a shuffle-count gap; the real cost here was a `Sort`, not an
  `Exchange`.

## Physical plan notes

Measured — `explain(mode="formatted")` on Spark 4.2.0, `local[2]`,
`shuffle.partitions=4`, AQE on (`isFinalPlan=false`, so these are the initial
physical plans).

| Route | Exchange | Sort | Window | HashAggregate | Filter |
|-------|----------|------|--------|---------------|--------|
| A (DSL) | 1 | 2 | 3 | 2 (partial+final) | 0 |
| A (SQL) | 1 | 2 | 3 | 2 (partial+final) | 0 |
| B (DSL) | 1 | 1 | 2 | 0 | 1 |
| B (SQL) | 1 | 1 | 2 | 0 | 1 |

Route B (DSL), full tree:

```
AdaptiveSparkPlan
+- Project
   +- Window [lead(changed_at) ... ]
      +- Project
         +- Filter (NOT (category <=> _we0) OR NOT (price <=> _we1))
            +- Window [lag(category), lag(price)]
               +- Sort [product_id ASC, changed_at ASC]
                  +- Exchange hashpartitioning(product_id, 4)
                     +- Scan ExistingRDD
```

Route A (DSL), full tree:

```
AdaptiveSparkPlan
+- Project
   +- Window [lead(effective_from) ...]
      +- Sort [product_id ASC, effective_from ASC]          <- the extra Sort
         +- HashAggregate (final)   Keys: product_id, version_id, category, price
            +- HashAggregate (partial)
               +- Project
                  +- Window [sum(_w0) ... RangeFrame unboundedpreceding..currentrow]
                     +- Project
                        +- Window [lag(category), lag(price)]
                           +- Sort [product_id ASC, changed_at ASC]
                              +- Exchange hashpartitioning(product_id, 4)
                                 +- Scan ExistingRDD
```

No `Exchange` between the `HashAggregate` and its child: the stream is already
`hashpartitioning(product_id)` and that satisfies clustering on the superset
key `(product_id, version_id, category, price)`.

Two smaller observations from the same plans:

- The `Window` nodes do **not** collapse into one even though they share
  `partitionBy(product_id).orderBy(changed_at)` — each depends on the previous
  one's output, so they must be separate nodes. They do share the single `Sort`
  (Route B nodes 3/4/7, Route A nodes 3/4/6), which is the Day 8 "same
  partitionBy shares one shuffle" result extended: same partitionBy **and** same
  orderBy also share the sort.
- Route B's SQL half computes `LEAD(changed_at)` **twice** inside the same
  `Window` node (once for `effective_to`, once for the `IS NULL` in
  `is_current`) — writing the expression twice does not get CSE'd into one.
  Same node, no extra shuffle, so this is **style**, not performance. The DSL
  half avoids it by referencing the already-materialized `effective_to` column.

## Echoes

- **Day 8** (dedup / keep-latest-per-key): the direct ancestor. Day 8's
  `rn = 1` keeps one row per key; SCD2 keeps *all* of them and closes intervals
  between them. Also reinforces Day 8's ordered-window default-frame lesson —
  same mechanism, opposite intent — and refines Day 8's shuffle-sharing result
  (subset-key partitioning also satisfies a downstream `groupBy`).
- **Day 13** (sessionization): the flag → running-`SUM` → island-key idiom is
  reused verbatim. Day 13's boundary was a time gap over a threshold; here it
  is an attribute inequality. Same skeleton, different predicate — which is the
  actual takeaway: gaps-and-islands is one pattern with a pluggable boundary.
- **Day 16** (NULL semantics): `<=>` earns its keep as the change predicate,
  and for the non-obvious reason — not NULL attributes, but the NULL that `lag`
  produces at each partition's first row.
- **Day 2** (longest login streak): the original constant-diff islands. Worth
  re-reading next to this one; Day 2's `value - row_number()` trick is the
  *special case* that only works when the boundary condition is "+1 exactly",
  which is why Day 13 and Day 17 both need the general form.
- **Day 15** (window frames): reinforces that `lead`/`lag` are positional
  (`ROWS`-flavored, offset-based) and do not care about calendar distance — the
  right call here, since a version boundary is defined by adjacency in the feed,
  not by elapsed days.
