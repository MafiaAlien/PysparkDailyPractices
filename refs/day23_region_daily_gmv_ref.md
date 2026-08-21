# Day 23 — agg_region_daily — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

ETL layer : L3 serving
Domain    : e-commerce (marketplace) orders
Failure mode (the trap axis — never stated in the day file):
            **dimension drift → fan-out double counting** — the config table
            holds more than one row per key covering the same date, so the
            attribution join multiplies fact rows before the aggregate.

## The trap

**What it is:** `seller_region` is supposed to hold one *effective* row per
seller per date — ops appends a new assignment and closes the previous one.
For `S3` the previous row was never closed:

    | S3 | CENTRAL | WH_C7 | 2026-05-01 | NULL |
    | S3 | CENTRAL | WH_C9 | 2026-06-01 | NULL |

Both rows cover any date on or after 2026-06-01. The interval-containment
join — `valid_from <= order_date < coalesce(valid_to, '9999-12-31')`, which
is exactly the rule the day file states — therefore emits **two** rows for
every S3 order dated on or after 2026-06-01. `SUM(qty * unit_price)` and
`SUM(refund_amount)` then count those orders twice. The fix is not a
different join predicate; it is collapsing the dimension to one row per
(seller, date) **before or during** the join.

**Why a naive solution passes anyway:** the failure is invisible in every
dimension except the money columns.

- Row **count** is unchanged: 5 expected rows, 5 naive rows. The duplicate
  lands in a group that already exists.
- `n_orders` is **correct on all five rows**. The contract says *distinct*
  orders, so `countDistinct(order_id)` absorbs the duplicate. A solution that
  follows the spec is immune on exactly the one column a human would use to
  sanity-check the row count against the 8-row input.
- No NULLs, no duplicate grain keys, nothing dropped. **P2's assertions all
  pass** — grain uniqueness and non-null `order_date` / `region` hold under
  the trap, so the job's own quality gate publishes the bad numbers.
- The inflation is not a clean multiple on the big group. Measured naive vs
  correct:

    | order_date | region | naive gross / refund / net | correct |
    |---|---|---|---|
    | 2026-05-20 | CENTRAL | 80.0 / 0.0 / 80.0 | same ✓ |
    | 2026-06-14 | CENTRAL | **295.0 / 55.0 / 240.0** | 235.0 / 40.0 / 195.0 |
    | 2026-06-14 | EAST | 120.0 / 40.0 / 80.0 | same ✓ |
    | 2026-06-15 | CENTRAL | **120.0 / 0.0 / 120.0** | 60.0 / 0.0 / 60.0 |
    | 2026-06-15 | UNKNOWN | 90.0 / 0.0 / 90.0 | same ✓ |

  The 06-14 CENTRAL group also contains S1's O1 and O8, which are **not**
  duplicated, so the group reads 295.0 instead of 235.0 — a 1.26x inflation,
  not a 2x. "Is any number exactly double?" does not find it. Only the
  single-order 06-15 CENTRAL group doubles cleanly, and a one-order group
  gives a reviewer nothing to compare against.

**Which row exposes it:** `seller_region` rows 4 and 5 (`S3 / WH_C7` and
`S3 / WH_C9`), against `orders` **O3** (2026-06-14) and **O7** (2026-06-15).

The control row is **O4** — same seller `S3`, dated 2026-05-20, which falls
inside `WH_C7`'s interval only. It is correct under both the naive and the
fixed solution. So "check the trap seller's orders" is not enough: one third
of S3's orders are fine, and it is the *date* that decides.

The decoy is **S2**, whose config has the same visual shape — two rows, same
region, a warehouse handover — but properly closed (`valid_to = 2026-06-01`)
and therefore correct. Under half-open semantics O2 on 2026-06-14 matches
`WH_E2` only. S2 is there so that "two rows for one seller" cannot be treated
as the tell by itself.

**Reading-time tell:** **two rows for the same seller with `valid_to = NULL`
at the same time.** NULL means open-ended; two open-ended assignments for one
key is a contradiction of the table's own stated maintenance rule ("ops
appends a new row and is supposed to close the previous one"). Reading down
the `valid_to` column: S1 one NULL, S2 one closed + one NULL, S3 **two
NULLs**, S4 one NULL. It is one column scan away, and it is the only thing in
the three input boxes that violates a stated invariant.

The second, solution-side tell: any join whose right side is a table with no
enforced uniqueness on the join key, without a preceding dedup / rank /
interval repair, is a fan-out waiting to happen. That reading rule does not
require noticing the S3 rows at all.

**Which pipeline stage it lives in:** **stage 3, "attribute region"**. Stages
1, 2 and 4 are byte-identical in the trapped and untrapped versions — the
naive and correct plans differ by one operator on one side of one join.

---

## Production constraints — audit

| # | Constraint | Satisfied by | How it fails if ignored |
|---|---|---|---|
| P1 | Every metric defined once; `net_gmv` derived from the reported `gross_gmv` and `refund_amount` | `net_gmv = gross_gmv - refund_amount` computed from the two aggregate outputs, not a second independent SUM over a differently-filtered row set | Drift: a re-derived `net` that filters or joins differently produces a row where `gross - refund != net`. On this data a second SUM over the un-deduped stream would give a `net` inconsistent with the `gross` printed beside it |
| P2 | Assert before returning: unique on grain, `order_date` / `region` never NULL | `df.groupBy(grain).count()` max == 1, and non-null counts == row count, raised as an assertion before the DataFrame is returned | **Nothing on this data.** All three assertions pass under the trap. This is the point: P2 is a real requirement that this day's failure walks straight through. Grade it on whether the assertions exist, never on whether they caught anything |
| P3 | Read only the needed columns from `seller_region` | `select("seller_id", "region", "valid_from", "valid_to")` before the join — `warehouse` never enters | No wrong rows. `warehouse` would ride through the join and force a wider shuffle payload; on 6 rows, unmeasurable. Style/perf, not correctness — and note the projection does **not** collapse the S3 duplicate, because the two rows still differ in `valid_from` |

**P2 is the constraint most likely to be mis-graded at Stage 5.** A solution
that writes all three assertions still ships wrong numbers. A reviewer who
concludes "assertions present, therefore the output is trustworthy" has
learned the wrong lesson: assertions constrain the output's *shape*, and
fan-out does not change the shape.

---

## Pipeline stages

1. **Filter and price the order feed** — drop `status = 'CANCELLED'`,
   compute `gross = qty * unit_price` and keep `order_date`.
2. **Roll refunds up to one row per order** — `groupBy(order_id).sum()`
   *before* joining. This also disposes of R3 (a refund against the cancelled
   O6): a LEFT join from orders never reaches it.
3. **Attribute region** — collapse `seller_region` to one row per
   (seller, date), LEFT join on the half-open interval, `coalesce(region,
   'UNKNOWN')`. **The trap lives here.**
4. **Aggregate and assert** — group to (order_date, region), emit the four
   metrics, run P2's assertions, return.

---

## Part 4 — Reference answers

Both routes are verified against the harness `expected`, DSL and SQL.

### Route A — rank and pick (join first, keep one dim row per order)

Let the fan-out happen, then rank it away with `row_number()` per order.
Ordering by `valid_from DESC` means "the most recently opened assignment
wins", which is the natural last-write-wins reading; the `region ASC`
tie-break makes it total. On this data both S3 rows carry the same region, so
the tie-break never changes the answer — do not credit it as load-bearing.

```python
OPEN = "9999-12-31"

def ref_build_agg_region_daily_dsl_a(orders, seller_region, refunds):
    live = (orders.where(F.col("status") != "CANCELLED")
                  .withColumn("gross", F.col("qty") * F.col("unit_price")))
    ref1 = (refunds.groupBy("order_id")
                   .agg(F.sum("refund_amount").alias("refund_amount")))
    base = (live.join(ref1, "order_id", "left")
                .withColumn("refund_amount",
                            F.coalesce(F.col("refund_amount"), F.lit(0.0))))

    dim = seller_region.select("seller_id", "region", "valid_from", "valid_to")
    j = base.join(
        dim,
        on=(base.seller_id == dim.seller_id)
           & (dim.valid_from <= base.order_date)
           & (base.order_date < F.coalesce(dim.valid_to, F.lit(OPEN))),
        how="left",
    )
    w = Window.partitionBy(base.order_id).orderBy(
        F.col("valid_from").desc_nulls_last(), F.col("region").asc())
    picked = (j.withColumn("rn", F.row_number().over(w))
               .where(F.col("rn") == 1)
               .withColumn("region",
                           F.coalesce(F.col("region"), F.lit("UNKNOWN"))))

    return (picked.groupBy("order_date", "region")
            .agg(F.countDistinct("order_id").alias("n_orders"),
                 F.sum("gross").alias("gross_gmv"),
                 F.sum("refund_amount").alias("refund_amount"))
            .withColumn("net_gmv", F.col("gross_gmv") - F.col("refund_amount"))
            .select("order_date", "region", "n_orders",
                    "gross_gmv", "refund_amount", "net_gmv"))
```

```sql
-- ref_build_agg_region_daily_sql_a
WITH live AS (
    SELECT order_id, seller_id, order_date, qty * unit_price AS gross
    FROM orders
    WHERE status <> 'CANCELLED'
),
refund_by_order AS (
    SELECT order_id, SUM(refund_amount) AS refund_amount
    FROM refunds
    GROUP BY order_id
),
dim AS (
    SELECT seller_id, region, valid_from, valid_to FROM seller_region
),
attributed AS (
    SELECT l.order_id, l.order_date, l.gross,
           COALESCE(r.refund_amount, 0.0) AS refund_amount,
           COALESCE(d.region, 'UNKNOWN')  AS region,
           ROW_NUMBER() OVER (PARTITION BY l.order_id
                              ORDER BY d.valid_from DESC NULLS LAST,
                                       d.region ASC) AS rn
    FROM live l
    LEFT JOIN refund_by_order r ON l.order_id = r.order_id
    LEFT JOIN dim d
           ON l.seller_id = d.seller_id
          AND d.valid_from <= l.order_date
          AND l.order_date < COALESCE(d.valid_to, '9999-12-31')
)
SELECT order_date, region,
       COUNT(DISTINCT order_id)             AS n_orders,
       SUM(gross)                           AS gross_gmv,
       SUM(refund_amount)                   AS refund_amount,
       SUM(gross) - SUM(refund_amount)      AS net_gmv
FROM attributed
WHERE rn = 1
GROUP BY order_date, region
```

### Route B — repair the dimension first (close each row at the next one)

Do not let the fan-out happen. Rewrite the config into non-overlapping
half-open intervals by closing every row at the next `valid_from` for the
same seller — `lead()`, the Day-17 interval-closing idiom, applied to a
dimension instead of a change feed. After the repair the containment join is
at most 1:1 **by construction**, and the downstream code needs no dedup at
all.

```python
def ref_build_agg_region_daily_dsl_b(orders, seller_region, refunds):
    live = (orders.where(F.col("status") != "CANCELLED")
                  .withColumn("gross", F.col("qty") * F.col("unit_price")))
    ref1 = (refunds.groupBy("order_id")
                   .agg(F.sum("refund_amount").alias("refund_amount")))
    base = (live.join(ref1, "order_id", "left")
                .withColumn("refund_amount",
                            F.coalesce(F.col("refund_amount"), F.lit(0.0))))

    w = Window.partitionBy("seller_id").orderBy("valid_from")
    dim = (seller_region
           .select("seller_id", "region", "valid_from", "valid_to")
           .withColumn("valid_to",
                       F.least(F.coalesce(F.col("valid_to"), F.lit(OPEN)),
                               F.coalesce(F.lead("valid_from").over(w),
                                          F.lit(OPEN)))))
    j = base.join(
        dim,
        on=(base.seller_id == dim.seller_id)
           & (dim.valid_from <= base.order_date)
           & (base.order_date < dim.valid_to),
        how="left",
    ).withColumn("region", F.coalesce(F.col("region"), F.lit("UNKNOWN")))

    return (j.groupBy("order_date", "region")
            .agg(F.countDistinct("order_id").alias("n_orders"),
                 F.sum("gross").alias("gross_gmv"),
                 F.sum("refund_amount").alias("refund_amount"))
            .withColumn("net_gmv", F.col("gross_gmv") - F.col("refund_amount"))
            .select("order_date", "region", "n_orders",
                    "gross_gmv", "refund_amount", "net_gmv"))
```

```sql
-- ref_build_agg_region_daily_sql_b   (only the `dim` CTE differs from A;
-- `attributed` drops the ROW_NUMBER and the final SELECT drops `WHERE rn = 1`)
dim AS (
    SELECT seller_id, region, valid_from,
           LEAST(COALESCE(valid_to, '9999-12-31'),
                 COALESCE(LEAD(valid_from) OVER (PARTITION BY seller_id
                                                 ORDER BY valid_from),
                          '9999-12-31')) AS valid_to
    FROM seller_region
)
```

Note the `LEAST(...)` rather than a bare `LEAD`: a row that was *correctly*
closed must keep its own `valid_to` when the next row opens later. Replacing
`valid_to` with `lead(valid_from)` unconditionally would silently extend a
deliberately closed assignment across a gap. There is no such gap in this
data — S2's `valid_to` and the next `valid_from` are both 2026-06-01, so
`LEAST` and a bare `LEAD` agree here. **Dead code on this input, load-bearing
in production**; the Day-15 load-bearing-vs-dead distinction, on the other
side of the ledger.

### Which route to prefer

Route B, and not for shuffle reasons — see the plan notes: the two routes
have the **same** Exchange count. The argument is structural:

- Route A's `row_number()` runs over the **fact** stream (one window per
  order); Route B's runs over the **dimension** (one window per seller
  assignment). Dimensions are small; fact tables are not.
- Route B produces a repaired dimension that is reusable and independently
  testable — "is this config self-consistent?" becomes a question you can
  assert on, and every downstream job that joins the same config gets the fix.
- Route A's guard is a `WHERE rn = 1` sitting several stages away from the
  table that caused the problem. Delete that line and the numbers get bigger
  with no other symptom.

Route A's advantage is that it is one line and needs no assumption about how
overlaps should be resolved beyond "latest wins".

---

## Part 5 — Concept takeaways

- **Fan-out is an aggregation bug that happens before the aggregate.** The
  join multiplies rows; `SUM` faithfully adds them up. Nothing downstream can
  detect it, because by the time the numbers are wrong the duplicate rows are
  gone. The only place to fix it is the join's right side.
- **Additive aggregates are the only ones that break.** `SUM` inflates,
  `COUNT(*)` inflates; `COUNT(DISTINCT k)`, `MIN`, `MAX`, and `collect_set`
  are all idempotent under duplication and stay correct. That is why the
  failure shape is a *half-right row* — the same shape as Day 18, where
  `SUM` survived salting and `COUNT(DISTINCT)` did not. Fan-out is Day 18's
  decomposability question with the two column families swapped.
- **A dimension is only "one row per key" if something enforces it.** An
  effective-dated config enforces nothing: uniqueness holds per (key, date)
  only when the intervals are non-overlapping, and that is a property of the
  *data*, maintained by hand, not of the schema. `dropDuplicates` on the key
  is not a contract; neither is a comment in the docstring.
- **Half-open intervals `[from, to)` make the boundary date unambiguous** and
  are why S2's handover on 2026-06-01 matches exactly one row. `valid_to`
  NULL means open-ended, so `COALESCE(valid_to, '9999-12-31')` is
  load-bearing — with ANSI-mode string comparison there is nothing to throw,
  but a bare `order_date < valid_to` returns NULL for the open row and the
  join predicate silently rejects it, losing every currently-assigned seller.
- **Repairing intervals with `lead(valid_from)` needs `LEAST`, not a bare
  replacement** — otherwise a correctly-closed row gets extended across a gap
  into a period the assignment did not cover.
- **The key is a fact-side key, not a dimension-side key.** S4 *has* a config
  row; the row just does not cover June. An orphan under an effective-dated
  join is "no row in effect on that date", not "no row at all", so
  `left_anti` on `seller_id` and a `region IS NULL` check after the join do
  **not** identify the same set of orders.
- **Output-shape assertions do not catch value-scale bugs.** Grain
  uniqueness, non-null, and row count are all invariant under fan-out. Real
  coverage would need a cross-check against an independently computed total
  (e.g. `SUM(gross)` over the fact table before any join equals `SUM` over
  the output), which is the reconciliation-style assertion this job does not
  have.
- Whitelist techniques exercised: interval-containment / effective-dated
  join; `row_number` dedup; `lead` interval closing; left / left-outer join
  with orphan keys and `COALESCE` sentinel; pre-aggregate before join;
  conditional filtering; `COUNT(*)` vs `COUNT(DISTINCT col)`.

## Physical plan notes

Measured on the harness data, `spark.sql.adaptive.enabled = false`,
`local[2]`, `shuffle.partitions = 4`, no broadcast hint. Counts from an
executed `queryExecution().executedPlan()`:

| Route | Exchange | Sort | Window | HashAggregate | Joins |
|---|---|---|---|---|---|
| Route A (rank-and-pick) | **6** | 6 | 1 | 6 | 2 SortMergeJoin |
| Route B (repair dimension) | **6** | 4 | 1 | 6 | 2 SortMergeJoin |
| Naive (trapped) | **6** | 4 | 0 | 6 | 2 SortMergeJoin |

**All three are 6 Exchanges. There is no shuffle story here, and none should
be invented.** The interesting part is *why* the two fixes are both free:

- **Route A reuses the window's Exchange for the `COUNT(DISTINCT)` rewrite.**
  The window shuffles by `order_id`; the distinct-rewrite aggregate requires
  `hashpartitioning(order_date, region, order_id)`. Existing partitioning
  `{order_id}` ⊆ required keys, so the requirement is already satisfied and
  no Exchange is inserted. Route A's plan shows
  `HashAggregate(keys=[order_date, region, order_id], ... partial_sum ...)`
  sitting **directly above** `Filter (rn = 1)` with no Exchange between —
  while Route B and the naive plan both carry an explicit
  `Exchange hashpartitioning(order_date, region, order_id, 4)` there.
  Fourth confirmation of the partition-key subset rule (Days 16, 17, 18).
- **Route B reuses the dimension's join Exchange for its window.** The join
  requires `hashpartitioning(seller_id)` and the window partitions by
  `seller_id` — same key, one Exchange (`plan_id=407`) feeds both.

So Route A pays for its window and gets the distinct-rewrite shuffle free;
Route B pays for the distinct-rewrite shuffle and gets its window free. They
land on the same number by different routes.

**The real difference is Sort count and what gets sorted: 6 vs 4.** Route A's
two extra sorts are `Sort [order_id, valid_from DESC, region]` on the
**fact** stream, once before and once after its Exchange. Route B's window
sort is `Sort [seller_id, valid_from]` on the 6-row **dimension**. At any
real scale that is the whole argument for Route B, and it is a per-row CPU +
spill story, not a shuffle-count story — the same shape of conclusion as
Day 19's inlined-expression finding.

Not measured: broadcast behaviour. With AQE off and no hint both joins
planned as SortMergeJoin. `F.broadcast(seller_region)` is the obvious
production move for a 6-row config and would change the counts; if either
side's solution claims a broadcast benefit at Stage 5, measure it before
agreeing.

## Echoes

- **Day 18** — the half-right row. There, `SUM` was decomposable across salt
  buckets and `COUNT(DISTINCT)` was not; here it is the mirror image, with
  `COUNT(DISTINCT)` immune to duplication and `SUM` not. Same lesson from the
  other side: know which of your metrics survive the transformation you are
  applying to the row set.
- **Day 16** — a dimension join whose failure surfaces as *plausible-looking
  rows* rather than missing rows, and is therefore invisible to a row-count
  check. Day 16's LEFT join produced `n_offers = 0` where a row should have
  matched; this one produces a larger number where a smaller one belongs.
- **Day 17** — `lead()` closing half-open intervals, reused in Route B on a
  dimension rather than a change feed. Day 17 also warned that a version is a
  *run* of rows, not a row; Day 23's config says a region assignment is an
  *interval*, not a row.
- **Day 5** — orphan keys under a dimension join. Day 5's orphan was a
  missing dimension row; Day 23's (S4) is a present dimension row that does
  not cover the date, which the `left_anti` idiom from Day 9 will not find.
- **Day 15** — load-bearing vs dead `COALESCE`. Here `COALESCE(valid_to,
  '9999-12-31')` is load-bearing, and the `LEAST` in Route B is dead on this
  data but load-bearing in production.
