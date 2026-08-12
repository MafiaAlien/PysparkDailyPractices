# Day 20 — ANSI mode & the try_* family — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

## The trap

**What it is:** `try_cast('12.0' AS INT)` returns **NULL**, not 12. A
string→INT cast demands an *integral literal*; it does not parse-then-truncate.
That is a different code path from a *numeric*→INT cast, which truncates
happily (`cast(12.7 AS INT)` → `12`, measured). So the natural one-liner for a
column the spec calls "a whole count stored as INT" —

```python
F.col("units_raw").try_cast("int")
```

— silently drops every count the float-serializing source emits. The record
still exists, its amount still counts, only its units evaporate. The correct
route goes through a numeric type first and only then narrows:

```python
F.col("units_raw").try_cast("double").try_cast("int")
```

The second hop must **also** be `try_cast`: `cast(3.0E9 AS INT)` throws
`CAST_OVERFLOW` under ANSI (measured), so a plain `.cast("int")` there is an
ANSI landmine on any out-of-range count.

**Why a naive solution passes anyway:** `try_cast(units_raw AS INT)` is
*exactly correct* on `'5'`, `'3'`, `'2'`, `'7'`, `'4'`, `'0'`, `'0'`, `'0'`,
`'0'` — nine of the ten input rows. Two of the three output rows (SRC_A,
SRC_C) come out **perfect**, including SRC_C's `total_units = 0` and its NULL
`avg_unit_price`. And within the failing row, `n_records`, `n_bad_amount` and
`net_amount` are all still right, because the units parse and the amount parse
are independent. Measured trap output:

| source | n_records | n_bad_amount | net_amount | total_units | avg_unit_price |
|---|---|---|---|---|---|
| SRC_A | 4 | 1 | 1980.5 | 17 | 116.5 | ← correct |
| SRC_B | 3 | 0 | 1519.5 | **4** | **379.88** | ← 16 and 94.97 expected |
| SRC_C | 3 | 1 | 500.0 | 0 | NULL | ← correct |

Two wrong cells out of eighteen, in one row, and the wrong `total_units` is
still a plausible small positive integer. Nothing about the shape of the
output says "parse failure".

**Which row exposes it:** R05 — `SRC_B / '420.00' / '12.0'`. It is the only
row whose `units_raw` carries a decimal point. Deliberately, its amount is the
*cleanest* string in the table (`'420.00'`: no `$`, no comma, no whitespace,
no sign), so attention while debugging goes to the units column and nowhere
else.

**Reading-time tell:** the target type of a **string** cast is part of the
parse, not a post-processing step. Wherever a string cast has a target
narrower than the form the value is written in, ask what the engine *accepts*,
not what the value *means*. The second tell is structural and is the real
lesson of the whole `try_*` family: **`try_cast` converts a loud failure into
a silent one.** A plain `cast('12.0' AS INT)` under ANSI throws
`CAST_INVALID_INPUT` and names the offending value (measured); wrapping it in
`try_` buys robustness by *destroying the diagnostic*. `try_*` is a
data-quality decision, not a robustness improvement — and a NULL it produces
is indistinguishable from a NULL the source actually sent.

**Adjacent, not the trap:** R03 `'-$120.00'`. Cleaning the amount with the
greedy `regexp_replace(amount_raw, '[^0-9.]', '')` ("strip everything that
isn't a digit or a dot") also strips the **minus sign** and turns the refund
into `+120.0` (measured), moving SRC_A's `net_amount` from 1980.5 to 2220.5.
The spec states the refund convention and the EXAMPLE box shows R03 resolved,
so this is work, not a hidden trap — but it is the same lesson in a different
clothes: blacklist the noise (`[$,]`), never whitelist the signal.

---

## Part 4 — Reference answers

### Route A — the parse *is* the validity test

One projection turns both raw strings into typed values; NULL from that
projection is the single, only definition of "unusable". Everything
downstream is ordinary aggregation.

```python
def ref_solve_dsl_a(df):
    amount = F.regexp_replace(F.col("amount_raw"), r"[$,]", "").try_cast("double")
    units = F.col("units_raw").try_cast("double").try_cast("int")

    parsed = df.select("source", amount.alias("amount"), units.alias("units"))

    agg = parsed.groupBy("source").agg(
        F.count(F.lit(1)).cast("int").alias("n_records"),
        F.count(F.when(F.col("amount").isNull(), F.lit(1))).cast("int").alias("n_bad_amount"),
        F.sum("amount").alias("net_amount"),
        F.sum("units").cast("int").alias("total_units"),
    )
    return agg.withColumn(
        "avg_unit_price",
        F.round(F.try_divide(F.col("net_amount"), F.col("total_units")), 2),
    )
```

```sql
-- ref_solve_sql_a
WITH parsed AS (
    SELECT
        source,
        try_cast(regexp_replace(amount_raw, '[$,]', '') AS DOUBLE) AS amount,
        try_cast(try_cast(units_raw AS DOUBLE) AS INT)             AS units
    FROM raw_sales
),
agg AS (
    SELECT
        source,
        CAST(count(1) AS INT)                                       AS n_records,
        CAST(count(CASE WHEN amount IS NULL THEN 1 END) AS INT)     AS n_bad_amount,
        sum(amount)                                                 AS net_amount,
        CAST(sum(units) AS INT)                                     AS total_units
    FROM parsed
    GROUP BY source
)
SELECT source, n_records, n_bad_amount, net_amount, total_units,
       round(try_divide(net_amount, total_units), 2) AS avg_unit_price
FROM agg
```

Notes on the pieces that are load-bearing versus decorative here:

- `count(CASE WHEN amount IS NULL THEN 1 END)` — no `ELSE`. `count(CASE ...
  ELSE 0 END)` counts every row (0 is non-NULL) and reports the group size.
  Day 12's threshold-count trap, reused verbatim.
- `sum(amount)` needs **no** `COALESCE`: every source in this data has at
  least one parseable amount, so the empty-`SUM`-is-NULL case never fires. A
  `COALESCE(sum(amount), 0.0)` here is **dead code**, not defense — the Day 15
  / Day 16 load-bearing-vs-dead distinction. Same for `sum(units)`: SRC_C's
  units are three usable **zeros**, so the sum is a genuine `0`, never NULL.
- `try_divide` **is** load-bearing, and SRC_C is what makes it so: `total_units`
  is exactly `0`, and `net_amount / total_units` throws `DIVIDE_BY_ZERO` under
  ANSI (measured). Note the ordering dependency — had SRC_C's units been
  *unknown* rather than zero, `sum(units)` would have been NULL, `x / NULL`
  would have returned NULL, and the divide guard would never have fired at
  all. Whether the ANSI divide surface exists depends on whether you
  `COALESCE` before or after the division.

### Route B — the regex is the validity contract

Decide usability with an explicit pattern, then convert only what passed.
This is what a team writes when it wants the ingestion contract written down
rather than delegated to cast internals.

```python
def ref_solve_dsl_b(df):
    cleaned = F.regexp_replace(F.col("amount_raw"), r"[$,]", "")
    amount = F.when(
        F.trim(cleaned).rlike(r"^-?\d+(\.\d+)?$"),
        F.trim(cleaned).try_cast("double"),
    )
    units = F.when(
        F.trim(F.col("units_raw")).rlike(r"^\d+(\.0*)?$"),
        F.trim(F.col("units_raw")).try_cast("double").try_cast("int"),
    )
    # ... identical groupBy/agg tail as Route A
```

**The tradeoff is not performance — it is who owns the definition of
"valid".** Measured, the two routes produce the *same* physical plan (see
below): same Exchange count, same aggregate, same scan. What differs is that
Route B now maintains **two** definitions of validity that must agree, and on
this test data they happen to agree on every row. Where they diverge (neither
case is in the data):

| input | Route A (`try_cast` chain) | Route B (regex-gated) | which is right? |
|---|---|---|---|
| `'2.5'` | `2` — silently **truncated** | NULL — rejected | Route B; spec says *whole* count |
| `'3000000000'` | NULL — INT overflow | regex says valid, then NULL | tie, but see below |
| `'1.2e3'` | `1200` | NULL — regex rejects `e` | arguable |
| `'NaN'` / `'Infinity'` | a real double (`nan` / `inf`) | NULL — rejected | Route B |

Route A's double-hop silently accepts a fractional count and truncates it;
Route A's amount parse silently accepts `'NaN'` and `'Infinity'` as numbers
(measured — `try_cast('NaN' AS DOUBLE)` is `nan`, not NULL). Both are real
holes that this test data cannot expose. Route B's hole is the mirror image:
if the regex ever drifts out of sync with the cast, the two disagree and the
`CASE` branch quietly wins. Note also that Route B **must still use
`try_cast`** inside the guarded branch — the `'3000000000'` row passes
`^\d+$`, and a plain `.cast("double").cast("int")` there throws
`CAST_OVERFLOW` (measured). A regex gate does not make a cast safe.

`to_number` / `try_to_number` with a format string (`try_to_number('$1,234.50',
'$999,999.99')` → `1234.50`, measured) is a real third option and is the
*right* tool when one source has one fixed layout. It is the wrong tool here:
a single format cannot cover `'$1,250.00'`, `'  850.50  '` and `'420.00'` at
once, which is precisely the condition of a multi-source landing table.

---

## Part 5 — Concept takeaways

- **ANSI is about *where the failure surfaces*, not about correctness.**
  Legacy mode returned NULL and let bad data through; ANSI throws and stops
  the job; `try_*` returns NULL again but only at the exact site you named.
  The three are not a quality ladder — they are three different choices about
  who finds out.
- **The target type is part of the parse.** `try_cast(s AS INT)` and
  `try_cast(try_cast(s AS DOUBLE) AS INT)` are different parsers, not the same
  parser with a cast tacked on. String→numeric requires a literal the target
  type accepts; numeric→numeric truncates and range-checks. Conflating them is
  this day's whole trap.
- **`try_cast` is a `Column` method, not a function.** `F.try_cast` **does not
  exist** in PySpark 4.1.1 (measured `hasattr(F, "try_cast") == False`) —
  it is `Column.try_cast(dataType)`. Meanwhile `F.try_divide`,
  `F.try_to_number`, `F.try_element_at` **are** functions. SQL has all of them
  under one uniform `try_*(...)` syntax plus `try_cast(x AS T)` as grammar.
  Same shape of API-surface asymmetry as Day 12's `F.grouping_sets` (doesn't
  exist; `DataFrame.groupingSets` does) — when in doubt about a `try_`, check
  whether it lives on the Column or in `functions`.
- **You cannot read eval mode off a physical plan.** Route A's plan renders as
  `round((net_amount#8 / cast(total_units#9 as double)), 2)` — a bare `/` —
  yet it returns NULL on a zero divisor instead of throwing (both measured).
  `try_divide` lowers to a `Divide` carrying `EvalMode.TRY`, which the plan
  printer does not show. Any review of ANSI safety has to happen on the
  *source*, never on `explain()` output.
- **Whitespace is free; separators are not.** `try_cast(' 12.5 ' AS DOUBLE)`
  → `12.5` — the cast trims. `try_cast('1,234.50' AS DOUBLE)` → NULL, and
  under a plain `cast` it throws. So `trim()` in a cleaning chain is usually
  decorative, while `regexp_replace(..., '[$,]', '')` is load-bearing.
- **`try_cast` accepts more than you think.** `'NaN'` → `nan`, `'Infinity'` →
  `inf`, `'1.2e3'` → `1200.0` (all measured). A non-NULL result from
  `try_cast` is not proof that the source sent a sane number, and `nan`
  poisons every downstream `SUM` it touches.
- **A `try_*` NULL and a source NULL are the same NULL.** Once you have chosen
  `try_`, "the value was missing" and "the value was garbage" are no longer
  distinguishable downstream. If the report needs to tell them apart — this
  day's `n_bad_amount` deliberately does not — the distinction has to be
  captured in the same projection that does the parse, before the information
  is gone.
- **`COALESCE(risky_expr, fallback)` is the pattern ANSI breaks.** The
  fallback only ever runs if the risky expression *returns* rather than
  *throws*. Under ANSI the throw comes first and the `COALESCE` is unreachable
  code that reads like a safety net. Day 14 hit this with `element_at`; this
  day's `sum(amount)` `COALESCE` is the harmless inverse — unreachable for a
  different reason (no source has an empty parse set).

## Physical plan notes

Measured on the 10-row test data, `local[2]`, `shuffle.partitions=4`, AQE
**off** (`spark.sql.adaptive.enabled=false`, so the counts below are the plan
itself and not an AQE-rewritten copy).

| route | Exchange | HashAggregate | Sort | SortAggregate | Scan |
|---|---|---|---|---|---|
| Route A (DSL) | **1** | 2 (partial + final) | 0 | 0 | 1 |
| Route A (SQL) | **1** | 2 (partial + final) | 0 | 0 | 1 |
| Route B (DSL) | **1** | 2 (partial + final) | 0 | 0 | 1 |

Identical, node for node. Both routes are one `Project` over the scan, a
partial aggregate, one `Exchange hashpartitioning(source, 4)`, a final
aggregate, and a closing `Project` for `avg_unit_price`. There is **no**
performance story between the routes and none should be invented — the entire
difference is semantic. Worth noting for its own sake: all four aggregates are
fixed-width (`count`/`sum` on numerics), so this stays `HashAggregate`; Day 17
showed that a `min`/`max` on a STRING column would have forced
`SortAggregate` instead.

The closing `avg_unit_price` projection adds **no** Exchange — it is computed
from two already-aggregated columns in the same stage as the final aggregate,
which is why writing it as a post-`agg` `withColumn` costs nothing versus
inlining it into the `agg`.

With AQE left at its default (on), the executed plan additionally carries one
`AQEShuffleRead coalesced` — the same 4→1 partition coalescing seen on Day 18,
and irrelevant to the route comparison.

## Echoes

- **Day 14** — the origin of this topic. `element_at(empty_array, 1)` throwing
  under ANSI, and the `COALESCE` around it that assumed a NULL ANSI never
  delivers. Day 20 generalizes it: the pattern was never about `element_at`,
  it was about every expression that can fail.
- **Day 12** — `F.grouping_sets` doesn't exist / `DataFrame.groupingSets`
  does. Exactly reprised by `F.try_cast` doesn't exist / `Column.try_cast`
  does. The heuristic "check which namespace it lives in before trusting a
  generated call" now has two independent confirmations.
- **Day 15 / Day 16** — load-bearing versus dead defensive code. Here
  `try_divide` is load-bearing (SRC_C forces it) while `COALESCE(sum(amount),
  0.0)` is dead, and the reason SRC_C forces it is *structural*: units that
  sum to a genuine zero, not units that are missing. Day 16's "defensive code
  is dictated by structure, not by spec" applies verbatim.
- **Day 12** (second echo) — `count(CASE WHEN cond THEN 1 END)` with no
  `ELSE`. The `ELSE 0` variant counts the whole group.
- **Day 19** — the failure shape. Day 19's trap was one wrong cell in a 5x5
  grid; this one is two wrong cells in a 3x6 grid, both in a single source's
  row, with the other four columns of that same row correct.
