---
description: Emit the Stage-2 incognito prompt (never the solution itself)
argument-hint: "[day number]"
---

The user is entering Stage 2 (GENERATE). **Do not solve the problem.**
An AI solution produced in this repo is contaminated by the reference answers
sitting on disk — that is the entire reason this command exists.

## Day 21 and earlier — the pre-ETL branch

Days up to and including **Day 21** were written from
`templates/template_v2.py`, not `templates/template_etl.py`. For those days
everything below still applies except:

- The docstring blocks are **PROBLEM / INPUT SCHEMA / EXPECTED OUTPUT /
  EXAMPLE**. `BUSINESS CONTEXT` / `INPUT TABLES` / `OUTPUT CONTRACT` /
  `PRODUCTION CONSTRAINTS` do not exist in a v2 file — do not go looking for
  them and do not invent them.
- Part 1 is a **`class Solution`** with `solve_dsl` / `solve_sql` methods, not
  two module-level functions.
- There are **no production constraints** on these days. Nothing to carry
  into the prompt; emit no `P#` lines.
- The two emitted signatures are fixed, with `self` dropped — the harness
  calls `ai_solve_dsl(df)`, so a leftover `self` would bind `df` to it:

  ```
  ai_solve_dsl(df: DataFrame) -> DataFrame
  ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame
  ```

From Day 22 on this branch is dead; delete it once Day 21 is generated.

## What to read

Only the header docstring of `days/day$ARGUMENTS*.py` — BUSINESS CONTEXT,
INPUT TABLES, OUTPUT CONTRACT, PRODUCTION CONSTRAINTS, and the example boxes
— plus the **two function signatures in Part 1**. Do **not** open
`refs/*_ref.md`. Do **not** read Part 4 of anything. Do **not** read the
test-data literals in the harness — the `spark.createDataFrame` blocks (one
per input table; `data = ` on a pre-Day-22 file) and the `expected` list.

## What to output

One fenced block, ready to paste into a separate incognito conversation,
containing:

- BUSINESS CONTEXT / INPUT TABLES / OUTPUT CONTRACT / example boxes, verbatim
- the schema of **every** input table — a missing table makes the answer
  unusable and the blind review meaningless
- the **numbered production constraints P1/P2/P3, verbatim** (Day 22 onward;
  a pre-Day-22 file declares none — emit none). They are public
  requirements stated in the problem, not hints, and the AI must be held to
  them exactly as the user is.
- the two required signatures, **copied from Part 1 of the day file** —
  same function names, same parameter names, same order, same type hints,
  with only an `ai_` prefix added. Never emit a hardcoded name: from Day 22
  the functions are named after the output table and differ every day.
  For a day whose Part 1 reads
  `build_daily_store_revenue_dsl(orders, stores, fx_rates)`, emit
  `ai_build_daily_store_revenue_dsl(orders: DataFrame, stores: DataFrame,
  fx_rates: DataFrame) -> DataFrame` and the matching `_sql` form with
  `spark: SparkSession` first.
- the instruction to solve it twice — once with the DataFrame API, once with
  Spark SQL
- the exact output column names **in order**
- "return only the two functions — no test code, no explanation, no comments
  about edge cases"

It must **not** contain:

- any hint of any kind
- any mention that a trap exists
- the test data or the `expected` list
- any expected output rows beyond the example boxes already in the problem
  statement
- the ETL layer / business domain / failure mode labels, wherever they appear
  — the reference file carries all three, and the day-file docstring heads
  itself with the ETL layer and the domain. Reading them in the docstring you
  are told to emit verbatim does not make them emittable: strip those two
  lines out of the block.

## After the block

One line only: paste the AI's answer into Part 3 **unmodified**, then review
it by reading before running anything.
