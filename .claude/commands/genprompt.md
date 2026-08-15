---
description: Emit the Stage-2 incognito prompt (never the solution itself)
argument-hint: "[day number]"
---

The user is entering Stage 2 (GENERATE). **Do not solve the problem.**
An AI solution produced in this repo is contaminated by the reference answers
sitting on disk — that is the entire reason this command exists.

## What to read

Only the header docstring of `days/day$ARGUMENTS*.py` — BUSINESS CONTEXT,
INPUT TABLES, OUTPUT CONTRACT, PRODUCTION CONSTRAINTS, and the example boxes
— plus the **two function signatures in Part 1**. Do **not** open
`refs/*_ref.md`. Do **not** read Part 4 of anything. Do **not** read the
`data = ` / `expected = ` literals in the harness.

## What to output

One fenced block, ready to paste into a separate incognito conversation,
containing:

- BUSINESS CONTEXT / INPUT TABLES / OUTPUT CONTRACT / example boxes, verbatim
- the schema of **every** input table — a missing table makes the answer
  unusable and the blind review meaningless
- the **numbered production constraints P1/P2/P3, verbatim**. They are public
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
- the ETL layer / business domain / failure mode labels from the reference
  file

## After the block

One line only: paste the AI's answer into Part 3 **unmodified**, then review
it by reading before running anything.
