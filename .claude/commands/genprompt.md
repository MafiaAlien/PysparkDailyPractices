---
description: Emit the Stage-2 incognito prompt (never the solution itself)
argument-hint: "[day number]"
---

The user is entering Stage 2 (GENERATE). **Do not solve the problem.**
An AI solution produced in this repo is contaminated by the reference answers
sitting on disk — that is the entire reason this command exists.

## What to read

Only the header docstring of `days/day$ARGUMENTS*.py`: PROBLEM, INPUT SCHEMA,
EXPECTED OUTPUT, EXAMPLE. Do **not** open `refs/*_ref.md`. Do **not** read Part 4 of
anything.

## What to output

One fenced block, ready to paste into a separate incognito conversation,
containing:

- PROBLEM / INPUT SCHEMA / EXPECTED OUTPUT / EXAMPLE, verbatim
- the two required signatures:
  - `ai_solve_dsl(df: DataFrame) -> DataFrame`
  - `ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame`
- the instruction to solve it twice — once with the DataFrame API, once with
  Spark SQL
- the exact output column names **in order**
- "return only the two functions — no test code, no explanation, no comments
  about edge cases"

It must **not** contain:

- any hint of any kind
- any mention that a trap exists
- the test data or the `expected` list
- any expected output rows beyond the EXAMPLE already in the problem statement

## After the block

One line only: paste the AI's answer into Part 3 **unmodified**, then review
it by reading before running anything.
