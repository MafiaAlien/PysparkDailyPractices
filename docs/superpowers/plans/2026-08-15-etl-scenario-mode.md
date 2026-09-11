# ETL Scenario Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch daily problem generation from "one new operator per day" to "one complete production ETL pipeline per day", starting Day 22.

**Architecture:** Two new template files (`templates/template_etl.py`, `templates/template_etl_ref.md`) become the scaffold for ETL-scenario days; the four slash commands in `.claude/commands/` and `CLAUDE.md` are updated to drive and grade that scaffold. Template v2 and every existing `days/day01–20` file are left untouched. No `log/` file is edited by this plan — HARD RULE 6 reserves that for `/digest`, so the roadmap changes are delivered as new `/digest` instructions instead.

**Tech Stack:** PySpark 4.1.1 (`.venv/bin/python`), Spark local[2], Markdown command files, no test framework in this repo.

## Global Constraints

- **HARD RULE 1** — never write into Part 1 of `days/*.py`. Part 1 belongs to the user.
- **HARD RULE 2** — never produce a Stage-2 "independent AI solution" inside this repo.
- **HARD RULE 3** — never read `refs/*_ref.md` (the sealed answer files). `templates/template_ref.md` and the new `templates/template_etl_ref.md` are templates, not sealed answers — reading and editing those is fine.
- **HARD RULE 6** — never edit `log/problem_log_and_roadmap.md` or `log/key_takeaways.md` outside `/digest`. **No task in this plan edits `log/`.**
- Script content (code, comments, docstrings, identifiers, SQL) is **100% English**. Markdown command files and this repo's docs stay Chinese-with-English-identifiers where they already are.
- Run Python with `.venv/bin/python` — the system python has no pyspark.
- Severity order used everywhere: `runtime break > wrong results on dirty data > production robustness > performance > portability > style`.
- Spec of record: [`docs/superpowers/specs/2026-08-15-etl-scenario-mode-design.md`](../specs/2026-08-15-etl-scenario-mode-design.md).
- Blacklist that must appear verbatim in CLAUDE.md and `/newday`: `try_*` family (`try_cast` / `try_to_number` / `try_divide` / `try_sum` / `try_element_at` / `try_add` / `try_avg` / `try_parse_url`), `pandas_udf` / ArrowEvalPython, `session_window` built-in, `zip_with` / `transform_keys` / `transform_values` / `map_filter`.
- ANSI mode is **not** blacklisted. Only the `try_*` API is.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `templates/template_etl.py` | Create | Day-file scaffold for ETL scenario days: 4-block PROBLEM docstring, Part 1 (two module-level job functions), Part 3 paste zone + REVIEW_NOTES, Part 2 harness, Part 5 |
| `templates/template_etl_ref.md` | Create | Sealed-reference scaffold: trap, per-constraint audit, pipeline stage breakdown, routes, plan notes, echoes |
| `templates/template_v2.py` | Untouched | Still used by drills and Day 21 |
| `templates/template_ref.md` | Untouched | Same |
| `.claude/commands/newday.md` | Modify | Three-axis topic selection, ETL template, new data-size caps, stage-count difficulty |
| `.claude/commands/genprompt.md` | Modify | Multi-table schemas, copy Part 1 signatures verbatim, carry P1/P2/P3 through |
| `.claude/commands/review.md` | Modify | New severity tier, per-constraint grading |
| `.claude/commands/digest.md` | Modify | Second log table for ETL days, scheduling matrix, no back-fill of Day 1–20 |
| `CLAUDE.md` | Modify | Severity tier, ETL-mode section + blacklist, repo layout |

**Verification approach.** This repo has no test framework. Task 1 is verified by building a throwaway filled-in day from the new template in the scratchpad and actually running it — that is the only task with executable behavior. Every other task is a Markdown edit verified by `grep`, by reading the diff, and (Tasks 3–7) by a consistency check against the spec.

Scratchpad root for all throwaway files:
`<scratchpad>`

---

### Task 1: ETL day-file template

**Files:**
- Create: `templates/template_etl.py`
- Test: `<scratchpad>/smoke_etl_day.py` (throwaway, never committed)

**Interfaces:**
- Consumes: nothing.
- Produces: the placeholder identifiers that Tasks 4 and 5 must rename or copy — input tables `table_a` / `table_b`, job functions `build_output_table_dsl(table_a, table_b) -> DataFrame` and `build_output_table_sql(spark, table_a, table_b) -> DataFrame`, AI counterparts `ai_build_output_table_dsl` / `ai_build_output_table_sql`, paste markers `# >>> PASTE BEGIN` / `# >>> PASTE END`, and the harness function `check(actual, expected_rows, label)`.

The placeholders are deliberately **valid Python identifiers** (`table_a`, not `<table_a>`) so the template itself parses and can be smoke-run. `/newday` renames them per day.

- [ ] **Step 1: Write the smoke test that the template must satisfy**

Create `<scratchpad>/smoke_etl_day.py`. This is a minimal *filled-in* day used only to prove the template's block ordering works — in particular that the Part 3 paste zone sits **above** `__main__` so the Stage-4 calls resolve. Write it now, before the template, so the template has a target to satisfy.

```python
"""Smoke: proves ETL template block ordering. Throwaway."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# PART 1 — YOUR JOB
def build_output_table_dsl(table_a: DataFrame, table_b: DataFrame) -> DataFrame:
    return (
        table_a.join(table_b, "k", "left")
        .groupBy("k")
        .agg(F.sum("v").alias("total"))
        .select("k", "total")
    )


def build_output_table_sql(
    spark: SparkSession, table_a: DataFrame, table_b: DataFrame
) -> DataFrame:
    table_a.createOrReplaceTempView("table_a")
    table_b.createOrReplaceTempView("table_b")
    return spark.sql(
        """
        SELECT a.k AS k, SUM(a.v) AS total
        FROM table_a a LEFT JOIN table_b b ON a.k = b.k
        GROUP BY a.k
        """
    )


# PART 3 — paste zone (must be ABOVE __main__)
# >>> PASTE BEGIN
def ai_build_output_table_dsl(table_a: DataFrame, table_b: DataFrame) -> DataFrame:
    return build_output_table_dsl(table_a, table_b)


def ai_build_output_table_sql(
    spark: SparkSession, table_a: DataFrame, table_b: DataFrame
) -> DataFrame:
    return build_output_table_sql(spark, table_a, table_b)
# >>> PASTE END


# PART 2 — harness
def check(actual: DataFrame, expected_rows: list, label: str) -> None:
    actual_rows = sorted([tuple(r) for r in actual.collect()])
    expected = sorted(expected_rows)
    status = "PASS" if actual_rows == expected else "FAIL"
    print(f"[{status}] {label}")
    if status == "FAIL":
        print("  expected:", expected)
        print("  actual  :", actual_rows)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("smoke")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    table_a = spark.createDataFrame([("x", 1), ("x", 2), ("y", 5)], "k string, v int")
    table_b = spark.createDataFrame([("x",)], "k string")
    expected = [("x", 3), ("y", 5)]

    check(build_output_table_dsl(table_a, table_b), expected, "DSL")
    check(build_output_table_sql(spark, table_a, table_b), expected, "SQL")
    check(ai_build_output_table_dsl(table_a, table_b), expected, "AI-DSL")
    check(ai_build_output_table_sql(spark, table_a, table_b), expected, "AI-SQL")

    spark.stop()
```

- [ ] **Step 2: Run the smoke test to confirm the ordering is sound**

```bash
.venv/bin/python "<scratchpad>/smoke_etl_day.py"
```

Expected: four lines, all `[PASS]` — `DSL`, `SQL`, `AI-DSL`, `AI-SQL`. If any line says `NameError`, the block ordering is wrong and the template must not be written until it passes.

- [ ] **Step 3: Write `templates/template_etl.py`**

Full content:

````python
"""
=====================================================================
PySpark Daily Practice — ETL Scenario Template (Day 22 onward)
=====================================================================
Day N — <output_table_name>  (<Medium / Medium-Hard / Hard>)

ETL layer : <L1 landing cleanup | L2 detail modeling | L3 serving | L4 incremental>
Domain    : <e-commerce orders | ad delivery | payment reconciliation |
             clickstream | logistics | content | subscription billing>

BUSINESS CONTEXT
----------------
<1-2 sentences: who consumes this table, in which system, how often the job
runs, and what upstream feeds it.>

INPUT TABLES
------------
<table_a>(col: type, ...)   -- append-only event feed; upstream may replay
<table_b>(col: type, ...)   -- daily snapshot dimension
<table_c>(col: type, ...)   -- hand-maintained config table

Render each input as a df.show()-style ASCII box with a header row.

<table_a>:

    +-------+-------+-------+
    | col_a | col_b | col_c |
    +-------+-------+-------+
    | ...   | ...   | ...   |
    +-------+-------+-------+

Per-row annotations go UNDER the box as notes, never inside the cells.

OUTPUT CONTRACT
---------------
Table    : <output_table_name>
Grain    : one row per <what>
Columns  : <name: type>, <name: type>, ...   (this exact order)
Ordering : irrelevant — check() sorts both sides

Expected:

    +-------+-------+
    | out_a | out_b |
    +-------+-------+
    | ...   | ...   |
    +-------+-------+

PRODUCTION CONSTRAINTS
----------------------
P1. <e.g. Re-running the same batch twice must produce byte-identical rows.>
P2. <e.g. An event arriving up to 3 days late belongs to its EVENT date,
     not its arrival date.>
P3. <optional third>

These are requirements, not hints. Missing one fails the tests.

WORKFLOW (v2)
-------------
Stage 1  SOLVE   : Implement the two Part 1 functions. Run this file.
Stage 2  GENERATE: Run /genprompt and paste the emitted prompt into a
                   SEPARATE incognito conversation. Never ask for the
                   solution inside this repo.
Stage 3  REVIEW  : Paste the AI answer into the Part 3 zone, UNMODIFIED.
                   Review by reading only. Fill REVIEW_NOTES and commit a
                   VERDICT BEFORE running anything.
Stage 4  VERIFY  : Un-comment the Stage-4 lines in Part 2 and run.
Stage 5  DIGEST  : Fill Part 5, then run /review and /digest.

Reference answers and concept takeaways live in refs/<this-file>_ref.md.
Do not open it before Stage 5.
=====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# #####################################################################
# ##                                                                 ##
# ##   PART 1 — YOUR JOB                                             ##
# ##                                                                 ##
# ##   Stage 1 touches this block and nothing else in this file.     ##
# ##                                                                 ##
# #####################################################################

# ------------------------------ 1a. DataFrame API --------------------
def build_output_table_dsl(
    table_a: DataFrame,
    table_b: DataFrame,
) -> DataFrame:
    """<one line: what this job produces>

    Hint: <one short hint>
    """
    # TODO: implement
    pass


# ------------------------------ 1b. Spark SQL ------------------------
def build_output_table_sql(
    spark: SparkSession,
    table_a: DataFrame,
    table_b: DataFrame,
) -> DataFrame:
    """<same job, Spark SQL>

    Hint: <one short hint>
    """
    table_a.createOrReplaceTempView("table_a")
    table_b.createOrReplaceTempView("table_b")

    sql = """
        -- write your SQL here
    """
    return spark.sql(sql)


# #####################################################################
# ##   END OF PART 1                                                 ##
# #####################################################################


# #####################################################################
# ##                                                                 ##
# ##   PART 3 — PASTE THE INCOGNITO AI ANSWER BELOW                  ##
# ##                                                                 ##
# ##   UNMODIFIED. Two functions, `ai_` prefix, signatures identical ##
# ##   to Part 1. Do not reformat it. Do not fix anything you spot — ##
# ##   spotting it is exactly what Stage 3 measures.                 ##
# ##                                                                 ##
# #####################################################################

# >>> PASTE BEGIN

# >>> PASTE END

# ---------------------------------------------------------------------
# REVIEW_NOTES — fill in BEFORE running anything.
# Severity order: runtime break > wrong results on dirty data
#               > production robustness > performance > portability > style
# ---------------------------------------------------------------------
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes:
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes:
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (cast, division, array index, element_at)?
#     notes:
# [ ] Production — walk P1/P2/P3 one at a time, each on its own line.
#     Re-run idempotent? Late data attributed to the event date? The same
#     metric computed the same way in every branch? Quality assertions there?
#     notes:
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (reading-stage hypothesis only — verified with .explain() later,
#     never asserted from memory)
#     notes:
# [ ] Robustness — hardcoded values, assumptions not in the output contract?
#     notes:
# [ ] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS / FAIL — because:
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything the reading missed?


# #####################################################################
# ##   PART 2 — HARNESS.  Do not edit.                               ##
# #####################################################################
def check(actual: DataFrame, expected_rows: list, label: str) -> None:
    """Order-insensitive comparison; sorted() absorbs tie-order instability."""
    actual_rows = sorted([tuple(r) for r in actual.collect()])
    expected = sorted(expected_rows)
    status = "PASS" if actual_rows == expected else "FAIL"
    print(f"[{status}] {label}")
    if status == "FAIL":
        print("  expected:", expected)
        print("  actual  :", actual_rows)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Input tables: <= 8 rows each, <= 25 rows across ALL inputs.
    table_a = spark.createDataFrame(
        [
            # TODO: rows (include the trap rows)
        ],
        schema="<schema string>",
    )
    table_b = spark.createDataFrame(
        [
            # TODO: rows
        ],
        schema="<schema string>",
    )

    expected = [
        # TODO: hand-derived tuples, one per output row
    ]

    check(build_output_table_dsl(table_a, table_b), expected, "DSL")
    check(build_output_table_sql(spark, table_a, table_b), expected, "SQL")

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    # check(ai_build_output_table_dsl(table_a, table_b),
    #       expected, "AI-DSL (post-review verification)")
    # check(ai_build_output_table_sql(spark, table_a, table_b),
    #       expected, "AI-SQL (post-review verification)")

    spark.stop()


# #####################################################################
# ##   PART 5 — Review takeaways (fill at Stage 5, before /review)   ##
# #####################################################################
# - What did the AI get wrong, or suspiciously right?
# - Which production constraint (P#) did either side miss, and why was it
#   missable by reading?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem: see refs/<this-file>_ref.md.
````

- [ ] **Step 4: Verify the template parses and the placeholders are valid identifiers**

```bash
.venv/bin/python -c "import ast,sys; ast.parse(open('templates/template_etl.py').read()); print('parses OK')"
```

Expected: `parses OK`. (The template is never *run* — `build_output_table_dsl` returns `None` by design — but it must parse, since `/newday` copies it and edits it in place.)

- [ ] **Step 5: Verify block ordering in the committed template**

```bash
grep -n "PART 1 — YOUR JOB\|PASTE BEGIN\|PASTE END\|REVIEW_NOTES\|PART 2 — HARNESS\|^if __name__\|Stage 4 —\|PART 5" templates/template_etl.py
```

Expected line numbers in strictly increasing order: `PART 1` < `PASTE BEGIN` < `PASTE END` < `REVIEW_NOTES` < `PART 2 — HARNESS` < `if __name__` < `Stage 4 —` < `PART 5`. This is the defect being fixed — in `templates/template_v2.py` the paste placeholder sits *after* `if __name__`, so a literal paste `NameError`s on the Stage-4 calls.

- [ ] **Step 6: Commit**

```bash
git add templates/template_etl.py && git commit -m "Add ETL scenario day template

Module-level job functions named after the output table, multi-table
signatures, an explicit paste zone above the harness so Stage-4 calls
resolve, and a Production tier in REVIEW_NOTES."
```

---

### Task 2: ETL reference template

**Files:**
- Create: `templates/template_etl_ref.md`

**Interfaces:**
- Consumes: the `P1/P2/P3` constraint numbering and the job function names from Task 1.
- Produces: the section headings `/review` step 4 and `/digest` will read — `## The trap`, `## Production constraints — audit`, `## Pipeline stages`, `## Part 4 — Reference answers`, `## Part 5 — Concept takeaways`, `## Physical plan notes`, `## Echoes`.

- [ ] **Step 1: Write `templates/template_etl_ref.md`**

Full content:

````markdown
# Day N — <output_table_name> — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

ETL layer : <L1 | L2 | L3 | L4>
Domain    : <business domain>
Failure mode (the trap axis — never stated in the day file):
            <replay | late data | dimension drift | metric drift | nullable key
             | empty collection | boundary closure | skew | decomposability>

## The trap

**What it is:** <one sentence>

**Why a naive solution passes anyway:** <which rows it gets right, and under
what clean-data assumption — the "coincidentally correct" framing>

**Which row exposes it:** <point at the specific input row, by table and value>

**Reading-time tell:** <what a reviewer could have noticed without running>

**Which pipeline stage it lives in:** <name the stage from the list below —
in a multi-stage job, saying "the trap is in the dedup stage" is most of the
diagnostic value>

---

## Production constraints — audit

One row per constraint declared in the day file. These are public
requirements, not traps; this table is how Stage 5 grades whether each side
actually satisfied them.

| # | Constraint | Satisfied by | How it fails if ignored |
|---|---|---|---|
| P1 | <verbatim from the day file> | <the exact expression or stage> | <observable symptom> |
| P2 | <...> | <...> | <...> |

---

## Pipeline stages

The reference solution, decomposed into named stages. Stage 5 aligns the
user's, the AI's, and this solution stage by stage — a long job diverges in
one stage while the rest matches, and naming the stages is what makes that
visible.

1. **<stage name>** — <what it does, one line>
2. **<stage name>** — <...>
3. **<stage name>** — <...>

---

## Part 4 — Reference answers

### Route A — <name>

```python
def ref_build_output_table_dsl_a(table_a, table_b):
    ...
```

```sql
-- ref_build_output_table_sql_a
```

### Route B — <name, only if meaningfully different>

<When two routes differ, state the actual tradeoff: Exchange count, whether
the second stage is a partial-aggregable aggregate or a full sort, whether
one route changes the grain. If the two routes are node-level isomorphic,
say so — "no performance story here, none should be invented".>

---

## Part 5 — Concept takeaways

- <3–8 dense bullets, mechanism-level, not syntax-level>
- <name any API contract that differs between DSL and SQL for this topic>
- <name any behavior that changes under ANSI mode>
- <name which whitelist techniques this day exercised, so the roadmap can
  track coverage>

## Physical plan notes

<Exchange / Expand / Window / HashAggregate / Sort counts for each route,
from an actually executed .explain(). If not yet measured, write NOT MEASURED
— never assert a count from memory. "Fewer shuffles" is a tempting and
frequently wrong story: when two routes have the SAME Exchange count, name
the real difference instead of inventing a shuffle gap.>

## Echoes

<Which existing log/04 entries this day reinforces, refines, or contradicts.>
````

- [ ] **Step 2: Verify every heading `/review` and `/digest` depend on is present**

```bash
grep -c "^## The trap$\|^## Production constraints — audit$\|^## Pipeline stages$\|^## Part 4 — Reference answers$\|^## Part 5 — Concept takeaways$\|^## Physical plan notes$\|^## Echoes$" templates/template_etl_ref.md
```

Expected: `7`.

- [ ] **Step 3: Commit**

```bash
git add templates/template_etl_ref.md && git commit -m "Add ETL scenario reference template

Adds a per-constraint audit table and a named pipeline-stage breakdown to the
existing trap/routes/plan/echoes skeleton, so Stage 5 can align three long
solutions stage by stage."
```

---

### Task 3: `/newday` — three-axis selection and ETL scaffolding

**Files:**
- Modify: `.claude/commands/newday.md`

**Interfaces:**
- Consumes: `templates/template_etl.py`, `templates/template_etl_ref.md` (Tasks 1–2).
- Produces: the reporting contract Task 5 (`/genprompt`) and Task 7 (`/digest`) rely on — every ETL day file declares `ETL layer`, `Domain`, and numbered `P1/P2/P3` constraints in its docstring, and its Part 1 holds exactly two module-level functions named `build_<output_table>_dsl` / `build_<output_table>_sql`.

- [ ] **Step 1: Replace step 2 (topic selection)**

Replace the existing step 2 bullet list with:

```markdown
2. Determine the next day number and pick the day:
   - **Day 21 and earlier style (single-technique days) is retired from Day 22
     on.** From Day 22, every day is one complete production ETL pipeline
     built from already-covered techniques.
   - Pick one cell from each of the three axes, and check the scheduling
     matrix in `log/problem_log_and_roadmap.md` so no (layer, domain,
     failure mode) combination repeats:
     - **ETL layer** (the main axis — what shape of job this is):
       L1 landing cleanup / L2 detail modeling / L3 serving / L4 incremental
     - **Business domain** (the skin — rotate, avoid recent repeats):
       e-commerce orders / ad delivery / payment reconciliation /
       clickstream / logistics / content consumption / subscription billing
     - **Failure mode** (the trap source — NEVER stated in the day file):
       replay / late data / dimension drift / metric drift / nullable key /
       empty collection / boundary closure / skew / decomposability
   - Compose the job from the technique whitelist in `CLAUDE.md`. Do not
     introduce an operator the user has never used — from Day 22 the day is
     about composing known techniques, not learning a new one.
   - **Blacklisted, never schedule:** the `try_*` family (`try_cast` /
     `try_to_number` / `try_divide` / `try_sum` / `try_element_at` /
     `try_add` / `try_avg` / `try_parse_url`), `pandas_udf` /
     ArrowEvalPython, the `session_window` built-in, and
     `zip_with` / `transform_keys` / `transform_values` / `map_filter`.
     Python UDFs are a **review target** only — the AI solution may use one
     and the user should catch it, but never require the user to write one.
     ANSI mode itself is NOT blacklisted; only the `try_*` API is.
   - Difficulty follows the **pipeline stage count**, not the topic:
     3 stages = Medium, 4–5 = Medium-Hard, 6+ = Hard (use sparingly).
   - if `$ARGUMENTS` is non-empty, use it as the scenario instead
```

- [ ] **Step 2: Replace step 3 (the STOP report)**

```markdown
3. **STOP.** Propose: day number, **ETL layer**, **business domain**, the
   output table and its grain, the **numbered production constraints
   (P1/P2/P3)** you intend to declare, the pipeline stage count and resulting
   difficulty, and one line on which logged takeaway this day echoes.
   **Never report the failure mode** — that is the trap. Wait for
   confirmation before writing.
```

- [ ] **Step 3: Replace step 4 (which templates to copy)**

```markdown
4. On confirmation, write two files:
   - **Day 21 only** (the last single-technique day, already scheduled as
     unpivot): use `templates/template_v2.py` + `templates/template_ref.md`
     and the pre-Day-22 rules — single `df` parameter, `class Solution`,
     `<= 15` rows, no production constraints. From Day 22 on this branch is
     dead; delete it once Day 21 is generated.
   - **Day 22 onward:**
   - `days/dayNN_<slug>.py` — from `templates/template_etl.py`
   - `refs/dayNN_<slug>_ref.md` — from `templates/template_etl_ref.md`
     (NOT in `days/` — the sealed answers live in the sibling `refs/` dir)
   Rename every placeholder identifier: `table_a` / `table_b` become the real
   input table names, and `build_output_table_dsl` / `build_output_table_sql`
   become `build_<output_table>_dsl` / `build_<output_table>_sql`. Update the
   two `createOrReplaceTempView` calls, the two `check(...)` calls, and the
   two commented Stage-4 lines to match. Leave the `# >>> PASTE BEGIN` /
   `# >>> PASTE END` markers empty.
```

- [ ] **Step 4: Rewrite the "Constraints on the generated day file" section**

Replace the whole section with:

```markdown
## Constraints on the generated day file

- The docstring carries all four PROBLEM blocks: BUSINESS CONTEXT (1–2
  sentences), INPUT TABLES (3–4 tables, each with its upstream nature noted:
  append-only feed / daily snapshot / hand-maintained config), OUTPUT
  CONTRACT (table name, grain, columns in order, ordering irrelevant), and
  PRODUCTION CONSTRAINTS (2–3 numbered `P#` lines).
- Production constraints are **public requirements written in plain sight**,
  never hints toward the trap. Pick them from: re-run idempotency /
  partition-overwrite semantics, late-arriving data attributed to the event
  date, upstream replay dedup, one metric computed identically everywhere,
  data-quality assertions (primary-key uniqueness, row count, non-null rate),
  reading only the necessary partitions and columns.
- Part 1 holds exactly **two module-level functions** — no class. Body is
  `pass` + `# TODO: implement` + a one-line hint in the docstring. Nothing
  else. Do not split the job into more functions: SQL cannot mirror the split,
  and a real submitted job is one unit.
- `build_<output_table>_sql` keeps the `createOrReplaceTempView` scaffolding
  for every input table and an empty SQL string placeholder.
- The Part 3 paste zone stays empty — markers and checklist comments only,
  never a pre-filled solution.
- Part 5 in the day file contains only the unanswered "Review takeaways"
  prompts. Concept takeaways go in `refs/dayNN_<slug>_ref.md`.
- The EXAMPLE boxes render every table as a `df.show()`-style ASCII box with
  a header row — one box per input table, one for the expected output. Never
  bare tuples or prose rows: without column headers the reader cannot tell
  which value is which column. Per-row annotations go under the box as notes,
  not inside the cells.
- Test data: **each input table <= 8 rows, all inputs together <= 25 rows**,
  including the trap rows. Small enough to reason about by hand.
- `expected` must be hand-derived row by row, not produced by running a
  solution. If any expected row is uncertain, say so explicitly.
- Script content 100% English.
- If the scenario touches dates, pin `spark.sql.session.timeZone` in the
  harness (the template already does).
```

- [ ] **Step 5: Rewrite the "Constraints on the reference file" section**

```markdown
## Constraints on the reference file

- Fill the **failure mode** line — it is recorded here and nowhere else.
- Fill the **production-constraint audit table**: one row per `P#`, naming
  the exact expression that satisfies it and the symptom when it is ignored.
- Fill the **pipeline stages** list with named stages, and say which stage
  the trap lives in.
- Show more than one route when they differ meaningfully (e.g. explode vs
  array-function, window vs struct-argmax). If the routes are node-level
  isomorphic, say so instead of inventing a performance story.
- Name the trap explicitly, and name **which naive solution passes anyway**
  on clean rows — the "coincidentally correct" framing.
- Concept takeaways: 3–8 bullets, dense, tied to mechanism not to syntax.
```

- [ ] **Step 6: Verify**

```bash
grep -n "template_etl\|ETL layer\|Business domain\|Failure mode\|<= 25 rows\|pipeline stage count\|try_\*\|pandas_udf" .claude/commands/newday.md
```

Expected: `template_etl.py` and `template_etl_ref.md` both referenced in step 4; the three axes present in step 2; the blacklist present; the row caps present. `templates/template_v2.py` must still appear **exactly once**, inside the Day-21-only branch — Day 21 is generated by this same command and still uses the old scaffold.

- [ ] **Step 7: Commit**

```bash
git add .claude/commands/newday.md && git commit -m "newday: three-axis ETL scenario selection

Topic choice moves from the backlog list to a (layer, domain, failure mode)
matrix, difficulty derives from pipeline stage count, and the blacklist is
stated inline so a retired API cannot be scheduled by accident."
```

---

### Task 4: `/genprompt` — carry multi-table schemas and real signatures

**Files:**
- Modify: `.claude/commands/genprompt.md`

**Interfaces:**
- Consumes: the day-file docstring blocks and Part 1 signatures produced by Task 3.
- Produces: nothing downstream in this plan.

The current file hardcodes `ai_solve_dsl(df: DataFrame)` and `ai_solve_sql(spark: SparkSession, df: DataFrame)`. Those names no longer exist from Day 22 on — the signature is per-day.

- [ ] **Step 1: Replace the "What to read" section**

```markdown
## What to read

Only the header docstring of `days/day$ARGUMENTS*.py` — BUSINESS CONTEXT,
INPUT TABLES, OUTPUT CONTRACT, PRODUCTION CONSTRAINTS, and the example boxes
— plus the **two function signatures in Part 1**. Do **not** open
`refs/*_ref.md`. Do **not** read Part 4 of anything. Do **not** read the
`data = ` / `expected = ` literals in the harness.
```

- [ ] **Step 2: Replace the "What to output" section**

```markdown
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
```

- [ ] **Step 3: Verify**

```bash
grep -n "ai_solve_dsl\|ai_solve_sql" .claude/commands/genprompt.md
```

Expected: **no output** — both hardcoded names are gone.

```bash
grep -c "copied from Part 1\|PRODUCTION CONSTRAINTS\|every.*input table" .claude/commands/genprompt.md
```

Expected: at least `3`.

- [ ] **Step 4: Commit**

```bash
git add .claude/commands/genprompt.md && git commit -m "genprompt: multi-table schemas and per-day signatures

Signatures are now copied from Part 1 rather than hardcoded, since ETL days
name their functions after the output table. Production constraints travel
with the prompt: they are public requirements, so the AI must be held to the
same contract the user is."
```

---

### Task 5: `/review` — new severity tier and per-constraint grading

**Files:**
- Modify: `.claude/commands/review.md`

**Interfaces:**
- Consumes: the reference-file headings from Task 2 and the `P#` numbering from Task 3.
- Produces: nothing downstream in this plan.

- [ ] **Step 1: Replace step 2's severity line**

Current text reads:

```
   **runtime break > wrong results on dirty data > performance > portability > style**
```

Replace with:

```
   **runtime break > wrong results on dirty data > production robustness
   > performance > portability > style**

   `production robustness` covers: re-run not idempotent, late data dropped
   or misattributed, the same metric computed two different ways in two
   branches, missing data-quality assertions. It sits below wrong-results
   because it produces correct output on the first run and only breaks on
   the second — later to surface, but still a correctness class, never
   demotable to performance.
```

- [ ] **Step 2: Insert a per-constraint grading sub-step into step 5**

Insert between the current 5a (Trap) and 5b (Three-way comparison):

```markdown
   a2. **Production constraints, one at a time.** For each `P#` declared in
       the day file, state whether the user's solution satisfies it, whether
       the AI's does, and — using the audit table in the reference file —
       what the observable symptom would be if it does not. A constraint
       that no test row can distinguish gets said out loud, same as any
       other undistinguishable divergence.
```

- [ ] **Step 3: Add pipeline-stage alignment to step 5b**

Append to the existing 5b paragraph:

```markdown
       For an ETL scenario day, align the three solutions **stage by stage**
       using the pipeline-stage list in the reference file. A long job
       usually agrees everywhere but one stage; naming that stage is most of
       the diagnostic value, and a whole-file diff hides it.
```

- [ ] **Step 4: Verify**

```bash
grep -n "production robustness" .claude/commands/review.md
```

Expected: at least two hits (the severity line and the explanation).

```bash
grep -n "a2\.\|stage by stage" .claude/commands/review.md
```

Expected: both present.

- [ ] **Step 5: Commit**

```bash
git add .claude/commands/review.md && git commit -m "review: production robustness tier and per-constraint grading

Grades each declared P# separately and aligns the three solutions stage by
stage, since a long pipeline agrees everywhere but one stage and a whole-file
diff hides which one."
```

---

### Task 6: `/digest` — second log table, scheduling matrix, no back-fill

**Files:**
- Modify: `.claude/commands/digest.md`

**Interfaces:**
- Consumes: the reference-file headings from Task 2 and the axis labels from Task 3.
- Produces: the roadmap structure that Task 3's step 2 reads back (the scheduling matrix).

This task writes **instructions only**. It must not edit `log/` — HARD RULE 6. The roadmap's new sections come into existence the first time `/digest` runs under these instructions.

- [ ] **Step 1: Replace the `log/03` bullet block in step 2**

```markdown
   **`log/03`:**
   - **Day 1–21 rows are frozen.** Do not back-fill the new columns into the
     existing "Completed problems" table — those rows' `Topic` column is not
     the same thing as `层级 / 业务域`, and migrating them loses information.
   - For Day 22 onward, append to a **separate table** titled
     `## ETL scenario days`, created on first use, with columns:
     `Day | 层级 | 业务域 | Difficulty | Output table | 生产约束 | Trap / key edge case`.
     The `生产约束` column lists the `P#` lines verbatim. The trap column is
     dense and specific — match the style of the existing rows, which name
     the mechanism and the conditions under which the wrong answer passes
     anyway.
   - Update the **scheduling matrix**, a section titled
     `## 调度矩阵（已用组合）` created on first use, with columns
     `Day | ETL layer | Domain | Failure mode`. `/newday` reads this to avoid
     repeating a combination. The failure mode is recorded here and in the
     reference file only — never in the day file.
   - Update "Scheduled next"
   - Update any backlog line this day closes out or partially closes
```

- [ ] **Step 2: Verify**

```bash
grep -n "ETL scenario days\|调度矩阵\|frozen\|back-fill" .claude/commands/digest.md
```

Expected: all four present.

```bash
git diff --stat -- log/
```

Expected: **no output** — this task changed nothing under `log/`.

- [ ] **Step 3: Commit**

```bash
git add .claude/commands/digest.md && git commit -m "digest: separate ETL-day log table and scheduling matrix

Day 1-21 rows stay frozen rather than being migrated: their Topic column is
not the same axis as layer/domain, so a back-fill would lose information."
```

---

### Task 7: `CLAUDE.md` — severity tier, ETL mode section, repo layout

**Files:**
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: everything above. This is the constitution, updated last so it describes files that already exist.
- Produces: the whitelist and blacklist that `/newday` step 2 refers to by name.

- [ ] **Step 1: Update the severity line in the "Q&A style" section**

Current:

```
  **runtime break > wrong results on dirty data > performance > portability > style**
```

Replace with:

```
  **runtime break > wrong results on dirty data > production robustness >
  performance > portability > style**
- `production robustness` = re-run not idempotent, late data dropped or
  misattributed, one metric computed two ways, missing quality assertions.
  It sits below wrong-results because it is correct on the first run and
  only breaks on the second — later to surface, still a correctness class,
  never demotable to performance.
```

- [ ] **Step 2: Update the "Repo layout" block**

Add two lines after the existing `templates/template_ref.md` line:

```
templates/template_etl.py     ETL scenario day skeleton (Day 22+)
templates/template_etl_ref.md ETL scenario reference skeleton (Day 22+)
docs/superpowers/specs/       design specs (not part of the daily workflow)
```

- [ ] **Step 3: Insert a new section after "Workflow v2 — five stages"**

```markdown
---

## Problem mode — Day 22 onward

Day 1–21 introduced one new operator or semantic edge per day. That is
**retired**. From Day 22, each day is **one complete production ETL
pipeline**, composed of techniques the user has already practiced. The point
is fluency and production instinct on a long job, not learning a new
function.

Two failure modes drove the switch, both recorded in the roadmap: the
remaining backlog had drifted toward low-production-value APIs, and Days 19
and 20 both lost their whole trap dimension because Stage 1 degenerated into
API teaching.

**Shape of a day:** 3–4 input tables, 1 output table, 3–6 named pipeline
stages, one hidden trap, and 2–3 production constraints written openly in the
problem statement as `P1/P2/P3`.

**Technique whitelist** (these recur; none is ever "the topic" again):
window `row_number` dedup / Top-N / `lag` / `lead` / running sum; ROWS vs
RANGE frames; inner / left / anti / semi joins, `broadcast`, nullable keys
(`<=>` vs sentinel), orphan keys; conditional aggregation, `GROUPING SETS`,
`COUNT(*)` vs `COUNT(col)`; UTC→local bucketing, date-dimension spine
gap-fill, event-date attribution, interval closing; gap-threshold
sessionization, run-based version splitting; SCD2 half-open intervals,
upsert; `explode_outer`, `collect_set`, dot-path access, array&lt;struct&gt;
`filter` / `size` / `aggregate`; NULL-ignoring aggregates, `NOT IN`
three-valued logic, window NULL ordering; pivot / unpivot; salting plus
two-phase aggregation; `explain()` Exchange counting, broadcast detection,
the partition-key subset rule; `regexp_extract` / `regexp_replace` / `split`.

**Blacklist — never schedule:** the `try_*` family (`try_cast` /
`try_to_number` / `try_divide` / `try_sum` / `try_element_at` / `try_add` /
`try_avg` / `try_parse_url`), `pandas_udf` / ArrowEvalPython, the
`session_window` built-in, `zip_with` / `transform_keys` /
`transform_values` / `map_filter`. Python UDFs are a **review target** only:
an AI solution may use one and the user should catch it, but never require
the user to write one.

**ANSI mode is not blacklisted.** Only the `try_*` API is. `cast`, division,
and array indexing still throw under ANSI, so the REVIEW_NOTES ANSI row stays
and still governs every `COALESCE(risky_expr, fallback)` that shows up in
review.

**Not simulated:** no real parquet partition directories. This runs on one
laptop, not a cluster; idempotency and overwrite semantics are expressed
through the output contract and assertions, and the harness stays in-memory
DataFrames.
```

- [ ] **Step 4: Verify the whole rule set is still internally consistent**

```bash
grep -n "template_etl\|production robustness\|try_\*\|pandas_udf\|Day 22" CLAUDE.md
```

Expected: the layout lines, the severity tier, the blacklist, and the new section header all present.

```bash
grep -n "runtime break" CLAUDE.md .claude/commands/review.md
```

Expected: both files show the identical six-tier ordering. A mismatch here means `/review` and the constitution disagree about severity — fix before committing.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md && git commit -m "CLAUDE.md: document ETL scenario mode from Day 22

Adds the production-robustness severity tier, the technique whitelist and
blacklist, and the note that ANSI stays in scope while the try_* API does
not."
```

---

### Task 8: End-to-end dry run against the real templates

**Files:**
- Test: `<scratchpad>/day22_dryrun.py` (throwaway, never committed)

**Interfaces:**
- Consumes: everything from Tasks 1–7.
- Produces: nothing. This is the gate before Day 22 is generated for real.

- [ ] **Step 1: Build a throwaway Day-22-shaped file from the real template**

```bash
cp templates/template_etl.py "<scratchpad>/day22_dryrun.py"
```

Then edit that copy exactly as `/newday` step 4 prescribes: rename `table_a` → `orders`, `table_b` → `stores`, `build_output_table_dsl` → `build_store_revenue_dsl`, `build_output_table_sql` → `build_store_revenue_sql`; update both `createOrReplaceTempView` calls, both `check(...)` calls, and both commented Stage-4 lines. Implement both functions as a trivial join-and-sum, paste a trivial `ai_` pair between the paste markers, fill three `orders` rows and two `stores` rows, hand-derive `expected`, and uncomment the two Stage-4 lines.

- [ ] **Step 2: Run it**

```bash
.venv/bin/python "<scratchpad>/day22_dryrun.py"
```

Expected: four lines, all `[PASS]` — `DSL`, `SQL`, `AI-DSL (post-review verification)`, `AI-SQL (post-review verification)`. A `NameError` on either AI line means the paste zone drifted back below `__main__`; a `FAIL` means the rename missed a call site.

- [ ] **Step 3: Confirm nothing under `log/` or `days/` changed across the whole plan**

```bash
git status --porcelain log/ days/ refs/
```

Expected: **no output**. This plan touches templates, commands, `CLAUDE.md`, and `docs/` only.

- [ ] **Step 4: Report readiness**

State plainly: templates verified by execution, four command files updated, `CLAUDE.md` consistent with `/review` on severity ordering, `log/` untouched. Day 22 can now be generated with `/newday`, which will report layer / domain / output table / grain / `P#` constraints / stage count and stop for confirmation.

---

## Self-Review

**Spec coverage.** §3 three-axis matrix → Task 3 step 1. §4 whitelist and blacklist → Task 7 step 3, restated in Task 3 step 1. §5 production constraints → Task 1 (template `P#` block), Task 3 step 4, Task 4 step 2, Task 5 step 2. §6 severity tier → Tasks 5, 7. §7.1 template → Task 1. §7.2 ref template → Task 2. §7.3 newday → Task 3. §7.4 genprompt → Task 4. §7.5 review → Task 5. §7.6 digest → Task 6. §7.7 CLAUDE.md → Task 7. §7.8 roadmap → deliberately **not** a task; delivered as `/digest` instructions in Task 6 because HARD RULE 6 forbids editing `log/` outside `/digest`. §8 transition plan → not a code change; Day 21 keeps template v2, which Tasks 1–2 preserve by creating new files rather than editing the old ones. §9 YAGNI list → Task 7 step 3 records the no-parquet decision; `check()` is copied unchanged into the new template; no Day 1–20 migration appears anywhere.

**Placeholder scan.** No "TBD" / "handle edge cases" / "similar to Task N". Every code step carries full literal content. The `<...>` markers inside the two template files are intentional author-fills-these slots in a scaffold, not plan placeholders — the plan states their full text verbatim.

**Type consistency.** `build_output_table_dsl(table_a, table_b) -> DataFrame` and `build_output_table_sql(spark, table_a, table_b) -> DataFrame` are used identically in Task 1 (template), Task 3 step 3 (rename instruction), Task 4 step 2 (`ai_` prefix rule), and Task 8 (dry run renames both). `check(actual, expected_rows, label)` is unchanged from `template_v2.py`. The seven reference headings asserted in Task 2 step 2 are the same seven written in Task 2 step 1 and read by Tasks 5–6.
