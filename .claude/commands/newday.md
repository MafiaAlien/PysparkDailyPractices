---
description: Generate the next day's practice problem from the roadmap
argument-hint: "[optional topic override]"
---

Generate the next day's practice problem. Do **not** solve it.

## Steps

1. Read `log/problem_log_and_roadmap.md`. It is authoritative for
   completion status — do not trust memory or a prior session summary.
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
3. **STOP.** Propose: day number, **ETL layer**, **business domain**, the
   output table and its grain, the **numbered production constraints
   (P1/P2/P3)** you intend to declare, the pipeline stage count and resulting
   difficulty, and one line on which logged takeaway this day echoes.
   **Never report the failure mode** — that is the trap. Wait for
   confirmation before writing.
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
5. Design exactly **one** deliberate trap: an edge case that passes under
   naive clean-data assumptions but fails on the test data. Seed the trap
   rows into the test data. The explanation goes **only** into
   `refs/dayNN_<slug>_ref.md`.
6. Report back: the two file paths, the difficulty, and the single sentence
   "这题埋了一个陷阱" — with **no** hint about what it is.
7. Remind the user to run `/clear` before starting Stage 1, since the
   reference content is in context right now.

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
