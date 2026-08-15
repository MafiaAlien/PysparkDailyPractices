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
   - **Precedence — the roadmap still schedules, the axes only shape.** If
     "Scheduled next" in `log/problem_log_and_roadmap.md` names a candidate,
     or it points at an open backlog item, honor it. Do not replace it with a
     free axis pick: express that topic **as a cell on the three axes** (which
     ETL layer is this job, which domain skins it, which failure mode hides
     the trap) and build the pipeline around it. Only when "Scheduled next" is
     empty and no backlog item is open do you pick all three axes freely.
     Per the transition plan, **Day 22 is MERGE INTO / upsert at the L4
     incremental layer** — the last high-value backlog item, and a topic the
     user already knows from Day 17, which is what makes it a safe first test
     of the ETL template.
   - Pick one cell from each of the three axes, and check the scheduling
     matrix in `log/problem_log_and_roadmap.md` so no (layer, domain,
     failure mode) combination repeats (the matrix is created by `/digest` on
     first use — if the section is absent, no combination has been used yet):
     - **ETL layer** (the main axis — what shape of job this is):
       L1 landing cleanup / L2 detail modeling / L3 serving / L4 incremental
     - **Business domain** (the skin — rotate, avoid recent repeats):
       e-commerce orders / ad delivery / payment reconciliation /
       clickstream / logistics / content consumption / subscription billing
     - **Failure mode** (the trap source — NEVER stated in the day file):
       replay / late data / dimension drift / metric drift / nullable key /
       empty collection / boundary closure / skew / decomposability
   - **The declared `P#` constraints must never name or restate the selected
     failure mode.** The constraint menu and the failure-mode axis overlap
     near-verbatim in three places — `replay` vs "upstream replay dedup",
     `late data` vs "late-arriving data attributed to the event date",
     `metric drift` vs "one metric computed identically everywhere". Picking
     the matching pair publishes the trap axis in the day file, and
     `/genprompt` then forwards it to the incognito AI as a public
     requirement, which zeroes out the Stage-3 blind review. Constraints are
     the requirements the trap hides **behind**, never a description of it.
     If the natural constraint for this job would restate the trap axis, pick
     a different constraint.
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
   **Never report the failure mode** — that is the trap. For Day 21 (the last
   v2-template day) drop the ETL-only items: report day number, topic,
   expected output and its grain, difficulty, and the echoed takeaway. Wait
   for confirmation before writing.
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
   become `build_<output_table>_dsl` / `build_<output_table>_sql`. The
   template's two parameters are a **starting point, not a limit** — add or
   delete parameters so the function signatures carry exactly the input
   tables this day declares, and keep all of the following in sync with
   that same count: the `createOrReplaceTempView` calls in the `_sql`
   function (one per input table), the `spark.createDataFrame` blocks in the
   harness (one per input table), the two `check(...)` call sites (they stay
   two — DSL and SQL — but each must pass every input table as an argument),
   and the two commented Stage-4 lines (same rule). Leave the
   `# >>> PASTE BEGIN` / `# >>> PASTE END` markers empty.
5. Design exactly **one** deliberate trap: an edge case that passes under
   naive clean-data assumptions but fails on the test data. Seed the trap
   rows into the test data. The explanation goes **only** into
   `refs/dayNN_<slug>_ref.md`.
6. Report back: the two file paths, the difficulty, and the single sentence
   "这题埋了一个陷阱" — with **no** hint about what it is.
7. Remind the user to run `/clear` before starting Stage 1, since the
   reference content is in context right now.

## Constraints on the generated day file (Day 22 onward)

**For Day 21 this whole section is ignored** — the pre-Day-22 constraints
named in step 4 apply instead (single `df` parameter, `class Solution` with
`solve_dsl` / `solve_sql` methods, the four `template_v2.py` docstring blocks
PROBLEM / INPUT SCHEMA / EXPECTED OUTPUT / EXAMPLE, `<= 15` rows, no
production constraints).

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
  **No `P#` may name or restate the selected failure mode.** Three menu items
  above shadow a failure-mode axis cell almost word for word (replay dedup /
  `replay`, event-date attribution / `late data`, one-metric-everywhere /
  `metric drift`). Declaring the shadow of today's trap axis publishes the
  trap — `/genprompt` copies `P#` verbatim into the incognito prompt. When
  the natural constraint collides with the trap axis, pick a different one.
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
- The ASCII boxes live inside the existing `INPUT TABLES` and `OUTPUT
  CONTRACT` blocks — do **not** add a fifth `EXAMPLE` block; that block name
  belongs to `template_v2.py`. Render every table as a `df.show()`-style
  ASCII box with a header row — one box per input table under `INPUT TABLES`,
  one for the expected output under `OUTPUT CONTRACT`. Never
  bare tuples or prose rows: without column headers the reader cannot tell
  which value is which column. Per-row annotations go under the box as notes,
  not inside the cells.
- **The boxes are the full test data, not a sample.** Every input box must
  reproduce the harness `spark.createDataFrame` rows exactly — same rows,
  same order of columns, same values — and the expected box must reproduce
  the `expected` list exactly. `/genprompt` sends the boxes and nothing else
  to the incognito AI, so any drift means the AI solves a different dataset
  and the Stage-4 AI checks fail for a reason that has nothing to do with the
  solution, destroying the verification signal. After writing the harness,
  re-read the boxes against it row by row.
- Test data: **each input table <= 8 rows, all inputs together <= 25 rows**,
  including the trap rows. Small enough to reason about by hand.
- `expected` must be hand-derived row by row, not produced by running a
  solution. If any expected row is uncertain, say so explicitly.
- Script content 100% English.
- If the scenario touches dates, pin `spark.sql.session.timeZone` in the
  harness (the template already does).

## Constraints on the reference file (Day 22 onward)

**For Day 21 this section is ignored too** — that day's reference comes from
`templates/template_ref.md`, which has no failure-mode line, no
production-constraint audit table and no pipeline-stage list. Fill the
sections that template actually has.

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
