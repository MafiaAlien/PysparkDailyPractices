---
description: Generate the next day's practice problem from the roadmap
argument-hint: "[optional topic override]"
---

Generate the next day's practice problem. Do **not** solve it.

## Steps

1. Read `log/problem_log_and_roadmap.md`. It is authoritative for
   completion status — do not trust memory or a prior session summary.
2. Determine the next day number and pick the topic:
   - honor the "Scheduled next" section if it names a candidate
   - respect topic rotation (no recent repeat) and the difficulty cadence
     (Easy/Medium → Medium → Medium-Hard; insert an Easy day after two
     consecutive hard days)
   - weight by interview frequency: windows, joins, dedup, dates are highest
   - if `$ARGUMENTS` is non-empty, use it as the topic instead
   - if a scheduled topic looks low-signal for interviews, say so and propose
     a swap rather than generating it silently
3. **STOP.** Propose: day number, topic, difficulty, and one line on which
   logged takeaway this day echoes. Wait for confirmation before writing.
4. On confirmation, write two files:
   - `days/dayNN_<slug>.py` — from `templates/template_v2.py`
   - `refs/dayNN_<slug>_ref.md` — from `templates/template_ref.md`
     (NOT in `days/` — the sealed answers live in the sibling `refs/` dir)
5. Design exactly **one** deliberate trap: an edge case that passes under
   naive clean-data assumptions but fails on the test data. Seed the trap
   rows into the test data. The explanation goes **only** into
   `refs/dayNN_<slug>_ref.md`.
6. Report back: the two file paths, the difficulty, and the single sentence
   "这题埋了一个陷阱" — with **no** hint about what it is.
7. Remind the user to run `/clear` before starting Stage 1, since the
   reference content is in context right now.

## Constraints on the generated day file

- Part 1 methods: `pass` + `# TODO: implement` + a one-line hint in the
  docstring. Nothing else.
- `solve_sql` keeps the `createOrReplaceTempView` scaffolding and an empty
  SQL string placeholder.
- Part 3 stays empty — checklist comments only, never a pre-filled solution.
- Part 5 in the day file contains only the unanswered "Review takeaways"
  prompts. Concept takeaways go in `refs/dayNN_<slug>_ref.md`.
- The EXAMPLE section renders every table as a `df.show()`-style ASCII box
  with a header row — one box per input table, one for the expected output.
  Never bare tuples or prose rows: without column headers the reader cannot
  tell which value is which column. Per-row annotations (e.g. why a row is
  the interesting one) go under the box as notes, not inside the cells.
- Test data: ≤ 15 rows, including the trap rows. Small enough to reason about
  by hand.
- `expected` must be hand-derived row by row, not produced by running a
  solution. If any expected row is uncertain, say so explicitly.
- Script content 100% English.
- If the topic touches dates, pin `spark.sql.session.timeZone` in the harness.

## Constraints on the reference file

- Show more than one route when they differ meaningfully (e.g. explode vs
  array-function, window vs struct-argmax).
- Name the trap explicitly, and name **which naive solution passes anyway**
  on clean rows — the "coincidentally correct" framing.
- Concept takeaways: 3–8 bullets, dense, tied to mechanism not to syntax.
