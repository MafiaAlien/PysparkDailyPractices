---
description: Update log/03 and log/04 with today's results
argument-hint: "[day number]"
---

Run after `/review`. This is the **only** command permitted to write into
`log/`.

## Steps

1. Read `log/03_problem_log_and_roadmap.md`, `log/04_key_takeaways.md`, and
   `days/day$ARGUMENTS*` including the `_ref.md`.

2. **Propose the edits in chat first, as a diff-style preview. Write nothing
   yet.**

   **`log/03`:**
   - one new row in "Completed problems": `Day | Topic | Difficulty |
     Problem | Trap / key edge case`. The trap column is dense and specific —
     match the style of the existing rows, which name the mechanism and the
     conditions under which the wrong answer passes anyway.
   - update "Scheduled next"
   - update any backlog line this day closes out or partially closes

   **`log/04`:**
   - only content that is genuinely **new**, or that **corrects** an existing
     entry. If a takeaway already exists, say so and skip it — do not restate.
   - organize by **concept, not by day**; append into the right existing
     section when one fits, create a new section only when nothing does.
   - Chinese prose; English for API names, function names, error class names,
     config keys.

3. **Corrections are additive, not destructive.** If today contradicts an
   existing `log/04` entry, mark the old claim as corrected and cite the
   evidence that overturned it. Do not silently delete it. Precedent: the
   Day 15 RANGE-frame entry that overturned the Day 13 record.

4. Wait for approval, then apply with targeted edits — not a full-file
   rewrite. `log/04` is long; a rewrite risks silent loss.

5. Finish with a one-line proposal for the next day's topic, consistent with
   the difficulty cadence, and reflect it in "Scheduled next".
