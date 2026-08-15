---
description: Update log/03 and log/04 with today's results
argument-hint: "[day number]"
---

Run after `/review`. This is the **only** command permitted to write into
`log/`.

## Steps

1. Read `log/problem_log_and_roadmap.md`, `log/key_takeaways.md`, and
   `days/day$ARGUMENTS*` and `refs/day$ARGUMENTS*_ref.md`.

2. **Propose the edits in chat first, as a diff-style preview. Write nothing
   yet.**

   **`log/03`:**
   - **Day 1–21 rows are frozen.** Do not back-fill the new columns into the
     existing "Completed problems" table — those rows' `Topic` column is not
     the same thing as `层级 / 业务域`, and migrating them loses information.
   - For Day 22 onward, append to a **separate table** titled
     `## ETL scenario days`, created on first use, with columns:
     `Day | 层级 | 业务域 | Difficulty | Output table | 生产约束 | Trap / key edge case`.
     The `生产约束` column lists the `P#` lines verbatim. The trap column is
     dense and specific — match the style of the Day 1–21 rows in the
     `Completed problems` table, which name the mechanism and the conditions
     under which the wrong answer passes anyway.
   - Update the **scheduling matrix**, a section titled
     `## 调度矩阵（已用组合）` created on first use, with columns
     `Day | ETL layer | Domain | Failure mode`. `/newday` reads this to avoid
     repeating a combination. The failure mode is recorded here and in the
     reference file only — never in the day file.
   - Update "Scheduled next"
   - Update any backlog line this day closes out or partially closes

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
