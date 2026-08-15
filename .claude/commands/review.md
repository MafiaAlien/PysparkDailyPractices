---
description: Stage 5 debrief — verify the AI solution and grade the user's blind review
argument-hint: "[day number]"
---

## Precondition

The user must have pasted the AI solution into Part 3, filled REVIEW_NOTES,
and **committed a VERDICT**. If VERDICT is blank, stop and say so — reading
the analysis before committing destroys the exercise. Do not proceed.

## Steps, in this order

1. Read `days/day$ARGUMENTS*.py` in full (Parts 1–3). Do **not** open
   `refs/day$ARGUMENTS*_ref.md` yet.

2. Review the AI code **by reading only**, before running anything. Produce
   your own findings list sorted by severity:
   **runtime break > wrong results on dirty data > production robustness
   > performance > portability > style**

   `production robustness` covers: re-run not idempotent, late data dropped
   or misattributed, the same metric computed two different ways in two
   branches, missing data-quality assertions. It sits below wrong-results
   because it produces correct output on the first run and only breaks on
   the second — later to surface, but still a correctness class, never
   demotable to performance.
   For each finding, state whether it was catchable by reading alone.

3. Run the harness with the Stage-4 lines uncommented. Report actual
   PASS/FAIL for all four: user-DSL, user-SQL, AI-DSL, AI-SQL.

4. **Now** read `refs/day$ARGUMENTS*_ref.md`.

5. Debrief in this order:

   a. **Trap.** Did the AI hit it? Did the user's review catch it by reading,
      or only the run exposed it? Name the gap precisely.

   a2. **Production constraints, one at a time.** For each `P#` declared in
       the day file, state whether the user's solution satisfies it, whether
       the AI's does, and — using the audit table in the reference file —
       what the observable symptom would be if it does not. A constraint
       that no test row can distinguish gets said out loud, same as any
       other undistinguishable divergence.

   b. **Three-way comparison** — user vs AI vs reference. For each
      difference, classify: real semantic divergence, cosmetic, or
      **undistinguishable by the test data**. That last category is the
      valuable one — call it out explicitly (the `= 0` vs `> 0` pattern:
      both green, they diverge on negative inputs the test data lacks).
      For an ETL scenario day, align the three solutions **stage by stage**
      using the pipeline-stage list in the reference file. A long job
      usually agrees everywhere but one stage; naming that stage is most of
      the diagnostic value, and a whole-file diff hides it.

   c. **Grade the user's REVIEW_NOTES item by item.** Confirm, correct, or
      reclassify. Say plainly when a finding is wrong, when it is redundant
      defensive code rather than a bug, and when it was filed under the wrong
      severity (an extra `withColumn` is style, not performance).

   d. **Any performance claim on either side** gets verified with a real
      `.explain()` and an Exchange count before you agree or disagree. Paste
      the plan fragment.

   e. Propose 1–2 candidate entries for the review-heuristics section of
      `log/04` — phrased as a reading-time trigger ("when you see X, ask Y"),
      not as a restatement of today's answer.

6. **Write nothing.** `/digest` does all file edits.
