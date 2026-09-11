# PySpark Daily Practice

A single-player, adversarial training loop for PySpark and Spark SQL, built
around one rule: **you must commit to a verdict before you are allowed to run
the code.**

Every day is one problem, solved twice — once in the DataFrame API, once in
Spark SQL — then reviewed against an independently generated AI solution that
was produced somewhere this repository cannot reach. Each problem contains a
deliberately planted trap: an edge case that passes on clean data and fails on
realistic data. The reference answer exists from day one, sealed in `refs/`,
and stays unread until the day is over.

It is prep for North American data-engineering interviews, but the shape of it
is really about building production instinct: the habit of finding the failure
before the failure finds you.

---

## Status

<!-- STATUS:BEGIN — updated by /digest at the close of Stage 5 -->

**Last completed: Day 24** — L2 detail modeling · logistics (cold-chain
warehouse telemetry) · **Medium-Hard**, 4 stages · output `fct_device_hour`

23 days done (Day 1–24; Day 21 was never generated — the ETL-scenario switch
landed first). Mode since Day 22: one full production pipeline per day.

| Day | Layer | Domain | Difficulty | Output | Failure mode |
|-----|-------|--------|-----------|--------|--------------|
| 24 | L2 detail modeling | logistics — cold-chain telemetry | Medium-Hard (4 stages) | `fct_device_hour` | replay |
| 23 | L3 serving | e-commerce marketplace orders | Medium-Hard (4 stages) | `agg_region_daily` | fan-out double counting |
| 22 | L4 incremental | subscription billing | Medium-Hard (5 stages) | `dim_subscription` | late data |

Days 1–21 were single-technique days — windows, gaps-and-islands, pivots,
map/struct columns, semi-anti joins, GROUPING SETS, timezone bucketing,
sessionization, UDF cost, window frames, NULL semantics, SCD2, skew salting,
higher-order array functions, ANSI mode. The full table, with difficulty and
the trap for each, is in [`log/problem_log_and_roadmap.md`](log/problem_log_and_roadmap.md).

> The status above intentionally lags: a day appears here only after Stage 5
> closes. Publishing an open day's topic would put it on the web ahead of the
> Stage-2 incognito conversation, which is exactly what that stage exists to
> avoid.

<!-- STATUS:END -->

---

## Why it is built this way

The obvious way to practice with an AI assistant is to ask it for the answer.
That produces the feeling of learning and very little of it. Three design
decisions push against that:

**The reference answer is sealed, not absent.** `refs/dayNN_*_ref.md` holds the
full solution, the trap explanation, and measured physical-plan notes — written
at problem-generation time, before any attempt. It opens only in Stage 5. A
hook (`.claude/hooks/protect_sealed_refs.sh`) and a permission rule in
`.claude/settings.json` enforce the seal against the assistant, so the seal is
mechanical rather than a promise.

**The second solution is generated off-repo, on purpose.** Reference answers
live here, so anything generated *here* is contaminated by them. Stage 2 runs in
a separate incognito conversation, seeded by a prompt containing only the
problem statement. That gives you a genuinely independent implementation to
review — one that is sometimes right, sometimes subtly wrong, and never
pre-agreed with your own.

**The verdict is committed before execution.** You read the AI's code, write
findings into a `REVIEW_NOTES` block, and record `PASS` / `FAIL` — all before
running anything. Then you run it. The gap between what you predicted and what
the harness prints is the actual signal, and it is the only part of the loop
that cannot be faked.

The assistant's role throughout is coach and reviewer, never solver. That
constraint is written into `CLAUDE.md` as hard rules.

---

## The five-stage loop

| Stage | Who | What happens |
|-------|-----|--------------|
| 1 · SOLVE | you | Implement `solve_dsl` and `solve_sql` in Part 1. Run until both pass. |
| 2 · GENERATE | you, elsewhere | `/genprompt NN` emits a prompt; you paste it into a **separate incognito conversation** and bring the answer back verbatim. |
| 3 · REVIEW | you | Paste the AI solution into Part 3. Review it **by reading only**. Fill `REVIEW_NOTES`, commit a `VERDICT`. |
| 4 · VERIFY | you | Uncomment the Stage-4 lines and run. Now you find out. |
| 5 · DIGEST | assistant | `/review NN` blind-reviews, runs, compares three implementations, and grades *your* review. `/digest NN` writes the logs. |

Stage 2 stays outside this repository permanently. That is the whole point of
it.

---

## Problem mode

**Days 1–21** each introduced one new operator or semantic edge: window frames,
NULL semantics, SCD2 merges, skew salting, higher-order array functions.

**Day 22 onward** is one complete production ETL pipeline per day, composed
entirely of techniques already practiced. The point is no longer learning a
function — it is fluency and production instinct across a long job. Two failure
modes forced the switch, both recorded in the roadmap: the remaining backlog had
drifted toward low-value APIs, and two consecutive days lost their entire trap
dimension because Stage 1 degenerated into API teaching.

A Day-22+ problem has 3–4 input tables, one output table, 3–6 named pipeline
stages, one hidden trap, and 2–3 production constraints stated openly in the
problem as `P1`/`P2`/`P3` — things like *"the job must assert its own output
before returning"* or *"a retransmission is a new transmission."*

Findings are always sorted by severity:

> runtime break › wrong results on dirty data › production robustness ›
> performance › portability › style

Where `production robustness` means: re-runs are not idempotent, late data is
dropped or misattributed, one metric is computed two ways, quality assertions
are missing. It sits below wrong-results because it is correct on the first run
and only breaks on the second.

**Performance claims require an executed `.explain()`.** Paste the plan
fragment, count Exchange / Window / HashAggregate nodes, *then* conclude — never
from memory. Several log entries exist specifically to correct that failure
mode; one day's measurement overturned an earlier day's confident claim about
RANGE frames.

---

## Layout

```
CLAUDE.md                      hard rules, workflow, Q&A style, severity ladder
CHEATSHEET.md                  the daily loop, condensed (Chinese)
SETUP.md                       environment bring-up (Chinese)

.claude/commands/              /newday /genprompt /review /digest
.claude/hooks/                 seal enforcement for refs/

days/dayNN_<slug>.py           the working file — you edit Part 1 and Part 3
refs/dayNN_<slug>_ref.md       SEALED: reference answers, trap, plan notes
templates/                     day-file and reference skeletons

log/problem_log_and_roadmap.md authoritative completion status + backlog
log/key_takeaways.md           accumulated findings, ~1600 lines
docs/superpowers/              design specs (not part of the daily loop)
```

A day file is one self-contained script with everything inline — schemas, test
data, a `check()` harness that compares order-insensitively:

- **Part 1** — your two implementations. *(off-limits to the assistant)*
- **Part 3** — the AI solution, pasted verbatim and unmodified, plus
  `REVIEW_NOTES` and `VERDICT`
- **Part 2** — the harness: fixtures, expected output, `check()`. Not edited,
  and placed *after* Part 3 so the Stage-4 lines you must uncomment sit below
  the verdict you already committed.
- **Part 5** — post-mortem prompts: what did it get wrong, what did it get
  *suspiciously right*, did you catch it by reading or only by running

---

## Running it

Requires JDK 17+ and Python 3.12.

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python days/day24_device_hour.py
```

Spark 4.1.1, local `master local[2]`, small `shuffle.partitions`. Note that
**ANSI mode is on by default in Spark 4.x** — `cast`, division, and array
indexing throw rather than returning NULL, which changes what every
`COALESCE(risky_expr, fallback)` pattern actually means. Any harness touching
dates pins `spark.sql.session.timeZone`.

---

## Notes for readers

**`refs/` contains spoilers.** If you want to try a day yourself, read the
docstring at the top of `days/dayNN_*.py` and stop there. The reference file
names the trap in its second section.

**The logs are in Chinese.** `log/key_takeaways.md` and
`log/problem_log_and_roadmap.md` are working notes written in Chinese prose with
English API names, error classes, and SQL keywords. Everything inside the day
files — code, comments, docstrings, identifiers — is English.

**This is not a library or a tutorial.** There is nothing to import. It is a
training rig with its own rules, and it is public mostly because the rules
themselves — sealed references, off-repo generation, verdict-before-execution —
seem worth stealing for other subjects.
