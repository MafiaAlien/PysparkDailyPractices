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
