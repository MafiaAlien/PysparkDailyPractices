# Day N — <Topic> — Reference

> SEALED until Stage 5. Do not open during SOLVE / GENERATE / REVIEW / VERIFY.

## The trap

**What it is:** <one sentence>

**Why a naive solution passes anyway:** <which rows it gets right, and under
what clean-data assumption — the "coincidentally correct" framing>

**Which row exposes it:** <point at the specific test row>

**Reading-time tell:** <what a reviewer could have noticed without running>

---

## Part 4 — Reference answers

### Route A — <name>

```python
def ref_solve_dsl_a(df):
    ...
```

```sql
-- ref_solve_sql_a
```

### Route B — <name, only if meaningfully different>

<When two routes differ, state the actual tradeoff: Exchange count, whether
the second stage is a partial-aggregable aggregate or a full sort, whether
one route changes the grain.>

---

## Part 5 — Concept takeaways

- <3–8 dense bullets, mechanism-level, not syntax-level>
- <name any API contract that differs between DSL and SQL for this topic>
- <name any behavior that changes under ANSI mode>

## Physical plan notes

<Exchange / Expand / Window / HashAggregate counts for each route, from an
actually executed .explain(). If not yet measured, write NOT MEASURED — never
assert a count from memory.>

## Echoes

<Which existing log/04 entries this day reinforces, refines, or contradicts.>
