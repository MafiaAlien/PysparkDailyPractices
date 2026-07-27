# Problem Log & Topic Roadmap

## Completed problems
| Day | Topic | Difficulty | Problem | Trap / key edge case |
|-----|-------|-----------|---------|----------------------|
| 1 | Window functions (dense_rank, Top-N per group) | Medium | Top-2 salaries per department, ties included | row_number would drop a tied employee; ambiguous dept_id after expression-join |
| 2 | Date handling, gaps-and-islands | Medium-Hard | Longest consecutive login streak per user | same-day duplicate shifts row_number and silently splits a streak |
| 3 | Pivot, conditional aggregation | Medium | Quarterly revenue wide report per product | NULL cells must become 0; explicit pivot values list vs schema drift |
| 4 | Array columns (explode, collect, array functions) | Easy-Medium | Per-customer distinct items + total item count | empty array row vanishes with plain explode(); explode_outer or no-explode route |
| 5 | Join strategies, broadcast join | Medium | Region revenue via fact-dim enrichment | orphan store_id lost by inner join; explicit F.broadcast / hint; explain() observation task |
| 6 | Map columns (map access, map_keys, explode on maps) | Medium | Per-user property-bag report: total / mobile / distinct keys | NULL map and empty map: explode() drops rows; collect_set of key-ARRAYS dedupes at the wrong level (empty [] counted as a member, coincidentally-correct counts) |
| 7 | Struct columns (dot-path access, nested field extraction, argmax-per-group) | Medium | Per-user device profile: total / mobile / distinct_os / primary_os | NULL struct vs NULL os sub-field are indistinguishable once you project device.os; mobile_events needs conditional count not COUNT(device.os) (macOS coincidentally-correct on clean rows); per-os argmax must filter NULL os + tie-break os asc + LEFT join so all-NULL user still emits UNKNOWN |
| 8 | Deduplication: keep latest per key (window rn=1 vs struct-argmax vs COUNT OVER) | Medium-Hard | Current product catalog from multi-feed updates: surviving row + n_versions | exact-duplicate replay must still count in n_versions; ordered window default frame turns count over into a RUNNING count (count window must have NO orderBy); dropDuplicates after orderBy is not a contract; same-partitionBy windows share ONE shuffle vs separate groupBy branch costing a second Exchange + second scan |
| 9 | Semi / anti joins as filter idioms (left_semi / left_anti, EXISTS / NOT EXISTS, IN / NOT IN) | Easy-Medium | Clean outreach list: customers with >=1 order AND zero complaints | NOT IN + nullable subquery column returns ZERO rows (three-valued logic); inner-join-as-existence-test multiplies rows (customer with 3 orders -> 3 rows) needing dedup; anonymous complaint (cust_id NULL) is the trap seed |
| 10 | Complex aggregation: GROUPING SETS / ROLLUP / CUBE, multi-grain one-pass | Medium | Four-grain sales summary (region+category / region / category / grand_total) in one pass, rolled-up dims -> 'ALL' | genuinely-NULL source region indistinguishable from the subtotal NULL cube injects once projected; must stay a distinct group and not collapse into 'ALL'. Two robust routes: pre-fill sentinel (coalesce BEFORE cube, kills source NULL so only cube-NULL remains) vs post-hoc discriminate (keep NULL, branch GROUPING()==1 -> 'ALL' BEFORE IS NULL -> sentinel; order-sensitive). rollup would MISS the (category) grain — needs cube or grouping sets |
| 11 | Date/time deep dive: UTC->local-tz bucketing + date-dimension spine gap-fill | Medium-Hard | Dense daily report per store over a 7-day window: revenue + n_txn bucketed by store LOCAL date, zero-filled on no-sale days | conversion DIRECTION (from_utc_timestamp, NOT to_utc_timestamp) is the whole trap — silent day-shift on rows straddling local midnight, PASSES on non-straddling rows; window filter must be applied on the LOCAL date AFTER tz conversion, never on the raw UTC date (an out-of-window UTC sale can land in-window locally, and vice versa); to_date depends on session tz -> pin it to avoid double-shift; gap-fill via explode(sequence) date axis crossJoin stores -> LEFT join sparse agg -> COALESCE 0 (dimension-table cousin of Day 2 gaps-and-islands) |
| 12 | Complex aggregation advanced: GROUPING SETS precise-grain control + conditional (threshold) aggregation | Medium-Hard | Three-grain sales summary at EXACTLY (region,category) / (region) / (category) — NO grand total — in one pass; measures total_amount + big_orders (count of amount>=1000); rolled dims -> 'ALL', level from grouping metadata | threshold-count formulation trap: COUNT(when(cond,1).otherwise(0)) counts EVERY row (0 is non-NULL) = group size, right on some grains + wrong on others; correct = SUM(when(cond,1).otherwise(0)) or COUNT(when(cond,1)) no-ELSE. Precise grains {(r,c),(r),(c)}: cube adds unwanted () (filter gid!=3), rollup MISSES (category) — only GROUPING SETS (or cube-then-filter, or union) hits exactly these three. Rebuild rolled dims from gid/grouping() ONLY, never `IS NULL AND gid` (IS NULL is redundant + misleading; gid alone is the root判据). AI-review: `F.grouping_sets` DSL function DOES NOT EXIST (even Spark 4.2) — API hallucination, AttributeError at runtime; the SQL `GROUP BY GROUPING SETS` clause is the real thing |
| 13 | Date/time round 2: event sessionization (gap-threshold islands: lag -> boundary flag -> running SUM(flag) session key) | Medium-Hard | Sessionize a raw clickstream: per (user,session) report session_start / session_end / n_events, gap <= 30 min = same session | Day 2's (value-row_number) constant-diff DIES (gap is a range 0..threshold, not a fixed +1) -> need flag+running-sum idiom. Traps (all coincidentally-correct on clean data): gap direction must be current-prev (prev-current is always negative, `> threshold` never fires -> everything collapses to one session); compare in SECONDS not truncated minutes (timestamp_diff('MINUTE') truncates 30:40->30, wrongly merged, passes on integer-minute test rows); boundary STRICTLY > (gap==1800s stays SAME session, `>=` is off-by-one on the exact-boundary row); duplicate ts are RANGE peers (harmless here: gap 0 -> flag 0). Ordered-window default frame RANGE UNBOUNDED..CURRENT = Day 8 BUG reused as the TOOL. session_window built-in MISMATCHES this spec (< boundary + end = last+gap). |
| 14 | UDFs: python UDF vs native expressions (cost model, serialization, registration) | Medium | Query-string payload normalization: n_params / user_tier / latency_bucket; parsing via a self-written Python UDF, bucketing native | empty-value trap: `tier=` must yield `""` NOT 'UNKNOWN' — regex `tier=([^&]+)` requires >=1 char so it silently falls through to the UNKNOWN branch (`+`->`*` fixes it), and an unanchored key regex also mis-matches the SUFFIX `user_tier=gold` (needs `(?:^\|&)` or split+partition). `"".split("&")` returns `[""]` (len 1, NOT 0) so empty payload must be intercepted BEFORE split. UDF registration: `@F.udf` is DSL-only, SQL needs `spark.udf.register`; passing a UDF object AND a returnType raises CANNOT_SPECIFY_RETURN_TYPE_FOR_UDF. AI-SQL: hand-rolled 5-layer transform/filter/element_at nest instead of `str_to_map` -> `element_at(empty_array, 1)` THROWS under ANSI (INVALID_ARRAY_INDEX_IN_ELEMENT_AT); the whole COALESCE fallback assumed a NULL that ANSI never delivers |
| 15 | Window frames as the primary topic: trailing moving average + period-over-period growth on a SPARSE daily series (ROWS vs RANGE, frame-as-value-lookup) | Medium | Per-store trend report on a sparse daily sales table: revenue / ma7 (trailing 7 CALENDAR days, denominator always 7) / prev_day_revenue / dod_growth_pct | "previous ROW" vs "previous DAY" on a sparse series: rowsBetween(-6,0) and lag(1) reach past the gap and are only wrong on rows adjacent to a gap (7 of 10 test rows agree either way). rangeBetween(-1,-1) is a frame used as a value LOOKUP; its empty frame -> NULL makes that COALESCE load-bearing while the ma7 COALESCE is dead code (frame containing currentRow can never be empty). AVG over a sparse RANGE frame has a data-dependent denominator (rows present), so a fixed 7-calendar-day denominator must be sum/7. RUNTIME BLOCKER (corrects an earlier logged claim): in PySpark a DATE order column + finite integer RANGE bound raises DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE (Python int -> BIGINT literal; frame boundaries only up-cast) -- SQL's RANGE BETWEEN 6 PRECEDING on DATE is fine, DSL needs a numeric day-number (unix_date / datediff-to-epoch). Only FINITE numeric bounds trigger the check; all-symbolic bounds (unbounded / currentRow, incl. literal 0) are safe on any order type. AI-review: the day-number CTE is redundant in the SQL half only (clarity, NOT performance -- verified 1 Exchange either way); hand-rolled datediff-to-epoch where unix_date exists (Day 14 heuristic again); `WHEN prev = 0 THEN NULL` (spec-literal) vs `WHEN prev > 0` (user) diverge on negative revenue, indistinguishable on the test data |
| 16 | NULL semantics special (null-safe equality `<=>`, NULL in joins, NULL ordering in windows / NULLS FIRST\|LAST) | Medium | **【待补:题面】** | **【待补:陷阱】** — 已确认的机制要点:`<=>` 在 Catalyst 物理计划里 desugar 成**两个** equi-join key(不是一个特殊比较算子);窗口 orderBy 的 NULL 默认位置(ASC -> NULLS FIRST)会让 `row_number()` 静默把 NULL 行排到第 1 名 -> 需要在开窗**之前**预过滤,而不是靠 tie-break 补救 |

> **Day 16 状态:Stage 1 完成,Stage 2–5 因会话长度跳过 — AI review 仍然欠着。**
> 迁移到 Claude Code 之后可以补做:`/genprompt 16` -> incognito 生成 -> 贴进 Part 3 ->
> `/review 16` -> `/digest 16`。补完后删掉本行,并把上表 Day 16 的两个【待补】填实。

## Supplemental drills completed
- Pivot mini-drills x5 (script form, no class): explicit values list,
  schema drift without list, values-list-as-filter, multi-agg column
  naming, pivot -> unpivot round trip (stack / unpivot).

## Scheduled next (planned cadence, Medium / Medium-Hard)
- Day 17 — candidate (NEXT IN QUEUE): SCD2-style incremental merge. Highest
  interview frequency of anything still untouched, and DE-specific rather than
  generic-SQL. Builds directly on Day 8 (keep-latest-per-key via window rn=1 /
  struct-argmax) — SCD2 is that machinery plus effective-date intervals and a
  current-flag. Medium-Hard.
- Day 18 — candidate: pandas_udf as the PRIMARY topic. Day 14 covered the
  python-UDF cost model in theory but never ran an ArrowEvalPython plan, so
  the vectorized half is still entirely unexercised — the three-tier model
  (native ⊃ vectorizable pandas_udf ⊃ row-wise python UDF) has an untested
  middle. Medium. Steps difficulty back down after Day 17.
- Later candidates: skew handling (salting, AQE skew join); higher-order array
  functions as the primary topic; ANSI / try_* family as a first-class topic;
  date/time round 4 — DST-crossing tz math (a full Day-15-shaped script exists
  and was RETIRED before Stage 1: judged low interview probability, kept as an
  optional 15-minute drill) or session_window built-in as the primary topic.

Cadence principle: alternate the two topics, step difficulty upward, each
problem echoes >=1 logged takeaway.

## Backlog (rotate, alternate difficulty, avoid recent repeats)
- Complex aggregation: multiple grains in one pass, grouping sets /
  rollup / cube   <- Day 10 + Day 12 DONE (base + advanced/precise-control)
- UDFs: python UDF vs pandas_udf, when to avoid, cost model   <- Day 14 DONE
  for the python-UDF half (returnType, staticmethod + pickle scope,
  spark.udf.register shapes, BatchEvalPython as optimizer barrier);
  pandas_udf / ArrowEvalPython still OPEN (Day 18 candidate)
- Higher-order array functions as the PRIMARY topic (transform / filter /
  aggregate / exists / forall / zip_with / transform_values)   <- surfaced
  Day 14 via AI review, never drilled directly
- ANSI mode & the try_* family (try_element_at / try_cast / try_divide):
  failure-vs-NULL semantics as a first-class topic   <- surfaced Day 14
- Unpivot / melt as the primary topic (only touched in drills)
- Date/time deep dive: timezones, timestamps, truncation, ranges,
  calendar join against a date dimension   <- Day 11 DONE (UTC->local
  bucketing + date-dim spine gap-fill); Day 13 DONE (gap-threshold
  sessionization via lag+running-sum); DST-crossing variant + session_window
  built-in still open (Day 15 candidate)
- Null semantics special: null-safe equality (<=>), null in joins,
  null ordering in windows   <- Day 16 DONE (Stage 1 only; AI review outstanding)
- Window frames as the PRIMARY topic (ROWS vs RANGE, rangeBetween units,
  frame-as-lookup, frame-sensitive vs positional functions)   <- Day 15 DONE
  (theory had been logged since Day 13 but never drilled)
- Incremental patterns: SCD2-style merge logic in PySpark   <- Day 17 NEXT
- Skew handling: salting, AQE skew join
- String processing: regexp_extract, split, sentence-level parsing
- Semi/anti joins: left_semi, left_anti as filter idioms   <- DONE (Day 9)

## Difficulty cadence
Alternate roughly Easy/Medium -> Medium -> Medium-Hard; insert an Easy
day after two consecutive hard days. Interview frequency weighs topic
priority (windows, joins, dedup, dates are highest frequency).
