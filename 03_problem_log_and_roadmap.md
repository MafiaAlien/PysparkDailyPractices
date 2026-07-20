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

## Supplemental drills completed
- Pivot mini-drills x5 (script form, no class): explicit values list,
  schema drift without list, values-list-as-filter, multi-agg column
  naming, pivot -> unpivot round trip (stack / unpivot).

## Scheduled next (planned cadence, Medium / Medium-Hard)
- Day 12 — Complex aggregation, advanced (NEXT IN QUEUE): multi-grain +
  conditional aggregation mixed, OR cube's combinatorial blow-up vs
  GROUPING SETS' precise control trade-off (echoes Day 3 pivot's
  "explicit values list" philosophy). Medium-Hard. Alternates off the
  Day 11 date/time topic.
- Day 13 — Date/time round 2 candidate: session-window / event-sessionization
  (session_window or lag-based gap threshold), OR DST-crossing tz math
  (Day 11 used a no-DST window on purpose — round 2 puts the clock change
  INSIDE the window so a local day has 23/25 hours). Medium-Hard.

Cadence principle: alternate the two topics, step difficulty upward, each
problem echoes >=1 logged takeaway.

## Backlog (rotate, alternate difficulty, avoid recent repeats)
- Complex aggregation: multiple grains in one pass, grouping sets /
  rollup / cube   <- Day 10 DONE; advanced variant still scheduled Day 12
- UDFs: python UDF vs pandas_udf, when to avoid, cost model
- Unpivot / melt as the primary topic (only touched in drills)
- Date/time deep dive: timezones, timestamps, truncation, ranges,
  calendar join against a date dimension   <- Day 11 DONE (UTC->local
  bucketing + date-dim spine gap-fill); DST-crossing / sessionization
  variant still open (Day 13 candidate)
- Null semantics special: null-safe equality (<=>), null in joins,
  null ordering in windows
- Incremental patterns: SCD2-style merge logic in PySpark
- Skew handling: salting, AQE skew join
- String processing: regexp_extract, split, sentence-level parsing
- Semi/anti joins: left_semi, left_anti as filter idioms   <- DONE (Day 9)

## Difficulty cadence
Alternate roughly Easy/Medium -> Medium -> Medium-Hard; insert an Easy
day after two consecutive hard days. Interview frequency weighs topic
priority (windows, joins, dedup, dates are highest frequency).
