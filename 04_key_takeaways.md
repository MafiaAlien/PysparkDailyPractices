# Accumulated Key Takeaways (Days 1-11)

## Window functions
- dense_rank vs rank vs row_number: dense (1,2,2,3) / rank (1,2,2,4) /
  row_number (unique, ties broken arbitrarily). Top-N with ties needs
  dense_rank; gaps-and-islands needs row_number's strict +1.
- dense_rank()/rank()/row_number() take NO arguments — ordering comes
  entirely from the window's orderBy. F.dense_rank(F.col(...)) is a
  TypeError.
- A window without partitionBy forces all data into one partition —
  performance killer on large tables; a standing review-checklist item.

## Gaps-and-islands
- (value - row_number) is constant within an island; for dates use
  DATE_SUB(d, rn) as the anchor/island key.
- Dedupe BEFORE numbering: a duplicate shifts rn and silently splits
  a streak — passes on clean data, fails on dirty data.

## Deduplication (4 equivalent-ish forms)
- SELECT DISTINCT A,B ≡ GROUP BY A,B (no aggregates) ≡
  select(A,B).distinct() — identical physical plan (Aggregate);
  one shuffle.
- dropDuplicates(["A","B"]) keeps ALL columns; which row survives per
  key is NONDETERMINISTIC.
- Window row_number()=1 keeps all columns AND deterministically picks
  the row (ORDER BY tie-breaker) — required for "keep latest per key".
- Choose: keys only -> DISTINCT/GROUP BY; need other columns, any row
  -> dropDuplicates; need other columns, specific row -> window.

## Keep-latest-per-key (Day 8, dedup applied)
- orderBy(...).dropDuplicates(keys) is NOT a contract: may look right
  locally, breaks on a real cluster (survivor still nondeterministic
  after shuffle). "Keep latest" REQUIRES window rn=1 or struct-argmax.
- struct-argmax works here with NO negation: both order keys are DESC
  ("take large") -> max(struct(updated_at, source_seq, payload...)).
  Payload fields ride along AFTER the sort keys — they are only
  compared when all keys tie, which for exact-duplicate replays picks
  between identical rows (harmless).
- Per-key TOTAL alongside the surviving row: COUNT(*) OVER
  (PARTITION BY key) with NO orderBy piggybacks on the SAME window
  shuffle as row_number — one Exchange, one scan. The separate
  groupBy-branch + join route costs a SECOND Exchange and a SECOND
  scan of the source (join itself reuses the matching partitioning
  via ENSURE_REQUIREMENTS, so the join adds no Exchange — the cost is
  the extra branch, not the join).
- Trade-off: window count has NO map-side partial aggregation; if you
  need ONLY the count (no ranking), groupBy is better — partial agg
  shrinks shuffle traffic. The piggyback only wins when a ranking
  window already forces full rows through the shuffle anyway.
- Ordered-window default frame trap: partitionBy-only window = whole
  partition frame (true total); ADDING orderBy silently switches the
  default frame to RANGE UNBOUNDED PRECEDING..CURRENT ROW, turning
  count/sum OVER into a RUNNING aggregate — rn=1 row would read 1,
  not the total. The count window must carry NO orderBy.
- Exact-duplicate replays still count toward n_versions (count rows,
  not distinct versions) — read the metric definition before reaching
  for countDistinct.

## Window shuffle mechanics (spec, not function)
- Shuffle belongs to the window SPEC, not the function: row_number /
  rank / lag / count-over all cost the same — one Exchange per
  DISTINCT partitionBy expression in the query. Same partitionBy ->
  windows share one Exchange; different partitionBy -> one each.
- orderBy inside a window adds a SORT after the Exchange, not another
  shuffle. No partitionBy = all rows to ONE partition (still one
  Exchange, but the single-partition killer).
- Review a query's window cost by counting DISTINCT partitionBy
  expressions, then checking for bare windows.

## Pivot
- pivot(col) without a values list triggers an EXTRA job (distinct +
  collect to driver) and makes the output schema data-dependent
  (schema drift). Always pass the list when the domain is known.
- The values list is also a FILTER: values not listed are silently
  dropped.
- Pivot cells with no data are NULL, not 0 — na.fill / COALESCE
  explicitly.
- Single aggregation: alias does NOT affect column names (pure values).
  Multiple aggregations: columns become <value>_<agg_alias>; count =
  |values| x |aggs|.
- In pivot's aggregation context F.count("*") fails to resolve
  (INVALID_USAGE_OF_STAR_OR_REGEX). Use F.count(F.lit(1)) in DSL
  everywhere; COUNT(*) in SQL is fine everywhere.
- Conditional aggregation SUM(CASE WHEN...) is the portable equivalent;
  nearly identical plan once the pivot list is explicit.

## Arrays
- explode() DROPS rows with empty/NULL arrays; explode_outer() keeps
  them (item = NULL). The #1 array-column bug.
- No-explode route: collect_list(arrays) -> flatten -> array_distinct /
  array_sort / size does group-level array math without row explosion.
- collect_set dedupes at map-side partial aggregation (smaller shuffle);
  collect_list preserves duplicates. Element order of collect_set is
  nondeterministic — array_sort before comparing.
- sort_array vs array_sort: both ascending by default; differ in NULL
  placement (first vs last) and extras (bool desc flag vs lambda
  comparator, 3.0+).
- SIZE(NULL) = -1 (legacy quirk) — COALESCE-guard nullable arrays.

## NULL semantics of aggregates
- ALL aggregate functions (count/sum/avg/collect_list/collect_set...)
  ignore NULL inputs. This one rule kills two review categories:
  missing-NULL-handling bugs AND redundant defensive code
  (filter(isNotNull) before collect_set, coalesce after na.fill).
- COUNT(column) counts non-NULL only; returns 0 (never NULL) for
  all-NULL groups. Footgun in one context (silent undercount), the
  right tool in another (zeroing empty-array customers after
  explode_outer).
- CASE WHEN ... ELSE 0 vs no-ELSE + COALESCE: both valid NULL
  strategies but must be paired consistently.

## Joins
- on="key" (string) merges the join key into ONE column; for left
  joins the merged key takes the LEFT side's value (orphan keys
  survive). Expression joins (a.k == b.k) keep BOTH columns ->
  AMBIGUOUS_REFERENCE risk; disambiguate via df["col"], alias
  qualification, or drop one side immediately.
- Self-joins REQUIRE aliases; even F.col is ambiguous there.
- LEFT join + COALESCE(dim_col, 'UNKNOWN') = standard pattern so
  unmatched fact rows survive enrichment; inner join silently loses
  revenue.
- Broadcast: F.broadcast(dim) / /*+ BROADCAST(alias) */ wraps the
  SMALL side. Three-tier mechanism: explicit hint (unconditional) >
  static autoBroadcastJoinThreshold (needs size STATISTICS — missing
  stats e.g. Scan ExistingRDD are treated as infinitely large, so no
  auto-broadcast) > AQE runtime conversion (real sizes at shuffle
  boundaries, but initial shuffles already paid). Cost order:
  explicit < AQE conversion < full SortMergeJoin.
- Single-block GROUP BY with transformed keys: repeat the EXACT
  expression in GROUP BY, or lift the transform into a CTE/subquery
  (cleanest — one copy, no drift). GROUP BY alias resolves input
  columns FIRST (silent rebinding) and is non-portable.

## Semi / anti joins & IN / EXISTS (Day 9)
- left_semi / left_anti are FILTERS wearing join syntax: output schema =
  LEFT table only, never emit right-side columns, never multiply rows.
  semi = EXISTS (keep left iff >=1 match); anti = NOT EXISTS (keep left
  iff no match). NOT a join-then-distinct.
- Catalyst decorrelation compiles ALL of these to the same LeftSemi /
  LeftAnti logical operators: IN (subquery), EXISTS, LEFT SEMI JOIN
  syntax, DSL left_semi -> LeftSemi; NOT IN (safe), NOT EXISTS,
  LEFT ANTI JOIN, DSL left_anti -> LeftAnti. Confirmed via explain():
  a query mixing NOT EXISTS + IN produced two BroadcastHashJoin nodes,
  one LeftAnti (from NOT EXISTS) + one LeftSemi (from IN), traced by
  the right-side attribute's Scan lineage.
- Why IN (subquery) becomes semi, not inner: IN means "appears at least
  once" -> must NOT duplicate left rows (orders with 3 matching rows
  still yields 1 customer row) AND must NOT emit right columns. Both
  constraints ARE the semi-join definition.

## NOT IN vs NOT EXISTS — three-valued-logic footgun
- NOT IN + subquery containing ANY NULL -> whole query returns ZERO
  rows: x <> NULL is UNKNOWN, so x NOT IN (..., NULL) is never TRUE.
  This is the negation-direction trap; positive IN is unaffected (a
  match is still TRUE, NULL just fails to match).
- NOT IN safety requires NO NULL on BOTH sides: subquery side AND outer
  column. isNotNull() on the subquery only patches ONE side; an outer
  NULL still yields NULL NOT IN (...) = UNKNOWN, silently dropped.
- NOT EXISTS / LEFT ANTI are NULL-safe by construction (built on =,
  NULL simply doesn't match) -> the correct DEFAULT for negated
  existence. Replacing NOT IN with NOT EXISTS is "choosing the right
  semantics" (NULL = not-a-match), NOT a blind equivalent rewrite:
  under NULLs the two genuinely differ, and standard NOT IN's
  NULL-poisoning is occasionally the intended behavior.
- Shuffle cost: once NOT IN is safely decorrelatable (no subquery-side
  NULL), it compiles to the SAME LeftAnti as NOT EXISTS -> identical
  Exchange count (0 if broadcast, 1-per-key if SMJ). The difference is
  NOT shuffle; it's the extra deletable filter-precondition NOT IN
  carries. When NOT IN CANNOT decorrelate (nullable subquery, no
  filter), Catalyst inserts a NULL-aware anti join -> can degrade to
  BroadcastNestedLoopJoin / extra null-check predicate = heavier plan.
  isNotNull() rescues NOT IN from the heavy plan back to the LeftAnti
  light plan.

## Redundant defensive code — join edition (Day 9)
- distinct() on the RIGHT side before semi/anti is redundant: existence
  semantics already ignore right-side multiplicity (1 match vs 100
  matches -> same left-row survival). It adds nothing to correctness
  but deterministically adds ONE groupBy Exchange (has partial-agg
  relief, but still a net-new shuffle). The Day-9 join-clothing version
  of "aggregate semantics already cover this filter" — semi/anti never
  multiply, so the right side never needs deduping.
- Inner-join-as-existence-test is the OPPOSITE mistake: it DOES multiply
  (grain change, Day 4/6 lesson in join clothing), forcing a downstream
  distinct/dropDuplicates that's easy to forget. Prefer semi join,
  which encodes "exists, don't multiply" directly.

## Maps
- map_keys / map_values / map_entries ~= dict.keys()/.values()/.items();
  all return ARRAY columns, so downstream work uses the array-function
  family (size, flatten, array_distinct...).
- map_keys(NULL) / map_values(NULL) return NULL, NOT an empty array.
  NULL map vs empty map is a real distinction that ripples downstream.
- Map access (props['k'], .getItem, element_at) returns NULL for both
  a missing key and a NULL map — never errors. Safe in filters
  (aggregate-NULL rule absorbs it), silent when debugging.
- explode() on a map emits TWO columns (key, value); explode on
  map_keys(...) emits one. Same drop-rule as arrays (Day 4):
  explode drops NULL/empty maps, explode_outer keeps the row.
- Dedup unit must match the row grain: at exploded grain keys are
  scalar elements, so collect_set(key) dedupes keys directly; at
  un-exploded grain each element is a whole ARRAY, so collect_set
  dedupes arrays ([] counts as a member!) — must
  flatten -> array_distinct instead. Companion rule to "explode
  changes grain, re-examine every COUNT" (count -> countDistinct(id)).
- countDistinct is pricier than count (expand/two-phase aggregation,
  weaker map-side partial agg) — the hidden cost of the explode route;
  verify via explain(), not memory.

## Structs
- Nested field access: device.os / F.col("device.os") /
  df["device"]["os"] are equivalent (dot-path, positional, fixed
  schema). A struct is NOT a map: fixed named fields, no dynamic keys,
  no element_at, no explode on a plain struct — explode is for the
  array(struct<...>) shape only.
- NULL parent struct vs NULL sub-field: accessing device.os when the
  WHOLE struct is NULL returns NULL (no error) — the SAME NULL as an
  explicitly-NULL os sub-field. Once you project device.os the two are
  INDISTINGUISHABLE. This is why the aggregate-NULL rule makes
  countDistinct(device.os) / COUNT(device.os) "just work" for both
  cases — but ALSO the trap: COUNT(device.os) counts every non-NULL os
  (macOS included), which is NOT "mobile events". mobile_events needs a
  conditional count over an explicit set — count(when(array_contains(
  [iOS,Android], os), 1)) — not COUNT(device.os). Clean rows where all
  non-null os happen to be mobile make the wrong version
  coincidentally-correct (camouflage again).
- primary_os = argmax-per-group. Three routes, all needing NULL-os
  pre-filter + deterministic tie-break:
  (a) row_number() over (partitionBy user orderBy cnt desc, os asc),
      keep rn=1 — most general, needed for Top-N>1 or multi-column keep.
  (b) min/max(struct(...)) — struct compares lexicographically field by
      field. See "struct composite-key argmax" below.
  (c) array_sort of collected (cnt,os) structs, take head.
  All three then LEFT JOIN back to the per-user base so a user with NO
  usable os still emits a row; COALESCE(primary_os,'UNKNOWN') fills it.
  Inner join would silently drop the all-NULL-os user — same lesson as
  Day 5 fact-dim enrichment.

## Struct composite-key argmax: min/max(struct(...))
- min/max on a struct<f1,f2,...> compares fields left-to-right,
  lexicographically (like a tuple). min drives ALL fields toward
  small, max drives ALL fields toward large — one direction for the
  whole struct, you cannot mix per-field directions natively.
- To encode "cnt DESC, os ASC" (conflicting directions) under a single
  aggregate: pick the aggregate for the tie-break field's direction,
  then NEGATE whichever field fights it. os asc = "take small" -> use
  min; cnt desc conflicts with min's "small", so negate it ->
  min(struct(-cnt, os)), then .select("best.os"). Equivalent to
  max(struct(cnt, ...)) when there's no conflicting tie-break; the
  negation is ONLY needed to reconcile opposite sort directions.
  Strings have no unary minus, so if the tie-break field needs the
  direction that fights max/min, min-with-negated-numeric is the clean
  escape hatch.
- No general multi-key argmax in Spark. max_by(value, ordering) takes
  a SINGLE ordering column and gives NO tie-break guarantee — cannot
  express "cnt desc + os asc" deterministically. Hence row_number /
  struct-argmax / array_sort are the real options.
- Perf: struct-argmax vs window BOTH cost 2 shuffles here (groupBy
  (user,os) then re-partition by user — (user,os)->(user) is
  superset->subset, no reuse). The win is NOT shuffle count: the second
  stage of struct-argmax is a groupBy+min AGGREGATE (map-side partial
  agg, tiny data crosses the shuffle), whereas window's second stage is
  a full SORT + Window over every os row (no partial-agg relief).
  Confirm by explain(): both show 2 Exchanges, but struct route's
  second is a HashAggregate pair, window route's is Sort+Window.
  Don't record the conclusion as "fewer shuffles" — that's wrong;
  record it as "aggregate beats sort at the second Exchange".

## Complex aggregation: GROUPING SETS / ROLLUP / CUBE (Day 10)
- Three DIFFERENT layers, not siblings:
  * GROUPING SETS / ROLLUP / CUBE are GROUP-BY clauses — they decide
    WHICH grains (rows) get produced. GROUPING SETS = list exactly the
    grains you want (most precise). ROLLUP(a,b) = hierarchical subset
    {(a,b),(a),()} — assumes a->b drill-down, DOES NOT include (b).
    CUBE(a,b) = all 2^n subsets {(a,b),(a),(b),()}.
    Equivalences: CUBE(a,b) == GROUPING SETS ((a,b),(a),(b),());
    ROLLUP(a,b) == GROUPING SETS ((a,b),(a),()). CUBE/ROLLUP are just
    syntax sugar for common GROUPING SETS combinations.
  * GROUPING(col) is an AGGREGATE FUNCTION returning 0/1 — reads, per
    output row, "was this col rolled up here?" (1 = aggregated away /
    subtotal dim, 0 = real value). ONLY reliable "is this a subtotal"
    signal: a bare `col IS NULL` cannot tell a real-NULL source value
    from the NULL the rollup machinery injects on subtotal rows (echoes
    Day 7 "NULL origin lost once projected"; GROUPING() recovers it).
  * GROUPING_ID(c1,...,cn) packs the per-col grouping bits into one int:
    LEFTMOST arg = HIGH bit. GROUPING_ID(region,category): region=bit1
    (value 2), category=bit0 (value 1). 0=base grain, all-ones=grand
    total. One CASE on the int maps cleanly to a 'level' label; the
    per-col grouping() form (gr==0 & gc==0 ...) is more verbose but
    self-documenting (no bit-order recall).
- Rule of thumb: GROUPING SETS/CUBE/ROLLUP PRODUCE the multi-grain rows;
  GROUPING/GROUPING_ID READ the label on each row. First makes data,
  second interprets it.
- DSL gap: there is df.cube(...) / df.rollup(...) + F.grouping /
  F.grouping_id, but NO df.grouping_sets() helper. To get "specific
  grains only" in pure DSL: cube-then-filter, or explicit groupBy+union,
  or switch to SQL. 2-dim cube here wastes nothing (its 4 subsets ARE
  the 4 wanted grains); with n dims cube = 2^n rows (combinatorial
  blow-up), so prefer GROUPING SETS to name exact grains — same
  "explicit values list" philosophy as Day 3 pivot.
- Single source scan: grouping sets / cube / rollup read the table ONCE
  and expand internally — physical plan shows one Expand (row-multiplying
  operator that fills NULL for non-participating dims) + one Exchange +
  HashAggregate pair. The 4-groupBy-union route scans the source 4x with
  4 separate Exchanges. Verify by counting Expand/Exchange in .explain().

## Real-NULL vs subtotal-NULL — two robust strategies (Day 10)
- The problem: a genuinely-NULL source dimension and the NULL that
  cube/rollup injects on a subtotal row are INDISTINGUISHABLE once
  projected. Two clean routes, different robustness profiles:
  * PRE-FILL SENTINEL (chosen route): coalesce(dim, '<sentinel>')
    BEFORE cube. Kills the source NULL entirely, so every remaining NULL
    can only be a subtotal NULL. Most robust — removes the confusion at
    the root; you can then rebuild dims from grouping_id ALONE without
    ever touching IS NULL. Cost: sentinel string could collide with a
    real dim value (guard by choosing an impossible sentinel).
  * POST-HOC DISCRIMINATE (reference route): keep the NULL, branch
    GROUPING(dim)==1 -> 'ALL' BEFORE IS NULL -> sentinel. More "correct"
    (doesn't mutate source) but ORDER-SENSITIVE: reverse the two
    branches and real-NULL + subtotal-NULL collapse together. The
    fragile path — the branch order is a deletable invariant the next
    editor can break.
- cast('long') on SUM(int): SUM(int)->bigint; explicit cast is schema
  hygiene for production sinks (check() won't catch either way).

## Date/time: tz bucketing + date-dimension gap-fill (Day 11)
- Conversion DIRECTION is the whole trap. from_utc_timestamp(ts, tz)
  reads "ts is a UTC instant, give me the wall-clock time in tz" — the
  correct direction for "stored in UTC, report in local". to_utc_timestamp
  is the INVERSE (local->UTC) and shifts the wrong way: an LA sale gets
  +8h instead of -8h, silently moving it to the next day. The bug PASSES
  on every row that doesn't straddle local midnight — only day-boundary
  rows expose it. Same "camouflage on clean rows" family as COUNT(device.os).
- to_date / cast(date) on a timestamp is evaluated in
  spark.sql.session.timeZone. You already localized via
  from_utc_timestamp; if the session tz is non-UTC, to_date re-shifts a
  SECOND time = double-conversion day-shift. Pin session tz (UTC in the
  harness) so the date you computed isn't silently re-bucketed.
- The window filter must be applied on the LOCAL date, AFTER tz
  conversion — never on the raw UTC date. An out-of-window UTC timestamp
  can land in-window locally (and vice versa), so filtering ts_utc first
  drops/keeps the wrong rows. This is the SECOND layer of the tz trap,
  distinct from the direction bug: right function, wrong axis to filter on.
- Gap-fill = manufacture a dense SPINE, don't detect gaps. Build the full
  key x date axis (explode(sequence(start, stop, interval 1 day)) crossJoin
  the dimension), LEFT join the sparse aggregate onto the spine (spine is
  the LEFT/preserved side), COALESCE nulls to 0. Dimension-table cousin of
  Day 2 gaps-and-islands: Day 2 DETECTED gaps via row_number arithmetic;
  here you build the complete axis so a missing day cannot hide.
- sequence(start, stop, interval) is INCLUSIVE on BOTH ends; explode turns
  the array into rows. Narrow generation (no shuffle). Aggregate sales to
  (store, day) grain BEFORE joining the spine: the join is then
  spine(small) LEFT agg(small) — never join raw sales to the spine (grain
  blow-up + needless shuffle), same "aggregate before the wide join"
  discipline as Day 8's groupBy-branch cost.
- Zero-fill both measures with correct types: revenue -> 0.0 (double,
  SUM(double) already double), n_txn -> CAST(0 AS BIGINT) to match COUNT's
  bigint. The double cast on revenue is redundant (int literal 0 promotes
  to double under coalesce with a double); the bigint cast on n_txn is the
  one that actually earns its keep. Redundant-vs-necessary defensive cast,
  same discriminate-don't-blanket-cast rule as elsewhere.

## Physical plans / explain()
- Read bottom-up; key nodes: Scan (source + stats quality), Exchange
  (= one shuffle each, the cost driver), join node (strategy +
  BuildLeft/Right side), HashAggregate pairs (partial/final =
  map-side pre-aggregation working).
- AdaptiveSparkPlan isFinalPlan=false means pre-execution static plan;
  trigger an action on the SAME DataFrame object, then explain() again
  for the AQE final plan (Initial vs Final diff = AQE's runtime
  decisions: join conversion, AQEShuffleRead coalesced partitions).
- Optimizer inserts operators you didn't write (isnotnull filter on
  the null-rejecting side of joins, column-pruning Projects).
- Shuffle-producing ops: groupBy/distinct/dropDuplicates (with partial
  agg relief), non-broadcast joins (both sides), Window.partitionBy,
  orderBy, repartition, intersect/except. Narrow (no shuffle):
  select/filter/withColumn/explode/union/coalesce(shrink).
- Same partitioning can be reused (ENSURE_REQUIREMENTS) only when the
  partition expressions MATCH; subset/superset keys still re-shuffle.

## API style conventions
- Pure column references (select/groupBy/on) -> plain strings; F.col()
  when the column participates in expressions (comparison, arithmetic,
  .desc()/.alias()/.cast(), when()), when referencing a column created
  mid-chain, alias-qualified names ("t.col"), or programmatic column
  generation. df["col"] (bound) for self-join disambiguation.
- DATE_SUB(date, int_col) over date - int (portability: MySQL/Hive
  differ); F.date_sub accepts Column only on Spark 3.3+.
- try_cast only for genuinely dirty data; plain cast states intent.
- cast("int") after count/sum: schema hygiene for production sinks
  (count returns BIGINT); tests won't catch it either way.
- LATERAL VIEW OUTER EXPLODE (Hive style, max compatibility) vs
  modern LATERAL explode_outer(...) — both fine in Spark 3.x.

## Review heuristics (accumulated)
- In unusual contexts (pivot agg, custom agg), distrust star/wildcard
  expressions — resolution paths differ.
- The aggregate-NULL rule prunes defensive code in BOTH directions.
- When explain() contradicts expectations, check the Scan node type
  first — statistics availability explains half of optimizer behavior.
- Column-order of output matters to positional tests; a select that
  reorders is a silent contract change for downstream consumers.
- Verify performance claims by counting Exchange nodes, not from
  memory.
- "Fewer shuffles" is a tempting but often-wrong perf story: when two
  routes have the SAME Exchange count, the real difference is the
  WEIGHT of each stage (partial-aggregatable aggregate vs full sort).
  Name the actual mechanism, don't assert a shuffle-count delta.
- A COUNT that isn't the count you think: COUNT(field) means "rows
  where field is non-NULL", which silently equals the wrong metric
  when the non-NULL set coincides with your intended set on clean data
  (COUNT(device.os) == mobile_events only because test rows' non-null
  os were all mobile). Re-derive what each COUNT actually counts from
  its argument's NULL behavior, per definition.
- Coincidentally-correct output is camouflage: a wrong-level dedup can
  produce the right number on some groups (user 1's three distinct
  key-arrays = three distinct keys). Verify the MECHANISM per edge-case
  row, not just the final numbers.
- Misleading column names survive tests but not code review: a column
  holding all property keys named "devices" is a contract lie to
  downstream readers.
- Negated existence in SQL: reach for NOT EXISTS / LEFT ANTI by default,
  not NOT IN. NOT IN needs BOTH sides NULL-free; the isNotNull patch is
  deletable by the next editor. Reserve NOT IN for constant lists or
  when three-valued NULL-poisoning is genuinely intended.
- "distinct before a join" smell: if the join is semi/anti, the distinct
  is always redundant (existence ignores multiplicity). If it's inner,
  the distinct may be compensating for a grain change the join
  shouldn't have caused — consider semi instead.
- Edge-case sample in the prompt is a CONTRACT, not decoration: Day 10's
  single real-NULL-region example row WAS the whole trap. The AI solution
  handled subtotal-NULL correctly but never surfaced the real NULL —
  it treated the lone example row as illustration, not as a first-class
  requirement. When a problem shows exactly one row exercising an edge
  case, promote it to a must-handle case before reading any solution.
- GROUPING_ID magic numbers (isin(2,3)) hide a bit-order assumption:
  gid.isin(2,3) == "region rolled up" is only true if region is the
  leftmost (high) arg to grouping_id. The per-column grouping() form is
  more verbose but needs no bit-order recall — prefer it in shared code,
  reserve grouping_id for many-dim single-CASE level mapping. Either way,
  the value->meaning map must be pinned to the ARG ORDER, not column
  order in the table.
- "Which NULL is this" on the aggregation axis: after any cube/rollup/
  grouping-sets, a NULL in a grouping column is ambiguous by default.
  Either pre-fill a sentinel before the aggregation (root fix) or gate
  every dim reconstruction on GROUPING()==1 first — never rebuild a dim
  from `IS NULL` alone in a grouping-sets result.
- HAVING that is secretly a WHERE (Day 11): filtering on a GROUP BY key
  in HAVING happens to equal WHERE only because the predicate touches a
  group key, not an aggregate. It PASSES but is a smell — it relies on
  "the filter column is a grouping key" and forfeits predicate pushdown
  to before the aggregate. Row-level predicates belong in WHERE; reserve
  HAVING for conditions on aggregate results. Catch it by reading: ask
  "is this predicate over a raw column or an aggregate?" — raw column in
  HAVING = should be WHERE.
- spark.range vs explode(sequence) for a small fixed axis is NOT a
  cheap/expensive story (Day 11 corrected a misconception). Neither is a
  shuffle; range is narrow too. The real difference: sequence is a
  compile-time constant array (matches the intent of a known date domain,
  and its crossJoin reliably degrades to a broadcast/nested-loop with ~0
  Exchange), while range yields a partitioned dataset that MAY introduce
  a tiny extra exchange at the crossJoin. On a 7-row axis both are
  effectively free. Argue it as "constant-domain intent + broadcast
  stability", verified by counting Exchange in .explain() — never as a
  memorized "range is expensive".
