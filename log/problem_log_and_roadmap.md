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
| 12 | Complex aggregation advanced: GROUPING SETS precise-grain control + conditional (threshold) aggregation | Medium-Hard | Three-grain sales summary at EXACTLY (region,category) / (region) / (category) — NO grand total — in one pass; measures total_amount + big_orders (count of amount>=1000); rolled dims -> 'ALL', level from grouping metadata | threshold-count formulation trap: COUNT(when(cond,1).otherwise(0)) counts EVERY row (0 is non-NULL) = group size, right on some grains + wrong on others; correct = SUM(when(cond,1).otherwise(0)) or COUNT(when(cond,1)) no-ELSE. Precise grains {(r,c),(r),(c)}: cube adds unwanted () (filter gid!=3), rollup MISSES (category) — only GROUPING SETS (or cube-then-filter, or union) hits exactly these three. Rebuild rolled dims from gid/grouping() ONLY, never `IS NULL AND gid` (IS NULL is redundant + misleading; gid alone is the root判据). AI-review: `F.grouping_sets` DOES NOT EXIST in any version (the functions module has only `grouping` / `grouping_id`) — AttributeError at runtime. The capability DOES exist, under a different namespace AND a different naming convention: `DataFrame.groupingSets(...) -> GroupedData`, added 4.0.0 (3.x genuinely had no DSL route). df methods are camelCase, functions are snake_case — the hallucination got both wrong at once |
| 13 | Date/time round 2: event sessionization (gap-threshold islands: lag -> boundary flag -> running SUM(flag) session key) | Medium-Hard | Sessionize a raw clickstream: per (user,session) report session_start / session_end / n_events, gap <= 30 min = same session | Day 2's (value-row_number) constant-diff DIES (gap is a range 0..threshold, not a fixed +1) -> need flag+running-sum idiom. Traps (all coincidentally-correct on clean data): gap direction must be current-prev (prev-current is always negative, `> threshold` never fires -> everything collapses to one session); compare in SECONDS not truncated minutes (timestamp_diff('MINUTE') truncates 30:40->30, wrongly merged, passes on integer-minute test rows); boundary STRICTLY > (gap==1800s stays SAME session, `>=` is off-by-one on the exact-boundary row); duplicate ts are RANGE peers (harmless here: gap 0 -> flag 0). Ordered-window default frame RANGE UNBOUNDED..CURRENT = Day 8 BUG reused as the TOOL. session_window built-in MISMATCHES this spec (< boundary + end = last+gap). |
| 14 | UDFs: python UDF vs native expressions (cost model, serialization, registration) | Medium | Query-string payload normalization: n_params / user_tier / latency_bucket; parsing via a self-written Python UDF, bucketing native | empty-value trap: `tier=` must yield `""` NOT 'UNKNOWN' — regex `tier=([^&]+)` requires >=1 char so it silently falls through to the UNKNOWN branch (`+`->`*` fixes it), and an unanchored key regex also mis-matches the SUFFIX `user_tier=gold` (needs `(?:^\|&)` or split+partition). `"".split("&")` returns `[""]` (len 1, NOT 0) so empty payload must be intercepted BEFORE split. UDF registration: `@F.udf` is DSL-only, SQL needs `spark.udf.register`; passing a UDF object AND a returnType raises CANNOT_SPECIFY_RETURN_TYPE_FOR_UDF. AI-SQL: hand-rolled 5-layer transform/filter/element_at nest instead of `str_to_map` -> `element_at(empty_array, 1)` THROWS under ANSI (INVALID_ARRAY_INDEX_IN_ELEMENT_AT); the whole COALESCE fallback assumed a NULL that ANSI never delivers |
| 15 | Window frames as the primary topic: trailing moving average + period-over-period growth on a SPARSE daily series (ROWS vs RANGE, frame-as-value-lookup) | Medium | Per-store trend report on a sparse daily sales table: revenue / ma7 (trailing 7 CALENDAR days, denominator always 7) / prev_day_revenue / dod_growth_pct | "previous ROW" vs "previous DAY" on a sparse series: rowsBetween(-6,0) and lag(1) reach past the gap and are only wrong on rows adjacent to a gap (7 of 10 test rows agree either way). rangeBetween(-1,-1) is a frame used as a value LOOKUP; its empty frame -> NULL makes that COALESCE load-bearing while the ma7 COALESCE is dead code (frame containing currentRow can never be empty). AVG over a sparse RANGE frame has a data-dependent denominator (rows present), so a fixed 7-calendar-day denominator must be sum/7. RUNTIME BLOCKER (corrects an earlier logged claim): in PySpark a DATE order column + finite integer RANGE bound raises DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE (Python int -> BIGINT literal; frame boundaries only up-cast) -- SQL's RANGE BETWEEN 6 PRECEDING on DATE is fine, DSL needs a numeric day-number (unix_date / datediff-to-epoch). Only FINITE numeric bounds trigger the check; all-symbolic bounds (unbounded / currentRow, incl. literal 0) are safe on any order type. AI-review: the day-number CTE is redundant in the SQL half only (clarity, NOT performance -- verified 1 Exchange either way); hand-rolled datediff-to-epoch where unix_date exists (Day 14 heuristic again); `WHEN prev = 0 THEN NULL` (spec-literal) vs `WHEN prev > 0` (user) diverge on negative revenue, indistinguishable on the test data |
| 16 | NULL semantics special (null-safe equality `<=>`, NULL in joins, NULL ordering in windows / NULLS FIRST\|LAST) | Medium | Catalog × marketplace offer reconciliation: per catalog entry emit n_offers / best_seller / best_price, where a NULL `variant` is a MEANINGFUL key value (the base product, labelled 'BASE') present in BOTH tables, and an unknown price can never win best_seller | nullable join key is the whole trap: `=` on `variant` silently drops every base product, and because the join is LEFT the failure surfaces as *plausible-looking rows* (n_offers 0, best_seller 'NONE') rather than missing rows — harder to spot than dropped rows. Two grains: n_offers over ALL offers vs best over KNOWN-PRICE offers only — filter FIRST then rank. Default NULL ordering ASC -> NULLS FIRST, so a plain `orderBy(price.asc())` floats the *missing* value to rank 1. Filter-vs-`NULLS LAST` are NOT interchangeable: spec says unknown price can never win = filter semantics; `NULLS LAST` still gives rn=1 to an all-unknown-price key, and once the filter is in it degrades to DEAD CODE (Day 15 load-bearing-vs-dead COALESCE sibling). Row S2 (key exists, every price unknown) is the ONLY row separating "no match" from "matched but nothing usable" — naive code is correct on every other row. After a LEFT join `COUNT(*)` reports 1 for an empty group, `COUNT(right_col)` reports 0. Defensive code is dictated by STRUCTURE not spec: join-then-groupBy gets the zero free from `count(seller)`, groupBy-then-join must write `COALESCE(n_offers,0)` — reordering the two invalidates every NULL guard. `MIN(struct(price,seller))` is only correct AFTER NULL prices are gone (NULL sorts smallest, an unknown price wins the MIN outright). Sentinel-vs-`<=>` four-way tradeoff: sentinel collides if 'BASE' is ever legitimate + the filled key doubles as output label (test compares by position, won't catch it); `<=>` has no collision surface but must be repeated at EVERY join. PLAN (measured): `<=>` desugars into TWO ordinary equi-keys `coalesce(v,'')` + `isnull(v)` — never a nested-loop join, still broadcastable — but those are DERIVED expressions that don't match an upstream `GROUP BY sku, variant` partitioning: forced SMJ = 3 Exchanges vs sentinel 2, and AQE+broadcast erases the gap entirely (broadcast has no partitioning requirement). Sibling branches on IDENTICAL keys do NOT share an Exchange (count Scans first, then Exchanges); reuse needs same lineage AND partition-keys ⊆ required-keys — this OVERTURNS the older log/04 claim that subset also reshuffles. `WindowGroupLimit ... Partial` below the Exchange means rank-filter windows DO have map-side reduction — limits the Day 7 record |
| 17 | Incremental patterns: SCD2 history build from a change feed (run-based versioning: lag -> boundary flag -> running SUM -> half-open interval closing) | Medium-Hard | Build the SCD2 history table from an append-only product attribute feed: one row per (product, version) with effective_from / effective_to / is_current, tracked attrs = category + price | no-op replay is the trap: a version is a *run* of feed rows, not a feed row. `lead(changed_at) OVER (partition by product_id order by changed_at)` IS the correct interval-closing idiom — it is only applied to the wrong row set, so one-row-per-version is right on every product whose consecutive rows genuinely differ (8 of 9 expected rows, 3 of 4 products correct); P1's 01-05 replay is the only exposing row, and it fails BOTH ways at once (spurious version AND the surviving interval truncated 01-01->01-05 instead of ->01-10). Adjacent, not the trap: P4 `40 -> 45 -> 40` — boundary detection must be POSITIONAL (lag vs current), never value-set-based; `dropDuplicates([id,category,price])` / DISTINCT collapses the two non-adjacent 40.0 runs and loses the third interval. Half-open `[from,to)` needs NO date arithmetic (`effective_to = lead(effective_from)`, no date_sub -1); `is_current = effective_to IS NULL` is DERIVED, a second `row_number() desc = 1` window re-sorts to learn the same fact. Window functions are ILLEGAL in WHERE/HAVING in both APIs (`AnalysisException: It is not allowed to use window functions inside WHERE clause`) — materialize via withColumn / a CTE first. AI-review: AI's change predicate `prev_category.isNull() \| (prev_cat != cat) \| (prev_price != price)` guards only ONE of the two attributes — on a NULL price the whole OR chain returns NULL, cast("int") -> NULL, running SUM skips it and FREEZES, collapsing every subsequent row into one version (measured: 4 dirty rows -> 1 version vs the correct 3). Test data can never expose it (spec guarantees non-NULL attrs). User's `F.struct(cat,price) != F.struct(prev_cat,prev_price)` was immune for a reason the user hadn't identified: struct comparison is FIELD-WISE NULL-SAFE, so the predicate never yields NULL and the `.otherwise(0)` was dead code (measured: user_flag = 1 on every product's first row, identical to AI's and the ref's). AI's explicit `ROWS` vs user/ref default `RANGE` on the running SUM: indistinguishable here only because the spec forbids duplicate changed_at per product. PLAN (measured): all 4 solutions = 1 Exchange (groupBy after a window adds none — window partitionBy ⊆ grouping keys, re-confirms Day 16); user 2 Sorts vs AI 3, because user's second window `orderBy('boundary')` reuses the aggregate's output ordering while AI's `orderBy('effective_from')` does not — an accidental win, and the ONLY plan difference. `min`/`max` on a STRING column forces SortAggregate (non-fixed-width agg buffer, extends the Day 16 `min(struct(...))` finding); moving the run-constant attrs into the grouping key (ref's route) restores HashAggregate — isolated A/B, only the agg list changed |
| 18 | Skew handling: salted join + two-phase aggregation (pmod salt -> dimension replication via explode(sequence) -> partial/final recombination) | Medium-Hard | Per-customer summary over a skewed fact table, forced through a salted join + two-phase agg: total_amount (SUM) + n_products (DISTINCT count) | DISTINCT 计数在盐桶上**不可分解**是全题靶心:partial `COUNT(DISTINCT product)` per (cust,salt) -> final `SUM` 对每个跨桶的 product 重复计数(实测 C_HOT n_products=**8** vs 正确 **4**),而同一个 agg 里的 total_amount 因为 SUM 可分解**永远正确**——输出是**半对的一行**:钱对、基数错。四行里只有热点键暴露:C1 的 10/13/16 全满足 `id%3==1` 整个落单桶,且它是唯一有重复 product 的冷客户(专骗"重复产品处理对吗"这类检查);C2 跨两桶但两个产品互不跨桶;C3 单行。两条正解:`collect_set` -> `flatten` -> `array_distinct` -> `size`(集合并可分解,`array_distinct` 是**承重**不是防御,因为 collect_set 只在桶内去重);或把 product 放进 partial 分组键把 distinct 推迟到第二阶段。用户解法两个 bug **互相抵消**:盐取自 join key(`pmod(hash(customer_id),3)`)-> 同一客户同盐、热点 8 行全落一个桶、维度白复制 3x,且第二阶段聚合**完全缺失**(SQL 侧 `GROUP BY` 里根本没有 salt,是单阶段)——把盐换成 `pmod(order_id,3)` 立刻从 4 行变 **7 行**。`approx_count_distinct` 在基数 <=4 时与精确值逐行相同,**任何测试数据都分辨不出**(默认 rsd=0.05)。AI(= 参考 Route B)全对,未踩陷阱。PLAN(实测,broadcast off):不加盐基线 **2** Exchange / 加盐+collect_set **3** / 加盐+product 进 partial 键 **4** —— **加盐路线是最贵的**;"去盐税"那个 Exchange 由 `salt ∈ 分区键 ∉ 最终分组键` 结构性导致(Exchange 子集规则第三次确认),`COUNT(DISTINCT)` 的重写还要再按 `(keys, product)` 多插一个。AQE skewJoin 需**同时**满足 >256MB **且** >5x 中位数,14 行两条都不达标,只观察到 `AQEShuffleRead coalesced`,**没有** `OptimizeSkewedJoin` |
| 19 | 高阶数组函数(transform / filter / exists / forall / aggregate / size,数组内原地计算) | Medium | 订单行项目以 array<struct> 反范式存储,每单出一行汇总:n_active / active_total / has_bulk / all_in_stock,禁止 explode + groupBy 回卷 | `forall([])` 返回 **true**(AND 单位元)是全题靶心:`filter(items, status='ACTIVE')` 清空数组后,零 ACTIVE 行的订单被判成"全部有货"。规格里"AT LEAST ONE ACTIVE ... AND ..."的第一个子句 `size(active) > 0` 是**承重**不是防御。同组四个度量只有它错:`size([])=0` ✓、`aggregate([],0.0D,…)=0.0` ✓、`exists([])=false`(OR 单位元)✓、`forall([])=true` ✗ —— 失败是 5x5 输出网格里的**单个格子**(O4.all_in_stock),同一行另外四列全对。唯一暴露行 O4 两行全 CANCELLED **且**两行 in_stock 都为 true(堵死"忘了 filter 也能蒙对"的后门);D2 qty=15 顺带校验 has_bulk 真的过滤了,O2.B2(CANCELLED, 20@100.0)让漏过滤在 active_total 上炸成 2048.0 而非 48.0。`aggregate` 的 zero 钉死累加器类型且**无隐式加宽**:DSL `F.lit(0)`、SQL `0`、SQL 裸 `0.0`(**DECIMAL(1,1)**,不是 double)三者全在分析期 `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` 死掉——响的,不是静默的。AI(= 参考 Route A)全对未踩陷阱;用户 Stage-1 也对,但**本日 trap 维度无有效信号**:用户在 Stage 1 中途直接问了"空数组怎么判",教练给了 `size()` 并说破"四列里只有一列需要守卫",陷阱在解题前已被拆除。PLAN(实测):HOF **0** Exchange(整题一个 Project over Scan)/ explode_outer + 条件聚合 **1** / explode + WHERE + groupBy + LEFT join 回卷 **2** + broadcast —— HOF 少有的"赢在计划形状而非风格"的场合,但优势来自**反范式布局**(数组与 key 已同置),行项目一旦独立成表就只剩 explode 路线。**超出参考的新发现**:`active` 存进 Python 变量复用 = 表达式**内联 5 份**,`CollapseProject` **主动拒绝**把非廉价表达式复制到多个使用点,所以 `withColumn` 命名版真的只算一次(Project x2 / `filter(items` x1),内联版 x5;五份拷贝的 lambda ExprId 各异(`x_7#18..#22`)因而**互不 semanticEquals**,plan 层 CSE 与运行时 subexpressionElimination 都补不全 —— 400k 行 x 12 元素实测 **0.83s vs 1.17s**(关掉 SEE 后 0.73 vs 1.39),Exchange 两边**同为 0**,是**每行 CPU** 不是 shuffle 的故事。用户 review 漏检此项,却把自己多出来的一层 `transform` 记在 Performance 栏(应为 style;实测该 transform 的代价被省下的四次 filter 完全覆盖),并误判 `F.lit("ACTIVE")` 比裸 `"ACTIVE"` 更健壮(`Column.__eq__` 自动包 lit,表达式完全相同;真正更健壮的是 AI 的 `x["status"]` 下标 vs 用户的 `x.status` 属性——后者撞上名为 `cast`/`alias` 的字段会拿到绑定方法)。用户 SQL 用 CTE 命名 `active` **优于参考答案**(ref 的 SQL 把 filter 抄了 5 遍,正文自承 CTE 更好) |
| 20 | ANSI 模式与 try_* 家族(try_cast / try_to_number / try_divide / try_sum),脏字符串入库 | Medium-Hard | 三个上游把全 STRING 的销售记录落到一张表,按 source 出入库报表:n_records / n_bad_amount / net_amount / total_units / avg_unit_price | `try_cast(units_raw AS INT)` 对 `'12.0'` 返回 **NULL** 是全题靶心:string→INT 要求**整数字面量**,与 numeric→INT 的"截断"是**两条不同的解析路径**(`cast(12.7 AS INT)`=12 实测)。唯一暴露行 R05,而它的 `amount_raw` 是全表最干净的串(`'420.00'`,无 $ 无逗号无空白无符号)——注意力被刻意引开。失败形状是 3x6 网格里**同一行的两格**(total_units 4 vs 16、avg 379.88 vs 94.97),该行的 n_records / n_bad_amount / net_amount **全对**(两个字段独立解析),且错误值 4 是个合理的小正整数,输出形状不含任何"解析失败"的信号。正解 `try_cast('double').try_cast('int')`,**第二跳也必须是 try_**(`cast(3.0E9 AS INT)` 在 ANSI 下抛 CAST_OVERFLOW)。邻近但非陷阱:贪婪的 `regexp_replace(amount_raw,'[^0-9.]','')` 会把**负号**一起剥掉,退款 -120 变 +120(SRC_A 1980.5→2220.5)——黑名单去噪 `[$,]`,永不白名单信号。**用户走了参考答案没有的第三条路**:双掩码 `try_to_number` coalesce(`S$999,999,999.99` / `S999,999,999.99`),绕开了 ref"单一 format 覆盖不了三种形态"的论据;实测它在 `'NaN'`/`'Infinity'` 上**比 ref Route A 更安全**(ref 的裸 `try_cast('double')` 把两者解析成 nan/inf,是 ref 自承的真洞),但在 `'$1234.56'`(带 $ 不带逗号)与超 10 亿的金额上**返回 NULL**——掩码里 `,` 是"位数够了就必需",且宽度即业务上限。可选元素需 2^k 个掩码。AI(≈ ref Route B)**未踩陷阱**:正则闸门 + `regexp_extract` 抠整数位,且在 `'12.5'` 上**比用户和 ref Route A 都更贴 spec**(两者静默截断成 12,AI 判 NULL;ref 自己承认 Route B 对)。AI 唯一实质问题 `COALESCE(sum(amt), 0.0)`:本数据每个 source 至少一个可解析金额,**不可达**(ref 定性为 dead code),但在"整组全不可解析"的输入上把 NULL 变成 0.0——spec 从未这么说。**用户 REVIEW_NOTES 六条里四条是裸 LGTM,唯一漏报恰好落在专为它设的 Robustness 栏。** trap 维度**本日无有效信号**(用户首次接触 try_*,Stage 1 中途由教练说破机制)——但 coalesce 漏报与裸 LGTM 是**未被污染的信号**,与 try_* 知识无关。PLAN(实测):user-DSL / user-SQL / AI-DSL / AI-SQL **四份节点级同构**,均 Exchange **1** / HashAggregate **2**(partial+final)/ Sort **0**,四个聚合全定宽故停在 HashAggregate;**两条路线之间没有性能故事**(ref 明写 "none should be invented")。附:`try_divide` 在执行计划里渲染成**裸 `/`**(EvalMode.TRY 不打印),ANSI 安全性**只能审源码,不能审 explain** |

> **Day 16 状态:已结。** Stage 1 + Stage 5 完成 —— log/04 里 Day 16 是全项目最大的
> 一块,含多项 explain 实测,并推翻了 Day 7 与 log/04 各一条旧记录。
> Stage 2–4 的 AI review **不再补做**,`days/day16_null_semantics.py` 的 Part 3
> 保持为空。

## ETL scenario days

> Day 1–21 的 `Completed problems` 表按"当日新技术"组织,与本表的三轴不是同一
> 回事,**不回填、不迁移**。Day 22 起的行只进本表。

| Day | 层级 | 业务域 | Difficulty | Output table | 生产约束 | Trap / key edge case |
|-----|------|--------|-----------|--------------|----------|----------------------|
| 22 | L4 incremental | subscription billing | Medium-Hard (5 stages) | `dim_subscription` | P1. Re-running this job on the same two inputs must produce byte-identical output. The batch is replayed whenever the scheduler retries.<br>P2. Every output row's `mrr` must come from `plan_catalog`. The `mrr` the change feed carries is advisory — upstream computes it independently and it is not the system of record.<br>P3. A subscription deleted in this batch (`op = 'D'`) must not appear in the output at all. | **一条变更不会自动比它落在的目标行新**是全题靶心:SUB2 的 `change_ts = 2026-08-17 10:00:00` 落在 `updated_at = 2026-08-17 18:30:00` 的行上,无防护应用会把订阅从 PRO **静默回滚**成 BASIC。失败形状极隐蔽:行数对、无 NULL、无重复,降级本身是正常业务事件,5 行里 4 行正确而错的那行**看不出错**;两个时间戳同为 8-17 只差几小时(其余变更全是 8-18),日期级目测抓不到。唯一读时信号是 **`updated_at` 会倒着走**(维表 last-updated 能变小 = 无防护 merge 的签名)。两条正解结构不同:**Route A 显式守卫**(`change_ts > target.updated_at`,可审计)vs **Route B 让目标行进同一排序竞争**(无 staleness 谓词,守卫**涌现**;反面是一旦 union 少了目标行或统一错了列,保护静默消失且**没有一行代码会缺**)。P1 **测试数据分辨不出 `>` 与 `>=`**(每个 key 时间戳互异),别给严格性记功;P2 忽略则 SUB3 输出 55.0 而非 100.0(feed 的 advisory 值恰好只在这一行错);P3 的墓碑必须从upsert 集与幸存目标集**两处**移除,只写 `op <> 'D'` 会删掉墓碑却留下目标行 = 订阅复活。**扰动实测(测试数据全绿)**:stale DELETE 与"同批先 D 后 I 重建"两种输入下,user-SQL 的 `NOT EXISTS (... op='D')` 抹掉整个 key,其余五份实现全部保留或重建——用户两半实现了不同语义,且**用户 SQL 才是 P3 的字面读法**,参考实现比自己的措辞宽松;plan 缺失于目录时四份解法 left join 产出 `mrr = NULL`(下游 SUM 静默吞成 0),ref 两路 inner join 整行丢弃,无一份写断言。**AI 未踩陷阱**(显式 Route A),且 P1 上**强于用户与参考答案**:五键全序 tie-break 保证平局也确定,用户两版与 ref 都只有单键。**用户 Stage-1 走 Route B 结构性免疫,但 REVIEW 未触及 trap 轴**——Correctness 栏裸 LGTM,反把 tie-break 标成"可能冗余",注意力刚好放反。PLAN(实测,AQE off):ref-B **2** Exchange / user-DSL **3**(多开一个窗口:CDC 先收敛一次、union 后再排一次,Route B 本可合并成一个)/ AI-DSL **4** / ref-A **9**(四路分叉未缓存,Scan **12** 次是真账)。`full_outer` 改 `left` 实测丢 SUB6(4 行 vs 5 行)且 **Exchange 同为 4**——零收益的正确性回归 |
| 23 | L3 serving | e-commerce (marketplace) orders | Medium-Hard (4 stages) | `agg_region_daily` | P1. Every metric is defined once. `net_gmv` must be derived from the same `gross_gmv` and `refund_amount` expressions the output reports — no branch may recompute a metric its own way.<br>P2. Before returning, the job must assert its own output: unique on the declared grain, and `order_date` / `region` never NULL. A silent bad publish is worse than a failed job.<br>P3. Read only the columns this job actually needs from `seller_region`. The config carries operational columns that must not enter the pipeline. | **维度漂移 -> 扇出双计**是全题靶心:`S3` 两条 `valid_to = NULL` 的开区间同时覆盖 06-01 之后的任意日期,attribution join 把 S3 的事实行翻倍,`SUM` 忠实相加。失败形状**在金额之外的每一个维度上都不可见**:行数不变(5 行)、无 NULL、无重复 grain 键,且 `n_orders` **五行全对**——契约要的是 DISTINCT,`countDistinct` 对重复免疫,恰好是人类最会拿来核对行数的那一列。**P2 的三条断言在 trap 下全部通过**,作业自己的质量闸门把错数字发布出去(ref 明写:P2 只按"断言是否存在"评分,永不按"抓没抓到"评分)。放大**不是干净的倍数**:06-14 CENTRAL 组里混着未被复制的 S1 订单 O1/O8,读作 **295.0 vs 235.0(1.26x)**,"有没有哪个数正好翻倍"抓不到;唯一干净翻倍的 06-15 CENTRAL 只有一个订单,没有可比对象。读时 tell 是**同一 seller 同时有两个 `valid_to = NULL`**——一次列扫描的事,且是三个输入框里唯一违反表自身维护规则的东西;**S2 是诱饵**(两行、同 region、仓库交接,但已正确关闭),**O4 是对照行**(同为 S3 但 05-20 只落在单区间内),所以"盯住 trap seller"与"两行就是坏"两条捷径都被堵死。第二条不依赖发现 S3 的通用规则:**右表对 join key 无强制唯一性、且 join 前无 dedup/rank/区间修复 = 扇出**。**用户抓到 trap 但靠跑**(Stage 1 FAIL 报出 295.0/120.0 后反推),非靠读。**用户两处测试数据分辨不出的分歧(实测)**:(a) `refunds` 未预聚合就 join,一单两笔退款时 `rn = 1` **把第二笔退款连同那行一起淘汰**——实测 06-14 CENTRAL refund **40.0 vs 正确 65.0**,而 `gross` 不受影响,症状是"钱变少"而非"钱变多",与该窗口本要修的方向相反;(b) `between` 是**闭区间**而契约要求半开 `valid_from <= d < valid_to`,在"已关闭且无后继"的边界日实测归给已失效的 assignment(**WEST vs 正确 UNKNOWN**),本数据看不出是因为 S2 交接日 `valid_to == 后继 valid_from`,闭区间的多匹配又恰好被 `valid_from DESC` 选回后继,**两个错误互相抵消**。用户另有 **P2 完全缺失**(两版皆无断言)与 `n_orders` **DSL `cast('int')` / SQL `bigint`** 的自相矛盾(契约要 bigint,`check()` 比 Python int 故不可见)。**AI(= 参考 Route A)未踩陷阱**,`partitionBy(order_id)` 一次到位、refunds 先滚到订单粒度、半开区间显式写成 `valid_to IS NULL OR d < valid_to`;三条断言齐全但**重复键断言恒真**(对 `groupBy(a,b)` 的产物再检查 `(a,b)` 唯一),满足 P2 字面要求、检测能力为零。**用户 REVIEW_NOTES 六条里四条裸 LGTM**,唯一的实质观察("AI 在 refunds 上预聚合会多一次 shuffle")**实测推翻**:user-DSL 与 AI-DSL **Exchange 同为 6**,差别在 **Sort 5 vs 7、SortMergeJoin 2 vs 3**;更关键的是**归类错误**——预聚合是 correctness 防线不是 performance 代价,用户已写出 "ensure the grain of refund amount before joining" 却停在"更严谨",没走完"它防的是什么 / 我的代码防了吗",而走完这一步就能在只读阶段发现自己的 (a)。ANSI 栏"cast 保证了 ANSI 合规"**推理错误结论无害**(全题比较均为 string↔string,本栏正确答案是"无可标记项")。VERDICT PASS **结论对、理由不成立**("logics are as same as mine" 在阶段 ② ③ 各有一处真实分歧,且两处都是 AI 对用户错)。GAP ANALYSIS 写 "No" 亦不成立——跑没暴露任何东西,**恰恰因为测试数据无力区分**。PLAN(ref 实测,AQE off):Route A / Route B / naive **三者 Exchange 全为 6**,ref 明写 "There is no shuffle story here, and none should be invented";真实差别是 **Sort 6 vs 4** 以及**排的是 fact 流还是 dim 流**(Route A 的 `Sort [order_id, valid_from DESC, region]` 在事实流上,Route B 的 `Sort [seller_id, valid_from]` 在 6 行维表上)。Route A 靠 `{order_id} ⊆ {order_date,region,order_id}` 白拿 distinct-rewrite 的 Exchange,Route B 靠 join 与 window 同为 `seller_id` 白拿窗口的 Exchange——**分区键子集规则第四次确认**(Day 16/17/18) |
| 24 | L2 detail modeling | logistics — cold-chain warehouse telemetry | Medium-Hard (4 stages) | `fct_device_hour` | P1. Every reading is attributed to the hour of its own `reading_ts`. `ingest_ts` records when the collector received the message and must never determine which hour a reading falls in.<br>P2. Before returning, the job must assert its own output: exactly one row per (device_id, reading_hour), and no NULL in `device_id` / `reading_hour` / `site_id`. A silent bad publish is worse than a failed job.<br>P3. Read only the columns this job needs from the two config tables. `model` and `ticket_id` are operational metadata and must not enter the pipeline. | **重传是一次新的传输**是全题靶心:landing 表 at-least-once,网关在**传输时刻**盖 `event_id`、collector 在**落库时刻**盖 `ingest_ts`,于是 E07 是 E02 的重发却带着全新的信封。能存活重传的身份只有载荷的业务键 `(device_id, reading_ts)`;按 `event_id` 或按整行去重都收不拢,且**必须收在入口**。失败形状是全项目最窄的一次:**整个输出只有一个整数格移动**(D1/09:00 的 `n_readings` 3 而非 2)。grain 由最后的 `groupBy` 构造性保证唯一、无 NULL、`avg` 与 `max` **双双不变**(D1 两条幸存读数都是 22.0,`mean{22,22} = mean{22,22,22}`)——AVG 只在被复制的值偏离组均值时才敏感,这份数据恰好不偏离。3 是一个合理的小时读数数量,输出里**没有任何形状信号**。诱饵是 E06/E06 的逐字节重复对:它被**每一条**去重路线抓到,包括两条错的,于是"重复已处理"这个错觉拿到了可见证据。读时 tell 两条,都只需一次列扫描:(1) 扫 `(device_id, reading_ts)` 找重复,E02/E07 在它上面撞车而信封每一列都不同;(2) E07 是全表**唯一** `ingest_ts` 落在比 `reading_ts` 更晚一个小时的行(09:20 测、10:18 到,58 分钟 vs 其余行 13-17 分钟),P1 正好把读者指向这对列。**AI 未踩陷阱但键选错了一档**:`select(device_id, reading_ts, temp_c).distinct()` 先投影掉信封再整行去重,同时收掉 E06 与 E07——但键里含**度量列**,比 ref Route B 还弱一档:Route B 在同键冲突时任意留一行,AI 的写法**两行都留**,一条订正重发(同 device 同 ts、`temp_c` 已修正)会整个漏过去,`n_readings = 2` 且 `avg` 是两者均值。**用户靠跑抓到 trap,非靠读**(Stage 1 首次 FAIL 报 3/2 后反推),连续第二天重演 Day 23 的同一形状。**用户 Stage 1 的真实缺陷是确定性而非正确性**:`row_number() OVER (PARTITION BY device_id, reading_ts ORDER BY reading_ts ASC)`——**`ORDER BY` 排的就是 partition key**,分区内恒定、全行平局,于是写出了 Route A 的语法、拿到的是 Route B 的保证(任意幸存者 + 重跑不幂等),而 ref 明写 Route A 是三种去重强度里"唯一能说明哪一份副本获胜"的那种。**P2 用户只做了一半**(有 NULL 断言、无 grain 唯一性断言,自己在 notes 里承认);AI 两条齐全。但 ref 实测给了更狠的判断:**完整的 P2 也看不见入口重复**——最后的 `groupBy` 让 grain 构造性唯一,唯一性断言在结构上不可能观察到已被折叠的重复,naive 路线三条断言全过。两侧暴露面**镜像**:用户 registry join 在聚合**后**,registry 若出现同 device 两行 -> 扇出 -> **grain 破坏** -> 恰好缺的就是那条断言;AI join 在聚合**前**,同样输入 -> `n_readings` 翻倍 -> grain 仍唯一、断言全绿。**P3 用户 DSL 违反、SQL 满足**(SQL 的 `fill_null_decommissioned_on` CTE 显式选四列,DSL 全表进 join),同一作业两个分支两种卫生标准;AI 两侧均满足。**但 P3 的症状实测为零**(见 log/04 列裁剪一条),归类应为 production robustness(两分支不一致),不是 performance。用户另有 `n_readings` **`cast('int')` 而契约要 bigint**——与 Day 23 完全相同的偏离,`check()` 比 Python int 故永远绿,**连续第二天**。**用户 REVIEW_NOTES 六行没有一行提到去重步骤**——刚被这一步烧过、AI 在同一步用了不同的键,这是当天最该被审的一级;Correctness 栏的实质观察("AI 聚合后 inner join registry 会不会扇出")方向正确但没走完,答案是 registry 被**声明**为一行一设备、而**没有任何代码强制这个声明**,那正是他自己跳过的断言的职责。Production 栏"site_id 作为 join key 不符合 P2"**两处皆错**:`site_id` 是 `groupBy` 里的函数依赖列不是 join 键,且与 P2(断言约束)无关;同栏"I bypass row count reconciliation"是准确自评。ANSI 栏**结论对理由错**(答"因为 inner join 不产 NULL",正确理由是"全程 string↔string、无 cast/除法/下标",ref 标准答案"无可标记项")。VERDICT PASS 结论对,理由"logic same as mine, differ at groupby step"**绕过了唯一要紧的那一级**——分歧在入口去重,不在 groupBy。PLAN(实测,AQE off,shuffle.partitions=4):**用户 DSL 4 / 用户 SQL 3 / AI DSL 4 / AI SQL 3** Exchange。`left_anti` 与 `NOT EXISTS` 的 Exchange 差在**两份独立编写的实现上同时复现**,SQL 侧把 feed 按 `device_id` **单键** shuffle 一次同时喂窗口与 join(`HashPartitioning(device_id)` 满足 `Window` 的 `ClusteredDistribution(device_id, reading_ts)`)——**分区键子集规则第六次确认**(Day 16/17/18/23)。AI DSL 的节点数与 ref Route B DSL **完全吻合**(Exch 4 / Sort 3 / Window 0 / HashAgg 4 / SMJ 2),`distinct` 与 `dropDuplicates` 同样计划成 partial+final 聚合;用户 DSL 是 4/6/1/2/2。用户与 AI 的真实差别**不是 shuffle 数**(都是 4),是 `Window` + `Sort` 换两个 `HashAggregate`。AI 的 `distinct` shuffle 键**宽一列**(`device_id, reading_ts, temp_c`),是"把 `temp_c` 放进去重键"这个设计选择的物理回声 |
| 25 | L3 serving (aggregation) | ad delivery | Medium-Hard (5 stages) | `agg_campaign_market_daily` | P1. Re-running this job on the same four inputs must produce byte-identical output. The batch is replayed whenever the scheduler retries.<br>P2. Before returning, the job must assert its own output: exactly one row per (campaign_id, market_code, local_date), and no NULL in `campaign_id` / `market_code` / `local_date`. A silent bad publish is worse than a failed job.<br>P3. Read only the columns this job needs from the two config tables. `channel` and `reporting_currency` are operational metadata and must not enter the pipeline. | **时区换算有两个客户,只有一个是响的**是全题靶心:`local_date` 与 flight 窗口同在 market 本地钟上,`event_ts_utc` 在另一口钟上。grain 列**逼着**换算(不换算发不出 `local_date`),stage 3 的 scope 谓词**不逼任何事**,写成 `to_date(event_ts_utc)` 或裸串比较即静默换钟。失败形状是全项目第二窄:**整个输出只有两个格移动**(`(C1,JP,2026-09-10)` 读作 `1 / 8.0` 而非 `2 / 20.0`),行数仍 5、grain 仍唯一、无 NULL、同一行 `n_clicks` 仍对,"1 次曝光 $8" 是完全合理的投放日。设计好的不对称:**过度包含会多出一行被行数抓到,于是那个变体被刻意从数据里拿掉**,唯一存活的症状是"数字变小"。诱饵是 E07(DE 22:30Z→本地 09-11)与 E08(JP 20:00Z→本地 09-11):这两行**确实**跨本地午夜、`local_date` 在输出里**显眼地正确**,给出"时区换算是好的"的可见证据,而它们在两口钟下都落在 flight 内**从未处于风险中**。唯一暴露行是 **E01**(09-09 15:30Z → Tokyo 09-10 00:30),全表唯一"UTC 日期 ≠ 本地日期**且** flight 边界正好夹在两者之间"的行。读时 tell **不需要扫数据巧合**,就在题面点明是两口钟的两列上:A2 说 `local_date` 只由 `event_ts_utc` + `tz_name` 决定,`campaign_flight` 注释说两个 flight 日期在 market 本地钟上,A3 把两者相比——审查问题只有一句:**scope 谓词读的是哪一列?** 裸串变体另有独立毒性:`'2026-09-10 10:00:00' <= '2026-09-10'` 字典序为 **false**(短串更小),它还会静默删掉每个 flight 末日的全部事件,本数据没暴露纯属**运气**。**用户与 AI 双双未踩**——两边 stage 3 都过滤 `local_date`,三方五个阶段唯一分歧不在 trap 阶段。**但用户是写对、不是审出来**:Correctness 栏写的是"我和 AI 逻辑几乎一样",用同构性替代对照契约,一旦双方一起读 `event_ts_utc` 会一起 PASS 掉错解;**连续第四天 REVIEW 未锚在管线阶段上**(22 未触及 trap 轴 / 23 四条裸 LGTM / 24 六行不提去重)。**用户最实质的自身缺陷是 P2 空转**:两版都写 `exactly_once_cnt = agg.groupBy(...).filter(dup_cnt > 1)` 然后 `assert (exactly_once_cnt is not None)`——断言的是一个 DataFrame 对象,**恒为真、连一个 job 都不提交**,而用户在 notes 里判"quality assertions are correct here"。声称存在却不存在的闸门比没有闸门更危险。但 ref 实测给了更狠的判断:**即使写对也抓不到今天的 trap**——grain 由 `groupBy` 键构造性唯一、三键来自 join 不可能为 NULL,错解下两条断言**全绿**,**连续第三天**(23/24/25)作业自己的质量闸门放行错数字;通用规则升级为**断言只能观察活着进入输出的东西,永远看不见半路被丢掉的东西**。P1 两侧均满足,幂等由"**没有引入**不确定性"给出而非某行代码做到(无 `row_number` 撞平局、无无序 `first`/`last`),**只能靠读评分**;用户答"not pretty sure"说明判定动作缺失(过 stage 列表找顺序敏感算子)。P3 用户两版均违反、AI 两版均满足(`pm`/`mc`/`fl` 三个投影),用户自己抓到——但**症状实测为零**,`.explain()` 显示 Catalyst 已在 Scan 之上剪掉 `channel`/`reporting_currency`(`Project [placement_id, market_code]` / `Project [market_code, tz_name]`),归 production robustness 不归 performance(Day 24 同款)。**三方逐阶段:1/2/3/4 语义一致,分歧全在 stage 4–5。** stage 1 用户 `left` vs AI/ref `inner`:**测试数据不可区分**(孤儿 placement 在用户路线下 NULL market → 匹配不上 flight → 被 filter 丢,落点相同,但用户的 NULL 断言因此永远看不到它);stage 2 AI 多一层 `to_timestamp(col, fmt)` 而 ref 直接喂字符串——**纯装饰性**,用户判"冗余"应重归 cosmetic(它把假定的输入格式写进代码,ANSI 下脏格式两者同样抛);stage 4 **真实语义分叉**:用户/ref 用 `count(when(...))`、AI 用 `sum(when(...).otherwise(0))`——`count` 数非 NULL 空组给 **0**,`sum` 空组给 **NULL**,AI 的 `otherwise` **不是冗余防御而是它选 `sum` 之后的必要条件**(去掉即在 `n_impressions=0`/`n_clicks=0` 那两行返 NULL 直接 FAIL),而期望输出里那两行正是为打这一点而设;三方的 `spend_usd` COALESCE(用户 `sum(coalesce(x,0))` / AI `coalesce(sum(x),0.0)` / ref 裸 `sum`)在**任何**输入下等价,前两者均为冗余防御代码。stage 5 用户 **DSL `cast('int')` / SQL `CAST(... AS BIGINT)`**,契约要 bigint,`check()` 比 Python int 故永远绿——**连续第三天**(23/24/25)同款偏离,且今天更差一层:**同一指标在自己两条分支里是两种类型**,归 production robustness。另有 `.drop("campaign_flight.campaign_id", ...)` **静默无操作**(`drop` 不解析限定名),重复列一直留着,靠 `groupBy(df.campaign_id)` 显式指列避开歧义 = style,但属"读的人会以为已处理"那类;AI 侧 `result.cache()` 从不 `unpersist`,缓存生命周期泄漏到函数外 = style。PLAN(实测):**用户 DSL 6 个 shuffle Exchange / 3 SortMergeJoin / 0 BroadcastHashJoin**,**AI DSL 1 个 shuffle Exchange + 3 BroadcastExchange / 3 BroadcastHashJoin / 0 SMJ**——用户"AI 用 broadcast 更好"的主张**成立**,但真正的形状不是单调的 6 对 1:**用户的 partial/final `HashAggregate` 之间没有 Exchange**,因为 stage 3 的 SMJ 已按 `(campaign_id, market_code)` 分区、而它是 groupBy 键 `(campaign_id, market_code, local_date)` 的**子集**——**分区键子集规则第七次确认**(Day 16/17/18/23/24),AI 的广播路线反而必须为最终聚合付一次 shuffle。真实对比是 **"3 次 join shuffle + 0 次聚合 shuffle" vs "0 次 join shuffle + 1 次聚合 shuffle"**;且三张配置表远低于 `autoBroadcastJoinThreshold`,用户侧 `isFinalPlan=false` 未观测到 AQE 最终态,**broadcast hint 的价值是保证性而非提速**。ref 的 Route B(把窗口换算成 UTC 区间下推)本日**未实现**,其半开边界要点仅按概念记录 |
| 26 | L1 staging / cleansing | inventory replenishment | Medium-Hard (4 stages) | `stg_replenishment_request` | P1. Every request in the landing batch must come out of the job exactly once — ACCEPTED or QUARANTINED, never dropped, never duplicated. Before returning, the job must assert it: count(ACCEPTED) + count(QUARANTINED) equals the row count of raw_replenishment_request, and `request_id` is unique in the output.<br>P2. Before returning, the job must also assert its routing is consistent: every ACCEPTED row has a non-NULL `dc_code` and `qty_units` and a NULL `reject_reason`; every QUARANTINED row has a non-NULL `reject_reason` and a NULL `dc_code` and `qty_units`. A silent bad publish is worse than a failed job.<br>P3. Read only the columns this job needs from the three reference tables. `store_name` and `buyer_id` are operational metadata and must not enter the pipeline. | **可空复合键上的假孤儿**是全题靶心:`units_per_case` 按 `(sku, pack_code)` 查,单包装 SKU 的 `pack_code` 在**两侧都是 NULL** 且表示同一件事("只有一种包装"),`NULL = NULL` 为 UNKNOWN,普通等值 join 永远查不到——R03(S03 / SKU-102 / NULL / 3 箱)门店已知、SKU 已知且 ACTIVE、它点名的 pack 行在 `sku_pack` 里存在,查找仍然落空。修法 `eqNullSafe` / `<=>` 或两侧同一个不撞车的哨兵。**R06 是诱饵**:另一条 NULL-pack 请求,已停售,契约在 QUARANTINED 行上本就把 `qty_units` 置 NULL,于是查找落空**不可见**,看起来作业已经扛过了 NULL pack。失败形状随路线变:**X 拆分 + union** -> R03 消失(7 行,P1 抓);**Y 左连接链 `=`** -> ACCEPTED / `qty_units = NULL`(P2 抓);**Y′ 从复合 pack join 推 UNKNOWN_SKU** -> 一致地误路由成 `UNKNOWN_SKU`,**P1/P2 全绿**,只有 `check()` 看得见。读时 tell 分散在题面三处(raw 注释、sku_pack 注释、上游保证),审查问题一句:**pack join 在两侧 NULL 时做什么?** **用户与 AI 双双未踩**(两边都 null-safe),且**用户的 REVIEW 首次靠读点中 trap**(Correctness 栏 "equalNullSafe is implemented on pack_code"),终止 22–25 连续四天的"REVIEW 未锚在 trap 阶段"。**但用户 stage 2 结构是 Y′ 形**:先 `sku_master ⋈ sku_pack` 再按 `(sku, pack <=>)` 查,`UNKNOWN_SKU` 读的是复合 join 的 `master_sku`——把 `<=>` 换成 `==` 就是那条唯一不被任何断言拦截的路线;扰动实测 `SKU-101/CS99`(超契约)下用户两版安静发布 `UNKNOWN_SKU`、AI/ref 因 P2 作业失败。**用户的两条质量闸门全部空转(实测)**:P1 唯一性 `assert unique_req`——对 DataFrame 做真值判断恒为真(`bool(spark.range(0)) == True`,`DataFrame` 无 `__bool__`/`__len__`),**Day 25 `is not None` 同款第二次**,重复 `request_id` 两版均正常返回;P2 写成 `(dc NULL \| qty NULL) & reason NOT NULL`(应为 OR),且 `load_status` 本由 `reject_reason` 推出,reason 子句在过滤谓词下**按构造恒假**,两个 filter 恒为 0——实测 ACCEPTED SKU 无任何 pack 行时两版**发布 `ACCEPTED / qty_units = NULL`**,即 P2 原文要防的 silent bad publish(AI 作业失败报 P2)。用户 P1 计数守恒写对(在入口数 raw),**守恒式断言首次被三方全部写出**(Day 23/24/25 六份实现无一人写过)。AI 另一亮点:`localCheckpoint(eager=True)` 钉住 raw 与 out,`raw_count` / 断言 / 返回值同一快照;用户断言跑在惰性重算上。**`qty_units` 用户 SQL `bigint` vs 契约 `int`**(DSL 对),**连续第四天**(23/24/25/26)类型偏离,且今天方向相反——不是 int 习惯,是**收尾没有对 schema**。REVIEW_NOTES:Performance 栏"不先合并 master 与 pack 会扇出"**两层皆错**(两表在所用键上各自唯一、无扇出;扇出是正确性类不是性能类),ANSI 栏把 `string→string` 的冗余 cast 归 ANSI(应为 style),Production 栏"我漏了 distinct id 检查"判错(写了但空转),且未看出自己 P2 空转。PLAN(实测 AQE 最终态,shuffle.partitions=4):**user-DSL 0 shuffle Exchange / 3 BHJ;user-SQL 4 / 3 BHJ;AI-DSL 初始 6 Exchange + 3 SMJ,最终 4 shuffle Exchange + 3 BroadcastExchange / 3 BHJ**——`Scan ExistingRDD` `sizeInBytes = Long.MaxValue` 静态不广播,AQE 运行时转 BHJ 但 shuffle 已付(raw 一次 + 每张维表各一次再广播)。用户 DSL 显式 hint 省 4 次 shuffle,SQL 分支没写,两分支卫生不一致。P3 三方均显式投影,无可测症状 |
| 27 | L2 detail modeling | mobile app clickstream | Medium-Hard (5 stages) | `fct_app_session` | P1. Re-running this job on the same three inputs must produce byte-identical output. session_id is a pure function of the session's own events — never of run time, row order, or a generated sequence.<br>P2. Before returning, the job must assert its own output: session_id is unique; sum(n_events) equals the number of engagement events in raw_app_event; and session_id / device_id / platform / session_date are never NULL. A silent bad publish is worse than a failed job.<br>P3. Read only the columns this job needs from the two config tables. owner_team and push_token are operational metadata and must not enter the pipeline. | **日历午夜不是会话边界**是全题靶心:会话只由 `> 30 min` 间隔关闭,`session_date` 是会话的**属性**(取 `session_start_ts` 的日期),不是键的一部分。D1 23:48 -> 23:55 -> 00:09(间隔 7/14 分钟)是**一个**会话;任何把**事件级日期** `to_date(event_ts)` 放进 window `partitionBy`、rollup `groupBy` 或逐事件 session_id 的路线都把它切成两段(5 行 vs 4 行,`check()` 抓)。**D2 是诱饵**:23:20 -> 00:05 跨午夜但间隔 45 分钟,本就是两个会话,午夜切法在它上面**恰好正确**,给读者"午夜已处理"的可见证据。**P2 只在一种变体下有牙**(ref 实测):session_id 在 rollup 后由 `min(ts)` 铸造时,两段拿到不同 id,唯一性过、守恒过(2+1=3),**全绿照发**;逐事件先铸 id 再按日期分组时两段撞 id、唯一性触发(ref 推导,未执行)。**守恒抓丢失、唯一性抓重复,两者都抓不到错分段。** **用户与 AI 双双未踩**(两边 partition/groupBy 键都只有 device_id + 会话标签)。**AI 无 trap 呈现,VERDICT PASS 正确,但 Correctness 栏"correct, nothing wrong"未记录检查过键里有没有日期**。用户侧实测缺陷全在 stage 5:(B1)**空批次 runtime break**——`.first()` 返回 `None` 再取下标 `TypeError`,两分支;(B2)**唯一性闸门空转**——`groupBy(session_id).count().first()['uni_cnt'] == 1` 只看**任意一组**,实测 ids `['A','B','B']` -> 1 通过,Day 25/26 空转闸门的第三种形态;(B3)**守恒分母与输出共用扇出**——`event_catalog` 是手工配置、契约未声明 event_name 唯一,重复一行时 left join 扇出,`n_events` 翻倍,而守恒基线数的是同一个扇出后的 `true_events`,**实测静默发布 n_events=2**;ref 的 inner join 同病,**只有 AI 的 `distinct` + `left_semi` 免疫**;(B4)`n_events` `int` vs 契约 `bigint`,**连续第五天**类型偏离;(B5)SQL 分支最终 join 与守恒断言直接读整张 `device_registry` / `event_catalog`(P3 卫生,Catalyst 已剪列,无计划症状)。AI 侧:无 hint,AQE 最终态 **4 shuffle Exchange vs 用户 1**(Day 26 结论第二次实测);`to_timestamp`+`unix_timestamp` 依赖 session TZ = UTC,实测 LA 会话时区下跨 DST 回拨把 20 分钟间隔算成 80 分钟、切成两段(ref 同路线同病,harness 钉 UTC 测不到);用户 `timestamp_diff` 作用在字符串上取的是会话时区墙钟差,**意外正确**。AI 闸门用 `raise RuntimeError` 而非 `assert`,`python -O` 下仍生效;用户与 ref 的 `assert` 在 `-O` 下全部消失。REVIEW_NOTES:Performance 栏"no"与 VERDICT 理由"AI did not broadcast"自相矛盾(发现本身对、实测 4 vs 1,应归 Performance);Production 栏"LGTM"未逐条走 P1/P2/P3;Robustness 栏"我漏了 prev_ts NULL 过滤"**判错**(首行 `when(NULL > 30)` 落 `otherwise(0)`,标签从 0 起,分组不变,非 bug) |
| 28 | L4 incremental | ride-hailing driver payout | Medium-Hard (5 stages) | `trip_payout_resolution` | P1. Every candidate trip comes out of the job exactly once — PUBLISHED or PENDING, never dropped, never duplicated. Before returning, the job must assert it: the output row count equals count(raw_trip) + count(pending_trip), and trip_id is unique in the output.<br>P2. A payout is never published incomplete. Before returning, the job must assert that driver_earning_cents is non-NULL on every PUBLISHED row and NULL on every PENDING row. A silent bad publish is worse than a failed job.<br>P3. A night with no completed trips is a normal night. With raw_trip empty the job must still succeed — every assertion included — and return exactly the carried pending trips' rows; with raw_trip and pending_trip both empty it must return 0 rows. | **孤儿在查找链的第二跳,不在第一跳**是全题靶心:能否付款由 driver → fleet → commission 的**最后一跳**决定。T103(D3 → `F9`,`fleet_commission` 无此行 = 真孤儿)与 T105(D5 → `fleet_id` NULL = 未分配)**司机都在 `dim_driver` 里**,却都不可付,必须 PENDING / earning NULL。题面的业务背景与 dim 注释全在讲"晚到的**司机**"(D9 当天入职、D4 昨天入职今天进快照),于是按这个故事写的门控落在司机那一跳(`d.driver_id IS NULL -> PENDING` 或 `left_anti dim_driver`);干净数据上"司机找到 ⇒ 可付"碰巧成立,本数据 8 行里对 6 行,**T090 是晚到维度已到达的那一行,它反而奖励司机跳的心智模型**。ref 实测四种 naive 变体:司机跳门控 -> T103/T105 PUBLISHED 且 earning NULL(**P2 抓**);inner join 发布 + `left_anti dim_driver` 待定 -> T103/T105 两边都不在,6 行(**P1 抓**);`fleet_id NOT IN (SELECT fleet_id ...)` 待定 -> **5 行**,T105/T104/T085 全没(**P1 抓**)——外层列 NULL 是作业**自己的 left join 制造的**(T104/T085 无司机行),子查询侧干净,`NOT IN` 三值逻辑的**外层那一半**首次实测,计划为 `BroadcastHashJoin ... LeftAnti, BuildRight, true`(null-aware anti join);`COALESCE(commission_pct, 0)` -> 付全额(**P2 抓**)。**本日 P1/P2 都是真闸门**,每个 naive 变体都触发其一。读时 tell:**决定 `load_status` 的谓词测的是哪一列?** 正确门控测的是 payout 由之计算的那一列(`commission_pct`);测 `d.driver_id` / `fleet_id` / 对维表做 anti-join 都是测中间跳。拆分路线的第二个 tell:PENDING 必须是 PUBLISHED 的**补集**,即在 **`trip_id` 上对已发布集合做 anti-join**,而不是对某张维表。**用户与 AI 双双未踩**:用户门控 `f.fleet_id IS NOT NULL`,AI 门控 `d_driver_id 非空 AND c_fleet_id 非空`,都测到了第二跳;但**都不是 ref 的 `commission_pct IS NOT NULL`**,在测试数据不可区分的输入上分歧(实测):`fleet_commission` 有一行 `('F9', NULL)`(契约只保证 fleet_id 非 NULL)时,用户与 AI 都把 T103 判为 PUBLISHED / earning NULL——**AI 的 P2 报错停下,用户两版静默发布**。**用户的闸门连续第四天空转,且这次是自己的代码**:P2 写成 `sum(when(bad,1).otherwise(0))` 先 filter 再聚合,然后 `assert a is not None or b is not None`——(a) `is not None` 只是存在性测试,违规数为 3 时也通过,**P2 从不触发**(实测静默发布);(b) filter 之后集合为空时 `sum` 返回 NULL,`otherwise(0)` 只管每一行的取值,管不到零行;两表都空时两个值都是 None,**断言失败,P3 runtime break**(实测两分支)。用户自己追问"`otherwise(0)` 了还会 NULL 吗"——这正是逐行表达式和聚合在空集上的区别。只改成 `== 0` 也不够:raw 为空、carried 只剩 PENDING 时 PUBLISHED 集合为空 -> `None == 0` 为 False -> 照样崩溃。P1 用户只写了行数守恒,**没有唯一性检查**(P1 明文要求);`count('trip_id')` 而非 `count(*)`。AI 三条 P# 全部满足(全局 `agg` + `count(when)` + 显式 `raise`,与 ref 的 `_assert_resolution` 几乎同构),P3a/P3b/P3c 实测全过。**逐阶段:1/2/3 三方一致,分歧在 stage 4 门控列(测试数据不可区分)与 stage 5 断言(真实语义分歧)。**其余测试数据不可区分项:用户 `/ 100` 得 double 再 cast vs AI/ref `DIV`(除不尽时截断碰巧等价,大数丢精度);trip_date 用户 `date_format(string)` 隐式 cast / AI `to_date(fmt)` / ref `substring`,只在格式脏时分歧。**类型偏离连续五天后首次中断**(`driver_earning_cents` bigint,两分支一致)。REVIEW_NOTES:Correctness"everything is ok"结论对、没写 T103/T105 走哪个分支;API 栏"to_date + date_format 冗余"只到 style;ANSI 栏"不确定 DIV 能否防 0 除数"**不算 finding**(除数是字面量 100,唯一的 ANSI 风险是 bigint 溢出);Production 栏 LGTM 未逐条走 P#,**错过了把 AI 的 `count(when)` 与自己的 `sum(when)` 并排比的机会**;Performance 栏 LGTM 漏了无 broadcast;Robustness 漏 AI-SQL 残留的 session 全局 temp view(style)。PLAN(实测 AQE 初始计划):**user-DSL 2 shuffle Exchange + 1 BroadcastExchange**(只给 `fleet_commission` 加 hint)/ **user-SQL 4 / AI-DSL 4 / AI-SQL 4**(两个 SMJ LeftOuter,0 broadcast)/ ref Route A **0 + 2 BroadcastExchange**(hint)。差别来自 hint,不是写法(`Scan ExistingRDD` 无统计信息,Day 26/27 结论第三次)。ref Route B(拆分 + `trip_id` 补集 anti-join)**4 Exchange / 7 BroadcastExchange**,已发布子树出现 3 次 |

## 调度矩阵(已用组合)

> `/newday` 读这张表来避免重复三轴组合。failure mode **只**记在这里和
> `refs/*_ref.md`,**永远不进 day 文件**。

| Day | ETL layer | Domain | Failure mode |
|-----|-----------|--------|--------------|
| 22 | L4 incremental | subscription billing | late data |
| 23 | L3 serving | e-commerce (marketplace) orders | fan-out double counting |
| 24 | L2 detail modeling | logistics — cold-chain warehouse telemetry | replay |
| 25 | L3 serving (aggregation) | ad delivery | timezone attribution |
| 26 | L1 staging / cleansing | inventory replenishment | nullable key |
| 27 | L2 detail modeling | mobile app clickstream | session boundary (boundary closure) |
| 28 | L4 incremental | ride-hailing driver payout | orphan keys (second-hop orphan + `NOT IN` outer-side NULL) |

> **层级重叠标注(Day 25)**:Day 25 的层级与 **Day 23 同为 L3 serving**
> (`refs/day25_*_ref.md` 头部写作 `L3 serving (aggregation)`),业务域与失败模式
> 两轴同时换新,按本表既有规则允许(同 backlog 里 "L4 incremental / inventory /
> orphan keys" 那条的判断)。**`L1 staging / cleansing` 至今完全未用过**,
> 是下一格最干净的选择。
>
> **轴标签与计划不符(Day 26)**:NEXT 计划写的是 `orphan keys`,ref 最终落在
> **`nullable key`**(假孤儿:每个键都合法,是比较失败)。`orphan keys` 轴
> **只被消耗了一半**——"孤儿该丢弃还是隔离"的契约问题已借 ACCEPTED/QUARANTINED
> 分流实操;**真孤儿**(键确实不存在)与 `NOT IN` 三值逻辑未被打到,ref 只作为
> Route X 的兄弟形态记录、未实测。至此 L1/L2/L4 各用一次、L3 两次。
>
> **层级重叠标注(Day 27)**:L2 第二次使用(Day 24 之后),业务域与失败模式两轴
> 同时换新,按既有规则允许。至此 L1 ×1 / L2 ×2 / L3 ×2 / L4 ×1;**L4 incremental
> 与 L1 staging 是最少用的两层**。`orphan keys` 轴仍只消耗一半(见 Day 26 标注)。
>
> **层级重叠标注(Day 28)**:L4 第二次使用(Day 22 之后),业务域与失败模式两轴
> 同时换新,按既有规则允许。**`orphan keys` 轴至此关闭**:Day 26 打了假孤儿(比较
> 失败),Day 28 打了真孤儿 + `NOT IN` 外层 NULL(ref 实测)。至此 L1 ×1 / L2 ×2 /
> L3 ×2 / L4 ×2;**L1 staging 是唯一只用过一次的层**。

## Supplemental drills completed
- Pivot mini-drills x5 (script form, no class): explicit values list,
  schema drift without list, values-list-as-filter, multi-agg column
  naming, pivot -> unpivot round trip (stack / unpivot).

## Scheduled next (planned cadence, Medium / Medium-Hard)
### 模式切换(2026-08-18 记录,追溯 2026-08-15 的分支合并)
**Day 21 从未生成。** Day 22 起进入 ETL scenario 模式(见 CLAUDE.md
"Problem mode — Day 22 onward"),下面这段 Day-21 计划**整体 SUPERSEDED**,
保留作为决策记录:

> ~~先行 drill(NEXT IN QUEUE,先于 Day 21):unpivot / melt mini-drills
> (`stack` / `DataFrame.unpivot`,无 trap、无 review 闭环)。~~
> ~~Day 21 — candidate: unpivot / melt 作为一等主题(宽表转长表:列名解析成
> 维度、NULL 值列的丢弃语义、多度量组)。Medium。前置条件是上面那个 drill。~~
> ~~备选:session_window 内置函数 vs Day 13 手搓的 lag+running-sum 会话化。~~
>
> 作废理由(两条,均在 Day 19/20 的行里有实证):剩余 backlog 已漂向
> **低生产价值的 API**;且 Day 19 与 Day 20 的 trap 维度**双双失效**——
> Stage 1 退化成 API 教学,陷阱必须讲破才能完成 Stage 1。单技术日的形式
> 本身是这两个失败的共因,故整体退役。**那个 unpivot drill 也未执行。**

### DONE(Day 23)
- Day 23 已完成:L3 serving / marketplace orders / fan-out double counting,
  4 stages Medium-Hard。三条选型理由**全部兑现**:三轴与 Day 22 零重叠;
  失败形状确实"行数对、无 NULL、金额偏大且看起来合理";Stage 1 未出现任何
  现学 API 的情况(Day 19/20 的失效模式未重演)。
  **但 trap 仍是靠跑抓到的,不是靠读**——读时 tell(同一 seller 同时有两个
  `valid_to = NULL`)在 Stage 1 全程未被触发。

### DONE(Day 24)
- Day 24 已完成:L2 detail modeling / cold-chain telemetry / replay,
  4 stages Medium-Hard。三条选型理由**全部兑现**:三轴与 Day 22、Day 23 零重叠;
  重复确实来自**数据源本身**而非 join 制造,收敛点成功从"join 右侧"移到
  "读入之后的第一步";Stage 1 未出现任何现学 API 的情况。
- **但 trap 连续第二天靠跑抓到,不是靠读。** Day 23 的读时 tell(同一 seller
  同时有两个 `valid_to = NULL`)当时未触发,Day 24 的两条 tell
  (`(device_id, reading_ts)` 撞车、E07 的 `ingest_ts` 跨小时滞后)同样未触发。
  **这已经是一个稳定模式,不是偶发。**
- **更值得记的是 REVIEW 侧连续三天的同形失效**:Day 22"REVIEW 未触及 trap 轴"、
  Day 23"六条里四条裸 LGTM"、Day 24"六行没有一行提到去重步骤"。三天的共同形状是
  **审查注意力锚在自己 Stage 1 挣扎过的地方,而不是锚在管线阶段上**——Day 24
  尤其刺眼:用户当天刚在入口去重上 FAIL 过一次,而 AI 恰好在同一级用了不同的键,
  那一行仍然没有被写下来。
- **连续第二天的 `cast('int')` vs 契约 `bigint`**(Day 23 同款),`check()` 比的是
  Python int 故永远看不见。这条已经不是"发现",是**习惯**。

### DONE(Day 25)
- Day 25 已完成:L3 serving (aggregation) / ad delivery / timezone attribution,
  5 stages Medium-Hard。三条选型理由**全部兑现**:业务域与失败模式两轴与 22/23/24
  零重叠(层级与 Day 23 同为 L3,见调度矩阵下的标注);Day 24 刻意回避的日期解析
  在本日成为一等主题而无需重学 Day 11 的 primitive;Stage 1 未出现任何现学 API。
- **本日的额外设计要求已达成,而且这是四天来第一个可验证的正面结果**:读时 tell
  被要求落在题面已点明的两列上(A2 / `campaign_flight` 注释 / A3),不再依赖主动
  扫描数据巧合。结果:**用户与 AI 双双未踩陷阱**,Day 23/24 那种"靠 Stage 1 FAIL
  反推 trap"的模式**首次没有重演**。设计干预有效,保留这条要求。
- **但 REVIEW 侧的连续失效进入第四天,且形状变了**:22 "未触及 trap 轴"、
  23 "六条里四条裸 LGTM"、24 "六行不提去重",25 是**用同构性替代对照契约**
  ——Correctness 栏写"我和 AI 逻辑几乎一样"。这一条比前三天更危险:前三天是
  注意力放错位置,这一条是**推理形式本身不成立**(双方一起错时它一起 PASS)。
  对应的落地问题只有一句、且当天题面已经给出:**scope 谓词读的是哪一列?**
- **P2 连续第三天(23/24/25)放行错数字**,而本日多了一层新东西:用户的去重断言
  是 `assert (DataFrame is not None)`,**恒真、从不提交 job**。前两天是"断言写对了
  但结构上抓不到",这一天是"断言根本没有执行"。ref 的通用规则已升级为
  **断言只能观察活着进入输出的东西,永远看不见半路被丢掉的东西**;
  **行数/输入守恒式断言是另一类检查,六份实现至今无一人写过**(跨 23/24/25)。
- **`cast('int')` vs 契约 `bigint` 连续第三天**(23/24/25),且本日升级为
  **同一指标在自己 DSL / SQL 两条分支里两种类型**。这已经不是"发现",是习惯,
  按 ref 的要求本日**显式记分**。
- **P3 的"症状为零"第二次实测确认**(Day 24 同款):Catalyst 的 ColumnPruning
  替你做完了投影,归 production robustness,不归 performance。

### DONE(Day 26)
- Day 26 已完成:L1 staging / cleansing / inventory replenishment / nullable key,
  4 stages Medium-Hard。选型理由 (1)(3) 兑现:三轴与 22–25 零重叠,Stage 1 未现学
  任何 API。理由 (2) **兑现但形状不同于预想**:守恒式断言**首次被三方全部写出**,
  ref 实测它能抓 Route X;但用户的唯一性断言与 P2 双双空转,且 ref 证明 Route Y′
  能骗过一切作业内断言。
- **REVIEW 首次靠读点中 trap**,连续四天的失效链在本日断开。设计归因:读时 tell
  落在题面明说的三处,不需要扫数据巧合(Day 25 引入的设计要求第二次奏效)。
- **新的稳定模式是"闸门写了但不生效"**:Day 25 `is not None`,Day 26 裸
  `assert df` + P2 的 AND / 恒假子句。连续两天用户的质量闸门**看起来存在、实际
  不执行**。下一天 Stage 1 自检应加一个动作:**故意喂一条违规行,看断言是否触发。**
- 类型偏离连续第四天(契约 `int`,SQL 写 `bigint`),方向与前三天相反。

(以下为 Day 26 的原计划,保留作决策记录)

- **Day 26 — L1 staging / cleansing / inventory replenishment /
  failure mode: orphan keys(含 `NOT IN` 三值逻辑 + 可空 join 键)。
  4 stages -> Medium-Hard。** 三条理由:(1) **三轴全新**——`L1 staging / cleansing`
  是四层里**唯一完全没用过**的一层(22 L4 / 23、25 L3 / 24 L2),彻底解掉 Day 25
  与 Day 23 的层级重叠;`inventory` 与 `orphan keys` 均未用过;(2) **它是唯一能给
  P2 装上牙齿的失败模式**——Day 23/24/25 连续三天的定论是"形状断言看不见入口级
  bug",而行数/输入守恒式断言六份实现至今无一人写过。孤儿键的失败形状天生是
  "行数对不上",所以把 P2 写成 **"作业必须断言输入守恒:每条入库事实要么出现在
  输出里、要么出现在被显式隔离的 reject 集里,两者行数之和必须等于输入行数"**,
  这类检查第一次成为必须写、且**能真正抓到东西**的代码,而不是第四天重复
  "断言全绿、错数字照发";(3) **白名单内、零现学 API**——`left_anti` /
  `NOT IN` 三值逻辑 / 可空键 `<=>` vs 哨兵 / `broadcast` 全部练过
  (Day 9、16、22、23、24),Day 23 的 ref 只消耗了"effective-dated 孤儿是
  fact-side key"这一个角度,**"孤儿该被丢弃还是被隔离"这个契约问题完全未碰**。
- stage 数 4(读入并规范化 -> 对齐参照表并分流孤儿 -> 隔离集 reject 归因 ->
  守恒断言与发布)。**不建议推到 6 stages**:本日的价值集中在那条守恒断言上,
  加长会稀释它。
- 后续候选(尚未排期):
  L4 incremental / inventory / **orphan keys**(层级与 Day 22 相同,
  业务域与失败模式已同时换,允许)——但注意 Day 23 的 ref 已把
  "effective-dated join 的孤儿是 fact-side key,`left_anti` 找不到同一批
  订单"写进概念要点,该轴的新鲜度已被部分消耗。
### DONE(Day 27)
- Day 27 已完成:L2 detail modeling / mobile app clickstream / session boundary,
  5 stages Medium-Hard。选型理由 (1)(3) 兑现:三轴与最近两天零重叠,Stage 1 未现学
  API(Stage 1 唯一卡点是 `to_timestamp` 的 format 方向——解析 vs 格式化,已在
  提示下自行修正)。理由 (2) 兑现:Day 13 的 lag + running sum 首次进入完整作业,
  双方都没把日期放进键。
- 上一日附加的设计要求"**至少一条 P# 在 naive 路线下能被真正触发**"**兑现**:
  inner join 丢 D3 -> 守恒 6 ≠ 7 触发(ref)。但 ref 同时证明最常见的 trap 变体
  在 P2 下**全绿**。
- **"闸门写了但不生效"连续第三天**(25 `is not None` / 26 裸 `assert df` + 恒假
  谓词 / 27 `.first()` 抽样一组 + 空批次崩溃)。Day 26 定下的自检动作"**故意喂一条
  违规行看断言是否触发**"本日**仍未执行**——执行了 B2 当场暴露。
- 类型偏离连续第五天(契约 `bigint`,用户 `int`)。

(以下为 Day 27 的原计划,保留作决策记录)

- **Day 27 — L2 detail modeling / mobile app clickstream / failure mode:
  session boundary(gap 阈值切会话 + 跨午夜会话的日期归属)。5 stages ->
  Medium-Hard。** 三条理由:(1) **业务域与失败模式全新**,L2 只在 Day 24 用过
  一次,层级与最近两天(L3/L1)不重叠;(2) **面试最高频的一类**——会话化在 Day 13
  只作为单技术练过(`lag` + running sum),从未放进 ETL 作业;"会话归开始日还是
  结束日"是事件日期归属,与 Day 25 的时区轴是不同的失败轴;(3) **白名单内、零现学
  API**:`lag` / running sum / gap 阈值切分 / 区间闭合 / 事件日期归属全部练过。
- 建议延续本日观察加一条设计要求:**至少一条 P# 断言在 naive 路线下能被真正
  触发**,用来检验 Day 25/26 连续两天的"闸门空转"是否已被修正。
### DONE(Day 28)
- Day 28 已完成:L4 incremental / ride-hailing driver payout / orphan keys,
  5 stages Medium-Hard。三条选型理由全部兑现:L4 第二次使用、业务域新;真孤儿与
  `NOT IN` 外层 NULL 由 ref 实测(5/8 行);Stage 1 未现学 API。两条延续的设计要求
  **均兑现**:P1/P2 在每个 naive 变体下都会真正触发;空批次写成公开的 P3 后,
  **当场抓到了用户的 runtime break**(两表都空 -> `sum` NULL -> 断言失败)。
- **"闸门写了但不生效"连续第四天**(25 `is not None` / 26 裸 `assert df` + 恒假谓词 /
  27 `.first()` 抽样 / 28 `sum` + `is not None`)。本日形态:被比较的对象终于是
  Python 标量(满足 Day 26 判据),但**比较的是"有没有值"而不是"是不是 0"**。
  Day 26 定下的"故意喂一条违规行"自检**连续第三天未执行**——执行了 P2 当场暴露。
- 类型偏离连续五天后**首次中断**。
- REVIEW 侧:VERDICT 对,但 Production 栏连续第二天裸 LGTM,未逐条走 P#。

(以下为 Day 28 的原计划,保留作决策记录)

- **Day 28 — L4 incremental / ride-hailing trips / failure mode: orphan keys
  (late-arriving dimension:事实先于维度行到达;真孤儿 + `NOT IN` 三值逻辑)。
  5 stages -> Medium-Hard。** 三条理由:(1) **L4 是最少用的层**(仅 Day 22),
  业务域全新;(2) **`orphan keys` 轴 Day 26 只消耗了一半**——"假孤儿"(比较失败)
  已打,**真孤儿**(键此刻确实不存在、下一批才到)与 `NOT IN` 遇 NULL 整体塌空
  从未实测;"晚到维度该丢弃、挂 UNKNOWN 占位还是隔离待重放"是增量作业的标准生产
  问题,天然落在 L4;(3) **白名单内、零现学 API**:`left_anti` / `NOT IN` /
  `<=>` / `broadcast` / upsert 全部练过(Day 9、16、22、26)。
- 延续两条设计要求:**至少一条 P# 在 naive 路线下能被真正触发**;并把
  "**作业必须在空 landing 批次上成功返回 0 行**"写成公开 P#——Day 27 B1 证明
  空批次是真实的 runtime break 面,且它是契约问题、不是 trap,公开写出不泄题。

### NEXT(Day 29)
- **Day 29 — L1 staging / cleansing / food-delivery orders(嵌套 JSON 落地)/
  failure mode: nested-array flattening(`array<struct>` 行项:空数组 vs NULL 数组
  vs 缺字段,`explode` vs `explode_outer`,展开后的 grain 与金额守恒)。4 stages ->
  Medium-Hard。** 三条理由:(1) **L1 是唯一只用过一次的层**,业务域与失败模式全新;
  (2) `explode_outer` / `filter` / `size` / `aggregate` / 点路径访问都在白名单里
  (Day 19 实测过空集合单位元),但**从未进过完整作业**;展开层天然带着"订单数守恒"
  与"金额在头表与行项之间对账"两类断言,延续 P# 必须有牙的要求;(3) 零现学 API。
- 延续设计要求:至少一条 P# 在 naive 路线下真正触发;**题面 P# 里明写"空批次 /
  空数组订单"是正常输入**(Day 28 证明公开写出的空输入约束能抓到真实缺陷)。

- Later candidates (pre-switch list, kept for reference): MERGE INTO /
  Delta-style upsert against an existing dim table — **CLOSED by Day 22**;
  the remaining array/map HOF family (zip_with / transform_keys /
  transform_values / map_filter) as a Day-19 sequel;
  date/time round 4 — DST-crossing tz math (a full Day-15-shaped script exists
  and was RETIRED before Stage 1: judged low interview probability, kept as an
  optional 15-minute drill) or session_window built-in as the primary topic.
- **DROPPED (2026-08-08, user decision): `pandas_udf` / ArrowEvalPython.** Not
  to be scheduled on Day 20 or any later day. Day 14's python-UDF half stands;
  the vectorized half is retired unpracticed.

Cadence principle: alternate the two topics, step difficulty upward, each
problem echoes >=1 logged takeaway.
**任何当日主题所需的核心 API,若从未实操过,先出一个 supplemental drill
(无 trap、无 review 闭环、只跑通签名与基本语义),drill 之后再进正式日。**
否则 Stage 1 会退化成 API 教学,trap 必须被讲破才能完成 Stage 1,
trap 维度不产生有效信号(Day 20 的实际结果;Day 19 是部分版本——
`transform` 在 Day 14 记过,属"已知 API + 未知边界",陷阱仍在 Stage 1 丢失)。
判据不是"这个主题做过没有"(backlog 里的条目按定义全都没做过),而是
**Stage 1 是否需要现学一个函数是干什么的**。
**Day 22 起这条判据由技术白名单直接承担**:ETL scenario 日只用已练过的技术
组合,所以"要不要先出 drill"这个问题原则上不再出现。难度改由 **pipeline
stage 数**决定(3 = Medium,4–5 = Medium-Hard,6+ = Hard),不再走
Easy/Medium/Medium-Hard 的交替节奏。

## Backlog (rotate, alternate difficulty, avoid recent repeats)
- Complex aggregation: multiple grains in one pass, grouping sets /
  rollup / cube   <- Day 10 + Day 12 DONE (base + advanced/precise-control)
- UDFs: python UDF vs pandas_udf, when to avoid, cost model   <- Day 14 DONE
  for the python-UDF half (returnType, staticmethod + pickle scope,
  spark.udf.register shapes, BatchEvalPython as optimizer barrier);
  pandas_udf / ArrowEvalPython **DROPPED** — user decision 2026-08-08,
  never to be scheduled
- Higher-order array functions as the PRIMARY topic (transform / filter /
  aggregate / exists / forall)   <- **Day 19 DONE**(空集合单位元、aggregate
  zero 定类型、`x["f"]` vs `x.f`、HOF 路线 0 Exchange vs explode 路线 1–2、
  表达式内联不被 CSE);zip_with / transform_keys / transform_values /
  map_filter 家族仍未实操
- ANSI mode & the try_* family (try_element_at / try_cast / try_divide):
  failure-vs-NULL semantics as a first-class topic   <- surfaced Day 14;
  Day 19 confirmed the HOF core has ZERO ANSI exposure, so still 100% open
  <- **Day 20 DONE**(string→INT 与 numeric→INT 是两个 parser;try_to_number
  的定长掩码语言与 2^k 可选元素问题;try_cast 把响的失败变成静默的失败;
  DSL 里 format 参数 ColumnOrName vs str 两套不统一的约定;EvalMode.TRY
  在 explain 里不可见)。`try_element_at` / `try_add` / `try_avg` /
  `try_parse_url` 等家族其余成员仅做过对照实测,未在题目里实操
- String processing: regexp_extract, split, sentence-level parsing
  <- **部分关闭(Day 20)**:`regexp_replace` + 字符类去噪、SQL 字符串里的
  两层反斜杠转义(`'\$'` 静默失效 / `'\\$'` 才对 / `[$,]` 两层都绕开)已实操;
  `regexp_extract` 只在 AI 解法里出现过,`split` / 句级解析仍 OPEN
- Unpivot / melt as the primary topic (only touched in drills)
  <- **RETIRED**(2026-08-18,随单技术日模式一起退役;那个前置 drill 也未执行,
  不再作为正式日主题)
- Date/time deep dive: timezones, timestamps, truncation, ranges,
  calendar join against a date dimension   <- Day 11 DONE (UTC->local
  bucketing + date-dim spine gap-fill); Day 13 DONE (gap-threshold
  sessionization via lag+running-sum); DST-crossing variant + session_window
  built-in still open (Day 15 candidate)
  <- **Day 25 DONE(serving 层复用)**:UTC->local 分桶搬到 L3 聚合作业上,新增
  Day 11 未覆盖的一条——**本地日历日是一个半开 UTC 区间** `[local 00:00,
  local+1d 00:00)`,宽度为 24h 仅当区间内无 DST 跳变;把本地日过滤下推到 UTC
  需要 `to_utc_timestamp(end_date + 1 day)` 配严格 `<`,**闭区间的本地 `<=` 末端
  没有闭区间的 UTC 等价物**。该下推路线(ref Route B)**本日未实现**,只按概念
  记录,`to_utc_timestamp` 的实操仍 OPEN。DST-crossing 变体仍 OPEN
  <- **Day 27 DONE(L2 复用 Day 13 会话化)**:新增"**日期是会话的属性,不是键**"
  (跨午夜短间隔会话)。DST 变体**部分关闭(实测,非题目)**:`timestampdiff`
  取会话时区**墙钟差**,`unix_timestamp` 相减取**真实流逝**——两者在 DST 跳变日
  相差 1h,方向随回拨/前拨相反。`session_window` 内置仍为黑名单
- Null semantics special: null-safe equality (<=>), null in joins,
  null ordering in windows   <- Day 16 DONE (Stage 1 + digest; AI review 未做,已结)
  <- **Day 26 DONE(staging 层复用)**:可空复合查找键 `(sku, pack_code)`,NULL 在
  两侧表示同一个值,用户与 AI 均用 `<=>`。新增一条:**哨兵撞车点可以由作业自己的
  清洗步骤制造**(`trim` 把 `'  '` 变成 `''`)
  <- **Day 28**:`NOT IN` 外层 NULL 那一半实测(见 Semi/anti 行)
- Window frames as the PRIMARY topic (ROWS vs RANGE, rangeBetween units,
  frame-as-lookup, frame-sensitive vs positional functions)   <- Day 15 DONE
  (theory had been logged since Day 13 but never drilled)
- Incremental patterns: SCD2-style merge logic in PySpark   <- Day 17 DONE
  (run-based versioning + half-open interval closing; MERGE INTO / Delta-style
  upsert against an existing dim table still OPEN — this day built history from
  a feed, it never merged into a target)
  <- **Day 22 DONE**(CDC 批次 merge 进已有维表:staleness guard 的两种形态
  ——显式谓词 vs 目标行进排序竞争;墓碑必须从 upsert 集与幸存目标集**两处**
  移除;"system of record 是契约问题不是数据问题")。**Delta 的 `MERGE INTO`
  语句本身仍未实操**——本项目是内存 DataFrame,不模拟真实 parquet 分区目录
  <- **Day 23 部分复用**:`lead(valid_from)` 的区间闭合 idiom 被搬到**维表**上
  (ref Route B),并补上了 Day 17 没覆盖的一条——必须写
  `LEAST(COALESCE(valid_to, OPEN), COALESCE(lead(valid_from), OPEN))` 而不是
  裸 `LEAD` 直接替换,否则一个**已被正确关闭**的区间会被静默延展跨过缺口。
  该 `LEAST` 在 Day 23 的数据上是死代码、在生产上承重
  <- **Day 28**:晚到维度的 **park-and-replay** 模式(pending 集合回灌,守恒对
  `pending ∪ new` 而不是对 landing 批次);与 Day 27 的 UNKNOWN 占位是两种标准答案,
  选哪种是契约决定
- Skew handling: salting, AQE skew join   <- Day 18 DONE (salted join +
  two-phase agg + decomposability; AQE skew join 只读到配置与两道触发门槛,
  14 行数据上无法触发 OptimizeSkewedJoin —— 真实倾斜数据上的 AQE 行为仍 OPEN)
- Semi/anti joins: left_semi, left_anti as filter idioms   <- DONE (Day 9)
  <- **Day 23 限定**:生效期维表上的"孤儿"是**没有覆盖该日期的行**,不是
  "没有行"。`left_anti` on `seller_id` 与 join 后 `region IS NULL` **识别的
  不是同一批订单**(S4 有配置行,只是不覆盖六月),Day 9 的 anti-join idiom
  在 effective-dated join 上不成立
  <- **Day 24 对照**:排除类 anti-join 的**平凡正确**形态。与 Day 23 的
  effective-dated 情形相反,这里"设备有几张维护单"与"该读数是否落在某张单的
  区间内"是两个问题,`left_anti` / `NOT EXISTS` 由构造保证不扇出,而
  `left join` + `IS NULL` 只在右表对 join key 唯一时等价——一台设备可以有
  任意多张工单,`MT-103`(D4,无读数)就是专门用来打死 inner-join 写法的对照行
  <- **Day 25 无新增**:全题四个 join 均为等值 inner/left,反连接未出现
  <- **Day 26 无实操**:用户与 AI 都走左连接链 + CASE;ref 把"拆分 + `left_anti`
  隔离 + union"记为 Route X(漏查行直接消失,P1 抓),并点名 `NOT IN` 为产生同样
  "行消失"形状的兄弟算子,均**未实测**
  <- **Day 27 AI 实操 `left_semi` 做配置过滤**:`distinct` + `left_semi` 使
  "手工配置表出现重复 event_name"时**不扇出**(实测 n_events 正确),用户
  left join + filter 与 ref inner join 均翻倍。semi join 作为"**存在性过滤不受右表
  重数影响**"的防扇出工具,Day 9 的 idiom 在配置表上第一次承重
  <- **Day 28 ref 实测**:`NOT IN` 外层列 NULL(由作业自己的 left join 制造)-> 5/8 行,
  null-aware anti join `BroadcastHashJoin ... LeftAnti, BuildRight, true`,无 hint 也走
  broadcast;拆分路线的 PENDING 分支必须是**对已发布 `trip_id` 的 anti-join**(补集
  按构造成立),对维表 anti-join 会漏行(6/8)

## Difficulty cadence
Alternate roughly Easy/Medium -> Medium -> Medium-Hard; insert an Easy
day after two consecutive hard days. Interview frequency weighs topic
priority (windows, joins, dedup, dates are highest frequency).
