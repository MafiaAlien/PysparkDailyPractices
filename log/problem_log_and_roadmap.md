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

## 调度矩阵(已用组合)

> `/newday` 读这张表来避免重复三轴组合。failure mode **只**记在这里和
> `refs/*_ref.md`,**永远不进 day 文件**。

| Day | ETL layer | Domain | Failure mode |
|-----|-----------|--------|--------------|
| 22 | L4 incremental | subscription billing | late data |
| 23 | L3 serving | e-commerce (marketplace) orders | fan-out double counting |
| 24 | L2 detail modeling | logistics — cold-chain warehouse telemetry | replay |

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

### NEXT(Day 25)
- **Day 25 — L3 aggregation / ad delivery / failure mode: timezone attribution。
  5 stages -> Medium-Hard。** 广告投放事件按**投放地本地日**归集,而落地表是 UTC。
  选它的三条理由:(1) 三轴与 Day 22/23/24 **全部不重叠**(L4 / L3 serving / L2
  -> L3 aggregation、billing / marketplace / logistics -> ad delivery、
  late data / fan-out / replay -> timezone attribution);(2) Day 24 **刻意回避了
  日期解析**(定宽字符串,题面明写 "about the pipeline, not date parsing"),把时区
  归属放在下一格是自然升级,且复用 Day 11 的 UTC->local 分桶而不是重学它;
  (3) 所需 primitive 全部已练过(UTC->local bucketing、event-date attribution、
  date-dimension spine gap-fill、conditional aggregation),**Stage 1 不需要现学
  任何函数**。
- **本日的额外设计要求(针对上面记录的连续失效)**:trap 的读时 tell 必须落在
  **题面已经明确要求读者比对的两列**上,而不是落在一个需要主动扫描才能发现的
  数据巧合上。Day 23 与 Day 24 的 tell 都属于后者,连续两次未被触发。
- 若要把难度推到 Hard(6 stages),加一级 date-dimension spine 补齐即可;
  但**不建议同时**——本日的失败模式本身已是三轴里最容易被"看起来对"掩盖的一个。
- 后续候选(尚未排期):
  L4 incremental / inventory / **orphan keys**(层级与 Day 22 相同,
  业务域与失败模式已同时换,允许)——但注意 Day 23 的 ref 已把
  "effective-dated join 的孤儿是 fact-side key,`left_anti` 找不到同一批
  订单"写进概念要点,该轴的新鲜度已被部分消耗。
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
- Null semantics special: null-safe equality (<=>), null in joins,
  null ordering in windows   <- Day 16 DONE (Stage 1 + digest; AI review 未做,已结)
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

## Difficulty cadence
Alternate roughly Easy/Medium -> Medium -> Medium-Hard; insert an Easy
day after two consecutive hard days. Interview frequency weighs topic
priority (windows, joins, dedup, dates are highest frequency).
