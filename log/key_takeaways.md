# 累积要点 (Day 1-18)

> 术语、API 名、函数名、报错类名一律保留英文;正文用中文。

## 窗口函数
- dense_rank vs rank vs row_number:dense (1,2,2,3) / rank (1,2,2,4) /
  row_number(唯一,并列时任意断开)。带并列的 Top-N 要 dense_rank;
  gaps-and-islands 要 row_number 严格 +1 的性质。
- dense_rank()/rank()/row_number() **不接参数**——排序完全来自窗口的
  orderBy。F.dense_rank(F.col(...)) 是 TypeError。
- 没有 partitionBy 的窗口会把全部数据压进一个分区,大表上的性能杀手;
  这是常驻 review checklist 项。

## Gaps-and-islands
- (value - row_number) 在一个 island 内是常数;日期场景用
  DATE_SUB(d, rn) 当 anchor / island key。
- **先去重再编号**:一条重复行会把 rn 顶偏,静默地把一段连续streak 劈成
  两段——干净数据上通过,脏数据上失败。

## 去重(四种近似等价形式)
- SELECT DISTINCT A,B ≡ GROUP BY A,B(无聚合)≡ select(A,B).distinct()
  ——物理计划完全相同(Aggregate),一次 shuffle。
- dropDuplicates(["A","B"]) 保留**所有列**;每个 key 存活哪一行是
  **不确定的**。
- 窗口 row_number()=1 既保留所有列,又能确定性地挑行(靠 ORDER BY 的
  tie-breaker)——"每个 key 保留最新"必须用它。
- 选择:只要 key -> DISTINCT/GROUP BY;要其他列、任意一行 ->
  dropDuplicates;要其他列、指定某一行 -> 窗口。

## 每个 key 保留最新(Day 8,去重的应用)
- orderBy(...).dropDuplicates(keys) **不是契约**:本地看着对,真集群上
  会坏(shuffle 之后存活行仍然不确定)。"保留最新"**必须**用窗口 rn=1
  或 struct-argmax。
- 这里的 struct-argmax **不需要取负**:两个排序键方向一致(都是 DESC /
  "取大")-> max(struct(updated_at, source_seq, payload...))。payload 字段
  跟在排序键**后面**搭便车,只有所有排序键都并列时才会被比较;对精确重放的
  重复行来说是在两条完全相同的行之间选,无害。
- 存活行旁边还要每个 key 的总数:COUNT(*) OVER (PARTITION BY key) 且
  **不带 orderBy**,能搭上 row_number 的**同一次** window shuffle——一个
  Exchange、一次扫描。另开一个 groupBy 分支再 join 的路线要多付**第二个**
  Exchange 和**第二次**源表扫描(join 本身通过 ENSURE_REQUIREMENTS 复用了
  匹配的分区,不加 Exchange——代价在多出来的那个分支,不在 join)。
- 取舍:window count **没有** map 端 partial aggregation;如果你**只**要
  计数(不做排名),groupBy 更好——partial agg 能缩小 shuffle 流量。
  搭便车只在"已经有一个排名窗口逼着整行过 shuffle"时才划算。
- **有序窗口默认 frame 陷阱**:只有 partitionBy 的窗口 = 整个分区的 frame
  (真·总计);**加上 orderBy** 会把默认 frame 静默切换成
  RANGE UNBOUNDED PRECEDING..CURRENT ROW,于是 count/sum OVER 变成
  **累计**聚合——rn=1 那行读到的是 1,不是总数。计数窗口必须**不带**
  orderBy。
- 精确重放的重复行**仍然**计入 n_versions(数的是行,不是不同版本数)
  ——先读清楚指标定义再去伸手拿 countDistinct。

## 窗口 shuffle 机制(属于 spec,不属于函数)
- shuffle 属于 window **SPEC**,不属于函数:row_number / rank / lag /
  count-over 代价相同——查询里每有一个**不同的** partitionBy 表达式,
  就是一个 Exchange。相同 partitionBy -> 共用一个 Exchange;不同 -> 各一个。
- 窗口里的 orderBy 只在 Exchange 之后加一个 SORT,不是另一次 shuffle。
  没有 partitionBy = 全部行进**一个**分区(仍然只有一个 Exchange,但是
  单分区杀手)。
- 审查一个查询的窗口代价:数**不同的** partitionBy 表达式个数,然后检查
  有没有裸窗口。

## Pivot
- pivot(col) 不给 values 列表会触发一个**额外 job**(distinct + collect
  回 driver),并且让输出 schema 依赖数据(schema drift)。domain 已知时
  永远显式传列表。
- values 列表同时也是**过滤器**:没列进去的值被静默丢弃。
- 没有数据的 pivot 单元格是 NULL,不是 0——显式 na.fill / COALESCE。
- 单个聚合:alias **不**影响列名(纯值)。多个聚合:列名变成
  <value>_<agg_alias>;列数 = |values| x |aggs|。
- 在 pivot 的聚合上下文里 F.count("*") 解析失败
  (INVALID_USAGE_OF_STAR_OR_REGEX)。DSL 里一律用 F.count(F.lit(1));
  SQL 里 COUNT(*) 处处可用。
- 条件聚合 SUM(CASE WHEN...) 是可移植的等价写法;一旦 pivot 列表显式给出,
  两者计划几乎相同。

## 数组
- explode() **丢弃**空数组/NULL 数组的行;explode_outer() 保留(元素为
  NULL)。数组列的头号 bug。
- 不 explode 的路线:collect_list(arrays) -> flatten -> array_distinct /
  array_sort / size,在组级别做数组运算,不用把行炸开。
- collect_set 在 map 端 partial aggregation 时就去重(shuffle 更小);
  collect_list 保留重复。collect_set 的元素顺序**不确定**——比较前先
  array_sort。
- sort_array vs array_sort:都默认升序;区别在 NULL 的位置(前 vs 后)和
  附加能力(bool 降序标志 vs lambda comparator,3.0+)。
- **~~SIZE(NULL) = -1(遗留怪癖)~~ 这句在 Spark 4.x 上是错的**(Day 19 实测修正):
  `size(CAST(NULL AS array<int>))` 返回 **NULL**。-1 的行为由
  `spark.sql.legacy.sizeOfNull` 控制,但**生效值是它与 `!ansi.enabled` 的与**
  ——4.x 里 ANSI 默认开,所以即使 `spark.conf.get("spark.sql.legacy.sizeOfNull")`
  读出来仍是 `"true"`,实际拿到的仍然是 NULL。**照 conf 读数下判断会二次踩坑:
  要看的是生效值,不是 conf 值。** 后果:`size(arr) > 0` 这个守卫在 NULL 数组上
  求值为 **NULL 而不是 false**(实测),布尔输出列会是 NULL 不是 False。
  可空数组仍要兜底,但兜的是 NULL 不是 -1。

## 聚合函数的 NULL 语义
- **所有**聚合函数(count/sum/avg/collect_list/collect_set...)都忽略
  NULL 输入。这一条规则同时消灭两类 review 项:漏处理 NULL 的 bug,
  **以及**冗余的防御代码(collect_set 前的 filter(isNotNull)、na.fill
  之后的 coalesce)。
- COUNT(column) 只数非 NULL;全 NULL 的组返回 0(永远不是 NULL)。
  在一种语境下是陷阱(静默少算),在另一种语境下正是对的工具
  (explode_outer 之后把空数组客户归零)。
- CASE WHEN ... ELSE 0 vs 不写 ELSE + COALESCE:两种 NULL 策略都合法,
  但必须成对一致使用。

## Join
- on="key"(字符串)把 join key 合并成**一列**;left join 时合并后的 key
  取**左侧**的值(孤儿 key 得以存活)。**full outer 时合并后的 key 是两侧的
  `coalesce`**(Day 22 实测:只存在于右侧的新增 key 正常出现在输出里,无需手写
  COALESCE;SQL 的 `USING` 同理,而写成 `ON a.k = b.k` 则**必须**自己 coalesce,
  否则新增行的 key 是 NULL)。推论:**upsert 里把 `full_outer` 改成 `left`
  不是优化**——它丢掉所有纯新增的 key(实测 5 行 -> 4 行),而 **Exchange 数
  完全相同(4 vs 4)**:零收益的正确性回归。表达式 join(a.k == b.k)保留
  **两列** -> AMBIGUOUS_REFERENCE 风险;用 df["col"]、alias 限定,或立刻
  drop 掉一侧来消歧。
- 自连接**必须**用 alias;那里连 F.col 都是有歧义的。
- LEFT join + COALESCE(dim_col, 'UNKNOWN') 是标准 enrichment 模式,让
  未匹配的事实行存活;inner join 会静默丢掉营收。
- Broadcast:F.broadcast(dim) / /*+ BROADCAST(alias) */ 包住**小**的一侧。
  三层机制:显式 hint(无条件)> 静态 autoBroadcastJoinThreshold(需要
  size **统计信息**——缺统计信息如 Scan ExistingRDD 会被当成无穷大,
  于是不会自动 broadcast)> AQE 运行时转换(拿到真实大小,但初始 shuffle
  已经付过了)。代价排序:显式 < AQE 转换 < 完整 SortMergeJoin。
- 单块 GROUP BY 里用了变换过的 key:要么在 GROUP BY 里**原样重复**同一个
  表达式,要么把变换提到 CTE/子查询里(最干净——只有一份,不会漂移)。
  GROUP BY 写 alias 会**优先解析输入列**(静默改绑定)且不可移植。

## Join 扇出(fan-out):在聚合之前发生的聚合 bug (Day 23 实测)
- **join 不是查字典,是配对**:左表每一行与右表所有满足条件的行逐一配对,
  右表有几行匹配,左行就被复制几份。把 join 想成"拿 key 去查一个值填回来"
  是这个 bug 的心智根源。实测:7 行事实表 join 一张 `S3` 有两行的配置表 ->
  **9 行**,`SUM(gross)` 从 585.0 变 705.0,而 `COUNT(DISTINCT order_id)`
  仍是 7。
- **join 把行乘出来,SUM 忠实地把它们加起来。** 错误发生在聚合**之前**,
  等聚合算完,产生错误的那些重复行已经消失了——下游没有任何一步能检测到它。
  唯一的修复位置是 **join 的右侧**。
- **只有可加聚合会坏。** `SUM` / `COUNT(*)` / `AVG` 被放大;
  `COUNT(DISTINCT k)` / `MIN` / `MAX` / `collect_set` 在重复下**幂等**,保持
  正确。所以失败形状是**半对的一行**:计数对、金额错。这是 **Day 18 的镜像**
  ——那里 `SUM` 可跨盐桶分解而 `COUNT(DISTINCT)` 不可,这里 `COUNT(DISTINCT)`
  免疫而 `SUM` 不免疫。**同一个问题的两侧:先搞清哪些度量能活过你正在对行集
  做的那个变换。**
- **`n_orders` 这类 DISTINCT 计数是最危险的免疫者**:它恰好是人类拿来核对
  "8 单去掉 1 个取消 = 7 单"的那一列。它对得上,旁边的金额是错的。
- **输出形状断言抓不到数值级 bug**(Day 23,P2 实录):grain 唯一、非 NULL、
  行数,在扇出下**全部不变**。作业写齐了断言、断言全过、错数字照发。真正能
  覆盖的是**对账式断言**——join 之前在事实表上算的 `SUM(gross)` 必须等于输出
  的 `SUM(gross_gmv)`。断言约束的是输出的**形状**,而扇出不改变形状。
- **维表"每个 key 一行"只有在有东西强制它时才成立。** 生效期配置表什么都不
  强制:唯一性是 (key, date) 上的性质,依赖区间不重叠,而那是**数据的属性**、
  靠人手维护,不是 schema 的属性。`dropDuplicates(key)` 不是契约,docstring
  里的一句注释更不是。
- **半开区间 `[from, to)` 让边界日无歧义**,是生效期 join 的默认写法;
  `valid_to IS NULL` 表示开口,所以 `COALESCE(valid_to,'9999-12-31')` 是
  **承重**的——裸写 `d < valid_to` 时开口行返回 NULL,谓词静默拒绝,
  **当前在岗的 seller 全部丢失**。用闭区间 `BETWEEN` 的分歧只在"已关闭且无
  后继"的日期上暴露(实测归给已失效的 assignment);当关闭日恰等于后继的开始
  日时,闭区间的多匹配会被 `valid_from DESC` 的选择**抵消掉**,测试数据因此
  看不出来。
- **两条修法,取舍是结构性的而非 shuffle 的**(Exchange 实测 6 = 6 = 6):
  **rank-and-pick**(让扇出发生,`row_number() partitionBy(事实键)` 再
  `rn = 1`)一行代码、只需假设"最新的赢";**修维表**(`lead` 闭合区间,见
  SCD2 一节)让 join **由构造保证** <=1:1,下游不需要任何去重。后者更好的理由
  是:窗口跑在**维表**(小)而不是**事实流**(大)——实测 Sort 6 vs 4,多出来
  的两次排的是事实流;修好的维表可复用、可独立断言;而 rank-and-pick 的守卫
  是一句离肇事表好几个 stage 的 `WHERE rn = 1`,删掉它数字变大、**没有任何
  其它症状**。
- **`rn = 1` 不区分重复的来源。** 它会把上游**任何**一个 join 带来的额外行一并
  吃掉,包括你需要的那些。Day 23 实录:为消除配置扇出而设的窗口,顺手吞掉了
  同一订单的第二笔退款——实测 refund 40.0 vs 正确 65.0,症状是**钱变少**,
  与它本要修的方向相反。推论:**每个 join 的右表都要单独回答"对 join key
  唯一吗",不能指望下游某个 dedup 兜底。**

## Semi / anti join 与 IN / EXISTS (Day 9)
- left_semi / left_anti 是**穿着 join 语法的过滤器**:输出 schema = 只有
  左表,永不发出右侧列,永不放大行数。semi = EXISTS(左行有 >=1 个匹配就
  保留);anti = NOT EXISTS(没有匹配才保留)。**不是** join 完再 distinct。
- Catalyst 的 decorrelation 把下面这些全部编译成同一组 LeftSemi / LeftAnti
  逻辑算子:IN (subquery)、EXISTS、LEFT SEMI JOIN 语法、DSL left_semi ->
  LeftSemi;NOT IN(安全时)、NOT EXISTS、LEFT ANTI JOIN、DSL left_anti ->
  LeftAnti。已用 explain() 验证:一个混用 NOT EXISTS + IN 的查询产生了两个
  BroadcastHashJoin 节点,一个 LeftAnti(来自 NOT EXISTS)+ 一个 LeftSemi
  (来自 IN),通过右侧属性的 Scan 血缘追踪确认。
- 为什么 IN (subquery) 变成 semi 而不是 inner:IN 的含义是"至少出现一次"
  -> 既**不能**放大左侧行(一个客户匹配 3 条订单仍然只出 1 行),**也不能**
  发出右侧列。这两个约束**就是** semi join 的定义。

## NOT IN vs NOT EXISTS —— 三值逻辑陷阱
- NOT IN + 子查询里**含任何 NULL** -> 整个查询返回**零行**:x <> NULL 是
  UNKNOWN,所以 x NOT IN (..., NULL) 永远不为 TRUE。这是**否定方向**特有的
  陷阱;正向的 IN 不受影响(匹配仍然是 TRUE,NULL 只是匹配不上)。
- NOT IN 的安全性要求**两侧**都没有 NULL:子查询侧**和**外层列。
  在子查询上加 isNotNull() 只补了**一侧**;外层的 NULL 仍然让
  NULL NOT IN (...) = UNKNOWN,被静默丢弃。
- NOT EXISTS / LEFT ANTI 天生 NULL 安全(建立在 = 之上,NULL 只是匹配不上)
  -> 否定存在性的**正确默认选择**。把 NOT IN 换成 NOT EXISTS 是"选择正确的
  语义"(NULL = 不匹配),**不是**无脑等价改写:有 NULL 时两者确实不同,而
  标准 NOT IN 的 NULL 毒化偶尔正是想要的行为。
- shuffle 代价:一旦 NOT IN 能安全 decorrelate(子查询侧无 NULL),它编译成
  和 NOT EXISTS **相同**的 LeftAnti -> Exchange 数完全一样(broadcast 则 0,
  SMJ 则每个 key 一次)。差别**不在** shuffle,而在 NOT IN 多背了一个
  可删除的过滤前提。当 NOT IN **无法** decorrelate(子查询可空、又没有过滤)
  时,Catalyst 会插入 NULL-aware anti join -> 可能退化成
  BroadcastNestedLoopJoin / 额外的 null 检查谓词 = 更重的计划。
  isNotNull() 把 NOT IN 从重计划救回轻的 LeftAnti 计划。

## 冗余防御代码 —— join 版 (Day 9)
- semi/anti 之前对**右侧** distinct() 是冗余的:存在性语义本来就忽略右侧的
  重数(匹配 1 次和匹配 100 次 -> 左行存活情况相同)。它对正确性零贡献,却
  确定地增加**一个** groupBy Exchange(虽有 partial-agg 缓解,仍是净增一次
  shuffle)。这是 Day 9 版的"聚合语义已经覆盖了这个过滤"——semi/anti 永不
  放大行数,所以右侧永远不需要去重。
- 用 inner join 做存在性判断是**相反**的错误:它**会**放大(改变 grain,
  Day 4/6 的教训换了身 join 的衣服),逼得下游必须补一个很容易忘的
  distinct/dropDuplicates。优先用 semi join,它直接把"存在但不放大"编码在
  语义里。

## Map
- map_keys / map_values / map_entries ≈ dict.keys()/.values()/.items();
  全都返回 **ARRAY** 列,所以后续处理走数组函数家族(size, flatten,
  array_distinct...)。
- map_keys(NULL) / map_values(NULL) 返回 **NULL,不是空数组**。NULL map 和
  空 map 是真实存在的区别,会一路影响下游。
- Map 取值(props['k'] / .getItem / element_at)对"key 不存在"和"map 为
  NULL"都返回 NULL——**永不报错**。在过滤里是安全的(被聚合-NULL 规则吸收),
  调试时是静默的。
- 对 map explode() 会产生**两列**(key, value);对 map_keys(...) explode
  只有一列。丢弃规则和数组相同(Day 4):explode 丢掉 NULL/空 map,
  explode_outer 保留该行。
- **去重的单位必须匹配行的 grain**:在 explode 之后的 grain 上,key 是标量
  元素,所以 collect_set(key) 直接对 key 去重;在未 explode 的 grain 上,
  每个元素是**整个数组**,collect_set 是对数组去重([] 也算一个成员!)
  ——必须改成 flatten -> array_distinct。"explode 改变 grain,重新检查每个
  COUNT"(count -> countDistinct(id))的姊妹条。
- countDistinct 比 count 贵(expand / 两阶段聚合,map 端 partial agg 更弱)
  ——这是 explode 路线的隐藏成本;用 explain() 验证,别凭记忆。
  **(Day 18 实测精化:"expand"只对**多个** distinct 成立。单个
  `COUNT(DISTINCT x)` 的计划里**没有 Expand 节点**,它被重写成两层堆叠的
  HashAggregate ——先 `(分组键, x)` 粒度、再 `(分组键)` 粒度。同一份数据实测:
  单 distinct = **0 个 Expand / 2 个 Exchange**,两个 distinct = **1 个 Expand /
  4 个 Exchange**。而且它贵不贵**取决于继承到的分区**:上游按 `customer_id`
  分区时,那两层之间**不插 Exchange**(子集规则满足);上游按 `(customer_id, salt)`
  分区时要多插一个。所以"countDistinct 更贵"必须落到具体上游,它不是常数代价。
  在计划里读到 `partial_count(distinct ...)` 就当成一个便宜的单算子,是这里的错误。)**

## Struct
- 嵌套字段访问:device.os / F.col("device.os") / df["device"]["os"] 三者
  等价(dot-path、按位置、固定 schema)。**struct 不是 map**:字段名固定、
  没有动态 key、没有 element_at、不能对普通 struct 做 explode——explode 只
  适用于 array(struct<...>) 这种形状。
- **父 struct 为 NULL vs 子字段为 NULL**:整个 device 为 NULL 时访问
  device.os 返回 NULL(不报错)——和显式把 os 置 NULL 得到**同一个** NULL。
  一旦你 project 出 device.os,两者**不可区分**。这正是聚合-NULL 规则让
  countDistinct(device.os) / COUNT(device.os) "刚好能用"的原因——但**也是**
  陷阱:COUNT(device.os) 数的是所有非 NULL 的 os(macOS 也算),这**不是**
  "mobile events"。mobile_events 需要对一个显式集合做条件计数——
  count(when(array_contains([iOS,Android], os), 1))——而不是 COUNT(device.os)。
  当干净数据里非空的 os 恰好全是移动端时,错的写法会"碰巧正确"(又一次伪装)。
- primary_os = 组内 argmax。三条路线,都需要先过滤 NULL os + 确定性
  tie-break:
  (a) row_number() over (partitionBy user orderBy cnt desc, os asc),取 rn=1
      ——最通用,Top-N>1 或要保留多列时必须用它。
  (b) min/max(struct(...))——struct 按字段逐个字典序比较。见下面"struct
      复合键 argmax"。
  (c) 把 (cnt,os) struct 收集起来 array_sort,取头元素。
  三条路线之后都要 LEFT JOIN 回每用户基表,让一个**没有**可用 os 的用户
  仍然出行;COALESCE(primary_os,'UNKNOWN') 填空。inner join 会静默丢掉
  os 全为 NULL 的用户——和 Day 5 事实-维度 enrichment 是同一个教训。

## struct 复合键 argmax: min/max(struct(...))
- 对 struct<f1,f2,...> 取 min/max 是**从左到右逐字段字典序**比较(像 tuple)。
  min 把**所有**字段推向小,max 把**所有**字段推向大——整个 struct 只有一个
  方向,原生**无法**按字段分别指定方向。
- 要在单个聚合里表达"cnt DESC, os ASC"(方向冲突):先按 **tie-break 字段**
  的方向选聚合函数,再把**跟它打架**的字段取负。os asc = "取小" -> 用 min;
  cnt desc 和 min 的"取小"冲突,所以取负 -> min(struct(-cnt, os)),然后
  .select("best.os")。当没有冲突的 tie-break 时它等价于 max(struct(cnt,...));
  取负**只**用于调和相反的排序方向。字符串没有一元负号,所以当 tie-break 字段
  需要的方向和 max/min 打架时,"min + 数值取负"是干净的逃生口。
- Spark **没有**通用的多键 argmax。max_by(value, ordering) 只接**一个**排序列,
  且**不保证** tie-break——无法确定性地表达"cnt desc + os asc"。所以真正的
  选项就是 row_number / struct-argmax / array_sort。
- 性能:struct-argmax 和 window 在这里**都是 2 次 shuffle**(先 groupBy
  (user,os),再按 user 重分区——(user,os)->(user) 是超集->子集,无法复用)。
  优势**不在** shuffle 次数:struct-argmax 的第二阶段是 groupBy+min **聚合**
  (有 map 端 partial agg,过 shuffle 的数据很小),而 window 的第二阶段是对
  每一行 os 做完整 SORT + Window(没有 partial-agg 缓解)。用 explain() 确认:
  两者都显示 2 个 Exchange,但 struct 路线的第二个是一对 HashAggregate,
  window 路线的是 Sort+Window。**别把结论记成"shuffle 更少"——那是错的;
  记成"第二个 Exchange 处聚合胜过排序"。**
- **(Day 16 限定)** 上面"window 没有 partial-agg 缓解"这句**只在没有 rank 过滤时
  成立**。一旦写了 `rk = 1` / `rk <= k`,Spark 3.5+ 会插入 `WindowGroupLimit`
  把 top-k 下推到 shuffle **之前**(计划里 Exchange 之下是 `..., Partial`,
  之上是 `..., Final`),等于 window 版的 map 端缩减。详见"修正:rank-filter
  window 有 map 端缩减"一节。

## 复杂聚合: GROUPING SETS / ROLLUP / CUBE (Day 10)
- 这是**三个不同的层次**,不是并列的兄弟:
  * GROUPING SETS / ROLLUP / CUBE 是 **GROUP BY 子句**——决定**产生哪些
    grain(哪些行)**。GROUPING SETS = 精确列出你要的 grain(最精准)。
    ROLLUP(a,b) = 层级子集 {(a,b),(a),()}——假定 a->b 的下钻关系,
    **不包含** (b)。CUBE(a,b) = 全部 2^n 个子集 {(a,b),(a),(b),()}。
    等价关系:CUBE(a,b) == GROUPING SETS ((a,b),(a),(b),());
    ROLLUP(a,b) == GROUPING SETS ((a,b),(a),())。CUBE/ROLLUP 只是常用
    GROUPING SETS 组合的语法糖。
  * GROUPING(col) 是**聚合函数**,返回 0/1——对每个输出行回答"这一列在这里
    被卷起来了吗"(1 = 被聚掉/小计维度,0 = 真实值)。这是判断"这行是不是
    小计"的**唯一可靠信号**:裸的 `col IS NULL` 分不清源数据里真实的 NULL
    和 rollup 机制注入的 NULL(呼应 Day 7"NULL 的来源在 project 之后就丢了";
    GROUPING() 把它找回来)。
  * GROUPING_ID(c1,...,cn) 把每列的 grouping 位打包成一个整数:
    **最左参数 = 最高位**。GROUPING_ID(region,category):region=bit1(值 2),
    category=bit0(值 1)。0=基础 grain,全 1=总计。对这个整数做一次 CASE 就能
    干净地映射出 'level' 标签;按列写的 grouping() 形式(gr==0 & gc==0 ...)
    更啰嗦但自解释(不需要回忆位序)。
- 口诀:GROUPING SETS/CUBE/ROLLUP **生产**多 grain 的行;GROUPING/GROUPING_ID
  **读取**每行上的标签。前者造数据,后者做解释。
- **DSL 缺口**:有 df.cube(...) / df.rollup(...) + F.grouping / F.grouping_id,
  但**没有** df.grouping_sets() 这个 helper。纯 DSL 里要"只要特定 grain":
  cube 之后再过滤,或显式 groupBy+union,或改用 SQL。这里 2 维 cube 不浪费
  (它的 4 个子集**就是**要的 4 个 grain);n 维时 cube = 2^n 行(组合爆炸),
  所以优先用 GROUPING SETS 点名精确的 grain——和 Day 3 pivot 的"显式 values
  列表"是同一种哲学。
- **单次源扫描**:grouping sets / cube / rollup 只读表**一次**,在内部展开;
  物理计划显示一个 Expand(负责放大行、给不参与的维填 NULL 的算子)+ 一个
  Exchange + 一对 HashAggregate。而 4 个 groupBy 再 union 的路线要扫源表
  **4 次**、4 个独立 Exchange。在 .explain() 里数 Expand/Exchange 来验证。

## 真实 NULL vs 小计 NULL —— 两种稳健策略 (Day 10)
- 问题:源数据里**真实为 NULL** 的维度,和 cube/rollup 在小计行上**注入**的
  NULL,一旦 project 出来就**不可区分**。两条干净的路线,稳健性不同:
  * **预填哨兵**(所选路线):在 cube **之前** coalesce(dim, '<sentinel>')。
    彻底消灭源 NULL,于是剩下的每个 NULL 都只可能是小计 NULL。**最稳健**——
    从根上消除混淆;之后可以**仅凭** grouping_id 重建维度,完全不碰 IS NULL。
    代价:哨兵字符串可能和真实维值撞车(挑一个不可能出现的值来防)。
  * **事后判别**(参考路线):保留 NULL,先分支 GROUPING(dim)==1 -> 'ALL',
    **再** IS NULL -> 哨兵。更"正确"(不改源数据)但**对顺序敏感**:两个分支
    调换,真实 NULL 和小计 NULL 就并到一起了。这是脆弱的那条路——分支顺序是
    一个下一个人随手就能破坏的可删除不变量。
- SUM(int) 上加 cast('long'):SUM(int)->bigint;显式 cast 是给生产 sink 的
  schema 卫生(check() 两种写法都抓不到)。

## 精确 grain 控制 + 条件计数 (Day 12)
- **grain 集合的选择 = 为精确的 grain 集合挑对工具**,而不是挑最强的工具。
  想要 {(r,c),(r),(c)}(三个非空 grain,**不要**总计):
  * GROUPING SETS ((r,c),(r),(c)) —— 精确点名这三个,不产生 () 行,不需要
    后置过滤。精准控制的赢家(同 Day 3 pivot 的"显式 values 列表"哲学)。
  * CUBE(r,c) —— 连总计 () 一起产生 4 个,必须过滤 gid!=3。先超产再裁剪。
  * ROLLUP(r,c) —— {(r,c),(r),()}:**集合就是错的**——多了不想要的 (),
    还**缺了** (category)。r->c 的层级假设不适合"两个对称单维"的需求。
  * DSL **没有** grouping_sets:用 cube-然后过滤(一个 Expand、一个 Exchange,
    多算一个用不上的 grain)或显式三分支 union(3 次扫描 + 3 个 Exchange,
    不浪费 grain)。小数据量两者都行;规模上去优先单次扫描的 grouping sets/cube。
- **阈值 / 条件计数陷阱**(埋的雷):big_orders = "amount>=1000 的行数"。
  **else 分支的取值决定一切**:
  * SUM(when(cond,1).otherwise(0))  -> 正确(把 1 加起来)
  * COUNT(when(cond,1))  【不写 otherwise】 -> 正确(else 是 NULL,COUNT 跳过)
  * COUNT(when(cond,1).otherwise(0)) -> **错**:0 是非 NULL,COUNT 数**每一行**
    = 组的大小。在所有行都满足条件的 grain 上恰好正确,在别的 grain 上错——
    "碰巧正确"的伪装(和 Day 7 的 COUNT(device.os) 同属聚合-NULL 家族)。
- 重建被卷起的维度**只用 grouping 元信息**,永远不要写 `col IS NULL AND gid=k`。
  那个 IS NULL 合取项在干净数据上是冗余的,而且**主动误导**:它暗示"NULL 是
  判据的一部分",而实际上 gid(或 grouping(col)=1)才是唯一的根信号。即使源维
  真的有 NULL,带 IS NULL 的版本也可能靠运气得出正确结果,但它是典型的
  可删除不变量气味——删掉 IS NULL,只留 gid。
- grouping(col) -> 每行 0/1(1 = 这一列在这里被卷起)。它**是聚合函数**,只在
  group by / grouping sets / cube / rollup 下合法。grouping_id(c1,...,cn) 把这些
  位打包成一个整数,**最左参数 = 最高位**:grouping_id(region,category) =
  grouping(region)*2 + grouping(category)*1。所以 gid=1 -> (region) grain
  (category 被卷起,填 category='ALL');gid=2 -> (category) grain(region 被卷起,
  填 region='ALL')。**注意这个交叉**:被填 'ALL' 的是**被卷起**的那一维,而
  level/grain 的名字来自**幸存**的那一维——两者天生相反(这就是 level 注释很容易
  写反的原因)。grouping(col)=1 形式自解释;grouping_id 的魔数需要把位序钉死在
  **参数顺序**上来回忆。

## 日期/时间: 时区分桶 + 日期维度补齐 (Day 11)
- **转换方向就是全部陷阱**。from_utc_timestamp(ts, tz) 读作"ts 是一个 UTC 瞬时,
  给我它在 tz 的墙钟时间"——这正是"以 UTC 存储、按本地报表"要的方向。
  to_utc_timestamp 是**反向**(本地->UTC),会朝错误方向平移:一笔 LA 的销售会
  +8h 而不是 -8h,静默地挪到第二天。这个 bug 在**任何不跨本地午夜的行上都通过**
  ——只有日界行才暴露。和 COUNT(device.os) 同属"干净行上伪装"家族。
- 对 timestamp 做 to_date / cast(date) 是在 **spark.sql.session.timeZone** 下求值的。
  你已经用 from_utc_timestamp 本地化过了;如果 session tz 非 UTC,to_date 会
  **第二次**平移 = 双重转换的日期漂移。把 session tz 钉死(harness 里用 UTC),
  让你算出来的日期不被静默地重新分桶。
- 窗口过滤必须作用在**本地日期**上、在时区转换**之后**——绝不能用原始 UTC 日期。
  一个 UTC 上看在窗口外的时间戳,本地看可能在窗口内(反之亦然),所以先按 ts_utc
  过滤会丢错/留错行。这是时区陷阱的**第二层**,和方向 bug 是两回事:函数对了,
  但过滤的轴错了。
- **补齐 = 制造一条稠密的 SPINE,而不是去检测缺口**。构造完整的 key x date 轴
  (explode(sequence(start, stop, interval 1 day)) crossJoin 维表),把稀疏的聚合
  结果 LEFT join 到 spine 上(spine 是 LEFT/被保留的一侧),再 COALESCE 成 0。
  这是 Day 2 gaps-and-islands 的维表版:Day 2 用 row_number 算术**检测**缺口;
  这里是把完整的轴造出来,让缺失的天无处藏身。
- sequence(start, stop, interval) 两端**都是闭区间**;explode 把数组变成行。
  窄依赖生成(不产生 shuffle)。**先**把销售聚合到 (store, day) grain **再**去 join
  spine:那样 join 是 spine(小) LEFT agg(小)——**永远不要**拿原始销售去 join spine
  (grain 爆炸 + 无谓 shuffle),和 Day 8"宽 join 前先聚合"是同一条纪律。
- 两个度量的零填充要带正确类型:revenue -> 0.0(double,SUM(double) 本来就是
  double),n_txn -> CAST(0 AS BIGINT) 以匹配 COUNT 的 bigint。revenue 上的 double
  cast 是冗余的(int 字面量 0 在与 double 的 coalesce 中会提升);n_txn 上的 bigint
  cast 才是真正有用的那个。冗余 cast vs 必要 cast,同样遵循"逐个判别、别一刀切"。

## 会话化与窗口 frame (Day 13)
- **按间隔阈值做会话切分 = 把 gaps-and-islands 的"严格 +1"换成"阈值"**。
  Day 2 的 (value - row_number) 常数差技巧在这里**失效**(间隔是 0..threshold
  之间的任意值,不是固定步长)。通用套路三步:lag(前一个 ts) -> 边界标志
  (prev IS NULL OR gap > threshold -> 1 否则 0) -> 在有序窗口上做
  running SUM(flag) = island/session key -> groupBy(user, key)。
- 那个在 Day 8 是 **BUG** 的有序窗口默认 frame(RANGE UNBOUNDED PRECEDING ..
  CURRENT ROW,导致意外的累计计数),在这里是**工具**(故意要 flag 的累计和)。
  同一个机制,相反的结论——**要对着意图判断 frame,不要凭条件反射**。
- 阈值的方向与单位陷阱(都在干净数据上"碰巧正确",同 Day 11 时区方向 /
  Day 7 COUNT 家族):
  * gap 必须是 current - prev;prev - current 恒为负,`> threshold` 永不触发
    -> 所有事件塌缩成一个 session。
  * 用**秒**比较(gap_sec > 1800),**不要**用截断后的分钟。
    timestamp_diff('MINUTE',...) / 整数分钟取整会把 30:40 截成 30,`> 30`
    判假 = 错误合并。整数分钟的测试数据能通过,亚分钟间隔才暴露。
  * 边界是**严格大于**:gap == 1800s 仍属**同一** session(规格:<= 30 分钟
    算同一个)。`>= 1800` 在恰好等于边界的那行上差一位。
- 窗口流水线的调试纪律:当 running sum 看起来不对时,**先 .show() 中间的 flag
  列**。一个全 0 的 flag 列(例如 when(...).otherwise(0) 两个分支都是 0)一眼
  就能看出来——别去怀疑 sum-over,它只是忠实地把一个坏输入加了起来。
- **内置 session_window vs 用 lag 手写的规格**:session_window 在
  last_ts + gap 处关闭,只有 new_ts < 该结束点才合并(**严格 <**),且它的 .end
  = 最后事件 + gap(不是最后事件的 ts)。和"gap <= 30 分钟算同一 session、
  end = 最后事件"这个规格有**两处契约不匹配**:恰好在边界的那行会被切开,
  而且每个 end 都平移了 +gap。**内置函数自带它自己的契约**——在伸手去用之前,
  先把它和写下来的规格对齐(当规格本身就是用 session_window 的语言写的时候
  它才是对的工具,尤其是流式场景)。

## SCD2 / 区间闭合 (Day 17)
- **一个版本是一段 run,不是一行**。SCD2 本质就是换了谓词的 gaps-and-islands:
  Day 13 的边界是"时间间隔 > 阈值",这里的边界是"属性与前一行不同"——
  **同一副骨架,可插拔的边界条件**。feed 行的粒度和版本的粒度不同,
  而"无变化的重发行"正是两者分叉的那一行。
- **边界检测必须是位置式的,永远不能是值集式的**。`lag` vs 当前行是契约。
  `distinct` / `dropDuplicates` 作用在属性列上会静默合并**不相邻**的重复值
  (`A -> B -> A` 变成两个版本而不是三个)——和"重发行"是两个不同的 bug,
  但同一条纪律能同时挡住。
- **半开区间 `[from, to)` 不需要任何日期算术**:`effective_to =
  lead(effective_from)`,没有 `date_sub(...,1)`,也没有 `date_add`。
  闭区间规格(`to = 下一个 from - 1 天`)才是 off-by-one 的产地,而且那个 `-1`
  的粒度(天?秒?)是**规格问题不是代码问题**——动手前先把用哪种约定钉死。
- **`is_current` 是 `effective_to IS NULL`,是派生量,不是第二个窗口**。
  `row_number() desc = 1` 或 `max(changed_at)` 会为了得到同一个事实再排一次序。
  区间一闭上,这个标志就是免费的。
- **窗口函数不能出现在 `WHERE` / `HAVING`**,两套 API 都一样。实测:
  `df.where(<含 lag 的表达式>)` -> `AnalysisException: It is not allowed to
  use window functions inside WHERE clause`。必须先 `withColumn` 物化
  (SQL 侧就是必须先有一个 CTE)。这是分析期错误,优化器不会救你。
- 两条等价路线,取舍点很清楚:**A 塌成版本级**(running sum -> groupBy ->
  lead)是**通用形式**——一旦版本级需要 run 内的聚合(run 里有几行 feed、
  run 内 max(price)),只有 A 写得出来;**B 过滤到版本起始行**(边界行本身
  就是版本的完整记录 -> 直接 lead)节点更少,但表达不了 run 内聚合。
  **下游形状反过来约束上游标记的语义**:A 路线下 flag 取 0/1/NULL 都不影响
  分组(running sum 只要求在 run 内恒定、跨边界递增);B 路线下每个 key 的
  **首行必须被标成边界**,否则第一个版本静默消失。
- 承接 Day 13 记的边界式 `prev IS NULL OR gap > threshold`:那个
  `prev IS NULL` 前缀在 null-safe 谓词下**可以整个去掉**——
  `~(c.eqNullSafe(lag(c)) & p.eqNullSafe(lag(p)))` 在首行天然为 true。
  少一个手写守卫,就少一个守漏的机会(见 Review 启发式里的对应条)。

- **`lead` 闭合区间同样适用于"修维表"**(Day 23):对一张生效期配置表按
  `partitionBy(key).orderBy(valid_from)` 求 `lead(valid_from)`,即可把 ops
  忘记关闭的区间补上,使 join 由构造保证 <=1:1。**但必须写
  `LEAST(COALESCE(valid_to, OPEN), COALESCE(lead(valid_from), OPEN))`,不能用
  裸 `lead` 直接替换 `valid_to`**——否则一个**已被正确关闭**的区间会被静默
  延展,跨过它本不覆盖的缺口。Day 23 的数据里 S2 的 `valid_to` 与后继
  `valid_from` 同为 2026-06-01,`LEAST` 与裸 `LEAD` 结果一致,所以那个 `LEAST`
  是**本数据上的死代码、生产上的承重代码**——Day 15 承重-vs-死代码判据的
  另一侧。

## Upsert / MERGE 进已有目标表 (Day 22)
- **一条变更不会自动比它要覆盖的那一行新。** 任何针对"自带 `updated_at` 的
  目标表"的 upsert,都必须对"这条变更比目标行还旧怎么办"给出**显式答案**。
  这不是边界情况——CDC feed 里的迟到行是常态。忽略它的症状是**维表的
  last-updated 列会倒着走**,而输出的形状(行数、NULL、重复)完全正常。
- **两种答案,产出相同、可审计性截然不同**:
  - **Route A 显式守卫**:`change_ts > target.updated_at`。谓词看得见、
    能被 review、能被测。代价是它是**手写的**,少写一处就漏一处。
  - **Route B 让目标行参与竞争**:把目标行和变更行统一成同一 schema
    `union` 进一个池子,单个 `row_number` 按时间戳选赢家。旧变更**输在排序上**,
    守卫从排序里**免费涌现**。代价是**代码里没有任何一行是那个守卫**——
    reviewer 找不到它,只能推理"它的缺席为什么是安全的";而一旦有人加了
    一个没有时间戳的目标行、或 union 时统一错了列,**保护静默消失,
    没有一行代码会显得缺失**。
  这是真实的工程取舍,不是风格偏好。Route B 还顺带把"批内多变更收敛"和
  "新旧对决"塌成**同一个窗口**——两者本来就是同一个问题:*每个 key 的所有
  候选行里,谁的时间戳最大*。
- **墓碑必须从两个地方移除,不是一个**:upsert 候选集 **和** 幸存目标集。
  只在 upsert 侧写 `op <> 'D'`,墓碑行被删掉了,**目标行还活着**——订阅复活,
  继续计费。这是 Day 16"防御代码的必要性由结构决定"的 merge 版本。
- **"被删除的不得出现"这类措辞比它看起来更含糊**。实测(扰动数据):
  `NOT EXISTS (... op='D')` = 只要本批出现过 D 就抹掉整个 key;
  `staleness guard 之后再判 D` = 只有 D 赢了时间戳才删。两者在
  "stale DELETE"和"同批先 D 后 I 重建"上结论**相反**,而**测试数据全绿**。
  动手前先钉死:**删除是一个状态,还是一个带时间戳的事件?**
- **派生度量的 system of record 是契约问题,不是数据问题**。feed 带的 `mrr`
  和目录的 `mrr` 在 5 行里有 4 行一致——**大部分时候一致,正是错误来源能通过
  code review 的原因**。最稳的写法不是"选对那一列",而是**把错误来源移出
  作用域**:收敛 CDC 时就不 select 它,最终 join 时作用域里只存在一个 `mrr`,
  无需表前缀也不可能选错(实测比"用 `p.mrr` 选对"强一级)。
- **配置维度 join 用 inner 还是 left,决定"配置缺失"是丢行还是产 NULL**。
  实测同一份扰动输入:inner 整行丢弃,left 产出 `mrr = NULL`——下游 `SUM`
  忽略 NULL,收入静默少一笔。**两种都能辩护,但都不该是无意识的**。
- 与 **Day 17** 的分界:Day 17 从 feed **构建**历史,从不合并进已有目标;
  本日是"目标表已经存在且带着自己的时间戳"。Day 17 的 `row_number` 收敛 run
  的写法在这里原样复用(stage 1),新增的全部在 stage 2。

## 窗口 frame 机制: ROWS vs RANGE,以及哪些函数受 frame 影响
- frame 是 **window spec 的一部分**,在 .over() **之前**构建在
  Window/WindowSpec 上:Window.partitionBy(...).orderBy(...).rowsBetween(a,b),
  **然后**才 fn.over(w)。.over() 返回的是 Column,是**收尾**步骤——
  col.over().rowsBetween(...) 是 AttributeError(Column 没有 frame 方法)。
  frame 属于窗口,不属于聚合结果。构建顺序:partitionBy -> orderBy ->
  rowsBetween/rangeBetween(frame 依赖 orderBy,所以放最后)。
- **WindowSpec 是不可变的**:.rangeBetween(...) 返回新对象而不是原地修改。
  所以一旦 base spec 用某个 orderBy 建好,由它派生出的所有 spec 都继承那个
  orderBy——改列不改 spec 是没用的(Day 15 实际踩到:加了 day_num 列但
  base spec 仍按 sale_date 排序)。
- ROWS 和 RANGE **只在 orderBy 有重复值(peers)时**才有区别:
  * RANGE 按**值**界定:所有 orderBy 相等的行互为 peer,被一起纳入
    (一个 peer 的 frame 包含它的所有 peer)。"重复值"对 RANGE 是有意义的概念。
  * ROWS 按**物理行位置**界定:每行就是第 N、N+1 行;重复值毫无特殊性,
    ROWS **永不**把 peer 归堆。代价:哪个重复行是"第 N 行"是不确定的
    (orderBy 的并列没被解开),所以单行的累计值可能不确定。当下游有 groupBy
    把分区重新塌缩时无害(Day 13);如果某一行的 frame 值本身重要,就在
    orderBy 里钉一个 tie-break 键。
  * Day 13 两者给出相同 session key,是因为重复 ts -> gap 0 -> flag 0,所以
    被归堆的 peer 贡献 0。当 peer 可能带非零值时优先显式用 ROWS——自解释且
    对 peer 安全("拿不准就钉 ROWS")。
- **frame 只影响对 frame 敏感的聚合**:窗口上的 sum / count / avg / max / min /
  collect_list 会响应 rowsBetween / rangeBetween。排名与位置函数——row_number /
  rank / dense_rank / lag / lead / ntile——**完全忽略** frame(它们的语义纯粹是
  位置/名次)。所以 unboundedPreceding 和自定义 frame **只**和聚合函数家族一起用;
  给 lag/rank 设 frame 是 no-op,是一种气味。

## RANGE frame 的类型约束 (Day 15,实测修正 Day 13 的旧记录)
- **旧记录是错的**,已在 Spark 4.1.1 实测推翻:04 曾写"DATE orderBy 上整数就是
  天,纯 DSL 无需 expr"、"TIMESTAMP orderBy 上整数是秒"。**在 PySpark 里这两条
  都跑不通**,报 DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE:
  `The data type "DATE" used in the order specification does not support the
  data type "BIGINT" which is used in the range frame`。
- **根因**:Catalyst 对 range frame 的类型规则里,DATE 排序列只接受
  IntegerType(及 interval 类型)的边界。SQL 里字面量 6 解析成 INT,所以 SQL
  走得通;而 PySpark 的 rangeBetween 把 Python int 交给 JVM 后落成 **BIGINT**
  字面量,DATE+BIGINT 不在白名单。而且 frame 边界的类型强制**只做 up-cast**,
  BIGINT->INT 是窄化,不会自动补 cast -> **分析期**就失败(连物理计划都没生成)。
- **触发条件收得更准**:类型检查**只由有限数值边界触发**。
  unboundedPreceding / unboundedFollowing / currentRow 在 Catalyst 里是
  **SpecialFrameBoundary**——符号,不带数据类型,压根不进这条校验。
  **两个边界都是符号 -> 安全;任意一个是有限数值偏移 -> 触发,DATE 上就挂。**
  一个就够。实测(DATE 排序列):
  * range(unboundedPreceding, currentRow)        OK
  * range(unboundedPreceding, unboundedFollowing) OK
  * range(currentRow, unboundedFollowing)         OK
  * range(0, 0)                                   OK
  * range(unboundedPreceding, -1)                 **FAIL**
  * range(-6, unboundedFollowing)                 **FAIL**
  * rowsBetween(任意整数)                          OK(ROWS 数行位置,与排序列
    类型无关)
  * 默认 frame(只写 orderBy)                      OK
- 实现细节:Window.currentRow 的值就是 **0**,unboundedPreceding 是 -2^63;
  PySpark 把这些哨兵值映射回符号边界。所以 rangeBetween(-6, 0) 里的 0 **早就是
  符号**了——你以为写了两个数字,其实只有 -6 是真的数值边界。报错里的
  `specifiedwindowframe(RangeFrame, -6, currentrow$())` 正好印证:左边是裸数字
  (BIGINT 字面量),右边是 currentrow$()(符号,无类型)。
- **DSL 的修法**:把排序列换成数值型的"天序号":
  `F.datediff("sale_date", F.lit(date(1970,1,1))).cast("long")`,或内置的
  `F.unix_date("sale_date")`(3.1+,返回 epoch 天数);INT/BIGINT 都能过。
  加这一列是窄投影,**不产生 shuffle**——实测整个查询仍然只有一个 Exchange,
  两个 window 共用同一次 hashpartitioning + 同一次 Sort。
  **不要**走"转 timestamp 再 cast long"那条路当日粒度用——那是**秒**,-6 会
  变成 6 秒(单位陷阱)。
- **SQL 那半边不用改**:`ORDER BY sale_date RANGE BETWEEN 6 PRECEDING AND
  CURRENT ROW` 直接可用。`INTERVAL 6 DAYS PRECEDING` 这种形式**只存在于 SQL 的
  frame 子句**里,`F.expr("INTERVAL 6 DAYS PRECEDING")` 不是合法表达式
  (PRECEDING 是 frame 语法关键字,不属于表达式语法),rangeBetween 也不收
  Column 类型的边界——传 Column 进去会在参数归一化时撞
  CANNOT_CONVERT_COLUMN_INTO_BOOL。
- **不要用数仓里常见的 date_key(yyyymmdd 整数)当 RANGE 排序列**:类型上过得去,
  但算术是错的(20260601 - 6 = 20260595,不是 5 月 26 日),跨月边界静默算错、
  月中完全正确——又一个"碰巧正确"的坑。RANGE 排序列必须是**均匀刻度**的
  (epoch 天 / epoch 秒),不能是编码型整数。

## 稀疏时间序列上的移动平均 (Day 15)
- **"前一行"和"前一天"在稀疏表上是两个概念**,这是整题的题眼。表是稀疏的
  (不营业的日子**没有行**,不是有一行 0),所以:
  * ROWS frame / lag(n) 数的是**物理行**,缺口之后会伸得太远。
  * RANGE frame 按 orderBy 的**值**界定,才等于"最近 7 个日历日"。
  * 这个 bug 在**连续**数据上两者完全一致——只有缺口旁边的行分叉
    (Day 15 的 10 行里只有 3 行会暴露)。同 Day 11 时区方向 / Day 13 间隔方向
    的伪装家族。
- **frame 也可以当"按值查找"用**:rangeBetween(-1, -1) 就是"前一个日历日",
  两个边界钉在同一个偏移上,frame 从"滑动区间"退化成"点查"。
  空 frame -> 聚合返回 **NULL,不是 0**,所以外面的 COALESCE 是**有实际作用**的
  (和那些被聚合-NULL 规则判为冗余的防御性 COALESCE 不是一回事)。
- lag **没有 frame 可以修**(位置函数忽略 frame),所以稀疏数据上要么用
  range-framed 聚合,要么先补齐日期轴,**绝不是**给 lag 加 frame。
  lag 的第三个参数 default **也不是**给缺口用的:它只在分区头部(真的没有前
  一行)生效;缺口之后前一行是存在的,default 不触发,你拿到的是几天前的值
  ——这个误用在首行上表现完全正确,很隐蔽。
- 用 lag 走通的正确写法是**把日期也 lag 出来自己校验**:
  lag(sale_date) 后判断 datediff(sale_date, prev_date) == 1。分区首行的
  prev_date 是 NULL -> datediff 返回 NULL -> when 条件不成立 -> 走 otherwise,
  所以**不需要**再加 isNotNull(那是冗余防御代码)。保留意见:那个
  `datediff == 1` 是可删除的不变量,下一个人看到"lag 已经取到前一行了"很容易
  把校验删掉,而测试在连续数据上照样绿。**正确性写在窗口规格里,优于写在一个
  if 里。**
- **AVG over 稀疏 RANGE frame 的分母是数据属性,不是常量**。frame 决定的是
  "哪些行进来",而 avg 的分母是"进来了几行"——这是行数概念,不是值区间概念。
  拆开看:SUM 对缺行**免疫**(缺一行 = 加 0,与"有一行 0"结果相同);
  AVG/COUNT 对缺行**不免疫**(它们靠数行数活着)。所以"分母恒为 7 个日历日"
  只能自己写 sum/7,frame 帮不上忙。语义上这是两个都合法的指标:
  sum/7 = 每个**日历日**平均营收(含歇业日);avg = 每个**营业日**平均营收。
  推论:如果先补齐日期轴让表变稠密,avg 就变成对的了——**avg 的正确性取决于
  表的稠密度**,这与 ROWS/RANGE 之争是同一件事的两面。这条是聚合-NULL 规则的
  同族:聚合忽略 NULL,同样也忽略"不存在的行";对 SUM 无害,对 AVG/COUNT 就是
  静默改语义。
- 除法要**显式**守卫(WHEN prev > 0),不要指望 x/0 返回 NULL:只有 ANSI 关闭时
  Spark 才返回 NULL,ANSI 打开会抛 DIVIDE_BY_ZERO(同 Day 14 规则——来自失败
  路径的 NULL 是 session 配置,不是语言保证)。
- 成本:**一个 partitionBy 表达式 = 一个 Exchange**,无论上面挂多少个不同 frame
  的 window,它们共用同一个 Exchange 和同一次 Sort。frame 是在已排好序的分区
  内部求值的,不额外收费。用 .explain() 数 Exchange 确认。
- **同一份解法里,一个 COALESCE 是承重的,另一个是死代码**(Day 15 AI review):
  * prev 那个 rangeBetween(-1,-1) 的 frame **可能为空**(前一天没有行)->
    sum 返回 NULL -> COALESCE **承重**。
  * ma7 那个 rangeBetween(-6, currentRow) 的 frame **永远至少包含当前行**
    (currentRow 是边界之一,而当前行必然存在;revenue 按规格非 NULL)->
    sum 永不为 NULL -> COALESCE 是**死代码**。
  判据不是"这是个聚合所以可能 NULL",而是**这个 frame 能不能为空**:
  边界含 currentRow -> 至少一行 -> 非空;两端都是有限偏移且不含当前行
  (如 (-1,-1))-> 可能为空。这是聚合-NULL 规则在 **frame 维度**上的细化。
- 内置函数优先:把日期转成天序号,`F.unix_date(col)`(3.1+)就是干这个的,
  手写 `datediff(col, lit('1970-01-01'))` 是同一件事的手搓版(SQL 侧对应
  `unix_date(sale_date)`)。功能上等价、代价相同,但它是 Day 14
  "有格式/专用内置函数却手搓"那条启发式在日期家族里的又一次触发。
  (顺带:epoch 这个锚点是任意的——同一分区内只有**差值**有意义,
  换成任何固定日期结果都一样;这也说明它本质上就是"给我一个均匀刻度的整数轴"
  这个内置语义。)

## Python UDF: 注册、序列化、成本 (Day 14)
- returnType 在实践中是**必填**:省略会默认成 StringType(),于是一个产出 int 的
  UDF 静默返回**字符串**,tuple 比较挂在**类型**上而不是值上。永远写
  `@F.udf(IntegerType())`。
- UDF **不感知 NULL**。NULL 以 Python None 的形式进来,函数体照样执行——不短路、
  不自动传播。原生表达式会替你传播 NULL;UDF 必须显式防御,否则在 executor 里抛异常。
- 类内 UDF 的装饰器顺序:@staticmethod 在**外**,@F.udf 在**内**。反过来 F.udf
  包住的是 staticmethod **描述符**而不是函数(3.10 之前不可调用)。static 对
  **序列化**很关键:实例方法会拖着 self,导致整个对象被 pickle 到 executor,
  任何不可 pickle 的成员(如一个 SparkSession 引用)都会引发 PicklingError。
  模块级函数是作用域最小的默认选择;类内也可以,但必须在 review 中说得通它是 static。
- @F.udf **只**注册给 DSL。不经 spark.udf.register 就在 SQL 里调用会抛
  UNRESOLVED_ROUTINE。两种注册形态:
  * register(name, udf_object)        -> returnType **来自** UDF 对象
  * register(name, fn.func, retType)  -> `.func` 剥回原始 Python 函数,
    所以此时 retType **必填**
  同时传 UDF 对象**和** returnType 会抛 CANNOT_SPECIFY_RETURN_TYPE_FOR_UDF
  (Spark 拒绝在两个返回类型之间做仲裁)。register() 本身**也返回**一个可用的
  UDF 对象,所以一次调用可以同时服务 DSL 和 SQL。注册作用域 = 当前 SparkSession;
  SQL 名字不必和 Python 名字相同。
- 成本模型:BatchEvalPython 把行序列化到 Python worker 再传回(pickle 往返),
  是**优化器屏障**(谓词无法下推穿过它、不做常量折叠),并且让该列无法参与
  whole-stage codegen。pandas_udf -> ArrowEvalPython:向量化批处理,每行开销
  低得多,但对优化器同样不透明。规则:**只有在没有任何原生表达式可用时**才伸手
  去写 UDF——Day 14 那题里 str_to_map / split / size / element_at 原生就能全覆盖。

## 字符串切分的精确行为 (Day 14)
- `"".split("&")` 返回 `[""]`——长度 **1,不是 0**。空 payload 必须在 split
  **之前**拦截,不能靠 len(split(...)) 去算 n_params。对比无参形式:
  `"".split()` 返回 `[]`。同一个方法名两种行为,极易混淆。
- `"nosep".split("&")` 返回 `["nosep"]`——一个元素,永不报错也永不返回空列表。
  单参数的 payload 不需要特判;**只有空字符串需要**。
- 正则量词方向是空值陷阱:`tier=([^&]+)` 要求 >=1 个字符,所以 `tier=`
  **匹配不上**,落到 'UNKNOWN' 分支——但规格里 'UNKNOWN' 是留给"键**缺失**"的。
  `+` -> `*` 才能匹配空值,group(1) 得到 ""。
- 未锚定的键正则会**误匹配后缀**:`tier=` 会在 `user_tier=gold` 内部匹配上。
  用 `(?:^|&)tier=([^&]*)` 锚定,或干脆放弃正则,改用 split('&') + partition('=')
  再**精确**比较键——后者在 review 中更好读,而且整类边界 bug 直接消失。
- `split(str, sep, limit)`:第三个参数是 Python str.partition 的 SQL 表亲。
  limit=2 能让含分隔符的值保持完整("a=b=c" -> ["a", "b=c"])。

## 高阶数组函数: transform 什么时候才是对的工具 (Day 14)
- 家族与 Python 对应:transform ~ map()(数组 -> 等长数组);filter ~ filter()
  (数组 -> 更短的数组);aggregate ~ reduce()(数组 -> 标量);exists / forall ~
  any() / all()(数组 -> 布尔);zip_with ~ zip()+map();map 用
  transform_keys / transform_values。
- 核心价值:**在数组 grain 上做逐元素处理而不用 explode**——和 Day 4 的
  "不 explode"纪律相同,避免 explode -> 处理 -> collect_list 的往返。
- 决策顺序,严格按此:
  1. 有没有专用内置函数? -> 用它(str_to_map, array_distinct, array_sort,
     array_max, array_contains)。**不要手搓**。
  2. grain 需要改变吗(一个元素 -> 一行)? -> explode。
  3. 都不是,而且数组形状必须保留 -> transform / filter。
- 反模式:用 transform 去归约成标量(该用 aggregate / array_max);
  transform 成布尔数组再 array_contains(true)(该用 exists);
  对一个刚 split 出来的字符串用 transform,而其实有格式专用内置函数能一把解析。
- AI 漏掉的前提:高阶函数适用于**数组本来就是这一列的自然形态**的场合。
  为了能用 transform 而手动把**字符串** split 成数组,本身就是"跳过了解析型
  内置函数"的信号。
- 同一个表达式内的**下标基准不一致**:数组下标 arr[i] 是 **0-based**,
  element_at(arr, i) 是 **1-based**。在一个表达式里混用 `kv[0]` 和
  `element_at(..., 1)` 合法且能跑通,但在 review 中是实打实的可读性缺陷。

### 空集合单位元 —— 本主题的靶心 (Day 19 实测)
- `forall([]) = true`(AND 单位元)、`exists([]) = false`(OR 单位元)、
  `size([]) = 0`、`aggregate([], z, ...) = z`。**四个里三个符合直觉,只有
  `forall` 不符合**——这就是它成为唯一被写坏的那个的原因。实测
  `SELECT forall(array(), x -> x), exists(array(), x -> x)` -> `true, false`。
- **"空数组"、"空分组"、"LEFT join 后的缺行"是三个不同的对象、三套不同的默认值。**
  同一句"对 S 中所有 x":`forall` 在 `S = []` 上给 **true**;`bool_and` / `min`
  在**零行分组**上给 **NULL**;LEFT join 之后那一行**根本不存在**。写之前先确认
  自己身处哪一种编码。呼应 Day 4(空数组行在 explode 下消失)与 Day 16
  (`COUNT(*)` vs `COUNT(col)`)——Day 19 是同一问题的第三种编码,而且**符号翻转**:
  漏掉的行是响的,错掉的布尔是哑的。
- 由此:`size(active) > 0 AND forall(active, ...)` 里的第一个连接词是**承重**,
  不是防御性代码。判据仍是 Day 15/16 那条——"去掉它,**具体哪一行**会变?"
- 反过来,同一个守卫在 explode + 条件聚合的路线里是**死代码**(那边由
  `COALESCE(..., false)` 承重)。**"要不要防御由你选的结构决定,不由规格决定"
  第三次成立**(Day 15、Day 16、Day 19)。

### aggregate 的 zero 值钉死累加器类型 (Day 19 实测)
- merge lambda 必须**恰好**返回 zero 的类型,**没有隐式加宽**。三种写法全部在
  分析期抛 `AnalysisException [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]`:

  | 写法 | 报错里说的 zero 类型 |
  |---|---|
  | DSL `F.aggregate(arr, F.lit(0), lambda a,x: a + <double>)` | `INT` |
  | SQL `aggregate(arr, 0, (a,x) -> a + <double>)` | `INT` |
  | SQL `aggregate(arr, 0.0, (a,x) -> a + <double>)` | **`DECIMAL(1,1)`** |

  最后一行是坑:**SQL 里裸字面量 `0.0` 不是 double,是 `DECIMAL(1,1)`**。
  正确写法:DSL `F.lit(0.0)`,SQL `CAST(0 AS DOUBLE)` 或 `0.0D`。
- 这是**响的失败,不是静默的**——分析期就死,根本跑不到数据。但它是 `aggregate`
  最常见的第一次报错。

### DSL lambda 里取 struct 字段:用下标,不用属性 (Day 19 实测)
- `lambda x: x["status"]` 与 `lambda x: x.status` 通常等价(后者走
  `Column.__getattr__`),但**字段名撞上 Column 的方法名时,属性形式拿到的是方法**。
  实测字段名为 `cast` 时:`lambda x: x.cast` 抛
  `PySparkValueError [HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN]`,
  `lambda x: x["cast"]` 正常返回 7。撞名清单不止 `cast`:`alias` / `name` /
  `desc` / `asc` / `when` / `over` / `between` ... **默认用下标形式。**
- 数组参数可以直接传列名字符串:`F.filter("items", lambda x: ...)`,不必 `F.col`。
- **字面量不需要手动 `F.lit`**:`x["status"] == "ACTIVE"` 与
  `x["status"] == F.lit("ACTIVE")` 生成**完全相同**的表达式(`Column.__eq__`
  自动包 lit)。写 `F.lit` 不增加任何健壮性——Day 19 的 review 里这一点被误判成
  "AI 的写法更稳",真正更稳的是它的**下标取字段**,不是它的 `F.lit`。

### HOF 路线 vs explode 路线的计划形状 (Day 19 实测)
- **反范式数组上,HOF 路线是 0 Exchange 的**:任何"每 key 一个度量"只要写得成
  "一行内对一个数组的函数",整条查询就是 `Project` over `Scan`,没有 stage 边界。
  实测三条路线:HOF **0** / `explode_outer` + 条件聚合 **1** / `explode` + WHERE
  + groupBy + LEFT join 回卷 **2** + broadcast。
- **但这个优势属于"布局",不属于"高阶函数"**:数组已经和它的 key 同置,分组是
  写数组的人替你做完的;`explode` + `groupBy` 是把一次免费的分组**重新买一遍**。
  行项目一旦独立成表,explode 路线就是唯一路线。
- B 与 B' 的差距不是抽象的"多一次 join shuffle":是 `WHERE status='ACTIVE'`
  **摧毁了一个分组**,重建完整 key 集要多扫一次源表加它自己的 Exchange。把 ACTIVE
  判定从 WHERE 推进聚合里(`count(when(act,1))`)就同时删掉了 join 和那次 shuffle。

## ANSI 模式决定"越界/失败"的行为 (Day 14)
- element_at、数组下标 arr[i]、cast、除零、算术溢出:在
  spark.sql.ansi.enabled 下会**抛异常**;ANSI 关闭时静默返回 **NULL**。
  ANSI 在 Spark 4.x **默认开启**、3.x 默认关闭,所以同一段 SQL 在不同版本/会话
  之间**失败类别**会变。
- 因此任何 `COALESCE(risky_expr, fallback)` 都携带一个隐含假设:risky_expr 会
  **产出 NULL**。在 ANSI 下它根本没有机会——作业在 COALESCE 执行之前就死了。
  AI 写的 COALESCE(UPPER(element_at(filter(...), 1)[1]), 'UNKNOWN') 就是这样在
  没有 tier 键的行上炸掉的:filter -> 空数组,element_at(空, 1) ->
  ArrayIndexOutOfBoundsException。
- **try_\* 家族是显式索要 NULL 语义的方式**:try_element_at / try_cast /
  try_divide / try_add / try_subtract / try_multiply / try_sum / try_avg。
  它们按构造就与配置无关——**只有内层换成 try_\* 之后,fallback 模式才成立**。
- MAP 取值**不属于**这个家族:props['missing_key'] 即使在 ANSI 下也返回 NULL
  (Day 6 的规则仍然成立)。这也是 str_to_map 路线永远不会有这个 bug 的又一个
  原因——手搓的数组路线**制造**了一个内置函数根本不可能有的失败模式。
- 用 `size(kv) = 2 AND kv[0] = 'tier'` 靠 AND 短路来保护下标访问是**运气,不是
  契约**:SQL 不保证 AND 的求值顺序。Catalyst 通常会短路,但**永远不要**把安全
  建立在它上面。用 try_element_at,或用一个根本不可能越界的内置函数。

- **显式 cast 到目标类型与"ANSI 合规"无关**(Day 23 review 实录,纠正一个常见
  误读):`cast('double')` 不会让任何东西变得更安全。ANSI 管的是**非法输入该抛
  还是该返回 NULL**;把一个已经是 double 的列再 cast 成 double 是 no-op
  (`SimplifyCasts` 直接消除,实测 optimizedPlan 里没有 Cast 节点)。ANSI 审查栏
  的正确问题始终是"**这一步会不会抛**",不是"**类型写清楚了没有**"。一整天的
  表达式全是 string<->string 比较时,这一栏的正确答案就是"无可标记项"——给一个
  不存在的风险记功,和漏掉一个真实风险一样是 review 噪音。

### 字符串 cast 的目标类型是 parse 的一部分 (Day 20 实测)
- `try_cast(s AS INT)` 与 `try_cast(try_cast(s AS DOUBLE) AS INT)` 是**两个不同
  的 parser**,不是"同一个 parser 后面挂了一次转换"。string→整数类型只接受
  **整数字面量**(可选符号 + 纯数字);numeric→整数类型才是截断 + 范围检查。
- 实测同一批串在不同目标类型上的结果(ANSI=true,`try_cast`):
  `'12.0'` → int **NULL** / decimal 12.00 / double 12.0;`'12.5'`、`'1e3'`、
  `'.5'`、`'12.'` 同样只有 int/bigint 判 NULL。`' 7 '` → 7(cast **会** trim)、
  `'0012'` → 12、`'+8'` → 8。
- **这是 3.x → 4.x 的静默行为变更**:`ansi=false` 时 `cast('12.0' AS INT)` 返回
  **12**(走宽松的"先解析再截断"路径),`ansi=true` 抛 CAST_INVALID_INPUT,包上
  `try_` 则是 NULL。同一段代码在 3.x 跑对、搬到 4.x **静默丢数**,单元测试不报错。
  注意非 ANSI 的宽松也不是无边界的(`cast('1e3' AS INT)` 在两种模式下**都是**
  NULL),所以"把 ANSI 关掉"从来不是修法。
- 第二跳**也必须是 try_**:`cast(3.0E9 AS INT)` 在 ANSI 下抛 CAST_OVERFLOW。
  正则闸门**不会**让一个 cast 变安全——`'3000000000'` 能通过 `^\d+$`。
- 数值 → int 是**截断**且**向零取整**(`-12.9` → `-12`,不是 -13),不是四舍五入;
  要四舍五入得显式 `round()`。这是两步路线第二跳必须自己决定的语义。
- 判"这个小数是不是整数值"的写法:`d == F.floor(d)`(先落到 decimal/double)。
  不做这一步就等于接受"截断"语义;spec 说 whole count 时,`'12.5'` → 12 是
  **偏离规格**(Day 20 里用户与 ref Route A 都踩了,AI 没踩)。

### try_cast 把响的失败变成静默的失败 (Day 20)
(扩展 "API 风格约定" 里 "try_cast 只用于确实脏的数据" 一条)
- 裸 `cast` 在 ANSI 下抛错并**报出具体的坏值**;包上 `try_` 换来的健壮性,代价是
  **销毁了这个诊断信息**。`try_*` 是一个**数据质量决策**,不是健壮性改进。
- 由此:`try_*` 产出的 NULL 与源端真的发来 NULL **是同一个 NULL**,下游再也分不开
  "值缺失"和"值是垃圾"。要区分,必须在**做解析的那一个 projection 里**同时捕获,
  信息一旦流走就没了。
- 反向的实用面(Day 20 靶心之外最有用的一条):**解析即校验**。`try_*` 返回 NULL
  这件事**定义上就是**"这个串不是该类型的合法字面量",所以 `parsed IS NULL`
  可以**同时**充当两个用途——`try_sum(parsed)` 天然跳过它(= 不计入),
  `count_if(parsed IS NULL)` 数它(= 计坏行)。两者读同一列,**不可能不一致**。
- 不要另写校验器。实测手写 `^-?[0-9]+(\.[0-9]+)?$` 与 `try_cast` 在 20 个样本里
  **分歧 5 处**(`1e3` / `+8` / `.5` / `12.` / `' 7 '` 全是 cast 接受、正则拒绝),
  且 `rlike` 对 NULL 输入返回 **NULL 而非 false**——`count_if(NOT valid)` 会让
  那一行既不算好也不算坏,直接从统计里蒸发。
- 但 `try_cast` 接受的东西也比直觉多:`'NaN'` → nan、`'Infinity'` → inf、
  `'1.2e3'` → 1200.0(均实测)。**非 NULL 不等于源端发来了一个 sane 的数**,
  而 nan 会毒化它经过的每一个 SUM。

### to_number / try_to_number 的 format 是定长掩码语言 (Day 20 实测)
- 心智模型:掩码**不是正则、不做清洗**,而是一条按位对齐的模板,套不上就整体判失败。
- 数字位:`9` 该位可空缺(位数少于掩码可以);`0` 该位必须有字符。**两者都不容忍
  位数超出**(`'1234'` 配 `'999'` 是 NULL,不是截断)。
- 字面元素的行为**不一致**,这是最反直觉处:
  * `$` 是**硬性双向绑定**——掩码有它输入必须有,掩码没有输入就不许有。
    **没有"可选"档位**。
  * `,` 是**条件必需**——数字没到千位时可以省(`'12.34'` 配 `'9,999.99'` ✓),
    到了千位却不写就判失败(`'1234.56'` 配 `'9,999.99'` ✗)。
  * 空白反而最宽松:两侧空白自动吃掉,**连中间空格也吃**(`'1 2.34'` → 12.34,
    一个明显损坏的值被静默接受)。
- 符号必须显式声明,且三种不等价(实测):`S` 收 `-` 与 `+`;`MI` 只收 `-`;
  `PR` 收会计式 `<1234>`。位置有意义(前置 `S` 只匹配前置符号)。默认掩码
  **不接受任何符号**,负数必然 NULL。
- 掩码同时决定输出的 `decimal(p,s)` **和业务上限**:`'$9,999.99'` → decimal(6,2),
  五位数金额判 NULL;小数位多了也是直接拒绝(`'1.999'` 配 `'9.99'` → NULL),
  **不四舍五入**。这是一个隐性硬编码假设,review 时属于 Robustness。
- **k 个可选元素需要 2^k 个掩码**。Day 20 实测:8 种单掩码在 7 个真实形态上
  最高只覆盖 3/7;`$`有无 × 逗号有无 = 4 掩码 coalesce 才全覆盖。
  多掩码 `coalesce(try_to_number(x, m1), try_to_number(x, m2), ...)` 是对付
  可选性的标准手法,但组合会爆炸。
- 与"正则去噪 + try_cast"的取舍**不是性能**(Day 20 实测四份计划节点级同构),
  **是谁拥有"合法"的定义**:掩码校验强、拒绝烂形状、但表达不了可选性;
  正则路线支持可选性、但形状不再被校验。两者**没有谁更严格**——
  `'1 2.34'` 只有正则路线挡住,`'1,00,0.5'`(烂分组)两条都放行。

## 物理计划 / explain()
- **`cache()` 减少的是重复计算,不是 Exchange 节点数**(Day 22)。分叉在一个
  未缓存的窗口中间结果上,每条分支都要把整条 windowed lineage 重算一遍——账记在
  **Scan 次数**上(实测某参考路线 Scan **12** 次扫 3 张输入表、Exchange 9),
  不记在 Exchange 上。给中间结果加 cache 之后,下游 join 该有的 Exchange 一个不少。
- **自下而上**读;关键节点:Scan(数据源 + 统计信息质量)、Exchange
  (**每个 = 一次 shuffle**,成本主因)、join 节点(策略 + BuildLeft/Right 侧)、
  HashAggregate 对(partial/final = map 端预聚合在起作用)。
- AdaptiveSparkPlan isFinalPlan=false 表示这是执行前的静态计划;对**同一个**
  DataFrame 对象触发一次 action,再 explain() 才能看到 AQE 的最终计划
  (Initial vs Final 的差异 = AQE 的运行时决策:join 转换、AQEShuffleRead 合并
  分区)。
- 优化器会插入你没写的算子(join 的 null-rejecting 侧上的 isnotnull 过滤、
  列裁剪的 Project)。
- 产生 shuffle 的算子:groupBy/distinct/dropDuplicates(有 partial agg 缓解)、
  非 broadcast join(两侧)、Window.partitionBy、orderBy、repartition、
  intersect/except。窄依赖(无 shuffle):select/filter/withColumn/explode/
  union/coalesce(缩小)。
- 相同分区的复用由 ENSURE_REQUIREMENTS 判定。**~~子集/超集 key 仍然要重新
  shuffle~~ 这句是错的**,已在 Day 16 实测推翻:**只有超集方向要重新 shuffle**,
  子集方向是免费的。完整规则和实测见下面"Exchange 复用的判定"一节。
- **窗口相关的 AnalysisException 先读 plan 片段,不要只读错误文案**:报错自带
  的 logical plan 里 `windowspecdefinition(...)` 直接告诉你窗口**实际**按什么排序、
  frame **实际**是什么,比文案准得多(Day 15:文案说"DATE 不支持 BIGINT",
  plan 才显示出窗口仍按 sale_date 排序、day_num 白加了)。
- **走 HashAggregate 还是 SortAggregate,由聚合 buffer 的类型决定,不由 key 决定**
  (Day 17 实测,扩展 Day 16 的 `min(struct(...))` 记录):隔离 A/B——grouping key
  完全不变,只改聚合列表——`min(date)` / `min(double)` -> HashAggregate ×2;
  加一个 `min(STRING)` -> **SortAggregate ×2**(变长 buffer 进不了定长的
  unsafe row)。修法:当那些列在组内**本来就是常量**时,把它们从 agg 挪进
  **grouping key**,HashAggregate 就回来了(实测复现)。所以"字符串 min/max"
  是读计划时的一个具体触发点,不只是 struct。
  **第三个算子:`collect_set` / `collect_list` 走 `ObjectHashAggregate`**
  (Day 18 实测:collect_set 路线全程 ObjectHashAggregate x4,对照路线全程
  HashAggregate)——buffer 里保存任意 JVM 对象,超过
  `spark.sql.objectHashAggregate.sortBased.fallbackThreshold` 后回退到 sort-based
  溢写。至此三个算子、三种 buffer 形态,**"由聚合函数列表决定、不由 grouping key
  决定"这条规律第三次成立**(Day 16 `min(struct)` -> SortAggregate、Day 17
  `min(STRING)` -> SortAggregate、Day 18 collect_set -> ObjectHashAggregate)。
- **聚合的输出有序性可以被下游 window 复用**(Day 17 实测):`SortAggregate` /
  `HashAggregate` 之后如果接一个 window,该 window 的 `orderBy` 是 grouping key
  的**前缀**时不插新的 `Sort`,否则要插。Day 17 里用户按 `boundary`(= grouping
  key 之一)排序 -> 2 个 Sort;AI 按 `effective_from`(聚合产物)排序 -> 3 个 Sort。
  两者 Exchange 都是 1,**唯一的计划差异就是这个 Sort**——又一次"别把结论记成
  shuffle 数量,要记成每个 stage 的重量"。
- **表达式复用 ≠ 计算复用:Catalyst 不会替你 CSE 掉一个重复写下的子树**
  (Day 19 实测)。把 `active = F.filter("items", ...)` 存进 Python 变量再用 5 次,
  和**抄 5 遍是同一件事**——实测 `executedPlan` 的单个 Project 里 `filter(items`
  出现 **5 次**。
  * **机制**:五份拷贝的 lambda 变量 ExprId 各不相同(`x_7#18` … `x_7#22`),
    因而彼此**不 `semanticEquals`**——plan 层 CSE 无从下手,运行时的
    `spark.sql.subexpressionElimination` 也只补回一部分。
  * **`withColumn("active", ...)` 命名一次是安全的,而且原因反直觉**:
    `CollapseProject` **主动拒绝**把非廉价表达式内联到多个使用点,所以两层
    Project 会被**保留**(实测 Project x2、`filter(items` x1),而不是被合并掉。
  * **代价实测**(400k 行 x 12 元素,`noop` sink,min of 3):命名版 **0.83s**
    vs 内联版 **1.17s**;关掉 subexpressionElimination 后 **0.73s vs 1.39s**
    ——消重只补回约一半。两边 **Exchange 都是 0**:这是**每行 CPU** 的故事,不是
    shuffle 的故事,又一次印证"Exchange 数相同时要说出真正的机制"。
    (注:命名版还**多**背了一层 `transform`,仍然赢——那层 transform 的代价被
    省下的四次 `filter` 完全覆盖。)
- **eval mode 在物理计划里不可见**(Day 20 实测):`try_divide(a,b)` 渲染成
  **裸 `/`**——`round((net_amount#8 / cast(total_units#9 as double)), 2)`——
  却在除零时返回 NULL 而不抛异常。`try_divide` 下降成一个携带 `EvalMode.TRY`
  的 `Divide`,而计划打印器**不显示**这个字段。
  **推论:ANSI 安全性的 review 只能在源码上做,永远不能在 explain() 输出上做。**
  这与"性能结论必须数 Exchange"是互补的两条规矩——**同一份 explain,
  对性能是权威,对失败语义是盲的。**

## API 风格约定
- 纯列引用(select/groupBy/on)-> 用普通字符串;当列参与表达式(比较、算术、
  .desc()/.alias()/.cast()、when())、引用链中途新建的列、alias 限定名
  ("t.col")、或程序化生成列时 -> 用 F.col()。自连接消歧用 df["col"](绑定的)。
- DATE_SUB(date, int_col) 优于 date - int(可移植性:MySQL/Hive 行为不同);
  F.date_sub 从 Spark 3.3+ 才接受 Column。
- try_cast 只用于**确实脏**的数据;普通 cast 表达意图。
- count/sum 之后 cast("int"):给生产 sink 的 schema 卫生(count 返回 BIGINT);
  测试两种写法都抓不到。
- LATERAL VIEW OUTER EXPLODE(Hive 风格,兼容性最好)vs 现代的
  LATERAL explode_outer(...)——Spark 3.x 里都可以。
- **`F.try_cast` 不存在**(实测 `hasattr(F,'try_cast') == False`,pyspark 4.1.1)。
  它是 **`Column.try_cast(dataType)`** 方法;而 `F.try_divide` / `F.try_to_number`
  / `F.try_element_at` / `F.try_sum` **是** functions。SQL 侧则统一:
  `try_xxx(...)` 全是函数,外加 **`try_cast(x AS T)` 是语法而非函数**——写成
  `try_cast(x, 'INT')` 报的是 `UNRESOLVED_ROUTINE`("找不到这个函数"),因为它
  根本不是函数。这是 Day 12 `F.grouping_sets` 不存在 / `DataFrame.groupingSets`
  存在的**第二次独立确认**:能力"存在"不等于"在你以为的那个命名空间里"。
- **DSL 里 format 参数有两套互斥的约定,且同族函数不一致**(Day 20 实测签名 + 行为):
  * `format: 'ColumnOrName'` → **必须** `F.lit(...)`:`to_number` / `try_to_number`
    / `to_char` / `to_binary` / `try_to_binary` / **`try_to_timestamp`**
  * `format: Optional[str]` → **必须**裸 str,传 `F.lit` 报 `NOT_ITERABLE`:
    **`to_timestamp`** / `to_date` / `try_to_date` / `date_format`
  * 注意 `to_timestamp` 与 `try_to_timestamp` **约定相反**——给一段跑通的
    `to_timestamp(x,'fmt')` 加上 `try_` 前缀就炸。`try_` 前缀**不能**预测约定。
  * 传裸 str 给 ColumnOrName 那批,报错不是 TypeError 而是
    **`UNRESOLVED_COLUMN`**——它在满世界找一个叫 `'$999,999.99'` 的列。
  * `ColumnOrName` 这个标注是**虚的**:format 必须 foldable,真传一个列会报
    `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT`,所以合法写法只有 `F.lit`。
  * 写之前 `inspect.signature(F.xxx)` 看一眼,两秒钟。**SQL 侧不存在这个问题。**
- **SQL 字符串里的正则有两层转义,写错是静默失效**(Day 20 实测):
  SQL 层 `'\$'` 和裸 `'$'` **都什么也不做**($ 是行尾锚点,匹配末尾空串),
  `'\\$'` 才对。再叠上 Python:普通三引号里写 `'\\$'` → SQL 收到 `'\$'` → 失效;
  要 `r"""..."""` 或写四个反斜杠。**用字符类 `'[$,]'` 可以两层都绕开**,
  零反斜杠且一次剥两种符号。
- **SQL 的 lateral column alias 能力比标准 SQL 宽**(Day 20 实测,Spark 3.4+):
  同层 SELECT 里可以引用前面的别名,**包括聚合别名**
  (`SELECT source, sum(x) AS a, a/2 AS b ... GROUP BY source` ✓),HAVING /
  ORDER BY 里也可以。**但逐行别名不能和聚合混在同一层**
  (`SELECT source, try_cast(u AS INT) AS p, sum(p) ... GROUP BY source`
  → `MISSING_AGGREGATION`)——`withColumn` 在 SQL 里的对应物是 **CTE/子查询**,
  不是别名。依赖 LCA 的代码**不可移植**(Postgres 不允许),属于 portability。

## NULL 语义总表 (Day 16)
- **`=` 在 NULL 上不自反**:`NULL = NULL` 是 UNKNOWN 而不是 TRUE。所以任何
  **可空列作为 join key** 的地方,那些行都会静默地匹配不上。null-safe 相等:
  DSL `a.eqNullSafe(b)` / SQL `a <=> b`(同义于 `a IS NOT DISTINCT FROM b`)。
  它对 NULL-NULL 返回 TRUE、对 NULL-值返回 FALSE,**永不返回 NULL**。
- **四套"相等"机制对 NULL 的态度并不一致**,这才是可空 key 危险的根源:
  * `JOIN ON a = b`      -> NULL 永不匹配(行被丢弃 / 被 null-padding)
  * `GROUP BY col`       -> 所有 NULL 归为**一组**
  * `Window.partitionBy` -> 所有 NULL 归为**一个分区**
  * `DISTINCT` / `dropDuplicates` -> NULL 之间**互相相等**
  一句话:**分组把 NULL 当值,连接把 NULL 当未知**。Day 16 的 SQL 里
  `GROUP BY sku, variant` 和 `ON c.variant <=> a.variant` 同时出现,正是这两种
  语义在同一个查询里各司其职。
- **默认 NULL 排序**:ASC -> NULLS FIRST,DESC -> NULLS LAST。所以一个普通的
  `orderBy(col.asc())` 排名会把**值缺失**的行顶到第一名。改写方式:DSL
  `asc_nulls_last()` / `desc_nulls_first()`,SQL `NULLS FIRST|LAST`。
- **"过滤掉 NULL"和"NULLS LAST"不是可互换的两种修法**。Day 16 里
  best_seller 的规格是"未知价格**不能**当选",这是**过滤**语义:某个 key 只有
  未知价格的 offer 时,NULLS LAST 照样让它 rn=1(输出 'foxtrot' 而不是 'NONE')。
  一旦加了 filter,`NULLS LAST` 就退化成**死代码**——同 Day 15"一个 COALESCE
  承重、另一个是死代码"的判别练习:两个都像在治 NULL,只有一个是承重的。
- **LEFT join 之后 COUNT(\*) vs COUNT(col)**:没有匹配的左行仍然产出**一条**
  物理行,所以 `COUNT(*)` 对空组报 **1**;`COUNT(右表的某列)` 才报 0
  (聚合-NULL 规则,Day 7 / Day 12 同族)。
- **NULL 的责任会随结构转移**。同一个规格,先 join 再 groupBy 时由
  `count(seller)` 吸收 padding 行;改成先 groupBy 再 join 之后,聚合的输入里
  根本没有 padding 行(`count(*)` 就对了),而"catalog 有、offers 没有"的那些
  key 是在 join **之后**才第一次以 NULL 出现,只能靠 `COALESCE(n_offers, 0)` 收。
  **要不要写防御代码,必须先看结构,不能背模板。**
- **同一个 NULL 可能有多个来源**:Day 16 的 best_price 为 NULL 既可能是
  "这个 key 在 offers 里不存在",也可能是"存在但全是未知价格"。本题两种都要
  NULL,所以无害;但要养成问一句的习惯——**这个 NULL 有几个来源?规格对每个
  来源要的是不是同一个值?** 要区分就需要 n_offers 或 grouping 位这类额外信号,
  单凭 `IS NULL` 不够(Day 10 / Day 12 的同一条纪律换到了 join 轴上)。

## 复合类型的比较语义 (Day 17 实测)
- **struct 之间的 `=` / `<>` 是逐字段且 null-safe 的**,和标量完全不同:
  字段里的 NULL **等于** NULL,结果永远是 true/false,**永不返回 NULL**。
  实测(Spark 4.1.1):`struct('a',NULL) = struct('a',NULL)` -> **true**
  (标量的 `NULL = NULL` 是 UNKNOWN);`struct(NULL,NULL) <> struct('a',1.0)`
  -> true;而对照的 `lc=rc AND lp=rp` 在同样五行里有四行是 NULL。
  所以 **`struct(a,b) <> struct(c,d)` 不是 `a<>c OR b<>d` 的等价改写**——
  前者自带 null-safe,后者要靠三值逻辑。排序上 NULL 排最前,逐字段比,
  第一个分出胜负的字段决定结果。
- **"字段全为 NULL 的 struct" ≠ "NULL 的 struct"**:`struct(NULL,NULL) IS NULL`
  返回 **false**。这个区别决定了 lag 放在哪一层:
  * `struct(lag(c), lag(p))` —— 外壳还在,字段是 NULL,分区首行比较得 **true**
  * `lag(struct(c,p))` —— 整个 struct 是 NULL,顶层三值逻辑,首行得 **NULL**
  同一个"封装成 struct"的想法,**lag 在里面还是在外面,首行答案相反**。
- 推论:在 struct 上 `=` 和 `<=>` **只在顶层可能为 NULL 时**才有区别;
  字段级的 null-safe 是 `=` 自带的,不需要 `<=>`。这是 Day 16 那张 NULL 语义
  总表缺的一格——当时只考察了标量层面,没碰过复合类型把 null-safe 内建进
  比较运算这一层。
- SQL 侧还有一个同形写法:**row constructor** `(c1, p1) <> (c2, p2)` 能直接跑,
  行为与 `struct(...) <> struct(...)` 一致(实测)。但它**只是 SQL 解析器的语法**
  ——DSL 里 `(a, b)` 是 Python tuple,没有任何 Spark 语义,会在
  `Column.__bool__` 上抛 `CANNOT_CONVERT_COLUMN_INTO_BOOL`。DSL 里写
  `F.struct(...) != F.struct(...)`。(Day 15"SQL 能跑不等于 DSL 同形写法能跑"
  的又一例,失败方式不同:那次是类型契约,这次是根本不存在这个语法。)

## 哨兵路线 vs `<=>` 路线 —— 四维取舍 (Day 16)
- **哨兵(sentinel)= 用一个值域里不可能出现的普通值顶替 NULL**,让 NULL 从此
  消失,于是 `=` 恢复正常工作(`coalesce(variant,'BASE')`)。这是 Day 10
  "cube 之前预填哨兵"的原样复用:买到一条贯穿全查询的不变量——"这列没有 NULL"。
- 两条路线的完整对比:
  | | 哨兵预填 | `<=>` / eqNullSafe |
  |---|---|---|
  | 撞车风险 | 有(哨兵值可能是合法值) | 无 |
  | 复发风险 | 无(填一次,全查询有效) | **每个** join 都要重写,漏一个就静默出错 |
  | 可读性 | key 列身兼二职,易与输出标签混淆 | 意图直白 |
  | 物理分区可复用性 | **优**(哨兵是真实列) | **劣**(派生表达式,见下节) |
- 哨兵路线的**实操坑**:被填过的 key 列会同时承担"join key"和"输出标签"两个
  身份,最后 select 时很自然就把它当原列输出了(Day 16 里输出成 `variant` 而
  规格要 `variant_label`——按位置比较的测试**抓不到**)。中间列另起名 `vkey`,
  输出时再 alias,可以从结构上消除这个混淆。
- 哨兵字面量必须**只写一次**(抽成 CTE / 变量):`COALESCE(v,'BASE')` 和
  `COALESCE(v,'UNKNOWN')` 看着像同一件事,但对 Catalyst 是**不同的表达式**,
  分区不复用、join 也匹配不上。

## null-safe join 的物理形态 (Day 16 实测, Spark 4.1.1)
- `a <=> b` **不是**一个特殊的 join 算子。Catalyst 把它 desugar 成**两个普通
  equi-key**:`coalesce(col, '')` 和 `isnull(col)`——**它自己做了哨兵预填,
  再补一位布尔标志来防撞车**。计划里直接可见:
  `BroadcastHashJoin [sku#0, coalesce(variant#1, ), isnull(variant#1)], [...]`
- 推论 1:null-safe join **不会**退化成 BroadcastNestedLoopJoin,它是完全正常的
  equi-join,能 broadcast、能 hash 分区。
- 推论 2(取舍里最容易漏的一笔):这对 key 是**派生表达式**,与上游任何
  `GROUP BY sku, variant` 的输出分区**都不匹配** -> 不复用,SMJ 计划里要多插一个
  Exchange 专门为 join 重分区。实测(同一份单次聚合的 SQL,只换 join 写法):
  * 强制 SMJ(AQE off / broadcast off):`<=>` = **3** 个 Exchange,哨兵 = **2**
  * AQE + broadcast(小表):两者都是 **2** —— **broadcast join 对两侧分区没有
    任何要求**,差异被完全抹平
- 所以**哨兵在分区复用上的优势,只有在 join 大到 broadcast 不了时才兑现**——
  恰好是它值钱的那个规模。小表上永远看不见。**这就是为什么这类结论必须关掉
  AQE + broadcast 再读一遍计划。**
- 记账要诚实:省掉的那个 Exchange 搬的是**已聚合**的数据(每 key 一行),是三个
  里最轻的;真正重的是 offers 那次 partial->final。数字站得住,但分量比数字小。

## Exchange 复用的判定 (Day 16 实测) ★核心
实测四组(同一张 offers 表,关掉 AQE 与 broadcast):

| 场景 | Exchange | Scan | 结论 |
|---|---|---|---|
| 1. 两个 `groupBy(sku,vkey)` **兄弟分支**再 join | **2** | **2** | key 全同也不复用 |
| 2. `window(sku,vkey)` -> `groupBy(sku,vkey)` **串联** | **1** | 1 | 复用 |
| 3. `groupBy(sku,vkey)` -> `groupBy(sku)` 串联 | **2** | 1 | 分区键是**超集**,不满足 |
| 4. `window(sku)` -> `groupBy(sku,vkey)` 串联 | **1** | 1 | 分区键是**子集**,满足 |

场景 1 和 2 是**同样两个聚合、同样的 key**,只是排列方式不同 -> 2 vs 1。

**复用需要两个条件同时成立:**

- **条件一:在同一条链上(血缘)。** Exchange 的产物是"一份已分好区的数据",
  只有沿血缘往上的算子拿得到。兄弟分支各读各的 Scan,谈不上复用。
  **读计划的信号:数这张表被 `Scan` 了几次。**场景 1 是 Scan=2,场景 2 是 Scan=1。
  Scan 重复了,底下的 Exchange 一个都省不掉,除非改写查询合并分支。
  第二重佐证是 attribute id(`sku#0` vs `sku#19` = 两次独立扫描)。
  注意复用是**逐个算子**判定的:场景 1 的 join 本身**没有**再加 Exchange
  (总数 2 不是 3),因为两侧都已按 `(sku,vkey)` 分好区。
- **条件二:交付的分区能满足下游的需求。** 各算子的需求:
  | 算子 | 需要的分布 |
  |---|---|
  | `HashAggregate` / `SortAggregate` | 按 **grouping keys** 聚簇 |
  | `Window` | 按 **partitionBy** 聚簇 |
  | `SortMergeJoin` / `ShuffledHashJoin` | 两侧各按 **join keys** 聚簇且互相兼容 |
  | `BroadcastHashJoin` | **无要求** |
  | `Project` / `Filter` | 无要求,原样透传子节点的分区 |

  **方向规则(极易记反):分区键 ⊆ 需求键 -> 满足;分区键 ⊋ 需求键 -> 不满足。**
  * 场景 4:交付 `(sku)`,下游要 `(sku, vkey)` -> **子集,满足**。直觉:
    `(sku,vkey)` 相同的行必然 `sku` 相同,本来就在同一分区里,只是分区更粗。
  * 场景 3:交付 `(sku, vkey)`,下游只要 `(sku)` -> **超集,不满足**。
    `sku` 相同但 `vkey` 不同的行被打散了,必须重新聚拢。
  * **这条推翻了 04 旧记录里"子集/超集都要重新 shuffle"的说法。**
- **匹配是在语义相等层面做的,不是名字**:实测里聚合 key 叫
  `_groupingexpression#36`、join key 叫 `vkey#29`,**名字不同照样复用**
  (alias 透明)。而 `coalesce(v,'')+isnull(v)` 与裸 `v` 是**真的不同的表达式**。
  判断方法:把两处 key **化简到底层表达式**再比。

**写代码之前的预判(不用等 explain,按顺序问三句):**
1. **这几个度量是不是都从同一张表出发?** 是 -> 它们注定各付一次 shuffle,
   除非合并成一个算子。**这是唯一真正能省下大头的动作**,其余都是边角。
2. **能不能合并?** 同一个 grain 的多个度量塞进**一个** `agg`
   (Day 16:`count(*)` + `min(struct(price, seller))`);"排名 + 计数"这类混合
   形态用同一个 `partitionBy` 的多个 window 叠在**一条链**上(Day 8)。
   Day 16 实测合并的收益:Exchange 3->2、源表扫描 2->1、join 2->1。
3. **不能合并的部分,下游的 key 是不是上游 key 的超集?** 是 -> 免费;
   反过来 -> 多付一次。但注意这条的**适用面很窄**,见下一节。

**读计划时的固定动作:**
1. 数 `Exchange hashpartitioning` 总数,数 `Scan` 次数;
2. 每个 Exchange 往上看**紧邻的算子**,确认它要什么、这次为什么不满足;
3. 看到 `BroadcastHashJoin` 就知道**该 join 上的复用讨论全部作废**(它对分区
   无要求)——这也是为什么小表上的计划会掩盖真实代价;
4. **关掉 AQE 和 broadcast 再看一遍**,那才是"大数据下会发生什么"。

**测量陷阱(实测时真踩到):** 第一版测场景 4 时,窗口列没有被下游使用,
**整个 Window 算子被列裁剪删掉了**,量到的是一个假计划(Exchange 显示在
`(sku,vkey)` 上而不是 `(sku)` 上)。**测某个算子的代价时,先确认它的输出真的被
最终结果依赖**,否则你量的是一个 Catalyst 已经删掉的东西。

- **第四次确认(Day 23 实测)**:rank-and-pick 路线的窗口按 `{order_id}` 分区,
  紧接着 `COUNT(DISTINCT)` 的重写需要
  `hashpartitioning(order_date, region, order_id)`——`{order_id}` 是所需键的
  子集,分区要求已满足,**不插 Exchange**(计划里
  `HashAggregate(keys=[order_date, region, order_id], partial_sum)` 直接坐在
  `Filter (rn = 1)` 上方);而修维表路线与 naive 版在同一位置都带一个显式
  `Exchange hashpartitioning(order_date, region, order_id, 4)`。
  **两条路线因此都是 6 个 Exchange,但省下的是不同的那一个**:一个用窗口的
  shuffle 换 distinct-rewrite 的,另一个用 join 的 shuffle 换窗口的。同一个
  总数,两条不同的路。

## 多 grain 汇总的方向性 (Day 16 实测)
- **"上游算粗的、下游算细的"作为一条 groupBy 链是语义上不可能的**:`groupBy`
  **塌缩行**,聚合完 `(sku)` 之后 `vkey` 这一列已经不存在了。实测直接报
  `AnalysisException [UNRESOLVED_COLUMN.WITH_SUGGESTION]`。
- 所以**细 -> 粗是唯一写得出来的方向**,而它恰好落在"超集 -> 不满足"那一侧:
  多层汇总**必然**多付一次 Exchange,且**改不掉**(实测链式 `(sku,vkey)->(sku)`
  = 2 个 Exchange / 1 次 Scan)。
- **那个"免费方向"真正的适用形态是:上游不塌缩行。** 场景 4 成立是因为上游是
  **window**——它给每行**贴一列**,行还在、`vkey` 也还在,下游才有细粒度可聚:
  `withColumn("sku_total", count().over(Window.partitionBy("sku")))` ->
  `groupBy("sku","vkey")`。这是真实模式(算占比分母 / 组内份额)。
  **判据收成一句:能白嫖上游 Exchange 的前提是上游没有把下游要的 key 吃掉**
  ——window / filter / project 可以当上游,groupBy 不行。
- **多 grain 的正解不是重排顺序,是一次扫描**:实测 `rollup(sku, vkey)` =
  **1 个 Exchange + 1 个 Expand + 1 次 Scan**,对比链式 groupBy 的 2 个 Exchange。
  分区键是 `hashpartitioning(sku, vkey, spark_grouping_id)`——grouping id 参与
  分区,所以不同 grain 的行天然分得开。
  接回 Day 10 / Day 12:当时记的是"单次扫描 vs 4 个 groupBy union 扫 4 次";
  今天补上另一半——**改成链式串联虽然只扫 1 次,但省下的是 Scan,省不下
  Exchange;两样都想省只有 grouping sets 家族。**
- 记账:链式那个多出来的 Exchange 搬的是**已聚合**的数据(每 key 一行),很轻。
  别为了躲它去扭曲代码结构——真正值得动手的还是"源表被 Scan 了几次"。

## 数据倾斜:加盐与两阶段聚合 (Day 18 实测)
- **加盐是物理布局干预,爆炸半径却在语义上。** 它不改变算什么,只改变在哪里算
  ——正确性代价**全部**集中在重组步骤:每个度量都必须在"组的任意划分"上可分解。
  判据一句话:**f(A ∪ B) 能否只由 f(A) 和 f(B) 算出?**
  * 可以:`SUM` / `COUNT` / `MIN` / `MAX` / `collect_set`
  * 不可以:`COUNT(DISTINCT)`、`AVG` 作为均值的均值、`MEDIAN` / 任何百分位、
    `FIRST` / `LAST`
  两阶段聚合里**每个**度量都要独立过这道关。Day 18 的 `total_amount`(SUM)和
  `n_products`(DISTINCT)在同一个 agg 里,一个对一个错,输出是**半对的一行**
  ——钱对、基数错(热点键实测 8,正确 4)。而失败**只落在热点键上**,也就是你
  当初之所以要加盐的那个键。
- **不可分解的度量有两条正解**,都不是"再包一层 SUM":
  * 每桶 `collect_set` -> `flatten` -> `array_distinct` 求并 -> `size` 计数。
    集合并可分解,这就是全部内容。`array_distinct` **承重**,不是防御代码
    ——collect_set 只在桶内去重,跨桶正是陷阱本身(Day 15/16"承重 vs 死代码"
    判别练习的又一例,这次答案是**承重**)。
  * 把去重列放进 partial 的**分组键**,让第二阶段做一次真正的 COUNT(DISTINCT)。
- **盐必须是"行"的函数,不能是"键"的函数。** `pmod(hash(join_key), N)` 对同一个
  key 恒定 -> 热点一行没被打散、维度侧还白白复制 N 倍。实测:热点 8 行全落同一桶;
  换成 `pmod(order_id, N)` 才分成 3/3/2。**代码形状完全正确,效果为零。**
- **加盐必然多付一个 Exchange,原因是结构性的。** 加盐后的 stage 按 `(key, salt)`
  分区,最终聚合按 `(key, ...)` 分组 —— `salt` 在分区键里、不在分组键里,子集判定
  必然失败(即"Exchange 复用的判定"一节的场景 3,第三次确认)。**去掉盐正是最终
  阶段存在的意义,所以这笔税任何加盐方案都躲不掉。** 反过来 partial 阶段是**免费**
  的:join on `(key,salt)` 之后 `groupBy(key, seg, salt)` 满足子集,不加 Exchange。
- **实测账**(Spark 4.1.1,`autoBroadcastJoinThreshold=-1`;不关的话 5 行维表被
  广播、两个 join Exchange 全消失,什么都看不见):

  | 路线 | Exchange | 聚合算子 |
  |---|---|---|
  | 不加盐基线 | **2** | HashAggregate x4 |
  | 加盐 + collect_set | **3** | ObjectHashAggregate x4 |
  | 加盐 + product 进 partial 键 | **4** | HashAggregate x6 |

  **不加盐那条 Exchange 最少。** 加盐是拿"多一次 shuffle + 更宽的中间结果"换
  "一个本来会拖垮单 task 的键能跑完";在测试规模的数据上它是**纯开销**——它的
  正确性代价必须由**实测到的倾斜**买单,不能凭反射动手。
- **只给热点键加盐、冷键固定盐 0**(实操优化,参考答案未覆盖):无差别加盐时
  维度侧膨胀恰好是 `dim_rows x N`,一张千万行维表按 N=50 复制是灾难。选择性加盐
  的不变量:事实侧冷键固定给 0,维度侧冷键只复制盐 0(热键才复制 0..N-1)。
  代价是要先算出热点名单,**且两侧名单必须严格一致**——事实侧按热键散成 0..N-1
  而维度侧只复制了 0,那些行会在 inner join 里**静默消失**。
- **AQE 的 skew join 与手工加盐解决重叠但不同的问题。**
  `spark.sql.adaptive.skewJoin.enabled` 挂在 sort-merge / shuffled-hash join 的
  **shuffle 读**上,把超大分区切片并复制另一侧的对应分区——机制上就是运行时版的
  replicate-and-fan-out。它**救不了**:broadcast join(没有 shuffle 可重读)、
  以及**倾斜的 groupBy**(聚合没有"另一侧"可复制)。**聚合倾斜只有手工加盐一条路**,
  而这正是 Day 18 这道题建立在其上的场景。触发门槛是**两道且必须同时满足**:
  分区 > `skewedPartitionThresholdInBytes`(默认 256MB)**且** >
  `skewedPartitionFactor`(默认 5.0)x 中位数。推论:**一张 100MB 表上 5 倍的
  倾斜,AQE 完全不管**——它故意忽略只是"相对"的倾斜。
- **`rand()` 加盐的通行说法是错的**(Day 18 实测):无 seed 的 `F.rand()` 在
  **构造表达式时**就固化 seed 并印进计划
  (`FLOOR((rand(-8997242134998194276) * 3.0))`),所以同一个 DataFrame 反复
  action **值是稳定的**,跨 shuffle 也稳定。它不稳定的是**独立构造的两个
  `rand()` 列**——那永远不会相等。所以真实失效路径不是"task retry 打乱你的
  join",而是:(a) 任何**两侧各自加盐**的方案从一开始就错;(b) 两次运行不可复现,
  错误结果无法 diff;(c) 上面那个不可分解陷阱会变成**非确定性地错**而不是稳定地错。
  `pmod(stable_id, N)` 不花任何代价消掉这三条。附带的性能事实:
  **非确定性表达式挡住谓词下推**——实测同一个 filter,`pmod` 版 Filter 沉到
  Project 之下,`rand` 版留在其上。

## 修正:rank-filter window 有 map 端缩减 (Day 16,限定 Day 7 的记录)
- 旧记录:"window 的第二阶段是对每行做完整 SORT + Window,没有 partial-agg
  缓解"。在 **`row_number() = 1` / `rk <= k` 这类 top-k 过滤**下**不成立**:
  计划里 `WindowGroupLimit [...], row_number(), 1, Partial` 位于 Exchange
  **之下**,`..., Final` 在其上——rank 过滤被下推到 shuffle 前,每个 map 分区
  只送出每个 key 的 top-1。这就是 window 版的 partial aggregation(Spark 3.5+)。
- 触发条件是**存在对 rank 结果的 <= k 过滤**;没有该过滤时旧结论仍然成立。
  Day 7 的"struct-argmax 第二阶段更轻"应降级为
  "**在没有 WindowGroupLimit 的场景下**更轻"。
- 配套发现:`min(struct(...))` 只能走 **SortAggregate**(struct 不是可变定长的
  聚合 buffer 类型),因此 partial / final 各多一个 `Sort`;`sum`/`count` 那支是
  `HashAggregate`。`partial_min` 仍在,map 端缩减没丢——但"struct-argmax 是纯
  聚合所以更轻"要加限定:**它是 sort-based 聚合**。又一次"别把结论记成 shuffle
  数量,要记成每个 stage 的重量"。

## AQE 复现确认 (Day 16,印证 Day 5)
- Day 16 两份计划的 Initial 都是 `SortMergeJoin`、Final 都是 `BroadcastHashJoin`
  ——`Scan ExistingRDD` 没有统计信息,静态阈值不敢 broadcast,AQE 拿到真实大小
  后才转换。**catalog 侧的 Exchange 已经付过了**,只能靠 `AQEShuffleRead local`
  省掉网络拉取。这正是 Day 5 记的成本序:显式 hint < AQE 转换 < 完整 SMJ。

## Review 启发式(累积)
- 在不寻常的上下文里(pivot 的聚合位、自定义聚合),不要信任 star/通配符表达式
  ——解析路径不同。
- 聚合-NULL 规则可以在**两个方向**上裁剪代码:补上漏掉的 NULL 处理,**以及**
  删掉冗余的防御代码。
- 当 explain() 与预期矛盾时,**先看 Scan 节点类型**——统计信息是否可用能解释
  一半的优化器行为。
- 输出的**列顺序**对按位置比较的测试是有意义的;一个重排列的 select 是对下游
  消费者的**静默契约变更**。
- 性能结论用**数 Exchange 节点**来验证,不要凭记忆。
- **"shuffle 更少"是一个诱人但常常错误的性能故事**:当两条路线的 Exchange 数
  **相同**时,真正的差别是每个 stage 的**重量**(可 partial 聚合的 aggregate
  vs 完整 sort)。说出真正的机制,不要断言 shuffle 数量的差异。
- **一个 COUNT 未必是你以为的那个 count**:COUNT(field) 的含义是"field 非 NULL
  的行数",当非 NULL 集合恰好与你想要的集合重合时,它会静默地等于错误的指标
  (COUNT(device.os) == mobile_events 只是因为测试行里非空的 os 恰好全是移动端)。
  按定义从参数的 NULL 行为**重新推导**每个 COUNT 实际在数什么。
- **碰巧正确的输出是伪装**:错误层级的去重可能在某些组上给出正确数字(用户 1
  的三个不同 key 数组 = 三个不同 key)。要**逐个边界行验证机制**,而不是只看
  最终数字。
- **误导性的列名能通过测试,但通不过 code review**:一个装着全部属性 key 的列
  却叫 "devices",是对下游读者的契约谎言。
- SQL 里的否定存在性:默认伸手拿 **NOT EXISTS / LEFT ANTI**,不是 NOT IN。
  NOT IN 要求**两侧**都无 NULL;isNotNull 这个补丁下一个人随手就能删。
  把 NOT IN 留给常量列表,或留给"确实想要三值 NULL 毒化"的场合。
- **"join 前 distinct"的气味**:如果这个 join 是 semi/anti,那 distinct 一定是
  冗余的(存在性忽略重数)。如果是 inner,那 distinct 可能是在补偿一个本不该
  发生的 grain 变化——考虑改用 semi。
- **题面里的边界样例是契约,不是装饰**:Day 10 那一行 region 真为 NULL 的样例
  **就是**整个陷阱。AI 的解法正确处理了小计 NULL,却从未让真实 NULL 浮出水面
  ——它把那一行当成了插图,而不是一等需求。当题目**恰好**给出一行来演示某个
  边界情况时,在读任何解法之前先把它提升为必须处理的用例。
- **GROUPING_ID 的魔数隐藏着位序假设**:gid.isin(2,3) == "region 被卷起"只有在
  region 是 grouping_id 的最左(最高位)参数时才成立。按列写的 grouping() 形式
  更啰嗦但不需要回忆位序——共享代码里优先用它,grouping_id 留给多维度单 CASE
  的 level 映射。无论哪种,值->含义的映射必须钉在**参数顺序**上,而不是表里的
  列顺序。
- **"这是哪一种 NULL"——聚合轴版本**:任何 cube/rollup/grouping-sets 之后,
  grouping 列里的 NULL 默认是有歧义的。要么在聚合**之前**预填哨兵(根治),
  要么每一次维度重建都**先**用 GROUPING()==1 把关——**永远不要**在 grouping-sets
  的结果上单凭 `IS NULL` 重建一个维度。
- **IS NULL 搭着 grouping 位一起出现**(Day 12):`WHEN col IS NULL AND gid=k
  THEN 'ALL'` 在干净数据上能过,但是气味——gid(或 grouping(col)=1)是**唯一**
  的根判据,IS NULL 合取项冗余且在告诉下一个人"NULL 是决策的一部分"。删成
  `WHEN gid=k`。这是 Day 10"永远不要单凭 IS NULL 重建维度"的姊妹条——这里的
  失败方向相反(IS NULL 存在但多余),修法相同:只留 gid。
- **HAVING 其实是 WHERE**(Day 11):在 HAVING 里对 GROUP BY 键做过滤,之所以
  恰好等价于 WHERE,只是因为该谓词碰的是分组键而不是聚合结果。它**能通过**但
  是气味——它依赖"过滤列恰好是分组键"这一点,并且放弃了把谓词下推到聚合之前的
  机会。行级谓词属于 WHERE;HAVING 留给对聚合结果的条件。**靠读就能抓到**:
  问一句"这个谓词作用在原始列上还是聚合结果上?"——原始列出现在 HAVING 里
  就该挪去 WHERE。
- **给排名/位置函数设 frame 是 no-op 气味**(Day 13):看到 rowsBetween /
  rangeBetween 挂在 row_number / rank / dense_rank / lag / lead / ntile 上,
  作者多半误解了 frame——这些函数完全忽略它。frame **只**对
  sum/count/avg/max/min/collect_list 窗口有意义。靠读就能抓到:位置函数上的
  frame 什么也不做,标志着心智模型有缺口。
- **"这个 NULL 是被交付的,还是被假定的?"**(Day 14):把 COALESCE / fallback
  包在一个**可能失败**的表达式(element_at、下标、cast、除法)外面,只有当该
  表达式返回 NULL 而不是抛异常时才是正确的——而这是 **session 配置**(ANSI),
  不是语言保证。把 fallback 当成 NULL 安全的证据,正是这里的错误。问一句
  "这个 NULL 从哪来?";如果答案是"函数失败时返回 NULL",就要求换成 try_\* 形式。
  这是聚合-NULL 规则的**失败模式兄弟**:那条规则因为 NULL 处理有保证而**删掉**
  防御代码,这条因为没有保证而**加上**防御代码。
- **有格式内置函数却手搓解析**(Day 14):AI 用了 5 层
  split/transform/filter/element_at 嵌套去解析一个 query string,而
  `str_to_map(payload,'&','=')` 一行就够。除啰嗦之外还有两笔代价——它在**一个**
  表达式里混用 0-based 下标(kv[0])和 1-based element_at,并且**制造**了一个内置
  函数不可能有的越界失败模式。审查触发点:**一串通用数组原语作用在一个刚被
  split 的字符串上**,就是去找格式专用函数的信号。这是 Day 12 幻觉启发式的
  兄弟条——那次 AI 发明了一个不存在的 API,这次它忽略了一个存在的 API。两者都是
  "你查过标准库里已经有什么了吗"。
- **API 幻觉——找错命名空间**(Day 12;2026-07-27 实测修正本条前一版):AI 在 DSL
  里写了 `F.grouping_sets([...], "region", "category")`。`F.grouping_sets`
  **在任何版本都不存在**(实测 pyspark 4.1.1:`dir(F)` 里含 grouping 的只有
  `grouping` 和 `grouping_id`),运行时 AttributeError,整个作业死掉。
  **但这个能力本身是存在的**——真名 `DataFrame.groupingSets(groupingSets, *cols)
  -> GroupedData`,`versionadded:: 4.0.0`,是 **df 的方法而不是 F 的函数**。
  所以 3.x 里 DSL 确实无路(只能 df.cube / df.rollup 或走 SQL),4.0 起有了
  ——**本条旧版写的"没有 grouping_sets helper"是错的**。
  两层教训:(1) **命名空间**——`F.*` 和 `df.*` 是两个不同的 API 面,能力"存在"
  不等于"在你以为的那个模块里";(2) **命名风格**——PySpark 里 DataFrame 方法是
  camelCase(`groupingSets` / `dropDuplicates` / `withColumn`),functions 模块是
  snake_case(`grouping_id` / `array_agg`);把 SQL 关键字直译成 snake_case 再挂到
  `F.` 上,这两条同时错了。启发式:对没亲手用过的调用,先问"**在哪个对象上、
  什么命名风格**",再 `dir()` 验证。与 SQL 的对称性是诱饵,不是保证。
- **"SQL 能跑"不等于"DSL 同形写法能跑"**(Day 15,与上一条对称):同一件事在
  两套 API 上的类型契约可以不一致。`RANGE BETWEEN 6 PRECEDING` 在 SQL 里对
  DATE 列合法(字面量解析成 INT),而 DSL 的 rangeBetween(-6, ...) 在同一列上
  分析期就失败(Python int 落成 BIGINT)。跨 API 移植窗口/frame 写法时,
  **先在目标 API 上跑一次**,别假设语义等价就意味着类型契约等价。
- **PySpark 的报错文案描述的是异常抛出点的语法现象,不是根因**(Day 15):
  CANNOT_CONVERT_COLUMN_INTO_BOOL 只说明"某个 Column 进了需要真值的位置"——
  除了查自己写的 and / 括号,还要查**传给 API 的参数类型是不是本该是 Python
  标量**(Spark 自己的库函数内部做 `if arg <= x` 时会触发同样的错)。
  相关的 Python 侧硬性习惯:`&` / `|` 的优先级**高于**比较运算符(而 `and`
  低于),所以每个操作数都要**各自加括号**;`&`/`|` 是逐行表达式,**不短路**。
- **一列"百分比"如果和原始金额同步变化,分母多半被约掉了**(Day 15):
  `(revenue - prev / prev * 100)` 因为 `/`、`*` 优先于 `-`,实际等于
  `revenue - 100`。它在某一行上会**碰巧正确**(200-100=100 恰好等于真实增长率
  100%),别的行才暴露。靠读抓的方法:**检查量纲**——增长率不该长得像营收。
- **"多余的列"几乎从来不是性能问题**(Day 15 AI review,严重度归类修正):
  看到一个用不上的中间列(如 SQL 里其实不需要的天序号 CTE),第一反应容易是
  "多算了 = 慢"。**先数 Exchange 再下结论**:实测带天序号列与直接 ORDER BY
  DATE 的 SQL,两者都是 **1 个 Exchange、1 个 Window 节点**,结果一致——
  窄投影不产生 shuffle,代价为零。这类发现属于 **Style/clarity(冗余、
  多一个概念要读)**,不属于 Performance。把它记在 Performance 栏会让
  review 的严重度排序失真:真正的 Performance 问题是多出来的 Exchange /
  多一次扫描 / 单分区窗口,不是多一个 withColumn。
  **限定(Day 19 实测)**:"先数 Exchange"是**必要条件,不是充分条件**。Day 19 里
  两条路线 Exchange **都是 0**,但把 `filter(...)` 内联 5 次的版本慢约 **40%**
  ——纯粹的每行 CPU,Exchange 计数**根本看不见**。完整判据是两步:**先数 Exchange
  (排除 shuffle 级问题);Exchange 相同,再数重复子树 / 每行工作量。** 只做第一步
  会把这一类真实的 Performance 问题误判成 style——方向和 Day 15 那次**正好相反**。
- **同一条发现在两套 API 上的结论可以相反**(Day 15):"这个天序号列是多余的"
  在 **SQL 侧成立**(SQL 的 frame 子句直接吃 DATE 列),在 **DSL 侧不成立**
  (PySpark 的 rangeBetween 必须要数值排序列,见 RANGE frame 类型约束一节)。
  跨 API 的 review 结论要分别落地,不能从一侧推另一侧。
- **`WHEN x = 0 THEN NULL` vs `WHEN x > 0 THEN ...`——分歧在负数上**(Day 15):
  规格写的是"prev_day_revenue 为 0 时返回 NULL",那么 `= 0` 是**字面忠实**的
  实现,而 `> 0` 在 prev 为**负数**(退款日)时会静默返回 NULL,偏离了规格。
  测试数据里没有负营收,两种写法都绿——又一个测试无法区分的分歧。
  读的时候问:**规格排除的是"零",还是"非正"?** 把条件写成规格里那个词。
  (附带:对**计算得来**的 double 做 `== 0.0` 精确比较通常脆弱;这里安全只是
  因为那个 0.0 来自 COALESCE 的字面量而非浮点运算。)
- **用 f-string 拼 SQL**(Day 15):AI 把视图名和 epoch 常量用 f-string 插进
  SQL。这里的值是模块级常量,没有注入风险;但这个习惯本身值得在 review 里点名
  ——一旦被插入的值来自外部输入,它就是注入路径。参数化或固定常量拼接是更稳的
  默认姿势。
- **可空列当 join key = 一级审查项**(Day 16):看到 `ON a.k = b.k` 时先问
  "**k 可空吗?NULL 在这里是缺失还是一个有意义的值?**"。如果 NULL 是有意义的
  键值(如"无变体的基础商品"),`=` 会静默丢掉整整一类实体。**而当 join 是 LEFT
  时,失败表现为"看起来完全合理的行"(计数 0、标签 'NONE'),不是缺行**——
  比丢行更难发现。同族:Day 7 COUNT(device.os)、Day 11 时区方向、Day 13 gap 方向。
- **"没有匹配"和"匹配了但没有可用值"必须能被区分**(Day 16):Day 16 里只有 S2
  那一行(key 存在、但所有价格未知)能把这两种情况分开;naive 写法在**其余每一行
  上都正确**。审查时主动去找**唯一能区分两条假设的那一行**;如果测试数据里没有,
  这个 review 结论就是没有被验证过的。呼应"题面里的边界样例是契约"。
- **两个都在治 NULL 的机制,通常只有一个是承重的**(Day 16,Day 15 的姊妹条):
  `WHERE price IS NOT NULL` 和 `ORDER BY price NULLS LAST` 同时出现时,先问
  "**去掉其中一个,哪一行会变?**"。Day 16 的答案是过滤承重、NULLS LAST 死代码。
  同理 Day 15 的两个 COALESCE。判据不是"这里可能有 NULL",而是**具体哪一行**。
- **防御代码的必要性由结构决定,不由规格决定**(Day 16):同一个"没有 offer 就
  报 0"的规格,先 join 再 groupBy 时靠 `count(col)` 免费拿到,先 groupBy 再 join
  时必须写 `COALESCE(...,0)`。**重构调换了聚合与 join 的顺序之后,要重新过一遍
  所有 NULL 处理**——旧结构留下的守卫会变成死代码(Day 16 里
  `SUM(CASE WHEN seller IS NOT NULL ...)` 就是这样的残迹,而且它在**谎报**
  "seller 可能为 NULL"这个不存在的前提)。
- **同 key 不等于同 shuffle**(Day 16):看到两个 `partitionBy` / `GROUP BY`
  写着完全相同的 key 就断定"共用一个 Exchange"是**错的**。先问"**它们在同一条链
  上,还是两条兄弟分支?**"——兄弟分支各付一次 shuffle **加一次源表扫描**。
  读计划时的落地动作:**先数 Scan 次数,再数 Exchange**。
- **性能结论必须声明它成立的条件**(Day 16):"`<=>` 比哨兵多一个 Exchange"
  只在 SMJ 下成立,broadcast 一来就归零;"window 没有 partial-agg"只在没有
  rank 过滤时成立。**在小数据 + AQE 上读到的计划,系统性地低估分区不匹配的代价**
  ——要下性能结论就关掉 AQE 和 broadcast 再读一遍。
- **测计划前先确认被测算子还活着**(Day 16):列裁剪会删掉输出未被使用的算子
  (实测中整个 `Window` 消失),于是你量到的是一个假计划。**让被测算子的输出
  真正参与最终结果**,再去数 Exchange。
- **"补守卫式"的比较谓词:先数守卫和被比较列是否一一对应**(Day 17)。
  看到 `x.isNull() | (x != y) | (p != q)` 这种形状,作者是在用裸 `!=` 加手工
  NULL 守卫。**只守了一部分列的,就是 bug**:剩下那列一旦为 NULL,整条 OR 链
  塌成 NULL,而三值逻辑**不报错**——它会静默改变下游。Day 17 实测:AI 只守了
  `prev_category` 没守 `prev_price`,一个 NULL price 之后 running SUM 冻结,
  四行塌成一个版本(正确答案是三个)。对照写法(`eqNullSafe` / struct 比较)
  **从构造上就不产生 NULL**,没有守漏的可能。**靠读就能抓到**,而且题面保证
  "属性永不为 NULL"时测试数据**永远**测不到——属于"两边全绿却在缺失输入上
  分歧"那一类(同 Day 15 `=0` vs `>0`)。
- **一个防御分支从不触发,可能不是"防御",而是你没搞清自己的谓词**(Day 17)。
  `F.when(cond, 1).otherwise(0)` 里的 `.otherwise(0)` 只有 cond 可能为 NULL/false
  时才有意义。Day 17 里用户以为这个 `.otherwise(0)` 在兜住首行的 NULL,实测
  首行 flag = **1** —— 因为 struct 比较是 null-safe 的,那个分支是**死代码**。
  落地动作:**别推断谓词的取值,把中间列 `.show()` 出来逐行看**(Day 13 已记过
  "running sum 不对时先看 flag 列",这里是同一动作用于**确认自己的心智模型**
  而不是找 bug)。搞错这一点的代价不是错误答案,是**在 review 里把因果讲反**。
- **看到加盐代码,先问"同一个 key 的行会不会拿到不同的盐"**(Day 18):任何
  `salt = f(...)`,若 `f` 的输入只有 join key / 分组键,它对每个 key 就是常量
  ——热点一行没被打散,而代码形状、计划形状**全都正确**。落地动作:**加盐代码
  通过测试时,把盐换成行级列再跑一次;行数变了就说明第二阶段是缺的。** Day 18 里
  "盐取自 join key"和"缺最终聚合"两个 bug 互相抵消,四行全绿(换盐后 4 行 -> 7 行)。
- **`approx_*` 家族出现在有精确期望值的测试里,判"测试数据分辨不出",不判 pass**
  (Day 18):`approx_count_distinct` / `percentile_approx` / 任何带 `rsd` 或
  `accuracy` 参数的聚合,在低基数上**退化为精确**(实测基数 <=4 时与 countDistinct
  逐行相同,默认 rsd=0.05)。小测试集**永远**分辨不出它与精确版本,而规格要精确值时
  它是一个**契约错误**。同族:Day 15 `=0` vs `>0`、Day 17 的 NULL 守卫——"两边全绿,
  却在测试数据缺失的输入上分歧"。
- **同族的集合谓词,空集合上的默认值可以是相反的**(Day 19):读到任何对集合的
  全称/存在判断(`forall` / `exists` / `bool_and` / `min(bool)` / `NOT EXISTS`),
  先问两句:**这个集合能不能是空的?现在用的是哪一种"无数据"编码——空数组、
  空分组,还是 LEFT join 后的缺行?**(三者默认值分别是 `true` / `NULL` / 行不存在。)
  `exists` 与 `forall` 是 De Morgan 对偶、空集单位元**相反**(false / true),
  任何**对称处理**它们的代码(都不加守卫,或只加一个)都在断言二者行为相同。
  第二个 tell 是纯文本的:规格写了"至少有一个 X **并且** ...",两个子句,而代码里
  只有一个子句。**靠读就能抓到。** 同族:Day 4、Day 16、Day 17——"两边全绿,却在
  某一类输入上分歧"。
- **同一个非平凡表达式被写了 >=2 次,不要假设 Catalyst 会消重**(Day 19):
  看到 `filter(...)` / `transform(...)` / 标量子查询在一个 `select` 里重复出现,
  去 `explain()` 数它在 `Project` 里出现几次,别凭直觉。**PySpark 里"存进变量再用
  几次"和"抄几遍"是同一件事**;HOF 尤其危险——每次调用生成新的 lambda ExprId,
  几份拷贝互不 `semanticEquals`,plan 层与运行时的消重都补不全(实测 5x 内联比
  `withColumn` 命名慢约 40%,而 Exchange 同为 0)。
  **附带的评审动作(本日真实漏检形状)**:用户只比对了**自己那侧**多出来的东西
  (多一层 `transform`,实为 style),没看**对方那侧**重复的东西(5x `filter`,
  实为 perf),于是 Performance 栏记的是自己的心事,不是对方的问题。
  **读对方代码时,先独立数一遍重复表达式,再去比对两版的差异。**
- **`COALESCE` 包住聚合函数时,先问"什么样的分组会让这个聚合返回 NULL"**(Day 20):
  看到 `COALESCE(sum(x), 0)` / `COALESCE(count(...), 0)` 这类**外层包聚合**的写法,
  如果答案是"测试数据里不存在这种分组",那它既不是防御、也不会被测出来——
  它是一个**未声明的语义选择**,应当按 **Robustness** 报出(spec 没说过全坏组该报 0),
  而不是按 style 放过。Day 20 实测:`COALESCE(sum(amt), 0.0)` 在本数据上不可达,
  在"某 source 全部金额不可解析"的输入上把 NULL 变成 0.0。
  **注意与 Day 14 那条区分,这是两种不同的死代码**:
  `COALESCE(risky_expr, fallback)` 在 ANSI 下**不可达**(先抛异常);
  `COALESCE(agg, literal)` **可达**,只是本数据不触发,一旦触发就**改语义**。
  前者要改成 try_*,后者要问 spec。
  (附:`count` 家族空组返回 **0**,`sum` 空组返回 **NULL**——这个差别正是
  `COALESCE(agg, literal)` 会不会被写出来的根源。)
- **一段代码里出现两个"合法性"定义时,先找出至少一个分歧样本再下 verdict**(Day 20):
  看到手写正则 / 白名单 / `IN (...)` 与 `try_cast` / `try_to_number` 出现在**同一条
  表达式链**里,问"**最终判据是谁?**"。两个独立的合法性定义必然在某组输入上分歧,
  常备探针:科学计数法 `1e3`、正负号 `+8`、`.5` / `12.`、前导/尾随空格、
  `NaN` / `Infinity`、超宽值、前导零。**举不出分歧样本 = 还没读懂那个正则**,
  此时正确的动作是把题面 EXAMPLE 框里的输入串逐个代入,而不是在 REVIEW_NOTES 里
  写 "not sure"。同族:Day 15 `=0` vs `>0`、Day 17 的 NULL 守卫、Day 18 的
  `approx_count_distinct`——"两边全绿,却在测试数据缺失的输入上分歧"。
- **Upsert 类作业:先去找那个比较两个时间戳的谓词**(Day 22)。看到目标表带
  `updated_at`、变更流带 `change_ts`,先问:代码里有没有**一处**把这两列放在
  一起比较?
  **有** -> Route A,谓词显式可审;去检查它是 `>` 还是 `>=`、NULL 分支怎么走。
  **没有** -> 只有两种可能:(a) Route B,目标行作为候选进了同一个排序,守卫是
  涌现的;(b) 漏了守卫,维表会被旧数据回滚。
  **区分 (a) 和 (b) 只看一件事:union 的候选池里有没有目标行。**
  这是"缺失的代码"类 finding——reading 时**没有任何一行会亮起来**,只能靠这个
  提问触发。同族:Day 16"没有匹配 vs 匹配了但没有可用值"。
- **看到一串"多余"的 tie-break 列,默认它在买确定性,不是噪音**(Day 22)。
  `row_number()` 的 `ORDER BY` **只有一个键**时,先问:**这个键在 partition 内
  唯一吗?** 不唯一 -> 平局赢家任意 -> 重跑结果可能不同 -> 幂等性约束不成立。
  反过来,看到 `ORDER BY a DESC, b DESC, c DESC, d DESC` 这种啰嗦的全序,判它
  冗余**之前**必须先证明第一个键在 partition 内唯一——而这通常**不能从代码
  看出来,只能从数据契约看出来**。Day 22 实测:AI 写了五键全序,用户两版与参考
  答案都只有单键;用户把它记进 Robustness 栏判为"可能冗余",**结论正好反了**,
  而那恰是 AI 解法里唯一强于参考答案的地方。这是"多余的列几乎从来不是性能
  问题"(Day 15)的**反向兄弟条**:那条说别把冗余当性能问题,这条说**先确认
  它真的冗余**。
- **每个 join 三问**(Day 23,用户点名收录):遇到任何 join,不看代码风格,
  先答这三句——
  **(1) 右表对 join key 唯一吗?** 不确定就跑
  `df.groupBy(key).count().filter("count > 1").show()`,不要靠"看数据像是
  唯一的"。
  **(2) 如果不唯一,我允许扇出吗?** 有时允许——比如你就是要把一张订单展开
  成多行明细。允许与否是**语义决定**,不是默认值。
  **(3) 如果不允许,收敛动作在哪一步?** 说不出**具体哪一行代码**
  (dedup / rank / 区间修复),就是没有。
  Day 23 实录:这三问同时命中了两个 join——`seller_region` 那个做了(rank),
  `refunds` 那个没做,而后者是用户唯一漏掉的正确性 bug。配套的诊断动作:
  **每个 join 前后各对一次行数**——这是唯一能在数字被聚合吞掉之前抓住扇出的
  时机。
- **对方比你多做了一步时,默认假设是"它防的是一个我没想到的输入"**(Day 23)。
  读到别人的代码多一个 `groupBy` / 多一个 CTE / 多一个 `isNull` 分支时,先答
  "**这一步防的是什么**",再答"**我的代码遇到那个输入会怎样**"。Day 23 实录:
  用户看见了 AI 的 refunds 预聚合,写下 "it can ensure the grain ... more
  rigorous",却把它记进 **Performance 栏当成代价**,没走第二问——走了就会在
  只读阶段发现自己少了这道防线。**code review 的一半价值是发现自己错了**;
  "和我写的一样所以 PASS"这个理由,前提恰恰是待检验的那件事。这是 Day 19
  "读对方代码时先独立数一遍重复表达式"的姊妹条:那条讲别只盯自己那侧多出来
  的东西,这条讲**对方多出来的东西要先当防线读**。
