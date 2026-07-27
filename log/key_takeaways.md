# 累积要点 (Day 1-16)

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
- SIZE(NULL) = -1(遗留怪癖)——可空数组要 COALESCE 兜底。

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
  取**左侧**的值(孤儿 key 得以存活)。表达式 join(a.k == b.k)保留
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

## 物理计划 / explain()
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
