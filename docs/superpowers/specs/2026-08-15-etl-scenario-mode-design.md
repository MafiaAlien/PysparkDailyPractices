# ETL 场景模式 — 设计文档

日期：2026-08-15
状态：已批准，待实现

---

## 1. 背景与目标

Day 1–20 的出题主轴是"每天引入一个新算子或新语义边界"。到 Day 20 为止，
backlog 里剩下的条目已经开始出现两类退化：

- **低生产价值**：`try_*` 家族、`pandas_udf`、`session_window` 内置、
  `zip_with` / `transform_keys` 家族——真实 ETL 里罕见。
- **Stage 1 退化成 API 教学**：Day 19 和 Day 20 的 trap 维度都因为"用户必须
  先学会这个函数干什么"而在解题前被讲破，当日 trap 信号归零
  （已记录在 roadmap 的 cadence principle 里）。

目标是把出题主轴换成 **真实生产 ETL 作业**：题目内容取自已学过、且在生产
环境里确实高频的技巧，一天完成一段完整的小管线。核心不再是"学会一个新
函数"，而是**把长管线写对、写快，并能审出别人写的长管线里的问题**。

## 2. 已确定的四个决策

| # | 决策 | 选定 |
|---|---|---|
| D1 | 切换时机 | 先清完高价值新方法（unpivot、MERGE INTO）；Day 22 用新模板试水，Day 23 起全面按三维矩阵出题 |
| D2 | 单日形状 | 单日一条完整小管线（3–4 张输入表，1 张输出表，约现规模 1.5x） |
| D3 | 陷阱策略 | 一个暗 trap + spec 里**明写**的 2–3 条生产约束 |
| D4 | 工作流 | 五阶段完整保留（Stage 2 独立 AI 解 + Stage 3 盲审 + Stage 4 验证） |

## 3. 出题矩阵（三个正交维度）

出题时三个维度各取一格，组合记录在 roadmap 的调度表里，保证不重样。

**维度 1 — ETL 层级**（决定这一天练什么形状的作业，主轴）

- L1 落地层清洗：脏字符串规整、去重、schema 对齐、坏行隔离
- L2 明细层建模：维度关联、SCD2 查找、口径统一、事实表打宽
- L3 汇总层出数：多口径聚合、指标计算、宽表落地
- L4 增量与回补：幂等重跑、迟到数据、分区覆写、upsert

**维度 2 — 业务域**（决定题面皮肤，轮换避免连续重复）

电商订单 / 广告投放 / 支付对账 / 用户行为埋点 / 物流轨迹 / 内容消费 /
订阅计费

**维度 3 — 失败模式**（trap 的来源，**永不出现在题面**）

重复投递 / 迟到数据 / 维度漂移 / 口径漂移 / 可空键 / 空集合 / 边界闭合 /
倾斜 / 分解性（decomposability）

## 4. 技巧白名单

以下技巧会在场景题里反复出现，不再作为"当日主题"单独讲解：

| 类别 | 技巧 | 来源 |
|---|---|---|
| 去重 | window `row_number` 取最新、重放计数、`dropDuplicates` 不是契约 | Day 8 |
| 窗口 | Top-N、`lag`/`lead`、running sum、ROWS vs RANGE 移动窗口 | Day 1/13/15 |
| Join | inner/left/anti/semi、`broadcast`、可空键（`<=>` vs sentinel）、孤儿键 | Day 5/9/16 |
| 聚合 | 条件聚合 `SUM(CASE WHEN)`、`GROUPING SETS` 多口径、`COUNT(*)` vs `COUNT(col)` | Day 10/12/16 |
| 日期 | UTC→本地分桶、日期维度 spine 补零、归属日、区间闭合 | Day 11/17 |
| 岛屿 | gap-threshold 会话化、run-based 版本切分 | Day 13/17 |
| 增量 | SCD2 半开区间、`is_current` 派生、upsert | Day 17 + Day 22 |
| 嵌套 | `explode_outer`、`collect_set`、点路径、array\<struct\> 的 `filter`/`size`/`aggregate` | Day 4/6/7/19 |
| NULL | 聚合忽略 NULL、`NOT IN` 三值逻辑、窗口 NULL 排序 | Day 9/16 |
| 形变 | pivot / unpivot | Day 3 / Day 21 |
| 倾斜 | salting + 两阶段聚合、AQE 门槛 | Day 18 |
| 计划 | `explain()` 数 Exchange、broadcast 判定、分区键子集规则 | Day 8/16/17/18 |
| 字符串 | `regexp_extract` / `regexp_replace`、`split` | Day 20 |

### 黑名单（不再出现在任何题目里）

`try_cast` / `try_to_number` / `try_divide` / `try_sum` 及 `try_*` 家族其余成员、
`pandas_udf` / ArrowEvalPython、`session_window` 内置函数、
`zip_with` / `transform_keys` / `transform_values` / `map_filter`。

Python UDF **降级为审查点**：场景里 AI 解可能用它，用户要能在 review 里指出
"这里不该用 UDF"，但不再要求用户亲手实现 UDF。

ANSI 模式本身**不进黑名单**——`cast` / 除法 / 数组下标在 ANSI 下抛异常的行为
仍然影响每一个 `COALESCE(risky_expr, fallback)` 写法，REVIEW_NOTES 的 ANSI 栏保留。
被砍掉的只有 `try_*` 这套 API。

## 5. 生产约束（非算子维度）

每天在 spec 里**公开明写** 2–3 条，编号 `P1` / `P2` / `P3`，轮换使用：

- 重跑幂等 / 分区覆写语义
- 迟到数据的归属日（按事件时间而非到达时间）
- 上游重发去重
- 同一指标多处计算的口径一致
- 数据质量断言（主键唯一、行数、非空率）
- 只读必要分区与列裁剪

它们不是陷阱，是需求；漏做一样挂测试。

## 6. 严重度排序新增一档

CLAUDE.md 的 Q&A style 与 `/review` step 2 的排序统一改为：

```
runtime break
  > wrong results on dirty data
  > production robustness      ← 新增
  > performance
  > portability
  > style
```

`production robustness` = 重跑不幂等、迟到数据丢失、口径不一致、缺质量断言。

放在 `wrong results` 之后的理由：幂等性问题**在首次运行时不产生错误结果**，
只在重跑时暴露，比脏数据错误晚一拍；但它仍然是正确性问题，不能降到
performance 档。

## 7. 文件改动清单

### 7.1 新增 `templates/template_etl.py`

在 v2 基础上改四处（v2 **保留不动**，供 drill 与偶尔的单点题使用）：

**PROBLEM 段拆成四块：**

1. 业务背景 —— 1–2 句，谁在什么系统里要这张表
2. 输入表 —— 3–4 个 `df.show()` 风格 ASCII box，每张注明上游性质
   （append-only 事件流 / 每日快照维表 / 人工维护的配置表）
3. 输出契约 —— 列名 + 类型 + **粒度**（一行代表什么）+ 排序无关
4. 生产约束 —— 编号列出 2–3 条

**丢弃 `class Solution`，改为模块级函数，按输出表命名。**

`class Solution` 是 LeetCode / Codility 平台的产物，`self` 从未被使用，生产里
没有人为一段 ETL 建类。但**函数边界要保留**：输入表以参数注入、输出返回
DataFrame、函数体内不读路径不写盘——这恰恰是生产 ETL "有单测时"的标准形状。
更硬的约束是，完全平铺的脚本只会留下一个 `result_df`，无法让用户解与 AI 解
并存于同一个 harness，D4（保留 Stage 2–4 盲审）会直接落空。

函数名跟随**输出表**，逐日不同：

```python
def build_daily_store_revenue_dsl(
    orders: DataFrame,
    stores: DataFrame,
    fx_rates: DataFrame,
) -> DataFrame: ...


def build_daily_store_revenue_sql(
    spark: SparkSession,
    orders: DataFrame,
    stores: DataFrame,
    fx_rates: DataFrame,
) -> DataFrame: ...
```

参数名即业务表名。`_sql` 版本为每张输入表各建一个同名临时视图。Part 1 仍然
只有这**两个函数**，不拆多步——拆了 SQL 侧对不齐，真实提交的也是一段作业。
AI 解用同名加 `ai_` 前缀。

**文件区块顺序重排 + 显式分隔横幅。**

模板 v2 有一个结构缺陷：`# Part 3 — AI Review` 占位符位于
`if __name__ == "__main__":` **之后**，而 `__main__` 里的 Stage-4 调用需要
`ai_*` 已定义——照字面粘贴会 `NameError`。实际使用中（day19 line 193、
day20 line 218）用户把代码粘在 `__main__` 之前，与横幅和 REVIEW_NOTES
相隔整个 harness。新模板修正为：

```
docstring: PROBLEM 四块 + WORKFLOW
imports

╔══ PART 1 — YOUR JOB ════════════════════════════════════╗
│  Stage 1 只写这一段，本文件其余部分不要改。              │
╚═════════════════════════════════════════════════════════╝
  ── 1a. DataFrame API ──
  def build_<table>_dsl(...)
  ── 1b. Spark SQL ──
  def build_<table>_sql(spark, ...)
╔══ END OF PART 1 ════════════════════════════════════════╝

╔══ PART 3 — PASTE THE INCOGNITO ANSWER BELOW ════════════╗
│  UNMODIFIED。两个函数，ai_ 前缀，签名与 Part 1 一致。    │
│  不要改格式、不要顺手修 bug——那是 Stage 3 要审的东西。   │
╚═════════════════════════════════════════════════════════╝
  # >>> PASTE BEGIN
  # >>> PASTE END

  REVIEW_NOTES  ← 紧贴粘贴区，不再隔着 harness
  VERDICT

Part 2 — harness（check() + __main__，不要编辑）
  含 Stage-4 注释行，此时 ai_* 已定义，取消注释即可运行

Part 5 — Review takeaways
```

**REVIEW_NOTES 增加 Production 栏**，并按新的严重度顺序排列：

```
[ ] Correctness
[ ] API usage
[ ] ANSI behavior
[ ] Production — 重跑幂等？迟到数据归属？口径一致？质量断言？
                 spec 里编号的 P1/P2/P3 逐条对照。
[ ] Performance
[ ] Robustness
[ ] Style/clarity
```

### 7.2 新增 `templates/template_etl_ref.md`

在 `template_ref.md` 基础上增加两节：

- **生产约束逐条核对** —— 每条 `P#` 对应参考解里哪一段代码满足它
- **管线环节分解** —— 参考解拆成有名字的环节（去重 → 维度关联 → 口径聚合
  → 空值兜底），Stage 5 debrief 时按环节对齐三方解法

其余（trap 三段式、Route A/B、Physical plan notes、Echoes）保持不变。

### 7.3 `.claude/commands/newday.md`

- Step 2 选题：从"backlog 挑主题"改为"三维矩阵取一格"，读 roadmap 的调度表
  避免重复组合
- Step 3 STOP 汇报改为：日期号、**ETL 层级**、**业务域**、**明写的生产约束**、
  难度、echo 哪条 takeaway。**不报失败模式**
- Step 4 改用 `templates/template_etl.py` + `templates/template_etl_ref.md`
- 数据规模上限：`≤ 15 行` 改为 **单表 ≤ 8 行，全部输入合计 ≤ 25 行**，
  `expected` 仍逐行手推
- 难度判据改为**管线环节数**：3 环 = Medium，4–5 环 = Medium-Hard，
  6+ = Hard（少用）

### 7.4 `.claude/commands/genprompt.md`

- "What to read" 增加：多张输入表的 schema 全部照抄
- 两行硬编码签名 `ai_solve_dsl(df: DataFrame)` 改为
  **"照抄当日文件 Part 1 的两个函数签名，函数名、参数名与顺序完全一致，
  仅加 `ai_` 前缀"**（函数名逐日不同，不得硬编码）
- 输出块必须包含 spec 里编号的生产约束 `P1/P2/P3`（它们是公开需求，不是提示）
- 不得包含的清单保持不变

### 7.5 `.claude/commands/review.md`

- Step 2 的严重度排序加入 `production robustness` 档
- Step 5c 评分时逐条对照 spec 的 `P#`，明确指出用户和 AI 各漏了哪条
- 其余（先读后跑、explain 实测、log/04 候选）不变

### 7.6 `.claude/commands/digest.md`

- `log/03` 新增行的列改为：`Day | 层级 | 业务域 | Difficulty | Problem |
  生产约束 | Trap / key edge case`。**只对 Day 22 起的新行填写新列，
  Day 1–20 的历史行不回填**——历史行的 `Topic` 列语义与 `层级/业务域` 不同，
  强行迁移会丢信息。做法是在 "Completed problems" 之下另起一张
  "ETL scenario days" 表，两张表并存。
- 同时更新调度表（已用过的三维组合）

### 7.7 `CLAUDE.md`

- Q&A style 的严重度排序加一档
- 新增一节说明 Day 23 起的出题模式，含黑名单
- Repo layout 加入两个新模板文件

### 7.8 `log/problem_log_and_roadmap.md`

由 `/digest` 写入（HARD RULE 6：`log/` 只有 `/digest` 能改）。新增一节
**调度矩阵**，记录已用过的 (层级, 业务域, 失败模式) 组合。

## 8. 过渡计划

| 顺序 | 内容 | 模式 |
|---|---|---|
| drill | unpivot / melt mini-drills | 旧（roadmap 已排，不动） |
| Day 21 | unpivot 正式日 | 旧模式 `template_v2.py` |
| Day 22 | MERGE INTO / upsert | **新模式试水** |
| Day 23 起 | 三维矩阵出题 | 新模式 |

Day 22 用新模式的理由：upsert 本身就是一段生产 ETL（L4 增量层），用一个用户
已经通过 Day 17 熟悉的主题去试新模板，暴露模板问题的成本最低，同时清掉
backlog 最后一个高价值条目。

## 9. 不做的事（YAGNI）

- 不做多日耦合的项目式管线（D2 已否决）
- 不为场景题引入真实文件 IO / 分区写盘——幂等与分区语义靠**输出契约和断言**
  表达，harness 仍然是内存 DataFrame
- 不改 `check()` 的比较逻辑
- 不迁移 Day 1–20 的历史文件
