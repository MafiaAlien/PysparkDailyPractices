# 迁移到 Claude Code — 操作步骤

## 1. 落地目录

```bash
mkdir -p ~/pyspark-daily && cd ~/pyspark-daily
git init
# 把本包内容解压到这里
```

把现有文件搬过去：

| 旧文件 | 新位置 | 说明 |
|---|---|---|
| `01_project_instructions.md` | 已并入 `CLAUDE.md` | 原文件可以删掉 |
| `02_template_v2.py` | `templates/template_v2.py` | 已重写，Part 4/5-concept 拆出去了 |
| `03_problem_log_and_roadmap.md` | `log/03_problem_log_and_roadmap.md` | **原样复制**，不要改内容 |
| `04_key_takeaways.md` | `log/04_key_takeaways.md` | **原样复制** |

历史 Day 文件（如果留着）放进 `days/`，参考答案那部分手动拆成
`days/dayNN_<slug>_ref.md`。不想拆就算了，只对新的天数生效即可。

`.gitignore` 建议：

```
.claude/settings.local.json
__pycache__/
spark-warehouse/
metastore_db/
derby.log
```

## 2. 环境自检

```bash
java -version          # 需要 JDK 17+
python -c "import pyspark; print(pyspark.__version__)"   # 期望 4.2.0
```

跑一次冒烟测试，确认 Claude Code 能真的执行 harness：

```bash
python -c "
from pyspark.sql import SparkSession
s = SparkSession.builder.master('local[2]').getOrCreate()
s.range(3).show(); s.stop()"
```

这一步不过，Claude Code 最大的收益（自己跑 `.explain()` 数 Exchange）就拿不到。

## 3. 启动

```bash
cd ~/pyspark-daily
claude
```

第一次进去先跑 `/context`，确认 `CLAUDE.md` 已加载；跑 `/permissions`，确认
`.claude/settings.json` 里的 ask/deny 规则生效。

## 4. 每天的节奏

```
/newday              → 确认选题 → 生成 days/dayNN_*.py 和 _ref.md
/clear               ← 关键，把参考答案清出上下文
（自己写 Part 1，自己跑）
/genprompt 17        → 拿到 prompt，去 web incognito 生成 AI 解
（把 AI 解贴进 Part 3，写 REVIEW_NOTES 和 VERDICT）
/review 17           → Claude 独立 review + 实跑 + 三方对比 + 批改你的 review
/digest 17           → 更新 log/03 和 log/04
```

## 5. 关于「密封」参考答案

`.claude/settings.json` 用的是 **ask** 而不是 **deny**：

- Stage 1–4 你回答 no，Claude 读不到参考答案；
- Stage 5 你回答 yes，`/review` 和 `/digest` 才拿得到。

deny 是一刀切的（deny 优先级最高、不能开例外），会把 Stage 5 也堵死，所以这里
不适用。

**但这只是护栏，不是保证**：权限规则挡的是内置 Read 工具，挡不住 Bash 里的
`cat` / `grep`（settings 里已经补了几条常见形式的 ask，但不可能穷举）。社区也
报过 deny 规则在某些版本下不生效的 bug。真正起作用的机制是两条纪律：

1. `/newday` 之后**立刻 `/clear`**；
2. Stage 2 永远在这个仓库**外面**做。

第 2 条无法内化到 Claude Code 里——参考答案就在磁盘上，任何在本仓库生成的
"独立解"都已经被污染了。这是设计约束，不是待办事项。

## 6. 需要留意的取舍

- **claude.ai 的 project memory 不会跟过来。** 但你的 memory 本来就规定
  「以 `03` 为准」，而 `03` 是文件，所以实际影响接近零 —— 反而变成全部显式。
- **CLAUDE.md 控制在 200 行以内。** 现在约 110 行。不要把 `04` 的内容塞进去；
  它是查阅材料，`/review` 和 `/digest` 里按需加载。
- **Claude Code 的默认倾向是替你把活干完。** `CLAUDE.md` 里的 HARD RULES 就是
  为此存在的。如果哪天它还是伸手改了 Part 1，把当时的具体情形补成一条新规则，
  别写成一句泛泛的"不要帮太多"。
