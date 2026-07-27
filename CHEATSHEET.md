# 每日操作速查

## 一天的完整循环

```
/newday                    Claude 读 log/03 → 提议选题 → 你确认 → 生成两个文件
/clear                     ★ 必须。把参考答案清出上下文
                           ↓
（自己写 Part 1 的 solve_dsl / solve_sql）
python days/dayNN_*.py     自己跑，直到 DSL 和 SQL 都 PASS
                           ↓
/genprompt NN              输出一段 prompt
→ 复制到 web 端 incognito 对话，拿 AI 解
→ 原样贴进 Part 3，不要改一个字
                           ↓
（读代码，填 REVIEW_NOTES，写 VERDICT —— 运行之前必须先下结论）
                           ↓
/review NN                 Claude 也先盲审 → 实跑 → 三方对比 → 批改你的 review
/digest NN                 更新 log/03 和 log/04（唯一会写 log 的命令）
                           ↓
git diff log/key_takeaways.md    ★ 十秒钟，只看新增行
git add -A && git commit -m "day NN: <topic>"
```

`NN` 用两位数补零：`/review 06` 不是 `/review 6`。Day 17 之后 `/newday` 生成的
文件名统一是 `dayNN_slug.py`，旧文件命名不一致，翻旧题时直接说文件名更快。

---

## 自定义命令

| 命令 | 阶段 | 做什么 | 会写文件吗 |
|---|---|---|---|
| `/newday [主题]` | — | 读 log/03 定选题，**先提议再生成**；产出 `days/dayNN_*.py` + `_ref.md` | 是（确认后） |
| `/genprompt NN` | 2 | 只读题面 docstring，输出 incognito prompt。**绝不在本仓库解题** | 否 |
| `/review NN` | 5 | 先盲审 → 实跑 → 读 `_ref.md` → 三方对比 → 逐条批改你的 REVIEW_NOTES | 否 |
| `/digest NN` | 5 | 增量更新 log/03 和 log/04，**先给预览再写** | 是（批准后） |

`/newday` 后面可以跟主题覆盖默认排期：`/newday skew handling`。

---

## 内置命令里用得上的

| 命令 | 什么时候用 |
|---|---|
| `/clear` | 每次 `/newday` 之后。也用于一天做完开新话题 |
| `/context` | 确认 CLAUDE.md 加载了、当前上下文占用多少 |
| `/compact` | 长会话快满时压缩。CLAUDE.md 会自动重新读回 |
| `/permissions` | 确认 `_ref.md` 的 ask 规则生效 |
| `/memory` | 看/改 Claude 自动记下的东西。发现它记了错的就在这删 |
| `/doctor` | 装完或出怪问题时自检 |
| `Esc` | 打断跑偏的执行，比等它跑完再纠正省事 |

---

## 常见偏离

**想换今天的主题** —— `/newday` 会先提议再动手，直接在那一步说「换成 X」。
它判断某个排期主题面试价值低时也会主动提议换。

**补做很久以前某一天的 review** —— 直接 `/genprompt 06`，密封规则只对进行中的
题目有意义，已完结的天数它读 `_ref.md` 无所谓。

**Claude 伸手改了 Part 1** —— 立刻 `Esc`，`git checkout -- days/dayNN_*.py`，
然后把这次的**具体表现**补成 CLAUDE.md 里第 7 条硬规则。不要写「不要帮太多」
这种泛泛的话，写清楚它做了什么、什么时候不该做。

**`/digest` 之后 diff 里出现大段无关改动** —— 它把 log/04 整篇重写了。
`git checkout -- log/key_takeaways.md` 退回去，让它改用定向 edit 重做。

**它凭记忆下性能结论** —— 要求实跑 `.explain()` 并贴计划片段。CLAUDE.md 里写了
这条，但它偶尔还是会滑过去。log/04 里已经有两条实测推翻旧记录的先例。

---

## 三条不能破的纪律

1. **Stage 2 永远在这个仓库外面做。** 参考答案就在磁盘上，任何在本仓库生成的
   "独立解"都已经被污染了。这是设计约束，不是待办事项。
2. **`/newday` 之后立刻 `/clear`。** 权限规则挡内置 Read，挡不住 Bash 里的
   `cat`；真正起作用的是清上下文。
3. **VERDICT 写完才能运行。** 先看结果再写审查意见，这个练习就没了——你 log/04
   里一半的 review 启发式都来自「这条靠读能不能抓到」这个判据。
