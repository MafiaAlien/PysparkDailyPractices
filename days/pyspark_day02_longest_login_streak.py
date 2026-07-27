"""
=====================================================================
PySpark Daily Practice — Template v2 (with AI Review stage)
=====================================================================
Day 2 — Date Handling & Gaps-and-Islands  (Difficulty: Medium-Hard)

PROBLEM
-------
Given a login event table, compute each user's LONGEST streak of
consecutive daily logins.

- A "streak" is a run of calendar days with no gap: e.g. logins on
  Jan 1, Jan 2, Jan 3 form a streak of length 3.
- A user may log in multiple times on the same day; duplicate
  (user_id, login_date) pairs must count as a single day.
- Every user with at least one login appears in the result; a user
  with no consecutive days has a longest streak of 1.

INPUT SCHEMA
------------
logins(user_id: int, login_date: date)

EXPECTED OUTPUT
---------------
One row per user:
    user_id: int | longest_streak: int
Row order does not matter (the test compares order-insensitively).

EXAMPLE
-------
Input logins:
    (1, 2026-01-01)
    (1, 2026-01-02)
    (1, 2026-01-02)   <- same-day duplicate, counts once
    (1, 2026-01-03)
    (1, 2026-01-05)
    (1, 2026-01-06)
    (2, 2026-01-10)
    (3, 2026-02-01)
    (3, 2026-02-02)
    (3, 2026-02-04)
    (3, 2026-02-05)
    (3, 2026-02-06)
    (3, 2026-02-07)

Output:
    user_id | longest_streak
    1       | 3              <- Jan 1-3 (Jan 5-6 is only 2)
    2       | 1
    3       | 4              <- Feb 4-7

WORKFLOW (v2)
-------------
Stage 1  SOLVE   : Implement solve_dsl() and solve_sql() yourself. Run tests.
Stage 2  GENERATE: Only AFTER your solutions pass, ask an AI to solve the
                   same problem. IMPORTANT: paste only the PROBLEM + SCHEMA
                   sections into the AI — never your own solution — so its
                   answer is independent and your review is a blind review.
Stage 3  REVIEW  : Paste the AI's code into Part 3 below. Review it by
                   reading only (no running). Fill in REVIEW_NOTES and
                   commit to a VERDICT before executing anything.
Stage 4  VERIFY  : Un-comment and run the AI code against the same check().
                   Compare the actual result with your verdict.
Stage 5  DIGEST  : Fill in "Review takeaways" in Part 5 — what the AI got
                   wrong/right, and whether YOU caught it by reading.
=====================================================================
"""

from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =====================================================================
# Part 1 — Solution (yours)
# =====================================================================
class Solution:
    def solve_dsl(self, df: DataFrame) -> DataFrame:
        """DataFrame API approach.

        Hint: the classic gaps-and-islands trick — number the (deduped)
        days per user with row_number(), then subtract that number of
        days from the date. Consecutive days collapse to the same
        anchor date; group on it.
        """
        # TODO: implement
        win_spec = Window\
            .partitionBy(
                F.col('user_id'))\
            .orderBy(F.col('unique_date').asc())

        res_df = df\
            .groupBy(
                F.col('user_id'),
                F.col('login_date'),
            ).agg(F.min(F.col('login_date')).alias('unique_date'))\
            .select(
                'user_id',
                'unique_date')\
            .withColumn(
                'baseline',
                F.row_number().over(win_spec)
            ).select(
                'user_id',
                F.date_sub(F.col('unique_date'), F.col(
                    'baseline')).alias('streak_diff'),
            ).groupBy(
                F.col('user_id'),
                F.col('streak_diff'),
            ).agg(
                F.count('*').alias('streak_cnt')
            ).groupBy(F.col('user_id'))\
            .agg(F.max(F.col('streak_cnt')).alias('longest_streak'))\
            .select(
                F.col('user_id'),
                F.col('longest_streak'))

        return res_df

    def solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """SparkSQL approach.

        Hint: same idea in SQL — DISTINCT first, then
        ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...),
        then DATE_SUB(login_date, rn) as the island key.
        Chain it with CTEs.
        """
        df.createOrReplaceTempView("logins")

        sql = """
            -- write your SQL here
            WITH dedup_date AS (
                SELECT
                    user_id,
                    login_date,
                    MAX(login_date) AS dedup_date 
                FROM
                    logins
                GROUP BY 
                    user_id,
                    login_date
            ),

            baseline_rk AS (
                SELECT
                    user_id,
                    dedup_date,
                    dedup_date - ROW_NUMBER()OVER(partition by user_id order by login_date) AS baseline
                FROM
                    dedup_date
            ),

            streak AS (
                SELECT
                    user_id,
                    baseline,
                    COUNT(*) AS streak_cnt
                FROM
                    baseline_rk
                GROUP BY
                    user_id,
                    baseline
            )

            SELECT
                user_id,
                MAX(streak_cnt) AS longest_streak
            FROM
                streak
            GROUP BY user_id;
        """
        return spark.sql(sql)
    
    def ai_solve_dsl(self, df: DataFrame) -> DataFrame:
        """DataFrame API 解法:经典 gaps-and-islands(缺口与孤岛)技巧。
 
        思路:去重后按日期排序编号 rn,用 login_date - rn 作为分组键。
        同一个连续区间内,日期每天 +1,rn 也每次 +1,差值恒定;
        一旦出现断档,差值就会变化,从而自然切分出各个 streak。
        """
        distinct_days = df.select("user_id", "login_date").distinct()
 
        w = Window.partitionBy("user_id").orderBy("login_date")
 
        grouped = distinct_days.withColumn(
            "grp", F.date_sub(F.col("login_date"), F.row_number().over(w))
        )
 
        streaks = grouped.groupBy("user_id", "grp").agg(
        F.count("*").alias("streak_len")
        )
 
        return streaks.groupBy("user_id").agg(
            F.max("streak_len").alias("longest_streak")
        ).select("user_id", "longest_streak")

    def ai_solve_sql(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView("logins")
    
        return spark.sql("""
            WITH distinct_days AS (
                SELECT DISTINCT user_id, login_date
                FROM logins
            ),
            numbered AS (
                SELECT
                    user_id,
                    login_date,
                    DATE_SUB(
                        login_date,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)
                    ) AS grp
                FROM distinct_days
            ),
            streaks AS (
                SELECT user_id, grp, COUNT(*) AS streak_len
                FROM numbered
                GROUP BY user_id, grp
            )
            SELECT user_id, MAX(streak_len) AS longest_streak
            FROM streaks
            GROUP BY user_id
        """)


# =====================================================================
# Part 2 — Tests
# =====================================================================
def check(actual: DataFrame, expected_rows: list, label: str) -> None:
    """Order-insensitive comparison; sorted() absorbs tie-order instability."""
    actual_rows = sorted([tuple(r) for r in actual.collect()])
    expected = sorted(expected_rows)
    status = "PASS" if actual_rows == expected else "FAIL"
    print(f"[{status}] {label}")
    if status == "FAIL":
        print("  expected:", expected)
        print("  actual  :", actual_rows)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("daily-practice").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data = [
        (1, date(2026, 1, 1)),
        (1, date(2026, 1, 2)),
        (1, date(2026, 1, 2)),   # same-day duplicate
        (1, date(2026, 1, 3)),
        (1, date(2026, 1, 5)),
        (1, date(2026, 1, 6)),
        (2, date(2026, 1, 10)),
        (3, date(2026, 2, 1)),
        (3, date(2026, 2, 2)),
        (3, date(2026, 2, 4)),
        (3, date(2026, 2, 5)),
        (3, date(2026, 2, 6)),
        (3, date(2026, 2, 7)),
    ]
    df = spark.createDataFrame(data, schema="user_id INT, login_date DATE")

    expected = [
        (1, 3),
        (2, 1),
        (3, 4),
    ]

    s = Solution()
    check(s.solve_dsl(df), expected, "DSL")
    check(s.solve_sql(spark, df), expected, "SQL")
    check(s.ai_solve_dsl(df), expected, "AI-DSL")
    check(s.ai_solve_sql(spark, df), expected, "AI-SQL")

    # ------------------------------------------------------------------
    # Stage 4 — run the AI solution through the SAME harness.
    # Un-comment after completing your written review in Part 3.
    # ------------------------------------------------------------------
    # check(ai_solve_dsl(df), expected, "AI-DSL (post-review verification)")
    # check(ai_solve_sql(spark, df), expected, "AI-SQL (post-review verification)")

    spark.stop()


# =====================================================================
# Part 3 — AI Review (NEW in v2)
# =====================================================================
# Paste the AI-generated solution here, UNMODIFIED. Review by reading only.
#
# def ai_solve_dsl(self, df: DataFrame) -> DataFrame:
#     """DataFrame API 解法:经典 gaps-and-islands(缺口与孤岛)技巧。
 
#     思路:去重后按日期排序编号 rn,用 login_date - rn 作为分组键。
#     同一个连续区间内,日期每天 +1,rn 也每次 +1,差值恒定;
#     一旦出现断档,差值就会变化,从而自然切分出各个 streak。
#     """
#     distinct_days = df.select("user_id", "login_date").distinct()
 
#     w = Window.partitionBy("user_id").orderBy("login_date")
 
#     grouped = distinct_days.withColumn(
#         "grp", F.date_sub(F.col("login_date"), F.row_number().over(w))
#     )
 
#     streaks = grouped.groupBy("user_id", "grp").agg(
#         F.count("*").alias("streak_len")
#     )
 
#     return streaks.groupBy("user_id").agg(
#         F.max("streak_len").alias("longest_streak")
#     ).select("user_id", "longest_streak")

# def ai_solve_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
#     df.createOrReplaceTempView("logins")
 
#     return spark.sql("""
#         WITH distinct_days AS (
#             SELECT DISTINCT user_id, login_date
#             FROM logins
#         ),
#         numbered AS (
#             SELECT
#                 user_id,
#                 login_date,
#                 DATE_SUB(
#                     login_date,
#                     ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)
#                 ) AS grp
#             FROM distinct_days
#         ),
#         streaks AS (
#             SELECT user_id, grp, COUNT(*) AS streak_len
#             FROM numbered
#             GROUP BY user_id, grp
#         )
#         SELECT user_id, MAX(streak_len) AS longest_streak
#         FROM streaks
#         GROUP BY user_id
#     """)
#
# REVIEW_NOTES (fill in BEFORE running):
# ----------------------------------------------------------------------
# [ ] Correctness — edge cases: ties? nulls? empty groups? duplicate keys?
#     notes:
# [ ] API usage — wrong signatures, deprecated calls, ambiguous column
#     references in joins, SQL syntax slips (trailing commas, quoting)?
#     notes:
# [ ] Performance — unnecessary shuffles, window over the whole table
#     without partitionBy, join strategy, redundant actions/collects?
#     notes:
# [ ] Robustness — hardcoded values, assumptions not in the problem
#     statement, behavior under data skew?
#     notes:
# [ ] Style/clarity — would you approve this in a real code review?
#     notes:
#
# VERDICT (commit before running): PASS — because:
# ACTUAL RESULT (after Stage 4 run):
# [PASS] AI-DSL
# [PASS] AI-SQL
# GAP ANALYSIS: did the run reveal anything your reading missed?


# =====================================================================
# Part 4 — Reference answers (keep commented)
# =====================================================================
# --- DataFrame API (DSL) ---
#
# def ref_solve_dsl(df):
#     w = Window.partitionBy("user_id").orderBy("login_date")
#     return (
#         df.dropDuplicates(["user_id", "login_date"])
#           .withColumn("rn", F.row_number().over(w))
#           .withColumn("anchor", F.date_sub(F.col("login_date"), F.col("rn")))
#           .groupBy("user_id", "anchor")
#           .agg(F.count(F.lit(1)).alias("streak"))
#           .groupBy("user_id")
#           .agg(F.max("streak").alias("longest_streak"))
#     )
#
# --- SparkSQL ---
#
# sql = """
#     WITH dedup AS (
#         SELECT DISTINCT user_id, login_date
#         FROM logins
#     ),
#     numbered AS (
#         SELECT user_id,
#                login_date,
#                ROW_NUMBER() OVER (
#                    PARTITION BY user_id
#                    ORDER BY login_date
#                ) AS rn
#         FROM dedup
#     ),
#     islands AS (
#         SELECT user_id,
#                DATE_SUB(login_date, rn) AS anchor,
#                COUNT(*) AS streak
#         FROM numbered
#         GROUP BY user_id, DATE_SUB(login_date, rn)
#     )
#     SELECT user_id,
#            MAX(streak) AS longest_streak
#     FROM islands
#     GROUP BY user_id
# """


# =====================================================================
# Part 5 — Key takeaways
# =====================================================================
# Concept takeaways:
# - Gaps-and-islands core idea: for consecutive sequences,
#   (value - row_number) is constant within an island. For dates, use
#   DATE_SUB(login_date, rn) — the "anchor date" — as the island key.
# - Dedupe BEFORE numbering: a same-day duplicate shifts row_number by 1
#   and silently breaks the anchor, splitting a real streak. This bug
#   passes on clean data and only surfaces on dirty data.
# - row_number (not rank/dense_rank) is the right tool here: after
#   dedup there are no ties within a user, and the trick needs a strict
#   +1 progression.
# - DATE_SUB accepts an int column for the day count; no need to cast
#   the date to unix timestamps for day arithmetic.
#
# Review takeaways (NEW in v2):
# — 1. distinct / dropDuplicates vs GROUP BY + MAX：性能差异基本为零
# 这三种写法在 Catalyst 优化器眼里几乎是同一个东西。distinct() 和 dropDuplicates([...]) 底层就是被翻译成一个按这些列做 key、
# 不带聚合表达式的 Aggregate 算子——和你的 GROUP BY user_id, login_date 物理计划相同，都需要一次按 key 的 shuffle，而 shuffle 才是成本大头。
# 你可以自己验证：在两种写法的结果上调 .explain()，会看到几乎一样的 HashAggregate + Exchange 结构。
# 所以差别不在性能，而在语义和可读性：

# 你的 MAX(login_date) 聚合是冗余的——login_date 本身就是分组键，组内每个值都相等，MAX 出来的 dedup_date 恒等于 login_date。
# 多算一个表达式的开销可以忽略，但读代码的人要多想一步"这个 MAX 是在干嘛"。SELECT DISTINCT 一眼就能看出意图是去重。
# 一个真实场景下的注意点：AI 用的 df.select(...).distinct() 和 SELECT DISTINCT 之所以安全，是因为先收窄到了两列。
# 如果表里还有别的列（比如 login_ts、device），直接 df.distinct() 是按全部列去重，达不到目的——这时 dropDuplicates(["user_id", "login_date"]) 才是精确工具。

# 结论：成本上无差别，写法上推荐 DISTINCT / dropDuplicates，意图更直白。
# 2. date - ROW_NUMBER()：能跑，但我建议换成显式 DATE_SUB
# 先回答"会不会有格式冲突"：在 Spark SQL 里，DATE 类型 - 整数 会被解析器自动解析为 date_sub，返回仍是 DATE 类型，不存在隐式转换陷阱——你的测试 PASS 也印证了这一点。所以它不是 bug。
# 但我在 code review 里还是会建议改掉，理由有两个：
# 可移植性。date - int 的行为在不同 SQL 引擎间差异很大：PostgreSQL 里合法且语义相同；MySQL 里 date_col - 1 会把日期先转成数字再做算术，结果完全是另一个东西；Hive 里干脆不支持。
# 写惯了这种简写，切引擎时容易埋雷。DATE_SUB(dedup_date, rn) 在哪都是明确的"减 n 天"。
# 
# 意图显式。- ROW_NUMBER() 读起来要先想一下"这是日期算术还是数字算术"，DATE_SUB 没有歧义。
# 至于你提的 login_date - INTERVAL rn DAYS 这个替代方案——这条路走不通：INTERVAL n DAYS 是字面量语法，n 必须是常量，不能塞列进去。
# 要用 interval 得写 login_date - make_interval(0, 0, 0, rn)，比 DATE_SUB 啰嗦得多且没有任何收益。所以排序是：DATE_SUB(date, int_col) 最优 > date - int（Spark 内合法但欠移植性）> interval 方案（不必要的绕路）。
# 顺带一个和你 DSL 写法相关的版本细节：F.date_sub(col, days) 的第二个参数接受 Column 是 Spark 3.3+ 的行为，
# 更早版本只接受 int 字面量，得用 F.expr("date_sub(unique_date, baseline)") 绕。你本地能跑说明版本没问题，但在老集群上这是个会踩的坑。

# 所以你现在集齐了去重的全部四种写法，可以这样归档到 Part 5：

# 1. SELECT DISTINCT A, B / select(A,B).distinct() —— 意图最直白，丢弃其他列
# 2. GROUP BY A, B（无聚合）—— 物理计划与 1 相同；它的真正价值是当你顺便还要算聚合时（比如去重的同时统计每天登录次数 COUNT(*)），DISTINCT 做不到这一点
# 3. dropDuplicates(["A","B"]) —— 保留所有列，但每组留哪行不确定
# 4. 窗口 ROW_NUMBER() ... WHERE rn = 1 —— 保留所有列且确定留哪行，代价是排序
