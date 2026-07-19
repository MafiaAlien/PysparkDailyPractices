"""
=====================================================================
PySpark Daily Practice - Day 1
=====================================================================
Problem: Top-2 Highest Salaries per Department (Top-N per Group)
Difficulty: Medium
Topics: Window Function (dense_rank), join, filter, ordering

[Description]
Given an `employee` table and a `department` table, find the
employees whose salary ranks in the top 2 within their department
(ties included).

employee schema:
    +-------------+--------+
    | column      | type   |
    +-------------+--------+
    | emp_id      | int    |
    | name        | string |
    | salary      | int    |
    | dept_id     | int    |
    +-------------+--------+

department schema:
    +-------------+--------+
    | column      | type   |
    +-------------+--------+
    | dept_id     | int    |
    | dept_name   | string |
    +-------------+--------+

[Expected Output]
Return a DataFrame with the following columns, ordered by
dept_name ASC, salary DESC:
    dept_name | name | salary

Notes:
1. Keep ALL employees tied at the same salary rank
   (use dense_rank instead of row_number).
2. Departments with no employees should NOT appear in the result.

[Example]
Input employee:
    (1, 'Alice',  90000, 1)
    (2, 'Bob',    80000, 1)
    (3, 'Carol',  80000, 1)
    (4, 'Dave',   70000, 1)
    (5, 'Eve',    60000, 2)
    (6, 'Frank',  50000, 2)

Input department:
    (1, 'Engineering')
    (2, 'Sales')
    (3, 'HR')            <- no employees, should be excluded

Output:
    Engineering | Alice | 90000
    Engineering | Bob   | 80000
    Engineering | Carol | 80000   <- tied at rank 2, keep it
    Sales       | Eve   | 60000
    Sales       | Frank | 50000
=====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class Solution:
    def solve_with_dsl(self, employee: DataFrame, department: DataFrame) -> DataFrame:
        """
        Approach 1: DataFrame API (DSL)

        TODO: implement your solution here.
        Hints:
          - Define a Window partitioned by department, ordered by salary.
          - Rank employees, keep rank <= 2, then join department names.
        """
        window_spec = Window\
            .partitionBy("dept_id")\
            .orderBy(F.desc('salary'))

        res_df = employee\
            .join(
                department,
                on='dept_id',
                how='inner'
            ).withColumn(
                'rank_salary',
                F.dense_rank().over(window_spec)
            )\
            .filter(F.col('rank_salary') <= 2)\
            .drop(F.col('rank_salary'))\
            .select(
                F.col('dept_name'),
                F.col('name'),
                F.col('salary')
            )
        return res_df

    def solve_with_sql(self, spark: SparkSession,
                       employee: DataFrame, department: DataFrame) -> DataFrame:
        """
        Approach 2: SparkSQL

        TODO: implement your solution here.
        Hints:
          - Register both DataFrames as temp views first.
          - Use DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...) in a subquery.
        """
        employee.createOrReplaceTempView("employee")
        department.createOrReplaceTempView("department")

        sql = """
            -- write your SQL here
            WITH joint_table AS (
                SELECT
                    e.dept_id,
                    dept_name,
                    name,
                    salary
                FROM
                    employee e JOIN department d ON e.dept_id = d.dept_id
            ),

            rk AS (
                SELECT
                    *,
                    DENSE_RANK()OVER(partition by dept_id order by salary DESC) AS rk
                FROM
                    joint_table
            )

            SELECT
                dept_name,
                name,
                salary
            FROM
                rk
            WHERE
                rk <= 2
        """
        return spark.sql(sql)


# =====================================================================
# Test section: init + test data + result validation
# =====================================================================
if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("pyspark-daily-day01")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")  # speed up local tests
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ---------- Build test data ----------
    employee = spark.createDataFrame(
        [
            (1, "Alice", 90000, 1),
            (2, "Bob",   80000, 1),
            (3, "Carol", 80000, 1),
            (4, "Dave",  70000, 1),
            (5, "Eve",   60000, 2),
            (6, "Frank", 50000, 2),
        ],
        schema="emp_id INT, name STRING, salary INT, dept_id INT",
    )

    department = spark.createDataFrame(
        [
            (1, "Engineering"),
            (2, "Sales"),
            (3, "HR"),
        ],
        schema="dept_id INT, dept_name STRING",
    )

    # ---------- Expected result ----------
    expected = [
        ("Engineering", "Alice", 90000),
        ("Engineering", "Bob",   80000),
        ("Engineering", "Carol", 80000),
        ("Sales",       "Eve",   60000),
        ("Sales",       "Frank", 50000),
    ]

    def check(actual_df: DataFrame, tag: str):
        actual = [tuple(row) for row in actual_df.collect()]
        # Compare as sorted lists so tied ranks with unstable ordering
        # do not cause false failures.
        assert sorted(actual) == sorted(expected), (
            f"[{tag}] FAILED!\n  expected: {sorted(expected)}\n  actual:   {sorted(actual)}"
        )
        print(f"[{tag}] PASSED")
        actual_df.show()

    sol = Solution()

    # ---------- Test the DSL approach ----------
    check(sol.solve_with_dsl(employee, department), "DSL")

    # ---------- Test the SQL approach ----------
    check(sol.solve_with_sql(spark, employee, department), "SQL")

    spark.stop()


# =====================================================================
# Reference Answers (spoiler alert -- try it yourself first!)
# =====================================================================
#
# --- Approach 1: DataFrame API (DSL) ---
#
#     def solve_with_dsl(self, employee, department):
#         w = Window.partitionBy("dept_id").orderBy(F.desc("salary"))
#         return (
#             employee
#             .withColumn("rk", F.dense_rank().over(w))
#             .filter(F.col("rk") <= 2)
#             .join(department, on="dept_id", how="inner")
#             .select("dept_name", "name", "salary")
#             .orderBy(F.asc("dept_name"), F.desc("salary"), F.asc("name"))
#         )
#
# --- Approach 2: SparkSQL ---
#
#     sql = """
#         SELECT d.dept_name,
#                e.name,
#                e.salary
#         FROM (
#             SELECT *,
#                    DENSE_RANK() OVER (
#                        PARTITION BY dept_id
#                        ORDER BY salary DESC
#                    ) AS rk
#             FROM employee
#         ) e
#         JOIN department d
#           ON e.dept_id = d.dept_id
#         WHERE e.rk <= 2
#         ORDER BY d.dept_name ASC, e.salary DESC, e.name ASC
#     """
#
# --- Key takeaways ---
# 1. dense_rank vs row_number vs rank:
#    - row_number: unique sequence, ties broken arbitrarily (would DROP Carol).
#    - rank: ties share the same rank but skip the next one (1, 2, 2, 4).
#    - dense_rank: ties share the same rank with no gaps (1, 2, 2, 3).
#    For "top N with ties", dense_rank is usually what you want.
# 2. Filter on the rank BEFORE the join to reduce shuffle volume.
# 3. Inner join naturally drops departments with no employees.
# =====================================================================
