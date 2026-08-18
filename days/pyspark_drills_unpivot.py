"""
=====================================================================
PySpark Mini Drills — unpivot / melt Special (DSL + SQL)
=====================================================================
Five short exercises on the wide -> long direction. No class wrapper —
write your code right below each drill and run the script top to
bottom. Reference answers are commented at the bottom of the file.

This is a DRILL, not a practice day: no trap, no AI review loop. The
goal is to close the "how do I even call this" layer so a later day can
put its trap in the semantics instead of the API surface.

Format per drill: PROBLEM / INPUT SCHEMA / EXPECTED OUTPUT.
=====================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("unpivot-drills")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# =====================================================================
# Shared input — a quarterly wide table with ONE missing cell
# =====================================================================
#     +-------+----+----+----+
#     | store | q1 | q2 | q3 |
#     +-------+----+----+----+
#     | s1    | 10 | 20 | 30 |
#     | s2    |  5 |NULL|  7 |
#     +-------+----+----+----+
#
# s2 never reported q2. That single NULL is what separates the routes
# in Drills 2 and 3 — keep an eye on it.
# ---------------------------------------------------------------------
wide = spark.createDataFrame(
    [("s1", 10, 20, 30), ("s2", 5, None, 7)],
    schema="store STRING, q1 INT, q2 INT, q3 INT",
)
wide.createOrReplaceTempView("wide")


# =====================================================================
# Drill 1 — DataFrame.unpivot: the signature
# =====================================================================
# PROBLEM: Melt `wide` into long form: one row per (store, quarter).
#          Use the DataFrame API. Name the two produced columns
#          `quarter` and `amount`.
#          Then answer in a comment: `melt` and `unpivot` — is one an
#          alias of the other, or do they differ? Check with hasattr or
#          the docstring; do not guess.
#
# INPUT SCHEMA: wide(store: string, q1: int, q2: int, q3: int)
#
# EXPECTED OUTPUT (columns: store, quarter, amount):
#     s1 | q1 | 10
#     s1 | q2 | 20
#     s1 | q3 | 30
#     s2 | q1 |  5
#     s2 | q2 | NULL
#     s2 | q3 |  7
# ---------------------------------------------------------------------

# TODO: your code here
long_df_unpivot = (wide.unpivot(
    ids=['store'],
    values=['q1', 'q2', 'q3'],
    variableColumnName='quarters',
    valueColumnName='amount'
))

long_df_unpivot.show()

# ANSWER (melt vs unpivot): no differences


# =====================================================================
# Drill 2 — The same melt in SQL, two ways. PREDICT THE ROW COUNTS.
# =====================================================================
# PROBLEM: Produce the same long table twice in Spark SQL:
#          (a) with `stack(n, 'label', col, ...)`
#          (b) with the `UNPIVOT (value FOR key IN (...))` clause
#
#          BEFORE running: write down how many rows you expect from
#          each. Then run both and compare against your prediction and
#          against Drill 1's DSL result.
#
#          This is the whole point of the drill — do not skip the
#          prediction step.
#
# INPUT SCHEMA: the `wide` temp view
#
# EXPECTED OUTPUT: predict it yourself, then verify.
# ---------------------------------------------------------------------

# PREDICTED rows — stack(): ...
# PREDICTED rows — UNPIVOT clause: ...

# TODO: your code here
long_df_stack = (wide.select(
    'store',
    F.stack(
        F.lit(3),
        F.lit('q1'),
        F.col('q1'),
        F.lit('q2'),
        F.col('q2'),
        F.lit('q3'),
        F.col('q3'),).alias('quarters', 'amount')
)
)

long_df_stack.show()
print('#'*50, 'count number: ', sep='\n')
print("unpivot: ")
print(long_df_unpivot.count())
print('stack: ')
print(long_df_stack.count())

# OBSERVED rows — stack(): 6
# OBSERVED rows — UNPIVOT clause: 6
# ANSWER: which routes agree, which one differs, and what is the
#         difference actually about?


# =====================================================================
# Drill 3 — Making the routes agree
# =====================================================================
# PROBLEM: Whichever route disagreed in Drill 2, make it match the
#          other two — without adding a filter or a join, using only
#          the clause's own syntax.
#          Then answer: if a row's value is missing for EVERY key in
#          the IN list, what happens to that store? Construct a
#          one-row DataFrame to find out rather than reasoning about
#          it.
#
# INPUT SCHEMA: the `wide` temp view, plus a probe table you build
#
# EXPECTED OUTPUT (columns: store, quarter, amount): all six rows,
#          i.e. identical to Drill 1.
# ---------------------------------------------------------------------

# TODO: your code here

# ANSWER (all values missing): ...


# =====================================================================
# Drill 4 — Two measures per key: find the wall, then go around it
# =====================================================================
# PROBLEM: `wide2` carries TWO measures per quarter. Melt it so that
#          each output row is one (store, quarter) carrying BOTH
#          measures side by side — `revenue` and `units` as separate
#          columns, NOT stacked into one value column.
#
#          Do it in three steps:
#          (a) try `DataFrame.unpivot` first. Write down in a comment
#              why it cannot express this.
#          (b) do it with `stack()`.
#          (c) do it with the `UNPIVOT` clause's column-group syntax.
#
# INPUT SCHEMA:
#     wide2(store: string, q1_rev: int, q1_units: int,
#           q2_rev: int, q2_units: int)
#
#     +-------+--------+----------+--------+----------+
#     | store | q1_rev | q1_units | q2_rev | q2_units |
#     +-------+--------+----------+--------+----------+
#     | s1    |     10 |        1 |     20 |        2 |
#     | s2    |      5 |        3 |   NULL |        4 |
#     +-------+--------+----------+--------+----------+
#
# EXPECTED OUTPUT (columns: store, quarter, revenue, units):
#     s1 | q1 | 10 | 1
#     s1 | q2 | 20 | 2
#     s2 | q1 |  5 | 3
#     s2 | q2 | NULL | 4
#
# Note the last row: it survives even though `revenue` is missing.
# Say why in a comment — and relate it to what you found in Drill 3.
# ---------------------------------------------------------------------
wide2 = spark.createDataFrame(
    [("s1", 10, 1, 20, 2), ("s2", 5, 3, None, 4)],
    schema="store STRING, q1_rev INT, q1_units INT, q2_rev INT, q2_units INT",
)
wide2.createOrReplaceTempView("wide2")

# TODO: your code here

# ANSWER (a) why DataFrame.unpivot cannot do it: ...
# ANSWER (why the NULL-revenue row survives): ...


# =====================================================================
# Drill 5 — What type does the value column end up as?
# =====================================================================
# PROBLEM: Two probes. Predict each before running.
#          (a) unpivot a table whose value columns are INT and DOUBLE.
#              What is the resulting value column's type, and what
#              happens to the INT cells?
#          (b) unpivot a table whose value columns are INT and STRING.
#
#          For (b), if it fails, record the exact error class name —
#          not a paraphrase.
#
# INPUT SCHEMA: build both probe tables yourself, one row each is enough.
#
# EXPECTED OUTPUT: predict it yourself, then verify.
# ---------------------------------------------------------------------

# PREDICTED (a) value column type: ...
# PREDICTED (b): ...

# TODO: your code here

# OBSERVED (a): ...
# OBSERVED (b) error class: ...


spark.stop()


# =====================================================================
# Reference answers (spoiler — attempt all five first!)
# =====================================================================
#
# --- Drill 1 ---
# long1 = wide.unpivot(
#     ids=["store"],
#     values=["q1", "q2", "q3"],
#     variableColumnName="quarter",
#     valueColumnName="amount",
# )
# `values=None` melts every non-id column, which is the same thing here.
# `melt` IS an alias of `unpivot` — both attributes exist on DataFrame
# and take the same four arguments. `unpivot` is the name to prefer;
# `melt` is there for pandas muscle memory.
# The NULL q2 cell for s2 is RETAINED: 6 rows out.
#
# --- Drill 2 ---
# (a) stack:
# spark.sql("""
#     SELECT store, stack(3, 'q1', q1, 'q2', q2, 'q3', q3) AS (quarter, amount)
#     FROM wide
# """)                                                  -- 6 rows
#
# (b) UNPIVOT clause:
# spark.sql("""
#     SELECT * FROM wide
#     UNPIVOT (amount FOR quarter IN (q1, q2, q3))
# """)                                                  -- 5 rows
#
# THE FINDING: the DSL `unpivot` and SQL `stack` keep the NULL row;
# the SQL `UNPIVOT` clause DROPS it. `UNPIVOT` defaults to
# EXCLUDE NULLS — it is the only one of the three that filters.
# So "unpivot" names two things with DIFFERENT default semantics
# depending on which API you reach for. A DSL-vs-SQL pair written from
# the same spec will silently disagree on row count wherever the wide
# table has a hole, and a wide table almost always has holes.
#
# --- Drill 3 ---
# spark.sql("""
#     SELECT * FROM wide
#     UNPIVOT INCLUDE NULLS (amount FOR quarter IN (q1, q2, q3))
# """)                                                  -- 6 rows
# INCLUDE NULLS is the opt-in; EXCLUDE NULLS is the (implicit) default.
# If a store's value is missing for EVERY key in the IN list, the plain
# UNPIVOT emits NO rows for that store at all — the store disappears
# from the output entirely rather than appearing with NULLs. That is
# the sharp end of the default: it is not "some cells vanish", it is
# "an entire key can vanish", and a downstream LEFT JOIN against a
# store dimension is what makes the loss visible as zeros.
#
# --- Drill 4 ---
# (a) `DataFrame.unpivot` produces exactly ONE variable column and ONE
#     value column — that is its whole signature. Two measures per key
#     needs two value columns, which the API cannot name. Calling it
#     with values=["q1_rev","q1_units"] gives you the LONG-est form
#     (k='q1_rev'/'q1_units'), not one row per quarter. Getting from
#     there to the target shape means pivoting back, which defeats it.
#
# (b) stack with 2 measures per group:
# spark.sql("""
#     SELECT store,
#            stack(2, 'q1', q1_rev, q1_units,
#                     'q2', q2_rev, q2_units) AS (quarter, revenue, units)
#     FROM wide2
# """)                                                  -- 4 rows
# The stack arity `n` counts GROUPS, not arguments: 2 groups x
# (1 label + 2 measures) = 6 arguments after the n.
#
# (c) UNPIVOT with column groups:
# spark.sql("""
#     SELECT * FROM wide2
#     UNPIVOT ((revenue, units) FOR quarter IN
#              ((q1_rev, q1_units) AS q1, (q2_rev, q2_units) AS q2))
# """)                                                  -- 4 rows
# The `AS q1` aliases are REQUIRED in the column-group form — without
# them there is no single column name to derive the key from.
#
# WHY the NULL-revenue row survives here but the NULL row vanished in
# Drill 2: EXCLUDE NULLS drops a row only when EVERY value in the group
# is NULL. `(NULL, 4)` still carries `units = 4`, so the row stays.
# Single-measure UNPIVOT is just the degenerate case where "every value
# in the group" means "the one value".
#
# --- Drill 5 ---
# (a) INT + DOUBLE -> the value column is DOUBLE. Unpivot computes a
#     least common type across the value columns and casts UP silently:
#     the INT `1` comes out as `1.0`. Nothing warns you. In a wide table
#     assembled by hand over months, one column typed DOUBLE by accident
#     re-types the entire melted measure.
# (b) INT + STRING -> AnalysisException, error class
#     UNPIVOT_VALUE_DATA_TYPE_MISMATCH:
#     "Unpivot value columns must share a least common type, some types
#      do not: [INT (`a`), STRING (`b`)]"
#     A loud failure, unlike (a)'s silent widening. The pair is the
#     lesson: the same mechanism is silent when the types are
#     compatible and fatal when they are not.
# =====================================================================
