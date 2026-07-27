"""
=====================================================================
PySpark Mini Drills — pivot() Special (DSL only)
=====================================================================
Five short exercises focused purely on groupBy().pivot().agg().
No class wrapper — write your code right below each drill and run
the script top to bottom. Reference answers are commented at the
bottom of the file.

Format per drill: PROBLEM / INPUT SCHEMA / EXPECTED OUTPUT.
=====================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("pivot-drills").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# =====================================================================
# Drill 1 — Basic pivot with explicit values + zero fill
# =====================================================================
# PROBLEM: One row per city, one column per weekday, cell = total
#          orders. Missing (city, day) combos must show 0.
#          Only 'Mon', 'Tue', 'Wed' exist in the domain.
#
# INPUT SCHEMA: orders(city: string, day: string, cnt: int)
#
# EXPECTED OUTPUT (columns: city, Mon, Tue, Wed):
#     LA  | 5 | 3 | 0
#     NY  | 2 | 0 | 7
# ---------------------------------------------------------------------
orders = spark.createDataFrame(
    [("LA", "Mon", 5), ("LA", "Tue", 3), ("NY", "Mon", 2), ("NY", "Wed", 7)],
    schema="city STRING, day STRING, cnt INT",
)

# TODO: your code here
WEEKDAYS = [ "Mon", "Tue", "Wed"]
result1 = orders\
    .groupBy("city")\
    .pivot("day", WEEKDAYS)\
    .agg(F.sum("cnt"))\
    .na.fill(0)

result1.select(
    'city',
    *WEEKDAYS,
    ).show()


# =====================================================================
# Drill 2 — Pivot WITHOUT a values list (observe the schema)
# =====================================================================
# PROBLEM: Same as Drill 1 but deliberately omit the values list.
#          Run it, then answer in a comment:
#          (a) how many Spark jobs did this trigger vs Drill 1?
#              (check the Spark UI, or just reason about it)
#          (b) what happens to the output columns if you filter the
#              input to exclude all 'Wed' rows first? Try it.
#
# INPUT SCHEMA: same `orders` table as Drill 1
#
# EXPECTED OUTPUT: same numbers as Drill 1 (NULL instead of 0 is fine
#                  here — the point is the schema, not the fill).
# ---------------------------------------------------------------------

# TODO: your code here
result2a = orders\
    .groupBy("city")\
    .pivot("day")\
    .agg(F.sum("cnt").alias("cnt"))          # full input, no values list

result2b = orders\
    .filter(F.col("day") != "Wed")\
    .groupBy("city")\
    .pivot("day")\
    .agg(F.sum("cnt").alias("cnt"))    # input filtered to day != 'Wed', no values list

result2a.show()
result2b.show()
# ANSWER (a): ...
# ANSWER (b): ...


# =====================================================================
# Drill 3 — Values list as a filter (intentional subset)
# =====================================================================
# PROBLEM: Marketing only cares about weekend traffic. Produce one row
#          per site with columns Sat and Sun only, cell = total visits.
#          Weekday rows must not appear as columns NOR affect any
#          number in the output.
#
# INPUT SCHEMA: visits(site: string, day: string, v: int)
#
# EXPECTED OUTPUT (columns: site, Sat, Sun):
#     blog | 10 | 40
#     shop | 25 | 0
# ---------------------------------------------------------------------
visits = spark.createDataFrame(
    [
        ("blog", "Fri", 99), ("blog", "Sat", 10), ("blog", "Sun", 40),
        ("shop", "Sat", 25), ("shop", "Mon", 77),
    ],
    schema="site STRING, day STRING, v INT",
)
WEEKEND = ["Sat", "Sun"]

# TODO: your code here
result3 = visits\
    .groupBy("site")\
    .pivot("day", WEEKEND)\
    .agg(F.sum("v"))

result3 = result3.select(
    "site",
    *[F.coalesce(F.col(day), F.lit(0)).try_cast("int").alias(day) for day in WEEKEND]
)
result3.show()


# =====================================================================
# Drill 4 — Multiple aggregations: predict the schema FIRST
# =====================================================================
# PROBLEM: One row per store; pivot on channel ('web', 'app');
#          for each channel compute BOTH total amount and order count.
#          BEFORE running: write down the exact output column names
#          you expect, in order. Then run and compare.
#
# INPUT SCHEMA: txn(store: string, channel: string, amount: int)
#
# EXPECTED OUTPUT (predict the column names yourself!):
#     s1 | 30 | 2 | 50 | 1
#     s2 | 0  | 0 | 15 | 1        <- zero-filled
# ---------------------------------------------------------------------
txn = spark.createDataFrame(
    [("s1", "web", 10), ("s1", "web", 20), ("s1", "app", 50), ("s2", "app", 15)],
    schema="store STRING, channel STRING, amount INT",
)

# PREDICTED COLUMNS: store, web_amount, web_cnt, app_amount, app_cnt
# TODO: your code here
CHANNELS = [ "web", "app"]
result4 = txn\
    .groupBy("store")\
    .pivot("channel", CHANNELS)\
    .agg(
        F.sum("amount").alias("amount"),
        F.count(F.lit(1)).alias("cnt")
)

result4 = result4.select(
    "store",
    *[F.coalesce(F.col(f"{channel}_amount"), F.lit(0)).cast("int").alias(f"{channel}_amount") for channel in CHANNELS],
    *[F.coalesce(F.col(f"{channel}_cnt"), F.lit(0)).cast("int").alias(f"{channel}_cnt") for channel in CHANNELS]
)

result4.show()


# =====================================================================
# Drill 5 — Round trip: pivot, then unpivot back
# =====================================================================
# PROBLEM: Take your Drill 1 result (city, Mon, Tue, Wed) and melt it
#          back to the long format (city, day, cnt) using stack() or
#          unpivot()/melt() (Spark 3.4+). Zero cells may be kept or
#          dropped — decide and say why in a comment.
#
# INPUT: your `result1` DataFrame from Drill 1
#
# EXPECTED OUTPUT (columns: city, day, cnt) — if keeping zeros:
#     LA | Mon | 5
#     LA | Tue | 3
#     LA | Wed | 0
#     NY | Mon | 2
#     NY | Tue | 0
#     NY | Wed | 7
# ---------------------------------------------------------------------

# TODO: your code here
result5 = result1.select(
    'city',
    F.expr("stack(3, 'Mon', Mon, 'Tue', Tue, 'Wed', Wed) AS (day, cnt)"),
)
result5.show()


spark.stop()


# =====================================================================
# Reference answers (spoiler — attempt all five first!)
# =====================================================================
#
# --- Drill 1 ---
# result1 = (
#     orders.groupBy("city")
#           .pivot("day", ["Mon", "Tue", "Wed"])
#           .agg(F.sum("cnt"))
#           .na.fill(0)
# )
#
# --- Drill 2 ---
# result2a = orders.groupBy("city").pivot("day").agg(F.sum("cnt"))
# result2b = (
#     orders.filter(F.col("day") != "Wed")
#           .groupBy("city").pivot("day").agg(F.sum("cnt"))
# )
# (a) One extra job: Spark first runs distinct() on `day` and collects
#     the values to the driver to determine the output schema.
# (b) result2b has NO 'Wed' column at all — the schema followed the
#     data. This is exactly why production pipelines pass the list:
#     downstream code that expects a 'Wed' column breaks silently.
#
# --- Drill 3 ---
# result3 = (
#     visits.groupBy("site")
#           .pivot("day", ["Sat", "Sun"])
#           .agg(F.sum("v"))
#           .na.fill(0)
# )
# Note: rows with Fri/Mon are dropped by the pivot itself — the values
# list acts as a filter; no explicit .filter() needed (though adding
# one before the groupBy is also fine and arguably more readable).
#
# --- Drill 4 ---
# result4 = (
#     txn.groupBy("store")
#        .pivot("channel", ["app", "web"])
#        .agg(F.sum("amount").alias("amt"), F.count("*").alias("cnt"))
#        .na.fill(0)
# )
# Columns: store, app_amt, app_cnt, web_amt, web_cnt
# Pattern: <pivot_value>_<agg_alias>; total = |values| x |aggs|.
# (If you don't alias the aggs, Spark generates ugly names like
#  `app_sum(amount)` — always alias when using multi-agg pivot.)
#
# --- Drill 5 ---
# # stack() flavor (works on all versions):
# result5 = result1.select(
#     "city",
#     F.expr("stack(3, 'Mon', Mon, 'Tue', Tue, 'Wed', Wed) AS (day, cnt)"),
# )
# # unpivot()/melt() flavor (Spark 3.4+):
# result5 = result1.unpivot(
#     ids=["city"], values=["Mon", "Tue", "Wed"],
#     variableColumnName="day", valueColumnName="cnt",
# )
# Keeping zeros vs dropping them is a grain decision: keep them if the
# long table should be dense (one row per city-day, good for joins
# against a calendar dim); drop them (.filter("cnt > 0")) to recover
# the original sparse event-like shape.
# =====================================================================
