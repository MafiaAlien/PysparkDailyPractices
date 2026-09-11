from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.types import *


class UserJourneyPipeline:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def deduplicate_events(self, df):
        """
        核心逻辑 1：基于业务主键去重
        要求：
        1. 相同的 event_id 可能出现多次。
        2. 请使用窗口函数 `row_number()`，按照 event_id 分组，并按 click_time 倒序排序（最新的优先）。
        3. 过滤出最新的一条，从而实现物理去重。
        """
        # ================== 开始编辑你的去重逻辑 ==================
        window_spec_event_id = W.partitionBy(
            F.col("event_id"))\
            .orderBy(F.col("click_time").desc())

        deduped_df = df.select(
            "*",
            F.row_number().over(window_spec_event_id).alias("rk")
        )\
            .where(F.col("rk") == 1)\
            .drop("rk")
        # ================== 结束编辑 ============================
        return deduped_df

    def calculate_time_delta(self, deduped_df):
        """
        核心逻辑 2：计算同用户相邻事件的时间差
        要求：
        1. 定义一个窗口：按 user_id 分组，按 click_time 正序排列。
        2. 使用 `lag()` 函数获取该用户上一次的 click_time。
        3. 计算当前 click_time 与上一次 click_time 的秒数差（可转换成unix时间戳后相减）。
        4. 如果上一次时间为 NULL（说明是首次点击），则将时间差填充为 0。
        5. 移除中间辅助列，保留最终期望的4个列。
        """
        # ================== 开始编辑你的窗口逻辑 ==================
        window_spec_user_id = W.partitionBy(F.col("user_id"))\
            .orderBy(F.col("click_time").asc())
        final_df = deduped_df.select(
            "*",
            F.row_number().over(window_spec_user_id).alias("user_rk")
        )\
            .withColumn(
            "prev_click_time",
            F.lag(F.col("click_time"), 1)
            .over(window_spec_user_id)
        )\
            .withColumn(
            "time_since_last_click_sec",
            F.timestamp_diff(
                "second",
                F.col("prev_click_time"),
                F.col("click_time")
            )
        )\
            .select(
            "user_id",
            "page_id",
            "click_time",
            "time_since_last_click_sec"
        )
        # ================== 结束编辑 ============================
        return final_df

    def run_pipeline(self, raw_df):
        print("[INFO] 正在启动 Day 2 消息去重阶段...")
        deduped_data = self.deduplicate_events(raw_df)

        print("[INFO] 正在通过窗口函数计算用户行为时间差...")
        journey_data = self.calculate_time_delta(deduped_data)

        return journey_data


# ==============================================================================
# 本地测试脚手架
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day2_Window_Function_Test")\
        .master("local[*]")\
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///opt/bitnami/spark/work-dir/spark-logs") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 模拟包含重复消息和乱序达到的点击流
    raw_data = [
        # user_101 的轨迹
        ("evt_1", "user_101", "2026-06-13 12:00:00", "homepage"),
        ("evt_1", "user_101", "2026-06-13 12:00:05", "homepage"),  # 发生重试，这条时间更新，应保留
        ("evt_2", "user_101", "2026-06-13 12:05:00", "detail_page"),
        ("evt_3", "user_101", "2026-06-13 12:05:30", "cart"),

        # user_102 的轨迹
        ("evt_4", "user_102", "2026-06-13 14:00:00", "homepage"),
        ("evt_5", "user_102", "2026-06-13 14:10:00", "search")
    ]

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        # 传入时为字符，内部计算需转为Timestamp
        StructField("click_time", StringType(), True),
        StructField("page_id", StringType(), True)
    ])

    input_df = spark.createDataFrame(raw_data, schema=schema) \
                    .withColumn("click_time", F.to_timestamp("click_time"))

    print("--- 原始输入数据 (包含网络重试重复) ---")
    input_df.show(truncate=False)

    pipeline = UserJourneyPipeline(spark)
    output_df = pipeline.run_pipeline(input_df)

    print("--- 最终用户行为转化分析表 ---")
    output_df.show(truncate=False)

    input("任务已跑完，进程已阻塞，请前往 http://localhost:4040 查看 UI。按回车键退出...")
    spark.stop()
