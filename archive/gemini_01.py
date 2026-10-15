from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

class DailyActivityPipeline:
    def __init__(self, spark_session: SparkSession):
        """
        初始化管道，传入 SparkSession 实例
        """
        self.spark = spark_session.builder\
        .master("local[*]")\
        .appName("Day1_Practice_Local_Test")\
        .getOrCreate()

    def clean_data(self, df):
        """
        核心逻辑 1：数据清洗 (Silver 层处理)
        要求：
        1. 过滤掉 user_id 为空 (NULL) 的行
        2. 将 event_time 统一转换为标准 Date 类型，列命名为 event_date
           提示：Spark 3.0+ 中可以使用 F.to_date() 或 F.when() 处理多种时间格式，
           或者利用 F.regexp_replace() 将斜杠替换为横线后再转换。
        3. 处理 price 列：若金额小于 0，视作脏数据，填充为 0.0
        """
        # ================== 开始编辑你的清洗逻辑 ==================
        cleaned_df = df.where((F.col("user_id").isNotNull())).select(
            F.col("user_id"),
            F.coalesce(
                F.to_date(F.col("event_time"), "yyyy-MM-dd HH:mm:ss"),
                F.to_date(F.col("event_time"), "yyyy/MM/dd HH:mm:ss"),
                ).alias("event_date")
            F.when(F.col("price") < 0, 0.0).otherwise(F.col("price")).alias("price"),
            F.col("event_type"),
            )
        # ================== 结束编辑 ============================
        return cleaned_df

    def transform_metrics(self, cleaned_df):
        """
        核心逻辑 2：指标聚合 (Gold 层处理)
        要求：
        1. 按 event_date 分组
        2. 计算去重活跃用户数命名为 dau
        3. 统计 event_type 为 'purchase' 的总行数命名为 purchase_count
        4. 统计 price 的总和命名为 total_revenue（四舍五入保留 2 位小数）
        5. 结果按 event_date 正序排列
        """
        # ================== 开始编辑你的聚合逻辑 ==================
        result_df = cleaned_df.groupBy(F.col("event_date"))\
        .agg(
            F.countDistinct(F.col("user_id")).alias("dau"),
            F.sum(
                F.when(F.col("event_type")== "purchase", 1)\
                .otherwise(0)).\
            alias("purchase_count")
            ,
            F.round(F.sum(
                F.col("price")), 2)\
            .alias("total_revenue")
        )\
        .orderBy(F.col("event_date").asc())

        return result_df


        # ================== 结束编辑 ============================
        return result_df

    def run_pipeline(self, raw_df):
        """
        管道执行主入口
        """
        print("[INFO] 正在启动 Day 1 数据清洗流水线...")
        cleaned_data = self.clean_data(raw_df)
        
        print("[INFO] 正在生成每日黄金指标层...")
        final_metrics = self.transform_metrics(cleaned_data)
        
        return final_metrics

# ==============================================================================
# 本地测试脚手架 (本地模拟数据验证)
# ==============================================================================
if __name__ == "__main__":
    # 创建本地测试的 SparkSession
    spark = SparkSession.builder \
        .appName("Day1_Practice_Local_Test") \
        .master("local[*]") \
        .getOrCreate()

    # 模拟上游原始输入数据（包含了各种脏数据和不规范格式）
    raw_data = [
        ("user_101", "2026-06-11 14:00:00", "view", None),
        ("user_101", "2026/06/11 15:30:00", "purchase", 99.932),
        (None, "2026-06-11 16:00:00", "click", None),          # 脏数据：用户为空
        ("user_102", "2026-06-11 09:00:00", "purchase", -10.5),  # 脏数据：金额为负
        ("user_102", "2026/06/12 10:15:00", "view", None),      # 跨天数据，且格式为斜杠
        ("user_103", "2026-06-12 11:00:00", "purchase", 45.50)
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("price", DoubleType(), True)
    ])

    input_df = spark.createDataFrame(raw_data, schema=schema)
    print("--- 原始输入数据 (Raw) ---")
    input_df.show()

    # 实例化并运行管道
    pipeline = DailyActivityPipeline(spark)
    output_df = pipeline.run_pipeline(input_df)

    print("--- 最终聚合指标 (Gold) ---")
    output_df.show()