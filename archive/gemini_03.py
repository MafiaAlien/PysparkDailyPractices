from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class NestedLogsPipeline:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def extract_and_flatten(self, df):
        """
        pass
        """
        df_exploded = df\
            .select(
                "event_id",
                F.col('user_metadata.country').alias('country'),
                F.col('user_metadata.device').alias('device'),
                F.explode_outer('items_clicked').alias('item'),
            )\
            .where(
                (F.col('item.quantity') >= 0) | (
                    F.col('item.quantity').isNull())
            )

        final_df = df_exploded.select(
            "event_id",
            "country",
            "device",
            F.col("item.item_id").alias("item_id"),
            F.col("item.quantity").alias('quantity'),
        )
        return final_df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day3_Nested_JSON_Parsing") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    metadata_schema = StructType([
        StructField("country", StringType(), True),
        StructField("device", StringType(), True)
    ])

    item_schema = StructType([
        StructField("item_id", StringType(), True),
        StructField("quantity", IntegerType(), True)
    ])

    raw_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_metadata", metadata_schema, True),
        StructField("items_clicked", ArrayType(item_schema), True)
    ])

    raw_nested_data = [
        ("evt_101", ("USA", "Mobile"), [("prod_A", 2), ("prod_B", 1)]),
        ("evt_102", ("China", "Desktop"), [("prod_C", 5), ("prod_D", -99)]),
        ("evt_103", ("Japan", "Tablet"), [])
    ]

    input_df = spark.createDataFrame(raw_nested_data, schema=raw_schema)

    print("--- 1.(Nested Bronze) ---")
    input_df.printSchema()
    input_df.show(truncate=False)

    pipeline = NestedLogsPipeline(spark)
    output_df = pipeline.extract_and_flatten(input_df)

    print("--- 2.(Flattened Gold) ---")
    output_df.printSchema()
    output_df.show(truncate=False)

    input("任务已跑完，进程已阻塞，请前往 http://localhost:4040 查看 UI。按回车键退出...")
    spark.stop()
