from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from alteryx_customer_demo_fixed.config.ConfigStore import *
from alteryx_customer_demo_fixed.udfs.UDFs import *

def orders_csv(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("order_id", StringType(), True), StructField("customer_id", StringType(), True), StructField("order_category", StringType(), True), StructField("amount", StringType(), True), StructField("order_status", StringType(), True), StructField("order_date", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("C:\\Users\\prophecy\\Downloads\\orders.csv")
