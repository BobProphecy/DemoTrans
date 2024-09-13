from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Orders(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("order_id", IntegerType(), True), StructField("customer_id", IntegerType(), True), StructField("order_category", StringType(), True), StructField("amount", StringType(), True), StructField("order_status", StringType(), True), StructField("order_date", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", "YES")\
        .csv("file")
