from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Customers(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("first_name", StringType(), True), StructField("customer_id", IntegerType(), True), StructField("email", StringType(), True), StructField("account_open_date", StringType(), True), StructField("country_code", StringType(), True), StructField("last_name", StringType(), True), StructField("account_flags", StringType(), True), StructField("phone", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", "YES")\
        .csv("file")
