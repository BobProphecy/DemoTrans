from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def customers_csv(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("first_name", StringType(), True), StructField("customer_id", StringType(), True), StructField("email", StringType(), True), StructField("account_open_date", StringType(), True), StructField("country_code", StringType(), True), StructField("last_name", StringType(), True), StructField("account_flags", StringType(), True), StructField("phone", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("C:\\Users\\prophecy\\Downloads\\customers.csv")
