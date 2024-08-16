from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ECF_STATUSCD_IN2_Hash(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("OUT_3", StringType(), True), StructField("OUT_1", StringType(), True), StructField("IN_1", StringType(), True), StructField("DATE", StringType(), True), StructField("DATA_DATE", StringType(), True), StructField("RptRecInd", StringType(), True), StructField("APPLICATIONS", StringType(), True), StructField("IN_2", StringType(), True), StructField("OUT_2", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv(f"{Config.HASH_DIR}/ECF_STATUSCD_IN2_Hash")
