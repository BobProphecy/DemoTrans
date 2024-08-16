from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DepositTIMSTNaturalHash_REF(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ACCT_ID", IntegerType(), True), StructField("TIMST_INST", StringType(), True), StructField("RptRecInd", StringType(), True), StructField("DATA_DATE", StringType(), True), StructField("DATE", StringType(), True), StructField("TIMST_ACCOUNT", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv(f"{Config.HASH_DIR}/DepositTIMSTNaturalHash")
