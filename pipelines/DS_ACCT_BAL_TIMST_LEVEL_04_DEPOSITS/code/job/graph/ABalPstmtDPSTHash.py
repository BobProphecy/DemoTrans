from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ABalPstmtDPSTHash(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("SUM_OF_P_STMT", IntegerType(), True), StructField("RFACO_ACCT", StringType(), True), StructField("APPL_CD", StringType(), True), StructField("RptRecInd", StringType(), True), StructField("DATA_DATE", StringType(), True), StructField("DATE", StringType(), True), StructField("RFACO_INST", IntegerType(), True), StructField("RFACO_OPTION", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv(f"{Config.HASH_DIR}/ABalPstmtDPSTHash")
