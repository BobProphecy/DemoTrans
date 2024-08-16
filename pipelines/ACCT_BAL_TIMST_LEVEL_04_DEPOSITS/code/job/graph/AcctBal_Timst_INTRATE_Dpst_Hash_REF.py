from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AcctBal_Timst_INTRATE_Dpst_Hash_REF(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("NEXT_INT_REPRC_DT", TimestampType(), True), StructField("TIMST_INTRATE", IntegerType(), True), StructField("TIMST_INST", IntegerType(), True), StructField("RptRecInd", StringType(), True), StructField("DATA_DATE", StringType(), True), StructField("DATE", StringType(), True), StructField("TIMST_ACCOUNT", IntegerType(), True), StructField("LAST_INT_REPRC_DT", TimestampType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv(f"{Config.HASH_DIR}/AcctBal_Timst_INTRATE_Dpst_Hash")
