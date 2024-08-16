from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def OdateHash_REF(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("PREV_ODATE", TimestampType(), True), StructField("DATA_DATE", TimestampType(), True), StructField("RptRecInd", StringType(), True), StructField("ODATE", TimestampType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv(f"{Config.HASH_DIR}/OdateHash")
