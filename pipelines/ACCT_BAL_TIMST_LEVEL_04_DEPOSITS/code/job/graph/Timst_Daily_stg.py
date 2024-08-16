from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Timst_Daily_stg(spark: SparkSession, SEQ_IN: DataFrame):
    SEQ_IN.write\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .mode("overwrite")\
        .option("separator", ",")\
        .option("header", False)\
        .csv(f"{Config.STG_DIR}/Timst_Daily_Final.stg")
