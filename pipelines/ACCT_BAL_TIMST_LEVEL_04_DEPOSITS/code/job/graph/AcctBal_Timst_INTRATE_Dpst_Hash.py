from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AcctBal_Timst_INTRATE_Dpst_Hash(spark: SparkSession, Timst_INTRATE_OUT: DataFrame):
    Timst_INTRATE_OUT.write\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .mode("overwrite")\
        .option("separator", ",")\
        .option("header", False)\
        .csv(f"{Config.HASH_DIR}/AcctBal_Timst_INTRATE_Dpst_Hash")
