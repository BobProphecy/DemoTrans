from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Shortcut_to_stage_DIA(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("BK_CD", StringType(), True), StructField("SCN_NM", StringType(), True), StructField("ACCT_QLFR_NM", StringType(), True), StructField("YR_NO", StringType(), True), StructField("AFLT_CD", StringType(), True), StructField("CCY_CD", StringType(), True), StructField("CNTR_PRTY_NO", StringType(), True), StructField("SRVC_NO", StringType(), True), StructField("LE_CD", StringType(), True), StructField("GLAC_NO", StringType(), True), StructField("CUBE_NM", StringType(), True), StructField("XPNS_AM", StringType(), True), StructField("MUNT_NO", StringType(), True), StructField("BAL_TYPE_CD", StringType(), True), StructField("DSPL_CCY_NM", StringType(), True), StructField("MTH_ABBR_NM", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", "YES")\
        .csv("path")
