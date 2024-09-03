from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def exp_Load_STG_MAQ_PR_VARS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SCN_NM", upper(ltrim(rtrim(col("i_SCN_NM")))))\
        .withColumn("v_FEED_TYPE", lit(Config.FEED_TYPE))\
        .withColumn("v_SCN_DTL_TXT", call_spark_fcn("string_substring", lit(Config.MAQ_FILE_NAME), lit(21), (lit(0) - lit(28))))\
        .withColumn(
          "MTH_KEY",
          concat(
            col("i_YR_NO"), 
            when((col("i_MTH_ABBR_NM") == lit("Jan")), lit("01"))\
              .when((col("i_MTH_ABBR_NM") == lit("Feb")), lit("02"))\
              .when((col("i_MTH_ABBR_NM") == lit("Mar")), lit("03"))\
              .when((col("i_MTH_ABBR_NM") == lit("Apr")), lit("04"))\
              .when((col("i_MTH_ABBR_NM") == lit("May")), lit("05"))\
              .when((col("i_MTH_ABBR_NM") == lit("Jun")), lit("06"))\
              .when((col("i_MTH_ABBR_NM") == lit("Jul")), lit("07"))\
              .when((col("i_MTH_ABBR_NM") == lit("Aug")), lit("08"))\
              .when((col("i_MTH_ABBR_NM") == lit("Sep")), lit("09"))\
              .when((col("i_MTH_ABBR_NM") == lit("Oct")), lit("10"))\
              .when((col("i_MTH_ABBR_NM") == lit("Nov")), lit("11"))\
              .when((col("i_MTH_ABBR_NM") == lit("Dec")), lit("12"))\
              .otherwise(lit("Error"))
          )
        )\
        .withColumn("v_PROC_RUN_ID", lit(Config.PROC_RUN_ID))
