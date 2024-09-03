from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def exp_Load_STG_MAQ_PR(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        ltrim(rtrim(col("i_YR_NO"))).cast(StringType()).alias("YR_NO"), 
        col("SCN_NM").alias("o_SCN_NM"), 
        col("v_SCN_DTL_TXT").alias("SCN_DTL_TXT"), 
        ltrim(rtrim(col("i_MUNT_NO"))).alias("MUNT_NO"), 
        ltrim(rtrim(col("i_LE_CD"))).alias("LE_CD"), 
        upper(ltrim(rtrim(col("i_ACCT_QLFR_NM")))).alias("ACCT_QLFR_NM"), 
        ltrim(rtrim(col("i_CCY_CD"))).alias("CCY_CD"), 
        upper(ltrim(rtrim(col("i_DSPL_CCY_NM")))).alias("DSPL_CCY_NM"), 
        ltrim(rtrim(col("i_BK_CD"))).alias("BK_CD"), 
        ltrim(rtrim(col("i_MTH_ABBR_NM"))).alias("MTH_ABBR_NM"), 
        ltrim(rtrim(col("i_GLAC_NO"))).alias("GLAC_NO"), 
        ltrim(rtrim(col("i_BAL_TYPE_CD"))).alias("BAL_TYPE_CD"), 
        ltrim(rtrim(col("i_XPNS_AM"))).cast(StringType()).alias("XPNS_AM"), 
        current_timestamp().alias("RCRD_CRT_DT"), 
        col("v_FEED_TYPE").alias("FEED_TYPE"), 
        when((col("SCN_NM") == lit("ACTUAL")), concat(lit("PR_Actual_"), col("MTH_KEY")))\
          .when((col("SCN_NM") == lit("PLAN")), concat(lit("PR_Plan_"), col("i_YR_NO")))\
          .when((col("SCN_NM") == lit("FCST_PR1")), concat(lit("PR_Forecast_"), col("i_YR_NO")))\
          .when((col("SCN_NM") == lit("FCST_CURYR")), concat(lit("PR_Forecast_CURYR_"), col("i_YR_NO")))\
          .when((col("SCN_NM") == lit("FCST_NXTYR")), concat(lit("PR_Forecast_NXTYR_"), col("i_YR_NO")))\
          .when((col("SCN_NM") == lit("FCST_MPF")), concat(lit("PR_Forecast_MPF_"), col("i_YR_NO")))\
          .otherwise(lit(None))\
          .alias("PARTITION_KEY"), 
        col("v_PROC_RUN_ID").alias("PROC_RUN_ID")
    )
