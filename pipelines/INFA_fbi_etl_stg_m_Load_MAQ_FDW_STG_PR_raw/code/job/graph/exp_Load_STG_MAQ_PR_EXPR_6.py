from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def exp_Load_STG_MAQ_PR_EXPR_6(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("BK_CD"), 
        col("MTH_ABBR_NM"), 
        col("GLAC_NO"), 
        col("BAL_TYPE_CD"), 
        col("XPNS_AM").alias("BAL_AM"), 
        col("RCRD_CRT_DT").alias("RCRD_CRT_TS"), 
        col("PARTITION_KEY"), 
        col("PROC_RUN_ID").alias("PROC_RUN_id"), 
        col("FEED_TYPE").alias("SRC_FR_NM"), 
        col("LE_CD"), 
        col("CCY_CD"), 
        col("DSPL_CCY_NM"), 
        col("SCN_DTL_TXT").alias("SCN_DTL_TX"), 
        col("ACCT_QLFR_NM"), 
        col("YR_NO"), 
        col("MUNT_NO"), 
        col("o_SCN_NM").alias("MAQ_SCN_NM")
    )
