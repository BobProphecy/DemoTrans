from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SQ_Shortcut_to_stage_PR_EXPR_7(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MTH_ABBR_NM").alias("i_MTH_ABBR_NM"), 
        col("GLAC_NO").alias("i_GLAC_NO"), 
        col("BAL_TYPE_CD").alias("i_BAL_TYPE_CD"), 
        col("XPNS_AM").alias("i_XPNS_AM"), 
        col("YR_NO").alias("i_YR_NO"), 
        col("SCN_NM").alias("i_SCN_NM"), 
        col("MUNT_NO").alias("i_MUNT_NO"), 
        col("LE_CD").alias("i_LE_CD"), 
        col("ACCT_QLFR_NM").alias("i_ACCT_QLFR_NM"), 
        col("CCY_CD").alias("i_CCY_CD"), 
        col("DSPL_CCY_NM").alias("i_DSPL_CCY_NM"), 
        col("BK_CD").alias("i_BK_CD")
    )
