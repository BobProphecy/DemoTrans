from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def exp_Load_STG_MAQ_DIA_EXPR_6(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("o_SCN_NM").alias("SCN_NM"), 
        col("PARTITION_KEY"), 
        col("MUNT_NO"), 
        col("YR_NO"), 
        col("SRVC_NO"), 
        col("CNTR_PRTY_NO"), 
        col("LE_CD"), 
        col("AFLT_CD"), 
        col("ACCT_QLFR_NM"), 
        col("CCY_CD"), 
        col("DSPL_CCY_NM"), 
        col("BK_CD"), 
        col("MTH_ABBR_NM"), 
        col("GLAC_NO"), 
        col("BAL_TYPE_CD"), 
        col("XPNS_AM"), 
        col("RCRD_CRT_DT")
    )
