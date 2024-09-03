from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def T_FDW_STG_MAQ_PR_EXP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YR_NO"), 
        col("MAQ_SCN_NM"), 
        col("SCN_DTL_TX"), 
        col("MUNT_NO"), 
        col("LE_CD"), 
        col("ACCT_QLFR_NM"), 
        col("CCY_CD"), 
        col("DSPL_CCY_NM"), 
        col("BK_CD"), 
        col("MTH_ABBR_NM"), 
        col("GLAC_NO"), 
        col("BAL_TYPE_CD"), 
        col("BAL_AM"), 
        col("RCRD_CRT_TS"), 
        col("SRC_FR_NM"), 
        col("PARTITION_KEY"), 
        col("PROC_RUN_id")
    )
