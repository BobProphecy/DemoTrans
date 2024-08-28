from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Acct_bal_transform_V65S3P3_reformat _1(spark: SparkSession, V65S3_join_link: DataFrame) -> DataFrame:
    return V65S3_join_link.select(
        col("YTD_HIGHEST_CUR_ACCT_BAL"), 
        col("GL_YTD_AVG_BANK_SHR_BAL"), 
        col("GL_YTD_AGGR_BANK_SHR_BAL"), 
        col("LAST_INT_REPRC_DT"), 
        col("MTD_LOWEST_CUR_ACCT_BAL"), 
        col("GL_MTD_AGGR_INT_AMT"), 
        col("INT_ACCR_BAL"), 
        col("GL_QTD_AGGR_CUR_BAL"), 
        col("ODATE"), 
        col("MTD_HIGHEST_CUR_ACCT_BAL"), 
        col("INT_PERDIEM_AMT"), 
        col("GL_YTD_AGGR_INT_AMT"), 
        col("MTD_AGGR_INT_RATE"), 
        col("GL_QTD_AVG_BANK_SHR_BAL"), 
        col("GL_MTD_AVG_BANK_BAL"), 
        col("ENTRY_DT"), 
        col("NEXT_INT_REPRC_DT"), 
        col("GL_YTD_AGGR_CUR_BAL"), 
        col("GL_MTD_AVG_CUR_BAL"), 
        when((col("OPEN_CLOSE_CD") == lit("C")), lit(0)).otherwise(col("PAPER_STMT_CNT")).alias("PAPER_STMT_CNT"), 
        col("GL_QTD_AVG_CUR_BAL"), 
        col("GL_YTD_AVG_CUR_BAL"), 
        col("GL_MTD_AGGR_CUR_BAL"), 
        col("AS_OF_DT"), 
        col("YTD_LOWEST_CUR_ACCT_BAL"), 
        col("GL_QTD_AGGR_BANK_SHR_BAL"), 
        when((col("OPEN_CLOSE_CD") == lit("C")), lit(0)).otherwise(col("ESTMT_CNT")).alias("ESTMT_CNT"), 
        col("GL_MTD_AGGR_BANK_SHR_INT_AMT"), 
        when((trim(col("INT_RATE_TYP")) == lit("V")), ((col("TIMST_INTRATE") - col("TIMST_INTINDEX")) * lit(100)))\
          .otherwise(lit(0))\
          .alias("INT_INDX_BASE_RATE"), 
        col("DATA_DT"), 
        when(((length(col("CUR_BAL")) > lit(0)) & num(col("CUR_BAL"))), col("CUR_BAL"))\
          .otherwise(lit(0))\
          .alias("CUR_BAL"), 
        col("ACCT_ID"), 
        col("BANK_SHR_INT_PERDIEM_AMT"), 
        col("GL_YTD_AGGR_BANK_SHR_INT_AMT"), 
        col("YTD_AGGR_INT_RATE"), 
        col("INT_RATE"), 
        col("GL_MTD_AGGR_BANK_SHR_BAL"), 
        col("MTD_AVG_WT_INT_RATE"), 
        col("INT_DT"), 
        col("BANK_SHR_INT_ACCR_BAL"), 
        when(((length(col("BANK_SHR_BAL")) > lit(0)) & num(col("BANK_SHR_BAL"))), col("BANK_SHR_BAL"))\
          .otherwise(lit(0))\
          .alias("BANK_SHR_BAL")
    )
