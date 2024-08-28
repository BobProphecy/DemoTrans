from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Acct_bal_Final_transform_V65S0P3_reformat _1(spark: SparkSession, V65S0_join_link: DataFrame) -> DataFrame:
    return V65S0_join_link.select(
        col("YTD_HIGHEST_CUR_ACCT_BAL"), 
        when(
            ((length(col("GL_YTD_AVG_BANK_SHR_BAL")) > lit(0)) & num(col("GL_YTD_AVG_BANK_SHR_BAL"))), 
            col("GL_YTD_AVG_BANK_SHR_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_YTD_AVG_BANK_SHR_BAL"), 
        when(
            ((length(col("GL_YTD_AGGR_BANK_SHR_BAL")) > lit(0)) & num(col("GL_YTD_AGGR_BANK_SHR_BAL"))), 
            col("GL_YTD_AGGR_BANK_SHR_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_YTD_AGGR_BANK_SHR_BAL"), 
        col("LAST_INT_REPRC_DT"), 
        col("MTD_LOWEST_CUR_ACCT_BAL"), 
        when(
            ((length(col("GL_MTD_AGGR_INT_AMT")) > lit(0)) & num(col("GL_MTD_AGGR_INT_AMT"))), 
            col("GL_MTD_AGGR_INT_AMT")
          )\
          .otherwise(lit(0))\
          .alias("GL_MTD_AGGR_INT_AMT"), 
        lit(0).alias("PR_6_CAL_DAY_CUR_BAL"), 
        col("INT_ACCR_BAL"), 
        when(
            ((length(col("GL_QTD_AGGR_CUR_BAL")) > lit(0)) & num(col("GL_QTD_AGGR_CUR_BAL"))), 
            col("GL_QTD_AGGR_CUR_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_QTD_AGGR_CUR_BAL"), 
        col("MTD_HIGHEST_CUR_ACCT_BAL"), 
        col("INT_PERDIEM_AMT"), 
        lit(0).alias("PR_3_CAL_DAY_CUR_BAL"), 
        when(
            ((length(col("GL_YTD_AGGR_INT_AMT")) > lit(0)) & num(col("GL_YTD_AGGR_INT_AMT"))), 
            col("GL_YTD_AGGR_INT_AMT")
          )\
          .otherwise(lit(0))\
          .alias("GL_YTD_AGGR_INT_AMT"), 
        col("MTD_AGGR_INT_RATE"), 
        when(
            ((length(col("GL_QTD_AVG_BANK_SHR_BAL")) > lit(0)) & num(col("GL_QTD_AVG_BANK_SHR_BAL"))), 
            col("GL_QTD_AVG_BANK_SHR_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_QTD_AVG_BANK_SHR_BAL"), 
        lit(0).alias("PR_9_CAL_DAY_CUR_BAL"), 
        when(
            ((length(col("GL_MTD_AVG_BANK_BAL")) > lit(0)) & num(col("GL_MTD_AVG_BANK_BAL"))), 
            col("GL_MTD_AVG_BANK_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_MTD_AVG_BANK_BAL"), 
        lit(0).alias("PR_4_CAL_DAY_CUR_BAL"), 
        col("ENTRY_DT"), 
        lit(0).alias("PR_1_CAL_DAY_CUR_BAL"), 
        col("NEXT_INT_REPRC_DT"), 
        when(
            ((length(col("GL_YTD_AGGR_CUR_BAL")) > lit(0)) & num(col("GL_YTD_AGGR_CUR_BAL"))), 
            col("GL_YTD_AGGR_CUR_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_YTD_AGGR_CUR_BAL"), 
        when(((length(col("GL_MTD_AVG_CUR_BAL")) > lit(0)) & num(col("GL_MTD_AVG_CUR_BAL"))), col("GL_MTD_AVG_CUR_BAL"))\
          .otherwise(lit(0))\
          .alias("GL_MTD_AVG_CUR_BAL"), 
        col("PAPER_STMT_CNT"), 
        when(((length(col("GL_QTD_AVG_CUR_BAL")) > lit(0)) & num(col("GL_QTD_AVG_CUR_BAL"))), col("GL_QTD_AVG_CUR_BAL"))\
          .otherwise(lit(0))\
          .alias("GL_QTD_AVG_CUR_BAL"), 
        when(((length(col("GL_YTD_AVG_CUR_BAL")) > lit(0)) & num(col("GL_YTD_AVG_CUR_BAL"))), col("GL_YTD_AVG_CUR_BAL"))\
          .otherwise(lit(0))\
          .alias("GL_YTD_AVG_CUR_BAL"), 
        when(
            ((length(col("GL_MTD_AGGR_CUR_BAL")) > lit(0)) & num(col("GL_MTD_AGGR_CUR_BAL"))), 
            col("GL_MTD_AGGR_CUR_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_MTD_AGGR_CUR_BAL"), 
        lit(0).alias("PR_2_CAL_DAY_CUR_BAL"), 
        col("AS_OF_DT"), 
        col("YTD_LOWEST_CUR_ACCT_BAL"), 
        when(
            ((length(col("GL_QTD_AGGR_BANK_SHR_BAL")) > lit(0)) & num(col("GL_QTD_AGGR_BANK_SHR_BAL"))), 
            col("GL_QTD_AGGR_BANK_SHR_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_QTD_AGGR_BANK_SHR_BAL"), 
        col("ESTMT_CNT"), 
        when(
            ((length(col("GL_MTD_AGGR_BANK_SHR_INT_AMT")) > lit(0)) & num(col("GL_MTD_AGGR_BANK_SHR_INT_AMT"))), 
            col("GL_MTD_AGGR_BANK_SHR_INT_AMT")
          )\
          .otherwise(lit(0))\
          .alias("GL_MTD_AGGR_BANK_SHR_INT_AMT"), 
        col("INT_INDX_BASE_RATE"), 
        col("DATA_DT"), 
        col("CUR_BAL"), 
        col("ACCT_ID"), 
        lit(0).alias("PR_5_CAL_DAY_CUR_BAL"), 
        col("BANK_SHR_INT_PERDIEM_AMT"), 
        lit(0).alias("PR_7_CAL_DAY_CUR_BAL"), 
        lit(0).alias("PR_8_CAL_DAY_CUR_BAL"), 
        when(
            ((length(col("GL_YTD_AGGR_BANK_SHR_INT_AMT")) > lit(0)) & num(col("GL_YTD_AGGR_BANK_SHR_INT_AMT"))), 
            col("GL_YTD_AGGR_BANK_SHR_INT_AMT")
          )\
          .otherwise(lit(0))\
          .alias("GL_YTD_AGGR_BANK_SHR_INT_AMT"), 
        col("YTD_AGGR_INT_RATE"), 
        col("INT_RATE"), 
        when(
            (length(col("GL_MTD_AGGR_BANK_SHR_BAL")) & num(col("GL_MTD_AGGR_BANK_SHR_BAL"))), 
            col("GL_MTD_AGGR_BANK_SHR_BAL")
          )\
          .otherwise(lit(0))\
          .alias("GL_MTD_AGGR_BANK_SHR_BAL"), 
        col("MTD_AVG_WT_INT_RATE"), 
        col("INT_DT"), 
        col("BANK_SHR_INT_ACCR_BAL"), 
        col("BANK_SHR_BAL")
    )
