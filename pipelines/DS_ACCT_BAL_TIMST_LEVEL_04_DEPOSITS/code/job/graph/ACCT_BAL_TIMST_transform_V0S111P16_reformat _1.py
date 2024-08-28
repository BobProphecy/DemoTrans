from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ACCT_BAL_TIMST_transform_V0S111P16_reformat _1(spark: SparkSession, V0S111_join_link: DataFrame) -> DataFrame:
    return V0S111_join_link.select(
        lit(0).alias("YTD_AVG_BANK_SHR_BAL"), 
        lit(0).alias("YTD_HIGHEST_CUR_ACCT_BAL"), 
        lit(0).alias("YTD_AGGR_BANK_SHR_BAL"), 
        lit(0).alias("GL_YTD_AVG_BANK_SHR_BAL"), 
        lit(0).alias("GL_YTD_AGGR_BANK_SHR_BAL"), 
        when((length(col("TIMST_INTENP")) == lit(0)), lit(0)).otherwise(col("TIMST_INTENP")).alias("MTD_INT_ACCR_AMT"), 
        when(
            (
              call_spark_fcn("iconv", trim(col("TIMST_INTDTCHG")), lit("D"))
              > call_spark_fcn("iconv", trim(col("TIMST_ISSDATE")), lit("D"))
            ), 
            trim(col("TIMST_INTDTCHG"))
          )\
          .otherwise(lit(None))\
          .alias("LAST_INT_REPRC_DT"), 
        lit(0).alias("MTD_AVG_BAL"), 
        lit(0).alias("MTD_LOWEST_CUR_ACCT_BAL"), 
        lit(0).alias("MTD_AGGR_BAL"), 
        lit(0).alias("GL_MTD_AGGR_INT_AMT"), 
        when((length(trim(col("TIMST_INTENP"))) > lit(0)), col("TIMST_INTENP")).otherwise(lit(0)).alias("INT_ACCR_BAL"), 
        lit(0).alias("GL_QTD_AGGR_CUR_BAL"), 
        lit(0).alias("MTD_HIGHEST_CUR_ACCT_BAL"), 
        col("RptRecInd"), 
        col("OUT_1").alias("OPEN_CLOSE_CD"), 
        when(
            ((col("TIMST_BALCUR") != lit(0)) & (col("TIMST_INTRATE") != lit(0))), 
            when(
                (col("TIMST_INTCODE") == lit("B")), 
                ((col("TIMST_BALCUR") * col("TIMST_INTRATE")) / noofdaysinyear(col("DATA_DATE")))
              )\
              .when(
                (col("TIMST_INTCODE") == lit("C")), 
                (
                  (
                    (
                      col("TIMST_BALCAGR")
                      / datastage_substring(lastdaycurrmonth(col("DATA_DATE")), lit(2), lit(1))
                    )
                    * col("TIMST_INTRATE")
                  )
                  / noofdaysinyear(col("DATA_DATE"))
                )
              )\
              .otherwise(lit(0))
          )\
          .otherwise(lit(0))\
          .alias("INT_PERDIEM_AMT"), 
        lit(0).alias("GL_YTD_AGGR_INT_AMT"), 
        lit(0).alias("MTD_AGGR_INT_RATE"), 
        lit(0).alias("GL_QTD_AVG_BANK_SHR_BAL"), 
        lit(0).alias("GL_MTD_AVG_BANK_BAL"), 
        call_spark_fcn("oconv", lit(Config.DATE), "yyyy-MM-dd").alias("ENTRY_DT"), 
        when((col("OUT_1") == lit("C")), lit(None))\
          .when(
            (
              call_spark_fcn("iconv", trim(col("TIMST_RATENEXT")), lit("D"))
              > call_spark_fcn("iconv", trim(col("TIMST_ISSDATE")), lit("D"))
            ), 
            trim(col("TIMST_RATENEXT"))
          )\
          .otherwise(trim(col("TIMST_RENNEXT")))\
          .alias("NEXT_INT_REPRC_DT"), 
        lit(0).alias("GL_YTD_AGGR_CUR_BAL"), 
        lit(0).alias("GL_MTD_AVG_CUR_BAL"), 
        lit(0).alias("YTD_INT_ACCR_AMT"), 
        lit(0).alias("YTD_AGGR_BAL"), 
        when(
            (
              ((((((length(trim(col("RFACO_ACCT"))) == lit(0)) & (length(trim(col("RFACO_ACCT"))) == lit(0))) & (length(trim(col("RFACO_ACCT"))) == lit(0))) & (length(trim(col("RFACO_ACCT"))) == lit(0))) & (length(trim(col("RFACO_ACCT"))) == lit(0))) & (length(trim(col("RFACO_ACCT"))) == lit(0)))
              & (length(trim(col("RFACO_ACCT"))) == lit(0))
            ), 
            lit(1)
          )\
          .when(
            (col("RFACO_OPTION") == lit("B")), 
            when(
                ((length(trim(col("SUM_OF_P_STMT"))) > lit(0)) & (col("SUM_OF_P_STMT") > lit(0))), 
                (col("SUM_OF_P_STMT") + col("SUM_OF_B_STMT"))
              )\
              .when((col("SUM_OF_B_STMT") > lit(0)), col("SUM_OF_B_STMT"))\
              .otherwise(lit(1))
          )\
          .when((col("RFACO_OPTION") == lit("P")), col("SUM_OF_P_STMT"))\
          .when(
            (
              (((length(trim(col("RFACO_ACCT"))) > lit(0)) & (length(trim(col("RFACO_ACCT"))) == lit(0))) & (length(trim(col("RFACO_ACCT"))) == lit(0)))
              & (length(trim(col("RFACO_ACCT"))) == lit(0))
            ), 
            when((col("SUM_OF_NULL_STMT") > lit(0)), col("SUM_OF_NULL_STMT")).otherwise(lit(1))
          )\
          .when(
            (
              ((col("RFACO_OPTION") == lit("C")) | (col("RFACO_OPTION") == lit("T")))
              | (col("RFACO_OPTION") == lit("N"))
            ), 
            lit(0)
          )\
          .otherwise(lit(0))\
          .alias("PAPER_STMT_CNT"), 
        lit(0).alias("GL_QTD_AVG_CUR_BAL"), 
        col("TIMST_ACCOUNT"), 
        lit(0).alias("MTD_AVG_BANK_BAL"), 
        lit(0).alias("GL_YTD_AVG_CUR_BAL"), 
        lit(0).alias("GL_MTD_AGGR_CUR_BAL"), 
        col("DATA_DATE").alias("AS_OF_DT"), 
        lit(0).alias("YTD_LOWEST_CUR_ACCT_BAL"), 
        lit(0).alias("GL_QTD_AGGR_BANK_SHR_BAL"), 
        col("TIMST_INST"), 
        when(
            (
              ((length(trim(col("SUM_OF_E_STMT"))) == lit(0)) & (length(trim(col("SUM_OF_T_STMT"))) == lit(0)))
              & (length(trim(col("SUM_OF_B_STMT"))) == lit(0))
            ), 
            lit(0)
          )\
          .when(
            ((col("SUM_OF_E_STMT") > lit(0)) | (col("SUM_OF_B_STMT") > lit(0))), 
            when((col("SUM_OF_T_STMT") > lit(0)), (col("SUM_OF_T_STMT") + lit(1))).otherwise(lit(1))
          )\
          .when((col("SUM_OF_T_STMT") > lit(0)), col("SUM_OF_T_STMT"))\
          .otherwise(lit(0))\
          .alias("ESTMT_CNT"), 
        lit(0).alias("GL_MTD_AGGR_BANK_SHR_INT_AMT"), 
        lit(None).alias("INT_INDX_BASE_RATE"), 
        lit(0).alias("YTD_AVG_BAL"), 
        lit(None).alias("DATA_DT"), 
        when((length(col("TIMST_BALCUR")) == lit(0)), lit(0)).otherwise(col("TIMST_BALCUR")).alias("CUR_BAL"), 
        col("ACCT_ID"), 
        when(
            ((col("TIMST_BALCUR") != lit(0)) & (col("TIMST_INTRATE") != lit(0))), 
            when(
                (col("TIMST_INTCODE") == lit("B")), 
                ((col("TIMST_BALCUR") * col("TIMST_INTRATE")) / noofdaysinyear(col("DATA_DATE")))
              )\
              .when(
                (col("TIMST_INTCODE") == lit("C")), 
                (
                  (
                    (
                      col("TIMST_BALCAGR")
                      / datastage_substring(lastdaycurrmonth(col("DATA_DATE")), lit(2), lit(1))
                    )
                    * col("TIMST_INTRATE")
                  )
                  / noofdaysinyear(col("DATA_DATE"))
                )
              )\
              .otherwise(lit(0))
          )\
          .otherwise(lit(0))\
          .alias("BANK_SHR_INT_PERDIEM_AMT"), 
        lit(0).alias("GL_YTD_AGGR_BANK_SHR_INT_AMT"), 
        lit(0).alias("YTD_AGGR_INT_RATE"), 
        when((length(col("TIMST_INTRATE")) == lit(0)), lit(0))\
          .otherwise((col("TIMST_INTRATE") * lit(100)))\
          .alias("INT_RATE"), 
        lit(0).alias("GL_MTD_AGGR_BANK_SHR_BAL"), 
        when(
            ((length(trim(col("TIMST_INTINDEX"))) > lit(0)) & num(trim(col("TIMST_INTINDEX")))), 
            trim(col("TIMST_INTINDEX"))
          )\
          .otherwise(lit(0))\
          .alias("TIMST_INTINDEX"), 
        lit(0).alias("MTD_AGGR_BANK_SHR_BAL"), 
        lit(0).alias("MTD_AVG_WT_INT_RATE"), 
        when(
            ((length(trim(col("TIMST_INTRATE"))) > lit(0)) & num(trim(col("TIMST_INTRATE")))), 
            trim(col("TIMST_INTRATE"))
          )\
          .otherwise(lit(0))\
          .alias("TIMST_INTRATE"), 
        lit(None).alias("INT_DT"), 
        when((length(trim(col("TIMST_INTENP"))) > lit(0)), col("TIMST_INTENP"))\
          .otherwise(lit(0))\
          .alias("BANK_SHR_INT_ACCR_BAL"), 
        when((length(col("TIMST_BALCUR")) == lit(0)), lit(0)).otherwise(col("TIMST_BALCUR")).alias("BANK_SHR_BAL")
    )
