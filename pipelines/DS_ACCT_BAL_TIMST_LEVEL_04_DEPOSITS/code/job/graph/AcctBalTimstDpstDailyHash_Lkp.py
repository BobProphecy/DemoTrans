from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AcctBalTimstDpstDailyHash_Lkp(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ACCT_ID", IntegerType(), True), StructField("ODATE", TimestampType(), True), StructField("MTD_LOWEST_CUR_ACCT_BAL", IntegerType(), True), StructField("MTD_HIGHEST_CUR_ACCT_BAL", IntegerType(), True), StructField("YTD_LOWEST_CUR_ACCT_BAL", IntegerType(), True), StructField("YTD_HIGHEST_CUR_ACCT_BAL", IntegerType(), True), StructField("GL_MTD_AGGR_BANK_SHR_INT_AMT", IntegerType(), True), StructField("GL_MTD_AGGR_BANK_SHR_BAL", IntegerType(), True), StructField("GL_MTD_AGGR_CUR_BAL", IntegerType(), True), StructField("GL_MTD_AGGR_INT_AMT", IntegerType(), True), StructField("GL_MTD_AVG_CUR_BAL", IntegerType(), True), StructField("GL_MTD_AVG_BANK_BAL", IntegerType(), True), StructField("GL_YTD_AGGR_BANK_SHR_INT_AMT", IntegerType(), True), StructField("GL_YTD_AGGR_BANK_SHR_BAL", IntegerType(), True), StructField("GL_YTD_AGGR_CUR_BAL", IntegerType(), True), StructField("GL_YTD_AGGR_INT_AMT", IntegerType(), True), StructField("GL_YTD_AVG_CUR_BAL", IntegerType(), True), StructField("GL_YTD_AVG_BANK_SHR_BAL", IntegerType(), True), StructField("CUR_BAL", IntegerType(), True), StructField("BANK_SHR_BAL", IntegerType(), True), StructField("INT_PERDIEM_AMT", IntegerType(), True), StructField("BANK_SHR_INT_PERDIEM_AMT", IntegerType(), True), StructField("APPL_CD", StringType(), True), StructField("ENTRY_DT", TimestampType(), True), StructField("AS_OF_DT", TimestampType(), True), StructField("GL_QTD_AGGR_BANK_SHR_BAL", IntegerType(), True), StructField("GL_QTD_AVG_BANK_SHR_BAL", IntegerType(), True), StructField("GL_QTD_AGGR_CUR_BAL", IntegerType(), True), StructField("GL_QTD_AVG_CUR_BAL", IntegerType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv(f"{Config.HASH_DIR}/AcctBalTimstDpstDailyHash")
