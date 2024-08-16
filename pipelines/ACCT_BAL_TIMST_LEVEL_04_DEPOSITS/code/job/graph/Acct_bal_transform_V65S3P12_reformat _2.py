from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Acct_bal_transform_V65S3P12_reformat _2(spark: SparkSession, V65S3_join_link: DataFrame) -> DataFrame:
    return V65S3_join_link.select(
        col("LAST_INT_REPRC_DT"), 
        col("NEXT_INT_REPRC_DT"), 
        col("TIMST_ACCOUNT"), 
        col("TIMST_INST"), 
        col("TIMST_INTRATE")
    )
