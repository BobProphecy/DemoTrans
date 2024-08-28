from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Acct_bal_transform_stage_var(spark: SparkSession, V65S3_join_link: DataFrame) -> DataFrame:
    return V65S3_join_link.withColumn(
        "RptRecInd",
        setrptrecind(col("OPEN_CLOSE_CD"), col("CLOSE_DT"), col("CUR_BAL"), col("BANK_SHR_BAL"), col("DATA_DATE"))
    )
