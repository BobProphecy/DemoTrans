from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_220(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("QTYProcessedWC", when(col("IsLastStepWorkCenter"), col("QTYProcessedPO")).otherwise(lit(0)))\
        .withColumn("QTYProcessedTotal", when(col("IsLastStepOrder"), col("QTYProcessedPO")).otherwise(lit(0)))
