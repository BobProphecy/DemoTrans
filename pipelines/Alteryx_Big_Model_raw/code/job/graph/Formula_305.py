from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_305(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("like_part", regexp_replace(col("like_part"), "[^x20-x7e]+", ""))\
        .withColumn("reschedule_notes", regexp_replace(col("reschedule_notes"), "[^x20-x7e]+", ""))\
        .withColumn("reschedule_reason", regexp_replace(col("reschedule_reason"), "[^x20-x7e]+", ""))
