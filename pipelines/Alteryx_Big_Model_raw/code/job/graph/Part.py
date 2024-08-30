from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Part(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("ReportingSeries"), 
        col("PartId"), 
        col("PartNumber"), 
        col("MaterialId"), 
        col("LikePartNumber")
    )
