from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_299(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("part_id").alias("PartId"), 
        col("material_id").alias("MaterialId"), 
        col("part_number").alias("PartNumber"), 
        col("like_part_number").alias("LikePartNumber"), 
        col("reporting_series").alias("ReportingSeries")
    )
