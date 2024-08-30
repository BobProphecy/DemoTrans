from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_296(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("work_center_id").alias("WorkCenterId"), 
        col("work_center_number").alias("WorkCenterNumber"), 
        col("work_center").alias("WorkCenter"), 
        col("plant").alias("Plant"), 
        col("facility").alias("Facility"), 
        col("review_board").alias("ReviewBoard"), 
        col("country").alias("Country")
    )
