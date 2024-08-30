from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_201(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("ManufacturingOrderId"), 
        col("RunHours"), 
        col("Description").alias("Process"), 
        col("IsCompleteShort"), 
        col("SetupHours"), 
        col("WorkCenterMasterId").alias("WorkCenterId"), 
        col("Routing"), 
        col("ProcessOrderId"), 
        col("StepNumber"), 
        col("Deleted")
    )
