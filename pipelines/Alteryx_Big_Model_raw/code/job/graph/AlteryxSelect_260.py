from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_260(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Plant Process Code`").alias("Plant Process Code"), 
        col("`Days per Quarter`").alias("Days per Quarter"), 
        col("Process"), 
        col("`Days per Month`").alias("Days per Month"), 
        col("`Machine Type`").alias("Machine Type"), 
        col("Plant"), 
        col("`Facility Machine Group Name`").alias("Facility Machine Group Name"), 
        col("`Machine Group Notes`").alias("Machine Group Notes"), 
        col("Pipeline"), 
        col("`Part Wildcard`").alias("Part Wildcard"), 
        col("`Projected Annual Growth`").alias("Projected Annual Growth"), 
        col("`Target Utilization`").alias("Target Utilization"), 
        col("`Daily Production Hours`").alias("Daily Production Hours"), 
        col("`Machine Group ID`").alias("Machine Group ID"), 
        col("`Average Hourly Output`").alias("Average Hourly Output"), 
        col("`Automation Level`").alias("Automation Level")
    )
