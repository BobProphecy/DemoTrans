from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_156(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Daily Production Hours`").alias("Daily Production Hours"), 
        col("Plant"), 
        col("`Machine Group ID`").alias("Machine Group ID"), 
        col("Name").alias("Facility Machine Group Name"), 
        col("`Average Hourly Output`").alias("Average Hourly Output"), 
        col("`Projected Annual Growth`").alias("Projected Annual Growth"), 
        col("`Target Utilization`").alias("Target Utilization"), 
        col("Pipeline"), 
        col("`Machine Group Notes`").alias("Machine Group Notes"), 
        col("`Days per Quarter`").alias("Days per Quarter"), 
        col("`Automation Level_Name`").alias("Automation Level"), 
        col("`Machine Type`").alias("Machine Type"), 
        col("`Days per Month`").alias("Days per Month")
    )
