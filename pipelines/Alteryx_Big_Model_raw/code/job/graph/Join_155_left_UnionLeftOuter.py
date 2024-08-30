from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_155_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`Machine Group ID`") == col("in1.`Machine Group ID`")), "leftouter")\
        .select(col("in0.`Daily Production Hours`").alias("Daily Production Hours"), col("in0.Plant").alias("Plant"), col("in0.`Machine Group ID`").alias("Machine Group ID"), col("in0.`Facility Machine Group Name`").alias("Facility Machine Group Name"), col("in1.Process").alias("Process"), col("in0.`Average Hourly Output`").alias("Average Hourly Output"), col("in0.`Projected Annual Growth`").alias("Projected Annual Growth"), col("in0.`Target Utilization`").alias("Target Utilization"), col("in0.Pipeline").alias("Pipeline"), col("in0.`Machine Group Notes`").alias("Machine Group Notes"), col("in1.`Plant Process Code`").alias("Plant Process Code"), col("in0.`Days per Quarter`").alias("Days per Quarter"), col("in0.`Automation Level`").alias("Automation Level"), col("in0.`Machine Type`").alias("Machine Type"), col("in0.`Days per Month`").alias("Days per Month"))
