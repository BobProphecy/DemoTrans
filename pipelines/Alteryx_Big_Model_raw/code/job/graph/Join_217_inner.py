from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_217_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.ManufacturingOrderId") == col("in1.ManufacturingOrderId"))
            & (col("in0.WorkCenterId") == col("in1.WorkCenterId"))
          ),
          "inner"
        )\
        .select(col("in0.ManufacturingOrderId").alias("ManufacturingOrderId"), col("in0.RunHours").alias("RunHours"), col("in0.Plant").alias("Plant"), col("in0.WorkCenterNumber").alias("WorkCenterNumber"), col("in0.Process").alias("Process"), col("in0.MaxStepNumberOrder").alias("MaxStepNumberOrder"), col("in0.IsCompleteShort").alias("IsCompleteShort"), col("in0.Country").alias("Country"), col("in0.SetupHours").alias("SetupHours"), col("in0.WorkCenter").alias("WorkCenter"), col("in0.WorkCenterId").alias("WorkCenterId"), col("in0.Routing").alias("Routing"), col("in0.ProcessOrderId").alias("ProcessOrderId"), col("in0.ReviewBoard").alias("ReviewBoard"), col("in0.Facility").alias("Facility"), col("in1.MaxStepNumberWC").alias("MaxStepNumberWC"), col("in0.StepNumber").alias("StepNumber"), col("in0.Deleted").alias("Deleted"))
