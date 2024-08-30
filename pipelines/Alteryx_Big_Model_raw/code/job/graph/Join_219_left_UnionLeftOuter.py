from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_219_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.ProcessOrderId") == col("in1.ProcessOrderId")), "leftouter")\
        .select(col("in0.ManufacturingOrderId").alias("ManufacturingOrderId"), col("in1.GoodQuantity").alias("QTYProcessedPO"), col("in0.RunHours").alias("RunHours"), col("in0.Plant").alias("Plant"), col("in0.IsLastStepWorkCenter").alias("IsLastStepWorkCenter"), col("in0.WorkCenterNumber").alias("WorkCenterNumber"), col("in0.Process").alias("Process"), col("in0.MaxStepNumberOrder").alias("MaxStepNumberOrder"), col("in0.IsLastStepOrder").alias("IsLastStepOrder"), col("in0.IsCompleteShort").alias("IsCompleteShort"), col("in0.Country").alias("Country"), col("in0.SetupHours").alias("SetupHours"), col("in0.WorkCenter").alias("WorkCenter"), col("in0.WorkCenterId").alias("WorkCenterId"), col("in0.Routing").alias("Routing"), col("in0.ProcessOrderId").alias("ProcessOrderId"), col("in0.ReviewBoard").alias("ReviewBoard"), col("in0.Facility").alias("Facility"), col("in0.MaxStepNumberWC").alias("MaxStepNumberWC"), col("in1.ScrapQuantity").alias("ScrapQuantity"), col("in1.TotalQuantity").alias("TotalQuantity"), col("in0.StepNumber").alias("StepNumber"), col("in0.Deleted").alias("Deleted"))
