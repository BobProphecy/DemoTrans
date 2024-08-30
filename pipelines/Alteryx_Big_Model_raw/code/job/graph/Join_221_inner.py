from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_221_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.ManufacturingOrderId") == col("in1.ManufacturingOrderId")), "inner")\
        .select(col("in0.DeliveryDueDate").alias("DeliveryDueDate"), col("in0.ManufacturingOrderId").alias("ManufacturingOrderId"), col("in1.QTYProcessedPO").alias("QTYProcessedPO"), col("in1.RunHours").alias("RunHours"), col("in1.Plant").alias("Plant"), col("in1.IsLastStepWorkCenter").alias("IsLastStepWorkCenter"), col("in0.LineNumber").alias("LineNumber"), col("in1.WorkCenterNumber").alias("WorkCenterNumber"), col("in1.Process").alias("Process"), col("in1.MaxStepNumberOrder").alias("MaxStepNumberOrder"), col("in1.IsLastStepOrder").alias("IsLastStepOrder"), col("in0.QuantityRequired").alias("QuantityRequired"), col("in1.QTYProcessedWC").alias("QTYProcessedWC"), col("in1.IsCompleteShort").alias("IsCompleteShort"), col("in1.Country").alias("Country"), col("in1.SetupHours").alias("SetupHours"), col("in1.WorkCenter").alias("WorkCenter"), col("in1.WorkCenterId").alias("WorkCenterId"), col("in0.ManufacturingDueDate").alias("ManufacturingDueDate"), col("in1.Routing").alias("Routing"), col("in1.ProcessOrderId").alias("ProcessOrderId"), col("in1.ReviewBoard").alias("ReviewBoard"), col("in0.MaterialId").alias("MaterialId"), col("in1.Facility").alias("Facility"), col("in1.QTYProcessedTotal").alias("QTYProcessedTotal"), col("in1.MaxStepNumberWC").alias("MaxStepNumberWC"), col("in1.ScrapQuantity").alias("ScrapQuantity"), col("in0.OrderNumber").alias("OrderNumber"), col("in1.TotalQuantity").alias("TotalQuantity"), col("in1.StepNumber").alias("StepNumber"), col("in0.Deleted").alias("Deleted"))
