from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_245_reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Entry").cast(StringType()).alias("Entry"), 
        col("Plating").cast(StringType()).alias("Plating"), 
        col("Routing").cast(StringType()).alias("Routing"), 
        col("ProcessOrderId").cast(IntegerType()).alias("ProcessOrderId"), 
        col("WorkCenterNumber").cast(StringType()).alias("WorkCenterNumber"), 
        date_format(to_date(col("ManufacturingDueDate"), "yyyy-MM-dd HH:mm:ss.SSSS"), "yyyy-MM-dd HH:mm:ss.SSSS")\
          .alias("ManufacturingDueDate"), 
        col("SetupHours").cast(DoubleType()).alias("SetupHours"), 
        col("ReceivingChannels").cast(StringType()).alias("ReceivingChannels"), 
        col("Polarized").cast(StringType()).alias("Polarized"), 
        col("QuantityRequired").cast(IntegerType()).alias("QuantityRequired"), 
        col("StrainRelief").cast(StringType()).alias("StrainRelief"), 
        col("Country").cast(StringType()).alias("Country"), 
        col("Latching").cast(StringType()).alias("Latching"), 
        col("RunHours").cast(DoubleType()).alias("RunHours"), 
        col("Length").cast(StringType()).alias("Length"), 
        col("Retention").cast(StringType()).alias("Retention"), 
        col("CableRetention").cast(StringType()).alias("CableRetention"), 
        col("ReverseConnector").cast(StringType()).alias("ReverseConnector"), 
        col("ScrewRetention").cast(StringType()).alias("ScrewRetention"), 
        col("`Like Part`").cast(StringType()).alias("Like Part"), 
        col("QTYProcessedWC").cast(IntegerType()).alias("QTYProcessedWC"), 
        col("Facility").cast(StringType()).alias("Facility"), 
        col("`Finished Good Part`").alias("Finished Good Part"), 
        col("WireGauge").cast(StringType()).alias("WireGauge"), 
        col("Rows").cast(StringType()).alias("Rows"), 
        col("Speed").cast(StringType()).alias("Speed"), 
        col("ScrapQuantity").cast(IntegerType()).alias("ScrapQuantity"), 
        col("MiddleReverse").cast(StringType()).alias("MiddleReverse"), 
        col("Plant").cast(StringType()).alias("Plant"), 
        col("ScrapHours").cast(DoubleType()).alias("ScrapHours"), 
        col("QTYProcessedPO").cast(IntegerType()).alias("QTYProcessedPO"), 
        col("WorkCenter").cast(StringType()).alias("WorkCenter"), 
        col("SpaceOption").cast(StringType()).alias("SpaceOption"), 
        col("ReviewBoard").cast(StringType()).alias("ReviewBoard"), 
        col("Breakout").cast(StringType()).alias("Breakout"), 
        col("SurfaceMount").cast(StringType()).alias("SurfaceMount"), 
        col("LeadFree").cast(StringType()).alias("LeadFree"), 
        date_format(to_date(col("DeliveryDueDate"), "yyyy-MM-dd HH:mm:ss.SSSS"), "yyyy-MM-dd HH:mm:ss.SSSS")\
          .alias("DeliveryDueDate"), 
        col("Packaging").cast(StringType()).alias("Packaging"), 
        col("MechanicalSample").cast(StringType()).alias("MechanicalSample"), 
        col("Engines").cast(StringType()).alias("Engines"), 
        col("DaisyChain").cast(StringType()).alias("DaisyChain"), 
        col("DifferentialPair").cast(StringType()).alias("DifferentialPair"), 
        col("TransmittingChannels").cast(StringType()).alias("TransmittingChannels"), 
        col("CompletedHours").cast(DoubleType()).alias("CompletedHours"), 
        col("DeadChannels").cast(StringType()).alias("DeadChannels"), 
        col("WiringOption").cast(StringType()).alias("WiringOption"), 
        col("Alignment").cast(StringType()).alias("Alignment"), 
        col("ReportingSeriesCode").cast(StringType()).alias("ReportingSeriesCode"), 
        col("Ends").cast(StringType()).alias("Ends"), 
        col("FiberType").cast(StringType()).alias("FiberType"), 
        col("OrderNumber").cast(IntegerType()).alias("OrderNumber"), 
        col("LineNumber").cast(IntegerType()).alias("LineNumber"), 
        col("QTYproducedTotal").cast(IntegerType()).alias("QTYproducedTotal"), 
        col("IsProcessCompleted").cast(BooleanType()).alias("IsProcessCompleted"), 
        col("Shield").cast(StringType()).alias("Shield"), 
        col("ManufacturingOrderId").cast(IntegerType()).alias("ManufacturingOrderId"), 
        col("Pad").cast(StringType()).alias("Pad"), 
        col("ReverseWire").cast(StringType()).alias("ReverseWire"), 
        col("OutsideReverse").cast(StringType()).alias("OutsideReverse"), 
        col("ReverseNotch").cast(StringType()).alias("ReverseNotch"), 
        col("CableOptions").cast(StringType()).alias("CableOptions"), 
        col("Gender").cast(StringType()).alias("Gender"), 
        col("Pitch").cast(StringType()).alias("Pitch"), 
        col("Lead").cast(StringType()).alias("Lead"), 
        col("Process").cast(StringType()).alias("Process"), 
        col("IsCustomerOrder").cast(BooleanType()).alias("IsCustomerOrder"), 
        col("PowerPins").cast(StringType()).alias("PowerPins"), 
        col("Notch").cast(StringType()).alias("Notch"), 
        col("StepNumber").cast(IntegerType()).alias("StepNumber"), 
        col("OpticsType").cast(StringType()).alias("OpticsType"), 
        col("Mount").cast(StringType()).alias("Mount"), 
        col("OpticEnd").cast(StringType()).alias("OpticEnd"), 
        col("Key").cast(StringType()).alias("Key"), 
        col("Positions").cast(StringType()).alias("Positions"), 
        col("Direction").cast(StringType()).alias("Direction"), 
        lit(None).cast(StringType()).alias("Part Number"), 
        lit(None).cast(StringType()).alias("Tooling Set")
    )
