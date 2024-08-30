from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_261(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Plant Process Code`").alias("Plant Process Code"), 
        col("Lead"), 
        col("DaisyChain"), 
        col("Country"), 
        col("ScrewRetention"), 
        col("`Days per Quarter`").alias("Days per Quarter"), 
        col("Shield"), 
        col("Process"), 
        col("DifferentialPair"), 
        col("StepNumber"), 
        col("Facility"), 
        col("Retention"), 
        col("`Days per Month`").alias("Days per Month"), 
        col("ReportingSeriesCode"), 
        col("Routing"), 
        col("DeadChannels"), 
        col("ReverseNotch"), 
        col("Rows"), 
        col("WiringOption"), 
        col("`Machine Type`").alias("Machine Type"), 
        col("SurfaceMount"), 
        col("IsCustomerOrder"), 
        col("Entry"), 
        col("QTYProcessedPO"), 
        col("WorkCenterNumber"), 
        col("ReviewBoard"), 
        col("RunHours"), 
        col("Plant"), 
        col("FiberType"), 
        col("Ends"), 
        col("CompletedHours"), 
        col("QTYproducedTotal"), 
        col("IsProcessCompleted"), 
        col("`Facility Machine Group Name`").alias("Facility Machine Group Name"), 
        col("OutsideReverse"), 
        col("MechanicalSample"), 
        col("Length"), 
        col("Packaging"), 
        col("ReverseWire"), 
        col("`Machine Group Notes`").alias("Machine Group Notes"), 
        col("OpticEnd"), 
        col("`Like Part`").alias("Like Part"), 
        col("Alignment"), 
        col("CableRetention"), 
        col("Direction"), 
        col("WorkCenter"), 
        col("ScrapHours"), 
        col("OrderNumber"), 
        col("ReverseConnector"), 
        col("Pitch"), 
        col("Positions"), 
        col("Latching"), 
        col("Pipeline"), 
        col("CableOptions"), 
        col("ManufacturingOrderId"), 
        col("`Part Wildcard`").alias("Part Wildcard"), 
        col("LeadFree"), 
        col("QTYProcessedWC"), 
        col("Speed"), 
        col("Notch"), 
        col("SetupHours"), 
        col("`Projected Annual Growth`").alias("Projected Annual Growth"), 
        col("Engines"), 
        col("PowerPins"), 
        col("QuantityRequired"), 
        col("LineNumber"), 
        col("`Target Utilization`").alias("Target Utilization"), 
        col("`Daily Production Hours`").alias("Daily Production Hours"), 
        col("ReceivingChannels"), 
        col("`Part Number`").alias("Part Number"), 
        col("isMatch"), 
        col("MiddleReverse"), 
        col("`Machine Group ID`").alias("Machine Group ID"), 
        col("TransmittingChannels"), 
        col("Breakout"), 
        col("Pad"), 
        col("OpticsType"), 
        col("Gender"), 
        col("`Finished Good Part`").alias("Finished Good Part"), 
        col("ManufacturingDueDate"), 
        col("`Average Hourly Output`").alias("Average Hourly Output"), 
        col("`Tooling Set`").alias("Tooling Set"), 
        col("ProcessOrderId"), 
        col("Key"), 
        col("SpaceOption"), 
        col("StrainRelief"), 
        col("`Automation Level`").alias("Automation Level"), 
        col("WireGauge"), 
        col("Plating"), 
        col("Polarized"), 
        col("Mount"), 
        col("ScrapQuantity"), 
        col("DeliveryDueDate")
    )
