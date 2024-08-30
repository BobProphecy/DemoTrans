from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_242_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in1.`Finished Good Part`") == col("in0.`Part Number`")), "leftanti")\
        .select(col("in0.MechanicalSample").alias("MechanicalSample"), col("in0.CableRetention").alias("CableRetention"), col("in0.Polarized").alias("Polarized"), col("in0.LeadFree").alias("LeadFree"), col("in0.DeliveryDueDate").alias("DeliveryDueDate"), col("in0.Packaging").alias("Packaging"), col("in0.WorkCenter").alias("WorkCenter"), col("in0.QTYprocessedPO").alias("QTYprocessedPO"), col("in0.Alignment").alias("Alignment"), col("in0.ReportingSeriesCode").alias("ReportingSeriesCode"), col("in0.WorkCenterNumber").alias("WorkCenterNumber"), col("in0.ScrewRetention").alias("ScrewRetention"), col("in0.DaisyChain").alias("DaisyChain"), col("in0.Routing").alias("Routing"), col("in0.IsProcessCompleted").alias("IsProcessCompleted"), col("in0.ProcessOrderId").alias("ProcessOrderId"), col("in0.MiddleReverse").alias("MiddleReverse"), col("in0.ScrapHours").alias("ScrapHours"), col("in0.Rows").alias("Rows"), col("in0.PowerPins").alias("PowerPins"), col("in0.Entry").alias("Entry"), col("in0.StepNumber").alias("StepNumber"), col("in0.Plating").alias("Plating"), col("in0.RunHours").alias("RunHours"), col("in0.Plant").alias("Plant"), col("in0.Country").alias("Country"), col("in0.StrainRelief").alias("StrainRelief"), col("in0.Pitch").alias("Pitch"), col("in0.QuantityRequired").alias("QuantityRequired"), col("in0.Speed").alias("Speed"), col("in0.ReverseWire").alias("ReverseWire"), col("in0.QTYProcessedWC").alias("QTYProcessedWC"), col("in0.ManufacturingDueDate").alias("ManufacturingDueDate"), col("in0.SurfaceMount").alias("SurfaceMount"), col("in0.ReverseNotch").alias("ReverseNotch"), col("in0.OpticEnd").alias("OpticEnd"), col("in0.Facility").alias("Facility"), col("in0.Length").alias("Length"), col("in0.Latching").alias("Latching"), col("in0.`Part Number`").alias("Part Number"), col("in0.Lead").alias("Lead"), col("in0.scrapquantity").alias("scrapquantity"), col("in0.CableOptions").alias("CableOptions"), col("in0.Engines").alias("Engines"), col("in0.Retention").alias("Retention"), col("in0.Mount").alias("Mount"), col("in0.DifferentialPair").alias("DifferentialPair"), col("in0.ManufacturingOrderId").alias("ManufacturingOrderId"), col("in0.LineNumber").alias("LineNumber"), col("in0.Process").alias("Process"), col("in0.Breakout").alias("Breakout"), col("in0.Notch").alias("Notch"), col("in0.ReceivingChannels").alias("ReceivingChannels"), col("in0.SpaceOption").alias("SpaceOption"), col("in0.`Like Part`").alias("Like Part"), col("in0.Direction").alias("Direction"), col("in0.ReviewBoard").alias("ReviewBoard"), col("in0.Gender").alias("Gender"), col("in0.OutsideReverse").alias("OutsideReverse"), col("in0.Positions").alias("Positions"), col("in0.OpticsType").alias("OpticsType"), col("in0.DeadChannels").alias("DeadChannels"), col("in0.QTYproducedTotal").alias("QTYproducedTotal"), col("in0.isCustomerOrder").alias("isCustomerOrder"), col("in0.TransmittingChannels").alias("TransmittingChannels"), col("in0.ReverseConnector").alias("ReverseConnector"), col("in0.Shield").alias("Shield"), col("in0.Pad").alias("Pad"), col("in0.WiringOption").alias("WiringOption"), col("in0.WireGauge").alias("WireGauge"), col("in0.OrderNumber").alias("OrderNumber"), col("in0.SetupHours").alias("SetupHours"), col("in0.Key").alias("Key"), col("in0.Ends").alias("Ends"), col("in0.CompletedHours").alias("CompletedHours"), col("in0.FiberType").alias("FiberType"))
