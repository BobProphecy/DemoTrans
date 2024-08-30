from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_286(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`spo.FiberType`").alias("spo.FiberType"), 
        col("`spo.CableRetention`").alias("spo.CableRetention"), 
        col("`Days per Quarter`").alias("Days per Quarter"), 
        col("Process"), 
        col("`spo.Gender`").alias("spo.Gender"), 
        col("`spo.Rows`").alias("spo.Rows"), 
        col("DateRefreshed"), 
        col("`Days per Month`").alias("Days per Month"), 
        col("`spo.Direction`").alias("spo.Direction"), 
        col("`spo.Pitch`").alias("spo.Pitch"), 
        col("`spo.ReverseNotch`").alias("spo.ReverseNotch"), 
        col("`Reschedule Reason`").alias("Reschedule Reason"), 
        col("`Completed Hours`").alias("Completed Hours"), 
        col("`spo.ReverseWire`").alias("spo.ReverseWire"), 
        col("`Manufacturing Order Id`").alias("Manufacturing Order Id"), 
        col("`spo.Packaging`").alias("spo.Packaging"), 
        col("`Tooling BOM Part Number`").alias("Tooling BOM Part Number"), 
        col("`spo.SpaceOption`").alias("spo.SpaceOption"), 
        col("`spo.Engines`").alias("spo.Engines"), 
        col("`spo.Breakout`").alias("spo.Breakout"), 
        col("`spo.OpticEnd`").alias("spo.OpticEnd"), 
        col("`Scrap Hours`").alias("Scrap Hours"), 
        col("`Work Center Number`").alias("Work Center Number"), 
        col("`Machine Type`").alias("Machine Type"), 
        col("`spo.Key`").alias("spo.Key"), 
        col("`Manufacturing On Time Status`").alias("Manufacturing On Time Status"), 
        col("`spo.ScrewRetention`").alias("spo.ScrewRetention"), 
        col("`spo.ReverseConnector`").alias("spo.ReverseConnector"), 
        col("ReviewBoard"), 
        col("`spo.MechanicalSample`").alias("spo.MechanicalSample"), 
        col("Plant"), 
        col("`spo.MiddleReverse`").alias("spo.MiddleReverse"), 
        col("`spo.OutsideReverse`").alias("spo.OutsideReverse"), 
        col("`spo.Plating`").alias("spo.Plating"), 
        col("`spo.Shield`").alias("spo.Shield"), 
        col("`spo.PowerPins`").alias("spo.PowerPins"), 
        col("`spo.StrainRelief`").alias("spo.StrainRelief"), 
        col("`Facility Machine Group Name`").alias("Facility Machine Group Name"), 
        col("`Machine Group Notes`").alias("Machine Group Notes"), 
        col("`spo.Polarized`").alias("spo.Polarized"), 
        col("`spo.Notch`").alias("spo.Notch"), 
        col("`Like Part`").alias("Like Part"), 
        col("`Process Order Quantity Processed`").alias("Process Order Quantity Processed"), 
        col("`Scrap Quantity`").alias("Scrap Quantity"), 
        col("`spo.Alignment`").alias("spo.Alignment"), 
        col("WorkCenter"), 
        col("`Is Scarlet`").alias("Is Scarlet"), 
        col("OrderNumber"), 
        col("Pipeline"), 
        col("`spo.DifferentialPair`").alias("spo.DifferentialPair"), 
        col("`Part Wildcard`").alias("Part Wildcard"), 
        col("`spo.Latching`").alias("spo.Latching"), 
        col("`spo.DaisyChain`").alias("spo.DaisyChain"), 
        col("`Manufacturing Due Date`").alias("Manufacturing Due Date"), 
        col("`spo.Length`").alias("spo.Length"), 
        col("SetupHours"), 
        col("`Projected Annual Growth`").alias("Projected Annual Growth"), 
        col("`spo.Positions`").alias("spo.Positions"), 
        col("QuantityRequired"), 
        col("`spo.Retention`").alias("spo.Retention"), 
        col("`spo.LeadFree`").alias("spo.LeadFree"), 
        col("LineNumber"), 
        col("`Target Utilization`").alias("Target Utilization"), 
        col("`Daily Production Hours`").alias("Daily Production Hours"), 
        col("`Process Plant Code`").alias("Process Plant Code"), 
        col("`spo.Lead`").alias("spo.Lead"), 
        col("`spo.OpticsType`").alias("spo.OpticsType"), 
        col("`spo.WiringOption`").alias("spo.WiringOption"), 
        col("`spo.TransmittingChannels`").alias("spo.TransmittingChannels"), 
        col("`Part Number`").alias("Part Number"), 
        col("`spo.Speed`").alias("spo.Speed"), 
        col("`spo.Pad`").alias("spo.Pad"), 
        col("`Deliver To`").alias("Deliver To"), 
        col("`Reschedule Notes`").alias("Reschedule Notes"), 
        col("`Machine Group ID`").alias("Machine Group ID"), 
        col("`Run Hours`").alias("Run Hours"), 
        col("`Is Customer Order`").alias("Is Customer Order"), 
        col("`spo.WireGauge`").alias("spo.WireGauge"), 
        col("`Date Entered`").alias("Date Entered"), 
        col("`Average Hourly Output`").alias("Average Hourly Output"), 
        col("`spo.Mount`").alias("spo.Mount"), 
        col("`Tooling Set`").alias("Tooling Set"), 
        col("`spo.CableOptions`").alias("spo.CableOptions"), 
        col("`Automation Level`").alias("Automation Level"), 
        col("`Reporting Series`").alias("Reporting Series"), 
        col("`spo.Entry`").alias("spo.Entry"), 
        col("`Shipping On Time Status`").alias("Shipping On Time Status"), 
        col("`spo.ReceivingChannels`").alias("spo.ReceivingChannels"), 
        col("`spo.DeadChannels`").alias("spo.DeadChannels"), 
        col("`spo.Ends`").alias("spo.Ends"), 
        col("`spo.SurfaceMount`").alias("spo.SurfaceMount"), 
        col("DeliveryDueDate")
    )
