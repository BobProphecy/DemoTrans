from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_279(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Packaging"), 
        col("FiberType"), 
        col("Lead"), 
        col("WireGauge"), 
        col("ScrewRetention"), 
        col("DeadChannels"), 
        col("LeadFree"), 
        col("SpaceOption"), 
        col("Ends"), 
        col("Polarized"), 
        col("Latching"), 
        col("Entry"), 
        col("Shield"), 
        col("ReverseConnector"), 
        col("ReceivingChannels"), 
        col("WiringOption"), 
        col("Direction"), 
        col("PartId"), 
        col("SurfaceMount"), 
        col("DaisyChain"), 
        col("Pad"), 
        col("OpticEnd"), 
        col("Key"), 
        col("Retention"), 
        col("TransmittingChannels"), 
        col("ReverseWire"), 
        col("Engines"), 
        col("MiddleReverse"), 
        col("Positions"), 
        col("StrainRelief"), 
        col("PowerPins"), 
        col("MechanicalSample"), 
        col("DifferentialPair"), 
        col("Pitch"), 
        col("Alignment"), 
        col("Length"), 
        col("OutsideReverse"), 
        col("CableOptions"), 
        col("Mount"), 
        col("Notch"), 
        col("OpticsType"), 
        col("Breakout"), 
        col("Rows"), 
        col("CableRetention"), 
        col("ReverseNotch"), 
        col("Speed"), 
        col("Gender"), 
        col("Plating")
    )
