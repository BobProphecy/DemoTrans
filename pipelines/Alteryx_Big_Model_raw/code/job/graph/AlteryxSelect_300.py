from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_300(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("IsScarlet"), 
        col("ManufacturingOnTimeStatus"), 
        col("IsSalesOrder"), 
        col("RescheduleReason"), 
        col("RescheduleNotes"), 
        col("DeliverTo"), 
        col("ShippingOnTimeStatus"), 
        col("DateEntered"), 
        col("ManufacturingOrderId"), 
        col("IsSampleOrder"), 
        col("ManufacturingDueDate")
    )
