from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_302(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("date_entered").alias("DateEntered"), 
        col("deliver_to").alias("DeliverTo"), 
        col("IsSalesOrder"), 
        col("IsSampleOrder"), 
        col("is_scarlet").alias("IsScarlet"), 
        col("manufacturing_due_date").alias("ManufacturingDueDate"), 
        col("manufacturing_on_time_status").alias("ManufacturingOnTimeStatus"), 
        col("manufacturing_order_id").alias("ManufacturingOrderId"), 
        col("reschedule_description").alias("RescheduleNotes"), 
        col("reschedule_reason").alias("RescheduleReason"), 
        col("shipping_on_time_status").alias("ShippingOnTimeStatus")
    )
