from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_304(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Like Part`").alias("like_part"), 
        col("`Reschedule Notes`").alias("reschedule_notes"), 
        col("Plant").alias("plant"), 
        col("`Is Customer Order`").alias("is_customer_order"), 
        col("`Tooling Set`").alias("tooling_set"), 
        col("`Manufacturing Order Id`").alias("manufacturing_order_id"), 
        col("`Deliver To`").alias("deliver_to"), 
        col("`Machine Group ID`").alias("machine_group_id"), 
        col("`Manufacturing On Time Status`").alias("manufacturing_on_time_status"), 
        col("OrderNumber").alias("order_number"), 
        col("`Is Scarlet`").alias("is_scarlet"), 
        col("Process").alias("process"), 
        col("`Days per Month`").alias("days_per_month"), 
        col("`Average Hourly Output`").alias("average_hourly_output"), 
        col("`Tooling BOM Part Number`").alias("tooling_bom_part_number"), 
        col("`Manufacturing Due Date`").alias("manufacturing_due_date"), 
        col("`Machine Group Notes`").alias("machine_group_notes"), 
        col("`Part Number`").alias("part_number"), 
        col("DeliveryDueDate").alias("delivery_due_date"), 
        col("`Scrap Hours`").alias("scrap_hours"), 
        col("`Target Utilization`").alias("target_utilization"), 
        col("`Daily Production Hours`").alias("daily_production_hours"), 
        col("`Reporting Series`").alias("reporting_series"), 
        col("`Run Hours`").alias("run_hours"), 
        col("`Reschedule Reason`").alias("reschedule_reason"), 
        col("Pipeline").alias("pipeline"), 
        col("`Completed Hours`").alias("completed_hours"), 
        col("SetupHours").alias("setup_hours"), 
        col("LineNumber").alias("line_number"), 
        col("`Part Wildcard`").alias("part_wildcard"), 
        col("`Work Center Number`").alias("work_center_number"), 
        col("WorkCenter").alias("work_center"), 
        col("`Machine Type`").alias("machine_type"), 
        col("`Process Order Quantity Processed`").alias("process_order_quantity"), 
        col("QuantityRequired").alias("quantity_required"), 
        col("`Days per Quarter`").alias("days_per_quarter"), 
        col("`Shipping On Time Status`").alias("shipping_on_time_status"), 
        col("`Automation Level`").alias("automation_level"), 
        col("ReviewBoard").alias("review_board"), 
        col("`Projected Annual Growth`").alias("projected_annual_growth"), 
        col("DateRefreshed").alias("date_refreshed"), 
        col("`Facility Machine Group Name`").alias("facility_machine_group_name"), 
        col("`Date Entered`").alias("date_entered"), 
        col("`Scrap Quantity`").alias("scrap_quantity"), 
        col("`Process Plant Code`").alias("process_plant_code")
    )
