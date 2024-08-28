from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SQ_Orders(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("order_id"), col("customer_id"), col("order_status"), col("order_date"), col("amount"))
