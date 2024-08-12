from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def reformatted_join_by_customer_id(spark: SparkSession, join_by_customer_id: DataFrame) -> DataFrame:
    return join_by_customer_id.select(col("customer_id"), col("first_name"), col("last_name"), col("amount"))
