from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Rollup_Reformat(spark: SparkSession, inDF: DataFrame) -> DataFrame:
    return inDF.select(col("customer_id"), col("first_name"), col("last_name"), col("amount"))
