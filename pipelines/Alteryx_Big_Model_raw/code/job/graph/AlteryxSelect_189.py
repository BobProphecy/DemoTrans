from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_189(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Tooling Set`").alias("Tooling Set"), 
        col("Code").alias("Machine Tooling Set Code"), 
        col("`Machine No`").alias("Machine Number")
    )
