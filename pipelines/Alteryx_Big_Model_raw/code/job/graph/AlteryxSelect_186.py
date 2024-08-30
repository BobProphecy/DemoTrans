from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_186(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("`Machine Group Id`").alias("Machine Group Id"), col("Name").alias("Machine Number"))
