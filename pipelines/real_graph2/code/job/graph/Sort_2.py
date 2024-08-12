from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Sort_2(spark: SparkSession, inDF: DataFrame) -> DataFrame:
    return inDF.orderBy(col("amount").desc())
