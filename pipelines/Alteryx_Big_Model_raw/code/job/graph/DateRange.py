from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DateRange(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (col("FullDate") > add_months(current_date(), (lit(- 2).cast(IntegerType()).cast(IntegerType()) * lit(12))))
          & (col("FullDate") < add_months(current_date(), (lit(2).cast(IntegerType()).cast(IntegerType()) * lit(12))))
        )
    )
