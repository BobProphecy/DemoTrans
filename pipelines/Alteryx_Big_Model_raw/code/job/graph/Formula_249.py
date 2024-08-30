from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_249(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "isMatch",
        (
          (length(expr("regexp_extract(`Like Part`, `Part Wildcard`, 0)")).cast(IntegerType()) > lit(0))
          | col("`Part Wildcard`").isNull()
        )
    )
