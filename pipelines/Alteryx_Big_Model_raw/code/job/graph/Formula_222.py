from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_222(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "CompletedHours",
          when((col("QuantityRequired").cast(IntegerType()) <= lit(0)), lit(0))\
            .when((col("QTYProcessedPO").isNull() | (col("QTYProcessedPO").cast(IntegerType()) <= lit(0))), lit(0))\
            .otherwise(((col("QTYProcessedPO") / col("QuantityRequired")) * col("RunHours")))
        )\
        .withColumn(
          "ScrapHours",
          when((col("QuantityRequired").cast(IntegerType()) <= lit(0)), lit(0))\
            .when((col("ScrapQuantity").isNull() | (col("ScrapQuantity").cast(IntegerType()) <= lit(0))), lit(0))\
            .otherwise((col("ScrapQuantity") * (col("RunHours") / col("QuantityRequired"))))
        )\
        .withColumn("IsProcessCompleted", (col("IsCompleteShort") | (col("QTYProcessedPO") >= col("QuantityRequired"))))\
        .withColumn("IsCustomerOrder", ~ (col("LineNumber").cast(IntegerType()) == lit(0)).cast(BooleanType()))
