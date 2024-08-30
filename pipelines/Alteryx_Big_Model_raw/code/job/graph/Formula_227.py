from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_227(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("QTYProcessedPO", when(col("QTYProcessedPO").isNull(), lit(0)).otherwise(col("QTYProcessedPO")))\
        .withColumn("TotalQuantity", when(col("TotalQuantity").isNull(), lit(0)).otherwise(col("TotalQuantity")))\
        .withColumn("ScrapQuantity", when(col("ScrapQuantity").isNull(), lit(0)).otherwise(col("ScrapQuantity")))\
        .withColumn("QTYProcessedWC", when(col("QTYProcessedWC").isNull(), lit(0)).otherwise(col("QTYProcessedWC")))\
        .withColumn("QTYProcessedTotal", when(col("QTYProcessedTotal").isNull(), lit(0)).otherwise(col("QTYProcessedTotal")))
