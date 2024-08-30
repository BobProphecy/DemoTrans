from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def salessampleorder(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("IsSalesOrder", (col("sales_order_type").cast(StringType()) == lit("Sales")))\
        .withColumn("IsSampleOrder", (col("sales_order_type").cast(StringType()) == lit("Sample")))
