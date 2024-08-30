from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_242_left(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`Finished Good Part`") == col("in1.`Part Number`")), "leftanti")\
        .select(col("in0.`Finished Good Part`").alias("Finished Good Part"), col("in0.`Tooling Set`").alias("Tooling Set"))
