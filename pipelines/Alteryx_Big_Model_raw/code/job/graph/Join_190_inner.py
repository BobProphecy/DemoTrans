from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_190_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`Machine Number`") == col("in1.`Machine Number`")), "inner")\
        .select(col("in0.Plant").alias("Plant"), col("in0.`Machine Group ID`").alias("Machine Group ID"), col("in0.`Machine Number`").alias("Machine Number"), col("in1.`Tooling Set`").alias("Tooling Set"), col("in1.`Machine Tooling Set Code`").alias("Machine Tooling Set Code"))
