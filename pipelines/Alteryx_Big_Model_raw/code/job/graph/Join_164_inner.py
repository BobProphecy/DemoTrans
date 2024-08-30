from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_164_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.FGPartId") == col("in1.PartId")), "inner")\
        .select(col("in0.`Machine Group ID`").alias("Machine Group ID"), col("in0.`Machine Number`").alias("Machine Number"), col("in1.PartNumber").alias("Finished Good Part"), col("in0.`Tooling Set`").alias("Tooling Set"), col("in0.`Machine Tooling Set Code`").alias("Machine Tooling Set Code"))
