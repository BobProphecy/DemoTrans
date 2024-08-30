from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_243_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            ((col("in1.ManufacturingDueDate") == col("in0.FullDate")) & (col("in1.`Tooling Set`") == col("in0.`Tooling Set`")))
            & (col("in1.Plant") == col("in0.Plant"))
          ),
          "leftanti"
        )\
        .select(col("in0.IsLastDayOfMonth").alias("IsLastDayOfMonth"), col("in0.FullDate").alias("FullDate"), col("in0.Plant").alias("Plant"), col("in0.IsLastDayofQuarter").alias("IsLastDayofQuarter"), col("in0.`Tooling Set`").alias("Tooling Set"))
