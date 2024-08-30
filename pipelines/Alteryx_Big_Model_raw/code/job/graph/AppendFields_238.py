from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AppendFields_238(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (lit(1) == lit(1).cast(IntegerType())), "inner")\
        .select(col("in1.Plant").alias("Plant"), col("in0.IsLastDayOfMonth").alias("IsLastDayOfMonth"), col("in0.FullDate").alias("FullDate"), col("in0.IsLastDayofQuarter").alias("IsLastDayofQuarter"), col("in0.`Tooling Set`").alias("Tooling Set"))
