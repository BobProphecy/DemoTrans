from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def exp_Load_STG_MAQ_DIA_add_missing_column(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("i_CUBE_NM", lit(None).cast(StringType()))
