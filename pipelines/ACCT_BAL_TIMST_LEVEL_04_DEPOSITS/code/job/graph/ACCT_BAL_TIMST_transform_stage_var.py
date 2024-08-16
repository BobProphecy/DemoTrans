from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ACCT_BAL_TIMST_transform_stage_var(spark: SparkSession, in0: DataFrame) -> DataFrame:
    pass
