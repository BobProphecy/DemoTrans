from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def PartSeriesPlusOption(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_PartSeriesPlusOption}")\
        .option("user", f"{Config.username_PartSeriesPlusOption}")\
        .option("password", f"{Config.password_PartSeriesPlusOption}")\
        .option("query", "X:\\ADW\\Common\\Part Series Plus Option.yxdb")\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
