from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ProcessOrderTransact(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_ProcessOrderTransact}")\
        .option("user", f"{Config.username_ProcessOrderTransact}")\
        .option("password", f"{Config.password_ProcessOrderTransact}")\
        .option("query", "X:\\source\\shop floor control\\Process Order Transaction.yxdb")\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
