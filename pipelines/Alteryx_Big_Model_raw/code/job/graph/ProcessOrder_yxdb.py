from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ProcessOrder_yxdb(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_ProcessOrder_yxdb}")\
        .option("user", f"{Config.username_ProcessOrder_yxdb}")\
        .option("password", f"{Config.password_ProcessOrder_yxdb}")\
        .option("query", "X:\\source\\shop floor control\\Process Order.yxdb")\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
