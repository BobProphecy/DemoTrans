from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ManufacturingOrder_y(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_ManufacturingOrder_y}")\
        .option("user", f"{Config.username_ManufacturingOrder_y}")\
        .option("password", f"{Config.password_ManufacturingOrder_y}")\
        .option("query", "X:\\source\\shop floor control\\Manufacturing Order.yxdb")\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
