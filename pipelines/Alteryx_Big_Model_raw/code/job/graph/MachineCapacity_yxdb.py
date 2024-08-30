from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MachineCapacity_yxdb(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_MachineCapacity_yxdb}")\
        .option("user", f"{Config.username_MachineCapacity_yxdb}")\
        .option("password", f"{Config.password_MachineCapacity_yxdb}")\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .mode("read")\
        .save()
