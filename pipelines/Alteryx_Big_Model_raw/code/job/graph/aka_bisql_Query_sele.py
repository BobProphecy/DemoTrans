from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_bisql_Query_sele(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_bisql_Query_sele}")\
        .option("user", f"{Config.username_aka_bisql_Query_sele}")\
        .option("password", f"{Config.password_aka_bisql_Query_sele}")\
        .option(
          "query",
          """select BIMDS.mdm.MachineToolingSet.Code,
\tBIMDS.mdm.MachineToolingSet.[Machine No],
\tBIMDS.mdm.MachineToolingSet.[Tooling Set] 
from BIMDS.mdm.MachineToolingSet"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
