from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_bisql_Query_sele_232(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_bisql_Query_sele_232}")\
        .option("user", f"{Config.username_aka_bisql_Query_sele_232}")\
        .option("password", f"{Config.password_aka_bisql_Query_sele_232}")\
        .option(
          "query",
          """select DW.Common.Date.FullDate,
\tDW.Common.Date.IsLastDayOfMonth,
\tDW.Common.Date.IsLastDayofQuarter 
from DW.Common.Date 
where DW.Common.Date.FullDate > DateAdd(YEAR, -2, GetDate())"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
