from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_miscsql_Query_se(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_miscsql_Query_se}")\
        .option("user", f"{Config.username_aka_miscsql_Query_se}")\
        .option("password", f"{Config.password_aka_miscsql_Query_se}")\
        .option(
          "query",
          """select operations.components.ComponentId,
\toperations.components.BillOfMaterialId,
\toperations.components.PartId 
from operations.components"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
