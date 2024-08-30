from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_miscsql_Query_se_168(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_miscsql_Query_se_168}")\
        .option("user", f"{Config.username_aka_miscsql_Query_se_168}")\
        .option("password", f"{Config.password_aka_miscsql_Query_se_168}")\
        .option(
          "query",
          """select operations.billofmaterial.BillOfMaterialId,
\toperations.billofmaterial.PartId,
\toperations.billofmaterial.Code 
from operations.billofmaterial"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
