from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_bisql_Query_sele_154(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_bisql_Query_sele_154}")\
        .option("user", f"{Config.username_aka_bisql_Query_sele_154}")\
        .option("password", f"{Config.password_aka_bisql_Query_sele_154}")\
        .option(
          "query",
          """select DW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group Process_Leaf].Name,
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group Process_Leaf].Code,
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group Process_Leaf].Process,
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group Process_Leaf].Plant,
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group Process_Leaf].[Machine Group ID] 
from DW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group Process_Leaf]"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
