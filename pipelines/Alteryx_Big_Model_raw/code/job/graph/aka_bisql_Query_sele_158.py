from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_bisql_Query_sele_158(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_bisql_Query_sele_158}")\
        .option("user", f"{Config.username_aka_bisql_Query_sele_158}")\
        .option("password", f"{Config.password_aka_bisql_Query_sele_158}")\
        .option(
          "query",
          """select DW.Analysis.vw_MDM_Operations_VERSION_1_MachineGroupPartGroup_Leaf.[Machine Group ID],
\tDW.Analysis.vw_MDM_Operations_VERSION_1_MachineGroupPartGroup_Leaf.Regex 
from DW.Analysis.vw_MDM_Operations_VERSION_1_MachineGroupPartGroup_Leaf"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
