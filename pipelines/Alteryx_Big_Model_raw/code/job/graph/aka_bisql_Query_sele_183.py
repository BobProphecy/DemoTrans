from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_bisql_Query_sele_183(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_bisql_Query_sele_183}")\
        .option("user", f"{Config.username_aka_bisql_Query_sele_183}")\
        .option("password", f"{Config.password_aka_bisql_Query_sele_183}")\
        .option(
          "query",
          """select DW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Machine Group ID],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].Name,
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].Code,
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Machine Type],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].Plant,
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].Pipeline,
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Target Utilization],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Projected Annual Growth],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Days per Month],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Days per Quarter],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Daily Production Hours],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Average Hourly Output],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Automation Level_Name],
\tDW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf].[Machine Group Notes] 
from DW.Analysis.[vw_MDM_Operations_VERSION_1_Machine Group_Leaf]"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
