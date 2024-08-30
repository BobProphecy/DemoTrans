from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_r2s_prod_edw_alt_298(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_r2s_prod_edw_alt_298}")\
        .option("user", f"{Config.username_DSN_r2s_prod_edw_alt_298}")\
        .option("password", f"{Config.password_DSN_r2s_prod_edw_alt_298}")\
        .option(
          "query",
          """select p.part_id,
\tp.material_id,
\tp.part_number,
\tp.like_part_number,
\tp.reporting_series 
from common.part p"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
