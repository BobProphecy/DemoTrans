from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_r2s_prod_edw_alt_297(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_r2s_prod_edw_alt_297}")\
        .option("user", f"{Config.username_DSN_r2s_prod_edw_alt_297}")\
        .option("password", f"{Config.password_DSN_r2s_prod_edw_alt_297}")\
        .option(
          "query",
          """select distinct manufacturing.work_center_group.work_center_id 
from manufacturing.work_center_group 
where manufacturing.work_center_group.group_id in (102, 117)"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
