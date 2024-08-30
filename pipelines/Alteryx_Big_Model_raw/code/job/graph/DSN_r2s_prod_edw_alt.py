from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_r2s_prod_edw_alt(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_r2s_prod_edw_alt}")\
        .option("user", f"{Config.username_DSN_r2s_prod_edw_alt}")\
        .option("password", f"{Config.password_DSN_r2s_prod_edw_alt}")\
        .option(
          "query",
          """select wc.work_center_id,
\twc.work_center_number,
\twc.work_center,
\twc.plant,
\twc.facility,
\twc.review_board,
\twc.country 
from manufacturing.work_center wc"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
