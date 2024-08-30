from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_r2s_prod_aa_alte(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_r2s_prod_aa_alte}")\
        .option("user", f"{Config.username_DSN_r2s_prod_aa_alte}")\
        .option("password", f"{Config.password_DSN_r2s_prod_aa_alte}")\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .mode("read")\
        .save()
