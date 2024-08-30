from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_r2s_prod_edw_alt_301(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_r2s_prod_edw_alt_301}")\
        .option("user", f"{Config.username_DSN_r2s_prod_edw_alt_301}")\
        .option("password", f"{Config.password_DSN_r2s_prod_edw_alt_301}")\
        .option(
          "query",
          """select mo.date_entered,
\ta.address_name as deliver_to,
\tmo.sales_order_type,
\tmo.is_scarlet,
\tmo.manufacturing_due_date,
\tmo.manufacturing_on_time_status,
\tmo.manufacturing_order_id,
\tmo.reschedule_description,
\tmo.reschedule_reason,
\tmo.shipping_on_time_status 
from advanced_analytics.manufacturing.manufacturing_order mo 
\tleft outer join relational_raw.prodsql_production.dbo__location l on l.location_id = mo.ship_to_id 
\tleft outer join relational_raw.prodsql_production.dbo__address a on a.address_id = l.address_id"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
