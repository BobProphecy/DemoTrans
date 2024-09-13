from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from alteryx_customer_demo_fixed.config.ConfigStore import *
from alteryx_customer_demo_fixed.udfs.UDFs import *

def SumAmounts(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("customer_id"))

    return df1.agg(
        sum(col("amount")).alias("Sum_amount"), 
        count(col("order_id")).alias("Count"), 
        last(col("account_open_date")).alias("Last_account_open_date")
    )
