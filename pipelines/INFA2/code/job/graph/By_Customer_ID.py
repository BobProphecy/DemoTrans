from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def By_Customer_ID(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.customer_id") == col("in0.customer_id")), "inner")\
        .select(col("in0.REFERRAL_CONSUMER_COUNT").alias("REFERRAL_CONSUMER_COUNT"), col("in1.first_name").alias("first_name"), col("in0.order_id").alias("order_id"), col("in0.ENROLLMENT_SOURCE_ID_CAS").alias("ENROLLMENT_SOURCE_ID_CAS"), col("in0.customer_id").alias("customer_id"), col("in0.CC_LOYALTY_PRMRY_CONTACT_FLAG").alias("CC_LOYALTY_PRMRY_CONTACT_FLAG"), col("in0.ENROLLMENT_SOURCE_ID_CC").alias("ENROLLMENT_SOURCE_ID_CC"), col("in0.amount").alias("amount"), col("in0.CONSUMER_PROFILE_KEY2").alias("CONSUMER_PROFILE_KEY2"), col("in1.account_open_date").alias("account_open_date"), col("in1.last_name").alias("last_name"), col("in0.order_status").alias("order_status"), col("in0.order_date").alias("order_date"), col("in0.CONSUMER_ID").alias("CONSUMER_ID"), col("in0.CAS_LOYALTY_PRMRY_CONTACT_FLAG").alias("CAS_LOYALTY_PRMRY_CONTACT_FLAG"), col("in1.phone").alias("phone"))
