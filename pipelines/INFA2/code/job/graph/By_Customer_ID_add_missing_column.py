from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def By_Customer_ID_add_missing_column(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("ENROLLMENT_SOURCE_ID_CC", lit(None).cast(StringType()))\
        .withColumn("CC_LOYALTY_PRMRY_CONTACT_FLAG", lit(None).cast(StringType()))\
        .withColumn("ENROLLMENT_SOURCE_ID_CAS", lit(None).cast(StringType()))\
        .withColumn("CAS_LOYALTY_PRMRY_CONTACT_FLAG", lit(None).cast(StringType()))\
        .withColumn("CONSUMER_ID", lit(None).cast(StringType()))\
        .withColumn("REFERRAL_CONSUMER_COUNT", lit(None).cast(StringType()))\
        .withColumn("CONSUMER_PROFILE_KEY2", lit(None).cast(StringType()))
