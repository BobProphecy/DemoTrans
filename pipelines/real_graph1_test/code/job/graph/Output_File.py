from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Output_File(spark: SparkSession, inDF: DataFrame):
    inDF.write\
        .option("header", False)\
        .option("sep", ",")\
        .mode("error")\
        .option("separator", ",")\
        .option("header", False)\
        .csv(Config.OP_AGG_RPT_FILE)
