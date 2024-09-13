from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from alteryx_customer_demo_fixed.config.ConfigStore import *
from alteryx_customer_demo_fixed.udfs.UDFs import *

def report_csv_fixed(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("sep", ",")\
        .mode("overwrite")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("dbfs:/FileStore/bobwelshmer/report_csv_fixed.csv")
