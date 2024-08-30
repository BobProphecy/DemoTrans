from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Orders = Orders(spark)
    df_SQ_Orders = SQ_Orders(spark, df_Orders)
    df_By_Customer_ID_add_missing_column = By_Customer_ID_add_missing_column(spark, df_SQ_Orders)
    df_Customers = Customers(spark)
    df_SQ_CUSTOMER = SQ_CUSTOMER(spark, df_Customers)
    df_By_Customer_ID = By_Customer_ID(spark, df_By_Customer_ID_add_missing_column, df_SQ_CUSTOMER)
    df_Cleanup_add_missing_column = Cleanup_add_missing_column(spark, df_By_Customer_ID)
    df_Cleanup = Cleanup(spark, df_Cleanup_add_missing_column)
    df_Sum_Amounts = Sum_Amounts(spark, df_Cleanup)
    df_Customer_Orders_EXP = Customer_Orders_EXP(spark, df_Sum_Amounts)
    Customer_Orders(spark, df_Customer_Orders_EXP)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/INFA2")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/INFA2", config = Config)(pipeline)

if __name__ == "__main__":
    main()
