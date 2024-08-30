from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers_csv = customers_csv(spark)
    df_orders_csv = orders_csv(spark)
    df_ByCustomerId_inner = ByCustomerId_inner(spark, df_customers_csv, df_orders_csv)
    df_SumAmounts = SumAmounts(spark, df_ByCustomerId_inner)
    report_csv(spark, df_SumAmounts)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Alteryx_customer_demo_raw")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Alteryx_customer_demo_raw", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
