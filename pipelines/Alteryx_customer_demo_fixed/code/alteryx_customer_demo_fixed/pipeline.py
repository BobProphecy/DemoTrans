from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from alteryx_customer_demo_fixed.config.ConfigStore import *
from alteryx_customer_demo_fixed.udfs.UDFs import *
from prophecy.utils import *
from alteryx_customer_demo_fixed.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers_fixed = customers_fixed(spark)
    df_orders_fixed = orders_fixed(spark)
    df_ByCustomerId_inner = ByCustomerId_inner(spark, df_customers_fixed, df_orders_fixed)
    df_SumAmounts = SumAmounts(spark, df_ByCustomerId_inner)
    report_csv_fixed(spark, df_SumAmounts)
    report_csv(spark)
    df_customers_csv = customers_csv(spark)
    df_orders_csv = orders_csv(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Alteryx_customer_demo_fixed")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Alteryx_customer_demo_fixed", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
