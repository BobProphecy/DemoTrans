from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Input_File = Input_File(spark)
    df_Sort = Sort(spark, df_Input_File)
    df_Input_File_1 = Input_File_1(spark)
    df_Sort_1 = Sort_1(spark, df_Input_File_1)
    df_Join = Join(spark, df_Sort, df_Sort_1)
    df_Rollup = Rollup(spark, df_Join)
    df_Rollup_Reformat = Rollup_Reformat(spark, df_Rollup)
    df_Reformat = Reformat(spark, df_Rollup_Reformat)
    df_Sort_2 = Sort_2(spark, df_Reformat)
    Output_File(spark, df_Sort_2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/real_graph1_test")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/real_graph1_test", config = Config)(pipeline)

if __name__ == "__main__":
    main()
