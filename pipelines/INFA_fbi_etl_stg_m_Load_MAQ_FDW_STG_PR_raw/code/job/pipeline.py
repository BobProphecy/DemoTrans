from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Shortcut_to_stage_PR = Shortcut_to_stage_PR(spark)
    df_SQ_Shortcut_to_stage_PR = SQ_Shortcut_to_stage_PR(spark, df_Shortcut_to_stage_PR)
    df_SQ_Shortcut_to_stage_PR_EXPR_7 = SQ_Shortcut_to_stage_PR_EXPR_7(spark, df_SQ_Shortcut_to_stage_PR)
    df_exp_Load_STG_MAQ_PR_VARS = exp_Load_STG_MAQ_PR_VARS(spark, df_SQ_Shortcut_to_stage_PR_EXPR_7)
    df_exp_Load_STG_MAQ_PR = exp_Load_STG_MAQ_PR(spark, df_exp_Load_STG_MAQ_PR_VARS)
    df_exp_Load_STG_MAQ_PR_EXPR_6 = exp_Load_STG_MAQ_PR_EXPR_6(spark, df_exp_Load_STG_MAQ_PR)
    df_T_FDW_STG_MAQ_PR_EXP = T_FDW_STG_MAQ_PR_EXP(spark, df_exp_Load_STG_MAQ_PR_EXPR_6)
    T_FDW_STG_MAQ_PR(spark, df_T_FDW_STG_MAQ_PR_EXP)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/INFA_fbi_etl_stg_m_Load_MAQ_FDW_STG_PR_raw")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/INFA_fbi_etl_stg_m_Load_MAQ_FDW_STG_PR_raw",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
