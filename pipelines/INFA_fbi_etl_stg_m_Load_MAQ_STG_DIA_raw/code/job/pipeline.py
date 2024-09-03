from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Shortcut_to_stage_DIA = Shortcut_to_stage_DIA(spark)
    df_SQ_Shortcut_to_stage_DIA = SQ_Shortcut_to_stage_DIA(spark, df_Shortcut_to_stage_DIA)
    df_SQ_Shortcut_to_stage_DIA_EXPR_7 = SQ_Shortcut_to_stage_DIA_EXPR_7(spark, df_SQ_Shortcut_to_stage_DIA)
    df_exp_Load_STG_MAQ_DIA_VARS = exp_Load_STG_MAQ_DIA_VARS(spark, df_SQ_Shortcut_to_stage_DIA_EXPR_7)
    df_exp_Load_STG_MAQ_DIA_add_missing_column = exp_Load_STG_MAQ_DIA_add_missing_column(
        spark, 
        df_exp_Load_STG_MAQ_DIA_VARS
    )
    df_exp_Load_STG_MAQ_DIA = exp_Load_STG_MAQ_DIA(spark, df_exp_Load_STG_MAQ_DIA_add_missing_column)
    df_exp_Load_STG_MAQ_DIA_EXPR_6 = exp_Load_STG_MAQ_DIA_EXPR_6(spark, df_exp_Load_STG_MAQ_DIA)
    df_Shortcut_to_T_FBI_STG_MAQ_DIA_EXP = Shortcut_to_T_FBI_STG_MAQ_DIA_EXP(spark, df_exp_Load_STG_MAQ_DIA_EXPR_6)
    Shortcut_to_T_FBI_STG_MAQ_DIA(spark, df_Shortcut_to_T_FBI_STG_MAQ_DIA_EXP)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/INFA_fbi_etl_stg_m_Load_MAQ_STG_DIA_raw")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/INFA_fbi_etl_stg_m_Load_MAQ_STG_DIA_raw",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
