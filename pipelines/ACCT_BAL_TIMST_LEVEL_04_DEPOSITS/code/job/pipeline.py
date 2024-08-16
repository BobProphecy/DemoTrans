from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_AcctBalTimstDpstDailyHash_Lkp = AcctBalTimstDpstDailyHash_Lkp(spark)
    df_DepositTIMSTNaturalHash_REF = DepositTIMSTNaturalHash_REF(spark)
    df_DepositTIMSTHash_REF = DepositTIMSTHash_REF(spark)
    df_ABalCstmtDPSTHash = ABalCstmtDPSTHash(spark)
    df_ABalEstmtDPSTHash = ABalEstmtDPSTHash(spark)
    df_ABalNULLstmtDPSTHash = ABalNULLstmtDPSTHash(spark)
    df_TIMST = TIMST(spark)
    df_AcctBal_Timst_INTRATE_Dpst_Hash_REF = AcctBal_Timst_INTRATE_Dpst_Hash_REF(spark)
    df_ABalTstmtDPSTHash = ABalTstmtDPSTHash(spark)
    df_ABalBstmtDPSTHash = ABalBstmtDPSTHash(spark)
    df_ECF_STATUSCD_IN2_Hash = ECF_STATUSCD_IN2_Hash(spark)
    df_ABalPstmtDPSTHash = ABalPstmtDPSTHash(spark)
    df_ABalNstmtDPSTHash = ABalNstmtDPSTHash(spark)
    df_ACCT_BAL_TIMST_transform_stage_var = ACCT_BAL_TIMST_transform_stage_var(spark)
    df_ACCT_BAL_TIMST_transform_V0S111P16_reformat _1 = ACCT_BAL_TIMST_transform_V0S111P16_reformat _1(
        spark, 
        df_ACCT_BAL_TIMST_transform_stage_var
    )
    df_OdateHash_REF = OdateHash_REF(spark)
    df_Acct_bal_join = Acct_bal_join(
        spark, 
        df_ACCT_BAL_TIMST_transform_V0S111P16_reformat _1, 
        df_DepositTIMSTHash_REF, 
        df_OdateHash_REF
    )
    df_Acct_bal_transform_stage_var = Acct_bal_transform_stage_var(spark, df_Acct_bal_join)
    df_Acct_bal_transform_V65S3P3_reformat _1 = Acct_bal_transform_V65S3P3_reformat _1(
        spark, 
        df_Acct_bal_transform_stage_var
    )
    df_Acct_bal_Final_join = Acct_bal_Final_join(
        spark, 
        df_Acct_bal_transform_V65S3P3_reformat _1, 
        df_AcctBalTimstDpstDailyHash_Lkp
    )
    df_Acct_bal_transform_V65S3P13_constraint_3 = Acct_bal_transform_V65S3P13_constraint_3(
        spark, 
        df_Acct_bal_transform_stage_var
    )
    df_Acct_bal_Final_transform_V65S0P3_reformat _1 = Acct_bal_Final_transform_V65S0P3_reformat _1(
        spark, 
        df_Acct_bal_Final_join
    )
    AcctBal_Timst_Dpst_Hash(spark, df_Acct_bal_Final_transform_V65S0P3_reformat _1)
    df_Acct_bal_transform_V65S3P12_constraint_2 = Acct_bal_transform_V65S3P12_constraint_2(
        spark, 
        df_Acct_bal_transform_stage_var
    )
    df_Acct_bal_transform_V65S3P12_reformat _2 = Acct_bal_transform_V65S3P12_reformat _2(
        spark, 
        df_Acct_bal_transform_V65S3P12_constraint_2
    )
    AcctBal_Timst_INTRATE_Dpst_Hash(spark, df_Acct_bal_transform_V65S3P12_reformat _2)
    Timst_Daily_stg(spark, df_Acct_bal_Final_transform_V65S0P3_reformat _1)
    df_Acct_bal_transform_V65S3P13_reformat _3 = Acct_bal_transform_V65S3P13_reformat _3(
        spark, 
        df_Acct_bal_transform_V65S3P13_constraint_3
    )
    DepositTIMSTHash(spark, df_Acct_bal_transform_V65S3P13_reformat _3)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/ACCT_BAL_TIMST_LEVEL_04_DEPOSITS")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/ACCT_BAL_TIMST_LEVEL_04_DEPOSITS", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
