from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_173_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.TSETPartId") == col("in1.PartId")), "inner")\
        .select(col("in0.TSETPartNumber").alias("TSETPartNumber"), col("in0.FGPartId").alias("FGPartId"), col("in1.BillOfMaterialId").alias("TSETBillOfMaterialId"), col("in0.TSETPartId").alias("TSETPartId"), col("in0.FGBillofMaterialId").alias("FGBillofMaterialId"), col("in0.TSETComponentId").alias("TSETComponentId"))
