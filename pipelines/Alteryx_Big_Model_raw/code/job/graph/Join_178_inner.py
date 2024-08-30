from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_178_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.BillOfMaterialId") == col("in1.BillOfMaterialId")), "inner")\
        .select(col("in0.BillOfMaterialId").alias("FGBillofMaterialId"), col("in0.PartId").alias("FGPartId"), col("in1.ComponentId").alias("TSETComponentId"), col("in1.PartId").alias("TSETPartId"))
