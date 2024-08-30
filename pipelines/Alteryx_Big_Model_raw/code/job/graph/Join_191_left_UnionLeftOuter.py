from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_191_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.TSETPartNumber") == col("in1.`Tooling Set`")), "leftouter")\
        .select(col("in1.Plant").alias("Plant"), col("in0.TSETPartNumber").alias("TSETPartNumber"), col("in0.FGPartId").alias("FGPartId"), col("in1.`Machine Group ID`").alias("Machine Group ID"), col("in0.TSETBillOfMaterialId").alias("TSETBillOfMaterialId"), col("in1.`Machine Number`").alias("Machine Number"), col("in0.TSETPartId").alias("TSETPartId"), col("in0.FGBillofMaterialId").alias("FGBillofMaterialId"), col("in0.TKITPartNumber").alias("TKITPartNumber"), col("in1.`Tooling Set`").alias("Tooling Set"), col("in1.`Machine Tooling Set Code`").alias("Machine Tooling Set Code"), col("in0.TKITPartId").alias("TKITPartId"), col("in0.TSETComponentId").alias("TSETComponentId"))
