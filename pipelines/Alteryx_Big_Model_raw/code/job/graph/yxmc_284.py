from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def yxmc_284(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def clean_columns(df, columns_to_clean):
        cleaned_df = df

        for column in columns_to_clean:
            column_type = df.schema[column].dataType

            if column_type == "string":
                cleaned_df = cleaned_df.withColumn(column, when(isnull(col(column)), "").otherwise(col(column)))
            elif column_type == "integer":
                cleaned_df = cleaned_df.withColumn(column, when(isnull(col(column)), 0).otherwise(col(column)))

        return cleaned_df

    def cleanse_data_frame(df: DataFrame) -> DataFrame:
        cleansed_df = df

        # Iterate over each column
        for col_name in df.columns:
            transformed_col = col(col_name)
            # Apply cleansing operations based on the boolean flags
            transformed_col = trim(transformed_col)
            # Replace the original column with the transformed column
            cleansed_df = cleansed_df.withColumn(col_name, transformed_col)

        return cleansed_df

    out2 = in0
    out1 = clean_columns(
        out2,
        ["PartId", "PartNumber", "LikePartNumber", "ReportingSeries", "MaterialId", "Right_PartId", "Mount",
         "Direction", "Lead", "Rows", "Positions", "Plating", "Packaging", "Pad", "Alignment", "Retention",
         "Key", "Latching", "Entry", "LeadFree", "Polarized", "Ends", "ReverseWire", "DaisyChain",
         "ReverseConnector", "MiddleReverse", "OutsideReverse", "ReverseNotch", "ScrewRetention", "Length",
         "CableOptions", "Shield", "SpaceOption", "SurfaceMount", "Engines", "TransmittingChannels",
         "ReceivingChannels", "DeadChannels", "Speed", "MechanicalSample", "OpticsType"]
    )
    out0 = cleanse_data_frame(out1)

    return out0
