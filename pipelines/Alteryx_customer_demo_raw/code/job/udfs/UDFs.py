from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)


def alteryxOutputColumns(all_columns, columns_to_drop):
    conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
    conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
    output_columns = []

    for col_name in all_columns:
        original_column_name = col_name.replace("in0_", "").replace("in1_", "")

        if col_name in columns_to_drop:
            continue
        else:
            if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
            elif col_name.startswith("in1_"):
                output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
            elif col_name.startswith("in0_"):
                output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
            else:
                output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

    return output_columns

def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
    split_col_df = exploded_df.withColumn(
        "splitCol",
        call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
          .alias("splitCol")
    )
    final_df = split_col_df
    index = - 1
    output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

    for column in output_schema_columns:
        index += 1
        final_df = final_df.withColumn(column, col("splitCol")[index])

    final_df = final_df.drop("splitCol").drop(input_column)

    return final_df

def transposeDataFrame(df, key_columns, data_columns):
    unpivoted_cols = (
        [col(column) for column in key_columns]
        + [lit(column).alias("Name") for column in data_columns]
        + [col(column).alias("Value") for column in data_columns]
    )

    return df.select(*unpivoted_cols)

def registerUDFs(spark: SparkSession):
    try:
        from prophecy.utils import ScalaUtil
        ScalaUtil.initializeUDFs(spark)
    except :
        pass
