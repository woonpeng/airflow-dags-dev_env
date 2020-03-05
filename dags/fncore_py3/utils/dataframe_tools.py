"""
Tools for manipulation of spark dataframes
"""

from pyspark.sql import HiveContext
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import ArrayType, StringType, StructType


def add_ticks(s):
    """Add leading and ending backticks to String"""
    if s[0] == s[-1] == "`":
        return s
    else:
        return "`" + s + "`"


def strip_ticks(s):
    """Strip string of leading and ending backticks"""
    if s is not None:
        if s[0] == s[-1] == "`":
            return s[1:-1]
        else:
            return s
    else:
        return None


def prepend(items):
    """
    Returns a UDF that prepend items to an iterable

    :param items: list of items to prepend to iterable
    :type items: iterable
    """
    return udf(lambda iterable: [item for item in items] + iterable, ArrayType(StringType()))


# pylint: disable=unnecessary-lambda
def array_to_str(delimiter):
    """
    Returns a UDF that converts an iterable to a string

    :param delimiter: delimiter to use to separate the items in the iterable in
                      string representation
    :type delimiter: str
    """
    return udf(lambda iterable: delimiter.join(iterable), StringType())


def union_all_by_name(spark_context, iterable):
    """
    Union all the Spark data frames in an iterable by their column names
    Any missing columns will be filled with all nulls

    :param iterable: an iterable of spark data frames
    :type iterable: an iterable of spark data frames
    """

    # TODO: Check for column name clash - i.e. same column name but different types
    # Should do it in the graph spec stage?
    # If we store in hive can do a name-type pair check quite easily

    sql_context = HiveContext(spark_context)

    # Union of schema, create empty init DF
    schema_acc = StructType([])
    df_acc = sql_context.createDataFrame([], schema_acc)

    # Create null cols of df does not contain column
    def gen_select_params(schema, df):
        return [
            col(add_ticks(c)) if c in df.columns
            else lit(None).alias(strip_ticks(c))
            for c in schema.fieldNames()
        ]

    for df in iterable:
        schema_acc = StructType(
            list(set(schema_acc.fields + df.schema.fields))
        )
        df1 = df_acc.select(*gen_select_params(schema_acc, df_acc))
        df2 = df.select(*gen_select_params(schema_acc, df))
        df_acc = df1.union(df2)

    return df_acc


def to_pandas_iterator(dataframe, max_result_size=1e9, factor=1.5):
    """
    Returns an iterable of Pandas dataframe from Spark dataframe

    :param dataframe: a spark dataframe
    :type dataframe: pyspark.sql.DataFrame
    :param max_result_size: the maximum result size (in bytes) that spark driver accept
    :type max_result_size: int
    :param factor: the safety factor in estimating how much should a batch size be.
                   A factor of 1.0 implies that the number of rows in a batch that can
                   fit into the maximnum result size will be based on the estimate of
                   the size of a single row, while a factor greater than 1.0, e.g. 1.2
                   means the estimate will be based on the assumption that the actual
                   size of a row is 1.2 times of the estimated size
    :type factor: float
    """

    # Get the number of rows in the dataframe
    num_rows = dataframe.count()

    # Get the size of the first row
    row = dataframe.limit(1).toPandas()
    row_size = len(row.to_csv(index=False, encoding='utf-8', header=False))

    # Get the size of the header
    header_size = len(row.to_csv(index=False, encoding='utf-8', header=True)) - row_size

    # Take into account of the safety factor
    header_size = header_size * factor
    row_size = row_size * factor

    # Compute the number of rows that will fit within the maximum result size
    num_rows_per_batch = int((max_result_size - header_size) / row_size)

    # Compute the number of batches
    num_batch = int(num_rows / num_rows_per_batch) + 1

    # Split the dataframe into the calculated number of batches
    df_list = dataframe.randomSplit([1.0 for _ in range(num_batch)])

    # Iterate through the splitted dataframes
    for cur_dataframe in df_list:
        yield cur_dataframe.toPandas()
