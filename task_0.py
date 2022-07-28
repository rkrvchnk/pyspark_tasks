from pyspark.sql import DataFrame, functions as F
from pyspark.sql.column import Column


def format_reported_price_col(column: Column) -> Column:
    """
    format column to client readable view
    :param column: Column
    :return: Column (e.g. '1.50' -> '1.50 $')
    """
    column = F.format_number(column, 2)
    return F.concat(F.regexp_replace(column, ',', ' '), F.lit(' $'))


def format_df(df: DataFrame) -> DataFrame:
    """
    Rename columns, format decimal rows in a given df. Substitute 'None' values with 'NOT DEFINED'
    :param df: input df
    :return: df with renamed columns, formatted 'Reported Price' values and 'NOT DEFINED' value instead of 'None'
    """
    if_null = F.col('BU Name').isNull()
    result_df = df.select(F.col('ndc11').alias('Product'), F.col('month').alias('Month'), F.col('value')\
                          .alias('Reported Price'), F.col('`bu.name`').alias('BU Name'))\
                  .withColumn('BU Name', F.when(if_null, 'NOT DEFINED').otherwise(F.col('BU Name')))\
                  .withColumn('Reported Price', format_reported_price_col(F.col('Reported Price')))
    return result_df
