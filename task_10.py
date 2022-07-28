import findspark

findspark.init()

from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from decimal import Decimal
from typing import Union

# spark session
spark = SparkSession.builder.appName('Task7').getOrCreate()

schema = StructType([
    StructField('ndc11', StringType()),
    StructField('ndc9', StringType()),
    StructField('name', StringType()),
    StructField('quarter', StringType()),
    StructField('is_line_extension', StringType()),
    StructField('value', DecimalType(38, 2))
])

data = [
    (11, 1, 'Product 11', '2017q4', 'Y', Decimal(100)),
    (12, 1, 'Product 12', '2018q2', 'N', None),
    (13, 1, 'Product 13', '2018q2', 'Y', Decimal(100)),
    (14, 1, 'Product 14', '2018q2', 'Y', Decimal(200)),
    (14, 1, 'Product 14', '2018q3', 'Y', Decimal(125)),
    (21, 2, 'Product 21', '2018q2', 'N', Decimal(130)),
    (22, 2, 'Product 22', '2018q2', 'Y', Decimal(120)),
    (23, 2, 'Product 23', '2018q3', 'Y', Decimal(170)),
    (31, 3, 'Product 31', '2018q3', 'Y', Decimal(150)),
    (32, 3, 'Product 32', '2018q3', 'Y', Decimal(150)),
    (33, 3, 'Product 33', '2018q2', 'Y', Decimal(110))
]

extension_products_df = spark.createDataFrame(data, schema=schema)

domain_schema = StructType([
    StructField('line_extension_ndc9', StringType()),
    StructField('initial_drug_ndc9', StringType()),
    StructField('start_date', DateType()),
    StructField('end_date', DateType()),
])

domain_data = [
    (1, 2, datetime(2018, 1, 1), datetime(2018, 6, 30)),
    (1, 3, datetime(2018, 4, 1), datetime(2018, 9, 30))
]

extension_domain_df = spark.createDataFrame(domain_data, domain_schema)


def join(
        left: DataFrame,
        right: DataFrame,
        condition: bool,
        select_filter: Union[None, list, tuple] = None,
) -> DataFrame:
    """
    join dfs by condition and select after join
    :param left:
    :param right:
    :param condition:
    :param select_filter:
    :return:
    """
    if not select_filter:
        return left.join(right, on=condition, how='left')

    return left.join(right, on=condition, how='left').select(*select_filter)


def adjust_column_by_first_in_window(
        df: DataFrame,
        keys: Union[list, None, str],
        ordering: Union[list, None, str],
        new_column: str,
        new_value: Column
) -> DataFrame:
    """
    adjust column through window and condition
    :param df:
    :param keys:
    :param ordering:
    :param new_column:
    :param new_value:
    :return:
    """
    window = Window.partitionBy(keys).orderBy(ordering)
    df = df.withColumn(new_column, F.first(new_value).over(window))
    return df


# def drop_duplicates(df: DataFrame, cols: list):
#     return df.dropDuplicates(cols)
#
#
# def adjust_col_by_condition(df: DataFrame, new_col: str, condition: bool) -> DataFrame:
#     return df.withColumn(new_col, condition)


if __name__ == '__main__':
    # make condition by date, 'is_line_extension' and family group
    date_condition = ((extension_products_df['quarter'].substr(6, 6) >= F.quarter(extension_domain_df['start_date'])) &
                      (extension_products_df['quarter'].substr(6, 6) <= F.quarter(extension_domain_df['end_date'])))
    extension_condition = (extension_products_df['is_line_extension'] == 'Y')
    family_condition = (extension_products_df['ndc9'] == extension_domain_df['line_extension_ndc9'])
    cond = family_condition & extension_condition & date_condition
    # select filter tuple
    select_filter = (extension_products_df['*'], extension_domain_df['initial_drug_ndc9'])
    line_extension_prod_with_initial_family_df = join(extension_products_df, extension_domain_df, cond, select_filter)

    # TODO: Why don't i can make it with ALIAS ?
    # so i need to rename cols to be able join same Datasets
    initial_df = extension_products_df.toDF(*['1_' + col for col in extension_products_df.columns])

    # make condition by family group and quarter
    group_condition = (line_extension_prod_with_initial_family_df['initial_drug_ndc9'] == initial_df['1_ndc9'])
    quarter_condition = (line_extension_prod_with_initial_family_df['quarter'] == initial_df['1_quarter'])
    cond1 = (group_condition & quarter_condition)
    # select_filter tuple
    select_filter = (line_extension_prod_with_initial_family_df['*'], initial_df['1_name'], initial_df['1_value'])
    # get columns with products we can make extension
    df_with_extensioned_products = join(line_extension_prod_with_initial_family_df, initial_df, cond1, select_filter)

    # get max value out of extension products
    df_with_alternative_value = adjust_column_by_first_in_window(df_with_extensioned_products, ['name', 'quarter'],
                                                                 F.col('1_value').desc(),
                                                                 'alternative_value', F.col('1_value'))
    # get name of product with max value out of extension products
    df_with_initial_product_name = adjust_column_by_first_in_window(df_with_alternative_value, ['name', 'quarter'],
                                                                    F.col('1_value').desc(),
                                                                    'initial_product', F.col('1_name'))

    # drop columns we don't need anymore
    df = df_with_initial_product_name.drop('1_name', '1_value', 'initial_drug_ndc9')

    # get reported value
    df = df.withColumn('reported_value', F.when(F.col('value') > F.col('alternative_value'), F.col('value'))
                       .otherwise(F.col('alternative_value')))

    # now we can drop duplicates
    df = df.dropDuplicates(['name', 'quarter'])
    df.sort('ndc11').show()