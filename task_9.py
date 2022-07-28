import findspark

findspark.init()

from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


# spark session
# spark = SparkSession.builder.appName('Task7').getOrCreate()
#
# schema = StructType([
#     StructField('ndc11', StringType()),
#     StructField('date', DateType()),
#     StructField('value', FloatType()),
#     StructField('customer', StringType()),
# ])
#
# data = [
#         (1, datetime(2018, 1, 21), 10.0, 'Ann'),
#         (1, datetime(2018, 1, 2), 2.0, 'Paul'),
#         (1, datetime(2018, 1, 25), 30.0, 'John'),
#         (1, datetime(2018, 1, 12), 29.0, 'Paul'),
#         (2, datetime(2018, 1, 4), 10.0, 'Paul'),
#         (2, datetime(2018, 1, 28), 10.0, 'Ann'),
#         (1, datetime(2018, 2, 28), 10.0, 'Ann'),
#         (1, datetime(2018, 2, 12), 2.0, 'Paul'),
#         (2, datetime(2018, 2, 10), 10.0, 'Martin'),
#         (2, datetime(2018, 2, 3), 11.0, 'John'),
#         ]
#
# df = spark.createDataFrame(data, schema=schema)


def get_sum_customer_by_month_and_product(df: DataFrame, window: Window) -> DataFrame:
    """
    add column 'sum' with sum values of customer by month and product
    :param df:
    :param window:
    :return: DataFrame
    """
    # w = Window.partitionBy(F.month('date'), 'ndc11', 'customer')
    return df.withColumn('sum', F.sum('value').over(window))


def get_best_customer_by_month(df: DataFrame, window: Window) -> DataFrame:
    """
    get best customer for product by month window
    :param df:
    :param window:
    :return: DataFrame
    """
    assert 'sum' in df.columns
    # Window.partitionBy(F.month('date'), 'ndc11').orderBy(F.col('sum').desc(), F.col('customer'))
    df = df.withColumn('best_customer', F.first(F.col('customer')).over(window)).drop('sum')
    return df.sort(F.month('date'), 'ndc11')


def get_periods_by_year_and_month(df: DataFrame, window: Window) -> DataFrame:
    """
    we need rank period by year and month to get prev best customer
    :param df:
    :param window:
    :return: DataFrame
    """
    # Window.orderBy(F.year('date'), F.month('date'))
    df = df.withColumn('period', F.dense_rank().over(window))
    return df.sort(F.month('date'), 'ndc11')


def get_best_customer_from_prev_month(df: DataFrame, window: Window) -> DataFrame:
    """
    getting best customer from prev period/month
    :param df:
    :param window:
    :return: DataFrame
    """
    # Window().partitionBy('ndc11').orderBy('period').rangeBetween(-1, 0)
    df = df.withColumn('best_prior_customer', F.when(F.first('period').over(window) != F.col('period'),
                                                     F.first('best_customer').over(window))).drop('period')
    return df.sort(F.month('date'), 'ndc11')
