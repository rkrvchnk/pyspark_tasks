import findspark

findspark.init()

from pyspark.sql.types import FloatType
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from typing import Union
from pyspark.sql.window import Window


# # spark session
# spark = SparkSession.builder.appName('Task7').getOrCreate()
#
# schema = StructType([
#     StructField('ndc11', StringType()),
#     StructField('period', StringType()),
#     StructField('value', FloatType()),
# ])
#
# data = [(1, 1, 30.0),
#         (1, 2, -200.0),
#         (1, 3, -100.0),
#         (1, 4, 100.0),
#         (2, 1, -100.0),
#         (2, 2, 40.0),
#         (2, 3, 50.0),
#         (2, 4, 0.0)]
#
# df = spark.createDataFrame(data, schema=schema)


def get_pos_value_from_prev_period(df: DataFrame, window: Window) -> DataFrame:
    """
        getting df with prev pos value in added column 'new_value' through window
        :param df:
        :param window:
        :return:
        """
    # Window.partitionBy('ndc11').orderBy('period').rowsBetween(Window.unboundedPreceding, -1)
    df = df.withColumn('new_value', F.when(F.col('value') > 0, F.col('value')))
    df = df.withColumn('new_value', F.when(F.col('new_value').isNull(), F.last('new_value', True).over(window)).
                       otherwise(F.col('new_value')))
    df = df.withColumn('new_value', F.when(F.col('new_value').isNull(), 0).otherwise(F.col('new_value')))
    return df


def get_origin_value(df: DataFrame) -> DataFrame:
    """
    get df with column 'origin_value' for changed values
    :param df:
    :return:
    """
    return df.withColumn('origin_value', F.when(F.col('value') != F.col('new_value'), F.col('value')))


def get_cf_period(df: DataFrame, condition: Union[None, Column], window: Window) -> DataFrame:
    """
    getting df with carry forward period for changed value
    :param df:
    :param condition:
    :param window:
    :return:
    """
    # Window.partitionBy('ndc11').orderBy('period').rowsBetween(Window.unboundedPreceding, -1)
    # condition = (F.col('value') != F.col('new_value'))
    df = df.withColumn('cf_period', F.when(F.col('value') > 0, F.col('period')))
    return df.withColumn('cf_period', F.when(condition & F.col('cf_period').isNull(), F.last('cf_period', True)
                                             .over(window)))


def get_comment(df: DataFrame) -> DataFrame:
    """
    add column 'comment' explains cf_period
    :return: Dataframe
    """
    return df.withColumn('comment', F.when(F.col('cf_period').isNull(), F.lit('-'))
                         .otherwise(F.concat(F.lit('Negative value exception, carry forward from '),
                                             F.col('cf_period'), F.lit(' period'))))


def normalize_df_columns(df: DataFrame) -> DataFrame:
    """
    drop and rename columns
    :param df:
    :return: Dataframe
    """
    return df.drop('value').withColumnRenamed('new_value', 'value')


def get_prior_value(df: DataFrame, window: Window) -> DataFrame:
    """
    add column prior_value with value from prior period through window
    :param df:
    :param window:
    :return: DataFrame
    """
    # Window.partitionBy('ndc11').orderBy('period')
    return df.withColumn('prior_value', F.lag('value', 1, 0).over(window))


def get_variance_percentage(df: DataFrame) -> DataFrame:
    """
    calculate variance percentage by prior_value and value
    :param df:
    :return: DataFrame
    """
    zero_condition = (F.col('value') == 0)
    not_zero_condition = F.round((F.col('value') - F.col('prior_value')) / F.col('prior_value') * 100, 2)
    variance_df = df.withColumn('variance', F.when(F.col('prior_value') == 0, F.when(zero_condition, 0).otherwise(100))
                                .otherwise(not_zero_condition.cast(FloatType())))
    return variance_df
