from routins_func import smoothing_period
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql import functions as F
from datetime import datetime


# spark session
spark = SparkSession.builder.appName('Task5').getOrCreate()

schema = StructType([
    StructField('ndc11', StringType()),
    StructField('package_size_intro_date', DateType()),
    StructField('last_lot_termination_date', DateType())
])

data = [(1, datetime(2016, 1, 1), datetime(2016, 10, 23)),
        (2, datetime(2016, 1, 1), datetime(2017, 1, 1)),
        (3, datetime(2017, 3, 22), datetime(2017, 4, 15)),
        (4, datetime(2017, 8, 22), datetime(2018, 3, 15)),
        (5, datetime(2018, 2, 5), datetime(2018, 4, 7)),
        (6, datetime(2018, 3, 21), datetime(2020, 1, 1)),
        (7, datetime(2018, 4, 12), None)]

# create product table
product_table = spark.createDataFrame(data, schema=schema)

RUN_PERIOD = {'start': '201801', 'end': '201805'}

# create a list with all months between start and end date with prior months from the start
prior_months = smoothing_period(RUN_PERIOD['start'], RUN_PERIOD['end'], 11)

# create df with months from prior_months list
report_periods_df = spark.createDataFrame([(d,) for d in prior_months], ['period'])

# Function defines the last month of a special quarter (2021.01.01 - > 2021.03.01)
def last_quarter_month_of_adj_year(column: Column) -> Column:
    adj_year = F.add_months(column, 12)
    return F.concat(F.year(adj_year), F.lit('-'), F.lpad(F.quarter(adj_year) * 3, 2, "0"), F.lit('-'), F.lit('01'))


# Function changes string type from "202101" to string type "2021-01-01"
def format_column_to_date_string(column: Column) -> Column:
    return F.concat(column.substr(0, 4), F.lit('-'), column.substr(5, 6), F.lit('-'), F.lit('01'))


def to_product_df_with_period(left_df: DataFrame, right_df: DataFrame, date: str):
    """
    Join product df with periods_df to populate each product with respective reported period, e.g. '201712', '201702'...
    :param left_df: DataFrame
    :param right_df: DataFrame
    :param date: str, a product start date, e.g 'package_size_intro_date'
    :return: DataFrame
    """

    # format date column to another view to make it possible to format into DateType
    period_date = format_column_to_date_string(right_df.period)

    # condition to check if period date bigger or equal to start unit date
    condition_for_start = F.when(F.year(left_df[date]) == F.year(period_date),
                     (F.month(left_df[date]) <= F.month(period_date))).otherwise(
            F.year(left_df[date]) <= F.year(period_date))

    # condition to check if period date less then or equal to last unit date consider last month of quarter
    condition_for_last_month_of_quarter = (F.to_date(period_date, 'yyyy-MM-dd') <=
                                               last_quarter_month_of_adj_year(left_df.last_lot_termination_date))

    # result condition to join two conditions and consider case when last unit date is NULL
    res_condition = F.when(left_df.last_lot_termination_date.isNull(),
                               condition_for_start).otherwise(condition_for_start & condition_for_last_month_of_quarter)

    # join product table with period on res_condition to add a reported period
    product_df = left_df.join(right_df, on=res_condition, how='left')

    return product_df

def add_if_reportable(df, start_date: str, end_date: str) -> DataFrame:
    """
    Add 'is_reportable' column to a df according to conditions: 'Y' - if product period is reportable; 'N' - if product
    is eligible for calculation, but not reportable.
    :param df: DataFrame with reported periods
    :param start: str, the start of a reported period,  e.g. '201801'
    :param end: str, the end of a reported period,  e.g. '201801'
    :param start_date: str, a product start date, e.g 'package_size_intro_date'
    :param end_date: str, a product end date, e.g 'last_lot_termination_date'
    :return:
    """

    # format date column to another view to make it possible to format into DateType
    product_period_date = format_column_to_date_string(df.period)

    # reported periods for product
    reported_periods = smoothing_period(RUN_PERIOD['start'], RUN_PERIOD['end'], 0)

    # condition for start unit date
    condition_for_start = (F.year(product_period_date) > F.year(F.col(start_date))) | \
                          ((F.year(product_period_date) == F.year(F.col(start_date)))
                           & (F.month(product_period_date) >= F.month(F.col(start_date))))

    # condition for end unit date
    condition_for_end = (F.year(product_period_date) < F.year(F.col(end_date))) | \
                        ((F.year(product_period_date) == F.year(F.col(end_date)))
                         & (F.month(product_period_date) <= F.month(F.col(end_date))))

    # join start and end condition and check if result in reported periods
    if_not_null = (condition_for_start & condition_for_end) & F.col('period').isin(reported_periods)

    # if end date of unit is NULL case
    if_null_condition = (F.col(end_date).isNull() & (
            F.year(product_period_date) >= F.year(F.col(start_date)))) & (
                                F.month(product_period_date) >= F.month(F.col(start_date))) & \
                        F.col('period').isin(reported_periods)

    # add a column 'is_reported' by our condition
    df = df.withColumn('is_reported', F.when(if_null_condition | if_not_null, F.lit('Y')).otherwise(F.lit('N')))

    return df

# to_product_df_with_period(product_table,  report_periods_df, 'package_size_intro_date').show()
# add_if_reportable(product_table, report_periods_df, 'package_size_intro_date', 'last_lot_termination_date').show(100)
