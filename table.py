from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime

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
product_table.show()


def smoothing_period(start_date: str, end_date: str, prior_months: int = 12) -> list:
    """
    Get list of dates between start_date and end_date + prior_months before start_date
    For example:
        start_date: '201802', end_date: '201804', prior_months: 2
        return: ['201712', '201801', '201802', '201803', '201804']
    """
    prior_months += months_between(start_date, end_date)

    year, month = int(end_date[:4]), int(end_date[4:])
    result = [end_date]

    for _ in range(1, prior_months + 1):
        month -= 1

        if month == 0:
            month = 12
            year -= 1

        result.append(str(year) + (str(month) if month > 9 else '0{}'.format(month)))

    return sorted(result)


def months_between(start_date: str, end_date: str) -> int:
    """Get amount of months between start_date and end_date"""
    if int(start_date) >= int(end_date):
        return 0
    year, month = int(end_date[:4]), int(end_date[4:])
    counter = 0
    while str(year) + (str(month) if month > 9 else '0{}'.format(month)) != start_date:
        counter += 1
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return counter


RUN_PERIOD = {'start': '201801', 'end': '201805'}

# Calculated month according to 'prior month' condition
calc_month = smoothing_period(RUN_PERIOD['start'], RUN_PERIOD['end'], 11)

# create a df with months from calculated period
calc_month_df = spark.createDataFrame([(d,) for d in calc_month], ['date'])
calc_month_df.show()


def quarter_last_month():
    """
    adjust a year to get the next one. Then, find the last month of a quarter
    :return: Column
    """
    adj_year = F.add_months(product_table.last_lot_termination_date, 12)
    return F.concat(F.year(adj_year), F.lit('-'),
                    F.lpad(F.quarter(product_table.last_lot_termination_date)
                           .cast(IntegerType()) * 3, 2, "0"), F.lit('-'), F.lit('01'))


# create a df with a new column 'last month'
df = product_table.withColumn('last_months',
                              F.when(product_table.last_lot_termination_date.isNull(),
                                     F.lit('2018-05-01')).otherwise(F.to_date(quarter_last_month(), 'yyyy-MM-dd')))

df.show()


def to_date_format(dataframe, column):
    """change a string from '201802' into a string '2018-02-01'"""
    return F.concat(dataframe[column].substr(0, 4), F.lit('-'), dataframe[column].substr(5, 6), F.lit('-'), F.lit('01'))


# change a string from '201802' into a string '2018-02-01'
date = to_date_format(calc_month_df, 'date')

# conditions reportable dates
same_year = F.year(product_table['package_size_intro_date']) == F.year(date)

condition = F.when(same_year, F.month(product_table['package_size_intro_date']) <= F.month(date))\
             .otherwise(F.year(product_table['package_size_intro_date']) <= F.year(date))
condition1 = (F.to_date(date, 'yyyy-MM-dd') <= df.last_months)

result_df = df.join(calc_month_df, on=(condition & condition1), how='left')
result_df.show()

reported_periods = ['201801', '201802', '201803', '201804', '201805']
