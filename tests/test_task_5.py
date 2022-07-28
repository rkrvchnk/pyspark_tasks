from routins_func import smoothing_period
from task_5 import last_quarter_month_of_adj_year, format_column_to_date_string, to_product_df_with_period, add_if_reportable

from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import datetime

import pytest


@pytest.mark.parametrize('row, expected', [(('2019-01-01',), '2020-03-01')])
def test_last_quarter_month_of_adj_year(spark, row, expected):
    df = spark.createDataFrame([row], ['period'])
    result_df = df.withColumn('format_date', last_quarter_month_of_adj_year(df.period))
    actual = result_df.collect()[0]['format_date']

    assert actual == expected


@pytest.mark.parametrize('row, expected', [(('201901',), '2019-01-01')])
def test_format_column_to_date_string(spark, row, expected):
    df = spark.createDataFrame([row], ['date'])
    result_df = df.withColumn('format_date', format_column_to_date_string(df.date))
    actual_column = result_df.collect()[0]['format_date']

    assert actual_column == expected


@pytest.mark.parametrize('row, expected_periods, start, end', [
    ((1, datetime(2016, 1, 1), datetime(2016, 10, 23)), 11, '201702', '201712'),
    ((2, datetime(2017, 8, 22), datetime(2018, 3, 15)), 10, '201708', '201805'),
    ((3, datetime(2018, 2, 5), datetime(2018, 4, 7)), 4, '201802', '201805'),
    ((4, datetime(2018, 4, 12), None), 2, '201804', '201805')])
def test_to_product_df_with_period(spark, row, expected_periods, start, end):
    schema = StructType([
        StructField('ndc11', StringType()),
        StructField('package_size_intro_date', DateType()),
        StructField('last_lot_termination_date', DateType())
    ])
    product_df = spark.createDataFrame([row], schema)
    run_period = {'start': '201801', 'end': '201805'}
    prior_months = smoothing_period(run_period['start'], run_period['end'], 11)
    report_periods_df = spark.createDataFrame([(d,) for d in prior_months], ['period'])

    actual_df = to_product_df_with_period(product_df,  report_periods_df, 'package_size_intro_date')
    actual_periods = [row.period for row in actual_df.collect()]

    assert len(actual_periods) == expected_periods
    assert actual_periods[0] == start
    assert actual_periods[-1] == end


@pytest.mark.parametrize('row, expected',
                         [((1, datetime(2016, 1, 1), datetime(2016, 10, 23), '201702'), 'N'),
                          ((4, datetime(2018, 4, 12), None, '201805'), 'Y')
                          ])
def test_add_if_reportable(spark, row, expected):
    schema = StructType([
        StructField('ndc11', StringType()),
        StructField('package_size_intro_date', DateType()),
        StructField('last_lot_termination_date', DateType()),
        StructField('period', StringType()),
    ])
    product_df = spark.createDataFrame([row], schema)
    actual_df = add_if_reportable(product_df, 'package_size_intro_date', 'last_lot_termination_date')

    assert actual_df.collect()[0]['is_reported'] == expected
