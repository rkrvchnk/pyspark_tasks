import pytest
from task_10 import (
    get_sum_customer_by_month_and_product,
    get_best_customer_by_month,
    get_best_customer_from_prev_month,
    get_periods_by_year_and_month
                    )
from pyspark.sql import Window
from pyspark.sql import functions as F
from datetime import datetime


@pytest.mark.parametrize('rows, expect_sum, expect_customer', [([(1, datetime(2018, 1, 21), 10.0, 'Ann'),
                                                                 (1, datetime(2018, 1, 21), 10.0, 'Ann'),
                                                                 (1, datetime(2018, 1, 21), 15.0, 'John')], 20, 'Ann'),
                                                               ([(1, datetime(2018, 1, 21), 10.0, 'Ann'),
                                                                 (1, datetime(2018, 1, 21), 10.0, 'Ann'),
                                                                 (1, datetime(2018, 1, 21), 35.0, 'John')], 35, 'John')
                                                               ])
def test_get_sum_customer_by_month_and_product(spark, rows, expect_sum, expect_customer):
    test_df = spark.createDataFrame(rows, ['ndc11', 'date', 'value', 'customer'])

    window = Window.partitionBy(F.month('date'), 'ndc11', 'customer')
    actual_df = get_sum_customer_by_month_and_product(test_df, window)
    best_customer = actual_df.select('customer', 'sum').orderBy(F.col('sum').desc())
    assert best_customer.collect()[0]['customer'] == expect_customer
    assert best_customer.collect()[0]['sum'] == expect_sum
    assert len(actual_df.columns) == 5


@pytest.mark.parametrize('rows, expect_customer', [([(1, datetime(2018, 1, 21), 10.0, 'Ann', 20),
                                                     (1, datetime(2018, 1, 21), 10.0, 'Ann', 20),
                                                     (1, datetime(2018, 1, 21), 15.0, 'John', 15)], 'Ann'),
                                                   ([(1, datetime(2018, 1, 21), 10.0, 'Ann', 20),
                                                     (1, datetime(2018, 1, 21), 10.0, 'Ann', 20),
                                                     (1, datetime(2018, 1, 21), 35.0, 'John', 35)], 'John')
                                                   ])
def test_get_best_customer_by_month(spark, rows, expect_customer):
    test_df = spark.createDataFrame(rows, ['ndc11', 'date', 'value', 'customer', 'sum'])
    window = Window.partitionBy(F.month('date'), 'ndc11').orderBy(F.col('sum').desc(), F.col('customer'))

    actual_df = get_best_customer_by_month(test_df, window)
    assert actual_df.collect()[0]['best_customer'] == expect_customer
    assert len(actual_df.columns) == 5


@pytest.mark.parametrize('rows, expect_period', [([(1, datetime(2018, 1, 21)),
                                                   (1, datetime(2018, 1, 21)),
                                                   (2, datetime(2018, 4, 21))], (1, 2)),
                                                 ([(1, datetime(2018, 1, 21)),
                                                   (1, datetime(2018, 2, 21)),
                                                   (2, datetime(2018, 4, 21))], (1, 3))
                                                 ])
def test_get_periods_by_year_and_month(spark, rows, expect_period):
    test_df = spark.createDataFrame(rows, ['ndc11', 'date'])

    window = Window.orderBy(F.year('date'), F.month('date'))
    actual_df = get_periods_by_year_and_month(test_df, window)
    assert actual_df.collect()[0]['period'] == expect_period[0]
    assert actual_df.collect()[-1]['period'] == expect_period[1]


@pytest.mark.parametrize('rows, expect_customer', [([(1, datetime(2018, 1, 21), 10.0, 'Ann', 'Ann', 1),
                                                     (1, datetime(2018, 1, 21), 10.0, 'Mark', 'Ann', 1),
                                                     (1, datetime(2018, 2, 21), 15.0, 'John', 'John', 2)], 'Ann'),
                                                   ([(2, datetime(2018, 4, 21), 22.0, 'John', 'John', 1),
                                                     (2, datetime(2018, 4, 21), 10.0, 'Ann', 'John', 1),
                                                     (2, datetime(2018, 5, 21), 35.0, 'John', 'John', 2)], 'John')
                                                   ])
def test_get_best_customer_from_prev_month(spark, rows, expect_customer):
    test_df = spark.createDataFrame(rows, ['ndc11', 'date', 'value', 'customer', 'best_customer', 'period'])

    window = Window().partitionBy('ndc11').orderBy('period').rangeBetween(-1, 0)
    actual_df = get_best_customer_from_prev_month(test_df, window)
    assert actual_df.collect()[0]['best_prior_customer'] is None
    assert actual_df.collect()[-1]['best_prior_customer'] == expect_customer
