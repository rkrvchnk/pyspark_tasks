import pytest
from task_8 import (
    get_comment,
    get_variance_percentage,
    get_prior_value,
    get_origin_value,
    get_cf_period,
    get_pos_value_from_prev_period,
    normalize_df_columns
    )
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType


@pytest.mark.parametrize('rows, expect', [([(1, 1, 50), (1, 2, -50)], 50),
                                          ([(1, 1, -12), (1, 2, -50)], 0)])
def test_get_pos_value_from_prev_period(spark, rows, expect):
    test_df = spark.createDataFrame(rows, ['ndc11', 'period', 'value'])
    w = Window.partitionBy('ndc11').orderBy('period').rowsBetween(Window.unboundedPreceding, -1)
    actual_df = get_pos_value_from_prev_period(test_df, w)
    assert actual_df.collect()[-1]['new_value'] == expect


@pytest.mark.parametrize('rows, expect', [([(1, 1, 50, 50), (1, 2, -50, 50)], -50),
                                          ([(1, 1, -12, 0), (1, 2, -33, 0)], -33)])
def test_origin_value(spark, rows, expect):
    test_df = spark.createDataFrame(rows, ['ndc11', 'period', 'value', 'new_value'])
    actual_df = get_origin_value(test_df)
    assert actual_df.collect()[-1]['origin_value'] == expect


@pytest.mark.parametrize('rows, expect', [([(1, 1, 50, 50), (1, 2, -50, 50)], 1),
                                          ([(1, 1, -12, 0), (1, 2, -33, 0)], None)])
def test_get_cf_period(spark, rows, expect):
    test_df = spark.createDataFrame(rows, ['ndc11', 'period', 'value', 'new_value'])

    w = Window.partitionBy('ndc11').orderBy('period').rowsBetween(Window.unboundedPreceding, -1)
    condition = (F.col('value') != F.col('new_value'))

    actual_df = get_cf_period(test_df, condition=condition, window=w)

    assert actual_df.collect()[-1]['cf_period'] == expect


@pytest.mark.parametrize('rows, expect', [([(1, 1, 50.0, 50.0, None), (1, 2, -50.0, 50.0, 1)], 1),
                                          ([(1, 1, -12.0, 0.0, None), (1, 2, -33.0, 0.0, None)], '-')])
def test_get_comment(spark, rows, expect):
    schema = StructType([
        StructField('ndc11', StringType()),
        StructField('period', StringType()),
        StructField('value', FloatType()),
        StructField('new_value', FloatType()),
        StructField('cf_period', StringType()),
    ])
    test_df = spark.createDataFrame(rows, schema)
    expect = '-' if expect == '-' else 'Negative value exception, carry forward from {} period'.format(expect)

    actual_df = get_comment(test_df)
    assert actual_df.collect()[-1]['comment'] == expect


@pytest.mark.parametrize('rows, expect_cols, expect_name', [((1, 1, 50), 2, 'value'),
                                                            ((1, 1, -12), 2, 'value')])
def test_normalize_df_columns(spark, rows, expect_cols, expect_name):
    test_df = spark.createDataFrame([rows], ['ndc11', 'value', 'new_value'])

    actual_df = normalize_df_columns(test_df)

    assert len(actual_df.columns) == expect_cols
    assert expect_name in actual_df.columns


@pytest.mark.parametrize('rows, expect', [([(1, 1, 20), (1, 2, 30)], 20),
                                          ([(1, 1, 0), (1, 2, 40)], 0)])
def test_get_prior_value(spark, rows, expect):
    test_df = spark.createDataFrame(rows, ['ndc11', 'period', 'value'])
    w = Window.partitionBy('ndc11').orderBy('period')

    actual_df = get_prior_value(test_df, w)
    assert actual_df.collect()[-1]['prior_value'] == expect


@pytest.mark.parametrize('rows, expect', [((30, 0), 100),
                                          ((30, 30), 0),
                                          ((100, 30), 233.33)])
def test_get_variance_percentage(spark, rows, expect):
    test_df = spark.createDataFrame([rows], ['value', 'prior_value'])

    actual_df = get_variance_percentage(test_df)
    assert round(actual_df.collect()[0]['variance'], 2) == expect
