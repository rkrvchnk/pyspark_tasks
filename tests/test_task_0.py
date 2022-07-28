import pytest
from decimal import Decimal
from datetime import datetime
from task_0 import format_reported_price_col, format_df

from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
from pyspark.sql.column import Column
from pyspark.sql import functions as F


# todo actual, expected
@pytest.mark.parametrize('column', ['345123.3395', '931001', '0.427323215', '1.50'])
def test_format_reported_price_col(spark, column):

    # create a test_df with one column 'value'
    data = [Decimal(345123.3395), Decimal(931001), Decimal(0.427323215), Decimal(1.500)]
    test_df = spark.createDataFrame(data, DecimalType(38, 10))

    result_column = format_reported_price_col(F.col(column))

    formatted_column_df = test_df.withColumn('Reported Price', format_reported_price_col('value'))
    expected_res = ['345 123.34 $', '931 001.00 $', '0.43 $', '1.50 $']
    actual_res = formatted_column_df.select(F.collect_list('Reported Price')).first()[0]

    assert isinstance(result_column, Column)
    assert expected_res == actual_res


@pytest.mark.parametrize('row', [
    (1, 201909, Decimal(345123.3395), 1, None, datetime(2000, 1, 1), datetime(2020, 1, 1))
])
def test_format_df(spark, row):

    # schema for input df
    schema = StructType([StructField("ndc11", StringType()),
                         StructField("month", StringType()),
                         StructField("value", DecimalType(38, 10)),
                         StructField("bu.id", StringType()),
                         StructField("bu.name", StringType()),
                         StructField("bu.start_date", DateType()),
                         StructField("bu.end_date", DateType())])

    # create input df
    test_df = spark.createDataFrame([row], schema=schema)

    expected_columns = ['Product', 'Month', 'Reported Price', 'BU Name']
    expected_reported_price = '345 123.34 $'
    expected_bu_name = 'NOT DEFINED'

    actual_result = format_df(test_df)

    assert expected_columns == actual_result.columns

    actual_result = actual_result.collect()

    assert expected_reported_price == actual_result[0]['Reported Price']
    assert expected_bu_name == actual_result[0]['BU Name']
