from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from pyspark.sql import SparkSession
from datetime import datetime

from task_4 import get_min_or_max_by_ppu
import pytest


# create a spark session
spark = SparkSession.builder.appName("task_0").getOrCreate()

# create a test dataframe
schema = StructType([
    StructField('ndc11', StringType()),
    StructField('invoice_date', DateType()),
    StructField('invoice_cost', FloatType()),
    StructField('invoice_quan', FloatType()),
    StructField('bu_.customer_name', StringType()),
])

data = [(1, datetime(2020, 1, 15), 40.0, 10.0, 'John'),
        (1, datetime(2020, 1, 7), 50.0, 10.0, 'Ann'),
        (1, datetime(2020, 1, 22), 40.0, 2.0, 'Ann'),
        (1, datetime(2020, 2, 15), 20.0, 10.0, 'John'),
        (1, datetime(2020, 2, 7), 50.0, 10.0, 'Ann'),
        (1, datetime(2020, 2, 21), 40.0, 20.0, 'Mathew'),
        (2, datetime(2020, 2, 22), 50.0, 10.0, 'Carter'),
        (2, datetime(2020, 2, 22), 40.0, 8.0, 'Ann')
        ]

test_trx_df = spark.createDataFrame(data, schema=schema)


@pytest.mark.parametrize('rows, sort, expected_cost, expected_names',
                         [(1, 'min', [40.0, 20.0, 40.0], ['Ann', 'John', 'John']),
                          (1, 'max', [40.0, 50.0, 40.0], ['Ann', 'Ann', 'Ann'])])
def test_get_min_or_max_by_ppu(rows, sort, expected_cost, expected_names):

    result_df = get_min_or_max_by_ppu(test_trx_df, rows, sort)
    actual_result = result_df.collect()
    actual_invoice_cost = [row.invoice_cost for row in actual_result]
    actual_names = [row['bu_.customer_name'] for row in actual_result]

    assert actual_invoice_cost == expected_cost
    assert actual_names == expected_names
