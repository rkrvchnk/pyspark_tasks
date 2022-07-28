from task_1 import routines
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from datetime import datetime

import pytest

@pytest.mark.parametrize('row, expected_month, expected_quarter, expected_year, expected_nfyear', [
    (('1', float(100), datetime(2019, 1, 25)), '201901', '2019q1', '2019', '2019'),
    (('2', float(200), datetime(2023, 9, 3)), '202309', '2023q3', '2023', '2023'),
    (('2', float(240), datetime(2020, 5, 2)), '202005', '2020q2', '2020', '2020'),
    (('1', float(310), datetime(2019, 10, 28)), '201910', '2019q4', '2019', '2020'),
])
def test_routines(spark, row, expected_month, expected_quarter, expected_year, expected_nfyear):

    # schema for df
    schema = StructType([
        StructField('ndc11', StringType()),
        StructField('cost', FloatType()),
        StructField('invoice_date', DateType()),
    ])
    # create a test_df
    test_df = spark.createDataFrame([row], schema=schema)
    result_df = routines(test_df)
    actual_result = result_df.collect()

    expected_columns = [('ndc11', 'string'),
                        ('cost', 'float'),
                        ('invoice_date', 'date'),
                        ('invoice_month', 'string'),
                        ('invoice_quarter', 'string'),
                        ('invoice_year', 'string'),
                        ('invoice_nfyear', 'string')
                        ]

    assert expected_columns == result_df.dtypes
    assert actual_result[0]['invoice_month'] == expected_month
    assert actual_result[0]['invoice_quarter'] == expected_quarter
    assert actual_result[0]['invoice_year'] == expected_year
    assert actual_result[0]['invoice_nfyear'] == expected_nfyear
