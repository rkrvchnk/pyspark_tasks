from task_3 import Bucket, Unbucketed, JoinBuckets
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from datetime import datetime

import pytest


spark = SparkSession.builder.appName("Clients").getOrCreate()

# schema for trx_table
schema = StructType([
    StructField('ndc11', StringType()),
    StructField('invoice_amount', IntegerType()),
    StructField('invoice_quantity', IntegerType()),
    StructField('bu_dir.state', StringType()),
    StructField('bu_whl.state', StringType()),
    StructField('invoice_date', DateType()),
])

# data for trx_table
data = [(1, 100, 10, 'USA', 'USA', datetime(2019, 1, 3)),
        (1, 24, 20, 'NM', 'NM', datetime(2019, 2, 24)),
        (1, 200, 23, 'USA', 'USA', datetime(2019, 1, 20)),
        (2, 270, 14, 'USA', 'USA', datetime(2019, 3, 15)),
        (2, 340, 55, 'USA', 'PR', datetime(2019, 2, 17)),
        (2, 312, 34, 'NM', 'USA', datetime(2019, 2, 4)),
        (2, 425, 22, None, 'USA', datetime(2019, 2, 9))
        ]

# create test_df
trx_table = spark.createDataFrame(data, schema=schema)


# add column 'month'
trx_table = trx_table.withColumn('month', F.date_format('invoice_date', 'yyyyMM'))

# from task_4 import Bucket, prepare_df_with_month
# import pytest
# from datetime import datetime
# from pyspark.sql import functions as F
# from pyspark.sql import Column
# from pyspark.sql.types import StructType, StructField, DateType
#
#
# @pytest.fixture
# def bucket(trx_table):
#     agg_cols = [(F.col('invoice_amount'), 'cost'), (F.col('invoice_quantity'), 'quan')]
#     bucket = Bucket(trx_table, [trx_table['`bu_dir.state`'] == 'USA'], ['ndc11', 'month'], agg_cols, 'dir_sls_')
#     return bucket
#
#
# def test_adjust_prefix(bucket, trx_table):
#     bucket = bucket.adjust_prefix(trx_table)
#     assert 'dir_sls_invoice_amount' in bucket.columns
#     assert 'ndc11' in bucket.columns
#
#
# def test_get_aggregations(bucket):
#     assert isinstance(bucket.get_aggregations()[0], Column)
#
#
# @pytest.mark.parametrize('expect_cols', ['ndc11', 'dir_sls_cost'])
# def test_create_bucket(bucket, expect_cols):
#     actual_bucket = bucket.create_bucket()
#
#     assert expect_cols in actual_bucket.columns
#     assert actual_bucket.collect()[0]['dir_sls_cost'] == 300
#
#
# @pytest.mark.parametrize('row, expect', [((datetime(2019, 1, 3),), '201901'),
#                                          ((datetime(2020, 3, 4),), '202003')])
# def test_prepare_df_with_month(spark, row, expect):
#     df = spark.createDataFrame([row], schema=StructType([StructField('invoice_date', DateType())]))
#     actual_df = prepare_df_with_month(df)
#     assert 'month' in actual_df.columns
#     assert actual_df.collect()[0]['month'] == expect