from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from datetime import datetime

def to_join_domain(
        left_table: DataFrame,
        right_table: DataFrame,
        left_item_id: Column,
        right_item_id: Column,
        prefix=None
                ) -> DataFrame:
    """
    join two tables by condition and change prefix if exist
    :param left_table: DataFrame
    :param right_table: DataFrame
    :param left_item_id: Column
    :param right_item_id: Column
    :param prefix: str or None
    :return: DataFrame
    """

    # conditions
    unit_active_condition_start_date = right_table.start_date <= left_table.invoice_date
    unit_active_condition_end_date_if_not_null = left_table.invoice_date <= right_table.end_date
    unit_active_condition_end_date_if_null = right_table.end_date.isNull()

    # stick all conditions together
    condition = (left_item_id == right_item_id) & unit_active_condition_start_date & \
                (unit_active_condition_end_date_if_null | unit_active_condition_end_date_if_not_null)

    # join tables
    dataframe = left_table.join(right_table, on=condition, how='left')

    # check prefix and make if exist
    if prefix:
        new_cols = [prefix + col if col in right_table.columns else col for col in dataframe.columns]
        dataframe = dataframe.toDF(*new_cols)
    return dataframe


if __name__ == '__main__':
    # spark session
    spark = SparkSession.builder.appName('Task2').getOrCreate()

    # schema for sales_trx df
    schema = StructType([
        StructField('ndc11', StringType()),
        StructField('cost', FloatType()),
        StructField('ship_to_id', StringType()),
        StructField('whl_id', StringType()),
        StructField('type', StringType()),
        StructField('invoice_date', DateType()),
    ])
    # data for sales_trx df
    data = [(1, float(100), 1, 3, 'DIR', datetime(2019, 1, 25)),
            (2, float(200), 1, 2, 'DIR', datetime(2023, 9, 3)),
            (2, float(240), 2, 3, 'WHL', datetime(2020, 5, 2)),
            (4, float(320), 3, 2, 'WHL', datetime(2017, 1, 4))]

    # create sales_trx df
    sales_trx_df = spark.createDataFrame(data, schema=schema)

    # schema for bu_domain df
    schema1 = StructType([StructField("id", StringType()),
                          StructField("name", StringType()),
                          StructField("address_id", StringType()),
                          StructField("start_date", DateType()),
                          StructField("end_date", DateType())])

    # data for bu_domain df
    data1 = [(1, "John", 1, datetime(2019, 1, 1), datetime(2019, 12, 31)),
             (1, "Parry", 2, datetime(2020, 1, 1), datetime(2025, 12, 31)),
             (2, "Paul", 1, datetime(2020, 1, 1), datetime(2020, 12, 31)),
             (3, "Mathew", 2, datetime(2015, 1, 31), None),
             (4, "Ann", 3, datetime(2000, 1, 1), None)]

    # create bu_domain df
    bu_domain_df = spark.createDataFrame(data1, schema=schema1)

    # schema for address_domain df
    schema2 = StructType([StructField("id", StringType()),
                          StructField("state", StringType()),
                          StructField("start_date", DateType()),
                          StructField("end_date", DateType())])

    # data for address_domain df
    data2 = [(1, "USA", datetime(2000, 1, 1), None),
             (2, "NM", datetime(2020, 1, 1), datetime(2020, 12, 31)),
             (2, "USA", datetime(2019, 1, 1), datetime(2019, 12, 31)),
             ]

    # create address_domain df
    address_domain_df = spark.createDataFrame(data2, schema=schema2)

    result = to_join_domain(sales_trx_df, bu_domain_df, sales_trx_df.ship_to_id, bu_domain_df.id, 'ship_to_bu.')
    result = to_join_domain(result, bu_domain_df, result.whl_id, bu_domain_df.id, 'whl_bu.')
    result = to_join_domain(result, address_domain_df, result['`ship_to_bu.address_id`'], address_domain_df.id, 'ship_to_address.')
    result = to_join_domain(result, address_domain_df, result['`whl_bu.address_id`'], address_domain_df.id, 'whl_address.')
    # we dont get expected result because we dont send condition address_df.type == 'WHL', we need to make changes to expected result table
    result = result.withColumn('bu_name', F.when(F.col('type') == 'DIR', F.col('`ship_to_bu.name`'))
                               .otherwise(F.col('`whl_bu.name`')))\
                                .withColumn('bu_state', F.when(F.col('type') == 'DIR', F.col('`ship_to_address.state`'))
                                .otherwise(F.col('`whl_address.state`')))

    result.sort('cost').show()
