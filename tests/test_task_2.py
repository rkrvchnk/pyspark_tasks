from task_2 import to_join_domain
from datetime import datetime


def test_to_join_domain(spark):

    # create test_df
    columns = ['ndc11', 'cost', 'ship_to_id', 'whl_id', 'type', 'invoice_date']
    data = [(1, float(100), 1, 3, 'DIR', datetime(2019, 1, 25)),
            (2,	float(200), 1, 2, 'DIR', datetime(2023, 9, 3)),
            (2,	float(240), 2, 3, 'WHL', datetime(2020, 5, 2)),
            (4,	float(320), 3, 2, 'WHL', datetime(2017, 1, 4))]
    test_df = spark.createDataFrame(data, columns)

    # create test_df1
    columns1 = ['id', 'name', 'address_id', 'start_date', 'end_date']
    data1 = [(1, "John", 1, datetime(2019, 1, 1), datetime(2019, 12, 31)),
             (1, "Parry", 2, datetime(2020, 1, 1), datetime(2025, 12, 31)),
             (2, "Paul", 1, datetime(2020, 1, 1), datetime(2020, 12, 31)),
             (3, "Mathew", 2, datetime(2015, 1, 31), None),
             (4, "Ann", 3, datetime(2000, 1, 1), None)]
    test_df1 = spark.createDataFrame(data1, columns1)

    expected_columns =['ndc11', 'cost', 'ship_to_id', 'whl_id', 'type', 'invoice_date', 'ship_to_bu.id',
                       'ship_to_bu.name', 'ship_to_bu.address_id', 'ship_to_bu.start_date', 'ship_to_bu.end_date']

    actual_result = to_join_domain(test_df, test_df1, test_df.ship_to_id, test_df1.id, 'ship_to_bu.')

    assert expected_columns == actual_result.columns
    assert actual_result.count() == 4
