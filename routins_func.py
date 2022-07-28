from pyspark.sql.functions import when, col, lit
from pyspark.sql.column import Column
from pyspark.sql.window import Window
import logging
from pyspark.sql import types as t
from pyspark.sql import functions as F
from datetime import datetime, timedelta

COLUMNS_DELIMITER = '.'

# DATE ROUTINES FUNCTIONS


def escape(column: str) -> str:
    """escape DataFrame column"""
    if COLUMNS_DELIMITER in column:
        column = "`{}`".format(column.strip('`'))
    return column


def to_pyspark_column(column) -> Column:
    """convert string column to pyspark Column"""
    if isinstance(column, str):
        column = col(escape(column))
    return column


def quarter_first_month(column) -> Column:
    """converts quarter (2008q1) into month (200801)"""
    column = F.coalesce(column, F.lit("0000q1"))
    year = column.substr(0, 4)
    qtr = column.substr(6, 6)
    return F.concat(year, F.lpad(qtr.cast(t.IntegerType()) * 3 - 2, 2, "0"))


def date_to_quarter(date_column) -> Column:
    date_column = to_pyspark_column(date_column)
    year = F.year(date_column)
    quarter = F.quarter(date_column)
    return when(
        date_column.isNull(), F.lit('0000q1')
    ).otherwise(
        F.concat(year, F.lit('q'), quarter)
    )


def date_to_month(column, fmt: str = 'yyyyMM') -> Column:
    """convert dates using given date format"""
    if isinstance(column, str):
        return F.date_format(to_pyspark_column(column), fmt).alias(column)
    return F.date_format(column, fmt)


def normalized(column) -> Column:
    # workaround to replace non-breaking spaces `\xa0`
    column = F.translate(to_pyspark_column(column), '\xa0', ' ')
    return F.lower(F.trim(column))


def quarter_adjust_spark(column, amount: int) -> Column:
    """adjust the quarter by the number of quarters for spark column"""
    column = to_pyspark_column(column)
    if not amount:
        return column
    mnths = quarter_first_month(column)
    mnths = month_adjust_spark(mnths, amount * 3, as_str=False)
    return date_to_quarter(mnths)


def month_adjust_spark(column, amount: int, fmt: str = 'yyyyMM', as_str=True) -> Column:
    """adjust the month (200801) by a number of months"""
    column = to_pyspark_column(column)
    month = F.to_date(column, fmt)
    adjusted = F.add_months(month, amount)
    if as_str:
        adjusted = date_to_month(adjusted, fmt)
    return adjusted


def period_adjust_spark(column, amount: int) -> Column:
    column = to_pyspark_column(column)
    if not amount:
        return column

    period_adjusted = when(
        # example: '2018Q1'
        (normalized(column).like('%q%')), quarter_adjust_spark(column, amount)
    ).when(
        # example: '201802'
        F.length(column) == 6, month_adjust_spark(column, amount)
    ).when(
        # example: '2018'
        F.length(column) == 4, (column.cast('int') + amount).cast('string')
    )

    return period_adjusted


def quarter_to_months_array(quarter) -> Column:
    """
    In [74]: _df.show(2)
    +-----------+-------+
    |      ndc11|quarter|
    +-----------+-------+
    |00597000160| 2019q3|
    |00597000201| 2019q3|
    +-----------+-------+
    only showing top 2 rows

    In [75]: _df.withColumn('arr', quarter_to_months_array('quarter')).show(2)
    +-----------+-------+------------+
    |      ndc11|quarter|         arr|
    +-----------+-------+------------+
    |00597000160| 2019q3|[07, 08, 09]|
    |00597000201| 2019q3|[07, 08, 09]|
    +-----------+-------+------------+
    """
    year = '0000'
    quarter_months_map = []
    for quarter_num in range(1, 5):
        quarter_num = str(quarter_num)
        months = quarter_to_months_python(year + 'q' + quarter_num)
        months = F.array(*[F.lit(m[len(year):]) for m in months])
        quarter_months_map.extend((F.lit(quarter_num), months))

    quarter_months_map = F.create_map(*quarter_months_map)
    quarter_num = to_pyspark_column(quarter).substr(6, 6)
    months_arr = quarter_months_map.getItem(quarter_num)

    return months_arr


def quarter_to_months_python(quarter: str) -> list:
    # returns list of months in quarter
    # "2017q1" -> ['201701', '201702', '201703']
    assert isinstance(quarter, str)
    quarter = quarter.strip().lower()
    year = quarter[:4]
    quarter_num = int(quarter[-1])
    assert quarter_num <= 4, quarter

    end_month = quarter_num * 3
    start_month = end_month - 2
    months = range(start_month, end_month + 1)
    return [year + str(m).zfill(2) for m in months]


def period_to_year(column) -> Column:
    """ '201801' -> '2018' """
    return to_pyspark_column(column).substr(0, 4)


def date_to_halfyear(column) -> Column:
    column = to_pyspark_column(column)
    year, month = F.year(column), F.month(column)
    halfyear = when(month <= 6, F.lit('h1')).when(month > 6, F.lit('h2'))
    return when(column.isNotNull(), F.concat(year, halfyear))

def union_by_name(*dfs):
    assert len(dfs) > 0
    dfs_iter = iter(dfs)
    first = next(dfs_iter)
    for df in dfs_iter:
        first = first.unionByName(df)  # without 'allowMissingColumn' test_union_by_name will fail
    return first


def unequal_union_dfs(df1, df2):
    logger = logging.getLogger('debugger')
    df1_schema = set((x.name, x.dataType) for x in df1.schema)
    df2_schema = set((x.name, x.dataType) for x in df2.schema)
    for column, _type in df2_schema.difference(df1_schema):
        if column in df1.columns:
            logger.warning(
                'Columns have different types. Left column: {}. Right column: {}.'.format(
                    list(*[x for x in df1.dtypes if x[0] == column]),
                    list(*[x for x in df2.dtypes if x[0] == column]),
                )
            )
        df1 = df1.withColumn(column, F.lit(None).cast(_type))

    for column, _type in df1_schema.difference(df2_schema):
        df2 = df2.withColumn(column, F.lit(None).cast(_type))

    common_schema_columns = [escape(c) for c in df1.columns]
    return df1.select(*common_schema_columns) \
        .union(df2.select(*common_schema_columns))


def unequal_union_multi_dfs(*dfs):
    """union multiple DataFrames with different schemas"""
    assert len(dfs) > 0
    dfs_iter = iter(dfs)
    first = next(dfs_iter)
    for df in dfs_iter:
        first = unequal_union_dfs(first, df)
    return first

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