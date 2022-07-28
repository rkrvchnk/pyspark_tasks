from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_min_or_max_by_ppu(dataframe: DataFrame, rows, sort_type=None) -> DataFrame:
    """
    get min or max by PPU with defined number of rows for each partition
    with alphabetical sort by names if we have similar values
    :param dataframe:
    :param rows:
    :param sort_type:
    :return: DataFrame
    """
    ppu_col =  F.col('invoice_cost') / F.col('invoice_quan')

    # define sorting type
    if sort_type == 'max':
        sort_type = ppu_col.desc()
    elif sort_type == 'min':
        sort_type = ppu_col

    # we need window to make partition by 'ndc11' and month as well and order it by sort_type and customer_name
    window = Window().partitionBy('ndc11', F.month('invoice_date')).orderBy([sort_type, '`bu_.customer_name`'])

    # we need to make condition which depends on number of rows we want to see
    if rows > 1:
        filtering = F.col('row') <= rows
    elif 1 >= rows >= 0:
        filtering = F.col('row') == rows
    else:
        raise ValueError('You cant pass negative row')

    dataframe = dataframe.withColumn('row', F.row_number().over(window)).filter(filtering).drop('row')

    return dataframe
