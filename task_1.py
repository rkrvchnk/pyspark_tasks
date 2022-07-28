from routins_func import quarter_adjust_spark
from pyspark.sql.types import StringType
from pyspark.sql import functions as F


def routines(df):

    quarter_cond = F.when(F.col('invoice_date').isNull(), F.lit('0000q1'))\
        .otherwise(F.concat(F.year('invoice_date'), F.lit('q'), F.quarter('invoice_date')))
    result_df = df.withColumn('invoice_month', F.date_format('invoice_date', 'yyyyMM')). \
        withColumn('invoice_quarter', quarter_cond). \
        withColumn('invoice_year', F.year('invoice_date').cast(StringType())). \
        withColumn('invoice_nfyear', quarter_adjust_spark('invoice_quarter', 1).substr(0, 4))

    return result_df
