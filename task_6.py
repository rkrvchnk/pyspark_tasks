import findspark
findspark.init()

from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from pyspark.sql import SparkSession, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

import sys



# # spark session
# spark = SparkSession.builder.appName('Task6').getOrCreate()
#
# # schema for input df
# input_df_schema = StructType([
#     StructField('ndc11', StringType()),
#     StructField('ndc9', StringType()),
#     StructField('period', StringType()),
#     StructField('dir_sis_cost', FloatType())
# ])
#
# # data for input df
# input_df_data = [
#     (11, 1, '201801', float(10)),
#     (12, 1, '201801', float(20)),
#     (21, 2, '201801', float(30)),
#     (22, 2, '201801', float(15)),
#     (11, 1, '201802', float(40)),
#     (21, 2, '201802', float(45)),
#     (31, 3, '201803', float(45)),
#     (32, 3, '201803', float(12)),
#     (41, 4, '201803', float(45)),
#     (51, 5, '201803', float(29)),
#     (61, 6, '201803', float(37)),
# ]
#
# # create input df
# input_df = spark.createDataFrame(input_df_data, schema=input_df_schema)
# input_df.show()
#
# # schema for product blend domain df
# product_blend_domain_schema = StructType([
#     StructField("ndc9", StringType()),
#     StructField("one_way_blend_9", StringType()),
#     StructField("ndc9_blend_group", StringType()),
#     StructField("start_date", DateType()),
#     StructField("end_date", DateType())
# ])
#
# # data for product blend domain df
# product_blend_domain_data = [
#     (1, 'n', 'Group1', datetime(2018, 1, 1), datetime(2018, 3, 31)),
#     (2, 'y', 'Group1', datetime(2017, 12, 1), datetime(2018, 1, 31)),
#     (2, 'n', 'Group1', datetime(2018, 2, 1), datetime(2018, 2, 28)),
#     (4, 'n', 'Group1', datetime(2018, 2, 1), None),
#     (5, 'n', 'Group1', datetime(2018, 3, 1), datetime(2018, 3, 31)),
#     (5, 'n', 'Group1', datetime(2018, 3, 2), datetime(2018, 3, 31)),
# ]
#
# # create product blend domain df
# product_blend_domain_df = spark.createDataFrame(product_blend_domain_data, schema=product_blend_domain_schema)
#
#
# # the function is used to change a string format e.g. '201801' into '2018-01-01'
# def format_column_to_date_type_string(column: Column) -> Column:
#     return F.concat(column.substr(0, 4), F.lit('-'), column.substr(5, 6), F.lit('-'), F.lit('01'))
#
#
# # change 'period' from '201801' into '2018-01-01'
# formated_period = format_column_to_date_type_string(input_df.period)
#
# # set conditions to join input and domain dfs
# date_condition = (formated_period >= product_blend_domain_df.start_date) & \
#                  (formated_period <= product_blend_domain_df.end_date)
# date_condition_if_null = (formated_period >= product_blend_domain_df.start_date)
# product_condition = input_df.ndc9 == product_blend_domain_df.ndc9
# result_condition = F.when(product_blend_domain_df.end_date.isNull(), (date_condition_if_null & product_condition))\
#                     .otherwise((date_condition & product_condition))
#
# # join input and domain dfs
# joined_df = input_df.join(product_blend_domain_df, on=result_condition, how='left')\
#     .select(input_df['*'], product_blend_domain_df.ndc9_blend_group.alias('blend_group'),
#             product_blend_domain_df.one_way_blend_9)
#
# # add a column 'one_way_blend', rename a column 'ndc9_blend_group' into 'blend_group'  and drop unused columns
# df = joined_df.withColumn('one_way_blend', F.when(F.col('one_way_blend_9').isNull(), F.col('ndc9'))\
#                                             .otherwise(F.col('one_way_blend_9'))).drop('one_way_blend_9')
#
# # change a string into another format
# date = format_column_to_date_type_string(F.col('period'))
#
# # creat windows
# w_if_n = Window().partitionBy(date, 'blend_group')
# w_y_and_null = Window().partitionBy(date, 'ndc9')
#
# # calculate blend_value
# blend_value_df = df.withColumn('blend_value', F.when(F.col('one_way_blend') == 'n', F.sum('dir_sis_cost').over(w_if_n))\
#                                .otherwise(F.sum('dir_sis_cost').over(w_y_and_null)))
# blend_value_df.sort('period', 'ndc9').show()


# spark session
spark = SparkSession.builder.appName('Task7').getOrCreate()


schema = StructType([
        StructField('ndc11', StringType()),
        StructField('period', StringType()),
        StructField('value', FloatType()),
    ])

data = [(1, 1, 30.0),
        (1, 2, -200.0),
        (1, 3, -100.0),
        (1, 4, 100.0),
        (2, 1, -100.0),
        (2, 2, 40.0),
        (2, 3, 50.0),
        (2, 4, 0.0)]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

w = Window.partitionBy('ndc11').orderBy('period')

positive_value_df = df.withColumn('value', F.when(F.col('value') < 0, F.lit('null')).otherwise(F.lag('value').over(w)))

positive_value_df.show()

w = Window.partitionBy('ndc11').orderBy('period').rowsBetween(-sys.maxsize, -1)
not_negative_df = df.withColumn('new_value', F.when(F.col('value') > 0, F.col('value')))
prior_value_df = not_negative_df.withColumn('new_value', F.when(F.col('new_value').isNull(), F.last('new_value', True).over(w)).otherwise(F.col('new_value')))
not_null_df = prior_value_df.withColumn('new_value', F.when(F.col('new_value').isNull(), 0).otherwise(F.col('new_value')))




not_null_df = not_null_df.withColumn('origin_value', F.when(F.col('value') != F.col('new_value'), F.col('value')))
# not_null_df = not_null_df.withColumn('cf_period', F.when(F.col('value') != F.col('new_value'), F.col('period')))


not_null_df = not_null_df.withColumn('cf_period', F.when(F.col('value') > 0, F.col('period')))
condition = (F.col('value') != F.col('new_value'))
not_null_df = not_null_df.withColumn('cf_period', F.when(condition & F.col('cf_period').isNull(), F.last('cf_period', True).over(w)))
# not_null_df = prior_value_df.withColumn('new_value', F.when(F.col('new_value').isNull(), 0).otherwise(F.col('new_value')))

not_null_df = not_null_df.withColumn('comment', F.when(F.col('cf_period').isNull(), F.lit('-'))
                                     .otherwise(F.concat(F.lit('Negative value excepton, carry forward from '), F.col('cf_period'), F.lit(' period'))))
not_null_df.show()
