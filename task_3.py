from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType



class Bucket:
    """
    Create an aggregation from a trx table
    """
    def __init__(self, df: DataFrame, name: str, filters: Column, key: list, agg: list):
        """
        :param df: trx table
        :param filters: filter condition, e.g. 'trx_table['`bu_dir.state`'] == 'USA''
        :param name: string, 'name' of a bucket, will be used as a prefix e.g. 'dir_sls_'
        :param key: aggregation keys, e.g. ['ndc11', 'month']
        :param agg: aggregation columns, e.g. [(F.col('invoice_amount'), 'cost')]
        :param prefix: a string to be added to column names, e.g. 'dir_sls_'
        """
        self.df = df
        self.filters = filters
        self.key = key
        self.agg = agg

    def create_bucket(self):
        bucket = self.df.filter(self.filters)
        bucket = bucket.groupBy(*self.key).agg(*self.get_aggregations())
        return bucket

    def get_aggregations(self):
        agg = []
        for a in self.agg:
            agg.append(F.sum((a[0])).alias(a[-1]))
        return agg


class Unbucketed:
    """
    Create a table with transactions, which are not included in any aggregations
    """
    def __init__(self, df: DataFrame, filters: Column):
        """
        :param df: trx dataframe
        :param filters: filter conditions, e.g. df['`bu_dir.state`'] != 'USA'
        """
        self.df = df
        self.filters = filters

    def create_unbucketed(self):

        unbucketed_df = self.df.filter(self.filters)

        return unbucketed_df


class JoinBuckets:
    """
    Join buckets with unique names
    """

    def __init__(self, buckets: list, key: list, how: str, prefix: list = None):
        """
        :param buckets: a list of buckets
        :param key: a list of column names
        :param how: a string to indicate a type of joining
        :param prefix: a string to make unique column names
        """
        self.buckets = buckets
        self.key = key
        self.how = how
        self.prefix = prefix

    @staticmethod
    def add_prefix(df: DataFrame, prefix) -> DataFrame:
        """add prefix to DataFrame columns"""
        columns = [col if col in ('month', 'ndc11') else prefix + col for col in df.columns]
        return df.toDF(*columns)


    def join_buckets(self):
        left_bucket = self.add_prefix(self.buckets[0], self.prefix[0])
        for i in range(1, len(self.buckets)):
            joined_buckets = left_bucket.join(self.add_prefix(self.buckets[i], self.prefix[i]), on=self.key, how=self.how)
            left_bucket = joined_buckets

        # should we normalize df here or it's better to create another method? normalize_numerics
        normilized_df = joined_buckets.fillna(value=0)

        return normilized_df



if __name__ == '__main__':
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
    # add a column 'month' to use it for grouping products by month
    trx_table = trx_table.withColumn('month', F.date_format('invoice_date', 'yyyyMM'))

    agg = [(F.col('invoice_amount'), 'cost'), (F.col('invoice_quantity'), 'quan')]

    filter1 = trx_table['`bu_dir.state`'] == 'USA'
    bucket1 = Bucket(trx_table, filter1, ['ndc11', 'month'], agg)
    bucket1 = bucket1.create_bucket()

    filter2 = trx_table['`bu_whl.state`'] != 'USA'
    bucket2 = Bucket(trx_table, filter2, ['ndc11', 'month'], agg)
    bucket2 = bucket2.create_bucket()

    filter_unbucketed = ((trx_table['`bu_dir.state`'] != 'USA') | (trx_table['`bu_dir.state`'].isNull())) &\
                                                                  (trx_table['`bu_whl.state`'] == 'USA')
    unbucketed = Unbucketed(trx_table, filter_unbucketed)
    unbucketed = unbucketed.create_unbucketed()

    buckets = [bucket1, bucket2]
    key = ['month', 'ndc11']
    joined = JoinBuckets(buckets, key, 'full', ['dir_sls_', 'inelig_sls_'])
    joined.join_buckets().show()
