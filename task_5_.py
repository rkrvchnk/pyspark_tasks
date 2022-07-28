from routins_func import smoothing_period
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql import functions as F
from datetime import datetime


class ProductView:
    def __init__(self, product_df, start, end, months, date, product_start, product_end):
        self.product_df = product_df
        self.start = start
        self.end = end
        self.months = months
        self.date = date
        self.product_start = product_start
        self.product_end = product_end

    def last_month_of_quarter_for_adj_year(Column) -> Column:
        adj_year = F.add_months(column, 12)
        return F.concat(F.year(adj_year), F.lit('-'), F.lpad(F.quarter(adj_year) * 3, 2, "0"), F.lit('-'), F.lit('01'))