from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# spark session
spark = SparkSession.builder.appName('Task7').getOrCreate()

# columns for input df
input_df_columns = ['ndc11', 'quarter',	'dir_sls_merch_middle_quan', 'dir_sls_grs_ttl_quan',
                    'dir_sls_grs_cost',	'dir_sls_grs_quan',	'dir_sls_inelig_non_us_cost', 'dir_sls_inelig_non_us_quan',
                    'dir_sls_inelig_cot_cost', 'dir_sls_inelig_cot_quan', 'dir_sls_inelig_con_cost',
                    'dir_sls_inelig_con_quan', 'dir_sls_free_gds_quan',	'dir_sls_adj_cost',	'dir_sls_adj_quan',
                    'indir_sls_inelig_con_cost', 'indir_sls_inelig_con_quan', 'indir_sls_inelig_cot_cost',
                    'indir_sls_inelig_cot_quan', 'tricare_sales_cost',	'tricare_sales_quan',
                    'net_dir_sls_whl_elig_ttl_quan', 'non_whl_rbts_elig_cost',	'retail_cppd_cost',
                    'whl_cppd_cost', 'whl_fees_cost','cbks_elig_cost', 'pac_elig_cost']

# data for input df
input_df_data = [
    ('1', '2018q1',	float(45),	float(540), float(3560), float(3200), float(510), float(500), float(490), float(480),
     float(470), float(460), float(450), float(440), float(430), float(420), float(410), float(400), float(390),
     float(380), float(370), float(810), float(350), float(340), float(330), float(320), float(310), float(300),),
    ('1', '2018q2',	float(55),	float(450), float(3560), float(3200), float(510), float(500), float(490), float(480),
     float(470), float(460), float(450), float(440), float(430), float(420), float(410), float(150), float(50),
     float(210), float(45), float(810), float(350), float(340), float(330), float(320), float(310), float(300),),
    ('1', '2018q3',	float(55),	float(450), float(1200), float(800), float(510), float(500), float(490), float(480),
     float(470), float(460), float(450), float(440), float(430), float(420), float(410), float(150), float(50),
     float(210), float(45), float(810), float(350), float(340), float(330), float(320), float(310), float(300),),
]
# create input df
input_df = spark.createDataFrame(input_df_data, input_df_columns)
input_df.show()

# create input df
input_df = spark.createDataFrame(input_df_data, input_df_columns)
input_df = input_df.withColumn('nfamp_ratio', F.round(F.col('dir_sls_merch_middle_quan') / F.col('dir_sls_grs_ttl_quan'), 9))
input_df = input_df.withColumn('nfamp_method', F.when(F.col('nfamp_ratio') > 0.1, 'WHL').otherwise('DIR'))
input_df = input_df.withColumn('dir_sls_elig_ttl_cost', (F.col('dir_sls_grs_cost') - F.col('dir_sls_inelig_non_us_cost') - F.col('dir_sls_inelig_cot_cost') - F.col('dir_sls_inelig_con_cost')))
input_df = input_df.withColumn('dir_sls_elig_ttl_quan', (F.col('dir_sls_grs_quan') - F.col('dir_sls_inelig_non_us_quan') - F.col('dir_sls_inelig_cot_quan') - F.col('dir_sls_inelig_con_quan') - F.col('dir_sls_free_gds_quan')))
input_df = input_df.withColumn('net_dir_sls_elig_ttl_cost', F.col('dir_sls_elig_ttl_cost') + F.col('dir_sls_adj_cost'))
input_df = input_df.withColumn('net_dir_sls_elig_ttl_quan', F.col('dir_sls_elig_ttl_quan') + F.col('dir_sls_adj_quan'))
input_df = input_df.withColumn('elig_sls_ttl_cost', F.when(F.col('nfamp_method') == 'WHL', F.col('net_dir_sls_elig_ttl_cost') - F.col('indir_sls_inelig_con_cost') - F.col('indir_sls_inelig_cot_cost') - F.col('tricare_sales_cost')).otherwise(F.col('net_dir_sls_elig_ttl_cost') - F.col('tricare_sales_cost')))
input_df = input_df.withColumn('elig_sls_ttl_quan', F.when(F.col('nfamp_method') == 'WHL', F.col('net_dir_sls_elig_ttl_quan') - F.col('indir_sls_inelig_con_quan') - F.col('indir_sls_inelig_cot_quan') - F.col('tricare_sales_quan')).otherwise(F.col('net_dir_sls_elig_ttl_quan') - F.col('tricare_sales_quan')))
input_df = input_df.withColumn('indir_sls_inelig_ttl_quan', F.when(F.col('nfamp_method') == 'WHL', F.col('indir_sls_inelig_con_quan') + F.col('indir_sls_inelig_cot_quan') + F.col('tricare_sales_quan')))
input_df = input_df.withColumn('indir_sls_inelig_ttl_ratio', F.when(F.col('nfamp_method') == 'WHL', F.round(F.col('indir_sls_inelig_ttl_quan')/F.col('net_dir_sls_whl_elig_ttl_quan'), 8)))
input_df = input_df.withColumn('indir_sls_inelig_ttl_ratio_inv', F.when(F.col('nfamp_method') == 'WHL', F.round(1 - F.col('indir_sls_inelig_ttl_ratio'), 8)))
input_df = input_df.withColumn('prorated_whl_fees_cost', F.when(F.col('nfamp_method') == 'WHL', F.col('whl_fees_cost') * F.col('indir_sls_inelig_ttl_ratio_inv')))
input_df = input_df.withColumn('prorated_whl_cppd_cost', F.when(F.col('nfamp_method') == 'WHL', F.col('whl_cppd_cost') * F.col('indir_sls_inelig_ttl_ratio_inv')))
input_df = input_df.withColumn('net_elig_sls_cost', F.when(F.col('nfamp_method') == 'DIR', F.col('elig_sls_ttl_cost') - F.col('retail_cppd_cost') - F.col('non_whl_rbts_elig_cost') + F.col('pac_elig_cost')).otherwise(F.col('elig_sls_ttl_cost') - F.col('prorated_whl_fees_cost') - F.col('prorated_whl_cppd_cost') - F.col('cbks_elig_cost') + F.col('pac_elig_cost')))
input_df = input_df.withColumn('net_elig_sls_quan', F.col('elig_sls_ttl_quan'))
input_df = input_df.withColumn('nfamp_calculated', F.round(F.col('net_elig_sls_cost')/ F.col('net_elig_sls_quan'), 8))
input_df = input_df.withColumn('nfamp_reported', F.when( F.col('nfamp_calculated') >= 1, F.round(F.round(F.col('nfamp_calculated'), 3), 2)).otherwise(F.floor(F.col('nfamp_calculated'))))
cond = F.col('net_elig_sls_cost') < 0
cond1 = F.col('net_elig_sls_quan') < 0
input_df = input_df.withColumn('is_false_positive', F.when(cond & cond1,'Y').otherwise('N'))

input_df.show()
