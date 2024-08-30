# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dcad6c99-0753-4b40-bf32-04f937b6a2b0",
# META       "default_lakehouse_name": "gold_layer",
# META       "default_lakehouse_workspace_id": "d6a29a8e-c8ea-40d4-8da0-82f3ac2cebe0",
# META       "known_lakehouses": [
# META         {
# META           "id": "dcad6c99-0753-4b40-bf32-04f937b6a2b0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import when
from pyspark.sql.functions import col, year, quarter, month, dayofmonth, dayofweek, date_format, expr, avg

spark = SparkSession.builder \
    .appName("Parquet File Loader") \
    .getOrCreate()


file_list = ["Tables/dim_geography", "Tables/dim_duration", "Tables/dim_old_new",
"Tables/dim_property_type", "Tables/dim_town_city", "Tables/price_fact", "Tables/price_fact_agg"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_geography = spark.read.parquet(file_list[0])
df_dim_geography.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_geography.filter(df_dim_geography.Town_City=='LONDON').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_duration = spark.read.parquet(file_list[1])
df_dim_duration.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_old_new = spark.read.parquet(file_list[2])
df_dim_old_new.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_property_type = spark.read.parquet(file_list[3])
df_dim_property_type.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_town_city = spark.read.parquet(file_list[4])
df_dim_town_city.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_town_city.filter(df_dim_town_city.Town_City=='LONDON').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_price = spark.read.parquet(file_list[5])
df_fact_price.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_price.dropDuplicates().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_london = df_fact_price.select( 'Date', 'transaction_id', 'TownCity_id', 'Price').filter((df_fact_price.Date>='2010-1-1') & (df_fact_price.Date<='2024-12-31') & (df_fact_price.TownCity_id==156))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_date_hierarchy = df_fact_price.select('Date').distinct().orderBy('Date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_date_hierarchy = df_fact_price.select('Date').distinct().orderBy('Date')
# Adicionar colunas de hierarquia
df_dim_date_hierarchy = df_dim_date_hierarchy \
    .withColumn("Year", year(col("date"))) \
    .withColumn("Quarter", expr("concat('Q', quarter(date))")) \
    .withColumn("Month", date_format(col("date"), "MMMM")) \
    .withColumn("Month_Number", month(col("date"))) \
    .withColumn("Day", dayofmonth(col("date"))) \
    .withColumn("Day_of_Week", date_format(col("date"), "EEEE"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_date_hierarchy.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_london = df_dim_date_hierarchy.join(df_london, df_london.Date == df_dim_date_hierarchy.Date, "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_london.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

avg_london = df_london.groupBy("Year").agg(
    avg("Price").alias("avg_value1")
)

avg_london.orderBy('Year').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_price_agg = spark.read.parquet(file_list[6])
df_fact_price_agg.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_list = ["Tables/dim_geography", "Tables/dim_duration", "Tables/dim_old_new",
"Tables/dim_property_type", "Tables/dim_town_city", "Tables/price_fact", "Tables/price_fact_agg"]

# salvar os dataframes como Delta tables
delta_table_path = "Tables/tables_for_powerbi"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_old_new.write.format("delta").mode("overwrite").save(f"{delta_table_path}/dim_old_new")
df_dim_property_type.write.format("delta").mode("overwrite").save(f"{delta_table_path}/dim_property_type")
df_dim_town_city.write.format("delta").mode("overwrite").save(f"{delta_table_path}/dim_town_city")
df_fact_price.write.format("delta").mode("overwrite").save(f"{delta_table_path}/fact_price")
df_fact_price_agg.write.format("delta").mode("overwrite").save(f"{delta_table_path}/fact_price_agg")
df_dim_duration.write.format("delta").mode("overwrite").save(f"{delta_table_path}/dim_duration")
df_dim_geography.write.format("delta").mode("overwrite").save(f"{delta_table_path}/dim_geography")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Supondo que df seja o DataFrame que você deseja exportar
df_dim_date_hierarchy.write \
    .format('parquet') \
    .mode('overwrite') \
    .save("Files/dim_date_hierarchy")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#import pyspark.pandas as ps
#import pandas as pd
#abfs_path = "abfss://Looplex@onelake.dfs.fabric.microsoft.com/gold_layer.Lakehouse/Files"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_ps_fact_price = df_fact_price.pandas_api()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_pd_fact_price = df_ps_fact_price.to_pandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_pd_fact_price.to_csv(f"{abfs_path}/csv_para_powerbi/price_fact.csv", index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
