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
