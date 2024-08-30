# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3e01b357-4077-45a1-845f-df3fc0582b5b",
# META       "default_lakehouse_name": "bronze_layer_1",
# META       "default_lakehouse_workspace_id": "d6a29a8e-c8ea-40d4-8da0-82f3ac2cebe0",
# META       "known_lakehouses": [
# META         {
# META           "id": "3e01b357-4077-45a1-845f-df3fc0582b5b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import StringType
from pyspark.sql.functions import sum as spark_sum


columns = [
    "Transaction unique identifier",
    "Price",
    "Date of Transfer",
    "Postcode",
    "Property Type",
    "Old/New",
    "Duration",
    "PAON",
    "SAON",
    "Street",
    "Locality",
    "Town/City",
    "District",
    "County",
    "PPD Category Type",
    "Record Status"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path = 'Files/pp_complete_1995_2024'
df = spark.read.csv(file_path, header=False, inferSchema=True)
df.show(5)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Renomear as colunas
df = df.toDF(*columns)

# Obtém uma lista de pares (nome_coluna, tipo_dado)
column_types = df.dtypes

# Exibe os tipos de dados
for column_name, column_type in column_types:
    print(f"Coluna: {column_name}, Tipo: {column_type}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verificar se há valores inválidos na coluna "Date of Transfer"
df.createOrReplaceTempView("data")
invalid_dates_df = spark.sql("""
    SELECT *
    FROM data
    WHERE CAST(`Date of Transfer` AS TIMESTAMP) IS NULL
""")
invalid_dates_count = invalid_dates_df.count()
print(f"Number of invalid timestamps in 'Date of Transfer': {invalid_dates_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verificar se há valores inválidos na coluna "Price"
invalid_prices_df = spark.sql("""
    SELECT *
    FROM data
    WHERE CAST(`Price` AS INT) IS NULL
""")
invalid_prices_count = invalid_prices_df.count()
print(f"Number of invalid integers in 'Price': {invalid_prices_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Criar uma lista com os nomes das colunas de tipo StringType
string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
print(string_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Aplicar trim a todas as colunas de tipo string
df_trimmed = df
for column_name in string_columns:
    df_trimmed = df_trimmed.withColumn(column_name, trim(col(column_name)))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Mostrar DataFrame após aplicar trim
print("DataFrame após aplicar trim em todas as colunas de string:")
df_trimmed.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Salvar arquivo para enviar ao Silver Layer
output_path = "Files/data_for_silver_layer"
df_trimmed.write.mode("overwrite").parquet(output_path)

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
