# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8e1b3a9c-2bea-4f81-9374-ec01df13b82c",
# META       "default_lakehouse_name": "silver_layer",
# META       "default_lakehouse_workspace_id": "d6a29a8e-c8ea-40d4-8da0-82f3ac2cebe0",
# META       "known_lakehouses": [
# META         {
# META           "id": "8e1b3a9c-2bea-4f81-9374-ec01df13b82c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Chama funções que serão utilizadas em todo o notebook
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import when, col 
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parquet_path = "Files/data_from_bronze_layer"
df = spark.read.parquet(parquet_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Carregar uma Delta Table
df_table = spark.read.format("delta").load("Tables/house_price_history")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show(5) 
df.printSchema()
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_table.show(5) 
df_table.printSchema()
df_table.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select('PPD Category Type').distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lista de colunas do dataframe
columns = df.columns

# Calcula quantidade de nulos/vazios
summary = []
for column in columns:
    null_count = df.filter(col(column).isNull()).count()
    empty_count = df.filter(col(column) == '').count()
    summary.append((column, null_count, empty_count))

# Cria dataframe para receber resumo de nulos/vazios
summary_df = spark.createDataFrame(summary, ["Column", "Null_Count", "Empty_Count"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mostra resumo dos nulos/vazios
summary_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Substituir vazios/nulos
df = df.fillna('Unknown')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calcular quantidade de campos vazios/nulos
summary = []
for column in columns:
    null_count = df.filter(col(column).isNull()).count()
    empty_count = df.filter(col(column) == '').count()
    summary.append((column, null_count, empty_count))

# Cria dataframe para receber quantidades de nulos/vazios
summary_df = spark.createDataFrame(summary, ["Column", "Null_Count", "Empty_Count"])

# Mostra quantidades
summary_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Incluí nova coluna de dimensão 'Description_Old/New'
df = df.withColumn(
    "Description_Old_New",
    when(col("Old/New") == 'Y', "New")
    .when(col("Old/New") == 'N', "Old")
    .otherwise(None)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Incluí nova coluna de dimensão 'Description_Property Type'
df = df.withColumn(
    "Description_Property_Type",
    when(col("Property Type") == 'D', "Detached")
    .when(col("Property Type") == 'S', "Semi-Detached")
    .when(col("Property Type") == 'T', "Terraced")
    .when(col("Property Type") == 'F', "Flats/Maisonettes")
    .when(col("Property Type") == 'O', "Other")
    .otherwise(None)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assuming 'df' is your DataFrame and 'column_name' is the column you want unique values from

duration_values = df.select("Duration").distinct()

# Show the unique values
duration_values.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Incluí nova coluna de dimensão 'Description_Duration'
df = df.withColumn(
    "Description_Duration",
    when(col("Duration") == 'F', "Freehold")
    .when(col("Duration") == 'L', "Leasehold")
    .when(col("Duration") == 'U', "Unspecified")
    .otherwise(None)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_geography = df.select('County', 'District', 'Town/City').distinct()
df_dim_geography.show(10)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_geography.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_geography = df_dim_geography.dropDuplicates(["County", "District", "Town/City"])
df_dim_geography.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_geography = df_dim_geography.withColumn("geography_id", monotonically_increasing_id())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_dim_address = df.select('County', 'District', 'Town/City', 'Locality', 'Street').distinct()
#df_dim_address.show(10)
#df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_dim_address = df_dim_address.withColumn("address_id", monotonically_increasing_id())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_dim_address.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_dim_address = df_dim_address.dropDuplicates(['County', 'District', 'Town/City', 'Locality', 'Street'])
#df_dim_address.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_dim_address.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select('PPD Category Type').distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Incluí nova coluna de dimensão 'Description_PPD_Category_Type'
df = df.withColumn(
    "Description_PPD_Category_Type",
    when(col("PPD Category Type") == 'A', "Standard Price Paid")
    .when(col("PPD Category Type") == 'B', "Additional Price Paid")
    .otherwise(None)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select('Record Status').distinct().show()
df.select('Record Status').count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Incluí nova coluna de dimensão 'Description_Record Status'

df = df.withColumn(
    "Description_Record_Status",
    when(col("Record Status") == 'A', "Addition")
    .when(col("Record Status") == 'C', "Change")
    .when(col("Record Status") == 'D', "Delete")
    .otherwise(None)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inclui coluna geography_id no dataframe principal e cria novo dataframe
df_fact_complete = df.join(
    df_dim_geography.select('County', 'District', 'Town/City', 'geography_id'), 
    on=['County', 'District', 'Town/City'], 
    how="left"
)
df_fact_complete.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inclui coluna address_id no dataframe principal 
#df_fact_complete = df_fact_complete.join(
#    df_dim_address.select('County', 'District', 'Town/City', 'Locality', 'Street', 'address_id'), 
#    on=['County', 'District', 'Town/City', 'Locality', 'Street'], 
#    how="left"
#)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Verifica colunas e conteúdo do dataframe
df_fact_complete.show(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Cria dataframe com as cidades distintas presentes no dataframe original
df_dim_town_city = df.select('Town/City').distinct()
df_dim_town_city.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Cria código identificador para cada cidade
df_dim_town_city = df_dim_town_city.withColumn("TownCity_id", monotonically_increasing_id())
df_dim_town_city.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inclui coluna TownCity_id no dataframe principal e cria novo dataframe 
df_fact_complete = df_fact_complete.join(
    df_dim_town_city.select('Town/City', 'TownCity_id'), 
    on=['Town/City'], 
    how="left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Verifica códigos criados
df_fact_complete.select('Town/City', 'TownCity_id').distinct().show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lista de colunas do dataframe
columns = df_fact_complete.columns

# Calcula quantidade de nulos/vazios
summary = []
for column in columns:
    null_count = df_fact_complete.filter(col(column).isNull()).count()
    empty_count = df_fact_complete.filter(col(column) == '').count()
    summary.append((column, null_count, empty_count))

# Cria dataframe para receber resumo de nulos/vazios
summary_df_fact_complete = spark.createDataFrame(summary, ["Column", "Null_Count", "Empty_Count"])

summary_df_fact_complete.show(30)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Converter a coluna 'Date of Transfer' de timestamp para date
df_fact_complete = df_fact_complete.withColumn('Date of Transfer', F.to_date('Date of Transfer'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_complete.select('Date of Transfer').show(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_complete.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

old_col_names = ['Town/City', 'County', 'District', 'Transaction unique identifier', 'Price',
'Date of Transfer', 'Postcode', 'Property Type', 'Old/New', 'Duration', 'PAON', 'SAON',
'Street', 'Locality', 'PPD Category Type', 'Record Status', 'Description_Old_New',
'Description_Property_Type', 'Description_Duration', 'Description_PPD_Category_Type',
'Description_Record_Status', 'geography_id', 'TownCity_id']

new_col_names = ['Town_City', 'County', 'District', 'transaction_id', 'Price',
'Date', 'Postcode', 'Property_Type_id', 'Old_New_id', 'Duration_id', 'PAON', 'SAON',
'Street', 'Locality', 'PPD_Category_Type_id', 'Record_Status_id', 'Description_Old_New',
'Description_Property_Type', 'Description_Duration', 'Description_PPD_Category_Type',
'Description_Record_Status', 'geography_id', 'TownCity_id']
# Creating the dictionary
rename_dict = dict(zip(old_col_names, new_col_names))

# Apply the renaming
for old_name, new_name in rename_dict.items():
    df_fact_complete = df_fact_complete.withColumnRenamed(old_name, new_name)



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

df_fact_complete.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Exclui colunas desnecessárias
df_dim_fact = df_fact_complete.drop('Locality', 'Street', 'PAON', 'SAON',
'address_id', 'Postcode', 'PPD_Category_Type_id', 'Record_Status_id',
'Description_PPD_Category_Type', 'Description_Record_Status')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_fact.show(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Criar dataframes de dimensão df_dim_property_type
df_dim_property_type = df_dim_fact \
.select('Property_Type_id', 'Description_Property_Type').distinct()
df_dim_property_type.show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Substitui 'Old' por 'Used' na coluna 'Description_Old_New'
df_dim_fact = df_dim_fact.withColumn(
    'Description_Old_New',
    F.when(F.col('Description_Old_New') == 'Old', 'Used').otherwise(F.col('Description_Old_New'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_old_new = df_dim_fact.select('Description_Old_New', 'Old_New_id').distinct()
df_dim_old_new.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_duration = df_dim_fact.select('Description_Duration', 'Duration_id').distinct()
df_dim_duration.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Excluir colunas descritivas do dataframe de dimensões e fatos e manter apenas
#códigos e fatos criando um novo dataframe de fatos
df_fact = df_dim_fact.select('geography_id', 'TownCity_id', 'Property_Type_id',
'Old_New_id', 'Duration_id', 'Date', 'transaction_id',
'Price')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_date_hierarchy = df_fact.select('Date').distinct().orderBy('Date')
# Adicionar colunas de hierarquia
df_dim_date_hierarchy = df_dim_date_hierarchy \
    .withColumn("Year", year(col("date"))) \
    .withColumn("Quarter", expr("concat('Q', quarter(date))")) \
    .withColumn("Month", date_format(col("date"), "MMMM")) \
    .withColumn("Month_Number", month(col("date"))) \
    .withColumn("Day", dayofmonth(col("date"))) \
    .withColumn("Day_of_Week", date_format(col("date"), "EEEE"))
df_dim_date_hierarchy.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_geography = df_dim_geography.withColumnRenamed("Town/City", "Town_City")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Salva arquivo com dimensão 'geography' que é a concatenação de 
#County/District/TownCity
df_dim_geography.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('Files/dim_geography')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Salva arquivo com dimensão 'Duration'
df_dim_duration.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('Files/dim_duration')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Salva arquivo com dimensão 'Old/New'
df_dim_old_new.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('Files/dim_old_new')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Salva arquivo com dimensão 'dim_property_type'
df_dim_property_type.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('Files/dim_property_type')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_town_city = df_dim_town_city.withColumnRenamed("Town/City", "Town_City")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_town_city.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#Salva arquivo com dimensão 'Town_City'
df_dim_town_city.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('Files/dim_town_city')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Cria dataframe de fatos, porém com agregação para redução da quantidade de dados
#para efeito de testes de desempenho no powerbi. Caso a tabela completa seja muito
#pesada, utilizarei apenas os dados agregados
df_fact_agg = df_fact.groupBy('geography_id','TownCity_id', 'Old_New_id', 
    'Date').agg(
    F.sum("Price").alias("total_value"),
    F.count("transaction_id").alias("transactions_count"),
    F.avg("Price").alias("avg_price")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_agg.show(2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Salva arquivo com fatos completo 'df_fact'
df_fact.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('Files/price_fact')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Salva arquivo com fatos agregados 'df_fact_agg'
df_fact_agg.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('Files/price_fact_agg')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
