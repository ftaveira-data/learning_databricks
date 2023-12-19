# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

dbutils.fs.rm("user/hive/warehouse/data_csv", recurse=True)

# File location and type
file_location = "/FileStore/tables/data.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# Create a view or table

#temp_table_name = "data_csv"

#df.write.format("parquet").saveAsTable(temp_table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Utilities
# MAGIC ### Comandos Databricks Utilities - dbutils

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.data.help("summarize")

# COMMAND ----------

# MAGIC %md
# MAGIC # Manipulando Arquivos 
# MAGIC ### Listar todos os arquivos dentro de uma pasta

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

for item in dbutils.fs.ls('/'):
    print(item.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Acessando os arquivos carregados no DBFS

# COMMAND ----------

dbutils.fs.ls('/FileStore/')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Listando as primeiras linhas de um arquivo
# MAGIC #####posso ver a estrutura do arquivo

# COMMAND ----------

dbutils.fs.head('/FileStore/tables/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # Removendo Arquivos

# COMMAND ----------

#dbutils.fs.rm('/FileStore/tables/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Datasets

# COMMAND ----------

for item in dbutils.fs.ls('/'): print(item.path)


# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))


# COMMAND ----------

display(dbutils.fs.ls('databricks-datasets/wine-quality'))

# COMMAND ----------

display(dbutils.fs.ls('databricks-datasets/wine-quality/README.md'))

# COMMAND ----------

dbutils.fs.head('databricks-datasets/wine-quality/README.md')


# COMMAND ----------

dbutils.fs.head('databricks-datasets/wine-quality/winequality-red.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # Diretórios e Arquivos

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/aula-databricks/vinhos')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks/vinhos'))

# COMMAND ----------

dbutils.fs.help('cp')

# COMMAND ----------

dbutils.fs.cp('/databricks-datasets/wine-quality', '/FileStore/tables/aula-databricks', recurse=True)

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/aula-databricks')

# COMMAND ----------

dbutils.fs.help('mv')

# COMMAND ----------

dbutils.fs.mv('/FileStore/tables/aula-databricks/', '/FileStore/tables/aula-databricks/vinhos/', recurse=True)

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks'))

# COMMAND ----------

for item in dbutils.fs.ls('/FileStore/tables/aula-databricks'):
    if item.size!=0:
        dbutils.fs.mv(f'/FileStore/tables/aula-databricks/{item.name}',
                      '/FileStore/tables/aula-databricks/vinhos/')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks/vinhos'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Usando SQL no Databricks  
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando um database
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS teste

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando uma tabela 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE usuarios(
# MAGIC   idade int, 
# MAGIC   estado STRING,
# MAGIC   salario FLOAT
# MAGIC )
# MAGIC   ROW FORMAT DELIMITED
# MAGIC   FIELDS TERMINATED BY ','
# MAGIC   LINES TERMINATED BY '\n'
# MAGIC   STORED AS textfile
# MAGIC   LOCATION '/FileStore/tables/aula-databricks/usuarios'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO usuarios VALUES (25, 'SP', 5000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM usuarios

# COMMAND ----------


