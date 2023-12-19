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


