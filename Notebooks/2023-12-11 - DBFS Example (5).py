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

# MAGIC %md
# MAGIC # Partições 
# MAGIC
# MAGIC
# MAGIC ####### Estatico precisamos espeificar o valor da coluna de partição em cada instrução carregada = PARTITION(country="BR")
# MAGIC
# MAGIC ####### Dinamico não precisamos especificar o valor da coluna da partição = PARTITION(country)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET hive.exec.dynamic.partition = true; 
# MAGIC SET hive.exec.dynamic.partition.mode = nonstrict;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE usuariosPart(
# MAGIC   idade INT,
# MAGIC   estado STRING,
# MAGIC   salario FLOAT  
# MAGIC )
# MAGIC ROW FORMAT DELIMITED 
# MAGIC FIELDS TERMINATED BY ','
# MAGIC LINES TERMINATED BY '\n'
# MAGIC STORED AS textfile
# MAGIC PARTITIONED BY (ano int)
# MAGIC LOCATION '/FileStore/tables/aula-databricks/usuariosPart'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO usuariosPart VALUES(25,'SP', 5000, 2021)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from usuariosPart

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO usuariosPart 
# MAGIC   PARTITION (ano=2020)
# MAGIC     VALUES (30, 'SP', 6000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from usuariosPart

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC # Carregando Dados
# MAGIC #### Red Wine

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS tabela_vinhos

# COMMAND ----------

# MAGIC %sql
# MAGIC USE tabela_vinhos

# COMMAND ----------

dbutils.fs.head('FileStore/tables/aula-databricks/vinhos/winequality-red.csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE red_wine(
# MAGIC   fixed_acidity float,
# MAGIC   volatile_acidity float,
# MAGIC   citric_acid float,
# MAGIC   residual_sugar float,
# MAGIC   chlorides float, 
# MAGIC   free_sulfur_dioxide int,
# MAGIC   total_sulfur_dioxide float, 
# MAGIC   density float,
# MAGIC   pH float, 
# MAGIC   sulphates float,
# MAGIC   alcohol float,
# MAGIC   quality float
# MAGIC )
# MAGIC   USING CSV
# MAGIC   OPTIONS (
# MAGIC     path '/FileStore/tables/aula-databricks/vinhos/winequality-red.csv',
# MAGIC     header 'true',
# MAGIC     delimiter ';'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM red_wine

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando Dados
# MAGIC #### White Wine

# COMMAND ----------

dbutils.fs.head('FileStore/tables/aula-databricks/vinhos/winequality-white.csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE white_wine(
# MAGIC   fixed_acidity float,
# MAGIC   volatile_acidity float,
# MAGIC   citric_acid float,
# MAGIC   residual_sugar float,
# MAGIC   chlorides float, 
# MAGIC   free_sulfur_dioxide int,
# MAGIC   total_sulfur_dioxide float, 
# MAGIC   density float,
# MAGIC   pH float, 
# MAGIC   sulphates float,
# MAGIC   alcohol float,
# MAGIC   quality float
# MAGIC )
# MAGIC   USING CSV
# MAGIC   OPTIONS (
# MAGIC     path '/FileStore/tables/aula-databricks/vinhos/winequality-white.csv',
# MAGIC     header 'true',
# MAGIC     delimiter ';'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM white_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE red_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(quality)
# MAGIC from red_wine
# MAGIC ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quality, COUNT(quality) AS freq
# MAGIC From red_wine
# MAGIC GROUP BY quality
# MAGIC ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quality, min(pH) AS pH_minimo, max(pH) AS pH_maximo
# MAGIC from red_wine
# MAGIC GROUP BY quality
# MAGIC ORDER BY quality DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # Comparar os vinhos
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE white_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT (quality)
# MAGIC   FROM white_wine
# MAGIC     ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quality, COUNT (quality) AS freq
# MAGIC   FROM white_wine
# MAGIC     GROUP BY quality
# MAGIC       ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quality, MIN (pH) AS pH_mimino, MAX (pH) AS pH_maximo    
# MAGIC   FROM white_wine
# MAGIC     GROUP BY quality
# MAGIC       ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT quality
# MAGIC FROM (
# MAGIC     SELECT quality FROM white_wine
# MAGIC     UNION
# MAGIC     SELECT quality FROM red_wine
# MAGIC ) AS combined_wines
# MAGIC ORDER BY quality DESC;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     (SELECT quality FROM white_wine LIMIT 1) AS white_wine_quality,
# MAGIC     (SELECT quality FROM red_wine LIMIT 1) AS red_wine_quality
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Juntando os Dados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE new_red_wine 
# MAGIC   AS SELECT *, 'red' AS wine_type
# MAGIC     FROM red_wine
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM new_red_wine
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE new_white_wine 
# MAGIC   AS select *, 'white' AS wine_type
# MAGIC     FROM white_wine
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM new_white_wine
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE combined_all_wines
# MAGIC AS SELECT *
# MAGIC From new_red_wine
# MAGIC UNION ALL SELECT * 
# MAGIC FROM new_white_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT wine_type, AVG (pH) AS pH_medio    
# MAGIC   FROM combined_all_wines
# MAGIC     GROUP BY wine_type
# MAGIC       ORDER BY wine_type
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Apache Spark

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

tabela = spark.table('tabela_vinhos.combined_all_wines')

# COMMAND ----------

tabela

# COMMAND ----------

tabela.show()

# COMMAND ----------

display(tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL com Spark
# MAGIC
# MAGIC ######spark.sql('query').show
# MAGIC
# MAGIC ou
# MAGIC
# MAGIC ######display(spark.sql('query'))
# MAGIC
# MAGIC se quisermos pular linhas na query teremos que utilizar 3 aspas simples: 
# MAGIC spark.sql('''
# MAGIC query
# MAGIC ''').show
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql('''
        SELECT DISTINCT (quality)
        FROM combined_all_wines
        ORDER BY quality DESC
        ''').show()

# COMMAND ----------

spark.sql('SELECT AVG (pH) FROM combined_all_wines').show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Registrando uma tabela

# COMMAND ----------

resultado = spark.sql('''
                      SELECT * FROM combined_all_Wines
                      WHERE pH < 3
                      ''') 

# COMMAND ----------

type(resultado)

resultado.createOrReplaceTempView('nova_tabela')

# COMMAND ----------

spark.sql('''
          SELECT quality, COUNT (quality) AS freq
          FROM nova_tabela
            GROUP BY quality
            ''').show()

# COMMAND ----------


