# Databricks notebook source
# ContainerName = "staging"
# azure_blobstorage_name = "dlcsv4891"
# mountpointname = "/mnt/staging"
# secret_key ="secret stuff" # This is supposed to be a relatively simple demonstration, so I'm not using Azure Key Vault so I can make it all cheaper and less complex.

# COMMAND ----------

# dbutils.fs.unmount("/mnt/staging")

# COMMAND ----------

# dbutils.fs.mount(source = f"wasbs://{ContainerName}@{azure_blobstorage_name}.blob.core.windows.net",mount_point = mountpointname ,extra_configs = {"fs.azure.account.key."+azure_blobstorage_name+".blob.core.windows.net":secret_key})

# COMMAND ----------

dbutils.fs.ls('/mnt/staging')

# COMMAND ----------

# ContainerName = "bronze"
# mountpointname = "/mnt/bronze"

# COMMAND ----------

# dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

# dbutils.fs.mount(source = f"wasbs://{ContainerName}@{azure_blobstorage_name}.blob.core.windows.net",mount_point = mountpointname ,extra_configs = {"fs.azure.account.key."+azure_blobstorage_name+".blob.core.windows.net":secret_key})

# COMMAND ----------

# dbutils.fs.unmount("/mnt/silver")

# COMMAND ----------

# ContainerName = "silver"
# mountpointname = "/mnt/silver"

# COMMAND ----------

# dbutils.fs.mount(source = f"wasbs://{ContainerName}@{azure_blobstorage_name}.blob.core.windows.net",mount_point = mountpointname ,extra_configs = {"fs.azure.account.key."+azure_blobstorage_name+".blob.core.windows.net":secret_key})

# COMMAND ----------

# MAGIC %pip install py7zr

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit
import os
import py7zr
import re
import unicodedata

# COMMAND ----------

# Define the path to the folder containing the 7z files
folder_path = "/dbfs/mnt/staging/"

# List all files in the folder
files = os.listdir(folder_path)

# Iterate through each file and decompress it
for file in files:
    if file.endswith(".7z"):
        file_path = os.path.join(folder_path, file)
        with py7zr.SevenZipFile(file_path, mode='r') as z:
            z.extractall(path=folder_path)

# COMMAND ----------

schema = StructType([
    StructField("Bairros SP", StringType(), True),
    StructField("Bairros Fortaleza", StringType(), True),
    StructField("Bairros RJ", StringType(), True),
    StructField("Causa Afastamento 1", IntegerType(), True),
    StructField("Causa Afastamento 2", IntegerType(), True),
    StructField("Causa Afastamento 3", IntegerType(), True),
    StructField("Motivo Desligamento", IntegerType(), True),
    StructField("CBO Ocupação 2002", StringType(), True),
    StructField("CNAE 2.0 Classe", IntegerType(), True),
    StructField("CNAE 95 Classe", IntegerType(), True),
    StructField("Distritos SP", StringType(), True),
    StructField("Vínculo Ativo 31/12", IntegerType(), True),
    StructField("Faixa Etária", IntegerType(), True),
    StructField("Faixa Hora Contrat", IntegerType(), True),
    StructField("Faixa Remun Dezem (SM)", IntegerType(), True),
    StructField("Faixa Remun Média (SM)", IntegerType(), True),
    StructField("Faixa Tempo Emprego", IntegerType(), True),
    StructField("Escolaridade após 2005", DoubleType(), True),
    StructField("Qtd Hora Contr", DoubleType(), True),
    StructField("Idade", DoubleType(), True),
    StructField("Ind CEI Vinculado", IntegerType(), True),
    StructField("Ind Simples", IntegerType(), True),
    StructField("Mês Admissão", IntegerType(), True),
    StructField("Mês Desligamento", StringType(), True),
    StructField("Mun Trab", IntegerType(), True),
    StructField("Município", IntegerType(), True),
    StructField("Nacionalidade", IntegerType(), True),
    StructField("Natureza Jurídica", IntegerType(), True),
    StructField("Ind Portador Defic", IntegerType(), True),
    StructField("Qtd Dias Afastamento", DoubleType(), True),
    StructField("Raça Cor", IntegerType(), True),
    StructField("Regiões Adm DF", IntegerType(), True),
    StructField("Vl Remun Dezembro Nom", StringType(), True),
    StructField("Vl Remun Dezembro (SM)", StringType(), True),
    StructField("Vl Remun Média Nom", StringType(), True),
    StructField("Vl Remun Média (SM)", StringType(), True),
    StructField("CNAE 2.0 Subclasse", IntegerType(), True),
    StructField("Sexo Trabalhador", DoubleType(), True),
    StructField("Tamanho Estabelecimento", IntegerType(), True),
    StructField("Tempo Emprego", StringType(), True),
    StructField("Tipo Admissão", IntegerType(), True),
    StructField("Tipo Estab41", IntegerType(), True),
    StructField("Tipo Estab42", StringType(), True),
    StructField("Tipo Defic", IntegerType(), True),
    StructField("Tipo Vínculo", IntegerType(), True),
    StructField("IBGE Subsetor", IntegerType(), True),
    StructField("Vl Rem Janeiro SC", StringType(), True),
    StructField("Vl Rem Fevereiro SC", StringType(), True),
    StructField("Vl Rem Março SC", StringType(), True),
    StructField("Vl Rem Abril SC", StringType(), True),
    StructField("Vl Rem Maio SC", StringType(), True),
    StructField("Vl Rem Junho SC", StringType(), True),
    StructField("Vl Rem Julho SC", StringType(), True),
    StructField("Vl Rem Agosto SC", StringType(), True),
    StructField("Vl Rem Setembro SC", StringType(), True),
    StructField("Vl Rem Outubro SC", StringType(), True),
    StructField("Vl Rem Novembro SC", StringType(), True),
    StructField("Ano Chegada Brasil", IntegerType(), True),
    StructField("Ind Trab Intermitente", IntegerType(), True),
    StructField("Ind Trab Parcial", IntegerType(), True),
])

# COMMAND ----------

# List all files in the folder
files = os.listdir(folder_path)

# Filter only the .txt files
txt_files = [file for file in files if file.endswith(".txt")]

# COMMAND ----------

folder_path = "/mnt/staging/"

# Read each .txt file into separate DataFrames

encoding = "latin1"
separator = ";"

dataframes = []
for txt_file in txt_files:
    file_path = os.path.join(folder_path, txt_file)
    df_txt = spark.read.option("header", "true").option("sep", separator).option("encoding", encoding).schema(schema).csv(file_path)
    dataframes.append(df_txt)

# Combine all DataFrames into one
df_combined = dataframes[0]
for df in dataframes[1:]:
    df_combined = df_combined.union(df)

df_combined.display()

# COMMAND ----------

# Define a function to remove diacritic marks from characters
def remove_diacritics(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

# Define a function to convert column names to snake case and remove diacritic marks
def to_snake_case(column_name):
    # Remove diacritic marks from the column name
    cleaned_name = remove_diacritics(column_name)
    
    # Replace any non-word characters with underscores
    snake_case_name = re.sub(r'\W+', '_', cleaned_name.lower())
    return snake_case_name

# Get the current column names
current_columns = df_combined.columns

# Create a mapping of current column names to cleaned column names
column_mapping = {column_name: to_snake_case(column_name) for column_name in current_columns}

# Rename the columns with the cleaned names using withColumnRenamed
df_cleaned = df_combined
for old_name, new_name in column_mapping.items():
    df_cleaned = df_cleaned.withColumnRenamed(old_name, new_name)

df_cleaned = df_cleaned.withColumn('ano', lit('2021'))

# Show the DataFrame schema and first 10 rows with cleaned column names

df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.write.csv("/mnt/bronze/RAIS_VINC_PUB_2021_csv", header=True, mode="overwrite")

# COMMAND ----------

directory_path = "/mnt/staging"

# List all files in the directory using dbutils.fs.ls()
files_to_delete = dbutils.fs.ls(directory_path)

# Iterate through the list of files and delete them one by one
for file_info in files_to_delete:
    file_path = file_info.path
    dbutils.fs.rm(file_path)
