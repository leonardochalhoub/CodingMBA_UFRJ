# Databricks notebook source
# Read Parquet files from the first bronze folder
bronze_folder_1 = "/mnt/bronze/RAIS_VINC_PUB_2020"
df_bronze_1 = spark.read.parquet(bronze_folder_1)

# Read Parquet files from the second bronze folder
bronze_folder_2 = "/mnt/bronze/RAIS_VINC_PUB_2021"
df_bronze_2 = spark.read.parquet(bronze_folder_2)

# Combine the two dataframes
combined_df = df_bronze_1.union(df_bronze_2)

# Write the combined dataframe to the silver layer
silver_folder = "/mnt/silver/RAIS_VINC_PUB"
combined_df.write.mode("overwrite").parquet(silver_folder)

# COMMAND ----------

parquet_path = "/mnt/silver/RAIS_VINC_PUB"
df = spark.read.parquet(parquet_path)
df.display()

# COMMAND ----------

# 136.443.175

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

df = combined_df

df = df.withColumn("uf", col("municipio").cast('string').substr(1,2).cast('int'))

df = (
    df
    .withColumn("mes_desligamento", col('mes_desligamento').cast('int')) \
    .withColumn("vl_remun_dezembro_nom", regexp_replace("vl_remun_dezembro_nom", ',', '.').cast('double')) \
    .withColumn("vl_remun_dezembro_sm_", regexp_replace("vl_remun_dezembro_sm_", ',', '.').cast('double')) \
    .withColumn("vl_remun_media_nom", regexp_replace("vl_remun_media_nom", ',', '.').cast('double')) \
    .withColumn("vl_remun_media_sm_", regexp_replace("vl_remun_media_sm_", ',', '.').cast('double')) \
    .withColumn("vl_rem_janeiro_sc", regexp_replace("vl_rem_janeiro_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_fevereiro_sc", regexp_replace("vl_rem_fevereiro_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_marco_sc", regexp_replace("vl_rem_marco_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_abril_sc", regexp_replace("vl_rem_abril_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_maio_sc", regexp_replace("vl_rem_maio_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_junho_sc", regexp_replace("vl_rem_junho_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_julho_sc", regexp_replace("vl_rem_julho_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_agosto_sc", regexp_replace("vl_rem_agosto_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_setembro_sc", regexp_replace("vl_rem_setembro_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_outubro_sc", regexp_replace("vl_rem_outubro_sc", ',', '.').cast('double')) \
    .withColumn("vl_rem_novembro_sc", regexp_replace("vl_rem_novembro_sc", ',', '.').cast('double')) 
)

df.createOrReplaceTempView('RAIS_v2')

# COMMAND ----------

motivo_cancelamento_df = spark.sql('SELECT ano, motivo_desligamento FROM RAIS_v2')

# Write the DataFrame as Parquet files
motivo_cancelamento_df.write.mode("overwrite").parquet("/mnt/silver/motivo_cancelamento")


# COMMAND ----------

folder_path = "/mnt/silver/RAIS_VINC_PUB"

folder_size = 0
for file_info in dbutils.fs.ls(folder_path):
    folder_size += file_info.size

print(f"Tamanho ocupado no Data Lake em {folder_path}: {round(folder_size/1e9, 2)} Gigabytes")

# COMMAND ----------

import time
import statistics

parquet_folder_path = "/mnt/silver/RAIS_VINC_PUB"

num_tests = 50

duration_times = []

for _ in range(num_tests):
    start_time = time.time()
    df = spark.read.parquet(parquet_folder_path)
    end_time = time.time()
    duration_times.append(end_time - start_time)

median_duration = statistics.median(duration_times)
print("Mediana do Tempo para se Ler uma Pasta Parquet", num_tests, "vezes:", round(median_duration, 2), "segundos")
