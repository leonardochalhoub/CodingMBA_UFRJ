# Databricks notebook source
# Read CSV files from the first bronze folder
bronze_folder_1 = "/mnt/bronze/RAIS_VINC_PUB_2020_csv"
df_bronze_1 = spark.read.csv(bronze_folder_1, header=True, inferSchema=False)

# Read CSV files from the second bronze folder
bronze_folder_2 = "/mnt/bronze/RAIS_VINC_PUB_2021_csv"
df_bronze_2 = spark.read.csv(bronze_folder_2, header=True, inferSchema=False)

# Combine the two dataframes
combined_df = df_bronze_1.union(df_bronze_2)

# Write the combined dataframe to the silver layer
silver_folder_csv = "/mnt/silver/RAIS_VINC_PUB_csv"
combined_df.write.mode("overwrite").csv(silver_folder_csv, header=True)

# COMMAND ----------

combined_df.count()

# COMMAND ----------

folder_path = "/mnt/silver/RAIS_VINC_PUB_csv"

# Get the size of the folder
folder_size = 0
for file_info in dbutils.fs.ls(folder_path):
    folder_size += file_info.size

print(f"Tamanho ocupado no Data Lake em {folder_path}: {round(folder_size/1e9, 2)} Gigabytes")

# COMMAND ----------

import time

# Define the path to your CSV folder
csv_folder_path = "/mnt/silver/RAIS_VINC_PUB_csv"

# Record the start time
start_time = time.time()

# Read CSV files into a DataFrame
df = spark.read.option("header", "true").csv(csv_folder_path)

# Record the end time
end_time = time.time()

# Calculate the time taken
reading_time_seconds = end_time - start_time

# Print the reading time
print("Time taken to read CSV folder:", reading_time_seconds, "seconds")

# COMMAND ----------

import time
import statistics

csv_folder_path = "/mnt/silver/RAIS_VINC_PUB_csv"

num_tests = 50

duration_times = []

for _ in range(num_tests):
    start_time = time.time()
    df = spark.read.option("header", "true").csv(csv_folder_path)
    end_time = time.time()
    duration_times.append(end_time - start_time)

median_duration = statistics.median(duration_times)

print("Mediana do Tempo para se Ler uma Pasta CSV", num_tests, "vezes:", round(median_duration, 2), "segundos")
