# Databricks notebook source
# MAGIC %md
# MAGIC ## Next step would be to understand Business needs and prepare some fact and dimension tables (oversimplified)

# COMMAND ----------

# DBTITLE 1,Query: 'Which is the second most common 'motivo_cancelamento'?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   ano,
# MAGIC   motivo_desligamento,
# MAGIC   COUNT(*) as count
# MAGIC FROM
# MAGIC   main.db_silver_dev.MOTIVO_CANCELAMENTO
# MAGIC GROUP BY
# MAGIC   ano,
# MAGIC   motivo_desligamento
# MAGIC ORDER BY
# MAGIC   COUNT(*) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.db_gold_dev.count_motivo_desligamento (
# MAGIC WITH MotivoCounts AS (
# MAGIC   SELECT
# MAGIC     ano,
# MAGIC     motivo_desligamento,
# MAGIC     COUNT(*) as count
# MAGIC   FROM
# MAGIC     main.db_silver_dev.MOTIVO_CANCELAMENTO
# MAGIC   GROUP BY
# MAGIC     ano,
# MAGIC     motivo_desligamento
# MAGIC ),
# MAGIC RankedMotivos AS (
# MAGIC   SELECT
# MAGIC     mc.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY mc.ano ORDER BY mc.count DESC) AS motivo_rank
# MAGIC   FROM
# MAGIC     MotivoCounts mc
# MAGIC ),
# MAGIC TotalCounts AS (
# MAGIC   SELECT
# MAGIC     ano,
# MAGIC     SUM(count) as total_count
# MAGIC   FROM
# MAGIC     MotivoCounts
# MAGIC   GROUP BY
# MAGIC     ano
# MAGIC ),
# MAGIC SumCounts AS (
# MAGIC   SELECT
# MAGIC     rm.ano,
# MAGIC     SUM(rm.count) AS sum_count
# MAGIC   FROM RankedMotivos rm
# MAGIC   GROUP BY rm.ano
# MAGIC )
# MAGIC SELECT
# MAGIC   rm.ano,
# MAGIC   CASE
# MAGIC     WHEN rm.motivo_rank <= 4 THEN rm.motivo_desligamento
# MAGIC     ELSE 'Other'
# MAGIC   END AS motivo_desligamento,
# MAGIC   SUM(rm.count) AS count,
# MAGIC   tc.total_count AS total_count_per_ano,
# MAGIC   (SUM(rm.count) * 100.0) / tc.total_count / 100 AS relative_count
# MAGIC FROM RankedMotivos rm
# MAGIC JOIN TotalCounts tc ON rm.ano = tc.ano
# MAGIC GROUP BY rm.ano, CASE WHEN rm.motivo_rank <= 4 THEN rm.motivo_desligamento ELSE 'Other' END, tc.total_count
# MAGIC ORDER BY rm.ano DESC, SUM(rm.count) DESC
# MAGIC )
