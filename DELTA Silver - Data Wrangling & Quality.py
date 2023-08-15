# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE main.db_silver_dev.RAIS_VINC_PUB (
# MAGIC   ano VARCHAR(4),
# MAGIC   bairros_sp VARCHAR(20),
# MAGIC   bairros_fortaleza VARCHAR(20),
# MAGIC   bairros_rj VARCHAR(20),
# MAGIC   causa_afastamento_1 INT,
# MAGIC   causa_afastamento_2 INT,
# MAGIC   causa_afastamento_3 INT,
# MAGIC   motivo_desligamento INT,
# MAGIC   cbo_ocupacao_2002 VARCHAR(20),
# MAGIC   cnae_2_0_classe INT,
# MAGIC   cnae_95_classe INT,
# MAGIC   distritos_sp VARCHAR(20),
# MAGIC   vinculo_ativo_31_12 INT,
# MAGIC   faixa_etaria INT,
# MAGIC   faixa_hora_contrat INT,
# MAGIC   faixa_remun_dezem_sm_ INT,
# MAGIC   faixa_remun_media_sm_ INT,
# MAGIC   faixa_tempo_emprego INT,
# MAGIC   escolaridade_apos_2005 DOUBLE,
# MAGIC   qtd_hora_contr DOUBLE,
# MAGIC   idade DOUBLE,
# MAGIC   ind_cei_vinculado INT,
# MAGIC   ind_simples INT,
# MAGIC   mes_admissao INT,
# MAGIC   mes_desligamento VARCHAR(20),
# MAGIC   mun_trab INT,
# MAGIC   municipio INT,
# MAGIC   nacionalidade INT,
# MAGIC   natureza_juridica INT,
# MAGIC   ind_portador_defic INT,
# MAGIC   qtd_dias_afastamento DOUBLE,
# MAGIC   raca_cor INT,
# MAGIC   regioes_adm_df INT,
# MAGIC   vl_remun_dezembro_nom VARCHAR(20),
# MAGIC   vl_remun_dezembro_sm_ VARCHAR(20),
# MAGIC   vl_remun_media_nom VARCHAR(20),
# MAGIC   vl_remun_media_sm_ VARCHAR(20),
# MAGIC   cnae_2_0_subclasse INT,
# MAGIC   sexo_trabalhador DOUBLE,
# MAGIC   tamanho_estabelecimento INT,
# MAGIC   tempo_emprego VARCHAR(20),
# MAGIC   tipo_admissao INT,
# MAGIC   tipo_estab41 INT,
# MAGIC   tipo_estab42 VARCHAR(20),
# MAGIC   tipo_defic INT,
# MAGIC   tipo_vinculo INT,
# MAGIC   ibge_subsetor INT,
# MAGIC   vl_rem_janeiro_sc VARCHAR(20),
# MAGIC   vl_rem_fevereiro_sc VARCHAR(20),
# MAGIC   vl_rem_marco_sc VARCHAR(20),
# MAGIC   vl_rem_abril_sc VARCHAR(20),
# MAGIC   vl_rem_maio_sc VARCHAR(20),
# MAGIC   vl_rem_junho_sc VARCHAR(20),
# MAGIC   vl_rem_julho_sc VARCHAR(20),
# MAGIC   vl_rem_agosto_sc VARCHAR(20),
# MAGIC   vl_rem_setembro_sc VARCHAR(20),
# MAGIC   vl_rem_outubro_sc VARCHAR(20),
# MAGIC   vl_rem_novembro_sc VARCHAR(20),
# MAGIC   ano_chegada_brasil INT,
# MAGIC   ind_trab_intermitente INT,
# MAGIC   ind_trab_parcial INT
# MAGIC   )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.db_silver_dev.RAIS_VINC_PUB (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     main.db_bronze_dev.RAIS_VINC_PUB_2020
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     main.db_bronze_dev.RAIS_VINC_PUB_2021
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM main.db_silver_dev.RAIS_VINC_PUB

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

df = spark.sql('''
               SELECT *
               FROM main.db_silver_dev.RAIS_VINC_PUB
               ''')

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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.db_silver_dev.MOTIVO_CANCELAMENTO (
# MAGIC   SELECT
# MAGIC   ano,
# MAGIC   motivo_desligamento
# MAGIC   FROM
# MAGIC     RAIS_v2
# MAGIC )
