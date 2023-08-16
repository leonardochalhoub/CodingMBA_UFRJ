`CodingMBA_UFRJ` é o conjunto de scripts que foram utilizados na elaboração da demonstração prática incluída na minha Monografia para Conclusão do Curso MBA em Engenharia de Dados da Escola Politécnica do Rio de Janeiro, tranando-se da implementação de arquitetura para ingestão e transformação de dados em uma arquitetura multi-hop (bronze, silver e gold) na Microsoft Azure.

Essa iniciativa não é um simples complemento para o trabalho acadêmico, mas sim uma forma de estimular e apoiar a reprodutibilidade do que é proposto no trabalho científico (link a ser inserido aqui, no futuro, quando o trabalho estiver publicado formalmente). Códigos de programação em PySpark ou Spark SQL na plataforma Databricks.


Fonte dos dados públicos utilizados. Apenas RAIS Vínculos Públicos para 2020 e 2021 = 136 milhões de linhas, Big Data.
[http://pdet.mte.gov.br/microdados-rais-e-caged](http://pdet.mte.gov.br/microdados-rais-e-caged)

Exemplo de código PySpark utilizado para gravar a tabela completa desejada na camada Silver em formato CSV
```r
silver_folder_csv = "/mnt/silver/RAIS_VINC_PUB_csv"
combined_df.write.mode("overwrite").csv(silver_folder_csv, header=True)
```
![Figura 9: Arquitetura de Ponta-a-Ponta para Ingestão de Big Data de Fonte Pública até um Data Lakehouse na Microsoft Azure](https://github.com/leonardochalhoub/CodingMBA_UFRJ/blob/main/Arquitetura.png)
