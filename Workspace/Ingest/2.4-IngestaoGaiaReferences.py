# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![Solid](https://www.dex.co/Content/images/dexco.svg#gh-light-mode-only)
# MAGIC
# MAGIC ## Processo - References 
# MAGIC
# MAGIC `Este notebook faz parte do processo de geração dos Dados de Produtos Dexco`
# MAGIC
# MAGIC <b>Tabelas de Leitura</b>: 
# MAGIC |Owner|Tabela|
# MAGIC | --- | --- | 
# MAGIC |gaia|mongo_itens_relacionados|
# MAGIC |gaia|mongo_itens_instalacao|
# MAGIC
# MAGIC `TK_2023-05-19`

# COMMAND ----------


from pyspark.sql.functions import *
spark.sql("SET TIME ZONE 'America/Sao_Paulo'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # References

# COMMAND ----------

# DBTITLE 1,Estrutura Itens Relacionados

df_ir =spark.sql('''select sku,  concat_ws(';',collect_list(value) ) as relatedItems from ( 
select sku , inline(keys)  from (
select sku,inline(references.relatedItems)
from ingest.gaia.mongo_itens_relacionados ) )
where id='COD_MATERIAL'
group by sku''')



# COMMAND ----------

# DBTITLE 1,Estrutura Itens Instalacao
df_it = spark.sql('''
select sku,  concat_ws(';',collect_list(value) ) as installationItems from ( 
select sku, inline(keys)  from (
select  sku,inline(references.installationItems) 
from ingest.gaia.mongo_itens_instalacao) )
where id='COD_MATERIAL'
group by sku''')

# COMMAND ----------

# DBTITLE 1,Cria DF Link 
df_link =df_ir.select("sku").union(df_it.select("sku") ) 

# COMMAND ----------

# DBTITLE 1,Ingestao Tabela Referencia 
df_reference =df_link.join(df_ir, on = df_link.sku ==df_ir.sku, how='left')\
                     .join(df_it, on = df_link.sku ==df_it.sku, how='left')\
                     .select(df_link.sku,df_ir.relatedItems ,df_it.installationItems)

#Update Table
df_reference.write.format("delta").mode("overwrite").option('mergeSchema','true').saveAsTable("trust.produtos.referencia")                     

