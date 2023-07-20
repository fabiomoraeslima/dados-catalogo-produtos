# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![Solid](https://www.dex.co/Content/images/dexco.svg#gh-light-mode-only)
# MAGIC
# MAGIC ## Processo - Assets 
# MAGIC
# MAGIC `Este notebook faz parte do processo de geração dos Dados de Produtos Dexco`
# MAGIC
# MAGIC <b>Tabelas de Leitura</b>: 
# MAGIC |Owner|Tabela|
# MAGIC | --- | --- | 
# MAGIC |gaia|mongo_asset_documentos|
# MAGIC |gaia|mongo_asset_imagens|
# MAGIC
# MAGIC `TK_2023-05-09`

# COMMAND ----------

# MAGIC %run ../Ingest/utils/function 

# COMMAND ----------

import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
spark.sql("SET TIME ZONE 'America/Sao_Paulo'").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Documentos

# COMMAND ----------

# DBTITLE 1,Ingestao Documento
df_documento = spark.sql("""
select distinct q2.sku 
      ,q2.marca
      ,'documento' tipo_arquivo
      ,q2.id1 as id_arquivo
      ,q2.name1 as nome_arquivo_documento
      ,inline(values)
      ,lower(id) as atributo
      --,replace(replace(id,'asset.',''),'-','_') as atributo
      ,q2.name
      ,lower(split(split(q2.type,",")[0],'-')[1]) id_ref
      ,replace (split(q2.type,",")[1],'}','') descricao
      ,q2.url
      ,q2.version as versao
from 
(
select distinct q1.sku 
      ,q1.marca
      ,q1.nome
      ,q1.assetType
      ,inline(q1.attributes)
      ,q1.id as id1
      ,q1.name as name1
      ,CAST(q1.type AS STRING)
      ,q1.url
      ,q1.version
from ( 
select
     sku
    ,brand marca
    ,name nome
    ,inline(assets.documents)
from
 ingest.gaia.mongo_asset_documentos  
) q1
  ) q2
""")

# COMMAND ----------

# DBTITLE 1,Filtro Entrada
ls_filtro = filtro_entrada()
df_documento = df_documento.filter(col("atributo").isin(ls_filtro)).distinct()

# COMMAND ----------

# DBTITLE 1,Atualiza Tabela Documento
#Pivot
pvt_documento = df_documento.groupBy("sku","marca","tipo_arquivo","nome_arquivo_documento","descricao","url","versao") \
                         .pivot("atributo") \
                         .agg({'value':'max'}).distinct()

#Update Table
pvt_documento.write.format("delta").mode("overwrite").option('mergeSchema','true').saveAsTable("trust.produtos.documento_temp")

# COMMAND ----------

# DBTITLE 1,Renomear Colunas
#Função que renomeia colunas
df_documento = Rename_Columns_Table("trust.produtos.documento_temp")

#Apaga tabela para gerar com novos campos
spark.sql("drop table if exists trust.produtos.documento")

#grava tabela com dados de produtos
df_documento.distinct().write.format("delta") \
              .mode("overwrite") \
              .saveAsTable("trust.produtos.documento")

#Apaga tabela temporaria
spark.sql("drop table if exists trust.produtos.documento_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC # Imagens

# COMMAND ----------

# DBTITLE 1,Imagem
df_imagem = spark.sql(""" 
select distinct q4.sku
,q4.marca
,q4.sub_marca
,q4.nome
,split(q4.tipo_imagem,'-')[1] as tipo_imagem
--,replace(replace(q4.atributo,'asset.',''),'-' ,'_') as atributo
,q4.atributo as atributo
,q4.nome_atributo
,value as valor
,uom as unidade_medida
,q4.tipo as tipo_arquivo
,q4.id_arquivo
,q4.nome_arquivo as nome_arquivo_imagem
,q4.resize 
,q4.url
,q4.versao
 from (
select distinct  q3.sku
      ,q3.marca
      ,q3.nome
      ,q3.subBrand as sub_marca
      ,q3.tipo
      ,q3.id atributo
      ,q3.name nome_atributo
      ,inline(q3.values)
      ,q3.id_arquivo
      ,q3.nome_arquivo
      ,q3.resize
      ,type['id'] as tipo_imagem 
      ,type['name'] as tipo_nome
      ,q3.url
      ,q3.versao
from (
select distinct q2.sku
      ,q2.marca
      ,q2.nome
      ,q2.subBrand
      ,q2.assetType tipo
      ,inline(attributes)
      ,q2.id as id_arquivo
      ,q2.name as nome_arquivo
      ,q2.resize
      ,q2.type
      ,q2.url
      ,q2.version versao
 from (
select distinct  q1.sku
      ,q1.marca
      ,q1.nome
      ,q1.subBrand
      ,inline(images)
from (
select sku sku
      ,brand marca
      ,name nome
      ,subBrand
      ,updateDate
      ,assets.images
 from ingest.gaia.mongo_asset_imagens
) q1 
 ) q2
  ) q3 )q4    
 """)

# COMMAND ----------

# DBTITLE 1,Filtro Entrada
ls_filtro = filtro_entrada()
df_imagem = df_imagem.distinct().filter(col("atributo").isin(ls_filtro))

# COMMAND ----------

# DBTITLE 1,Atualiza Tabela Imagem
pvt_imagem = df_imagem.groupBy("sku","marca","sub_marca","nome","tipo_imagem","tipo_arquivo","id_arquivo","nome_arquivo_imagem","resize","nome_atributo",'unidade_medida')\
                         .pivot("atributo") \
                         .agg({'valor':'max'}).distinct()
 
#Update Table
pvt_imagem.write.format("delta").mode("overwrite").option('mergeSchema','true').saveAsTable("trust.produtos.imagem_tmp")

# COMMAND ----------

# DBTITLE 1,Renomear Colunas
#Função que renomeia colunas
df_imagem = Rename_Columns_Table("trust.produtos.imagem_tmp")

#Apaga tabela para gerar com novos campos
spark.sql("drop table if exists trust.produtos.imagem")

#grava tabela com dados de produtos
df_imagem.distinct().write.format("delta") \
              .mode("overwrite") \
              .saveAsTable("trust.produtos.imagem")

#Apaga tabela temporaria
spark.sql("drop table if exists trust.produtos.imagem_tmp")
