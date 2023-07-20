# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![Solid](https://www.dex.co/Content/images/dexco.svg#gh-light-mode-only)
# MAGIC
# MAGIC ## Processo - Ingestão Atributos
# MAGIC
# MAGIC `Este notebook faz parte do processo de geração dos Dados de Produtos Dexco`
# MAGIC
# MAGIC <b>Tabelas de Leitura</b>: 
# MAGIC |Catalogo|Owner|Tabela|
# MAGIC | --- | --- | --- |
# MAGIC |hive|gaia|mongo_atributos| 
# MAGIC |trust|produtos|filtro_entrada| 
# MAGIC
# MAGIC <b>Tabelas Atualizadas</b>: 
# MAGIC |Catalogo|Owner|Tabela|
# MAGIC | --- | --- | --- |
# MAGIC |trust|produtos|atributo_string| 
# MAGIC
# MAGIC `TK_2023-05-11`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Atributo Gaia

# COMMAND ----------

# MAGIC %run ../Ingest/utils/function

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Carrega Dados de Atributos 
df = spark.sql('''SELECT distinct q1.sku,
                   q1.nome_produto,
                   q1.id AS id,
                    q1.brand AS brand,
                    q1.name AS name,
                    q1.uom AS uom,
                    q1.value AS value, 
                    q1.valueId AS  valueId,
                    q1.subValue AS  subValue,
                    q1.dataAtualizacao
                FROM ( SELECT sku,
                           nome_produto,
                            id,
                            brand,
                            name,                             
                            inline(array_sort(values)),  
                            dataAtualizacao                         
                    FROM ( SELECT sku, 
                                   name as nome_produto,
                                    brand,
                                    inline(attributes), 
                                     updateDate dataAtualizacao
                            FROM   ingest.gaia.mongo_atributos where brand is not null )) q1 ''')

# COMMAND ----------

# DBTITLE 1,Filtro Entrada
#careega tabela de filtro
lst_filtro_en = filtro_entrada()

#aplica filtro no DF
df = df.filter(lower(col("id")).isin(lst_filtro_en))

# COMMAND ----------

#Juntar Unidade Medida (Separados por espaço) -- Se houver!!
df_attributes = df.withColumn("value_uom", when ((trim(col("uom")) != '') & \
                                                   (~col("uom").isNull()) , concat(col("value"),lit(' '),col("uom"))) \
                                            .otherwise(col("value")))

#Juntar os dados Multivalorados (Separados por virgula + espaço)
df_attributes_2 = df_attributes.groupBy("sku","nome_produto","id","brand") \
                        .agg(collect_list("value_uom") \
                        .alias("value_array")) \
                        .withColumn("value",
                        concat_ws(" ; ",(col("value_array"))))

# Dados prontos para inserir "trust.produtos.atributo_string"
df_attributes_2.select(col("sku").alias("sku"),
                       col("nome_produto"),
                       col("id").alias("atributo"),
                       col("brand").alias("marca"),
                       col("value").alias("valor")
                ).withColumn("origem", lit("gaia.attributes")) \
                 .withColumn("data_ingestao", current_timestamp()).distinct() \
                .createOrReplaceTempView("vw_gaia_attributes")
 

# COMMAND ----------

spark.sql("delete from trust.produtos.atributo_string where origem ='gaia'")
spark.sql('''
          
            select distinct 'gaia' as origem
            ,current_timestamp() as data_ingestao
            ,sku
            ,nome_produto
            ,atributo
            , valor
            from vw_gaia_attributes 
            where lower(atributo ) != 'brand'
            union all 
            select distinct  'gaia' as origem
            ,current_timestamp() as data_ingestao
            ,sku
            ,name as nome_produto
            ,'marca' as atributo
            ,brand as valor
            from ingest.gaia.mongo_atributos ''') \
                .write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("trust.produtos.atributo_string")
