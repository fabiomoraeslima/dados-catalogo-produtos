# Databricks notebook source
# MAGIC %md
# MAGIC #Biblioteca

# COMMAND ----------

# DBTITLE 1,Biblioteca

import os
import time 
import pytz
import datetime
import pyspark
import psycopg2
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.window import Window
spark.sql("SET TIME ZONE 'America/Sao_Paulo'").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Valida Site URL

# COMMAND ----------


##---------------------------------------------------------------------------------------------------------------------
## CRIA FUNCAO VERIFICACAO
##---------------------------------------------------------------------------------------------------------------------

 
def valida_url(df):
    import requests
    from  urllib.parse  import quote
    import time
    import sys
       
    contador =0 
    lista=[]
    
    total = df.count()
    for url_i,sku in  df.select("url","sku" ).collect():
        contador=contador+1
        sys.stdout.write ("\r" + f"Execucao {contador} de {total}")
        sys.stdout.flush()       
        try:
            url = url_i             
            result =requests.get(url)            
            if result.status_code > 200:
                lista.append(str(result.status_code )+";"+sku +";"+url)
        except Exception as erro:
            print(u"\u2192","ATENÇÃO ERRO->",erro) 
            lista.append(str(result.status_code)+";"+sku +";"+url)         

        time.sleep(5)
    return lista

##---------------------------------------------------------------------------------------------------------------------
## MONTA DATAFRAME URL SITE
##---------------------------------------------------------------------------------------------------------------------

df_url_site = spark.sql('''select sku,url_catalogo_institucional as url
from  trust.produtos.produto_strapi where url_catalogo_institucional is not  null''').distinct()


print(u"\u2192","INICIO VALIDAÇÃO URL Site Insitutcional")

##---------------------------------------------------------------------------------------------------------------------
## EXECUTA FUNCAO COM PRINT DOS URL COM RETORNO 404
##---------------------------------------------------------------------------------------------------------------------
lista_url_site =valida_url(df_url_site) 

import pandas as pd


df_url_site_404 = pd.DataFrame(lista_url_site, columns=['url_site'])
current_timestamp = pd.Timestamp.now()
df_url_site_404['data_ingestao'] = current_timestamp

# Converter DataFrame pandas em DataFrame do Spark
df_spark = spark.createDataFrame(df_url_site_404)


 Gravar DataFrame do Spark em tabela Delta
df_spark.write.format("delta").mode("append").saveAsTable("analytics.temp_valida_url_site")



df_spark.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from analytics.temp_valida_url_site

# COMMAND ----------

# DBTITLE 1,Validacao com TMP estatico vs tabela final

'''
%sql
select sku,url_catalogo_institucional,length( url_catalogo_institucional) qtd_caracter   from  trust.produtos.produto_strapi 
where   sku='P.60.85'

union all

select sku, url_catalogo_institucional, length( url_catalogo_institucional) qtd_caracter from trust.produtos.strapi_site_dado_temporario
where sku ='P.60.85'

'''



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Tabela TMP url institucional - do Rafa

# COMMAND ----------

'''

%sql


--select * from trust.produtos.strapi_site_dado_temporario
select sku, url_catalogo_institucional from trust.produtos.produto_strapi
where sku in ('4509405',
'4916.MR105.PQ.MT',
'4916.INX104.PQ',
'4916.INX105.PQ',
'L.90.94',
'4916.MR104.PQ.MT',
'L.90.17',
'L.90.22'  )

'''


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Regra URL SITE automatico

# COMMAND ----------


#analise URL montado automatico para todos urls que são NULLS

'''

from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
#Carrega dados que não possuiam url estabelecida(strapi_site_dado_temporario)

#Regra : urlBase/ambientes/Hierarquia1/Hierarquia2/Hierarquia3/nome_produto+sku
df_url_site = spark.sql("""
select distinct 
     produto.sku
    ,lower(concat ('https://www.deca.com.br/ambientes/', 
            replace(hierarquia.nivel1,' ','-'), '/'
            ,replace(hierarquia.nivel2,' ','-'),'/'
            ,replace(hierarquia.descricao,' ','-'),'/'
    ,replace(replace(replace(replace(nome_produto,'"',''),'/',''),' ','-'),'.','') 
    ,replace(produto.sku,'.',''))) as url_catalogo_institucional
   ,replace(hierarquia.nivel1,' ','-') hie_nivel1_tratado
   ,replace(hierarquia.nivel2,' ','-') hie_nivel2_tratado
   ,replace(hierarquia.descricao,' ','-') hie_nivel3_tratado
   ,replace(replace(replace(replace(trim(nome_produto),'"',''),'/',''),' ','-'),'.','')   nome_produto_tratado
   ,replace(produto.sku,'.','') sku_tratado

 from trust.produtos.produto_strapi produto
 join trust.produtos.hierarquia_site_link link on produto.sku = link.sku
 join trust.produtos.hierarquia hierarquia on link.hierarquia = hierarquia.hierarquia
 where 1=1
 and  url_catalogo_institucional is not  null 
   and trim(hierarquia.nivel1) <> ''
and trim(hierarquia.nivel2) <> ''
and produto.sku in ('4509405',
'4916.MR105.PQ.MT',
'4916.INX104.PQ',
'4916.INX105.PQ',
'L.90.94',
'4916.MR104.PQ.MT',
'L.90.17','L.90.22')
""")


df_url_site = df_url_site.filter("url_catalogo_institucional is not null")\
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[áàâäã]", "a")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[éèêë]", "e")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[íìîï]", "i")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[óòôöõ]", "o")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[úùûü]", "u")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[ç]", "c")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", '"', ''))

df_url_site.display()
#Aplica o Upsert na Tabela Final
delta_prd = DeltaTable.forName(spark, 'trust.produtos.produto_strapi')

delta_prd.alias('prd_strapi')\
    .merge(df_url_site.alias('url_site'), "prd_strapi.sku = url_site.sku")\
    .whenMatchedUpdate(
        set ={"prd_strapi.url_catalogo_institucional" : "url_site.url_catalogo_institucional"}
        ).execute() 

        '''
