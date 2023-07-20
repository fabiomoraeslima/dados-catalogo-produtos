# Databricks notebook source

import os
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
# MAGIC
# MAGIC ###REgra Vtex Marketplace

# COMMAND ----------

#df_arketplace_vtex = spark.table("ingest.marketplace_vtex.skus")\
#                     .where(col("DetailUrl").isNotNull())\
#                     .select(regexp_replace(trim(col("ProductRefId")),"_PAI","").cast("string").alias("sku")
#                             ,col("DetailUrl").alias("sku")).distinct()
                     

# COMMAND ----------

# MAGIC %md
# MAGIC #Valida URL MarketPlace

# COMMAND ----------

from pyspark.sql.functions import *

##---------------------------------------------------------------------------------------------------------------------
## CRIA FUNCAO VERIFICACAO
##---------------------------------------------------------------------------------------------------------------------

def valida_url_mkt(df):
    texto_procurado = 'Desculpe, a página não foi encontrada'
    import requests
    from  urllib.parse  import quote
    import time
    import sys

    contador = 0
    lista = []
    
    total = df.count()
    
    for url_i, sku in  df.select("url","sku" ).collect():
        contador = contador + 1
        sys.stdout.write ("\r" + f"Execucao {contador} de {total}")
        sys.stdout.flush()
        
        try:
            url = url_i 
            result = requests.get(url)
        
            if result.status_code == 200:
                html = result.text
                if texto_procurado in html:
                    lista.append(str(result.status_code) + ";" + sku + ";" + url)
            elif result.status_code > 200: 
                lista.append(str(result.status_code) + ";" + sku + ";" + url)
        except Exception as erro:
            print(u"\u2192","ATENÇÃO ERRO->",erro) 
            lista.append('-1' +";"+ sku +";"+url) 
    
    return lista

##---------------------------------------------------------------------------------------------------------------------
## MONTA DATAFRAME URL SITE
##---------------------------------------------------------------------------------------------------------------------

df_url_marketplace = spark.sql('''select sku,url_marketing_place_b_2_c as url
from  trust.produtos.produto_strapi where url_marketing_place_b_2_c is not  null ''').distinct()

print(u"\u2192",'INICIO VALIDACAO URL LOJA MarketPlace')


##---------------------------------------------------------------------------------------------------------------------
## EXECUTA FUNCAO COM PRINT DOS URL COM RETORNO 404
##---------------------------------------------------------------------------------------------------------------------
lista_url_b2c =valida_url_mkt(df_url_marketplace)


import pandas as pd
df_url_site_404 = pd.DataFrame(lista_url_b2c, columns=['url_site'])
current_timestamp = pd.Timestamp.now()
df_url_site_404['data_ingestao'] = current_timestamp

# Converter DataFrame pandas em DataFrame do Spark
df_spark = spark.createDataFrame(df_url_site_404)

# Gravar DataFrame do Spark em tabela Delta
df_spark.write.format("delta").mode("append").saveAsTable("analytics.temp_valida_url_market_place")
df_spark.display() 

