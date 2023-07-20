# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![Solid](https://www.dex.co/Content/images/dexco.svg#gh-light-mode-only)
# MAGIC
# MAGIC ## Ingestao - MongoDB Gaia 
# MAGIC
# MAGIC `Este notebook faz parte do processo de validação da  Ingestão dos dados de Produtos do Gaia`
# MAGIC
# MAGIC **Tabelas Trust**
# MAGIC |Catalogo|Owner | Tabela|
# MAGIC |----| ---- | ---- |
# MAGIC |ingest |gaia|mongo_asset_documentos|
# MAGIC |ingest |gaia|mongo_asset_imagens|
# MAGIC |ingest |gaia|mongo_atributos|
# MAGIC |ingest |gaia|mongo_itens_relacionados|
# MAGIC |ingest |gaia|mongo_itens_instalacao|
# MAGIC |ingest |gaia|mongo_hierarquia|
# MAGIC |ingest |gaia|mongo_hierarquia_sku_primaria|
# MAGIC |ingest |gaia|mongo_hierarquia_sku_marketing|
# MAGIC |ingest |gaia|mongo_hierarquia_sku_website|
# MAGIC |ingest |gaia|mongo_hierarquia_sku_site|
# MAGIC |ingest |gaia|mongo_collection_attributes|
# MAGIC |ingest |gaia|mongo_lovs|
# MAGIC
# MAGIC
# MAGIC
# MAGIC `TK_2023-05-08`

# COMMAND ----------

# MAGIC %md
# MAGIC #Objetivo 
# MAGIC <p/>
# MAGIC
# MAGIC * Validar o dado que vem do mongo db (Gaia) consistindo se esta aderente com as normas de governancia de dados
# MAGIC <p>

# COMMAND ----------

# DBTITLE 0, 
# MAGIC %md
# MAGIC #Controle Versao

# COMMAND ----------

# DBTITLE 1,Controle Versao
# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC |Data| Autor| Acao| Alteracao| 
# MAGIC |----| ---- | ---- | ---- |
# MAGIC |01/06/2023|Luciano Fidelis|Criacao|Criado processos de validacao dos datatype/ url documento e imagem/nome coluna|
# MAGIC |01/06/2023|Flavio Morares|Manutencao|Manutencao notebook melhorando a performance ao validar URL|

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Importa Bibliotecas

# COMMAND ----------

# DBTITLE 1,Importa Bibliotecas
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Prepara ambiente

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##View Apoio
# MAGIC
# MAGIC **Views**
# MAGIC |View|
# MAGIC |----|
# MAGIC |vw_atributo_documento|
# MAGIC |vw_atributo_imagem|
# MAGIC |vw_atributo_lov|
# MAGIC |vw_atributo_atributo_text|
# MAGIC |vw_atributo_atributo_number|

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,Filttro.Entrada Atributo documento
# MAGIC %sql
# MAGIC
# MAGIC create or replace temporary view vw_atributo_documento as 
# MAGIC
# MAGIC select distinct 
# MAGIC          trim(lower(filtro.id_atributo_entrada)) id_atributo_entrada
# MAGIC         , catalogo.id_atributo_catalogo
# MAGIC from trust.produtos.filtro_entrada as filtro
# MAGIC inner join trust.produtos.catalogo_atributo as catalogo on filtro.id_atributo_catalogo=catalogo.id_atributo_catalogo
# MAGIC where 1=1
# MAGIC and catalogo.nome_tabela ='documento'
# MAGIC or  catalogo.nome_tabela='documento;imagem' 
# MAGIC order by 1;
# MAGIC
# MAGIC select * from vw_atributo_documento

# COMMAND ----------

# DBTITLE 1,Filttro.Entrada Atributo Imagem
# MAGIC %sql
# MAGIC
# MAGIC create or replace temporary view vw_atributo_imagem as 
# MAGIC
# MAGIC select distinct 
# MAGIC         trim(lower(filtro.id_atributo_entrada)) id_atributo_entrada
# MAGIC         , catalogo.id_atributo_catalogo
# MAGIC from trust.produtos.filtro_entrada as filtro
# MAGIC inner join trust.produtos.catalogo_atributo as catalogo on filtro.id_atributo_catalogo=catalogo.id_atributo_catalogo
# MAGIC where 1=1 
# MAGIC and  catalogo.nome_tabela='imagem' 
# MAGIC or  catalogo.nome_tabela='documento;imagem' 
# MAGIC order by 1;
# MAGIC
# MAGIC select * from vw_atributo_imagem

# COMMAND ----------

# DBTITLE 1,Filttro.Entrada so origem golden_Record
# MAGIC %sql
# MAGIC
# MAGIC create or replace temporary view vw_atributo_goden_record as 
# MAGIC
# MAGIC select distinct 
# MAGIC         trim(lower(filtro.id_atributo_entrada)) id_atributo_entrada
# MAGIC         , catalogo.id_atributo_catalogo
# MAGIC from trust.produtos.filtro_entrada as filtro
# MAGIC inner join trust.produtos.catalogo_atributo as catalogo on filtro.id_atributo_catalogo=catalogo.id_atributo_catalogo
# MAGIC where 1=1 
# MAGIC and not  (catalogo.nome_tabela ='documento'
# MAGIC or  catalogo.nome_tabela='documento;imagem')
# MAGIC order by 1;
# MAGIC
# MAGIC select * from vw_atributo_goden_record

# COMMAND ----------

# DBTITLE 1,Filttro.Entrada Atributo LOV
# MAGIC %sql
# MAGIC
# MAGIC create or replace temporary view vw_atributo_lov as 
# MAGIC
# MAGIC select distinct 
# MAGIC         trim(lower(filtro.id_atributo_entrada)) id_atributo_entrada
# MAGIC         , catalogo.id_atributo_catalogo
# MAGIC from trust.produtos.filtro_entrada as filtro
# MAGIC inner join trust.produtos.catalogo_atributo as catalogo on filtro.id_atributo_catalogo=catalogo.id_atributo_catalogo
# MAGIC where 1=1 
# MAGIC and  catalogo.tipo_dado_atributo='LOV'
# MAGIC and not  (catalogo.nome_tabela ='documento'
# MAGIC or  catalogo.nome_tabela='documento;imagem')
# MAGIC
# MAGIC order by 1;
# MAGIC
# MAGIC select * from vw_atributo_lov

# COMMAND ----------

# DBTITLE 1,Filttro.Entrada Atributo Text
# MAGIC %sql
# MAGIC
# MAGIC create or replace temporary view vw_atributo_text as 
# MAGIC
# MAGIC select distinct 
# MAGIC        trim(lower(filtro.id_atributo_entrada)) id_atributo_entrada
# MAGIC         , catalogo.id_atributo_catalogo
# MAGIC from trust.produtos.filtro_entrada as filtro
# MAGIC inner join trust.produtos.catalogo_atributo as catalogo on filtro.id_atributo_catalogo=catalogo.id_atributo_catalogo
# MAGIC where 1=1 
# MAGIC and  catalogo.tipo_dado_atributo in ('string', 'text')
# MAGIC and not  (catalogo.nome_tabela ='documento'
# MAGIC or  catalogo.nome_tabela='documento;imagem')
# MAGIC order by 1;
# MAGIC
# MAGIC select * from vw_atributo_text

# COMMAND ----------

# DBTITLE 1,Filttro.Entrada Atributo Number
# MAGIC %sql
# MAGIC
# MAGIC create or replace temporary view vw_atributo_number as 
# MAGIC
# MAGIC select distinct 
# MAGIC         trim(lower(filtro.id_atributo_entrada)) id_atributo_entrada
# MAGIC         , catalogo.id_atributo_catalogo
# MAGIC from trust.produtos.filtro_entrada as filtro
# MAGIC inner join trust.produtos.catalogo_atributo as catalogo on filtro.id_atributo_catalogo=catalogo.id_atributo_catalogo
# MAGIC where 1=1 
# MAGIC and  catalogo.tipo_dado_atributo in ('number')
# MAGIC and not  (catalogo.nome_tabela ='documento'
# MAGIC or  catalogo.nome_tabela='documento;imagem')
# MAGIC order by 1;
# MAGIC
# MAGIC select * from vw_atributo_number

# COMMAND ----------

# MAGIC %md
# MAGIC #Valida DataType

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Number

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from 
# MAGIC
# MAGIC select distinct
# MAGIC  n3.brand 
# MAGIC  ,sku 
# MAGIC ,n3.id_atributo_entrada
# MAGIC ,n3.valor
# MAGIC --,max(value)  max_valor_Atributo
# MAGIC --,min(value) min_valor_Atributo
# MAGIC from ( select distinct brand 
# MAGIC                ,sku 
# MAGIC                ,value as valor
# MAGIC                ,id_atributo_entrada
# MAGIC                ,cast(value as numeric) valida
# MAGIC           from (select sku, brand,  trim(lower(id)) as id_atributo_entrada , inline(values)
# MAGIC                from (select sku, brand, inline(attributes) 
# MAGIC                     from  ingest.gaia.mongo_atributos where brand is not null
# MAGIC ) n1 )n2 )n3 inner join  vw_atributo_number vw_number on vw_number.id_atributo_entrada  =n3.id_atributo_entrada 
# MAGIC where  valida  is      null  
# MAGIC order by n3.sku, n3.id_atributo_entrada

# COMMAND ----------

# MAGIC %md
# MAGIC #Validacao Diversas 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Nome 

# COMMAND ----------

# DBTITLE 1,Nome Vazio - >golden_record.attributes 
# MAGIC  %sql
# MAGIC select  distinct 
# MAGIC          --sku
# MAGIC          brand
# MAGIC         ,n2.id_atributo_entrada 
# MAGIC         ,nome
# MAGIC from (select sku, brand, trim(lower(id)) as id_atributo_entrada ,name as nome
# MAGIC       from (select sku, brand, inline(attributes) 
# MAGIC                from  ingest.gaia.mongo_atributos where brand is not null
# MAGIC )n1)n2 inner join  vw_atributo_goden_record as golden_record  on n2.id_atributo_entrada=golden_record.id_atributo_entrada
# MAGIC where nome is null
# MAGIC      or trim(nome) = ''     

# COMMAND ----------

# DBTITLE 1,Nome Vazio - >golden_record.images 
# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC
# MAGIC  brand 
# MAGIC  ,'golden_record.images' as  Origem
# MAGIC  ,sku
# MAGIC , trim(lower(id)) as id_imagem
# MAGIC , name as nome
# MAGIC from (select sku,brand,inline(assets.images)
# MAGIC        from ingest.gaia.mongo_asset_imagens
# MAGIC )n1  
# MAGIC where name is null
# MAGIC      or trim(name) = ''   

# COMMAND ----------

# DBTITLE 1,Nome Vazio - >golden_record.images.attributes
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select brand
# MAGIC       ,'golden_record.images.attributes' as  Origem
# MAGIC       ,assetType
# MAGIC       ,sku 
# MAGIC       ,trim(lower(id))  as id_atributo_entrada  
# MAGIC       , name as nome
# MAGIC from (
# MAGIC select  sku, brand ,assetType, trim(lower(id)) as id_imagem, name as nome,inline(attributes)
# MAGIC from (select sku,brand,inline(assets.images)
# MAGIC        from ingest.gaia.mongo_asset_imagens
# MAGIC )n1  )n2
# MAGIC
# MAGIC where name is null
# MAGIC      or trim(name) = ''   

# COMMAND ----------

# DBTITLE 1,Nome Vazio - >golden_record.documents
# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC
# MAGIC  brand 
# MAGIC  ,'golden_record.documents' as  Origem
# MAGIC  ,sku
# MAGIC , trim(lower(id)) as id_documento
# MAGIC , name as nome
# MAGIC from (select sku,brand,assetType, inline(assets.documents)
# MAGIC        from ingest.gaia.mongo_asset_documentos
# MAGIC
# MAGIC )n1  
# MAGIC where name is null
# MAGIC   or trim(name) = ''   

# COMMAND ----------

# DBTITLE 1,Nome Vazio - >golden_record.documents.attributes
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select brand
# MAGIC       ,'golden_record.documents.attributes' as  Origem
# MAGIC       ,assetType
# MAGIC       ,sku 
# MAGIC       ,trim(lower(id))  as id_atributo_entrada  
# MAGIC       , name as nome
# MAGIC       ,url
# MAGIC from (
# MAGIC select  sku, brand ,assetType,url, trim(lower(id)) as id_documento, name as nome,inline(attributes)
# MAGIC from (select sku,brand,inline(assets.documents)
# MAGIC        from ingest.gaia.mongo_asset_documentos
# MAGIC )n1  )n2
# MAGIC
# MAGIC where name is null
# MAGIC      or trim(name) = ''   

# COMMAND ----------

# MAGIC %md
# MAGIC ## URL

# COMMAND ----------

# DBTITLE 1,URL Vazio - >golden_record.images
# MAGIC %sql
# MAGIC select      'golden_record.images' as  Origem      
# MAGIC             ,n1.brand
# MAGIC             ,n1.sku                  
# MAGIC             ,trim(lower(id)) as id_imagem
# MAGIC             , name as nome 
# MAGIC             ,url
# MAGIC from (select sku,brand,inline(assets.images)
# MAGIC        from ingest.gaia.mongo_asset_imagens
# MAGIC )n1   where n1.brand is not null
# MAGIC and url is  null 
# MAGIC or trim(url)=''

# COMMAND ----------

# DBTITLE 1,URL Vazio - >golden_record.documents
# MAGIC %sql
# MAGIC select    distinct   'golden_record.documents' as  Origem      
# MAGIC             ,n1.brand
# MAGIC             ,n1.sku                  
# MAGIC             ,trim(lower(id)) as id_documento
# MAGIC             , name as nome 
# MAGIC             ,url
# MAGIC from (select sku,brand,inline(assets.documents)
# MAGIC        from ingest.gaia.mongo_asset_documentos
# MAGIC )n1   where n1.brand is not null
# MAGIC and url is  null 
# MAGIC or trim(url)=''

# COMMAND ----------

# DBTITLE 1,Validar se a URL retorna 200 em um request para documento + imagem
sql = """
select distinct origem  
              ,sku
              ,trim(url) as url
from (
select distinct 'golden_record.images' as  Origem      
            ,n1.brand
            ,n1.sku                  
            ,trim(lower(id)) as id_arquivo
            , name as nome 
            ,url
from (select sku,brand,inline(assets.images)
       from ingest.gaia.mongo_asset_imagens
)n1   where n1.brand is not null
union all 
select    distinct   'golden_record.documents' as  Origem      
            ,n1.brand
            ,n1.sku                  
            ,trim(lower(id)) as id_arquivo
            , name as nome 
            ,url
from (select sku,brand,inline(assets.documents)
       from ingest.gaia.mongo_asset_documentos
)n1   where n1.brand is not null
) """

# COMMAND ----------

def valida_url(sql):

    import requests
    from  urllib.parse  import quote
    import time
    import sys

    contador =0 
    lista=[]
    df_url_arquivo = spark.sql(sql)
    total = df_url_arquivo.count()
    
    for url_arquivo,sku,origem in  df_url_arquivo.select("url","sku","origem" ).collect():
        
        contador=contador+1
        sys.stdout.write ("\r" + f"Execucao {contador} de {total}")
        sys.stdout.flush()

        try:
            result =requests.get(url_arquivo)
            if result.status_code > 200:
                lista.append(origem +";"+sku +";"+url_arquivo + str(result.status_code))
                #print(origem +";"+sku +";"+url_arquivo, result.status_code)
        except:
            lista.append("except =" + origem + ";"+sku +";"+url_arquivo + str(result.status_code))
    
    return lista

# COMMAND ----------

lst = []
lst.append(valida_url(sql))
print (lst)

# COMMAND ----------

# MAGIC %md
# MAGIC ###VALIDA URL SITE

# COMMAND ----------

import requests
from  urllib.parse  import quote
import time
import sys
from pyspark.sql.functions import col, regexp_replace, concat

##---------------------------------------------------------------------------------------------------------------------
## CONEXÃO PROGRESS
##---------------------------------------------------------------------------------------------------------------------

jdbcUsername = 'usrdecasite' #dbutils.secrets.get(scope = "AWS_REDSHIFT", key = "DTX_DECA_SELLIN_USER")
jdbcPassword = 'RN3wZgqrRtCf' #dbutils.secrets.get(scope = "AWS_REDSHIFT", key = "DTX_DECA_SELLIN_PASSWORD")
jdbcHostname = 'db-site-deca-db.cetutwvkvrhj.us-east-1.rds.amazonaws.com' #dbutils.secrets.get(scope = "AWS_REDSHIFT", key = "DTX_DECA_SELLIN_HOST")
jdbcPort = '5432' #(dbutils.secrets.get(scope = "AWS_REDSHIFT", key = "DTX_DECA_SELLIN_PORT"))
jdbcDatabase = 'dbsitedeca'#dbutils.secrets.get(scope = "AWS_REDSHIFT", key = "DTX_DECA_SELLIN_DATABASE")
jdbcUrl = f'jdbc:postgresql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?user={jdbcUsername}&password={jdbcPassword}'

#tempdir = multiple_run_parameters['bucket_s3'] or 'dexco-dev-data-transfer'
tempdir = 'dexco-dev-data-transfer'
pathname = '/tmp/new_dbsitedeca/'

##---------------------------------------------------------------------------------------------------------------------
## RECUPERA DADO DO SITE
##---------------------------------------------------------------------------------------------------------------------

## --> in (1086,1174,1559 ) dados em duplicidade gerando uma coluna preenchida com o url_mrk_place e outra nao
sql = """
select p.id id_site 
	  ,p.material_code as sku
	  ,p.keyword as palavra_chave
	  ,p.related_product as produto_relacionado 
	  ,p.url 
	  ,p.url_mktplace as url_marketplace_b2c
	  ,cuba.title as tipo_cuba 
	  ,arc.title as assinatura_arquiteto
	  ,arc.permalink as arquiteto_permalink
	  ,arc.about as sobre_arquiteto
	  ,arc.about_line as sobre_linha
	  ,drive_format.title as formato_acionamento  
 from public.products p 
 left join public.products_cuba_type_links cuba_link on p.id  = cuba_link.product_id 
 left join public.product_field_cuba_types cuba on  cuba_link.product_field_cuba_type_id = cuba.id 
 left join public.products_architect_signature_links arc_link on p.id = arc_link.product_id 
 left join public.architects arc on arc_link.architect_id = arc.id 
 left join public.products_drive_format_links drive_link on p.id = drive_link.product_id 
 left join public.product_field_drive_formats drive_format on drive_link.product_field_drive_format_id = drive_format.id
 where p.id not in (1086,1174,1559 )
"""


df_strapi_site = spark.read \
  .format("jdbc") \
  .option("url", jdbcUrl) \
  .option("query", sql) \
  .option("user", jdbcUsername) \
  .option("password", jdbcPassword) \
  .option("driver", "org.postgresql.Driver") \
  .load()
df_strapi_site.createOrReplaceTempView("vw_strapi_site_dado_temporario")


##---------------------------------------------------------------------------------------------------------------------
## CRIA URL_CATALOGO_INSITUTCIONAL - SEM https://www.deca.com.br
##---------------------------------------------------------------------------------------------------------------------

prefixo='https://www.deca.com.br/'

# Crie uma exibição temporária do DataFrame
df_strapi_site.createOrReplaceTempView("vw_strapi_site_dado_temporario")

df_url_site=spark.sql ( f"""
   select 
       tmp_site.*
       ,concat('ambientes/'
              ,lower(replace(concat(hie.nivel1,"/",hie.nivel2,"/",hie.descricao,"/")," ","-")))   url_catalogo_institucional 
       ,current_timestamp() data_criacao
from  trust.produtos.hierarquia_site_link hie_link
inner join  trust.produtos.hierarquia as hie  on  hie_link.hierarquia =hie.hierarquia
inner join  vw_strapi_site_dado_temporario tmp_site on tmp_site.sku=hie_link.sku
where ( url is not null  
       and  hie.nivel1 is not null
       and  hie.nivel2 is not null
       and hie.descricao is not null)
""").withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[áàâäã]", "a")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[éèêë]", "e")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[íìîï]", "i")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[óòôöõ]", "o")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[úùûü]", "u")) \
    .withColumn("url_catalogo_institucional", regexp_replace("url_catalogo_institucional", "[ç]", "c"))\
    .withColumn("url_catalogo_institucional", concat(col("url_catalogo_institucional"),col("url")))\
        .drop("url")\
  .select("id_site"
          ,"sku"
          ,"palavra_chave"
          ,"produto_relacionado"
          ,"url_catalogo_institucional"
          ,"url_marketplace_b2c"
          ,"tipo_cuba"
          ,"assinatura_arquiteto"
          ,"arquiteto_permalink"
          ,"sobre_arquiteto"
          ,"sobre_linha"
          ,"formato_acionamento"
          ,"data_criacao").orderBy("id_site")
  
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
    for url_i,sku in  df.select("url_catalogo_institucional","sku" ).collect():
        contador=contador+1
        sys.stdout.write ("\r" + f"Execucao {contador} de {total}")
        sys.stdout.flush()
        url = prefixo + quote(url_i)

        result =requests.get(url)
        if result.status_code > 200:
            lista.append(sku +";"+url)
    return lista

##---------------------------------------------------------------------------------------------------------------------
## EXECUTA FUNCAO COM PRINT DOS URL COM RETORNO 404
##---------------------------------------------------------------------------------------------------------------------

lista_retorno =valida_url(df_url_site)


# COMMAND ----------

for url in lista_retorno:
    #url_f = ((url).split(';')[1])
    #result = requests.get(url_f)
    print (url)
