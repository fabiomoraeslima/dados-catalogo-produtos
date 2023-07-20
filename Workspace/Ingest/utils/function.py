# Databricks notebook source
pip install pymongo 

# COMMAND ----------

import pymongo
from pymongo import MongoClient
from bson import json_util
from pyspark.sql.functions import col,lower
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Função para carregar dados do Mongo Gaia
def Read_Mongo(collect, projections, filtro, table):


    mongo_jdbc_url_read = dbutils.secrets.get(scope = "MONGODB-GAIA", key = "URL_JBDC_READ_GAIA")

    client = pymongo.MongoClient(mongo_jdbc_url_read)
    dbase = client["product"]

    # Read collection
    collection = dbase[collect]

    # Obtém todos os documentos da collection
    documents = collection.find(filtro, projection=projections)

    # Grava os documentos em um arquivo JSON
    with open(f"/tmp/mongo_record_{table}.json", "w") as outfile:
        outfile.write(json_util.dumps(documents))

    # Realiza a copia do arquivo gerado
    dbutils.fs.cp(f"file:/tmp/mongo_record_{table}.json",
                  f"dbfs:/FileStore/mongo_record_{table}.json")

    # Le o arquivo json e carrega na tabela de injestão
    spark.read.format('org.apache.spark.sql.json') \
        .option('multiline', 'true') \
        .load(f"dbfs:/FileStore/mongo_record_{table}.json") \
        .write.mode('overwrite') \
        .option('mergeSchema', 'true') \
        .saveAsTable(f"{table}")

    return print(f"Total de linhas inseridas na Tabela {table}:", spark.table(f"{table}").count())

# COMMAND ----------

# DBTITLE 1,Função cria Filtro Entrada
def filtro_entrada():
    #careega tabela de filtro
    df_filtro_en = spark.table("trust.produtos.filtro_entrada") \
                            .select(col("id_atributo_entrada")) \
                            .filter(col("ativo")==1)\
                            .distinct()

    #tranforma coluna em lista
    ls_filtro_en_series = df_filtro_en.select(df_filtro_en.id_atributo_entrada).to_koalas()['id_atributo_entrada']
    lst_filtro_en = ls_filtro_en_series.tolist()
    
    return lst_filtro_en

# COMMAND ----------

# DBTITLE 1,Renomeia Colunas usando Filtro Saída
def Rename_Columns_Table(table_rename):

    # Inicializa o SparkSession
    spark = SparkSession.builder.getOrCreate()

    table_param = "trust.produtos.filtro_entrada"    
    column = 'id_atributo_entrada'
    new_column =  'id_atributo_catalogo'


    df_table = spark.table(f"{table_rename}")
    ls = spark.table(f"{table_param}").select(*(f"{column}",f"{new_column}")).collect()

    dict_rename = {}
    for linha in ls:
        # Acesso às colunas da linha pelo nome da coluna
        chave = linha[column]
        valor = linha[new_column]
        dict_rename[chave.lower()] = valor.lower()

    for col in df_table.columns:
        col=col.lower() 
        if col in dict_rename:
            print(f"{col} -> {dict_rename[col]}")
            df_table = df_table.withColumnRenamed(col, dict_rename[col])

    return df_table

# COMMAND ----------

# DBTITLE 1,Criar tabela usando Catalogo de Dados : Função executada pela "update_table"
def create_table(tabela, aplicacao):

    # Executa consulta no Filtro de Saida, Catalogo de Dados e Filtro de Saida
    lst_param = spark.sql(f"""
            select id_atributo_entrada
                  ,atributo_saida
                  ,case when casa_decimal is not null then concat (tipos, ' ', casa_decimal)
                        else tipos
                  end tipos
                  ,descricao
             from (
              select entrada.id_atributo_entrada 
                      ,nvl(saida.id_atributo_saida,saida.id_atributo_catalogo) atributo_saida
                      ,case when lower(catalogo.tipo_dado_atributo) = 'lov' then 'string'
                            when lower(catalogo.tipo_dado_atributo) = 'number' then 'numeric'
                            when lower(catalogo.tipo_dado_atributo) = 'isodate' then 'timestamp'
                            when catalogo.tipo_dado_atributo is null then 'string'
                        else catalogo.tipo_dado_atributo
                      end tipos
                      ,catalogo.casa_decimal
                      ,catalogo.descricao
                 from trust.produtos.filtro_saida saida
             left join trust.produtos.catalogo_atributo catalogo on lower(saida.id_atributo_catalogo) = lower(catalogo.id_atributo_catalogo)
             left join trust.produtos.filtro_entrada entrada on lower(saida.id_atributo_catalogo) = lower(entrada.id_atributo_catalogo)
                 where lower(saida.sistema_destino) = lower("{aplicacao}")
                   and saida.ativo = 1
             )
       """).collect()

    # Formatar a lista de colunas e tipos de dados
    colunas_formatadas = [col['atributo_saida'] for col in lst_param]
    tipos_formatados = [tipo['tipos'] for tipo in lst_param]
    coment_formatado = [descricao['descricao'] for descricao in lst_param]

    # Gerar a instrução SQL CREATE TABLE
    sql_create_table = f"""
        create table if not exists {tabela} (
            {', '.join([f"{col} {tipo} comment '{coment}'" for col, tipo, coment in zip(colunas_formatadas, tipos_formatados, coment_formatado)])}
        )
    """

    # Executar a instrução SQL
    spark.sql(f"drop table if exists {tabela}")
    spark.sql(sql_create_table)

    print(f"Tabela -> {tabela} criada com sucesso!!")
    return lst_param

# COMMAND ----------

# DBTITLE 1,Atualiza datatypes do Daframe e insere os dados na tabela
def update_table(tabela,aplicacao,df):

    df_prod = df

    #executa function create_table
    lst_param = (create_table(tabela, aplicacao))

    print(f"-> Renomeando as colunas")

    #renomeia as colunas do dataframe antes de fazer o insert
    for c in lst_param:
        if c["id_atributo_entrada"] != None:
            column = c["id_atributo_entrada"]
            new_column = c["atributo_saida"]
            df_prod = df_prod.withColumnRenamed(column, new_column)

    print(f"-> Aplicando os Casts")
   
    columns_to_cast = df_prod.columns

     #Aplica os cast em todas as colunas
    for column in columns_to_cast:
        for i in lst_param:
            if column == i["atributo_saida"]:
                datatype = i["tipos"]
                df_prod = df_prod.withColumn(column, col(column).cast(f"{datatype}"))

    print(f"-> Inserindo os dados na Tabela {tabela}")
    df_prod.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(tabela)

# COMMAND ----------

# DBTITLE 1,Data Quality - Consiste se coluna iguais entre 2 dataframes possuem valor do dado distinto


def DataQuality_ComparaDadoColuna(df_01,df_02,df_03_link,col_pk,col_compara,prefixo_nome_saida_col01,prefixo_nome_saida_col02):
#--------------------------------------------------------------------------------------------------------------------------  
# Criado por..............:  Luciano Fidelis
# Data....................:  05/06/2023
# df_01...................:  Dataframe que vai ter coluna comparada com df_02
# df_02...................:  Dataframe que vai ter coluna comparada com df_01
# df_03_link..............:  Dataframe que com arelação de todas as colunas chaves informada na col_pk
# col_pk..................:  Col PK relaciona  df_01 ,df_02 e df_link (não aceita chave composta) ex. df01.sku=df.02.sku=df_lin.sku
# col_compara.............:  coluna que sera comparada entre df_01 e df_02 geralmente recuperada de um loop  ex.   for c in  df.columns
# prefixo_nome_saida_col01:  para nao conflitar ambiquidade de coluna  deve fornecer um prefixo para o nome da coluna  da saida df_01
# prefixo_nome_saida_col02:  para nao conflitar ambiquidade de coluna  deve fornecer um prefixo para o nome da coluna  da saida df_02
#--------------------------------------------------------------------------------------------------------------------------  
    from pyspark.sql.functions import col, lower, trim, when
    
    ##----------------------------------------------------------------------------
    ##Recupera dado da col_compara do site
    ##----------------------------------------------------------------------------
    t_df01 =df_01.alias("t_df01")
    t_df02 =df_02.alias("t_df02")
    t_link =df_03_link.alias("t_link") 
    
    ##----------------------------------------------------------------------------
    ##Recupera dado da col_compara  
    ##----------------------------------------------------------------------------
    
    df01_link=t_df01.select(col(col_pk),col(col_compara)).join(t_link, on =col(f"t_df01.{col_pk}") ==col(f"t_link.{col_pk}"),how='inner').drop(col(f"t_link.{col_pk}")).alias("df01_link")
    
    df02_link=t_df02.select(col(col_pk),col(col_compara)).join(t_link, on =col(f"t_df02.{col_pk}") ==col(f"t_link.{col_pk}"),how='inner').drop(col(f"t_link.{col_pk}")).alias("df02_link")
    
    ##----------------------------------------------------------------------------
    ##Compara dado das col_comparas  e cria df_retorno
    ##---------------------------------------------------------------------------- 
    ax=df01_link.select(f"df01_link.{col_compara}")
    df_retorno_saida = df02_link.join(df01_link,
                                 on=[col(f"df02_link.{col_pk}") == col(f"df01_link.{col_pk}")],
                                 how='inner') \
                          .where(lower(trim(when(col(f"df01_link.{col_compara}").isNull(), '-1')
                                           .otherwise(trim(lower(col(f"df01_link.{col_compara}")))))) !=
                                 lower(trim(when(col(f"df02_link.{col_compara}").isNull(), '-1')
                                             .otherwise(trim(lower(col(f"df02_link.{col_compara}")))))))\
                      .select (f"df01_link.{col_pk}",
                                    trim(f"df01_link.{col_compara}").alias(f"{prefixo_nome_saida_col01}_{col_compara}"),   
                                        trim(f"df02_link.{col_compara}").alias(f"{prefixo_nome_saida_col02}_{col_compara}")   
                               
                               ) 
                      
    ##----------------------------------------------------------------------------
    ## # Liberar memória do DataFrame 
    ##----------------------------------------------------------------------------                       
    df_01.unpersist()
    df_02.unpersist()
    t_link.unpersist()  
    t_df01.unpersist()          
    t_df02.unpersist()  
    df01_link.unpersist()               
    df02_link.unpersist()               

    return df_retorno_saida
	
	
