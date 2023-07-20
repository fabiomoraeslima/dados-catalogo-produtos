# Databricks notebook source
import os
import pytz
import datetime
import pyspark
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql import SparkSession
spark.sql("SET TIME ZONE 'America/Sao_Paulo'")

# COMMAND ----------

# DBTITLE 1,Classe Produto
class Dados_Produto:
        # Classe para trabalhar com dados de Produtos
    def __init__(self,msg):
        print(u"\u2192",msg)

    def cria_lista_param(self,sistema_destino):

        self.msg = "Gerando Filtro"
        self.sistema_destino = sistema_destino.lower()
        # Executa consulta no Filtro de Saida, Catalogo de Dados e Filtro de Saida
        lst_param = spark.sql(f"""
                select lower(id_atributo_entrada) as id_atributo_entrada
                      ,lower(atributo_saida) as atributo_saida
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
                     join trust.produtos.catalogo_atributo catalogo on lower(saida.id_atributo_catalogo) = lower(catalogo.  id_atributo_catalogo)
                     join trust.produtos.filtro_entrada entrada on lower(saida.id_atributo_catalogo) = lower(entrada.   id_atributo_catalogo)
                     where lower(saida.sistema_destino) = lower("{self.sistema_destino}")
                       and saida.ativo = 1
                       and entrada.ativo = 1
                 )
           """).collect()

        return lst_param
    
    def pivot_sap(self, tab_atributo, lst_param):

        self.tab_atributo = tab_atributo
        self.lst_param = lst_param
        #lista de atributos no filtro de entrada com base na aplicação
        lst_atrib_ent = [col['id_atributo_entrada'] for col in self.lst_param]

        #Carrega Dados de Atributos
        sap_attributes = spark.table(self.tab_atributo) \
                            .filter(lower(col("atributo")).isin(lst_atrib_ent))


        #Pivot Table para criar BASE CONDIÇÃO
        pvt_sap_attributes = sap_attributes.groupBy("sku") \
                                     .pivot("atributo") \
                                     .agg({'valor':'max'})

        #print(sap_attributes)
        return pvt_sap_attributes

# COMMAND ----------

# DBTITLE 1,Gerar Dataframe Produtos
def Gerar_Tabelas_Pivot(lst_param):
    #Lista Todas as tabelas no schema trust produtos
    lst_tables = spark.sql("show tables in trust.produtos") \
                        .filter((col("tableName").like('atributo_%')) 
                              & (~col("tableName").like('%_link%'))).collect()

    #Acrescenta catálogo e schema nas tabelas
    lst_tabelas_sap = ['trust.produtos.' + t['tableName'] for t in lst_tables if 'atributo_' in t['tableName']]

    #Laço para criar uma tabela de link
    for i in range(len(lst_tabelas_sap)):
        for tab_atributo in lst_tabelas_sap:
            if spark.table(tab_atributo).count() > 0:
                if i == 0:
                    df_link_join = spark.table(tab_atributo).select(trim(col("sku")).alias('sku'))
                else:
                    df_append = spark.table(tab_atributo).select(trim(col("sku")).alias('sku'))
                    df_link_join = df_link_join.union(df_append)

    df_link_join = df_link_join.distinct()

    def Join_Tables(df_link_join, lst_param):
        for i, tabela in enumerate(lst_tabelas_sap[1:], start=1):
            if spark.table(tabela).count() > 0:
            
                #print(u"\u2192", f"{i} - Executando: {tabela}")
                df_x = Dados_Produto(f"{i} - Executando: {tabela}").pivot_sap(tabela,lst_param) 
                df_link_join = (df_link_join.join(df_x.alias('update'),
                             on =(df_link_join.sku == df_x.sku),
                             how = 'left')).drop(df_x.sku)
                df_link_join.count()
        
        produto = df_link_join.distinct()

        return produto

    #Retorna a Função Join que está retonando os dados de Produtos
    return Join_Tables(df_link_join, lst_param)

# COMMAND ----------

# DBTITLE 1,Ajusta Colunas e Medidas 
def Ajusta_Medidas(produto,define_uom):

    print(u"\u2192","Ajustando Colunas de Medidas")
    #cria lista de campos que podem conter unidade de medida
    lst_col = spark.sql("select distinct id from gaia.attributes where uom is not null and trim(uom) <> ''")
    lst_col_series = lst_col.select(lst_col.id).to_koalas()['id']
    lst_col_f = lst_col_series.tolist()

    #valida as colunas que existem na tabela de saida com unidade de medida
    for c in produto.columns:
        produto = produto.withColumnRenamed(c,c.lower())
        if c in lst_col_f:
            v_col = c.lower()
            v_col_uom = c.lower() + '_unid'
            produto_uom = produto.withColumn(f"{v_col_uom}",split(f"{v_col}",' ')[1]) \
                              .withColumn(f"{v_col}",split(f"{v_col}",' ')[0])

        else:
            produto_uom = produto.withColumnRenamed(c,c.lower())
    
    print (u"\u2192","Fim ...")
    
    #unidade medida junto
    if define_uom == 1:
        return produto
    else:
        return produto_uom

# COMMAND ----------

# DBTITLE 1,Classe Tabela
class Tabela:
    # Classe para trabalhar com dados de Produtos
    def __init__(self,msg):
        print(" ")
    
    def create_table(self,tabela, lst_param):

        # Formatar a lista de colunas e tipos de dados
        colunas_formatadas = [col['atributo_saida'] for col in lst_param]
        tipos_formatados = [tipo['tipos'] for tipo in lst_param]
        coment_formatado = [descricao['descricao'] for descricao in lst_param]

        # Gerar a instrução SQL CREATE TABLE
        sql_create_table = f"""
            create table if not exists {tabela} (
                {', '.join([f"{col} {tipo} comment '{coment}'" for col, tipo, coment in zip(colunas_formatadas,     tipos_formatados, coment_formatado)])}
            )
        """

        # Executar a instrução SQL
        spark.sql(f"drop table if exists {tabela}")
        spark.sql(sql_create_table)

        print(f"Tabela -> {tabela} criada com sucesso!!")
        #return lst_param

    def update_table(self, tabela,aplicacao,df_produto,lst_param):
        self.tabela = tabela
        #self.aplicacao = aplicacao
        self.df_prod = df_produto
        self.lst_param = lst_param

        #executa function create_table
        Tabela('').create_table(self.tabela, self.lst_param)

        print(f"-> Renomeando as colunas")

        #renomeia as colunas do dataframe antes de fazer o insert
        for c in lst_param:
            if c["id_atributo_entrada"] != None:
                column = c["id_atributo_entrada"]
                new_column = c["atributo_saida"]
                df_prod = self.df_prod.withColumnRenamed(column, new_column)

        columns_to_cast = df_prod.columns

        print(f"-> Aplicando cast nas colunas")
        #Aplica os cast em todas as colunas
        for column in columns_to_cast:
            for i in lst_param:
                if column == i["atributo_saida"]:
                    datatype = i["tipos"]
                    df_prod = df_prod.withColumn(column, col(column).cast(f"{datatype}"))

        print(f"-> Inserindo Dados na Tabela {self.tabela}")
        df_prod.distinct() \
        .write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{self.tabela}")

        print(f"-> Fim ")
        #return lst_param
