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
spark.sql("SET TIME ZONE 'America/Sao_Paulo'").display()

# COMMAND ----------

# DBTITLE 1,Classe Produto
class Dados_Produto:
    # Classe para trabalhar com dados de Produtos
    def __init__(self,msg):
        print(u"\u2192",msg)

    #Função para gerar filtro a partir do "id_atributo" da tabela de pametro    
    def Gerar_Filtro_Lista(self,filtro_saida, sistema_destino):

        self.msg = "Gerando Filtro lista"
        self.filtro_saida = filtro_saida
        self.sistema_destino = sistema_destino.lower()

        #carrega tabela de entrada
        df_entrada = spark.table("trust.produtos.filtro_entrada")
        
        #carrega tabela de saida
        df_param = spark.table("trust.produtos.filtro_saida") \
                .select(col("sistema_destino")
                       ,col("id_atributo_catalogo").alias("atributo")
                       ,col("id_atributo_saida").alias("atributo_saida")) \
                .filter((lower(col("sistema_destino")) == "strapi-site_deca" ) & (col("ativo")== True))
        
        #gerar filtro de saida
        lst_param = df_param.join(df_entrada,
                                   on = df_param.atributo == df_entrada.id_atributo_catalogo,
                                   how = "inner" ) \
                                .select(col("id_atributo_entrada"),
                                        col("atributo"),
                                        col("atributo_saida"))
        
        #transforma df em lista
        lst_filter_series = lst_param.select(lst_param.id_atributo_entrada).to_koalas()['id_atributo_entrada']
        lst_filter_atrib = lst_filter_series.tolist()

        #lst_ini = gerar_filtro(lst,'trust.produtos.catalogo_atributo')
        lst_atrib = ['MARCA','STATUS_EXIB_SITE','STATUS_VENDA']

        for i in lst_filter_atrib:
            lst_atrib.append(i.upper())

        self.lst_atrib = list(set(lst_atrib))
        #print(sorted(self.lst_atrib))
        return self.lst_atrib
    
    #### Carregar Atributos SAP
    def pivot_sap(self, filtro, tab_atributo):
        
        self.tab_atributo = tab_atributo

        #Carrega Dados de Atributos
        sap_attributes = spark.table(self.tab_atributo) \
                            .filter (upper(col("atributo")) \
                            .isin(filtro)) 

        #Pivot Table para criar BASE CONDIÇÃO
        pvt_sap_attributes = sap_attributes.groupBy("sku") \
                                 .pivot("atributo") \
                                 .agg({'valor':'max'})

        #print(sap_attributes)
        return pvt_sap_attributes


# COMMAND ----------

def Gerar_Tabelas_Pivot(tab_filtro_saida, sistema_destino):

    lst_tabelas_sap = ['trust.produtos.atributo_string', 'trust.produtos.atributo_binary', 'trust.produtos.atributo_date', 'trust.produtos.atributo_decimal', 'trust.produtos.atributo_int', 'trust.produtos.atributo_timestamp']

    # Gera Filtro de saida a partir da tabela de Parametros
    filtro = Dados_Produto('Gerando Lista Filtro').Gerar_Filtro_Lista(tab_filtro_saida, sistema_destino)

    # Faz o Pivot de Todas as tabelas de Atributos dentro do laço de repetição
    for tab in lst_tabelas_sap:

        if tab == 'trust.produtos.atributo_string':
            pvt_atributo_string = Dados_Produto('Carregando Atributo String').pivot_sap(filtro,tab)
            if pvt_atributo_string.count() > 0:
                pvt_atributo_string.select(pvt_atributo_string.sku).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("trust.produtos.  atributo_link_join")
            pvt_atributo_string = pvt_atributo_string.withColumnRenamed("sku","sku_string")

        if tab == 'trust.produtos.atributo_decimal':
            pvt_atributo_decimal = Dados_Produto('Carregando Atributo Decimal').pivot_sap(filtro,tab)
            if pvt_atributo_decimal.count() > 0:
                pvt_atributo_decimal.select(pvt_atributo_string.sku).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("trust.produtos. atributo_link_join")
            pvt_atributo_decimal = pvt_atributo_decimal.withColumnRenamed("sku","sku_decimal")
    
        if tab == 'trust.produtos.atributo_int':
            pvt_atributo_int = Dados_Produto('Carregando Atributo Inteiro').pivot_sap(filtro,tab)
            if pvt_atributo_int.count() > 0:
                pvt_atributo_int.select(pvt_atributo_string.sku).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("trust.produtos.atributo_link_join")
            pvt_atributo_int = pvt_atributo_int.withColumnRenamed("sku","sku_int")
    
        if tab == 'trust.produtos.atributo_timestamp':
            pvt_atributo_timestamp = Dados_Produto('Carregando Atributo Timestamp').pivot_sap(filtro,tab)
            if pvt_atributo_timestamp.count() > 0:
                pvt_atributo_timestamp.select(pvt_atributo_string.sku).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("trust.produtos. atributo_link_join")
            pvt_atributo_timestamp = pvt_atributo_timestamp.withColumnRenamed("sku","sku_timestamp")

        if tab == 'trust.produtos.atributo_date':
            pvt_atributo_date = Dados_Produto('Carregando Atributo Date').pivot_sap(filtro,tab)
            if pvt_atributo_date.count() > 0:
                pvt_atributo_date.select(pvt_atributo_string.sku).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("trust.produtos.  atributo_link_join")
            pvt_atributo_date = pvt_atributo_date.withColumnRenamed("sku","sku_date")

        if tab == 'trust.produtos.atributo_binary':
            pvt_atributo_binary = Dados_Produto('Carregando Atributo Binary').pivot_sap(filtro,tab)
            if pvt_atributo_binary.count() > 0:
                pvt_atributo_binary.select(pvt_atributo_string.sku).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("trust.produtos.  atributo_link_join")
            pvt_atributo_binary = pvt_atributo_binary.withColumnRenamed("sku","sku_binary")

    # Faz o Join entre Todas as Tabelas Geradas a partir da função acima
    def Join_Tables():
        print (u"\u2192","Executando Join's")

        link = spark.read.table("trust.produtos.atributo_link_join").select("sku")

        pvt_final = link.alias("link").join(pvt_atributo_string.alias("string"),
                                    on = link.sku == pvt_atributo_string.sku_string,
                                    how = "left" ) \
                                .join(pvt_atributo_decimal.alias("decimal"), \
                                    on= link.sku == pvt_atributo_decimal.sku_decimal, \
                                    how = "left") \
                                .join(pvt_atributo_int.alias("int"), \
                                    on= link.sku == pvt_atributo_int.sku_int, \
                                    how = "left")  \
                                .join(pvt_atributo_timestamp.alias("timestamp"), \
                                    on= link.sku == pvt_atributo_timestamp.sku_timestamp, \
                                    how = "left")  \
                                .join(pvt_atributo_date.alias("date"), \
                                    on= link.sku == pvt_atributo_date.sku_date, \
                                    how = "left")  \
                                .join(pvt_atributo_binary.alias("binary"), \
                                    on= link.sku == pvt_atributo_binary.sku_binary, \
                                    how = "left")

        produto = pvt_final.drop(*("sku_string", "sku_decimal","sku_int","sku_timestamp","sku_date","sku_binary"))
        return produto

    #Retorna a Função Join que está retonando os dados de Produtos
    return Join_Tables()


# COMMAND ----------

# DBTITLE 1,Ajusta Colunas e Medidas 
def Ajusta_Medidas(produto):

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
    return produto
