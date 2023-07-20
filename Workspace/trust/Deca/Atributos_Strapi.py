# Databricks notebook source
# MAGIC %md
# MAGIC <h1> DEXCO <h1>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Objetivo 
# MAGIC <p/>
# MAGIC
# MAGIC * Aplicar transformações e disponibilizar dados do site para o banco Postgree. 
# MAGIC <p>
# MAGIC
# MAGIC  
# MAGIC `Este notebook faz parte do processo de geração dos Dados de Produtos Dexco`

# COMMAND ----------

# MAGIC %md
# MAGIC #Controle Versao 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC |Data| Autor| Acao| Alteracao| 
# MAGIC |----| ---- | ---- | ---- |
# MAGIC |01/06/2023|Fabio Moraes|Criação Notebook| | 
# MAGIC |07/07/2023|Luciano Fidelis|Alteração|Alterada Regra de criação do URL Institucional - Limpando do nome do produto os caracteres especiais `"\"` e `"`  | 
# MAGIC |07/07/2023|Luciano Fidelis|Alteração|Regra url marketplace deve ser recuperado do sistema vtex tabela ingest.marketplace_vtex.skus  |
# MAGIC |12/07/2023|Fabio Moraes|Alteração|Filtro validando hierarquia Site e Alteração na regra de criação da url independente dos níveis de hierarquia preenchidos|

# COMMAND ----------

# MAGIC %md
# MAGIC #Importa Funcões

# COMMAND ----------

# MAGIC %run ../../Ingest/utils/function

# COMMAND ----------

# MAGIC %run ../../Ingest/utils/Produtos_Dexco

# COMMAND ----------

# MAGIC %md
# MAGIC #Importa Bibliotecas

# COMMAND ----------

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
# MAGIC # Processo ETL Produtos-Strapi
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tabelas de Leitura

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  
# MAGIC
# MAGIC
# MAGIC <b>Tabelas de Leitura</b>: 
# MAGIC |Catalogo|Owner|Tabela|
# MAGIC | --- | --- | --- |
# MAGIC |trust|produtos|parametro|
# MAGIC |trust|produtos|atributo_string| 
# MAGIC |trust|produtos|atributo_binary|
# MAGIC |trust|produtos|atributo_date| 
# MAGIC |trust|produtos|atributo_decimal| 
# MAGIC |trust|produtos|atributo_int|
# MAGIC |trust|produtos|atributo_timestamp|
# MAGIC |trust|produtos|documento|
# MAGIC |trust|produtos|imagem|
# MAGIC |trust|produtos|referencia|
# MAGIC |ingest|marketplace_vtex|skus|
# MAGIC
# MAGIC
# MAGIC
# MAGIC `TK_2023-05-02`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Tabelas Atualizadas

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC <b>Tabelas Atualizadas</b>: 
# MAGIC |Catalogo|Owner|Tabela|
# MAGIC | --- | --- | --- |
# MAGIC |trust|produtos|produto_strapi|
# MAGIC |postgre|delta|produto_delta
# MAGIC |postgre|delta|hierarquia
# MAGIC |postgre|delta|hierarquia_link
# MAGIC |postgre|delta|imagem_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gera Tabela Produtos Dexco

# COMMAND ----------

# MAGIC %md
# MAGIC ###ETL Produtos

# COMMAND ----------

#Carrega variáveis (Tabela que será criada e qual aplicacao irá utilizar)
sistema_destino = "strapi-site_deca"
tabela = "trust.produtos.produto_strapi"
# 1 para unidade de medida junta e 2 separada
define_uom = 1

# COMMAND ----------

# DBTITLE 1,Executando Funções : Ingest/utils/Produtos_Dexco
lst_param = Dados_Produto('Gerando Lista Filtro').cria_lista_param(sistema_destino)
df_produto = Ajusta_Medidas(Gerar_Tabelas_Pivot(lst_param),define_uom)

df_produto = df_produto.drop_duplicates()

# COMMAND ----------

# DBTITLE 1,Filtro Site (uom concatenada com valor)
#somente produtos liberados para o site
produto = (df_produto
            .filter(lower(trim(col("STATUS_EXIB_SITE")))=='sim')).distinct()

#somente skus que possuem hierarquia site 
lst_hierarquia_site = []

lst_sku = spark.table("trust.produtos.hierarquia_site_link").alias('link') \
           .join(spark.table("trust.produtos.hierarquia").alias('hierarquia'), 
                 on = (col("link.hierarquia") == col("hierarquia.hierarquia")), 
                 how="inner"
).filter(col("hierarquia.nivel1").isNotNull() &
         col("hierarquia.nivel2").isNotNull()
         ).select("sku").collect()

for sku in lst_sku:
    lst_hierarquia_site.append(sku["sku"])

produto = produto.filter(col("sku").isin(lst_hierarquia_site))

# COMMAND ----------

# MAGIC %md
# MAGIC ###ETL Documentos

# COMMAND ----------

df_doc = spark.sql("""
select q2.sku 
      ,q2.marca
      ,'documento' tipo_arquivo
      ,q2.id1 as id_arquivo
      ,q2.name1 as nome_arquivo
      ,inline(values)
      ,lower(id) as atributo
      ,q2.name
      ,lower(replace(split(split(q2.type,",")[0],',')[0],'{','')) id_ref
      ,replace (split(q2.type,",")[1],'}','') descricao
      ,q2.url
      ,q2.version as versao
from 
(
select q1.sku 
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
""").distinct()

# COMMAND ----------

# DBTITLE 1,Pivot Documentos
#Pivot Table para criar BASE CONDIÇÃO
produto_documento = df_doc.groupBy("sku","marca","tipo_arquivo") \
                 .pivot("id_ref") \
                 .agg({'url':'max'}).distinct()

produto_documento = produto_documento.withColumnRenamed("sku", "doc_sku") \
                                     .withColumnRenamed("marca", "doc_marca")

# COMMAND ----------

# DBTITLE 1,Update Produto + Documento Strapi
df_produto = produto.join(produto_documento,
                            on = produto.sku == produto_documento.doc_sku, 
                            how = "left") \
                                                 
produto_strapi = df_produto.drop("doc_sku","doc_marca").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###ETL Referencia

# COMMAND ----------


referencia =spark.table("trust.produtos.referencia").withColumnRenamed("sku","ref_sku")
produto_strapi = produto_strapi.join(referencia, on =produto_strapi.sku==referencia.ref_sku, how='left').drop("ref_sku") 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cria URL Institucional

# COMMAND ----------

# DBTITLE 1,URL Institucional
strapi_site_tmp = spark.sql("""select distinct 
       r.sku ref_sku
      ,replace (translate(lower
               (concat ('https://www.deca.com.br/ambientes/',
               trim(r.nivel1)
              ,trim(r.nivel2)
              ,trim(r.nivel3)
              ,trim(r.nivel4)
              ,nvl(r.sufixo_hist, r.sufixo))
       ),'áàâãäéèêëíìîïóòôõöúùûüç', 'aaaaaeeeeiiiiooooouuuuc'),'"','') url_catalogo_institucional
       ,r.palavra_chave
       ,r.produto_relacionado
       ,r.assinatura_arquiteto
from (
select produto.sku
     ,case when trim (hierarquia.nivel1) <> '' 
            then concat(replace(hierarquia.nivel1,' ','-'), '/')
        else concat (replace(hierarquia.descricao,' ','-'),'/')
    end nivel1
     ,case when trim (hierarquia.nivel2) <> '' 
            then  concat(replace(hierarquia.nivel2,' ','-'), '/')
           when trim (hierarquia.nivel1) = '' then  '' 
      else  concat (replace(hierarquia.descricao,' ','-'),'/')  
    end nivel2
     ,case when trim (hierarquia.nivel3) <> '' 
            then concat(replace(hierarquia.nivel3,' ','-'), '/')
           when trim (hierarquia.nivel2) = '' then  '' 
      else  concat (replace(hierarquia.descricao,' ','-'),'/') 
    end nivel3
     ,case when trim (hierarquia.nivel4) <> '' 
            then concat(replace(hierarquia.nivel4,' ','-'), '/')
         when trim (hierarquia.nivel3) = '' then  ''   
      else  concat (replace(hierarquia.descricao,' ','-'),'/')
    end nivel4
    , concat(replace(replace(replace(replace(trim(nome_produto),'"',''),'/',''),' ','-'),'.',''), 
                    replace(produto.sku,'.','')) sufixo
    ,hist.url as sufixo_hist
    ,hist.palavra_chave
    ,hist.produto_relacionado
    ,hist.assinatura_arquiteto
 from trust.produtos.produto_strapi produto
     join trust.produtos.hierarquia_site_link link on produto.sku = link.sku
     join trust.produtos.hierarquia hierarquia on link.hierarquia = hierarquia.hierarquia
left join trust.produtos.strapi_url_site_historico hist on produto.sku = hist.sku
) r
  """)

produto_strapi = produto_strapi.join(strapi_site_tmp
                                    , on =produto_strapi.sku==strapi_site_tmp.ref_sku
                                    , how='left') \
                                .drop("ref_sku") 

# COMMAND ----------

# DBTITLE 1,Criar Tabela Final usando Catalogo de Dados
produto_strapi.count()
#executa Funcao na "utils/function"
update_table(tabela,sistema_destino,produto_strapi)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###ETL Cria URL Marketplace B2C

# COMMAND ----------

#--------------------------------------------------------------------------------------------
#   REGRA: considerar o link do marketplace do sistema VTEX coluna DetailUrl
#--------------------------------------------------------------------------------------------

delta_prd = DeltaTable.forName(spark, 'trust.produtos.produto_strapi') 

df_marketplace_vtex = spark.table("ingest.marketplace_vtex.skus")\
                     .where(col("DetailUrl").isNotNull())\
                     .select(regexp_replace(trim(col("ProductRefId")),"_PAI","").cast("string").alias("sku")
                             ,col("DetailUrl").alias("url_marketing_place_b_2_c")).distinct()

#Aplica o Upsert na Tabela Final
delta_prd.alias('prd_strapi')\
.merge(df_marketplace_vtex.alias('url_b2c'), "lower(trim(prd_strapi.sku)) = lower(trim(url_b2c.sku))")\
 .whenMatchedUpdate(
        set ={"prd_strapi.url_marketing_place_b_2_c" : "url_b2c.url_marketing_place_b_2_c"}
        ).execute() 

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestão Postgre

# COMMAND ----------

# DBTITLE 1,Dados de Conexao
jdbcUsername = dbutils.secrets.get(scope = "POSTGRE-STRAPI", key = "POSTGRE_USER")
jdbcPassword =  dbutils.secrets.get(scope = "POSTGRE-STRAPI", key = "POSTGRE_PASSWORD")
jdbcHostname = dbutils.secrets.get(scope = "POSTGRE-STRAPI", key = "POSTGRE_HOST")
jdbcPort = dbutils.secrets.get(scope = "POSTGRE-STRAPI", key = "POSTGRE_PORT")
jdbcDatabase = dbutils.secrets.get(scope = "POSTGRE-STRAPI", key = "POSTGRE_DATABASE")
jdbcUrl = f'jdbc:postgresql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?user={jdbcUsername}&password={jdbcPassword}'

connectionProperties = {
  "user": jdbcUsername,
  "password": jdbcPassword
}

#tempdir = multiple_run_parameters['bucket_s3'] or 'dexco-dev-data-transfer'
tempdir = 'dexco-dev-data-transfer'
pathname = '/tmp/new_dbsitedeca/'   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestao Produto

# COMMAND ----------

# DBTITLE 1,Update Delta Postgre - Produto

spark.table("trust.produtos.produto_strapi") \
 .select( col("sku"),
          col("ambiente"),
          col("categoria"),
          col("codigo_barras"),
          col("codigo_ncm"),
          col("altura"),
          col("altura_produto"),
          col("bitola_entrada"),
          col("bitola_saida"),
          col("alta_performance"),
          col("aplicacao_produto"),
          col("assinatura_arquiteto"),
          col("beneficios_produto"),
          col("borda_fina"),
          col("certificado_garantia"),
          col("classe_pressao"),
          col("cod_multembal_tp_embal"),
          col("codigo_agrupador_cor"),
          col("coeficiente_perda_carga"),
          col("composicao_anel_vedacao"),
          col("composicao_assento"),
          col("composicao_basica"),
          col("composicao_componente"),
          col("comprimento_sem_embalagem"),
          col("consumo_consciente"),
          col("consumo_eletrico_kwh"),
          col("consumo_eletrico_wh"),
          col("cor_componente"),
          col("cor_principal"),
          col("cor_secundaria"),
          col("data_lancamento"),
          col("diametro_nominal"),
          col("diametro_sifao"),
          col("dispositivo_economizador"),
          col("estilo_design_evidencia"),
          col("extensao"),
          col("formato"),
          col("formato_acionamento"),
          col("formato_produto"),
          col("formato_volante"),
          col("indicacao_uso"),
          col("inovacao_tecnologica"),
          col("itens_instalacao"),
          col("itens_relacionados"),
          col("largura"),
          col("largura_sem_embalagem"),
          col("linha"),
          col("linha_assinada"),
          col("manual_instalacao"),
          initcap(col("marca")).alias("marca_portfolio"),
          initcap(col("marca_produto")).alias("marca_produto"),
          col("mesa_metais"),
          col("nome_arquivo"),
          col("nome_produto"),
          col("numero_norma_decreto"),
          col("pais_origem_produto"),
          col("palavra_chave"),
          col("peso_liquido"),
          col("potencia"),
          col("prazo_garantia"),
          col("pressao_maxima"),
          col("pressao_minima"),
          col("pressao_minima_acumulo"),
          col("pressao_minima_passagem"),
          col("produto_dcoat"),
          col("produto_relacionado"),
          col("produto_similar"),
          col("quantidade_bojo"),
          col("saida_esgoto"),
          col("sustentavel"),
          col("tamanho"),
          col("tecnico_acionamento"),
          col("tem_componente_metalico"),
          col("texto_atributo"),
          col("texto_comunicacao_marketing"),
          col("tipo_acionamento"),
          col("tipo_argola"),
          col("tipo_arquivo_transmitido"),
          col("tipo_cuba"),
          col("tipo_dispositivo_economizador"),
          col("tipo_embalagem"),
          col("tipo_fixacao"),
          col("tipo_haste"),
          col("tipo_instalacao"),
          col("tipo_jato"),
          col("tipo_jato_auxiliar"),
          col("tipo_mecanismo"),
          col("tipo_produto"),
          col("tipo_rosca_entrada"),
          col("tipo_rosca_saida"),
          col("tipo_sifao"),
          col("tipo_vedacao"),
          col("universo_infantil"),
          col("url_marketing_place_b_2_c"),
          col("url_catalogo_institucional"),
          col("uso_pcd"),
          col("vazao_pressao_maxima"),
          col("vazao_pressao_minima"),
          col("vedacao_entre_haste_castelo"),
          col("voltagem_eletrica"),
          col("volume_descarga"),
          col("volume_transbordamento"),
          col("url_marketing_place_b_2_c").alias("url_marketplace_b2c"),
          col("arquivo_bim"),
          col("arquivo_blocos"),
          col("arquivo_cad_2_d"),
          col("arquivo_curva_vazao"),
          col("arquivo_desenho_tecnico"),
          col("arquivo_ficha_tecnica"),
          col("arquivo_peca_reposicao"),
          col("arquivo_revit"),
          col("arquivo_sketchup"),
          col("arquivo_stl")) \
    .withColumn("created_at", lit(current_timestamp())) \
    .withColumn("updated_at", lit(current_timestamp())) \
    .withColumn("created_by_id", lit('')) \
    .withColumn("updated_by_id", lit('')) \
    .withColumn("sitemap_exclude", lit('')) \
    .withColumn("id", hash("sku")) \
    .distinct() \
    .write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "delta.produto_delta") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# DBTITLE 1,Scripts Postgre SQL - Produtos
sql_insert = """
insert into public.produtos
(id,alta_performance,altura_produto,ambiente,aplicacao_produto,beneficios_produto,arquivo_bim,bitola_entrada,bitola_saida,arquivo_blocos,borda_fina,marca_portfolio,arquivo_cad_2_d,categoria,certificado_garantia,classe_pressao,codigo_barras,cod_multembal_tp_embal,codigo_ncm,codigo_agrupador_cor,coeficiente_perda_carga,tem_componente_metalico,composicao_anel_vedacao,composicao_assento,composicao_basica,composicao_componente,comprimento_sem_embalagem,consumo_eletrico_wh,consumo_consciente,consumo_eletrico_kwh,cor_componente,cor_principal,cor_secundaria,arquivo_curva_vazao,data_lancamento,arquivo_desenho_tecnico,diametro_nominal,diametro_sifao,dispositivo_economizador,estilo_design_evidencia,extensao,arquivo_ficha_tecnica,nome_arquivo,formato,formato_produto,formato_volante,prazo_garantia,altura,indicacao_uso,inovacao_tecnologica,tipo_instalacao,itens_instalacao,itens_relacionados,tipo_jato_auxiliar,largura_sem_embalagem,linha,linha_assinada,manual_instalacao,marca_produto,mesa_metais,tipo_arquivo_transmitido,numero_norma_decreto,pais_origem_produto,arquivo_peca_reposicao,peso_liquido,potencia,pressao_maxima,pressao_minima,pressao_minima_acumulo,pressao_minima_passagem,produto_dcoat,quantidade_bojo,arquivo_revit,saida_esgoto,tamanho,arquivo_sketchup,arquivo_stl,sustentavel,tipo_acionamento,voltagem_eletrica,texto_atributo,nome_produto,texto_comunicacao_marketing,tipo_argola,tipo_fixacao,tipo_sifao,tipo_dispositivo_economizador,tipo_produto,tipo_embalagem,tipo_haste,tipo_jato,tipo_mecanismo,tipo_rosca_entrada,tipo_rosca_saida,tipo_vedacao,universo_infantil,uso_pcd,vazao_pressao_maxima,vazao_pressao_minima,vedacao_entre_haste_castelo,volume_descarga,volume_transbordamento,largura,sku,tecnico_acionamento,assinatura_arquiteto,tipo_cuba,formato_acionamento,palavra_chave,produto_relacionado,produto_similar,url_catalogo_institucional,url_marketing_place_b_2_c,created_at)
select delta.id
	,delta.alta_performance
	,delta.altura_produto
	,delta.ambiente
	,delta.aplicacao_produto
	,delta.beneficios_produto
	,delta.arquivo_bim
	,delta.bitola_entrada
	,delta.bitola_saida
	,delta.arquivo_blocos
	,delta.borda_fina
	,delta.marca_portfolio
	,delta.arquivo_cad_2_d
	,delta.categoria
	,delta.certificado_garantia
	,delta.classe_pressao
	,delta.codigo_barras
	,delta.cod_multembal_tp_embal
	,delta.codigo_ncm
	,delta.codigo_agrupador_cor
	,delta.coeficiente_perda_carga
	,delta.tem_componente_metalico
	,delta.composicao_anel_vedacao
	,delta.composicao_assento
	,delta.composicao_basica
	,delta.composicao_componente
	,delta.comprimento_sem_embalagem
	,delta.consumo_eletrico_wh
	,delta.consumo_consciente
	,delta.consumo_eletrico_kwh
	,delta.cor_componente
	,delta.cor_principal
	,delta.cor_secundaria
	,delta.arquivo_curva_vazao
	,delta.data_lancamento
	,delta.arquivo_desenho_tecnico
	,delta.diametro_nominal
	,delta.diametro_sifao
	,delta.dispositivo_economizador
	,delta.estilo_design_evidencia
	,delta.extensao
	,delta.arquivo_ficha_tecnica
	,delta.nome_arquivo
	,delta.formato
	,delta.formato_produto
	,delta.formato_volante
	,delta.prazo_garantia
	,delta.altura
	,delta.indicacao_uso
	,delta.inovacao_tecnologica
	,delta.tipo_instalacao
	,delta.itens_instalacao
	,delta.itens_relacionados
	,delta.tipo_jato_auxiliar
	,delta.largura_sem_embalagem
	,delta.linha
	,delta.linha_assinada
	,delta.manual_instalacao
	,delta.marca_produto
	,delta.mesa_metais
	,delta.tipo_arquivo_transmitido
	,delta.numero_norma_decreto
	,delta.pais_origem_produto
	,delta.arquivo_peca_reposicao
	,delta.peso_liquido
	,delta.potencia
	,delta.pressao_maxima
	,delta.pressao_minima
	,delta.pressao_minima_acumulo
	,delta.pressao_minima_passagem
	,delta.produto_dcoat
	,delta.quantidade_bojo
	,delta.arquivo_revit
	,delta.saida_esgoto
	,delta.tamanho
	,delta.arquivo_sketchup
	,delta.arquivo_stl
	,delta.sustentavel
	,delta.tipo_acionamento
	,delta.voltagem_eletrica
	,delta.texto_atributo
	,delta.nome_produto
	,delta.texto_comunicacao_marketing
	,delta.tipo_argola
	,delta.tipo_fixacao
	,delta.tipo_sifao
	,delta.tipo_dispositivo_economizador
	,delta.tipo_produto
	,delta.tipo_embalagem
	,delta.tipo_haste
	,delta.tipo_jato
	,delta.tipo_mecanismo
	,delta.tipo_rosca_entrada
	,delta.tipo_rosca_saida
	,delta.tipo_vedacao
	,delta.universo_infantil
	,delta.uso_pcd
	,delta.vazao_pressao_maxima
	,delta.vazao_pressao_minima
	,delta.vedacao_entre_haste_castelo
	,delta.volume_descarga
	,delta.volume_transbordamento
	,delta.largura
	,delta.sku
	,delta.tecnico_acionamento
	,delta.assinatura_arquiteto
	,delta.tipo_cuba
	,delta.formato_acionamento
	,delta.palavra_chave
	,delta.produto_relacionado
	,delta.produto_similar
	,delta.url_catalogo_institucional
	,delta.url_marketing_place_b_2_c
	,delta.created_at
from delta.produto_delta delta
left join public.produtos produto on delta.sku = produto.sku  
where produto.sku is null ; 
"""

sql_update = """
update public.produtos produto
 set alta_performance = delta.alta_performance
,altura = delta.altura
,altura_produto = delta.altura_produto
,ambiente = delta.ambiente
,aplicacao_produto = delta.aplicacao_produto
,arquivo_bim = delta.arquivo_bim
,arquivo_blocos = delta.arquivo_blocos
,arquivo_cad_2_d = delta.arquivo_cad_2_d
,arquivo_curva_vazao = delta.arquivo_curva_vazao
,arquivo_desenho_tecnico = delta.arquivo_desenho_tecnico
,arquivo_ficha_tecnica = delta.arquivo_ficha_tecnica
,arquivo_peca_reposicao = delta.arquivo_peca_reposicao
,arquivo_revit = delta.arquivo_revit
,arquivo_sketchup = delta.arquivo_sketchup
,arquivo_stl = delta.arquivo_stl
,assinatura_arquiteto = delta.assinatura_arquiteto
,beneficios_produto = delta.beneficios_produto
,bitola_entrada = delta.bitola_entrada
,bitola_saida = delta.bitola_saida
,borda_fina = delta.borda_fina
,categoria = delta.categoria
,certificado_garantia = delta.certificado_garantia
,classe_pressao = delta.classe_pressao
,cod_multembal_tp_embal = delta.cod_multembal_tp_embal
,codigo_agrupador_cor = delta.codigo_agrupador_cor
,codigo_barras = delta.codigo_barras
,codigo_ncm = delta.codigo_ncm
,coeficiente_perda_carga = delta.coeficiente_perda_carga
,composicao_anel_vedacao = delta.composicao_anel_vedacao
,composicao_assento = delta.composicao_assento
,composicao_basica = delta.composicao_basica
,composicao_componente = delta.composicao_componente
,comprimento_sem_embalagem = delta.comprimento_sem_embalagem
,consumo_consciente = delta.consumo_consciente
,consumo_eletrico_kwh = delta.consumo_eletrico_kwh
,consumo_eletrico_wh = delta.consumo_eletrico_wh
,cor_componente = delta.cor_componente
,cor_principal = delta.cor_principal
,cor_secundaria = delta.cor_secundaria
,data_lancamento = delta.data_lancamento
,diametro_nominal = delta.diametro_nominal
,diametro_sifao = delta.diametro_sifao
,dispositivo_economizador = delta.dispositivo_economizador
,estilo_design_evidencia = delta.estilo_design_evidencia
,extensao = delta.extensao
,formato = delta.formato
,formato_acionamento = delta.formato_acionamento
,formato_produto = delta.formato_produto
,formato_volante = delta.formato_volante
,indicacao_uso = delta.indicacao_uso
,inovacao_tecnologica = delta.inovacao_tecnologica
,itens_instalacao = delta.itens_instalacao
,itens_relacionados = delta.itens_relacionados
,largura = delta.largura
,largura_sem_embalagem = delta.largura_sem_embalagem
,linha = delta.linha
,linha_assinada = delta.linha_assinada
,manual_instalacao = delta.manual_instalacao
,marca_produto = delta.marca_produto
,mesa_metais = delta.mesa_metais
,nome_arquivo = delta.nome_arquivo
,nome_produto = delta.nome_produto
,numero_norma_decreto = delta.numero_norma_decreto
,pais_origem_produto = delta.pais_origem_produto
,palavra_chave = delta.palavra_chave
,peso_liquido = delta.peso_liquido
,potencia = delta.potencia
,prazo_garantia = delta.prazo_garantia
,pressao_maxima = delta.pressao_maxima
,pressao_minima = delta.pressao_minima
,pressao_minima_acumulo = delta.pressao_minima_acumulo
,pressao_minima_passagem = delta.pressao_minima_passagem
,produto_dcoat = delta.produto_dcoat
,produto_relacionado = delta.produto_relacionado
,produto_similar = delta.produto_similar
,quantidade_bojo = delta.quantidade_bojo
,saida_esgoto = delta.saida_esgoto
,sustentavel = delta.sustentavel
,tamanho = delta.tamanho
,tecnico_acionamento = delta.tecnico_acionamento
,tem_componente_metalico = delta.tem_componente_metalico
,texto_atributo = delta.texto_atributo
,texto_comunicacao_marketing = delta.texto_comunicacao_marketing
,tipo_acionamento = delta.tipo_acionamento
,tipo_argola = delta.tipo_argola
,tipo_arquivo_transmitido = delta.tipo_arquivo_transmitido
,tipo_cuba = delta.tipo_cuba
,tipo_dispositivo_economizador = delta.tipo_dispositivo_economizador
,tipo_embalagem = delta.tipo_embalagem
,tipo_fixacao = delta.tipo_fixacao
,tipo_haste = delta.tipo_haste
,tipo_instalacao = delta.tipo_instalacao
,tipo_jato = delta.tipo_jato
,tipo_jato_auxiliar = delta.tipo_jato_auxiliar
,tipo_mecanismo = delta.tipo_mecanismo
,tipo_produto = delta.tipo_produto
,tipo_rosca_entrada = delta.tipo_rosca_entrada
,tipo_rosca_saida = delta.tipo_rosca_saida
,tipo_sifao = delta.tipo_sifao
,tipo_vedacao = delta.tipo_vedacao
,universo_infantil = delta.universo_infantil
,url_catalogo_institucional = delta.url_catalogo_institucional
,url_marketing_place_b_2_c = delta.url_marketing_place_b_2_c
,uso_pcd = delta.uso_pcd
,vazao_pressao_maxima = delta.vazao_pressao_maxima
,vazao_pressao_minima = delta.vazao_pressao_minima
,vedacao_entre_haste_castelo = delta.vedacao_entre_haste_castelo
,voltagem_eletrica = delta.voltagem_eletrica
,volume_descarga = delta.volume_descarga
,volume_transbordamento = delta.volume_transbordamento
,marca_portfolio = delta.marca_portfolio
,id = delta.id
,updated_at = delta.updated_at
from delta.produto_delta delta 
where produto.sku = delta.sku 
"""

sql_delete = """
-- Deleta os dados que não estão mais disponíveis 
delete from public.produtos
where sku in ( select produto.sku 
				from public.produtos produto 
				left join delta.produto_delta delta on delta.sku = produto.sku  
				where delta.sku is null
				);
"""

# COMMAND ----------

# DBTITLE 1,Execução Postgre SQL - Produtos
#Abre Conexao com Postgre
postgre = psycopg2.connect (
    dbname=jdbcDatabase
    , user=jdbcUsername
    , password=jdbcPassword
    , port=jdbcPort
    , host=jdbcHostname
) 

print(u"\u2192", "Insert")
cur = postgre.cursor()
cur.execute(sql_insert)
postgre.commit()
cur.close()

print(u"\u2192", "Update")
cur2 = postgre.cursor()
cur2.execute(sql_update)
postgre.commit()
cur2.close()

print(u"\u2192", "Delete")
cur3 = postgre.cursor()
cur3.execute(sql_delete)
postgre.commit()
cur3.close()

#Fecha Conexão Postgre
postgre.close()

print(u"\u2192", "Fim okay")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestao Hierarquia

# COMMAND ----------

# DBTITLE 1,Update Delta Postgre - Hierarquia
table_rename = 'trust.produtos.hierarquia'
Rename_Columns_Table(table_rename)

spark.table("trust.produtos.hierarquia") \
    .select(col("hierarquia"),
            col("descricao"),
            col("marca"),
            col("Nivel0"),
            col("Nivel1"),
            col("Nivel2"),
            col("Nivel3"),
            col("Nivel4"),
            col("data_ingestao")
            ).withColumn("created_at", lit('')) \
        .withColumn("updated_at", lit('')) \
        .withColumn("created_by_id", lit('')) \
        .withColumn("updated_by_id", lit('')) \
        .withColumn("sitemap_exclude", lit('')) \
        .withColumn("id", row_number().over(Window.orderBy("hierarquia")))\
        .distinct() \
  .write \
  .format("jdbc") \
  .option("url", jdbcUrl) \
  .option("dbtable", "delta.hierarquia") \
  .option("user", jdbcUsername) \
  .option("password", jdbcPassword) \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# DBTITLE 1,Update Delta Postgre - Hierarquia_Link

spark.table("trust.produtos.hierarquia_site_link") \
            .select(col("sku").alias("sku"),
                    col("idx").alias("indice"),
                    col("hierarquia")
            ).distinct() \
.write \
  .format("jdbc") \
  .option("url", jdbcUrl) \
  .option("dbtable", "delta.hierarquia_link") \
  .option("user", jdbcUsername) \
  .option("password", jdbcPassword) \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestao Asset Imagem

# COMMAND ----------

# DBTITLE 1,Update Delta Postgre - Imagens

spark.table("trust.produtos.imagem") \
       .selectExpr("sku",
                   "marca",
                   "sub_marca",
                   "nome",
                   "tipo_imagem",
                   "tipo_arquivo",
                   "id_arquivo",
                   "nome_arquivo",
                   "cast (array(resize) as string) tamanho").distinct() \
        .withColumn("created_at", lit('')) \
        .withColumn("updated_at", lit('')) \
        .withColumn("created_by_id", lit('')) \
        .withColumn("updated_by_id", lit('')) \
        .withColumn("sitemap_exclude", lit('')) \
        .withColumn("id", row_number().over(Window.orderBy("sku")))\
        .distinct() \
  .write \
  .format("jdbc") \
  .option("url", jdbcUrl) \
  .option("dbtable", "delta.imagem_delta") \
  .option("user", jdbcUsername) \
  .option("password", jdbcPassword) \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# DBTITLE 1,Scripts Postgre SQL - Imagens
sql_update_imagem = """
update public.produto_imagens imagens
set id = delta.id
   ,tamanho = delta.tamanho
   ,marca = delta.marca 
   ,sub_marca = delta.sub_marca 
   ,nome = delta.nome 
   ,tipo_arquivo = delta.tipo_arquivo 
   ,nome_arquivo = delta.nome_arquivo 
   ,tipo_imagem = delta.tipo_imagem 
   ,id_arquivo = delta.id_arquivo 
   ,tamanho_g = delta.tamanho_g
   ,tamanho_m = delta.tamanho_m
   ,tamanho_p = delta.tamanho_p
   ,tamanho_sqr_1_k = delta.tamanho_sqr_1_k
   ,tamanho_gg = delta.tamanho_gg
   ,tamanho_ggg = delta.tamanho_ggg
from (
select distinct 
	   r.id
      ,r.tamanho
      ,r.sku 
      ,r.marca 
      ,r.sub_marca 
      ,r.nome 
      ,r.tipo_arquivo 
      ,r.nome_arquivo 
      ,r.tipo_imagem 
      ,r.id_arquivo 
      ,regexp_replace(r.tamanho[1],'[\{\}\[\]]', '', 'g')::text tamanho_g
	  ,regexp_replace(r.tamanho[2],'[\{\}\[\]]', '', 'g')::text::text tamanho_m
	  ,regexp_replace(r.tamanho[3],'[\{\}\[\]]', '', 'g')::text::text tamanho_p
	  ,regexp_replace(r.tamanho[4],'[\{\}\[\]]', '', 'g')::text::text tamanho_sqr_1_k
	  ,regexp_replace(r.tamanho[5],'[\{\}\[\]]', '', 'g')::text::text tamanho_gg
	  ,regexp_replace(r.tamanho[6],'[\{\}\[\]]', '', 'g')::text::text tamanho_ggg
from (
	  select sku 
	  ,marca 
	  ,sub_marca 
	  ,nome 
	  ,tipo_imagem 
	  ,tipo_arquivo 
	  ,id_arquivo
	  ,nome_arquivo 
	  ,string_to_array(tamanho, ',') tamanho 
	  ,id 
 from delta.imagem_delta 
 ) r 
 	) delta 
 where imagens.sku = delta.sku 
 and imagens.id_arquivo = delta.id_arquivo
 and imagens.tipo_imagem = delta.tipo_imagem
 and imagens.nome_arquivo = delta.nome_arquivo
 and imagens.id = delta.id
"""

sql_insert_imagem = """
insert into public.produto_imagens (id,tamanho,sku, marca, sub_marca, nome, tipo_arquivo, nome_arquivo, tipo_imagem, id_arquivo, tamanho_g, tamanho_m, tamanho_p, tamanho_sqr_1_k, tamanho_gg, tamanho_ggg)
select distinct 
	   r.id
      ,r.tamanho
      ,r.sku 
      ,r.marca 
      ,r.sub_marca 
      ,r.nome 
      ,r.tipo_arquivo 
      ,r.nome_arquivo 
      ,r.tipo_imagem 
      ,r.id_arquivo 
      ,regexp_replace(r.tamanho[1],'[\{\}\[\]]', '', 'g')::text tamanho_g
	  ,regexp_replace(r.tamanho[2],'[\{\}\[\]]', '', 'g')::text::text tamanho_m
	  ,regexp_replace(r.tamanho[3],'[\{\}\[\]]', '', 'g')::text::text tamanho_p
	  ,regexp_replace(r.tamanho[4],'[\{\}\[\]]', '', 'g')::text::text tamanho_sqr_1_k
	  ,regexp_replace(r.tamanho[5],'[\{\}\[\]]', '', 'g')::text::text tamanho_gg
	  ,regexp_replace(r.tamanho[6],'[\{\}\[\]]', '', 'g')::text::text tamanho_ggg
from (
	  select sku 
	  ,marca 
	  ,sub_marca 
	  ,nome 
	  ,tipo_imagem 
	  ,tipo_arquivo 
	  ,id_arquivo
	  ,nome_arquivo 
	  ,string_to_array(tamanho, ',') tamanho 
	  ,id 
 from delta.imagem_delta 
 ) r
 left join public.produto_imagens imagens on r.sku = imagens.sku 
 where imagens.sku is null ;
"""

sql_delete_imagem = """
delete from public.produto_imagens
where sku in (select imagens.sku
		        from public.produto_imagens imagens
			left join delta.imagem_delta delta on imagens.sku = delta.sku
				where delta.sku is null);
"""

sql_delete_link_imagem = """
delete from public.produto_imagens_produto_links;
"""

sql_insert_link_imagem = """
insert into public.produto_imagens_produto_links
select distinct 
       i.id produto_imagem_id
	  ,p.id produto_id
from public.produto_imagens i
join public.produtos p on p.sku =i.sku  
 and i.nome_arquivo is not null 
 left join public.produto_imagens_produto_links links on i.id = links.produto_imagem_id 
 													 and p.id = links.produto_id 
where links.produto_imagem_id is null  
 and links.produto_id is null;
"""

# COMMAND ----------

# DBTITLE 1,Execução Postgre SQL - Imagens
#Abre Conexao com Postgre
postgre = psycopg2.connect (
    dbname=jdbcDatabase
    , user=jdbcUsername
    , password=jdbcPassword
    , port=jdbcPort
    , host=jdbcHostname
) 

print(u"\u2192", "delete_imagem")
cur3 = postgre.cursor()
cur3.execute(sql_delete_imagem)
postgre.commit()
cur3.close()

print(u"\u2192", "insert_imagem")
cur = postgre.cursor()
cur.execute(sql_insert_imagem)
postgre.commit()
cur.close()

print(u"\u2192", "update_imagem")
cur2 = postgre.cursor()
cur2.execute(sql_update_imagem)
postgre.commit()
cur2.close()

print(u"\u2192", "delete_link_imagem")
cur4 = postgre.cursor()
cur4.execute(sql_delete_link_imagem)
postgre.commit()
cur4.close()

print(u"\u2192", "insert_link_imagem")
cur5 = postgre.cursor()
cur5.execute(sql_insert_link_imagem)
postgre.commit()
cur5.close()

#Fecha Conexão Postgre
postgre.close()

print(u"\u2192", "Fim okay")
