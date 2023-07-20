# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark.sql("SET TIME ZONE 'America/Sao_Paulo'").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Classificação de Materiais
sql = """
SELECT
    TRIM(MARA.matnr)  as sku
    ,trim(mara.ean11) as codigo_barras
    ,TRIM(MARA.spart) as setor_atividade
    ,TRIM(AUSP.ATINN) as caracteristica
    ,TRIM(AUSP.KLART) as tipo_classe
    ,trim(mara.meins) as unidade_medida_basica
    ,trim(mara.prdha) as prdha
    ,lower(TRIM(CABN.ATNAM)) as atributo
    ,split_part(trim(mara.groes), '-', 1) as formato
    ,case when (TRIM(AUSP.ATWRT)) is not null then  TRIM(AUSP.ATWRT)
        when (trim(AUSP.DATE_FROM))  is not null then trim(AUSP.DATE_FROM)
        when (trim(AUSP.ATFLV)) is not null and (trim(AUSP.DATE_FROM)) is null then trim (AUSP.ATFLV)
        else null
    end valor
    ,case when TRIM(CAWNT.ATWTB) is not null then TRIM(CAWNT.ATWTB)
        when (TRIM(AUSP.ATWRT)) is not null then  TRIM(AUSP.ATWRT)
        when (trim(AUSP.DATE_FROM))  is not null then trim(AUSP.DATE_FROM)
        when (trim(AUSP.ATFLV)) is not null and (trim(AUSP.DATE_FROM)) is null then trim (AUSP.ATFLV)
    else null end as caracteristica_interna
    FROM ingest.SAP.MARA MARA
    JOIN ingest.SAP.INOB INOB ON trim(MARA.MATNR) = trim(INOB.OBJEK) AND TRIM(INOB.KLART) = '001'
    JOIN ingest.SAP.AUSP AUSP ON TRIM(INOB.CUOBJ) = TRIM(AUSP.OBJEK)
    JOIN ingest.SAP.CABN CABN ON  trim(AUSP.ATINN) = trim(CABN.ATINN)
    LEFT JOIN ingest.SAP.CAWN  CAWN   ON trim (ausp.ATINN) = trim(CAWN.ATINN )
                            AND trim(AUSP.atwrt) = trim (CAWN.atwrt)
    LEFT JOIN ingest.SAP.CAWNT CAWNT  ON trim(CAWN.ATINN) = trim(CAWNT.ATINN)
                            AND trim(CAWN.ATZHL) = trim(CAWNT.ATZHL)
                            and CAWNT.SPRAS = 'PT'
                            --and upper(TRIM(MARA.spart)) in ('LS','HY','MS','LF','01')
                            --where mara.matnr='4888.012.CS'
"""

# COMMAND ----------

def pivot_sap(tab_atributo):

    tab_atributo = tab_atributo

        #Valida se é uma tabela ou uma query
    try:
        assert spark.table(tab_atributo)
        sap_attributes = spark.table(tab_atributo)
    except:
         sap_attributes = spark.sql(tab_atributo)

    #Pivot Table para criar BASE CONDIÇÃO
    pvt_sap_attributes = sap_attributes.groupBy("sku","setor_atividade","tipo_classe","codigo_barras","formato", "unidade_medida_basica","prdha") \
                             .pivot("atributo") \
                             .agg({'valor':'max'})
    #print(sap_attributes)
    return pvt_sap_attributes

# COMMAND ----------

pivot_sap(sql) \
    .withColumn("data_ingestao", lit(current_timestamp())) \
    .write.mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("trust.produtos.classificacao_material")
