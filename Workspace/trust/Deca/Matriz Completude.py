# Databricks notebook source
# MAGIC %md
# MAGIC <h1> DEXCO <h1>
# MAGIC
# MAGIC ### Matriz Completude 
# MAGIC
# MAGIC Este notebook faz parte dos processos de Governança de Dados.
# MAGIC
# MAGIC <b>Tabelas de Leitura</b>:
# MAGIC
# MAGIC | Catalogo | Owner |Table |
# MAGIC | --- | --- | --- |
# MAGIC |ingest| gaia | mongo_hierarquia_sku_primaria |
# MAGIC
# MAGIC <b>Tabela Atualizada</b>: 
# MAGIC
# MAGIC | Catalogo | Owner |Table |
# MAGIC | --- | --- | --- |
# MAGIC |stage| arquitetura_dados | matriz_completude |
# MAGIC
# MAGIC <b>View Atualizada</b>: 
# MAGIC | Catalogo | Owner |Table |
# MAGIC | --- | --- | --- |
# MAGIC |trust| produtos | matriz_completude |
# MAGIC
# MAGIC `TK_2023-07-13`

# COMMAND ----------

# DBTITLE 1,Filtros Importantes
#somente atributos obrigatórios
required = 'true' 
#somente skus que vão para o site
exibe_site = "= 'sim'"
#Temp Table 
table_ = "stage.arquitetura_dados.matriz_completude"

# COMMAND ----------

# DBTITLE 1,Querie Principal - Create Table
spark.sql(f"""
select vw_sku_hie.sku
      ,vw_sku_hie.atributo atributo_hierarquia
      ,case when vw_sku_hie.obrigatorio is true then 'Sim'
        else 'Não'
      end obrigatorio
      ,atributo.atributo atributo_golden_record
      ,atributo.valor
      ,case when atributo.atributo is not null 
          then 'Sim' else 'Não'
      end as preenchido
from (
WITH hierar_atributo AS (
		SELECT _id id_hierarquia
			,name
			,atributos_ob ['field'] field
			,atributos_ob ['required'] required
		FROM (
			SELECT _id
				,name
				,explode(attributes) atributos_ob
			FROM ingest.gaia.mongo_hierarquia
			)
		)
SELECT distinct sku_hierarquia.sku
	,hierar_atributo_0.field Atributo
	,hierar_atributo_0.required Obrigatorio
FROM (
	SELECT sku
		,hierarchies ['primary'] [0] AS Nivel_0
	FROM ingest.gaia.mongo_hierarquia_sku_primaria
	--WHERE sku = 'AC.300.42.MULT.VD'
	) sku_hierarquia
LEFT JOIN hierar_atributo AS hierar_atributo_0 ON sku_hierarquia.Nivel_0 = hierar_atributo_0.id_hierarquia

UNION ALL

SELECT sku_hierarquia.sku
	,hierar_atributo_1.field Atributo
	,hierar_atributo_1.required Obrigatorio
FROM (
	SELECT sku
		,hierarchies ['primary'] [1] AS Nivel_1
	FROM ingest.gaia.mongo_hierarquia_sku_primaria
	--WHERE sku = 'AC.300.42.MULT.VD'
	) sku_hierarquia
LEFT JOIN hierar_atributo AS hierar_atributo_1 ON sku_hierarquia.Nivel_1 = hierar_atributo_1.id_hierarquia

UNION ALL

SELECT sku_hierarquia.sku
	,hierar_atributo_2.field Atributo
	,hierar_atributo_2.required Obrigatorio
FROM (
	SELECT sku
		,hierarchies ['primary'] [2] AS Nivel_2
	FROM ingest.gaia.mongo_hierarquia_sku_primaria
	--WHERE sku = 'AC.300.42.MULT.VD'
	) sku_hierarquia
LEFT JOIN hierar_atributo AS hierar_atributo_2 ON sku_hierarquia.Nivel_2 = hierar_atributo_2.id_hierarquia

UNION ALL

SELECT sku_hierarquia.sku
	,hierar_atributo_3.field Atributo
	,hierar_atributo_3.required Obrigatorio
FROM (
	SELECT sku
		,hierarchies ['primary'] [3] AS Nivel_3
	FROM ingest.gaia.mongo_hierarquia_sku_primaria
	--WHERE sku = 'AC.300.42.MULT.VD'
	) sku_hierarquia
LEFT JOIN hierar_atributo AS hierar_atributo_3 ON sku_hierarquia.Nivel_3 = hierar_atributo_3.id_hierarquia

UNION ALL

SELECT sku_hierarquia.sku
	,hierar_atributo_4.field Atributo
	,hierar_atributo_4.required Obrigatorio
FROM (
	SELECT sku
		,hierarchies ['primary'] [4] AS Nivel_4
	FROM ingest.gaia.mongo_hierarquia_sku_primaria
	--WHERE sku = 'AC.300.42.MULT.VD'
	) sku_hierarquia
LEFT JOIN hierar_atributo AS hierar_atributo_4 ON sku_hierarquia.Nivel_4 = hierar_atributo_4.id_hierarquia
) vw_sku_hie 
 left join trust.produtos.atributo_string as atributo 
      on lower(trim (vw_sku_hie.sku || vw_sku_hie.atributo)) = lower(trim(atributo.sku || atributo.atributo))
  join (select * from trust.produtos.atributo_string where lower(atributo) = 'status_exib_site'
                                                      and lower(valor) {exibe_site}) exib_site on vw_sku_hie.sku = exib_site.sku
 where lower(vw_sku_hie.atributo) not like '%ref%'
 and lower(vw_sku_hie.atributo) <> 'hierarquia_web'
 and vw_sku_hie.obrigatorio is {required}
 """).write.mode("overwrite").saveAsTable(f"{table_}")

# COMMAND ----------

# DBTITLE 1,View para o Time de Produtos Digitais
spark.sql(""" create or replace view trust.produtos.vw_matriz_completude as 
select sku.obrigatorio
      ,sku.preenchido
      ,sku.contagem
      ,total.total
      ,cast (sku.contagem/total.total as decimal(18,2)) * 100 `percentual%`
 from (
select obrigatorio
      ,preenchido
      ,count(1) contagem
 from stage.arquitetura_dados.matriz_completude
 group by all ) sku 
 join (select obrigatorio
      ,count(1) total
 from stage.arquitetura_dados.matriz_completude
 group by all) total on total.obrigatorio = sku.obrigatorio
 """)

# COMMAND ----------

# MAGIC %md
# MAGIC Os Dados de percentual por SKU poderão ser utilizados como filtro dos que são carregados no site da Deca, a principio temos apenas a lógica criada, mas ainda sem utilização! 
# MAGIC
# MAGIC `TK_2023-07-13`

# COMMAND ----------

# DBTITLE 1,SQL Final - Gerar Percentual por SKU
spark.sql("""
select sku.sku
      ,sku.obrigatorio
      ,sku.preenchido
      ,sku.contagem
      ,total.Total
      ,replace(cast (cast ((sku.contagem / total.total) * 100 as decimal(18,2)) as string),'.',',') as percentual
 from ( (
          select sku
          		  ,obrigatorio
          			,preenchido
          			,count(1) contagem
          from stage.arquitetura_dados.matriz_completude  
          group by all
          order by 1 desc ) sku
join ( select sku
		  ,obrigatorio
			,count(1) Total
from stage.arquitetura_dados.matriz_completude 
group by all
order by 1 desc ) total on sku.sku = total.sku
 ) order by sku.sku
          , sku.preenchido 
          """)
