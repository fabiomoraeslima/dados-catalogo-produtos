# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Ingestao Hirarquia

# COMMAND ----------

# DBTITLE 1,Ingestao Hirarquia 
spark.sql("""
    select  distinct
     _id hierarquia
    ,name descricao
    ,brand marca
    ,ancestors[0] as nivel0
    ,coalesce (ancestors[1],'') as nivel1
    ,coalesce (ancestors[2],'') as nivel2
    ,coalesce (ancestors[3],'') as nivel3
    ,coalesce (ancestors[4],'') as nivel4
    ,current_timestamp() as data_ingestao 
from ingest.gaia.mongo_hierarquia
  """).distinct().write.format("delta").mode("overwrite").saveAsTable("trust.produtos.hierarquia")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Ingestao Hirarquia Link (sku)

# COMMAND ----------

# DBTITLE 1,Hierarquia Primaria

spark.sql (''' SELECT distinct sku
	  ,brand
	  ,CASE 
		WHEN nvl(hierarchies.PRIMARY [5], hierarchies.PRIMARY [4]) IS NOT NULL
			THEN nvl(hierarchies.PRIMARY [5], hierarchies.PRIMARY [4])
		WHEN nvl(hierarchies.PRIMARY [3], hierarchies.PRIMARY [2]) IS NOT NULL
			THEN nvl(hierarchies.PRIMARY [3], hierarchies.PRIMARY [2])
		WHEN nvl(hierarchies.PRIMARY [2], hierarchies.PRIMARY [1]) IS NOT NULL
			THEN nvl(hierarchies.PRIMARY [2], hierarchies.PRIMARY [1])
        WHEN nvl(hierarchies.PRIMARY [1], hierarchies.PRIMARY [0]) IS NOT NULL
			THEN nvl(hierarchies.PRIMARY [1], hierarchies.PRIMARY [0])			
		ELSE hierarchies.PRIMARY [0]
		END hierarquia_primaria
FROM ingest.gaia.mongo_hierarquia_sku_primaria

 ''').createOrReplaceTempView("vw_primaria")	


# COMMAND ----------

# DBTITLE 1,Hierarquia Marketing
 spark.sql('''
 select distinct sku
	  ,brand
	  ,CASE 
		WHEN nvl(hierarchies.secondaries.marketing[0][5] ,hierarchies.secondaries.marketing[0][4] ) IS NOT NULL
			THEN nvl(hierarchies.secondaries.marketing[0][5] ,hierarchies.secondaries.marketing[0][4] ) 

    WHEN nvl(hierarchies.secondaries.marketing[0][4] ,hierarchies.secondaries.marketing[0][3] ) IS NOT NULL
			THEN nvl(hierarchies.secondaries.marketing[0][4] ,hierarchies.secondaries.marketing[0][3] ) 

    WHEN nvl(hierarchies.secondaries.marketing[0][3] ,hierarchies.secondaries.marketing[0][2] ) IS NOT NULL
    THEN nvl(hierarchies.secondaries.marketing[0][3] ,hierarchies.secondaries.marketing[0][2] ) 

    WHEN nvl(hierarchies.secondaries.marketing[0][2] ,hierarchies.secondaries.marketing[0][1] ) IS NOT NULL
			THEN nvl(hierarchies.secondaries.marketing[0][2] ,hierarchies.secondaries.marketing[0][1] ) 

      WHEN nvl(hierarchies.secondaries.marketing[0][5] ,hierarchies.secondaries.marketing[0][4] ) IS NOT NULL
			THEN nvl(hierarchies.secondaries.marketing[0][5] ,hierarchies.secondaries.marketing[0][4] )  
      
      ELSE hierarchies.secondaries.marketing[0][0] 
		END hierarquia_marketing 

FROM ingest.gaia.mongo_hierarquia_sku_marketing
''').createOrReplaceTempView("vw_marketing")

# COMMAND ----------

# DBTITLE 1,Hierarquia Website
spark.sql('''
 select distinct sku
	  ,brand
	  ,CASE 
		WHEN nvl(hierarchies.secondaries.website[0][5] ,hierarchies.secondaries.website[0][4] ) IS NOT NULL
			THEN nvl(hierarchies.secondaries.website[0][5] ,hierarchies.secondaries.website[0][4] ) 

    WHEN nvl(hierarchies.secondaries.website[0][4] ,hierarchies.secondaries.website[0][3] ) IS NOT NULL
			THEN nvl(hierarchies.secondaries.website[0][4] ,hierarchies.secondaries.website[0][3] ) 

    WHEN nvl(hierarchies.secondaries.website[0][3] ,hierarchies.secondaries.website[0][2] ) IS NOT NULL
    THEN nvl(hierarchies.secondaries.website[0][3] ,hierarchies.secondaries.website[0][2] ) 

    WHEN nvl(hierarchies.secondaries.website[0][2] ,hierarchies.secondaries.website[0][1] ) IS NOT NULL
			THEN nvl(hierarchies.secondaries.website[0][2] ,hierarchies.secondaries.website[0][1] ) 

      WHEN nvl(hierarchies.secondaries.website[0][5] ,hierarchies.secondaries.website[0][4] ) IS NOT NULL
			THEN nvl(hierarchies.secondaries.website[0][5] ,hierarchies.secondaries.website[0][4] )  
      
      ELSE hierarchies.secondaries.website[0][0] 
		END hierarquia_website
    
FROM ingest.gaia.mongo_hierarquia_sku_website

''').createOrReplaceTempView("vw_website")

# COMMAND ----------

# DBTITLE 1,Cria View Link
# MAGIC %sql
# MAGIC  
# MAGIC  create or replace temporary view vw_hier_link as 
# MAGIC  select distinct sku     from vw_primaria  
# MAGIC  union 
# MAGIC   select distinct sku  from  vw_marketing 
# MAGIC   union 
# MAGIC  select distinct sku   from   vw_website 
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Ingestao Hirarquia Link

 df_insert =spark.sql('''
 select 
  l.sku as sku
 ,h0.brand as marca
 ,h0.hierarquia_primaria 
 ,h1.hierarquia_marketing
 ,h3.hierarquia_website 
  ,'gaia' origem
 ,current_timestamp() as data_ingestao
 from vw_hier_link  l
 left join vw_primaria h0  on l.sku = h0.sku
left join vw_marketing h1 on h0.sku=h1.sku
left join vw_website h3 on h0.sku=h3.sku''').distinct()\
        .write.format("delta").mode("overwrite").saveAsTable("trust.produtos.hierarquia_link")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hirarquia Site Link

# COMMAND ----------


spark.sql("""
SELECT distinct  sku
	,pos idx
	,CASE 
		WHEN nvl(Nivel5, Nivel4) IS NOT NULL
			THEN replace(replace(nvl(Nivel5, Nivel4), '"', ''), ']', '')
		WHEN nvl(Nivel3, Nivel2) IS NOT NULL
			THEN replace(replace(nvl(Nivel3, Nivel2), '"', ''), ']', '')
		ELSE replace(replace(nvl(Nivel1, Nivel0), '"', ''), ']', '')
		END hierarquia
from (
			select distinct sku sku
            ,pos
            ,replace (replace(Nivel[0],'["',''),'"','') Nivel0
            ,replace(Nivel[1],'"') Nivel1
            ,replace(Nivel[2],'"') Nivel2
            ,replace(Nivel[3],'"') Nivel3
            ,replace(Nivel[4],'"') Nivel4
            ,replace(replace(Nivel[5],'"'),']','') Nivel5
			FROM (
				SELECT sku
					,pos
					,explode(array(split(col, ","))) AS Nivel
				FROM (
					SELECT sku
						,posexplode_outer(hierarchies.secondaries.site [0]) (
							pos
							,col
							)
					FROM ingest.gaia.mongo_hierarquia_sku_site
					)
				) where trim(replace (replace(Nivel[0],'["',''),'"',''))  <> 'SITE-Site'
            and Nivel[1] is not null 
union 
select distinct sku material
		,0 pos
		,hierarchies.secondaries.site [0] [0] Nivel0
		,hierarchies.secondaries.site [0] [1] Nivel1
		,hierarchies.secondaries.site [0] [2] Nivel2
		,hierarchies.secondaries.site [0] [3] Nivel3
		,hierarchies.secondaries.site [0] [4] Nivel4
		,hierarchies.secondaries.site [0] [5] Nivel5
	FROM ingest.gaia.mongo_hierarquia_sku_site
	WHERE sku IN (
			SELECT sku
			FROM (
				SELECT sku
					,pos
					,explode(array(split(col, ","))) AS Nivel
				FROM (
					SELECT sku
						,posexplode_outer(hierarchies.secondaries.site [0]) (
							pos
							,col
							)
					FROM ingest.gaia.mongo_hierarquia_sku_site
					)
				)
			WHERE Nivel [1] IS NULL
			) )
""").distinct()\
     .write.format("delta")\
     .mode("overwrite").saveAsTable("trust.produtos.hierarquia_site_link")  
