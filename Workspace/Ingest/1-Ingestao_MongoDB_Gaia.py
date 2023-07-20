# Databricks notebook source
# MAGIC %md
# MAGIC ![Solid](https://www.dex.co/wp-content/themes/dexco/assets/images/logos/logo.svg)
# MAGIC
# MAGIC ## Ingestao - MongoDB Gaia 
# MAGIC
# MAGIC `Este notebook faz parte do processo de Ingestão dos dados de Produtos do Gaia`
# MAGIC
# MAGIC **Tabelas Atualizadas**
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

# DBTITLE 1,Funções
# MAGIC %run  ../Ingest/utils/function

# COMMAND ----------

# DBTITLE 1,Ingestão Atributos
collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "attributes": 1, "updateDate": 1, "brand":1, "keys":1, "subBrand":1}
filtro = {"attributes": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_atributos'

Read_Mongo(collect,projections,filtro,table)

# COMMAND ----------

# DBTITLE 1,Ingestao Assets Documentos

collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "assets.documents": 1, "updateDate": 1, "brand":1, "keys":1, "subBrand":1}
filtro = {"assets.documents": {"$ne": None}}
table = 'ingest.gaia.mongo_asset_documentos'

Read_Mongo(collect,projections,filtro,table)

# COMMAND ----------

# DBTITLE 1,Ingestao Assets Imagens
collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "assets.images": 1, "updateDate": 1, "brand":1, "keys":1, "subBrand":1}
filtro = {"assets.images": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_asset_imagens'

Read_Mongo(collect,projections,filtro,table)

# COMMAND ----------

# DBTITLE 1,Ingestao Reference Itens Relacionados
collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "references.relatedItems": 1, "updateDate": 1, "brand":1 }
filtro = {"references.relatedItems": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_itens_relacionados'

Read_Mongo(collect,projections,filtro,table)

# COMMAND ----------

# DBTITLE 1,Ingestao Reference Itens de Instalacao
collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "references.installationItems": 1, "updateDate": 1, "brand":1}
filtro = {"references.installationItems": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_itens_instalacao'

Read_Mongo(collect,projections,filtro,table)

# COMMAND ----------

# DBTITLE 1,Ingestao hierarchies
collect = "hierarchies"
projections = {"_id": 1,"name": 1,"attributes":1, "brand":1, "ancestorsId": 1, "ancestors": 1}
filtro = {"_id": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_hierarquia'

Read_Mongo(collect,projections,filtro,table)

# COMMAND ----------

# DBTITLE 1,Ingestao Hierarchies Sku
## Hierarquia Primaria 
collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "hierarchies.primary": 1, "updateDate": 1, "brand":1, "keys":1, "subBrand":1}
filtro = {"hierarchies.primary": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_hierarquia_sku_primaria'

print(f"Updating table {table}", end=' ...\n')
Read_Mongo(collect,projections,filtro,table)
print (' ')

## -----------------------------------
## Hierarquia Marketing
collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "hierarchies.secondaries.marketing": 1, "updateDate": 1, "brand":1, "keys":1, "subBrand":1}
filtro = {"hierarchies.secondaries.marketing": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_hierarquia_sku_marketing'

print(f"Updating table {table}", end=' ...\n')
Read_Mongo(collect,projections,filtro,table)
print (' ')

## -----------------------------------
## Hierarquia Website
collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "hierarchies.secondaries.website": 1, "updateDate": 1, "brand":1, "keys":1, "subBrand":1}
filtro = {"hierarchies.secondaries.website": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_hierarquia_sku_website'

print(f"Updating table {table}", end=' ...\n')
Read_Mongo(collect,projections,filtro,table)
print (' ')

## -----------------------------------
##  Hierarquia Site
collect = "golden_record"
projections = {"_id": 0,"sku": 1, "name":1, "hierarchies.secondaries.site": 1, "updateDate": 1, "brand":1, "keys":1, "subBrand":1}
filtro = {"hierarchies.secondaries.site": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_hierarquia_sku_site'

print(f"Updating table {table}", end=' ...\n')
Read_Mongo(collect,projections,filtro,table)


# COMMAND ----------

# DBTITLE 1,Ingestao Collection Attributes
collect = "attributes"
projections = {"_id": 0,"id": 1, "type":1, "brands": 1, "domains": 1, "status":1, "values":1}
filtro = {"id": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_collection_attributes'

Read_Mongo(collect,projections,filtro,table)

# COMMAND ----------

# DBTITLE 1,Ingestao Collection Products.Attributes
collect = "product_attributes"
projections = {"_id": 1,"type": 1,"name": 1, "uom": 1,"format": 1,"multiSelect": 1,"maxSize": 1,"min": 1,"max": 1,"mask": 1,"brandMetadata": 1}
filtro = {"_id": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_product_attributes'

Read_Mongo(collect,projections,filtro,table)

# COMMAND ----------

# DBTITLE 1,Ingestao Collection Lov
collect = "lovs"
projections = {"_id": 1,"lovName": 1, "valueHelp":1}
filtro = {"_id": {"$exists": True, "$ne": None}}
table = 'ingest.gaia.mongo_lovs'

Read_Mongo(collect,projections,filtro,table)
