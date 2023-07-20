# Databricks notebook source
from pyspark.sql.functions import col,lit,current_timestamp,lower,trim
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Recuperando os atributos MARA
def filtro_entrada(sistema,table):

    tabela = sistema + '.' + table

    print(f"-> Executando describe na tabela: {tabela}")

    df_delta = spark.sql(f"describe {tabela}") \
        .select(lit(f"{sistema}").alias("sistema_origem"),
                lit(f"{table}").alias("tabela_origem"),
                lower(trim(col("col_name"))).alias("id_atributo_entrada"),
                lit("").alias("id_atributo_catalogo"),
                lit("false").alias("ativo"),
                lit(current_timestamp()).alias("data_criacao") 
        )
    
    print(f"-> Executando Atualização")

    delta_prd = DeltaTable.forName(spark, 'trust.produtos.filtro_entrada')
    delta_prd.alias("prd") \
        .merge(
            df_delta.alias("updates"),
            'prd.sistema_origem = updates.sistema_origem and '
            'prd.tabela_origem = updates.tabela_origem and '
            'prd.id_atributo_entrada = updates.id_atributo_entrada'
          ) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()

    return print ("-> Tabela trust.produtos.filtro_entrada atualizada com sucesso")         

# COMMAND ----------

sistema = "sap"
table = "mara"

filtro_entrada(sistema,table) 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Split Tabelas Catalogos

# COMMAND ----------

def fn_unpivot_split_datatype(catalog_origem,banco_origem,tabela_origem,chave_tabela_origem,catalog_destino,banco_destino):

    tabela_gravada_df = spark.createDataFrame([], StructType([StructField('tabela',StringType(), True)]))

    list_dataType =["char","string","int","numeric","decimal","float","double","void","boolean","binary","timestamp","date","array"]
    
    t = spark.sql(f"show tables in {catalog_destino}.{banco_destino}")   

    for var_tipo in list_dataType:
        data_type =spark.sql(f"describe {catalog_origem}.{banco_origem}.{tabela_origem}").where(col("data_type").like(f"%{var_tipo}%")).drop("data_type")

        lst_atributes=[data[0] for data in data_type.collect()]
        
        if var_tipo in ("char","varchar") : var_tipo='string'       
        if var_tipo in ("numeric","decimal") : var_tipo='decimal'
        if var_tipo in ("float","double") : var_tipo='float'
        if var_tipo in ("int","long") : var_tipo='bigint'
        
        if lst_atributes!=[]:
            tabela_destino="atributo_" + var_tipo
            df_origem =spark.table(f"{catalog_origem}.{banco_origem}.{tabela_origem}")
              
            df_unpivot=df_origem.unpivot(col(chave_tabela_origem).alias("sku") , [lst_atributes], "atributo", "valor")\
            .withColumn("Origem",lit(f"{banco_origem}.{tabela_origem}"))\
            .withColumn("Data_Ingestao",lit(current_timestamp()))\
            .withColumn("nome_produto",lit("").cast("string"))\
            .select("origem","data_ingestao","sku","nome_produto","atributo","valor")

            if len(t.filter(t.tableName == tabela_destino).collect()) > 0:
                spark.sql(f"delete from {catalog_destino}.{banco_destino}.{tabela_destino} where Origem ='{banco_origem}.{tabela_origem}'")
                
            df_unpivot.write.format("delta").mode("append").saveAsTable(f"{catalog_destino}.{banco_destino}.{tabela_destino}")
            columns = ['tabela']
            vals = [(f"{banco_destino}.{tabela_destino}",)]            
            newRow_df = spark.createDataFrame(vals, columns)
            tabela_gravada_df=tabela_gravada_df.unionAll(newRow_df) 
    return tabela_gravada_df

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("catalog_origem", "")
catalog_origem=dbutils.widgets.get("catalog_origem")

dbutils.widgets.text("banco_origem", "")
banco_origem =dbutils.widgets.get("banco_origem") 
    
dbutils.widgets.text("tabela_origem", "")
tabela_origem=dbutils.widgets.get("tabela_origem")    

dbutils.widgets.text("catalog_destino", "")
catalog_destino=dbutils.widgets.get("catalog_destino") 

dbutils.widgets.text("banco_destino", "")
banco_destino=dbutils.widgets.get("banco_destino")   

dbutils.widgets.text("chave_tabela_origem", "")
chave_tabela_origem=dbutils.widgets.get("chave_tabela_origem")    
 

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
tabela_gravada_df =fn_unpivot_split_datatype(catalog_origem,banco_origem,tabela_origem,chave_tabela_origem,catalog_destino,banco_destino)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desctibe sap.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select * from sap.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
