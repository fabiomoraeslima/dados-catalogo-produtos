# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![Solid](https://www.dex.co/Content/images/dexco.svg#gh-light-mode-only)
# MAGIC
# MAGIC ## Ingestao - MongoDB Gaia 
# MAGIC
# MAGIC `Este notebook faz parte do processo de validação da  Ingestão dos dados de Produtos do Gaia`

# COMMAND ----------

# MAGIC %md
# MAGIC #Objetivo 
# MAGIC <p/>
# MAGIC
# MAGIC * Gerar uma Matriz de diverengencia de dado entre a base legada do Site e o Gaia salvando na tabela final somente colunas que deram divergência
# MAGIC <p>
# MAGIC
# MAGIC * Note: As colunas que serão comparadas devem serem especificadas no dataframe Df_Strapi e DF_Gaia

# COMMAND ----------

# MAGIC %md
# MAGIC #Controle Versao

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC |Data| Autor| Acao| Alteracao| 
# MAGIC |----| ---- | ---- | ---- |
# MAGIC |01/06/2023|Luciano Fidelis|Criaçao|Atraves da funcap DataQuality_ComparaDadoColuna pipiline compara dado de 2 colunas de tabeles diferente que tenham o mesmo nome  salvando em uma tabelaas colunas con dado inconsistentes.
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Importa Bibliotecas

# COMMAND ----------

# MAGIC %run ../../Ingest/utils/function

# COMMAND ----------

# DBTITLE 1,Importa Bibliotecas
from pyspark.sql.functions import *
from IPython.display import clear_output
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
spark.sql("SET TIME ZONE 'America/Sao_Paulo'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Monta ambiente Otimizando

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Grava Temp Table Site

# COMMAND ----------

# DBTITLE 1,Cria DF Dados Strapi Somente Atributo Comparacao

spark.sql("drop table if exists analytics.tmp_fidelis_Site_Produto_Matriz_apoio")
spark.sql('''
 
select 

sku
,nome_produto
--,Hierarquia_01
--,Hierarquia_02
--,Hierarquia_03
,pais_origem_produto
,aplicacao_produto
,tipo_jato_auxiliar
,tipo_sifao
,marca_produto
,quantidade_bojo
,consumo_consciente
,tecnico_acionamento
,tipo_fixacao
,indicacao_uso
,tipo_rosca_entrada
,concat_ws(';',collect_list(tipo_rosca_saida ) ) as tipo_rosca_saida
,tipo_instalacao
,formato_produto
,tipo_jato
,cor_principal
,cor_componente
,diametro_nominal
,bitola_entrada
,bitola_saida
,classe_pressao
,tipo_haste
,tipo_dispositivo_economizador
,vedacao_entre_haste_castelo
,composicao_anel_vedacao
,saida_esgoto
,concat_ws(';',collect_list(numero_norma_decreto) )  as numero_norma_decreto
 
,formato_volante
,voltagem_eletrica
,prazo_garantia
from (
         select distinct sku as sku
,name as nome_produto
, ambiences.title as  Hierarquia_01 
,categories.title as Hierarquia_02 
,sub_categories.title   as Hierarquia_03
, product_field_country_origins.title as pais_origem_produto
,field_applications.title as aplicacao_produto
,field_auxiliary_jet_types.title as tipo_jato_auxiliar
,field_basin_siphon_types.title as tipo_sifao 
,field_brands.title as marca_produto 
,field_bulk_quantities.title as quantidade_bojo
,conscious_consumptions.title as consumo_consciente
,field_drive_types.title as tecnico_acionamento
,field_fixation_types.title as tipo_fixacao 
,field_indication_uses.title as indicacao_uso
,field_input_thread_types.title as tipo_rosca_entrada
,output_thread_types.title as tipo_rosca_saida
,field_installation_types.title as tipo_instalacao
,field_formats.title as formato_produto
,field_jet_types.title as tipo_jato
,field_main_colors.title as cor_principal
,products.component_colors as cor_componente
,field_nominal_diameters.title as diametro_nominal 
,products.water_inlet_gauge as bitola_entrada
,products.water_outlet_gauge as bitola_saida 
,field_pressure_classes.title as classe_pressao 
,field_rod_types.title as tipo_haste
,field_saver_device_types.title as   tipo_dispositivo_economizador
,field_seal_between_stem_and_castles.title as vedacao_entre_haste_castelo
,field_sealing_ring_compositions.title as composicao_anel_vedacao
,field_sewer_outlets.title as saida_esgoto 

,field_standard_decree_numbers.title  as numero_norma_decreto

,field_steering_wheel_shapes.title as formato_volante
 
--,field_sub_type_installations.title as instalacao_sub_tipo  -->Descontinuar campo duplicado - e usar o INSTALACAO_TIPO (Verificar GAIA
--,field_type_of_seals.title as tipo_vedacao


,field_voltages.title as voltagem_eletrica
,field_warranty_months.title as prazo_garantia

from analytics.products as  products   
left join  analytics.products_ambience_links  as products_ambience_links
             on  products.id = products_ambience_links.product_id

--ambiente 
left join  analytics.ambiences ambiences on products_ambience_links.ambience_id =ambiences.id

-- origem_prod
left join analytics.products_country_origin_links as products_country_origin_link
            on products_country_origin_link.product_id= products.id
left join  analytics.product_field_country_origins as product_field_country_origins 
          on product_field_country_origins.id =products_country_origin_link.product_field_country_origin_id

--aplicacao_produto
left join analytics.products_application_links as application_links on  products.id= application_links.product_id    
left join analytics.product_field_applications as field_applications  on field_applications.id  =application_links.product_field_application_id   

---jato_auxiliar
left join analytics.products_auxiliary_jet_type_links as l2 on   l2.product_id = products.id
left join  analytics.product_field_auxiliary_jet_types as field_auxiliary_jet_types 
            on field_auxiliary_jet_types.id =l2.product_field_auxiliary_jet_type_id


---tipo_de_sifao
left join  analytics.products_basin_siphon_type_links l3 on l3.product_id =products.id
left join analytics.product_field_basin_siphon_types as field_basin_siphon_types on field_basin_siphon_types.id=l3.product_field_basin_siphon_type_id

---ambiente utilizacao
left join analytics.products_brand_links l4  on l4.product_id =products.id
left join analytics.product_field_brands as  field_brands  on field_brands.id=l4.product_field_brand_id

--quantidade_bojo
left join analytics.products_bulk_quantity_links as l5    on l5.product_id=products.id
left join  analytics.product_field_bulk_quantities   as  field_bulk_quantities  on l5.product_field_bulk_quantity_id = field_bulk_quantities.id

--categoria
left join analytics.products_category_links as l6 on l6.product_id =products.id
left join analytics.categories as categories   on l6.category_id =categories.id

--consumo_consciente
left join analytics.products_conscious_consumption_links l7 on  l7.product_id =products.id
left join analytics.product_field_conscious_consumptions as conscious_consumptions 
                on conscious_consumptions.id =l7.product_field_conscious_consumption_id

--tecnico_acionamento                
left join analytics.products_drive_type_links as l8   on l8.product_id=products.id
left join analytics.product_field_drive_types as field_drive_types on field_drive_types.id =l8.product_field_drive_type_id

--tipo_de_fixacao
left join analytics.products_fixation_type_links l9 on l9.product_id=products.id
left join analytics.product_field_fixation_types as field_fixation_types on field_fixation_types.id= l9.product_field_fixation_type_id


--indicacao_uso
left join analytics.products_indication_use_links l11  on l11.product_id =products.id
left join analytics.product_field_indication_uses as field_indication_uses on field_indication_uses.id=l11.product_field_indication_use_id


--tipo_rosca_entrada

left join analytics.products_input_thread_type_links l12   on  l12.product_id=products.id
left join analytics.product_field_input_thread_types as field_input_thread_types on field_input_thread_types.id=l12.product_field_input_thread_type_id

--tipo_rosca_saida
left join analytics.products_output_thread_type_links as l100  on  l100.product_id =products.id
left  join analytics.product_field_output_thread_types as output_thread_types  
on  l100.product_field_output_thread_type_id=output_thread_types.id

--formato_produto
left join  analytics.products_format_links  as l200 on l200.product_field_format_id=products.id
left join analytics.product_field_formats  as field_formats on l200.product_field_format_id=field_formats.id



--instalacao_tipo
left join analytics.products_installation_type_links l13   on l13.product_id=products.id
left join analytics.product_field_installation_types as field_installation_types on field_installation_types.id=l13.product_field_installation_type_id

--tipo_jato
left join analytics.products_jet_type_links l14  on l14.product_id=products.id
left join analytics.product_field_jet_types as field_jet_types on field_jet_types.id=l14.product_field_jet_type_id

--linha
left join analytics.product_field_lines_products_links l15 on l15.product_id=products.id
left join analytics.product_field_lines as field_lines on field_lines.id=l15.product_field_line_id

---cor_principal
left join analytics.products_main_color_links l16  on l16.product_id=products.id
left join analytics.product_field_main_colors field_main_colors on field_main_colors.id =l16.product_field_main_color_id


---diametro_nom
left join analytics.products_nominal_diameter_links l17 on l17.product_id=products.id
left join analytics.product_field_nominal_diameters as field_nominal_diameters  on field_nominal_diameters.id =l17.product_field_nominal_diameter_id
 
--tipo_embalagem
left join analytics.products_packing_type_links l19  on l19.product_id=products.id
left join analytics.product_field_packing_types as field_packing_types on field_packing_types.id = l19.product_field_packing_type_id 

--classe_de_pressao
left join analytics.products_pressure_class_links l20   on l20.product_id==products.id
left join analytics.product_field_pressure_classes as field_pressure_classes on field_pressure_classes.id =l20.product_field_pressure_class_id

--tipo_de_haste
left join analytics.products_rod_type_links l21 on l21.product_id=products.id
left join analytics.product_field_rod_types as field_rod_types on field_rod_types.id =l21.product_field_rod_type_id

--tipo_disp_econo

left join analytics.products_saver_device_type_links l22 on l22.product_id=products.id
left join analytics.product_field_saver_device_types as field_saver_device_types on field_saver_device_types.id =l22.product_field_saver_device_type_id


--vedacao
left join analytics.products_seal_between_stem_and_castle_links l23 on l23.product_id=products.id
left join analytics.product_field_seal_between_stem_and_castles as field_seal_between_stem_and_castles
               on field_seal_between_stem_and_castles.id =l23.product_field_seal_between_stem_and_castle_id

--composicao_anel_vedacao
left join analytics.products_sealing_ring_composition_links l24 on l24.product_id  =products.id       
left join analytics.product_field_sealing_ring_compositions as field_sealing_ring_compositions 
               on field_sealing_ring_compositions.id =l24.product_field_sealing_ring_composition_id


--saida_de_esgoto
left join analytics.products_sewer_outlet_links l25   on l25.product_id =products.id       
left join analytics.product_field_sewer_outlets as field_sewer_outlets  on field_sewer_outlets.id=l25.product_field_sewer_outlet_id

--num_norm
left join analytics.products_standard_decree_number_links l26 on l26.product_id =products.id       
left join analytics.product_field_standard_decree_numbers as field_standard_decree_numbers on field_standard_decree_numbers.id =l26.product_field_standard_decree_number_id

--formato_volante
left join analytics.products_steering_wheel_shape_links l27   on l27.product_id =products.id    
left join analytics.product_field_steering_wheel_shapes as field_steering_wheel_shapes on    field_steering_wheel_shapes.id =l27.product_field_steering_wheel_shape_id

--tipo_produto (sub-categoria)
left join analytics.products_sub_category_links l28 on l28.product_id=products.id    
left join analytics.sub_categories sub_categories on sub_categories.id=l28.sub_category_id

--instalacao_sub_tipo
left join analytics.products_sub_type_installation_links l29  on l29.product_id=products.id    
left join analytics.product_field_sub_type_installations as field_sub_type_installations on field_sub_type_installations.id=l29.product_field_sub_type_installation_id

--tipo_vedacao
left join  analytics.products_type_of_seal_links l30    on l30.product_id =products.id    
left join analytics.product_field_type_of_seals as field_type_of_seals on field_type_of_seals.id =l30.product_field_type_of_seal_id

--voltagem_eletrica
left join analytics.products_voltage_links l31 on l31.product_id=products.id    
left join analytics.product_field_voltages as field_voltages on  field_voltages.id =l31.product_field_voltage_id

--prazo_garantia
left join analytics.products_warranty_months_links l32   on l32.product_id =products.id    
left join analytics.product_field_warranty_months as field_warranty_months on field_warranty_months.id=l32.product_field_warranty_month_id

--ProductType
left join analytics.products_product_type_links l33 on l33.product_id=products.id    
left join analytics.product_types as product_types on product_types.id =l33.product_type_id 
 ) tmp_dados_strapi  

 group by 
 sku
,nome_produto
,pais_origem_produto
,aplicacao_produto
,tipo_jato_auxiliar
,tipo_sifao
,marca_produto
,quantidade_bojo
,consumo_consciente
,tecnico_acionamento
,tipo_fixacao
,formato_produto
,indicacao_uso
,tipo_rosca_entrada 
,tipo_instalacao
,tipo_jato
,cor_principal
,cor_componente
,diametro_nominal
,bitola_entrada
,bitola_saida
,classe_pressao
,tipo_haste
,tipo_dispositivo_economizador
,vedacao_entre_haste_castelo
,composicao_anel_vedacao
,saida_esgoto
,formato_volante
,voltagem_eletrica
,prazo_garantia
''').write.format("delta").saveAsTable("analytics.tmp_fidelis_Site_Produto_Matriz_apoio")
 


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Grava Temp Table Gaia

# COMMAND ----------

# DBTITLE 1,Cria DF Dados Gaia Somente Atributo Comparacao

spark.sql("drop table if exists analytics.tmp_fidelis_Gaia_Produto_Matriz_apoio")
spark.sql('''
select distinct prod.sku  
,nome_produto
--,hie.nivel1 as Hierarquia_01 
--,hie.nivel2 as Hierarquia_02 
--,hie.nivel3  as Hierarquia_03 
,pais_origem_produto  -------alterado
,aplicacao_produto
,tipo_jato_auxiliar -------alterado
,tipo_sifao   -------alterado
,marca_produto 
,quantidade_bojo -------remover ->Carregar no site
--,categoria
,consumo_consciente ------>Carregar no site
,tecnico_acionamento ------>Carregar no site
,tipo_fixacao    ------>Carregar no site
,indicacao_uso ------>Carregar Site e Sanear GAIA
,tipo_rosca_entrada
,prod.tipo_rosca_saida
,tipo_instalacao   ------>Carregar no site
,formato_produto ------>Carregar Site e Sanear GA
,tipo_jato
,cor_principal
--,prod.cor_secundaria
,prod.cor_componente
,diametro_nominal   -------Carregar Site 
,prod.bitola_entrada
,bitola_saida
--,tipo_embalagem  -- remover -->  Criar atributo site

,classe_pressao -------alterado
,tipo_haste -------alterado


,tipo_dispositivo_economizador -------> Carregar Site 
,vedacao_entre_haste_castelo------alterado
,composicao_anel_vedacao -------> Carregar Site 
,saida_esgoto -------> Carregar Site 
,numero_norma_decreto -------> Carregar Site 
,formato_volante -------> Carregar Site 
 
--,instalacao_sub_tipo -------removido -Descontinuar campo duplicado - e usar o INSTALACAO_TIPO (Verificar GAIA
--,tipo_vedacao
,voltagem_eletrica  --->carregar do site o campo TENSAO
,prazo_garantia  -->carregar do site o campo TENSAO
from trust.produtos.produto_strapi as prod
left join trust.produtos.hierarquia_site_link hie_link on hie_link.sku  =prod.sku 
inner join  trust.produtos.hierarquia as hie  on  hie_link.hierarquia =hie.hierarquia

''').write.format("delta").saveAsTable("analytics.tmp_fidelis_Gaia_Produto_Matriz_apoio")   

 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Otimizando Tabelas

# COMMAND ----------

spark.sql("SET spark.databricks.deltdepara.schemdepara.autoMerge.enabled=true")
print("* Otimizando tabela analytics.tmp_fidelis_Site_Produto_Matriz_apoio")
spark.sql(f"OPTIMIZE analytics.tmp_fidelis_Site_Produto_Matriz_apoio ZORDER BY (sku)")

print("* Otimizando tabela analytics.tmp_fidelis_Gaia_Produto_Matriz_apoio")
spark.sql(f"OPTIMIZE analytics.tmp_fidelis_Gaia_Produto_Matriz_apoio ZORDER BY (sku)")
print("Tabelas otimizadas...")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Cria DataFrames Apoio

# COMMAND ----------

#df_site= spark.table("analytics.tmp_fidelis_Site_Produto_Matriz_apoio").where("sku='1140.BL.MT'").alias("df_site")
#df_gaia=spark.table("analytics.tmp_fidelis_Gaia_Produto_Matriz_apoio").where("sku='1140.BL.MT'").alias("df_gaia")

df_site= spark.table("analytics.tmp_fidelis_Site_Produto_Matriz_apoio").alias("df_site")
df_gaia=spark.table("analytics.tmp_fidelis_Gaia_Produto_Matriz_apoio").alias("df_gaia")

df_sku_link= df_gaia.join(df_site, on = df_gaia.sku ==df_site.sku,how="inner").distinct()\
    .select(trim(df_gaia.sku).alias("sku")).orderBy("sku") 

df_sku_link.count()    


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Funcao  DataQuality_ComparaDadoColuna

# COMMAND ----------



def DataQuality_ComparaDadoColuna(df_01,df_02,df_03_link,col_pk,col_compara,prefixo_nome_saida_col01,prefixo_nome_saida_col02):
#--------------------------------------------------------------------------------------------------------------------------  
# df_01....................: Dataframe que vai ter coluna comparada com df_02
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
	
	

# COMMAND ----------

# MAGIC %md
# MAGIC #Executa DataQuality Site vs Gaia

# COMMAND ----------

# MAGIC %md
# MAGIC ##Parâmetros Iniciais

# COMMAND ----------

# DBTITLE 1,Inicializa Variáveis
 
col_pk='sku' 
prefixo_nome_saida_col01='site'
prefixo_nome_saida_col02='gaia'
tabela_temp_apoio='analytics.tmp_Matriz_Data_Quality_Site_Gaia_v2'
tabela_destino='analytics.Matriz_Data_Quality_Site_Gaia_v2_final'

##Variaveis de controle
contador_tabela=0 #-> se 1 primeira interacao e dropa a tabela se ja existe.

interacao_grava_tabela =3 #-> define a quanidade de interacao para gravar o lote. Usado junto com a variavel contaodr_loop
                              #So gravara o lote quando for encontrata dado inconsistente

contador_loop=0  #->controla aa quantidade de interacao para gravar os dados em uma tabela 
num_coluna = len(df_gaia.columns)  #-:> total de colunas que serao validadas
x=0 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inicia Processo

# COMMAND ----------

# DBTITLE 1,Inicia Comparação 


#-----------------------------------------------------------------------------------------------------------------
#Compara valor do dado  coluna site vs gaia - So será desviado para a tabela final colunas com  valor diferente 
#-----------------------------------------------------------------------------------------------------------------
for col_compara in df_gaia.columns:
    contador_tabela+=1    
    if col_compara != "sku":
        df_retorno_DataQuality =DataQuality_ComparaDadoColuna(df_site
                                                              ,df_gaia
                                                              ,df_sku_link
                                                              ,col_pk
                                                              ,col_compara
                                                              ,prefixo_nome_saida_col01
                                                              ,prefixo_nome_saida_col02)
        x+=1
        print("Validando coluna",col_compara," ", x, "de",num_coluna)
        try:
            if df_retorno_DataQuality.count() >0:
            
                contador_loop+=1
                print("**ATENCAO** -->  Coluna",col_compara, "com dado inconsistente")
                print("-----------------")
                df_sku_link =df_sku_link.join(df_retorno_DataQuality , on= df_retorno_DataQuality.sku==df_sku_link.sku, how='left')\
                    .drop(df_retorno_DataQuality.sku)


#---------------------------------------------------------------
#Grava Tabela em Lote  a cada 3 interacoes do loop principal
#---------------------------------------------------------------    
                if contador_loop==interacao_grava_tabela:
                    #
                    if contador_tabela==1:
                        spark.sql("drop table if exists " f"{tabela_temp_apoio}")
                    else:
                        for c in df_sku_link.columns:
                            df_sku_link_append=df_sku_link.withColumn(c,when(col(c).isNull(),'').otherwise(col(c)))
                     # Grava Tabela em Lote particionado
                    print("**Gravando Lote.." )
                              
                    df_sku_link_append.distinct().where("sku is not null")\
                    .write.format("delta")\
                    .mode("append")\
                    .option("mergeSchema", "true")\
                    .saveAsTable(f"{tabela_temp_apoio}")

                    print(f"* Otimizando tabela {tabela_temp_apoio}")
                    spark.sql(f"OPTIMIZE {tabela_temp_apoio} ZORDER BY (sku)")
                    print("Tabelas otimizadas...")
    
                     #Liberar memória do DataFrame 
                    df_sku_link_append.unpersist()                        
                    df_sku_link.unpersist()
                
                     # Limpar todas as linhas do DataFrame
                    df_sku_link_append = df_sku_link_append.filter("1 = 0")
                
                     #zera contador
                    contador_loop=0
        
            # Limpar cache memoria
            df_retorno_DataQuality.unpersist()
            # Limpar todas as linhas do DataFrame 
            df_retorno_DataQuality = df_retorno_DataQuality.filter("1 = 0")
        
        except Exception as erro:
           print("**** ERRO **** COLUNA ",col_compara," NÃO FOI VERIFICADA. Erro:--> ",erro)           
 
              

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Tratamento Dado

# COMMAND ----------

  df_null =spark.table(f"{tabela_temp_apoio}") 
 for c in df_null.columns:
     print(c)
     df_null=df_null.withColumn(c,when(trim(c).isNull(),'').otherwise(trim(c))) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ingestão Dado

# COMMAND ----------

# DBTITLE 1,Ingestão Destino

df_null.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{tabela_destino}")
spark.sql("drop table if exists {tabela_temp_apoio}")

# COMMAND ----------

# MAGIC %md
# MAGIC # OPTIMIZE e VACUUM

# COMMAND ----------

print("Otimizando tabela...")
spark.sql("SET spark.databricks.deltdepara.schemdepara.autoMerge.enabled=true")
spark.sql(f"OPTIMIZE {tabela_destino} ZORDER BY ({col_pk})")
print("Tabela otimizada...")

print("Setando 7 dias para limpeza de versionamento para o hive deltalake...")
spark.sql(f"VACUUM {tabela_destino} RETAIN 168 Hours")
print("Dias de limpeza setados com sucesso!!")
