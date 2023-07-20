# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql import SparkSession
spark.sql("SET TIME ZONE 'America/Sao_Paulo'").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Variáveis
#Carrega variáveis (Tabela que será criada e qual aplicacao irá utilizar)
aplicacao = "Business-Deca"
tabela = "trust.produtos.produto_full"
# 1 para unidade de medida junta e 2 separada
define_uom = 1


# COMMAND ----------

# MAGIC %md
# MAGIC # Produtos

# COMMAND ----------

def cria_lista_param(aplicacao):

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
                 join trust.produtos.catalogo_atributo catalogo on lower(saida.id_atributo_catalogo) = lower(catalogo.id_atributo_catalogo)
                 join trust.produtos.filtro_entrada entrada on lower(saida.id_atributo_catalogo) = lower(entrada.id_atributo_catalogo)
                 where lower(saida.sistema_destino) = lower("{aplicacao}")
                   and saida.ativo = 1
                   and entrada.ativo = 1
             )
       """).collect()

    return lst_param

# COMMAND ----------

# DBTITLE 1,Executa Função - Cria lista de colunas 
lst_param = cria_lista_param(aplicacao)

# COMMAND ----------

# DBTITLE 1,Função Pivot
def pivot_sap(tab_atributo, lst_param):

    #lista de atributos no filtro de entrada com base na aplicação
    lst_atrib_ent = [col['id_atributo_entrada'] for col in lst_param]

    #Carrega Dados de Atributos
    sap_attributes = spark.table(tab_atributo) \
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
            
                print(u"\u2192", f"{i} - Executando: {tabela}")
                df_x = pivot_sap(tabela, lst_param)
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

# DBTITLE 1,Gera dados de Produtos
df_produto = Ajusta_Medidas(Gerar_Tabelas_Pivot(lst_param),define_uom)

# COMMAND ----------

# DBTITLE 1,Criar tabela usando Catalogo de Dados : Função executada pela "update_table"
def create_table(tabela, lst_param):

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
    #return lst_param

# COMMAND ----------

# DBTITLE 1,Atualiza datatypes do Daframe e insere os dados na tabela
def update_table(tabela,aplicacao,df,lst_param):

    df_prod = df

    #executa function create_table
    create_table(tabela, lst_param)

    print(f"-> Renomeando as colunas")

    #renomeia as colunas do dataframe antes de fazer o insert
    for c in lst_param:
        if c["id_atributo_entrada"] != None:
            column = c["id_atributo_entrada"]
            new_column = c["atributo_saida"]
            df_prod = df_prod.withColumnRenamed(column, new_column)
    
    columns_to_cast = df_prod.columns

    print(f"-> Aplicando cast nas colunas")
    #Aplica os cast em todas as colunas
    for column in columns_to_cast:
        for i in lst_param:
            if column == i["atributo_saida"]:
                datatype = i["tipos"]
                df_prod = df_prod.withColumn(column, col(column).cast(f"{datatype}"))
    
    print(f"-> Inserindo Dados na Tabela {tabela}")
    df_prod.distinct() \
    .write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{tabela}")

    print(f"-> Fim ")
    #return lst_param

# COMMAND ----------

# DBTITLE 1,Criar Tabela Final usando Catalogo de Dados
df_produto = df_produto.dropDuplicates()

#executa Funcao na "utils/function"
update_table(tabela,aplicacao,df_produto,lst_param)

# COMMAND ----------

# DBTITLE 1,Atualiza View Produto Full Deca 
#Variaveis
view_deca = 'deca.produto.produtos'
lst = ('sku','codigo_barras','marca','codigo_ncm','peso_bruto','peso_liquido','texto_breve_material','codigo_material')

# Formatar a lista de colunas e tipos de dados
colunas_formatadas = [col['atributo_saida'] for col in lst_param]

# Gerar a instrução SQL CREATE VIEW
sql_create_view = f""" create or replace view {view_deca} as
 select sku, codigo_barras, codigo_ncm, peso_bruto, peso_liquido, texto_breve_material, 
 {', '.join(f"{col}" for col in colunas_formatadas if col not in (lst))}
 from {tabela}
 where lower(marca) = 'deca'
"""

#print (sql_create_view)
display(spark.sql(sql_create_view))

# COMMAND ----------

# DBTITLE 1,Comercial Produtos - Large
display(spark.sql("""
create or replace view deca.produto.produtos_comercial as 
select nvl(produto.sku, material.sku) sku
     ,material.codigo_barras
     ,nvl(produto.marca,material.setor_atividade) marca
     ,nvl(produto.status_venda, produto.status_venda_sap) status_venda
     ,produto.area_negocio
     ,left(material.prdha,3) codigo_tamanho
     --,produto.tamanho
     ,left(material.prdha,8) segmento
     ,material.linha_2
     ,produto.cor_principal
     ,produto.categoria
     ,produto.sub_grupo
     ,produto.tipo_produto
     ,produto.nivel_qualidade
     ,nvl(produto.padrao, material.padrao) as padrao
     ,nvl(produto.ambiente, material.ambiente) as ambiente
     ,produto.produto_dcoat
     ,produto.formato_produto
     ,material.conversor
     ,material.unidade_medida_basica
     ,material.prdha as codigo_hierarquia
     ,produto.texto_breve_material
     ,produto.texto_longo_material
     ,produto.texto_comunicacao_marketing
 from trust.produtos.produto_full produto
 right join ( select material.sku
                   ,material.setor_atividade
                   ,material.codigo_barras
                   ,ifnull(nvl(material.deca_linha_etiqueta, material.hydra_mkt_linha),
                           nvl(material.cad_rc_colecao_pt, material.mad_sub_grupo)) as linha_2
                   ,material.cad_rc_inspiracao as padrao
                   ,nvl(material.cad_rc_classe_uso, material.deca_segmento_etiqueta) as ambiente
                   ,material.cad_rc_conversor as conversor
                   ,material.unidade_medida_basica
                   ,material.prdha
              from trust.produtos.classificacao_material material
              where material.setor_atividade in ('LS','HY','MS','LF','01')
              ) material on produto.sku = material.sku
"""))

# COMMAND ----------

# DBTITLE 1,Atualiza View's Deca 
display(spark.sql("""
create or replace view deca.produto.imagens as 
select * from  trust.produtos.imagem
where lower(marca) = 'deca'
"""))

display(spark.sql("""
create or replace view deca.produto.hierarquias_site_link as 
select * from  trust.produtos.hierarquia_site_link
"""))

display(spark.sql("""
create or replace view deca.produto.hierarquias_link as 
select * from  trust.produtos.hierarquia_link
where lower(marca) = 'deca'
"""))

display(spark.sql("""
create or replace view deca.produto.hierarquias as 
select * from  trust.produtos.hierarquia
where lower(marca) = 'deca'
"""))

display(spark.sql("""
create or replace view deca.produto.documentos as 
select * from  trust.produtos.documento
where lower(marca) = 'deca'
"""))
