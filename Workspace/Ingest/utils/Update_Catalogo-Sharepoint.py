# Databricks notebook source
# MAGIC %md
# MAGIC #Install Packages

# COMMAND ----------

pip install sharepy

# COMMAND ----------

pip install Office365-REST-Python-Client

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ![Solid](https://www.dex.co/Content/images/dexco.svg#gh-light-mode-only)
# MAGIC ## Processo Read CSV Sharepoint
# MAGIC
# MAGIC <b>Sharepoint</b>:
# MAGIC https://duratexsa.sharepoint.com/sites/GovernanadeDados
# MAGIC
# MAGIC <b>Pasta<b>:
# MAGIC Documentos -> Governança Analytics -> 3.Catalogo_Produtos
# MAGIC
# MAGIC <b>Conceito</b>:
# MAGIC Ler arquivo XLSX do Sharepoint e dependendo da aba do excel atualiza a tabela correspondente do catálogo.
# MAGIC
# MAGIC | ABA EXCEL | ARQUIVO DESTINO|
# MAGIC | --- | --- |
# MAGIC |sheet_excel|filtro_entrada|
# MAGIC |sheet_excel|catalogo_atributo|
# MAGIC |sheet_excel|filtro_saida|
# MAGIC
# MAGIC
# MAGIC `TK_2023-06-26`

# COMMAND ----------

# MAGIC %md
# MAGIC # Import Libraries

# COMMAND ----------

import io
import sharepy
import pyspark.pandas as ps
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File 
from openpyxl import load_workbook

# COMMAND ----------

# MAGIC %md
# MAGIC # Variables

# COMMAND ----------

# DBTITLE 1,Escolher a Tabela que deseja Atualizar 
#sheet_excel ='filtro_entrada'
#sheet_excel ='catalogo_atributo'
sheet_excel ='filtro_saida'

# COMMAND ----------

#Dados de Acesso Sharepoint
username_shrpt = dbutils.secrets.get(scope = "EMAIL-SVC-ANALYTICS", key = "EMAIL")
password_shrpt = dbutils.secrets.get(scope = "EMAIL-SVC-ANALYTICS", key = "PASSWORD")

#Endereco Sharepoint / Arquivo
url_shrpt = 'https://duratexsa.sharepoint.com'
folder_url_shrpt = '/sites/GovernanadeDados/Documentos%20Partilhados/Governan%C3%A7a%20Analytics/3.Catalogo_Produtos/'
File = 'catalogo_produto.xlsx'

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

def extract_xlsx_df(sheet_excel, url_shrpt, username_shrpt,password_shrpt):
    session = sharepy.connect(url_shrpt, username=username_shrpt, password=password_shrpt)
    response = session.get(url_shrpt + folder_url_shrpt + File)
    
    #Valida Requisição 
    if (response.status_code) > 200:
        print (f"Erro de conexão: Status {response.status_code}")
        return 

    fol = io.BytesIO(response.content)

    df_koalas = ps.read_excel(fol, sheet_name=sheet_excel).squeeze("columns")
    
    return df_koalas.to_spark()

# COMMAND ----------

# DBTITLE 1,Executando Função
df_update = extract_xlsx_df(sheet_excel, url_shrpt, username_shrpt,password_shrpt)
df_update.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Update Table

# COMMAND ----------

if sheet_excel == 'filtro_entrada':
    print (f"Atualizando tabela trust.produtos.filtro_entrada")
    #df_update.write.mode("overwrite").saveAsTable("trust.produtos.filtro_entrada")

if sheet_excel == 'catalogo_atributo':
    print (f"Atualizando tabela trust.produtos.catalogo_atributo")
    #df_update.write.mode("overwrite").saveAsTable("trust.produtos.catalogo_atributo") 

else:
    print (f"Atualizando tabela trust.produtos.filtro_saida")
    df_update.write.mode("overwrite").saveAsTable("trust.produtos.filtro_saida") 
