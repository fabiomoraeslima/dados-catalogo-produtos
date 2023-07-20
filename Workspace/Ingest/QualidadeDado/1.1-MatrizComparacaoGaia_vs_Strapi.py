# Databricks notebook source
def valida(*colunas):

    colunas_selec = ", ".join(colunas)

    df = spark.sql(f""" select {colunas_selec}
                          from analytics.tmp_fidelis_Site_Produto_Matriz_apoio site
                        join analytics.tmp_fidelis_Gaia_Produto_Matriz_apoio gaia on site.sku = gaia.sku
                        where 1=1
    """)

    return df

# COMMAND ----------

col_site = spark.table("analytics.tmp_fidelis_Site_Produto_Matriz_apoio").columns
col_gaia = spark.table("analytics.tmp_fidelis_Gaia_Produto_Matriz_apoio").columns

lst = []

for gaia in col_gaia:
    if gaia in (col_site):
        lst.append('site.' + gaia + ' as site_' + gaia + 
                   ', gaia.' + gaia + ' as gaia_' + gaia +
                   ' , trim(lower(ifnull(site.' + gaia + ",'')))" + ' = trim(lower(ifnull(gaia.' + gaia + ",''))) as validar_" + gaia)

#print(lst)
separador = ', '
#print(separador.join(lst))

df = valida(separador.join(lst))
display(df)
#df.write.mode("overwrite").saveAsTable("analytics.arq_validar")
