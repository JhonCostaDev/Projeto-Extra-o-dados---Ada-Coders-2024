# Databricks notebook source
# MAGIC %md
# MAGIC # Projeto de Extração de Dados a partir das API's do IPEA e IBGE
# MAGIC
# MAGIC ## Objetivo
# MAGIC Realização de um ETL - Extract, Transform, Load. que consiste em:
# MAGIC   * Extract: Reunir os dados das API's.
# MAGIC   * Transform: Fazer as conversões e limpeza.
# MAGIC   * Load: Salvar os Dados consolidados em um banco de dados, ou warehouse para que possam ser consultados e analisados.
# MAGIC
# MAGIC ### Alvo:
# MAGIC Catalogar a série histórica de feminicídios no Brasil.
# MAGIC
# MAGIC Fontes: 
# MAGIC * [IPEA - Instituto de Pesquisa Econômica Aplicada](http://www.ipeadata.gov.br)
# MAGIC * [IBGE - Instituto Brasileiro de Geografia e Estatística](https://www.ibge.gov.br/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sobre as API's
# MAGIC ### IPEA
# MAGIC O site do IPEA disponibiliza uma biblioteca Python que ajuda nas requisições da API, e possui um repositório no [GitHub](https://github.com/luanborelli/ipeadatapy/) com sua documentação. É através dela que extraímos os dados sobre feminicídio.
# MAGIC
# MAGIC ### IBGE
# MAGIC O IBGE também disponibiliza uma [API](https://servicodados.ibge.gov.br/api/docs/localidades) para consultas de código de região onde utilizamos para melhor idêntificação dos territórios, já que a API do IPEA identifica apenas por código de território. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXTRACT
# MAGIC

# COMMAND ----------

#Instalação da biblioteca disponibilizada pelo site do IPEA.
!pip install ipeadatapy


# COMMAND ----------

# Fazendo a requisição na API
import requests 

url = 'http://www.ipeadata.gov.br/api/odata4/'

response = requests.get(url)
response

# COMMAND ----------

# Verificando o retorno
response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilizando o  Ipea Data

# COMMAND ----------

# Importando a biblioteca e listando as séries disponíveis
import ipeadatapy as ipea

series = ipea.list_series()
series

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fazendo a busca na série através do tema escolhido 

# COMMAND ----------

# 1359	HOMICF	Número de homicídios do sexo feminino

buscar = 'homicídio'
series[ series['NAME'].str.contains(buscar, case=False, na=False)]

# COMMAND ----------

# construindo o dataframe com o método da biblioteca do ipea passando o códico da série.
# A biblioteca do IPEA importa o pandas junto a ela.

df = ipea.timeseries('HOMICF').reset_index()
df.head()

# COMMAND ----------

# Import da bibliotece json e fazendo as requisições HTTPs
import pandas as pd
import json
url_municipios = 'https://servicodados.ibge.gov.br/api/v1/localidades/distritos/'
url_estados = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/'
resp_municipio = requests.get(url_municipios)
resp_estados = requests.get(url_estados)

# COMMAND ----------

# Atribuindo a resposta a uma variável
data_municipio = resp_municipio.json()
data_estados = resp_estados.json()

# construindo os dataframes
ibge_municicio = pd.DataFrame(data_municipio)
ibge_estados = pd.DataFrame(data_estados)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORM

# COMMAND ----------

# Verificando Dados nulos
df.isnull().sum()

# COMMAND ----------

# Verificando Dados NaN
df.isna().sum()

# COMMAND ----------

# Excluindo as colunas que não serão utilizadas
df.keys() # ['DATE', 'CODE', 'RAW DATE', 'TERCODIGO', 'YEAR', 'NIVNOME','VALUE (Unidade)']
excluir = ['DATE', 'CODE', 'RAW DATE']
df.drop(columns=excluir, inplace=True)
df

# COMMAND ----------

# Uma Amostra Geral
df.sample(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renomeando as colunas

# COMMAND ----------

# Renomeando as colunas

colunas = ['id', 'Ano', 'Territorio', 'Quantidade_vitimas']
df.columns = colunas

# COMMAND ----------

df.sample(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convertendo a coluna quantidade para int

# COMMAND ----------

# Convertendo a coluna quantidade para int

df['Quantidade_vitimas'] = df['Quantidade_vitimas'].astype(int)

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integrar a tabela de cod de municipios do IBGE

# COMMAND ----------

# Excluindo a coluna não utilizada
ibge_municicio.drop(columns='municipio', inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe com os código de idêntificação dos municípios

# COMMAND ----------

ibge_municicio.sample(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe com os códigos de idêntificação dos Estados

# COMMAND ----------

ibge_estados.drop(columns='regiao', inplace=True)
ibge_estados.sample(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe de Feminicídios a nível Nacional

# COMMAND ----------

# Homicidios Nacional - Dataframe com o número de homicídios a nível Nacional.
df_brasil = df[df['Territorio'] == 'Brasil']
df_brasil.sample(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe de Feminicídios a nível Estadual

# COMMAND ----------

# Homicidios por Estado - Dataframe com o número de homicídios a nível Estadual.
df_estados = df[ df['Territorio'] == 'Estados'].reset_index()
df_estados

# COMMAND ----------

# Aqui foi necessário converter o tipo da coluna id para poder realizar o Merge
df_estados['id'] = df_estados['id'].astype(str)
ibge_estados['id'] =ibge_estados['id'].astype(str)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando o Merge do Dataframe do IPEA com IBGE

# COMMAND ----------


df_estados = pd.merge(df_estados, ibge_estados[['id', 'sigla', 'nome']], on= 'id', how='left').drop(columns='index')

# COMMAND ----------

# Ordenar as colunas

df_estados.reset_index()
ordem = ['id', 'Ano', 'Territorio', 'sigla', 'nome', 'Quantidade_vitimas']
df_estados = df_estados[ordem]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe final a nível de Estados

# COMMAND ----------

df_estados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe de Feminicídios a nível de Municípios

# COMMAND ----------

# Homicidios por Município - Dataframe com o número de homicídios a nível Municipal.
df_municipios = df[ df['Territorio'] == 'Municípios' ]
df_municipios

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando PySpark para salvar as tabelas em formato delta.

# COMMAND ----------

df_municipios

# COMMAND ----------

import pyspark.pandas as ps

# Convertendo os dataframes Pandas para df spark
feminicidios_nacional = ps.DataFrame(df_brasil)
feminicidios_estados = ps.DataFrame(df_estados)
feminicidios_municipios = ps.DataFrame(df_municipios)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvando as Tabelas no Catalog do Databriks

# COMMAND ----------


feminicidios_nacional.to_spark().write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('femicide_brasil')

feminicidios_estados.to_spark().write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('femicide_states')

feminicidios_municipios.to_spark().write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('femicide_municipality')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando Consultas nas Tabelas Criadas com SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM femicide_states WHERE nome == 'Ceará';

# COMMAND ----------

# Exibindo em um dataframe Spark
ceara = _sqldf
ceara.display()

# COMMAND ----------

# Convertendo em um dataframe pandas para poder plotar o gráfico
df_ceara = ceara.toPandas()
df_ceara

# COMMAND ----------


import matplotlib.pyplot as plt 

plt.figure(figsize=(10, 6))
plt.bar(df_ceara['Ano'], df_ceara['Quantidade_vitimas'], color='blue')

# Rotulos
plt.xlabel('Ano')
plt.ylabel('Quantidade_Vitimas')
plt.title('Número de homicídios do Sexo Feminino no Estado do Ceará')

plt.show()

# COMMAND ----------

descricao = df_ceara.describe().reset_index()

# COMMAND ----------

descricao

# COMMAND ----------

# MAGIC %md
# MAGIC
