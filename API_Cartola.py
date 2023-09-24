# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # MVP Sprint III - Engenharia de Dados - CartolaFC
# MAGIC ##Adriana Legal Reis##

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Definição do Problema
# MAGIC
# MAGIC O propósito fundamental deste projeto reside na análise do desempenho dos jogadores ao longo do Campeonato Brasileiro de Futebol 2023 - Série A (Brasileirão), com o intuito de fornecer uma base sólida para as decisões de escalação nas rodadas futuras do CartolaFC.
# MAGIC
# MAGIC Dentro desse contexto, temos o objetivo de responder às seguintes questões:<br>
# MAGIC 1. Qual jogador detém a melhor média de pontos ao longo do campeonato?<br>
# MAGIC 2. Quais são os jogadores com melhor desempenho ao longo do campeonato?<br>
# MAGIC 3. Qual jogador apresenta o maior número de gols marcados?<br>
# MAGIC 4. Qual goleiro é o menos vazado?<br>
# MAGIC 5. Qual é a relação entre o número de cartões amarelos ou vermelhos recebidos por um jogador e sua pontuação média?<br>
# MAGIC 6. Quais são os jogadores que se destacam em assistências, ou seja, aqueles que mais contribuíram para os gols de suas equipes?<br>
# MAGIC 7. Existe alguma correlação entre a posição do jogador (atacante, meio-campista, zagueiro, lateral e goleiro) e sua pontuação?<br>
# MAGIC 8. Quais são os times que mais pontuaram no CartolaFC e quais jogadores contribuíram significativamente para essas pontuações?<br>
# MAGIC 9. Como o desempenho dos jogadores varia em jogos em casa versus jogos fora de casa?<br>
# MAGIC
# MAGIC Este projeto visa utilizar análises estatísticas e dados históricos para oferecer insights valiosos aos gestores de equipes no CartolaFC, auxiliando-os na tomada de decisões estratégicas para o sucesso em suas escalações.

# COMMAND ----------

# Importação das bibliotecas necessárias
import requests
import json
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from IPython.display import Image
from delta import DeltaTable
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from pyspark.sql.types import LongType
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Coleta dos Dados
# MAGIC
# MAGIC Nesse trabalho estamos utilizando o Azure Databricks que é uma plataforma de análise e processamento de big data que permite a análise e visualização de dados em escala. Existem várias formas de carga de dados no Azure Databricks, dependendo da fonte de dados e do tipo de operação que você deseja realizar. Abaixo, descrevo algumas das formas comuns de carregar dados no Azure Databricks:<br>
# MAGIC <br>
# MAGIC **1. Leitura de Dados Locais:**<br>
# MAGIC Podemos fazer o upload de arquivos locais diretamente para o Azure Databricks por meio do ambiente de trabalho. Os arquivos podem ser carregados usando a interface do usuário do Databricks ou programaticamente via código.<br>
# MAGIC <br>
# MAGIC **2. Armazenamento em Nuvem:**<br>
# MAGIC O Azure Databricks se integra perfeitamente com os serviços de armazenamento em nuvem, como o Azure Data Lake Storage, o Azure Blob Storage e o Amazon S3. Podemos acessar dados diretamente dessas fontes usando bibliotecas de conexão específicas ou comandos SQL.<br>
# MAGIC <br>
# MAGIC **3. Leitura de Bancos de Dados:**<br>
# MAGIC O Databricks permite que nos conectemos e leiamos dados diretamente de bancos de dados relacionais e NoSQL, como o Azure SQL Database, o Azure Cosmos DB, o Amazon Redshift e outros. Podemos usar comandos SQL para consultar e extrair dados dessas fontes.<br>
# MAGIC <br>
# MAGIC **4. Streaming de Dados:**<br>
# MAGIC Para processar dados de streaming em tempo real, podemos usar estruturas como o Apache Kafka ou o Azure Event Hubs para capturar os dados de streaming e, em seguida, processá-los e analisá-los no Databricks usando o Apache Spark Streaming.<br>
# MAGIC <br>
# MAGIC **5. ETL (Extração, Transformação e Carga):**<br>
# MAGIC O Databricks é especialmente adequado para processos de ETL. Podemos extrair dados de várias fontes, aplicar transformações complexas e carregar os dados transformados em um local de armazenamento ou data warehouse para análise posterior.<br>
# MAGIC <br>
# MAGIC **6. APIs e Serviços Web:**<br>
# MAGIC Podemos consumir dados de APIs e serviços da web diretamente em nosso ambiente Databricks. Isso é útil para casos em que os dados estão em serviços externos ou são atualizados continuamente por meio de APIs RESTful. <br>
# MAGIC <br>
# MAGIC **7. Integração com Ferramentas de Visualização:**<br>
# MAGIC Podemos conectar ferramentas de visualização, como o Power BI, o Tableau ou o Databricks' built-in Data Visualization, para criar painéis interativos e relatórios com base nos dados carregados no ambiente.<br>
# MAGIC <br>
# MAGIC **8. Formatos de Dados Suportados:**<br>
# MAGIC Podemos conectar ferramentas de visualização, como o Power BI, o Tableau ou o Databricks' built-in Data Visualization, para criar painéis interativos e relatórios com base nos dados carregados no ambiente.<br>
# MAGIC <br>
# MAGIC **9. Agendamento de Cargas de Dados:**<br>
# MAGIC Podemos automatizar a carga de dados agendando tarefas para executar rotinas de importação de dados em intervalos específicos.<br>
# MAGIC <br>
# MAGIC **10. Replicação de Dados:**<br>
# MAGIC O Azure Databricks se integra perfeitamente com os serviços de armazenamento em nuvem, como o Azure Data Lake Storage, o Azure Blob Storage e o Amazon S3. Podemos acessar dados diretamente dessas fontes usando bibliotecas de conexão específicas ou comandos SQL.<br>
# MAGIC <br>
# MAGIC A escolha da abordagem para carregar os dados depende das circunstâncias específicas do projeto e das fontes de dados envolvidas. Para o nosso projeto em particular, adotamos uma estratégia que começou pela aquisição dos dados por meio da API oficial do CartolaFC. Posteriormente, esses dados foram armazenados em formato JSON e CSV em um repositório na nuvem, hospedado no ambiente do Databricks.
# MAGIC
# MAGIC Vale ressaltar que também implementaremos um processo contínuo de carga de dados para as próximas rodadas, detalhado no Capítulo 6, intitulado "Atualização dos Dados". Nessa etapa, utilizaremos agendamento para automatizar o fluxo de importação de dados, garantindo a atualização constante das informações.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Importação das rodadas do Campeonato Brasileiro de 2023**
# MAGIC
# MAGIC Nesta seção, abordaremos o processo de importação das informações referentes às rodadas do Campeonato Brasileiro de 2023. Para realizar essa tarefa, faremos uso de uma API dedicada que nos permite acessar e adquirir os dados das rodadas de forma eficiente e precisa.

# COMMAND ----------

# URL da API do CartolaFC
url = "https://api.cartola.globo.com/rodadas"

# Chamada à API
resposta = requests.request("GET", url)

# Verificação se a chamada foi bem-sucedida
if resposta.status_code == 200:
    objetos = json.loads(resposta.text)
    # Criando um DataFrame Pandas com os dados
    dfrod = pd.DataFrame(objetos)

    # Caminho para salvar o arquivo CSV no Databricks
    csv_path = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/rodadas.csv'
    # Salvar o DataFrame como um arquivo CSV
    dfrod.to_csv(csv_path, index=False)

    # Caminho para salvar o arquivo JSON no Databricks
    json_path_club = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/rodadas.json'
    # Salvar os DataFrames como arquivos JSON
    dfrod.to_json(json_path_club, orient='records', lines=True)
else:
    print("Erro ao acessar a API do CartolaFC")

# COMMAND ----------

# MAGIC %md
# MAGIC **Carregamento os Dados das Rodadas no DataFrame dfrod**
# MAGIC
# MAGIC Nesta etapa, realizamos o carregamento dos dados das rodadas e os armazenamos no DataFrame chamado "dfrod". Os dados são organizados em uma estrutura tabular, tornando-os prontos para serem utilizados nas próximas etapas do processo.

# COMMAND ----------

# Caminho do arquivo CSV
file_location = "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/rodadas.csv"

# Lê o arquivo CSV usando o Pandas
dfrod = pd.read_csv(file_location)

# Exibe as primeiras linhas do DataFrame
display(dfrod)

# COMMAND ----------

# MAGIC %md
# MAGIC __Importação das partidas de cada rodada do Campeonato__
# MAGIC
# MAGIC Nesta fase, avançamos para a etapa de importação dos dados das partidas correspondentes a cada rodada do Campeonato Brasileiro de Futebol 2023. Para realizar essa operação, executamos um loop que abrange todas as rodadas do campeonato, obtendo os detalhes das partidas através de uma API dedicada.

# COMMAND ----------

# Cria uma sessão Spark
spark = SparkSession.builder.appName("Cartola").getOrCreate()

# Define a URL da API do CartolaFC
baseurl = "https://api.cartola.globo.com/partidas/"

# Itera sobre os valores da coluna "rodada_id" e faz as chamadas à API
for rodada_id in dfrod['rodada_id']:
    url = baseurl + str(rodada_id)

    # Faz a chamada à API
    resposta = requests.request("GET", url)

    # Verifica se a chamada foi bem-sucedida
    if resposta.status_code == 200:
        objetos = json.loads(resposta.text)
        # Crie um DataFrame Pandas com os dados
        dfpart = pd.json_normalize(objetos['partidas'])
        dfpart['rodada_id'] = rodada_id

        # Especifica o caminho para salvar o arquivo CSV no Databricks
        basecsv_path = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/partidas_rodada_'
        csv_path = basecsv_path + str(rodada_id) + ".csv"
        # Salva o DataFrame como um arquivo CSV
        dfpart.to_csv(csv_path, index=False)

        # Caminho para salvar o arquivo JSON no Databricks
        json_path_part = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/partidas_rodada_'
                # Salvar os DataFrames como arquivos JSON
        dfpart.to_json(json_path_part + str(rodada_id) + ".json", orient='records', lines=True)
    else:
        print(f"Erro ao acessar a API do CartolaFC Rodada " + str(rodada_id))

# COMMAND ----------

# MAGIC %md
# MAGIC **Importação dos dados dos Clubes**
# MAGIC
# MAGIC Nesta etapa, utilizaremos uma API dedicada para importar os dados referentes aos clubes que participam do Campeonato Brasileiro de Futebol de 2023.

# COMMAND ----------

# URL da API do CartolaFC
url = "https://api.cartola.globo.com/clubes"

# Chamada à API
resposta = requests.request("GET", url)

# Verificação se a chamada foi bem-sucedida
if resposta.status_code == 200:
    # Carrega o JSON para um dicionário
    dados_dict = json.loads(resposta.text)
    # Converte o dicionário para um DataFrame
    dfclub = pd.DataFrame(dados_dict).T

    # Especifica o caminho para salvar o arquivo CSV no Databricks
    csv_path = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/clubes.csv'
    # Salva o DataFrame como um arquivo CSV
    dfclub.to_csv(csv_path, index=False)

    # Caminho para salvar o arquivo JSON no Databricks
    json_path_club = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/clubes.json'
    # Salvar os DataFrames como arquivos JSON
    dfclub.to_json(json_path_club, orient='records', lines=True)
else:
    print("Erro ao acessar a API do CartolaFC")

# COMMAND ----------

# MAGIC %md
# MAGIC **Importação dos dados da Posição dos jogadores**
# MAGIC
# MAGIC A partir da API específica iremos adquirir informações detalhadas sobre as posições nas quais os jogadores podem ser escalados no CartolaFC.

# COMMAND ----------

# URL da API do CartolaFC
url = "https://api.cartola.globo.com/posicoes"

# Chamada à API
resposta = requests.request("GET", url)

# Verificação se a chamada foi bem-sucedida
if resposta.status_code == 200:
    # Carrega o JSON para um dicionário
    dados_dict = json.loads(resposta.text)
    # Converte o dicionário para um DataFrame
    dfpos = pd.DataFrame(dados_dict).T

    # Especifica o caminho para salvar o arquivo CSV no Databricks
    csv_path = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/posicoes.csv'
    # Salva o DataFrame como um arquivo CSV
    dfpos.to_csv(csv_path, index=False)

    # Caminho para salvar o arquivo JSON no Databricks
    json_path_pos= '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/posicoes.json'
    # Salvar os DataFrames como arquivos JSON
    dfpos.to_json(json_path_pos, orient='records', lines=True)
else:
    print("Erro ao acessar a API do CartolaFC")

# COMMAND ----------

# MAGIC %md
# MAGIC __Importação dos Dados de Pontuação dos Jogadores por Rodada de 2023__
# MAGIC
# MAGIC Nessa etapaa, realizaremos a importação dos dados de pontuação dos jogadores por rodada através de uma API específica. Este processo envolve a execução de um loop que percorrerá cada rodada do Campeonato Brasileiro de Futebol 2023, com o objetivode  adquirir informações detalhadas sobre o desempenho dos jogadores em cada rodada. O loop percorrerá as diferentes rodadas do campeonato, permitindo a obtenção dos dados relevantes do CartolaFC.

# COMMAND ----------

# Cria uma sessão Spark
spark = SparkSession.builder.appName("Cartola").getOrCreate()

# Define a URL da API do CartolaFC
baseurl = "https://api.cartola.globo.com/atletas/pontuados/"

# Itera sobre os valores da coluna "rodada_id" e faz as chamadas à API
for rodada_id in dfrod['rodada_id']:
    url = baseurl + str(rodada_id)

    # Faz a chamada à API
    resposta = requests.request("GET", url)

    # Verifica se a chamada foi bem-sucedida
    if resposta.status_code == 200:
        objetos = json.loads(resposta.text)
        # Cria um DataFrame Pandas com os dados
        df = pd.DataFrame(objetos['atletas'])
        df = df.T
        df['rodada_id'] = rodada_id
        # Usando apply + pd.Series para criar colunas separadas
        dfscout = df['scout'].apply(pd.Series)
         # Juntar os DataFrames ao longo do eixo das colunas
        df = pd.concat([df, dfscout], axis=1)

        # Especifica o caminho para salvar o arquivo CSV no Databricks
        basecsv_path = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/pontuacao_rodada_'
        csv_path = basecsv_path + str(rodada_id) + ".csv"
        # Salva o DataFrame como um arquivo CSV
        df.to_csv(csv_path, index=False)

        # Caminho para salvar o arquivo JSON no Databricks
        json_path_part = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/pontuacao_rodada_'
        # Salvar os DataFrames como arquivos JSON
        df.to_json(json_path_part + str(rodada_id) + ".json", orient='records', lines=True)
    else:
        print(f"API do CartolaFC Rodada " + str(rodada_id) + " não disponível")

# COMMAND ----------

# MAGIC %md
# MAGIC As rodadas não importadas, indicadas pela mensagem "API do CartolaFC Rodada xx não disponível", correspondem às rodadas que ainda não aconteceram no Campeonato.

# COMMAND ----------

# MAGIC %md
# MAGIC __Dados salvos na Nuvem__
# MAGIC
# MAGIC Na imagem abaixo, é possível visualizar os arquivos que foram inseridos no databricks por meio da consulta à API do CartolaFC.<br>
# MAGIC
# MAGIC ![Arquivos](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Arquivos%20Salvos%20na%20Nuvem.png?raw=true)
# MAGIC
# MAGIC Exemplo mostrando o conteúdo do CSV salvo:<br>
# MAGIC
# MAGIC ![CSV](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Arquivos%20Salvos%20na%20Nuvem%202.png?raw=true)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Modelagem dos Dados
# MAGIC
# MAGIC Neste projeto, optamos por adotar o esquema de modelagem em estrela para a organização dos dados. A modelagem em estrela é uma abordagem de design de banco de dados relacional que simplifica a estrutura das tabelas e é especialmente indicada para a criação de um data warehouse.
# MAGIC
# MAGIC Ao construir o modelo em estrela, uma tabela central de fatos é centralizada, cercada por tabelas de dimensão que contêm informações descritivas sobre os dados presentes na tabela de fatos. Estas tabelas de dimensão são conectadas diretamente à tabela de fatos através de chaves estrangeiras, tornando mais fácil a realização de consultas complexas e análises.
# MAGIC
# MAGIC Utilizamos a plataforma GenMyModel para criar e visualizar a estrutura do nosso modelo em estrela. A representação visual deste modelo está ilustrada abaixo:
# MAGIC
# MAGIC
# MAGIC ![Modelagem](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Fato_Pontua%C3%A7%C3%A3o_Rodadas.jpeg?raw=true)
# MAGIC
# MAGIC Na imagem acima, é possível identificar claramente as chaves primárias, destacadas em amarelo, bem como as chaves estrangeiras, realçadas em cinza. Além disso, a representação visual também evidencia os relacionamentos essenciais entre as tabelas do nosso modelo de dados. Essa visualização facilita a compreensão das conexões fundamentais que moldam a estrutura do nosso banco de dados no esquema estrela.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Catálago de Dados
# MAGIC
# MAGIC A seguir, apresentamos o catálogo de dados referente a cada tabela do nosso projeto.
# MAGIC
# MAGIC **Catálogo de Dados - Tabela "Rodadas"**
# MAGIC
# MAGIC **Descrição Geral:**<br>
# MAGIC Este conjunto de dados contém as informações detalhadas sobre as rodadas do Campeonato Brasileiro de Futebol - Seríe A de 2023.<br>
# MAGIC
# MAGIC **Metadados Principais e Linhagem dos Dados:**<br>
# MAGIC **Nome do Conjunto de Dados:** rodadas<br>
# MAGIC **Formato:** Banco de Dados SQL<br>
# MAGIC **Data de Criação:** 21/09/2023<br>
# MAGIC **Localização:** abfss://unity-catalog@stshdptbdlkc.dfs.core.windows.net/d142e1fb-3fac-44a8-a2dc-08218f92afc5/tables/96a8aa8e-db25-47b8-bbc5-b3d228a3d929<br>
# MAGIC **Origem dos Dados:** Os dados foram coletados diretamente do API do CartolaFC na url https://api.cartola.globo.com/rodadas.<br>
# MAGIC ![Rodadas](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Rodadas.png?raw=true)<br>
# MAGIC <br>
# MAGIC
# MAGIC **Catálogo de Dados - Tabela "Clubes"**
# MAGIC
# MAGIC **Descrição Geral:**<br>
# MAGIC Este conjunto de dados contém as informações dos clubes que disputam o Campeonato Brasileiro de Futebol - Seríe A de 2023.
# MAGIC
# MAGIC **Metadados Principais e Linhagem dos Dados:**<br>
# MAGIC **Nome do Conjunto de Dados:** clubes<br>
# MAGIC **Formato:** Banco de Dados SQL<br>
# MAGIC **Data de Criação:** 21/09/2023<br>
# MAGIC **Localização:** abfss://unity-catalog@stshdptbdlkc.dfs.core.windows.net/d142e1fb-3fac-44a8-a2dc-08218f92afc5/tables/d325f838-f9cf-42f5-ba38-960962f71764<br>
# MAGIC **Origem dos Dados:** Os dados foram coletados diretamente do API do CartolaFC na url https://api.cartola.globo.com/clubes.<br>
# MAGIC ![Clubes](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Clube.png?raw=true)<br>
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC **Catálogo de Dados - Tabela "Posiçao"**
# MAGIC
# MAGIC **Descrição Geral:**<br>
# MAGIC Este conjunto de dados contém as informações das posições em que os jogadores podem ser escalados no CartolaFC.
# MAGIC
# MAGIC **Metadados Principais e Linhagem dos Dados:**<br>
# MAGIC **Nome do Conjunto de Dados:** posições<br>
# MAGIC **Formato:** Banco de Dados SQL<br>
# MAGIC **Data de Criação:** 21/09/2023<br>
# MAGIC **Localização:** abfss://unity-catalog@stshdptbdlkc.dfs.core.windows.net/d142e1fb-3fac-44a8-a2dc-08218f92afc5/tables/7c1e5dfe-fdf3-4811-86c5-cde753916ea5<br>
# MAGIC **Origem dos Dados:** Os dados foram coletados diretamente do API do CartolaFC na url https://api.cartola.globo.com/posicoes.<br>
# MAGIC
# MAGIC ![Posições](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Posicao.png?raw=true)<br>
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC __Catálogo de Dados - Partidas__
# MAGIC
# MAGIC Descrição Geral:<br>
# MAGIC Este conjunto de dados contém as informações das partidas do Campeonato Brasileiro de Futebol - Seríe A de 2023.
# MAGIC
# MAGIC **Metadados Principais e Linhagem dos Dados:**<br>
# MAGIC **Nome do Conjunto de Dados:** partidas<br>
# MAGIC **Formato:** Banco de Dados SQL<br>
# MAGIC **Data de Criação:** 21/09/2023<br>
# MAGIC **Localização:** abfss://unity-catalog@stshdptbdlkc.dfs.core.windows.net/d142e1fb-3fac-44a8-a2dc-08218f92afc5/tables/ad5cc4f3-a1f1-415a-bfb1-28b98c16c36e<br>
# MAGIC **Origem dos Dados:** Os dados foram coletados diretamente do API do CartolaFC na url https://api.cartola.globo.com/partidas/.<br>
# MAGIC
# MAGIC ![Partidas](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Partidas.png?raw=true)<br>
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC __Catálogo de Dados - Pontuação__
# MAGIC
# MAGIC **Descrição Geral:**<br>
# MAGIC Este conjunto de dados contém as informações das pontuações que os jogadores obtiveram em cada partida do Campeonato Brasileiro de Futebol - Seríe A de 2023.
# MAGIC
# MAGIC **Metadados Principais e Linhagem dos Dados:**<br>
# MAGIC **Nome do Conjunto de Dados:** pontuacao<br>
# MAGIC **Formato:** Banco de Dados SQL<br>
# MAGIC **Data de Criação:** 21/09/2023<br>
# MAGIC **Origem dos Dados:** Os dados foram coletados diretamente do API do CartolaFC na url https://api.cartola.globo.com/atletas/pontuados/.<br>
# MAGIC
# MAGIC ![Pontuação](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Pontuacao.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Criação do Banco de Dados
# MAGIC
# MAGIC A criação do banco de dados é um passo fundamental no desenvolvimento de projetos de tecnologia da informação. No contexto específico, utilizamos o comando "CREATE DATABASE IF NOT EXISTS dt0028_dev.PUC_MVP_CartolaFC;" para estabelecer a estrutura de armazenamento de dados para o nosso projeto.
# MAGIC
# MAGIC Esse comando garante que o banco de dados "PUC_MVP_CartolaFC" seja criado apenas se ele ainda não existir, evitando duplicações desnecessárias. Esse banco de dados servirá como o repositório central para as informações relacionadas ao nosso projeto, permitindo o armazenamento, a organização e a recuperação eficiente de dados relacionados ao Campeonato Brasileiro de Futebol - Série A de 2023.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dt0028_dev.PUC_MVP_CartolaFC;

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos verificar a criação do banco de dados vazio na imagem a seguir.<br>
# MAGIC <br>
# MAGIC
# MAGIC ![BancoDados](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Banco%20de%20Dados.png?raw=true)<br>
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Banco%20de%20Dados%202.png?raw=true" alt="BancoDados2" width="600" height="450"><br>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Criação das Tabelas
# MAGIC
# MAGIC A criação das tabelas é um passo crucial no processo de desenvolvimento do nosso banco de dados. Utilizamos comandos SQL específicos para definir a estrutura de cada tabela, incluindo os campos, os tipos de dados, as chaves primárias e as chaves estrangeiras necessárias para armazenar e organizar os dados de forma adequada.
# MAGIC
# MAGIC Cada tabela foi cuidadosamente projetada para representar informações específicas e relevantes para o nosso projeto. Isso inclui detalhes sobre rodadas do Campeonato Brasileiro de Futebol - Série A de 2023, como pontuações, times, jogadores e estatísticas de desempenho.
# MAGIC
# MAGIC A tabela "rodadas" foi criada para registrar detalhes sobre as rodadas do campeonato, incluindo datas de início e término, além de identificadores exclusivos para cada rodada.
# MAGIC
# MAGIC A tabela "clubes" armazena informações sobre os clubes participantes do campeonato, como seus nomes, abreviações, apelidos e outras informações relevantes.
# MAGIC
# MAGIC A tabela "posicao" contém dados sobre as posições dos jogadores, incluindo abreviações e descrições.
# MAGIC
# MAGIC A tabela "partidas" registra informações detalhadas sobre cada partida, incluindo local, data, placar, posições dos clubes, e se a partida foi válida para o Cartola.
# MAGIC
# MAGIC Por fim, a tabela "pontuacao" armazena estatísticas individuais dos jogadores em cada partida, como pontuações, cartões, gols, assistências e muito mais, permitindo uma análise detalhada do desempenho de cada jogador ao longo do campeonato.
# MAGIC
# MAGIC Essas tabelas fornecem a estrutura essencial para o nosso banco de dados, permitindo o armazenamento organizado e a posterior consulta e análise dos dados do Campeonato Brasileiro de Futebol - Série A de 2023.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE dt0028_dev.PUC_MVP_CartolaFC;
# MAGIC
# MAGIC -- Criação da tabela 'rodadas'
# MAGIC CREATE OR REPLACE TABLE rodadas
# MAGIC (
# MAGIC   rodada_id BIGINT NOT NULL PRIMARY KEY COMMENT 'Número único que identifica cada rodada',
# MAGIC   inicio TIMESTAMP COMMENT 'Data de início da rodada',
# MAGIC   fim TIMESTAMP COMMENT 'Data final da rodada',
# MAGIC   nome_rodada VARCHAR(10) COMMENT 'Denominação da Rodada'
# MAGIC );
# MAGIC
# MAGIC -- Criação da tabela 'clubes'
# MAGIC CREATE OR REPLACE TABLE clubes
# MAGIC (
# MAGIC   clube_id BIGINT NOT NULL PRIMARY KEY COMMENT 'Número único que identifica cada clube',
# MAGIC   clube VARCHAR(255) COMMENT 'Nome do clube',
# MAGIC   abrev_clube VARCHAR(60) COMMENT 'Abreviação do nome do clube ',
# MAGIC   slug_clube VARCHAR(60) COMMENT 'Rótulo simplificado do clube',
# MAGIC   apelido_clube VARCHAR(60) COMMENT 'Apelido do clube',
# MAGIC   nome_fantasia VARCHAR(60) COMMENT 'Nome Fantasia do clube'
# MAGIC );
# MAGIC
# MAGIC -- Criação da tabela 'posicao'
# MAGIC CREATE OR REPLACE TABLE posicao
# MAGIC (
# MAGIC   posicao_id BIGINT NOT NULL PRIMARY KEY COMMENT 'Número único que identifica cada posição',
# MAGIC   abrev_posicao VARCHAR(3) COMMENT 'Abreviatura da posição do jogador',
# MAGIC   posicao VARCHAR(10) COMMENT 'Denominação da posição do jogador'
# MAGIC );
# MAGIC
# MAGIC -- Criação da tabela 'partidas'
# MAGIC CREATE OR REPLACE TABLE partidas
# MAGIC (
# MAGIC   partida_id BIGINT NOT NULL PRIMARY KEY COMMENT 'Número único que identifica cada partida',
# MAGIC   local VARCHAR(150) COMMENT 'Local da Partida',
# MAGIC   partida_data TIMESTAMP COMMENT 'Data da Partida',
# MAGIC   placar_oficial_visitante INTEGER COMMENT 'Placar (quantidade de gols) do time visitante',
# MAGIC   placar_oficial_mandante INTEGER COMMENT 'Placar (quantidade de gols) do time mandante',
# MAGIC   clube_visitante_posicao INTEGER COMMENT 'Posição do time visitante no campeonato',
# MAGIC   clube_casa_posicao INTEGER COMMENT 'Posição do time mandante no campeonato',
# MAGIC   clube_visitante_id BIGINT COMMENT 'Número que identifica o clube visitante',
# MAGIC   clube_casa_id BIGINT COMMENT 'Número que identifica o clube mandante',
# MAGIC   valida BOOLEAN COMMENT 'Indica se a partida foi ou não válida para o Cartola',
# MAGIC   rodada_id BIGINT COMMENT 'Número que identifica a rodada',
# MAGIC   CONSTRAINT partidas_clubes_visitante_fk FOREIGN KEY (clube_visitante_id) REFERENCES clubes(clube_id),
# MAGIC   CONSTRAINT partidas_clubes_casa_fk FOREIGN KEY (clube_casa_id) REFERENCES clubes(clube_id),
# MAGIC   FOREIGN KEY (rodada_id) REFERENCES rodadas(rodada_id)
# MAGIC );
# MAGIC
# MAGIC -- Criação da tabela 'pontuacao'
# MAGIC CREATE OR REPLACE TABLE pontuacao
# MAGIC (
# MAGIC   apelido VARCHAR(60) COMMENT 'Apelido do jogador',
# MAGIC   pontuacao DECIMAL(4, 2) COMMENT 'Pontuação do jogador na partida',
# MAGIC   posicao_id BIGINT COMMENT 'Número que identifica a posição do jogador',
# MAGIC   clube_id BIGINT COMMENT 'Número que identifica o clube do jogador',
# MAGIC   entrou_em_campo BOOLEAN COMMENT 'Indica se o jogador jogou na partida',
# MAGIC   partida_id BIGINT COMMENT 'Número que identifica a partida',
# MAGIC   rodada_id BIGINT COMMENT 'Número que identifica a rodada',
# MAGIC   CA INTEGER COMMENT 'Quantidade de Cartões Amarelos',
# MAGIC   DS INTEGER COMMENT 'Quantidade de Desarmes',
# MAGIC   FC INTEGER COMMENT 'Quantidade de Faltas Cometidas',
# MAGIC   FF INTEGER COMMENT 'Quantidade de Finalizações para Fora',
# MAGIC   FD INTEGER COMMENT 'Quantidade de Finalizações Defendidas',
# MAGIC   FS INTEGER COMMENT 'Quantidade de Faltas Sofridas',
# MAGIC   I INTEGER COMMENT 'Quantidade de Impedimentos',
# MAGIC   SG INTEGER COMMENT 'Indica se o time não tomou Gol',
# MAGIC   A INTEGER COMMENT 'Quantidade de Assistências',
# MAGIC   G INTEGER COMMENT 'Quantidade de Gols',
# MAGIC   DE INTEGER COMMENT 'Quantidade de Defesas',
# MAGIC   GS INTEGER COMMENT 'Quantidade de Gols Sofridos',
# MAGIC   V INTEGER COMMENT 'Indica se o time venceu a partida',
# MAGIC   PS INTEGER COMMENT 'Quantidade de Pênaltis Sofridos',
# MAGIC   FT INTEGER COMMENT 'Quantidade de Finalizações na Trave',
# MAGIC   PP INTEGER COMMENT 'Quantidade de Pênaltis Perdidos',
# MAGIC   DP INTEGER COMMENT 'Quantidade de Defesas de Pênaltis',
# MAGIC   CV INTEGER COMMENT 'Quantidade de Cartões Vermelhos',
# MAGIC   PC INTEGER COMMENT 'Quantidade de Pênaltis Cometidos',
# MAGIC   PRIMARY KEY (apelido, rodada_id, clube_id),
# MAGIC   FOREIGN KEY (posicao_id) REFERENCES posicao(posicao_id),
# MAGIC   FOREIGN KEY (clube_id) REFERENCES clubes(clube_id),
# MAGIC   FOREIGN KEY (rodada_id) REFERENCES rodadas(rodada_id),
# MAGIC   FOREIGN KEY (partida_id) REFERENCES partidas(partida_id)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos verificar na imagem abaixo que as tabelas foram criadas com sucesso no diretório do Databricks.<br>
# MAGIC
# MAGIC <img src="https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Tabelas%20Criadas.png?raw=true" alt="Tabelas" width="800" height="600"><br>
# MAGIC
# MAGIC
# MAGIC Abaixo estão as imagens de cada tabela criada com suas chaves primárias e estrangeiras:<br>
# MAGIC
# MAGIC **Tabela Rodadas**<br>
# MAGIC <img src="https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Tabela%20Rodadas.png?raw=true" alt="Rodada" width="400" height="300"><br>
# MAGIC
# MAGIC  **Tabela Clubes**<br>
# MAGIC  <img src="https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Tabela%20Clubes.png?raw=true" alt="Clubes" width="400" height="300"><br>
# MAGIC
# MAGIC **Tabela Posição**<br>
# MAGIC <img src="https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Tabela%20Posicao.png?raw=true" alt="Posição" width="400" height="300"><br>
# MAGIC
# MAGIC **Tabela Partidas**<br>
# MAGIC <img src="https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Tabela%20Partidas.png?raw=true" alt="Partidas" width="400" height="300"><br>
# MAGIC
# MAGIC **Tabela Pontuação**<br>
# MAGIC <img src="https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Tabela%20Pontuacao.png?raw=true" alt="Pontuação" width="400" height="300"><br>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga dos Dados

# COMMAND ----------

# MAGIC %md
# MAGIC Para executar o processo de carregamento de dados em nossas tabelas de banco de dados, é fundamental adotar um procedimento bem definido. Esse procedimento engloba várias etapas, incluindo a leitura de arquivos nos formatos CSV ou JSON, a conversão desses dados em tabelas Delta e, posteriormente, a inserção seletiva dos campos que identificamos como pertinentes para nossa operação. Essa abordagem garante que apenas os dados relevantes sejam incorporados ao nosso ambiente de armazenamento, otimizando assim a eficiência e a qualidade do nosso sistema.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Importação dos dados para a Tabela Rodadas**
# MAGIC
# MAGIC A importação da tabela "rodadas" foi realizada com sucesso no ambiente do Databricks. Abaixo, descrevo o processo:
# MAGIC
# MAGIC Primeiramente, inicializamos a sessão Spark para permitir a manipulação e processamento eficiente dos dados no ambiente de análise.
# MAGIC
# MAGIC Em seguida, especificamos o caminho do arquivo CSV contendo os dados da tabela "rodadas". O arquivo estava localizado em "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/rodadas.csv".
# MAGIC
# MAGIC Utilizamos a biblioteca Pandas para ler o arquivo CSV e armazenar os dados em um DataFrame Pandas. Além disso, aplicamos a conversão das colunas "inicio" e "fim" para o formato de data e hora usando "pd.to_datetime" para garantir a representação correta desses dados.
# MAGIC
# MAGIC Posteriormente, o DataFrame Pandas foi convertido em um DataFrame Spark por meio do método "spark.createDataFrame", permitindo a transição dos dados para o ambiente de análise distribuída do Spark.
# MAGIC
# MAGIC Por fim, escrevemos o DataFrame Spark diretamente na tabela Delta "dt0028_dev.puc_mvp_cartolafc.rodadas". O modo "overwrite" foi especificado, o que significa que os dados existentes na tabela foram substituídos pelos dados importados do DataFrame Spark, garantindo que a tabela esteja atualizada com os dados do arquivo CSV recém-importado.
# MAGIC
# MAGIC Com esse processo, os dados da tabela "rodadas" foram importados e estão prontos para serem explorados e analisados no ambiente do Databricks.

# COMMAND ----------

# Inicializa a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do arquivo CSV
file_location = "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/rodadas.csv"

# Lê o arquivo CSV usando o Pandas
dfrodadas = pd.read_csv(file_location)
dfrodadas['inicio'] = pd.to_datetime(dfrodadas['inicio'])
dfrodadas['fim'] = pd.to_datetime(dfrodadas['fim'])

# Converte o DataFrame Pandas em um DataFrame Spark
dfspark = spark.createDataFrame(dfrodadas)

# Escreve o DataFrame Spark diretamente na tabela Delta
dfspark.write.mode("overwrite").saveAsTable("dt0028_dev.puc_mvp_cartolafc.rodadas")

# COMMAND ----------

# MAGIC %md
# MAGIC **Importação dos dados para a Tabela Clubes**
# MAGIC
# MAGIC A importação da tabela "clubes" também foi bem-sucedida no ambiente do Databricks. Aqui está uma descrição do processo:
# MAGIC
# MAGIC Primeiramente, inicializamos a sessão Spark para permitir o processamento distribuído dos dados no ambiente.
# MAGIC
# MAGIC Em seguida, especificamos o caminho do arquivo CSV que continha os dados da tabela "clubes". O arquivo estava localizado em "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/clubes.csv".
# MAGIC
# MAGIC Utilizamos a biblioteca Pandas para ler o arquivo CSV e armazenar os dados em um DataFrame Pandas.
# MAGIC
# MAGIC Após a leitura dos dados, realizamos algumas etapas de preparação dos dados no DataFrame Spark:
# MAGIC
# MAGIC Renomeamos as colunas do DataFrame Spark para corresponder aos nomes das colunas desejadas na tabela "clubes", utilizando o método "withColumnRenamed".
# MAGIC
# MAGIC Selecionamos apenas as colunas necessárias para a tabela "clubes", definindo a lista de colunas selecionadas em "colunas_selecionadas".
# MAGIC
# MAGIC Com os dados devidamente preparados, escrevemos o DataFrame Spark diretamente na tabela Delta "dt0028_dev.puc_mvp_cartolafc.clubes". Novamente, o modo "overwrite" foi especificado, garantindo que os dados na tabela sejam substituídos pelos dados importados do DataFrame Spark.
# MAGIC
# MAGIC Completando esse processo, os dados da tabela "clubes" foram importados com sucesso e estão prontos para serem utilizados em análises e consultas no ambiente do Databricks.

# COMMAND ----------

# Inicializa a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do arquivo CSV
file_location = "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/clubes.csv"

# Lê o arquivo CSV usando o Pandas
dfclubes = pd.read_csv(file_location)

# Converte o DataFrame Pandas em um DataFrame Spark
df_spark = spark.createDataFrame(dfclubes)
# Renomeia as colunas
df_spark = df_spark.withColumnRenamed("id", "clube_id")
df_spark = df_spark.withColumnRenamed("nome", "clube")
df_spark = df_spark.withColumnRenamed("abreviacao", "abrev_clube")
df_spark = df_spark.withColumnRenamed("apelido", "apelido_clube")
df_spark = df_spark.withColumnRenamed("slug", "slug_clube")

# Seleciona as colunas desejadas
colunas_selecionadas = ["clube_id", "clube", "abrev_clube", "slug_clube", "apelido_clube", "nome_fantasia"]
df_spark = df_spark.select(*colunas_selecionadas)

# Escreve o DataFrame Spark diretamente na tabela Delta
df_spark.write.mode("overwrite").saveAsTable("dt0028_dev.puc_mvp_cartolafc.clubes")

# COMMAND ----------

# MAGIC %md
# MAGIC **Importação dos dados para a Tabela Posição**
# MAGIC
# MAGIC A importação da tabela "posicao" foi realizada com sucesso no ambiente do Databricks, seguindo os mesmos procedimentos descritos para a importação da tabela "clubes".
# MAGIC
# MAGIC O arquivo CSV estava localizado em "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/posicoes.csv".
# MAGIC
# MAGIC Após a preparação adequada dos dados, os escrevemos no DataFrame Spark, que foi então diretamente incorporado à tabela Delta "dt0028_dev.puc_mvp_cartolafc.posicao". Novamente, optamos pelo modo "overwrite" para assegurar que os dados na tabela fossem atualizados pelos dados recém-importados do DataFrame Spark, conforme necessário.

# COMMAND ----------

# Inicializa a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do arquivo CSV
file_location = "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/posicoes.csv"

# Lê o arquivo CSV usando o Pandas
dfposicoes = pd.read_csv(file_location)

# Converte o DataFrame Pandas em um DataFrame Spark
df_spark = spark.createDataFrame(dfposicoes)
# Renomeia as colunas
df_spark = df_spark.withColumnRenamed("id", "posicao_id")
df_spark = df_spark.withColumnRenamed("abreviacao", "abrev_posicao")
df_spark = df_spark.withColumnRenamed("nome", "posicao")

# Seleciona as colunas desejadas
colunas_selecionadas = ["posicao_id", "abrev_posicao", "posicao"]
df_spark = df_spark.select(*colunas_selecionadas)

# Escreve o DataFrame Spark diretamente na tabela Delta
df_spark.write.mode("overwrite").saveAsTable("dt0028_dev.puc_mvp_cartolafc.posicao")

# COMMAND ----------

# MAGIC %md
# MAGIC **Importação dos dados para a Tabela Partidas**
# MAGIC
# MAGIC A importação da tabela "partidas" foi realizada com êxito no ambiente do Databricks, seguindo um procedimento que envolveu a leitura e a preparação de múltiplos arquivos CSV localizados no diretório "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/".
# MAGIC
# MAGIC Primeiramente, utilizamos a biblioteca Pandas para listar todos os arquivos no diretório que começavam com "partidas_" e tinham a extensão ".csv". Esses arquivos continham os dados das partidas, e os DataFrames correspondentes a cada um deles foram criados e adicionados a uma lista.
# MAGIC
# MAGIC Em seguida, todos esses DataFrames foram concatenados em um único DataFrame Pandas, permitindo a combinação dos dados de todas as partidas.
# MAGIC
# MAGIC Após a concatenação, realizamos a conversão das datas da coluna "partida_data" para o formato apropriado usando "pd.to_datetime".
# MAGIC
# MAGIC Posteriormente, o DataFrame Pandas foi convertido em um DataFrame Spark, permitindo a transição dos dados para o ambiente de análise distribuída do Spark.
# MAGIC
# MAGIC Selecionamos apenas as colunas desejadas e realizamos algumas conversões de tipo de dados, como transformar os placares em números inteiros.
# MAGIC
# MAGIC Por fim, escrevemos o DataFrame Spark diretamente na tabela Delta "dt0028_dev.puc_mvp_cartolafc.partidas". Foi especificado o uso da opção "mergeSchema" para garantir a mesclagem automática de esquema, e o modo "overwrite" foi selecionado para garantir que os dados na tabela fossem atualizados com os dados recém-importados do DataFrame Spark.
# MAGIC
# MAGIC Com este processo, os dados da tabela "partidas" foram importados com sucesso e estão prontos para serem utilizados em análises e consultas no ambiente do Databricks.

# COMMAND ----------

# Inicializa a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do arquivo CSV
diretorio = "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/"

# Lista de arquivos no diretório que começam com "partidas_" e têm extensão ".csv"
arquivos_no_diretorio = [arquivo for arquivo in os.listdir(diretorio) if arquivo.startswith("partidas_") and arquivo.endswith(".csv")]

# Lista para armazenar os DataFrames de cada arquivo
dataframes = []

# Para cada arquivo na lista filtrada
for arquivo in arquivos_no_diretorio:
    # Cria o caminho completo para o arquivo
    caminho_arquivo = os.path.join(diretorio, arquivo)
    # Lê o arquivo CSV usando o Pandas
    df = pd.read_csv(caminho_arquivo)
    # Adiciona o DataFrame à lista
    dataframes.append(df)

# Concatena todos os DataFrames em um único DataFrame
dfpartidas = pd.concat(dataframes, ignore_index=True)
dfpartidas['partida_data'] = pd.to_datetime(dfpartidas['partida_data'])

# Converte o DataFrame Pandas em um DataFrame Spark
dfspark = spark.createDataFrame(dfpartidas)

# Seleciona as colunas desejadas
dfspark = dfspark.select(
    "partida_id",
    "local",
    "partida_data",
    col("placar_oficial_visitante").cast("int").alias("placar_oficial_visitante"),
    col("placar_oficial_mandante").cast("int").alias("placar_oficial_mandante"),
    col("clube_visitante_posicao").cast("int").alias("clube_visitante_posicao"),
    col("clube_casa_posicao").cast("int").alias("clube_casa_posicao"),
    "clube_visitante_id",
    "clube_casa_id",
    "valida",
    "rodada_id"
)

# Escreve o DataFrame Spark diretamente na tabela Delta com mesclagem automática de esquema
dfspark.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("dt0028_dev.puc_mvp_cartolafc.partidas")

# COMMAND ----------

# MAGIC %md
# MAGIC **Importação dos dados para a Tabela Pontuação**
# MAGIC
# MAGIC A importação da tabela "pontuacao" foi concluída com êxito no ambiente do Databricks, seguindo um procedimento semelhante ao utilizado para importação da tabela "partidas".
# MAGIC
# MAGIC Primeiramente, inicializamos a sessão Spark para possibilitar o processamento distribuído dos dados no ambiente.
# MAGIC
# MAGIC Em seguida, especificamos o diretório que continha os arquivos CSV da tabela "pontuacao". A lista de arquivos foi filtrada para incluir apenas aqueles que começavam com "pontuacao_rodada_" e tinham a extensão ".csv". Esses arquivos continham os dados das pontuações dos jogadores em diferentes rodadas.
# MAGIC
# MAGIC Os DataFrames correspondentes a cada arquivo foram criados e adicionados a uma lista.
# MAGIC
# MAGIC Após a leitura dos dados, todos esses DataFrames foram concatenados em um único DataFrame Pandas, permitindo a combinação dos dados de todas as rodadas.
# MAGIC
# MAGIC Em seguida, realizamos algumas etapas de preparação dos dados no DataFrame Spark:
# MAGIC
# MAGIC Utilizamos a função "cast" para converter a coluna "pontuacao" para o tipo de dado "decimal(4,2)" para representar as pontuações dos jogadores com precisão.
# MAGIC
# MAGIC Selecionamos apenas as colunas desejadas e realizamos conversões de tipo de dados, como transformar as pontuações, cartões e outras estatísticas em números inteiros.
# MAGIC
# MAGIC Por fim, escrevemos o DataFrame Spark diretamente na tabela Delta "dt0028_dev.puc_mvp_cartolafc.pontuacao". Novamente, foi especificado o uso da opção "mergeSchema" para garantir a mesclagem automática de esquema, e o modo "overwrite" foi selecionado para garantir que os dados na tabela fossem atualizados com os dados recém-importados do DataFrame Spark, se necessário.
# MAGIC
# MAGIC Com este processo, os dados da tabela "pontuacao" foram importados com sucesso e estão prontos para serem utilizados em análises e consultas no ambiente do Databricks.

# COMMAND ----------

# Inicializa a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do arquivo CSV
diretorio = "/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/"

# Lista de arquivos no diretório que começam com "pontuacoes_" e têm extensão ".csv"
arquivos_no_diretorio = [arquivo for arquivo in os.listdir(diretorio) if arquivo.startswith("pontuacao_rodada_") and arquivo.endswith(".csv")]

# Lista para armazenar os DataFrames de cada arquivo
dataframes = []

# Para cada arquivo na lista filtrada
for arquivo in arquivos_no_diretorio:
    # Cria o caminho completo para o arquivo
    caminho_arquivo = os.path.join(diretorio, arquivo)
    # Lê o arquivo CSV usando o Pandas
    df = pd.read_csv(caminho_arquivo)
    # Adiciona o DataFrame à lista
    dataframes.append(df)

# Concatena todos os DataFrames em um único DataFrame
dfpontuacoes = pd.concat(dataframes, ignore_index=True)

# Converte o DataFrame Pandas em um DataFrame Spark
dfspark = spark.createDataFrame(dfpontuacoes)
# Use a função cast para converter a coluna 'pontuacao' para decimal(4,2)
dfspark = dfspark.withColumn("pontuacao", col("pontuacao").cast("decimal(4,2)"))

# Seleciona as colunas desejadas
dfspark = dfspark.select(
    "apelido",
    "pontuacao",
    "posicao_id",
    "clube_id",
    "entrou_em_campo",
    "rodada_id", 
    col("CA").cast("int").alias("CA"),
    col("DS").cast("int").alias("DS"),
    col("FC").cast("int").alias("FC"),
    col("FF").cast("int").alias("FF"),
    col("FD").cast("int").alias("FD"),
    col("FS").cast("int").alias("FS"),
    col("I").cast("int").alias("I"),
    col("SG").cast("int").alias("SG"),
    col("A").cast("int").alias("A"),
    col("G").cast("int").alias("G"),
    col("DE").cast("int").alias("DE"),
    col("GS").cast("int").alias("GS"),
    col("V").cast("int").alias("V"),
    col("PS").cast("int").alias("PS"),
    col("FT").cast("int").alias("FT"),
    col("PP").cast("int").alias("PP"),
    col("DP").cast("int").alias("DP"),
    col("CV").cast("int").alias("CV"),
    col("PC").cast("int").alias("PC"),
)

# Escreve o DataFrame Spark diretamente na tabela Delta com mesclagem automática de esquema
dfspark.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("dt0028_dev.puc_mvp_cartolafc.pontuacao")

# COMMAND ----------

# MAGIC %md
# MAGIC **Inclusão do Campo "partida_id" na Tabela "Pontuação"**
# MAGIC
# MAGIC É importante notar que a tabela "pontuação", importada por meio da API, não contém o campo "partida_id", que desempenha um papel crucial para associar as pontuações dos jogadores às partidas específicas. Para resolver essa lacuna, realizamos um procedimento de consulta à tabela "partidas" para adquirir as informações necessárias e preencher o campo "partida_id" na tabela "pontuação".
# MAGIC
# MAGIC O código SQL abaixo utiliza a cláusula **MERGE INTO** para combinar as tabelas "pontuação" e "partidas" com base em condições específicas. Aqui está uma explicação passo a passo do que o código faz:
# MAGIC
# MAGIC **Especificar o Banco de Dados de Destino:** O comando **USE** é utilizado para selecionar o banco de dados de destino, neste caso, "dt0028_dev.PUC_MVP_CartolaFC".
# MAGIC
# MAGIC **MERGE INTO:** A cláusula **MERGE INTO** é usada para realizar uma operação de mesclagem entre duas tabelas, neste caso, "pontuação" e "partidas".
# MAGIC
# MAGIC **ON Clause:** A cláusula **ON** especifica as condições de correspondência para determinar como as linhas das duas tabelas devem ser combinadas. O código utiliza duas condições:
# MAGIC
# MAGIC  - **p.rodada_id = pt.rodada_id:** Isso garante que apenas as linhas com a mesma rodada em ambas as tabelas sejam combinadas.
# MAGIC
# MAGIC  - **(p.clube_id = pt.clube_visitante_id OR p.clube_id = pt.clube_casa_id):** Isso verifica se o clube na tabela "pontuação" corresponde ao clube visitante ou ao clube da casa na tabela "partidas".
# MAGIC
# MAGIC **WHEN MATCHED:** A cláusula **WHEN MATCHED** especifica o que fazer quando uma correspondência é encontrada entre as duas tabelas.
# MAGIC
# MAGIC  - **UPDATE SET p.partida_id = pt.partida_id:** Isso atualiza o campo "partida_id" na tabela "pontuação" com o valor correspondente da tabela "partidas".
# MAGIC  
# MAGIC No geral, este código realiza a operação de mesclagem entre as duas tabelas com base nas condições fornecidas, garantindo que o campo "partida_id" na tabela "pontuação" seja preenchido corretamente com as informações de partida da tabela "partidas". Isso é fundamental para vincular as pontuações dos jogadores às partidas específicas e, assim, possibilitar análises precisas dos dados.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE dt0028_dev.PUC_MVP_CartolaFC;
# MAGIC
# MAGIC MERGE INTO pontuacao AS p
# MAGIC USING partidas AS pt
# MAGIC ON p.rodada_id = pt.rodada_id
# MAGIC    AND (p.clube_id = pt.clube_visitante_id OR p.clube_id = pt.clube_casa_id)
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET p.partida_id = pt.partida_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Para confirmar o sucesso das operações de carga de dados nas tabelas, utilizei o código SQL abaixo para contar o número de registros em cada uma delas. Esse código nos permitiu verificar o número total de registros em cada tabela, confirmando assim a integridade das cargas de dados realizadas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contagem de registros em cada tabela
# MAGIC SELECT 'rodadas' AS tabela, COUNT(*) AS total_registros FROM dt0028_dev.PUC_MVP_CartolaFC.rodadas
# MAGIC UNION ALL
# MAGIC SELECT 'partidas' AS tabela, COUNT(*) AS total_registros FROM dt0028_dev.PUC_MVP_CartolaFC.partidas
# MAGIC UNION ALL
# MAGIC SELECT 'posicao' AS tabela, COUNT(*) AS total_registros FROM dt0028_dev.PUC_MVP_CartolaFC.posicao
# MAGIC UNION ALL
# MAGIC SELECT 'clubes' AS tabela, COUNT(*) AS total_registros FROM dt0028_dev.PUC_MVP_CartolaFC.clubes
# MAGIC UNION ALL
# MAGIC SELECT 'pontuacao' AS tabela, COUNT(*) AS total_registros FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Análise e Tratamento dos Dados

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Qualidade de dados
# MAGIC
# MAGIC É essencial realizar uma análise de qualidade para cada atributo presente em nosso conjunto de dados. Esta análise visa identificar qualquer problema ou inconsistência nos dados que possa impactar as respostas às perguntas que buscamos resolver.
# MAGIC
# MAGIC Ao realizar essa análise, podemos detectar problemas como dados ausentes, valores inconsistentes, ou erros de formatação. Para resolver esses problemas, podemos adotar estratégias como o preenchimento de valores ausentes com informações relevantes, a correção de erros de digitação, ou a padronização de formatos.
# MAGIC
# MAGIC Para iniciar nossa análise, começaremos avaliando as dimensões de nossas tabelas.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE dt0028_dev.PUC_MVP_CartolaFC;
# MAGIC
# MAGIC -- Visualize as dimensões dos datasets
# MAGIC SELECT 'rodadas' AS tabela, COUNT(*) AS total_linhas, COUNT(DISTINCT rodada_id) AS qtde_dist 
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.rodadas
# MAGIC UNION ALL
# MAGIC SELECT 'partidas' AS tabela, COUNT(*) AS total_linhas, COUNT(DISTINCT partida_id) AS qtde_dist
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.partidas
# MAGIC UNION ALL
# MAGIC SELECT 'posicao' AS tabela, COUNT(*) AS total_linhas, COUNT(DISTINCT posicao_id) AS qtde_dist
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.posicao
# MAGIC UNION ALL
# MAGIC SELECT 'clubes' AS tabela, COUNT(*) AS total_linhas, COUNT(DISTINCT clube_id) AS qtde_dist
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.clubes
# MAGIC UNION ALL
# MAGIC SELECT 'pontuacao' AS tabela, COUNT(*) AS total_linhas, COUNT(DISTINCT apelido) AS qtde_dist
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC No resultado apresentado na tabela acima, já podemos observar algumas informações importantes.
# MAGIC
# MAGIC A quantidade de rodadas apresentada está correta, visto que o Campeonato Brasileiro é composto por 38 rodadas. Além disso, a quantidade de partidas também é coerente, uma vez que temos 10 jogos em cada rodada, totalizando as 380 partidas listadas na tabela.
# MAGIC
# MAGIC A tabela de posições exibe 6 registros, que correspondem às posições utilizadas para a escalação no CartolaFC. Podemos verificar essas informações ao exibir os detalhes da tabela "posicao":

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir todas as informações da tabela posicao
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.posicao;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC A quantidade distinta de times apresentada na tabela "clubes" não está de acordo com o valor esperado. O Campeonato Brasileiro é composto por 20 times, porém a tabela "clubes" contém 67 registros, ultrapassando o número esperado. Para verificar essa discrepância, realizamos uma junção entre as tabelas "pontuação" e "clubes" e identificamos quais clubes possuem pontuação igual a zero ou nula.
# MAGIC
# MAGIC Este código realiza uma junção entre as tabelas "clubes" e "pontuacao" usando a cláusula "LEFT JOIN", o que significa que todos os registros da tabela "clubes" serão incluídos na saída, independentemente de terem correspondências na tabela "pontuacao". Em seguida, agrupamos os resultados pelo nome do clube e calculamos a soma das pontuações de cada clube usando a função "SUM". Utilizamos a função "COALESCE" para tratar valores nulos, substituindo-os por zero.
# MAGIC
# MAGIC O resultado da consulta nos mostrará quais clubes têm uma pontuação total igual a zero, o que nos ajudará a identificar os clubes que podem estar contribuindo para a discrepância na quantidade de registros na tabela "clubes".

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Junte a tabela pontuacao com a tabela clubes e filtre clubes com pontuação igual a zero ou null
# MAGIC SELECT
# MAGIC   c.clube AS Clube,  
# MAGIC   SUM(COALESCE(p.pontuacao, 0)) AS Pontuacao_Clube
# MAGIC FROM
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.clubes c
# MAGIC LEFT JOIN
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.pontuacao p
# MAGIC ON
# MAGIC   p.clube_id = c.clube_id
# MAGIC GROUP BY
# MAGIC   c.clube
# MAGIC HAVING
# MAGIC   SUM(COALESCE(p.pontuacao, 0)) = 0
# MAGIC ORDER BY
# MAGIC   Clube ASC

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos observar que nossa consulta identificou 47 clubes com pontuação zero, o que confirma que na tabela "pontuação" temos apenas os 20 clubes do campeonato com pontuações válidas.

# COMMAND ----------

# MAGIC %md
# MAGIC Agora, procederemos à exibição das informações de cada tabela do Banco de Dados, incluindo um exemplo dos dados contidos em cada uma delas.

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabela Rodadas**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os tipos de dados da tabela 'rodadas'
# MAGIC DESCRIBE dt0028_dev.PUC_MVP_CartolaFC.rodadas;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os primeiros dados da tabela 'rodadas'
# MAGIC SELECT * FROM dt0028_dev.PUC_MVP_CartolaFC.rodadas LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabela Partidas**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os tipos de dados da tabela 'partidas'
# MAGIC DESCRIBE dt0028_dev.PUC_MVP_CartolaFC.partidas;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os primeiros dados da tabela 'partidas'
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.partidas
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabela Posição**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os tipos de dados da tabela 'posicao'
# MAGIC DESCRIBE dt0028_dev.PUC_MVP_CartolaFC.posicao;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os primeiros dados da tabela 'posicao'
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.posicao
# MAGIC LIMIT 6;

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabela Clubes**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os tipos de dados da tabela 'clubes'
# MAGIC DESCRIBE dt0028_dev.PUC_MVP_CartolaFC.clubes;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os primeiros dados da tabela 'clubes'
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.clubes
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabela Pontuação**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os tipos de dados da tabela 'pontuacao'
# MAGIC DESCRIBE dt0028_dev.PUC_MVP_CartolaFC.pontuacao;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os primeiros dados da tabela 'pontuacao'
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Com base nessas análises, podemos validar nossas tabelas, com exceção da tabela "clubes", que contém informações sobre times que não estão atualmente participando do campeonato. Essa informação não afeta as análises a serem realizadas, uma vez que a tabela de "pontuação" não inclue dados desses times. No entanto, por motivos didáticos e para manter a integridade dos dados, optamos por realizar uma limpeza da tabela "clubes".
# MAGIC
# MAGIC O código abaixo cria uma tabela temporária chamada "clubes_a_excluir" que identifica os clubes com pontuação igual a zero. Em seguida, os clubes identificados na tabela temporária são excluídos da tabela "clubes". Isso nos permite manter apenas os clubes relevantes para as análises em nossa base de dados.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crie uma tabela temporária para identificar os clubes com pontuação igual a zero
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clubes_a_excluir AS
# MAGIC SELECT
# MAGIC   c.clube_id
# MAGIC FROM
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.clubes c
# MAGIC LEFT JOIN
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.pontuacao p
# MAGIC ON
# MAGIC   p.clube_id = c.clube_id
# MAGIC GROUP BY
# MAGIC   c.clube_id
# MAGIC HAVING
# MAGIC   SUM(COALESCE(p.pontuacao, 0)) = 0;
# MAGIC
# MAGIC -- Exclua os clubes com base na tabela temporária
# MAGIC DELETE FROM dt0028_dev.PUC_MVP_CartolaFC.clubes
# MAGIC WHERE clube_id IN (SELECT clube_id FROM clubes_a_excluir);

# COMMAND ----------

# MAGIC %md
# MAGIC Agora podemos exibir os dados da tabela "clubes", que conterá apenas os 20 clubes que estão participando do Campeonato de 2023.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os primeiros dados da tabela 'clubes'
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.clubes

# COMMAND ----------

# MAGIC %md
# MAGIC **Tratamento de Valores Nulos e Zeros**
# MAGIC
# MAGIC Nesta etapa, nossa análise será direcionada para o tratamento de valores nulos e zeros em nossa base de dados, com ênfase na tabela "pontuação". Esta tabela contém os dados quantitativos essenciais para nossa análise. No entanto, para uma abordagem mais minuciosa e abrangente, realizaremos a junção com outras tabelas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN pontuacao IS NULL THEN 1 ELSE 0 END) AS pontuacao,
# MAGIC   SUM(CASE WHEN CA IS NULL THEN 1 ELSE 0 END) AS CS,
# MAGIC   SUM(CASE WHEN DS IS NULL THEN 1 ELSE 0 END) AS DS,
# MAGIC   SUM(CASE WHEN FC IS NULL THEN 1 ELSE 0 END) AS FC,
# MAGIC   SUM(CASE WHEN FF IS NULL THEN 1 ELSE 0 END) AS FF,
# MAGIC   SUM(CASE WHEN FD IS NULL THEN 1 ELSE 0 END) AS FD,
# MAGIC   SUM(CASE WHEN FS IS NULL THEN 1 ELSE 0 END) AS FS,
# MAGIC   SUM(CASE WHEN I IS NULL THEN 1 ELSE 0 END) AS I,
# MAGIC   SUM(CASE WHEN SG IS NULL THEN 1 ELSE 0 END) AS SG,
# MAGIC   SUM(CASE WHEN A IS NULL THEN 1 ELSE 0 END) AS A,
# MAGIC   SUM(CASE WHEN G IS NULL THEN 1 ELSE 0 END) AS G,
# MAGIC   SUM(CASE WHEN DE IS NULL THEN 1 ELSE 0 END) AS DE,
# MAGIC   SUM(CASE WHEN GS IS NULL THEN 1 ELSE 0 END) AS GS,
# MAGIC   SUM(CASE WHEN V IS NULL THEN 1 ELSE 0 END) AS V,
# MAGIC   SUM(CASE WHEN PS IS NULL THEN 1 ELSE 0 END) AS PS,
# MAGIC   SUM(CASE WHEN FT IS NULL THEN 1 ELSE 0 END) AS FT,
# MAGIC   SUM(CASE WHEN PP IS NULL THEN 1 ELSE 0 END) AS PP,
# MAGIC   SUM(CASE WHEN DP IS NULL THEN 1 ELSE 0 END) AS DP,
# MAGIC   SUM(CASE WHEN CV IS NULL THEN 1 ELSE 0 END) AS CV,
# MAGIC   SUM(CASE WHEN PC IS NULL THEN 1 ELSE 0 END) AS PC
# MAGIC FROM
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.pontuacao;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS Qtd_Pontuacao_Zero
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC WHERE pontuacao = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Observamos que o campo "pontuação" e as colunas relacionadas aos scouts não contêm valores nulos. No entanto, identificamos a presença de 571 valores zerados no campo de pontuação. Vale destacar que os scouts podem naturalmente apresentar valores zero, por isso não foram incluídos nesta análise. Agora, concentraremos nossa análise nas pontuações que apresentam valor zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir informações da tabela pontuação para jogadores com pontuação zero
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC WHERE pontuacao = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Realizamos uma análise detalhada dos jogadores que obtiveram pontuação zero. É importante ressaltar que um jogador pode entrar em campo e não pontuar, ou ainda, pode ter scouts positivos e negativos que se anulam mutuamente. Vamos considerar o exemplo do jogador Bento na 7ª rodada como ilustração.
# MAGIC
# MAGIC Nessa rodada, Bento registrou zero pontos, mas ao examinar seus scouts, notamos que ele teve 2 DE (Defesas) e 2 GS (Gols Sofridos). No Cartola, cada defesa vale 1 ponto, enquanto cada gol sofrido tem uma pontuação negativa de -1. Portanto, a pontuação zero de Bento na 7ª rodada é validada pela combinação desses scouts, onde as defesas positivas e os gols sofridos negativos se anulam, resultando em uma pontuação líquida de zero. Essa análise ilustra como os diferentes scouts podem afetar a pontuação final de um jogador no Cartola.

# COMMAND ----------

# MAGIC %md
# MAGIC No entanto, na tabela "pontuação", dispomos da coluna "entrou_em_campo" que sinaliza se o jogador efetivamente participou da rodada. Para compreender melhor essa informação, realizamos uma análise específica. 
# MAGIC
# MAGIC O código a seguir nos permite verificar a distribuição de valores "true" (verdadeiro) e "false" (falso) na coluna "entrou_em_campo", fornecendo informações sobre a participação ou ausência de um jogador em uma determinada rodada.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar a quantidade de true e false na coluna "entrou_em_campo"
# MAGIC SELECT
# MAGIC   entrou_em_campo,
# MAGIC   COUNT(*) AS quantidade
# MAGIC FROM
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC GROUP BY
# MAGIC   entrou_em_campo;

# COMMAND ----------

# MAGIC %md
# MAGIC Identificamos que a tabela "pontuação" contém 37 registros de jogadores que não entraram em campo durante suas respectivas rodadas. A fim de analisar mais detalhadamente esses casos, realizamos a seguinte consulta:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir informações da tabela pontuação para jogadores com pontuação zero
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC WHERE entrou_em_campo = false;

# COMMAND ----------

# MAGIC %md
# MAGIC Analisando o resultado da tabela apresentada acima, observamos que um jogador pode receber cartões amarelos ou vermelhos mesmo não tendo participado efetivamente da partida. Portanto, essas pontuações são válidas. Para prosseguir com nossas análises, optamos por remover apenas os casos em que o jogador não esteve em campo e não recebeu cartões. Utilizamos a seguinte consulta:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir informações da tabela pontuação para jogadores com pontuação zero
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC WHERE entrou_em_campo = false
# MAGIC   AND CA = 0
# MAGIC   AND CV = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Ainda foram apresentadas 10 linhas de casos com pontuação e scouts zerados. No entanto, na tabela "partidas," existe o campo "valida" que indica se a partida foi considerada válida para o Cartola. Vamos incorporar esse campo às nossas análises para uma compreensão mais completa desses casos.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir informações da tabela partidas para os jogadores sem scouts CA e CV e com pontuação zero
# MAGIC SELECT
# MAGIC   p.apelido AS Jogador,
# MAGIC   po.posicao AS Posicao,
# MAGIC   c.clube AS Clube, 
# MAGIC   p.rodada_id AS Rodada, 
# MAGIC   p.entrou_em_campo AS Jogou,
# MAGIC   pa.valida AS Valida,
# MAGIC   SUM(pontuacao) AS Pontuacao_Total
# MAGIC FROM
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.pontuacao p
# MAGIC JOIN
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.posicao po
# MAGIC ON
# MAGIC   p.posicao_id = po.posicao_id
# MAGIC JOIN
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.clubes c
# MAGIC ON
# MAGIC   p.clube_id = c.clube_id
# MAGIC JOIN
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.partidas pa
# MAGIC ON
# MAGIC   p.partida_id = pa.partida_id
# MAGIC WHERE p.entrou_em_campo = false
# MAGIC   AND p.CA = 0
# MAGIC   AND p.CV = 0
# MAGIC GROUP BY
# MAGIC   p.apelido, po.posicao, c.clube, p.rodada_id, p.entrou_em_campo, pa.valida
# MAGIC ORDER BY
# MAGIC   p.apelido;

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos verificar que, mesmo quando a partida não é considerada válida pelo Cartola, o sistema mantém a linha de pontuação para os técnicos. No entanto, para uma análise abrangente, desejamos verificar se essa condição se aplica a outros jogadores além dos técnicos.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir informações da tabela partifas para os técnicos sem pontuação com pontuação zero
# MAGIC SELECT
# MAGIC   p.apelido AS Jogador,
# MAGIC   po.posicao AS Posicao,
# MAGIC   c.clube AS Clube, 
# MAGIC   p.rodada_id AS Rodada, 
# MAGIC   p.entrou_em_campo AS Jogou,
# MAGIC   pa.valida AS Valida,
# MAGIC   SUM(pontuacao) AS Pontuacao_Total
# MAGIC FROM
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.pontuacao p
# MAGIC JOIN
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.posicao po
# MAGIC ON
# MAGIC   p.posicao_id = po.posicao_id
# MAGIC JOIN
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.clubes c
# MAGIC ON
# MAGIC   p.clube_id = c.clube_id
# MAGIC JOIN
# MAGIC   dt0028_dev.PUC_MVP_CartolaFC.partidas pa
# MAGIC ON
# MAGIC   p.partida_id = pa.partida_id
# MAGIC WHERE pa.valida = false
# MAGIC GROUP BY
# MAGIC   p.apelido, po.posicao, c.clube, p.rodada_id, p.entrou_em_campo, pa.valida
# MAGIC ORDER BY
# MAGIC   p.apelido;

# COMMAND ----------

# MAGIC %md
# MAGIC Após uma análise detalhada, podemos confirmar que todos os casos em que a partida não é considerada válida correspondem às linhas de técnicos. Nessas situações, não encontramos nenhum registro de pontuação ou scouts associados a essas partidas. Portanto, com base nessa constatação, procederemos à exclusão dessas linhas da nossa tabela de pontuação. Isso nos permitirá manter apenas os dados relevantes para os jogadores que efetivamente participaram das partidas válidas no Cartola.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exclua as linhas da tabela pontuacao com base nos critérios especificados
# MAGIC DELETE FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC WHERE partida_id IN (
# MAGIC   SELECT p.partida_id
# MAGIC   FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao p
# MAGIC   JOIN dt0028_dev.PUC_MVP_CartolaFC.partidas pa ON p.partida_id = pa.partida_id
# MAGIC   WHERE pa.valida = false
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Agora restam apenas dois casos para análise após a exclusão dos registros indesejados. Abaixo está a consulta realizada para examinar essas ocorrências específicas:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir informações da tabela pontuação para jogadores com pontuação zero
# MAGIC SELECT *
# MAGIC FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC WHERE entrou_em_campo = false
# MAGIC   AND CA = 0
# MAGIC   AND CV = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Esses jogadores são aqueles que não entraram em campo em partidas válidas para o CartolaFC, não registraram pontuações positivas ou negativas e não acumularam nenhum scout. Manter esses casos em nossa base de dados pode comprometer os indicadores calculados, especialmente em volumes significativos. Portanto, decidimos excluí-los da nossa base de dados, segmentando o somatório das pontuações em scouts negativos e positivos.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM dt0028_dev.PUC_MVP_CartolaFC.pontuacao
# MAGIC WHERE entrou_em_campo = false
# MAGIC   AND pontuacao = 0
# MAGIC   AND (DS + FF + FD + FS + SG + A + G + DE + V + PS + FT + DP) = 0
# MAGIC   AND (CA + FC + I + GS + PP + CV + PC) = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Solução do problema

# COMMAND ----------

# MAGIC %md
# MAGIC A partir deste ponto, iremos conduzir consultas, gerar gráficos e realizar análises essenciais para responder às questões que foram formuladas no nosso problema. Para viabilizar esse processo de análise de dados, criaremos uma única tabela consolidada contendo todas as informações necessárias para gerar os gráficos no PowerBI.
# MAGIC
# MAGIC O código abaixo realiza uma junção entre as tabelas "pontuacao", "partidas", "clubes", "rodadas" e "posicao", reunindo todos os dados relevantes em uma única tabela.
# MAGIC
# MAGIC O resultado dessa consulta foi convertido em um DataFrame Pandas e salvo como um arquivo CSV no Databricks, que servirá como fonte de dados para a criação dos gráficos no PowerBI.
# MAGIC
# MAGIC Estamos agora prontos para iniciar as análises e gerar as visualizações necessárias para abordar as questões propostas.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE dt0028_dev.PUC_MVP_CartolaFC;

# COMMAND ----------

sql = " select * "
sql += " from pontuacao o ,  partidas p, clubes c1, rodadas r, posicao po"
sql += " where o.partida_id = p.partida_id"
sql += " and o.clube_id = c1.clube_id"
sql += " and p.rodada_id = r.rodada_id"
sql += " and o.posicao_id = po.posicao_id"

dfsql = spark.sql(sql)
display(dfsql)

# Converta a lista de rodadas_id não encontradas em um DataFrame Pandas
dftotal = dfsql.toPandas()

# Especifica o caminho para salvar o arquivo CSV no Databricks
csv_path = '/Workspace/Users/adriana.legal@petrobras.com.br/Arquivos/resultados.csv'
# Salva o DataFrame como um arquivo CSV
dftotal.to_csv(csv_path, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Questões**
# MAGIC
# MAGIC A partir deste ponto, iniciaremos nossas análises para responder às questões que foram formuladas no âmbito do nosso projeto.<br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Qual jogador detém a melhor média de pontos ao longo do campeonato?<br>
# MAGIC
# MAGIC Para identificar o jogador com a melhor média de pontos ao longo do campeonato, realizamos uma consulta em nosso DataFrame dftotal.

# COMMAND ----------

# Calcula a média de pontuação por jogador
top_10_jogadores = dftotal.groupby('apelido')['pontuacao'].mean().reset_index()
top_10_jogadores['pontuacao'] = top_10_jogadores['pontuacao'].round(2)
top_10_jogadores = top_10_jogadores.sort_values(by='pontuacao', ascending=False).head(10)

# Exibe as informações
print("Top 10 Jogadores com a Maior Média de Pontuação no Campeonato:")
display(top_10_jogadores)

# COMMAND ----------

# MAGIC %md
# MAGIC Identificamos que o jogador Slimani possui a maior média de pontos do campeonato, registrando uma média de 10 pontos. No entanto, é crucial considerar outro fator relevante para uma avaliação completa: o número de partidas disputadas por cada jogador.

# COMMAND ----------

# Calcula a média de pontuação e a quantidade de jogos disputados por jogador
top_jogadores = dftotal.groupby('apelido')['pontuacao'].agg(['mean', 'count']).reset_index()
top_jogadores.columns = ['apelido', 'pontuacao_media', 'jogos_disputados']
top_jogadores['pontuacao_media'] = top_jogadores['pontuacao_media'].round(2)

# Seleciona os 10 jogadores com as maiores médias de pontuação
top_10_jogadores = top_jogadores.sort_values(by='pontuacao_media', ascending=False).head(10)

# Exibe as informações
print("Top 10 Jogadores com a Maior Média de Pontuação e Quantidade de Jogos Disputados:")
display(top_10_jogadores)

# COMMAND ----------

# MAGIC %md
# MAGIC Também podemos apresentar essa análise por meio de um gráfico.
# MAGIC
# MAGIC ![Questao1](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%201a.png?raw=true)<br>

# COMMAND ----------

# MAGIC %md
# MAGIC Incluindo essa informação, podemos observar que os dois primeiros colocados no top 10 disputaram apenas uma partida. Para calcular uma média confiável, é aconselhável considerar mais de 3 jogos disputados. Portanto, vamos aplicar essa restrição à nossa consulta.

# COMMAND ----------

# Calcula a média de pontuação e a quantidade de jogos disputados por jogador
top_jogadores = dftotal.groupby('apelido')['pontuacao'].agg(['mean', 'count']).reset_index()
top_jogadores.columns = ['apelido', 'pontuacao_media', 'jogos_disputados']
top_jogadores['pontuacao_media'] = top_jogadores['pontuacao_media'].round(2)

# Filtra os jogadores com mais de 3 jogos disputados
top_jogadores = top_jogadores[top_jogadores['jogos_disputados'] > 3]

# Seleciona os 10 jogadores com as maiores médias de pontuação
top_10_jogadores = top_jogadores.sort_values(by='pontuacao_media', ascending=False).head(10)

# Exibe as informações
print("Top 10 Jogadores com a Maior Média de Pontuação e Quantidade de Jogos Disputados:")
display(top_10_jogadores)

# COMMAND ----------

# MAGIC %md
# MAGIC Para responder à questão 1, podemos realizar uma análise com base na média de pontos dos jogadores e na quantidade de jogos disputados. Essa análise pode ser ainda mais informativa se considerarmos a segregação por posição, como ilustrado nos gráficos a seguir:
# MAGIC
# MAGIC ![Questao1b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%201b.png?raw=true)<br>
# MAGIC
# MAGIC Caso desejemos identificar os atacantes com as maiores médias no campeonato, podemos recorrer ao gráfico a seguir:
# MAGIC
# MAGIC ![Questao1b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%201c.png?raw=true)<br>
# MAGIC
# MAGIC Concluímos, assim, que o atacante Vegetti, do Vasco, ostenta a maior média de pontuação no campeonato. No entanto, é importante notar que ele participou de apenas seis jogos. Por outro lado, jogadores como Tiquinho Soares, Luis Suárez e Hulk apresentam médias acima de 8 pontos, tendo disputado 19, 18 e 20 jogos, respectivamente.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Quais são os jogadores com melhor desempenho ao longo do campeonato?<br>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Para responder a esse tipo de pergunta, o gráfico mais adequado é o gráfico radar. Também conhecido como gráfico de teia ou gráfico de aranha, ele é uma representação visual que possibilita uma análise comparativa de múltiplas variáveis em relação a um ponto central. Esse tipo de gráfico é particularmente valioso quando se pretende avaliar o desempenho ou as características de diferentes elementos em um conjunto de dados, como no caso da análise de jogadores em uma competição esportiva.
# MAGIC
# MAGIC Aqui, estamos comparando os cinco atacantes com as maiores médias de pontuação, considerando aqueles que disputaram mais de três jogos. Ao examinar o raio que representa a média de pontuação, observamos que todos os jogadores têm uma média de 8 pontos, com Vegetti ligeiramente à frente dos demais. No entanto, quando consideramos a quantidade de jogos disputados, percebemos que ele é o jogador com a menor quantidade, o que já sabemos influencia a média de pontos.
# MAGIC
# MAGIC No extremo oposto do gráfico, destacamos Tiquinho Soares, que lidera com a maior quantidade de gols, seguido de perto por Vitor Roque. Hulk e Luis Suárez sobressaem-se pela quantidade significativa de assistências que oferecem. Vitor Roque também se destaca pelo grande número de desarmes efetuados, o que pode ser um fator diferencial na escolha entre ele e Tiquinho Soares na hora de escalar um time.
# MAGIC
# MAGIC ![Questao2](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Atacantes.png?raw=true)<br>
# MAGIC
# MAGIC O gráfico radar é uma ferramenta poderosa que permite comparar várias variáveis simultaneamente e identificar padrões e relações entre elas. Sua facilidade de interpretação baseia-se na área total apresentada, onde quanto maior a área, mais um jogador se destaca em relação aos outros.
# MAGIC
# MAGIC Além disso, é possível personalizar o gráfico incluindo ou excluindo jogadores para comparação. Isso oferece flexibilidade para analisar os jogadores desejados e selecionar os scouts mais relevantes para cada posição, pois cada posição pode ter métricas distintas de destaque. No gráfico abaixo, por exemplo, são apresentados os scouts selecionados para a análise dos goleiros:
# MAGIC
# MAGIC ![Questao2b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Goleiros.png?raw=true)<br>
# MAGIC
# MAGIC Em resumo, o gráfico radar é uma ferramenta versátil e visualmente informativa que auxilia na tomada de decisões ao comparar o desempenho de elementos em várias métricas ao mesmo tempo.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Qual jogador apresenta o maior número de gols marcados?<br>

# COMMAND ----------

# MAGIC %md
# MAGIC No gráfico a seguir, podemos constatar que Tiquinho Soares, jogador do Botafogo, lidera a artilharia do campeonato, tendo assinalado 13 gols, enquanto Vitor Roque, do Atlético Paranaense, segue de perto com 11 gols.
# MAGIC ![Questao3](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%203a.png?raw=true)<br>
# MAGIC
# MAGIC Além disso, é possível realizar essa análise segmentada por posição. No que diz respeito aos meio-campistas, identificamos que Rafael Veiga, jogador do Palmeiras, se destaca como o principal goleador, tendo anotado um total de 7 gols.
# MAGIC
# MAGIC ![Questao3b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%203b.png?raw=true)<br>

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Qual goleiro é o menos vazado?<br>

# COMMAND ----------

# MAGIC %md
# MAGIC Esta questão, apesar de sua simplicidade aparente, apresenta uma resposta bastante complexa, e para compreendê-la em sua totalidade, precisamos explorar diversas perspectivas:
# MAGIC
# MAGIC Primeiramente, ao analisarmos apenas a quantidade de gols sofridos, identificamos que o goleiro com menos gols sofridos é Fernando Miguel, do Fortaleza, como demonstrado no gráfico abaixo:
# MAGIC
# MAGIC ![Questao4a](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%204a.png?raw=true)<br>
# MAGIC
# MAGIC No entanto, para uma análise completa, é essencial levar em consideração a quantidade de jogos que cada goleiro disputou. Isso é evidenciado quando observamos que Fernando Miguel participou de apenas 4 jogos, como é o caso de outros jogadores, com Lucas Peri, do Botafogo, e Cleiton, do Bragantino, tendo uma quantidade de jogos mais significativa, como ilustrado no gráfico a seguir:
# MAGIC
# MAGIC ![Questao4b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%204b.png?raw=true)<br>
# MAGIC
# MAGIC Agora, para uma perspectiva ainda mais refinada, é importante considerar o indicador de Saldo de Gols (SG), que revela quantos jogos o goleiro manteve o gol invicto. Esse é um dos melhores indicadores para avaliar os goleiros, como exemplificado no gráfico abaixo, onde Lucas Peri, apesar de ter sofrido a mesma quantidade de gols que Cleiton, manteve o SG em 13 jogos, enquanto Cleiton conseguiu essa marca em apenas 11 partidas:
# MAGIC
# MAGIC ![Questao4c](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%204c.png?raw=true)<br>
# MAGIC
# MAGIC Para concluir a análise dos goleiros, é relevante considerar a quantidade de defesas feitas por cada um, pois, em algumas situações, um goleiro pode não manter o SG, mas realizar um número substancial de defesas que compensa sua pontuação. Apresentamos, no gráfico de área a seguir, essa informação secundária, destacando o potencial dos goleiros em termos de defesas:
# MAGIC
# MAGIC ![Questao4d](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%204d.png?raw=true)<br>
# MAGIC
# MAGIC Dessa forma, ao analisarmos esses diversos aspectos, obtemos uma visão mais completa e precisa do desempenho dos goleiros no campeonato.

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Qual é a relação entre o número de cartões amarelos ou vermelhos recebidos por um jogador e sua pontuação média?<br>

# COMMAND ----------

# MAGIC %md
# MAGIC No CartolaFC, os cartões amarelos e vermelhos são considerados scouts negativos, representando uma dedução de -1 ponto para os cartões amarelos e -3 pontos para os cartões vermelhos. Naturalmente, esses valores têm um impacto negativo na média de pontos dos jogadores. No entanto, é importante destacar que a simples presença de um alto número de cartões amarelos não significa necessariamente que um jogador seja uma má opção. 
# MAGIC
# MAGIC Para uma análise mais aprofundada, é crucial considerar os segmentos de jogadores, uma vez que esses scouts negativos têm um impacto muito mais significativo em jogadores de defesa, como os laterais. Vamos examinar o caso dos laterais.
# MAGIC
# MAGIC No gráfico a seguir, observamos que o jogador Juninho Capixaba apresenta a maior quantidade de cartões amarelos, porém possui a maior média de pontuação entre esses jogadores. Isso indica que, apesar de receber muitos cartões amarelos, Capixaba também consegue acumular muitos scouts positivos, o que compensa a perda de pontos decorrente dos cartões. O mesmo não pode ser dito dos jogadores Reinaldo e Saraiva, que, apesar de terem uma quantidade menor de cartões amarelos, receberam 1 cartão vermelho e não conseguiram alcançar pontuações suficientes para melhorar sua média.
# MAGIC
# MAGIC ![Questao5a](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Cart%C3%B5es%20Amarelos%20e%20Vermelhos.png?raw=true)<br>
# MAGIC
# MAGIC Além de considerar os cartões negativos, incluímos na análise scouts positivos, como gols, assistências e saldo de gols. Isso nos permite entender o que contribuiu para a média dos jogadores. Capixaba, por exemplo, possui 2 gols e 1 assistência, o que resulta em 21 pontos, enquanto Reinaldo possui 1 gol e 4 assistências, somando 28 pontos. Portanto, fica evidente que o diferencial não está nos gols e assistências, mas sim em manter o saldo de gols. Para jogadores de defesa, a capacidade de evitar sofrer gols é crucial para manter uma média elevada de pontuação.
# MAGIC
# MAGIC ![Questao5b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Cart%C3%B5es%20Amarelos%20e%20Vermelhos%20d.png?raw=true)<br>
# MAGIC
# MAGIC Essa análise detalhada dos scouts negativos e positivos nos permite compreender melhor como esses elementos afetam a performance dos jogadores no CartolaFC e como os jogadores de diferentes posições podem ser avaliados de forma mais precisa.

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Quais são os jogadores que se destacam em assistências, ou seja, aqueles que mais contribuíram para os gols de suas equipes?<br>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC No gráfico a seguir, podemos observar que cinco jogadores estão empatados como os líderes em assistências no campeonato:
# MAGIC
# MAGIC ![Questao6a](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%206a.png?raw=true)<br>
# MAGIC
# MAGIC No entanto, uma análise mais abrangente é recomendada, considerando tanto os gols quanto as assistências. Ao fazê-lo, percebemos que, embora Hulk seja o jogador com o maior número de assistências (cinco), ao levarmos em conta a pontuação de gols (sete), Tiquinho Soarez se destaca com apenas 4 assistências, porém uma impressionante marca de 13 gols no campeonato:
# MAGIC
# MAGIC ![Questao6b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%206c.png?raw=true)<br>
# MAGIC
# MAGIC Essa análise conjunta nos proporciona uma visão mais completa do desempenho desses jogadores, considerando tanto suas contribuições em assistências quanto em gols.

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Existe alguma correlação entre a posição do jogador (atacante, meio-campista, zagueiro, lateral e goleiro) e sua pontuação?<br>

# COMMAND ----------

# MAGIC %md
# MAGIC Sim, de fato, existe uma correlação significativa entre a pontuação dos jogadores e suas posições dentro de um time. Isso se deve em grande parte aos scouts de ataque, que geralmente concedem uma pontuação mais alta do que os scouts de defesa. Além disso, jogadores em posições defensivas, como zagueiros e laterais, têm uma tendência maior a acumular scouts negativos, devido à sua responsabilidade de impedir os avanços e os gols do time adversário.
# MAGIC
# MAGIC No gráfico abaixo, essa discrepância fica claramente evidenciada. Uma observação interessante é que as posições de meia e atacante apresentam pontuações bastante próximas, embora os atacantes obtenham seus pontos principalmente por meio de gols e assistências, enquanto os meio-campistas se destacam por sua capacidade de realizar desarmes eficazes.
# MAGIC
# MAGIC ![Questao7a](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%207b.png?raw=true)<br>
# MAGIC
# MAGIC Essa análise reforça a ideia de que a posição de um jogador desempenha um papel fundamental na sua pontuação no CartolaFC, e diferentes posições têm ênfases distintas em termos de scouts positivos e negativos. Compreender essa dinâmica é fundamental para a montagem de um time bem-sucedido no jogo.

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Quais são os times que mais pontuaram no CartolaFC e quais jogadores contribuíram significativamente para essas pontuações?<br>

# COMMAND ----------

# MAGIC %md
# MAGIC No gráfico a seguir, apresentamos a pontuação acumulada pelos clubes no CartolaFC até a rodada 23:No gráfico abaixo temos a pontuação que os clubes acumularam no CartolaFC até a rodada de número 23.
# MAGIC
# MAGIC ![Questao8a](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%208a.png?raw=true)<br>
# MAGIC
# MAGIC É crucial ressaltar que a pontuação do clube no CartolaFC não deve ser confundida com a pontuação que o mesmo obteve no Campeonato Brasileiro. Para evitar essa confusão, incluímos a posição de cada clube nos rótulos de dados, permitindo-nos verificar sua posição na tabela do Brasileirão. O gráfico a seguir ilustra essa relação, revelando que, por exemplo, o Grêmio ocupa a 3ª posição na tabela do campeonato, embora seus jogadores não tenham obtido tantos pontos no CartolaFC:
# MAGIC
# MAGIC ![Questao8b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%208b.png?raw=true)<br>
# MAGIC
# MAGIC Para analisar os jogadores que mais contribuíram para a pontuação de seus clubes, é possível selecionar o clube desejado e examinar o gráfico correspondente. Esse gráfico apresenta os maiores pontuadores do clube, juntamente com a quantidade de partidas que disputaram, como exemplificado abaixo:
# MAGIC
# MAGIC ![Questao8c](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%208c.png?raw=true)<br>
# MAGIC
# MAGIC Alternadamente, exibimos os cinco maiores pontuadores de cada clube, possibilitando uma análise mais detalhada e aprofundada das informações, conforme ilustrado no gráfico a seguir:
# MAGIC
# MAGIC ![Questao8d](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%208d.png?raw=true)<br>
# MAGIC
# MAGIC Dessa forma, podemos identificar de maneira mais precisa quais jogadores contribuíram significativamente para o desempenho de seus clubes no CartolaFC e como essa performance se relaciona com a posição dos clubes no Campeonato Brasileiro.

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Como o desempenho dos jogadores varia em jogos em casa versus jogos fora de casa?<br>

# COMMAND ----------

# MAGIC %md
# MAGIC Sim, de fato, existe uma relação notável entre os jogos disputados em casa e os jogos disputados como visitante em uma competição esportiva. Fatores como o apoio da torcida, o conhecimento do campo e até mesmo o cansaço decorrente de uma viagem desempenham um papel crucial na performance de um jogador quando ele entra em campo.
# MAGIC
# MAGIC No gráfico a seguir, apresentamos novamente os 10 maiores pontuadores do campeonato, desta vez dividindo suas médias de pontos em partidas em casa e partidas fora de casa. É notável que, dentro deste grupo seleto, apenas um jogador, Luan Cândido, que atua na posição defensiva, conquistou uma média de pontos superior fora de casa:
# MAGIC
# MAGIC ![Questao9b](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%209b.png?raw=true)<br>
# MAGIC
# MAGIC É interessante ampliar essa análise e observar o desempenho por clube. Notamos que, entre os 20 times que participam do campeonato, apenas o Palmeiras e o Cruzeiro possuem pontuações médias superiores em jogos disputados fora de casa, como ilustrado no gráfico a seguir:
# MAGIC
# MAGIC ![Questao9a](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Gr%C3%A1fico%20quest%C3%A3o%209a.png?raw=true)<br>
# MAGIC
# MAGIC Essa análise evidencia a influência substancial do ambiente, das condições e da dinâmica de um jogo em casa e fora de casa no desempenho dos jogadores e das equipes. Compreender essas variações é essencial para avaliar a estratégia de um time e os fatores que podem afetar seu desempenho em diferentes cenários de jogo.

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Atualização dos Dados

# COMMAND ----------

# MAGIC %md
# MAGIC Além das análises e consultas realizadas até o momento, implementamos uma solução para a atualização automatizada dos dados, garantindo que nosso banco de dados esteja sempre sincronizado com as informações mais recentes. Para isso, criamos um notebook adicional denominado "Atualização Rodadas API_Cartola".
# MAGIC
# MAGIC Esse notebook faz parte de um processo maior conhecido como workflow no Databricks. Um workflow é uma sequência de tarefas e operações que são executadas de forma automatizada e ordenada para alcançar um objetivo específico. No nosso caso, o objetivo é atualizar regularmente os dados do CartolaFC.
# MAGIC
# MAGIC A atualização dos dados é realizada diariamente por meio do agendamento de uma tarefa. Esse agendamento garante que, a cada dia, os dados mais recentes sejam automaticamente baixados da API do CartolaFC e inseridos na nossa base de dados. Isso proporciona uma fonte de dados sempre atualizada para as análises contínuas que realizamos.
# MAGIC
# MAGIC Esse processo de atualização automatizada é fundamental para garantir que nossas análises e relatórios reflitam com precisão as informações mais recentes do CartolaFC, permitindo tomadas de decisão baseadas em dados atualizados e relevantes.

# COMMAND ----------

# MAGIC %md
# MAGIC Na imagem abaixo, podemos visualizar a etapa em que o fluxo de trabalho (workflow) é criado para executar o notebook dedicado à atualização das rodadas do CartolaFC.
# MAGIC
# MAGIC ![Fluxo](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Cria%C3%A7%C3%A3o%20do%20Fluxo%20de%20Atualiza%C3%A7%C3%A3o.png?raw=true)<br>
# MAGIC
# MAGIC Nesta etapa, estamos configurando a tarefa para executar o fluxo diariamente.
# MAGIC
# MAGIC <img src="https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Agendando%20o%20Fluxo%20de%20Atualiza%C3%A7%C3%A3o.png?raw=true" alt="Fluxo" width="600" height="450"><br>
# MAGIC
# MAGIC Iniciamos realizando um teste com a execução manual do fluxo. Na imagem abaixo, é possível verificar que a execução foi concluída com êxito.
# MAGIC
# MAGIC ![Fluxo](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Execu%C3%A7%C3%A3o%20Fluxo%20de%20Atualiza%C3%A7%C3%A3o%20Bem%20Sucedida.png?raw=true)<br>
# MAGIC <br>
# MAGIC
# MAGIC Após esse teste, fizemos a alteração na tarefa para que ela fosse executada em um intervalo de 5 minutos e iniciamos o monitoramento da execução do workflow.
# MAGIC
# MAGIC ![Fluxo](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Fluxo%20Agendado.png?raw=true)<br>
# MAGIC
# MAGIC Na imagem abaixo, podemos verificar a data e a hora da execução, a qual foi realizada conforme o agendamento previamente configurado. A execução teve uma duração total de 1 minuto e 10 segundos, e, o mais importante, foi concluída com sucesso.
# MAGIC
# MAGIC ![Fluxo](https://github.com/AdrianaLegalReis/PUC_SPRINT_III/blob/main/Execu%C3%A7%C3%A3o%20Bem%20Sucedida%20pelo%20Agendador.png?raw=true)<br>
# MAGIC
# MAGIC O processo de configuração do workflow para atualização automática dos dados do Cartola FC foi concluído com êxito. A tarefa de atualização programada está operando de acordo com o planejado, garantindo que nossas informações permaneçam atualizadas diariamente. Esse fluxo automatizado economiza tempo e esforços, garantindo que os dados estejam sempre prontos para análises e relatórios atualizados. Com essa implementação bem-sucedida, nossa equipe pode concentrar-se nas análises e insights, aproveitando ao máximo os dados do Cartola FC.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##7. Conclusão
# MAGIC
# MAGIC Ao longo deste projeto, exploramos minuciosamente a vasta quantidade de dados fornecidos pelo CartolaFC para compreender as nuances do desempenho dos jogadores e equipes no mundo do futebol fantasy. Desde a carga inicial dos dados até a análise complexa, cada etapa foi essencial para extrair insights valiosos.
# MAGIC
# MAGIC A integração de fontes de dados e a padronização dos dados foram etapas fundamentais para a qualidade da análise. Neste aspecto, o uso da plataforma Databricks foi fundamental, proporcionando uma infraestrutura ágil e escalável para lidar com grandes volumes de dados, integrar diversas fontes e executar transformações complexas.
# MAGIC
# MAGIC Exploramos uma variedade de métricas e gráficos para compreender melhor os fatores que influenciam a pontuação dos jogadores e equipes no jogo. Durante essa jornada, identificamos tendências e relações interessantes que nos forneceram conclusões valiosas sobre como as variáveis afetam o desempenho dos participantes no campeonato.
# MAGIC
# MAGIC Observamos que a posição de um jogador tem um impacto significativo em sua pontuação, com jogadores de ataque obtendo pontuações mais elevadas, impulsionadas por gols e assistências, enquanto jogadores de defesa enfrentam o desafio de equilibrar essas métricas com os cartões negativos. Além disso, constatamos que o desempenho em casa versus fora de casa desempenha um papel crucial, com fatores como o apoio da torcida, o conhecimento do campo e a fadiga de viagem influenciando significativamente o desempenho dos jogadores e equipes.
# MAGIC
# MAGIC Além disso, exploramos diferentes perspectivas, como a análise de jogadores individuais, a comparação entre posições e a avaliação do desempenho dos clubes. Essa abordagem multifacetada nos permitiu entender melhor os elementos que contribuem para o sucesso no CartolaFC.
# MAGIC
# MAGIC Em suma, este projeto demonstrou a importância da análise de dados no contexto do CartolaFC. A utilização do Databricks como ferramenta de análise enriqueceu a eficiência, destacando como a tecnologia avançada pode potencializar a análise de dados tradicional em projetos desafiadores como este. O resultado é uma análise informada e esclarecedora que pode beneficiar entusiastas e participantes do futebol fantasy, elevando a experiência do CartolaFC a um novo patamar.
