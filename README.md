# e-commercy-database-design

Feito até agora:

#####
1) Design do banco de dados 3NF OLTP (Schema) usando o draw.io
2) Implementação dos comandos SQL de criação das tabelas
3) Implementação dos comandos de criação das tabelas usando a ORM SQLAlchemy+psycopg2

---
####
Subir o airflow 
docker-compose up airflow-init
docker-compose up -d


###
Instalar as libs extras no airflow: Dockerfile
Criar banco de dados Ecommerce_OLTP:
Passos para Criar e Usar o Banco Ecommerce_OLTP
1. Criar o Banco no PostgreSQL
Você já tem o contêiner postgres rodando no seu docker-compose.yaml. Vamos criar o banco Ecommerce_OLTP dentro dele:

depois de rodar a dag, para conferir se as tabelas foram criadas:

docker exec -it <nome_do_container_postgres> psql -U oltp -d ecommerce_oltp
\dt

devo ver:

Schema |        Name        | Type  |     Owner      
--------+--------------------+-------+----------------
 public | addresses          | table | ecommerce_oltp
 public | categories         | table | ecommerce_oltp
 public | customer_addresses | table | ecommerce_oltp
 public | customers          | table | ecommerce_oltp
 public | inventory          | table | ecommerce_oltp
 public | item               | table | ecommerce_oltp
 public | phones             | table | ecommerce_oltp
 public | phones_customers   | table | ecommerce_oltp
 public | phones_store       | table | ecommerce_oltp
 public | price              | table | ecommerce_oltp
 public | purchase           | table | ecommerce_oltp
 public | purchase_status    | table | ecommerce_oltp
 public | sizes              | table | ecommerce_oltp
 public | store_address      | table | ecommerce_oltp
 public | stores             | table | ecommerce_oltp
(15 rows)
####

so cliente br 

####

Planejando a População do Banco
Para manter a integridade do banco, você deve popular as tabelas na ordem correta, respeitando as dependências de chaves estrangeiras. Com base no seu diagrama, a ordem ideal seria:

Tabelas sem FKs (ou com FKs opcionais):
categories
sizes
stores
phones
addresses

Tabelas com FKs:
item (depende de categories)
price (depende de item e sizes)
inventory (depende de item, sizes e stores)
store_address (depende de stores e addresses)
phones_store (depende de phones e stores)

Tabelas com dependências mais complexas:
customers
customer_addresses (depende de customers e addresses)
phones_customers (depende de phones e customers)
purchase (depende de customers, item, sizes e stores)
purchase_status (depende de purchase)

#### 

Sequencia dos containers para subir o sistema:
1) create_ecommerce_tables
2) init_ecommerce
3) register_purchases_and_customers



#####
Planejamento olap:
- quais perguntas (KPIs) serão respondidas? 
2. Identificando KPIs Relevantes
KPIs (Key Performance Indicators) são métricas que refletem o desempenho do negócio. Como seu projeto simula um e-commerce/varejo, aqui estão alguns KPIs relevantes que podem ser calculados com base no seu modelo OLAP:

a) KPIs de Vendas
Vendas Totais:
Soma de total_value em Fact_Sales.
Exemplo: "Qual o total de vendas por loja ou categoria?"
Número de Transações:
Contagem de purchase_id em Fact_Sales.
Exemplo: "Quantas compras foram feitas por mês?"
Valor Médio do Pedido (AOV):
Vendas Totais / Número de Transações.
Exemplo: "Qual o valor médio das compras por cliente?"
b) KPIs de Clientes
Taxa de Retenção de Clientes:
Percentual de customer_id com mais de uma compra em Fact_Sales.
Exemplo: "Quantos clientes retornam para comprar novamente?"
Novos Clientes Adquiridos:
Contagem de novos customer_id em um período, baseada em created_at.
Exemplo: "Quantos novos clientes por trimestre?"
c) KPIs de Inventário
Níveis de Estoque:
Média de quantity em Fact_Inventory por loja ou item.
Exemplo: "Qual o estoque médio por loja?"
Taxa de Rotatividade de Estoque:
Quantidade vendida (de Fact_Sales) / Quantidade média em estoque (de Fact_Inventory).
Exemplo: "Com que velocidade os itens estão sendo vendidos?"
d) KPIs Geográficos
Vendas por Região:
Soma de total_value agrupada por state ou city em Dim_Stores.
Exemplo: "Quais estados geram mais vendas?"
Esses KPIs são exemplos iniciais. Você pode ajustá-los com base no foco do seu portfólio (ex.: vendas para marketing, estoque para operações).

####
Criar banco OLAP:
depois de rodar a dag, para conferir se as tabelas foram criadas:

docker exec -it <nome_do_container_postgres> psql -U olap -d ecommerce_olap
\dt
List of relations
 Schema |      Name      | Type  | Owner 
--------+----------------+-------+-------
 public | dim_customers  | table | olap
 public | dim_items      | table | olap
 public | dim_sizes      | table | olap
 public | dim_stores     | table | olap
 public | dim_time       | table | olap
 public | fact_inventory | table | olap
 public | fact_sales     | table | olap
(7 rows)

-> EXPLICAR DAG ORQUESTRAÇÃO 

 etl para popular o olap


Ja tem no draw io:
    Adicionar purchase status na OLAP com as constraints 

Não tem no draw io:
    adicionar "region" na dim_stores
    adicionar "price_range" em dim_items
    adicionar em dim_time: 
        is_weekend (BOOLEAN NOT NULL): Para identificar se o dia é fim de semana.


 Ja fiz as alterações no esquema (e baixei) mas não ainda no codigo: 
    inclusao da tabela item_sizes [x]
    mudança em size VARCHAR(10) [x]
    Substitua FLOAT por DECIMAL no campo value de prices para garantir precisão em cálculos financeiros. [x]
    Considere aumentar o limite de quantity em inventory (ex.: usar BIGINT) para suportar [x]
    
    Adicione constraints (ex.: CHECK para validar formatos de tamanho) e índices nas colunas usadas em joins [x] frequentes para melhorar o desempenho.
    
    Na hora de inicializar:
    Transforma tudo em função e importa isso para limpar o arquivo

    sizes
    categories

    Como lidar com decimal? []
    Email lojas
    complemento -> ta um lixo

    Na hora de popular:
    Extra:
        -Emails 
        -Complementos ( ta randonzao)

        Por que adicionar um index faz o sql rodar mais rapido?