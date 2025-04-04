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

Acesse o contêiner PostgreSQL:
bash

docker exec -it <nome_do_container_postgres> psql -U airflow
(Substitua <nome_do_container_postgres> pelo nome real, geralmente algo como seu_projeto_postgres_1. Use docker ps para verificar.)

CREATE DATABASE "Ecommerce_OLTP";

CREATE USER ecommerce_oltp WITH PASSWORD 'ecommerce123';
GRANT ALL PRIVILEGES ON DATABASE "Ecommerce_OLTP" TO ecommerce_oltp ;
Saia do psql com \q.

---

depois de rodar a dag, para conferir se as tabelas foram criadas:

docker exec -it <nome_do_container_postgres> psql -U ecommerce_oltp -d Ecommerce_OLTP
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



ALTERAR TABELA ITEM PARA ITENS!
PRICE PRICES
store_address Stores_addresses
Phones_store Phones_stores
Customer_addresses  Customers_addresses 