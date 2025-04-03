# e-commercy-database-design

Feito até agora:

#####
1) Design do banco de dados 3NF OLTP (Schema) usando o draw.io
2) Implementação dos comandos SQL de criação das tabelas
3) Implementação dos comandos de criação das tabelas usando a ORM SQLAlchemy+psycopg2

---
####
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
(Opcional) Crie um usuário específico para maior segurança e realismo:
sql

CREATE USER ecommerce_oltp WITH PASSWORD 'ecommerce123';
GRANT ALL PRIVILEGES ON DATABASE "Ecommerce_OLTP" TO ecommerce_oltp ;
Saia do psql com \q.

---

depois de rodar a dag, para conferir se as tabelas foram criadas:

docker exec -it <nome_do_container_postgres> psql -U airflow -d Ecommerce_OLTP
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