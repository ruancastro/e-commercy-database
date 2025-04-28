# E-commerce Database ETL Pipeline

## Overview

This project is a demonstration of my skills as a **Data Engineer**, developed as part of my portfolio to showcase my expertise in building end-to-end ETL (Extract, Transform, Load) pipelines. The goal was to design and implement a data pipeline for a fictional Brazilian e-commerce platform, focusing on managing inventory, sales, and customer data. The pipeline includes both an **OLTP (Online Transaction Processing)** database for operational data and an **OLAP (Online Analytical Processing)** database for analytical reporting, with initial and incremental data loads orchestrated using **Apache Airflow**.

The project covers the full data engineering lifecycle: from schema design to data generation, database creation, and automated ETL processes. It demonstrates my ability to work with relational databases, orchestrate workflows, handle data transformations, and ensure scalability and maintainability in a production-like environment.

---

## Project Structure

The project is organized into the following directories:

- **/config**: Configuration files (e.g., database credentials, environment variables).
- **/dags**: Apache Airflow DAGs for orchestrating the ETL pipeline.
  - `dag_create_ecommerce_tables_oltp.py`: Creates the OLTP database tables.
  - `dag_create_ecommerce_tables_olap.py`: Creates the OLAP database tables.
  - `dag_initial_load_oltp.py`: Populates the OLTP database with initial data.
  - `dag_initial_load_olap.py`: Performs the initial load of the OLAP database.
  - `dag_incremental_load_olap.py`: Performs incremental loads into the OLAP database.
  - `dag_register_purchases_and_clients.py`: Generates new purchases and customer data for testing incremental loads.
  - **/utils**: Utility scripts for the ETL pipeline.
    - `etl.py`: Contains the ETL logic for initial and incremental loads.
    - `brazilian_address_complement.py`: Helper script to generate Brazilian address data.
    - `customer_email.py`: Helper script to generate customer email addresses.
    - `ecommerce_starter.py`: Script to generate initial e-commerce data (items, categories, stores).
    - `phone_utils.py`: Helper script to generate phone numbers.
    - `store_email.py`: Helper script to generate store email addresses.
- **/data**: Data files used for initial population.
  - `items_and_categories.csv`: Initial dataset with items and categories for the OLTP database.
- **/logs**: Directory to store logs (not shown in the pipeline but recommended for production).
- **/plugins**: Custom Airflow plugins (if any, currently empty).
- **/env**: Environment configuration files.
  - `.gitignore`: Excludes sensitive files from version control.
  - `docker-compose.yaml`: Docker configuration to run Airflow and PostgreSQL databases.
  - `Dockerfile`: Dockerfile to set up the Airflow environment.
- **E_commerce_dbs-OLAP.png**: OLAP schema diagram created with Draw.io.
- **E_commerce_dbs-OLTP.png**: OLTP schema diagram created with Draw.io.
- **README.md**: This file.
- **requirements.txt**: Python dependencies for the project.

---

## Key Concepts and Technologies Used

### 1. Schema Design with Draw.io

To design the database schemas for both OLTP and OLAP databases, I used **Draw.io**, a free diagramming tool. This allowed me to:

- **Visualize Relationships**: Clearly define the relationships between tables (e.g., foreign keys, one-to-many relationships).
- **Plan for Scalability**: Ensure the schemas were normalized (OLTP) for transactional efficiency and denormalized (OLAP) for analytical queries.
- **Iterate Quickly**: Make adjustments to the schema design before implementation.

The resulting diagrams are:

- **OLTP Schema**:

  ![OLTP Schema](E_commerce_dbs-OLTP.png)

- **OLAP Schema**:

  ![OLAP Schema](E_commerce_dbs-OLAP.png)

**Why Draw.io**? I chose Draw.io because it is lightweight, free, and integrates well with documentation workflows. It allowed me to share my schema designs with stakeholders (or in this case, as part of my portfolio) in a clear and professional manner.

### 2. Database Creation with SQLAlchemy

I used **SQLAlchemy**, a Python ORM (Object-Relational Mapping) library, to define and create the database tables for both OLTP and OLAP databases. This was done in the following DAGs:

- `dag_create_ecommerce_tables_oltp.py`: Defines the OLTP schema with tables like `customers`, `purchases`, `items`, `stores`, `sizes`, and their relationships.
- `dag_create_ecommerce_tables_olap.py`: Defines the OLAP schema with a star schema structure, including dimension and fact tables.

**Why SQLAlchemy?**

- **Code Reusability**: SQLAlchemy allows me to define schemas as Python classes, making them reusable across different DAGs (e.g., for initial and incremental loads).
- **Constraints and Validation**: I could easily add constraints (e.g., `CheckConstraint` for valid regions, foreign keys) to ensure data integrity.
- **Portability**: SQLAlchemy abstracts the database layer, making it easier to switch between database engines (e.g., from PostgreSQL to MySQL) if needed.

### 3. Initial Data Population (OLTP) from CSV

The OLTP database was initially populated using data from `items_and_categories.csv`, which contains a list of items and their categories. Additionally, I created helper scripts to generate realistic Brazilian e-commerce data:

- `ecommerce_starter.py`: Generates initial data for items, categories, and stores.
- `brazilian_address_complement.py`: Generates Brazilian addresses with proper formatting (e.g., city, state, zip code).
- `customer_email.py` and `store_email.py`: Generate realistic email addresses for customers and stores.
- `phone_utils.py`: Generates phone numbers in Brazilian format.

The population was handled by the DAG `dag_initial_load_oltp.py`, which:

1. Reads the CSV file.
2. Uses the helper scripts to generate additional data (e.g., addresses, emails).
3. Inserts the data into the OLTP database using SQLAlchemy.

**Why CSV and Helper Scripts?**

- **CSV for Simplicity**: The CSV file provides a simple way to bootstrap the database with initial data, mimicking a real-world scenario where data might come from an external source.
- **Helper Scripts for Realism**: Generating realistic Brazilian data (e.g., addresses, emails) ensures the project reflects a real-world e-commerce scenario, making it more relevant for my portfolio.

### 4. Random Data Generation for Clients and Purchases

To simulate ongoing e-commerce activity, I created the DAG `dag_register_purchases_and_clients.py`. This DAG:

- Generates new customers with random names, emails, addresses, and phone numbers.
- Creates random purchases for these customers, including items, quantities, and purchase statuses (e.g., "Pending", "Delivered").
- Uses Python's `random` module to introduce variability in the data (e.g., random item selection, purchase dates).

**Why Random Data?**

- **Testing Incremental Loads**: Randomly generated data allows me to test the incremental load process by simulating new transactions over time.
- **Realism**: Randomness mimics the unpredictable nature of e-commerce transactions, making the pipeline more realistic.

### 5. OLAP Database Creation

The OLAP database was created using a star schema, which is ideal for analytical queries. The schema includes:

- **Dimension Tables**: `dim_customers`, `dim_items`, `dim_sizes`, `dim_stores`, `dim_time`.
- **Fact Tables**: `fact_sales` (for sales data), `fact_inventory` (for inventory data).

The creation was handled by the DAG `dag_create_ecommerce_tables_olap.py`, which uses SQLAlchemy to define the schema with appropriate constraints (e.g., `CheckConstraint` for regions, foreign keys).

**Why a Star Schema?**

- **Analytical Performance**: A star schema simplifies analytical queries (e.g., aggregations, joins) and is optimized for tools like BI dashboards.
- **Scalability**: Separating dimensions and facts allows for efficient querying and easier schema evolution.

### 6. Initial and Incremental ETL Loads with Airflow

The ETL pipeline was implemented using **Apache Airflow**, with two main DAGs for loading data into the OLAP database:

- `dag_initial_load_olap.py`: Performs the initial load of the OLAP database by extracting all data from the OLTP database, transforming it, and loading it into the OLAP database.
- `dag_incremental_load_olap.py`: Performs incremental loads by extracting only new or updated data (based on the `created_at` timestamp) since the last execution, transforming it, and loading it into the OLAP database.

The ETL logic is encapsulated in the `etl.py` script, which defines two classes:

- `ETLInitial`: Handles the initial load by dropping and recreating tables, then loading all data.
- `ETLIncremental`: Handles incremental loads by filtering new data and appending it to existing tables, using `ON CONFLICT DO NOTHING` to avoid duplicates.

**Why Airflow?**

- **Orchestration**: Airflow allows me to schedule and manage the ETL pipeline, ensuring tasks run in the correct order (e.g., extract → transform → load).
- **Monitoring**: Airflow's UI provides visibility into task execution, logs, and failures, which is critical for production pipelines.
- **Scalability**: Airflow can scale to handle more complex workflows as the project grows.

### 7. XCom for Task Communication

In the Airflow DAGs (`dag_initial_load_olap.py` and `dag_incremental_load_olap.py`), I used **XCom** (Cross-Communication) to pass data between tasks:

- The `extract_task` extracts data and pushes it to XCom with a key (e.g., `extracted_data`).
- The `transform_task` pulls the extracted data from XCom, transforms it, and pushes the result with another key (e.g., `transformed_data`).
- The `load_task` pulls the transformed data from XCom and loads it into the OLAP database.

**Why XCom?**

- **Simplicity**: XCom eliminates the need for external storage (e.g., files, databases) to pass data between tasks, simplifying the pipeline.
- **Efficiency**: It allows tasks to communicate directly within the Airflow environment, reducing overhead.

**Nuance**: XCom is not suitable for very large datasets because it stores data in the Airflow metadata database. In a production environment, I would adjust the pipeline to save large datasets to an external location (e.g., S3) and pass the file path via XCom.

### 8. Data Transformations

During the ETL process, several transformations were applied:

- **Normalization to Denormalization**: The OLTP database is normalized (e.g., separate tables for addresses, phones). In the OLAP database, I denormalized the data into a star schema for faster analytical queries.
- **Region Mapping**: Brazilian states were mapped to regions (e.g., "SP" → "Sudeste") using a dictionary in the `etl.py` script. I adjusted the regions to Portuguese (e.g., "Northeast" → "Nordeste") to reflect the Brazilian context.
- **Time Dimension**: Created a `dim_time` table with attributes like day, month, quarter, and year to enable time-based analysis.
- **Handling Null Values**: Adjusted the transformation logic to handle `NaN` values in nullable columns (e.g., `customer_id` in `fact_sales`), converting them to `None` for proper insertion into the OLAP database.

**Why These Transformations?**

- **Analytical Readiness**: The transformations ensure the OLAP database is optimized for reporting (e.g., aggregations by region, time).
- **Cultural Relevance**: Using Portuguese region names aligns  with the Brazilian context, making the data more meaningful for local stakeholders.
- **Data Integrity**: Handling `NaN` values ensures compatibility with the database schema and prevents insertion errors.

### 9. Error Handling and Debugging

Throughout the project, I encountered and resolved several issues:

- **String Length Mismatch**: Fixed an error where the `size` column in the OLAP database had a length constraint (`VARCHAR(4)`) that was too small for values like "Pequeno". I increased it to `VARCHAR(10)`.
- **Region Naming Inconsistency**: Corrected a mismatch between the region names in the transformation logic ("North East") and the schema constraint ("Northeast"), later adjusting to Portuguese ("Nordeste").
- **Numeric Value Out of Range Error**: Encountered a `psycopg2.errors.NumericValueOutOfRange` error when loading data into `fact_sales`. The issue was caused by `NaN` values in the `customer_id` column (a nullable `INTEGER` field), which PostgreSQL couldn't handle. I resolved this by converting `NaN` to `None` before insertion and rewrote the `load_facts` method to use SQLAlchemy's `Session` for better handling of nullable fields.
- **Timezone Mismatch in Incremental Loads**: Faced issues with the incremental load DAG (`dag_incremental_load_olap.py`) failing due to a timezone mismatch between the Airflow DAG (configured in `America/Sao_Paulo`) and the OLTP database (in `UTC`). This caused the `created_at` comparisons to fail, resulting in no new records being extracted. I fixed this by explicitly converting the `last_execution_date` to UTC before querying the OLTP database, ensuring accurate incremental data extraction.
- **Logging Enhancements**: Added detailed logs in the `extract` and `load_facts` methods to track the number of new records extracted and loaded, as well as their `created_at` timestamps, improving visibility into the pipeline's behavior.

**Why This Matters?**

- **Reliability**: Proper error handling ensures the pipeline is robust and can recover from failures.
- **Debugging Skills**: Highlighting these fixes in my portfolio shows my ability to troubleshoot complex issues, such as timezone mismatches and database errors, and implement effective solutions.

### 10. Deployment with Docker

The project is containerized using **Docker** and **Docker Compose**:

- `Dockerfile`: Defines the Airflow environment with necessary dependencies (e.g., SQLAlchemy, psycopg2).
- `docker-compose.yaml`: Sets up services for Airflow (webserver, scheduler, worker) and PostgreSQL databases for OLTP and OLAP.

**Why Docker?**

- **Reproducibility**: Docker ensures the project can run on any machine with the same environment.
- **Portability**: It makes the project easy to share and deploy, which is a key skill for a data engineer.

---

## How to Run the Project

1. **Prerequisites**:

   - Docker and Docker Compose installed.
   - Python 3.12+ (if running without Docker).
   - PostgreSQL (if running without Docker).
   - Airflow (if running without Docker).

2. **Setup**:

   - Clone the repository:

     ```
     git clone <repository_url>
     cd ECOMMERCY-DATABASE
     ```

   - Install dependencies (if not using Docker):

     ```
     pip install -r requirements.txt
     ```

   - Update the database credentials in `OLTP_URL` and `OLAP_URL` in the DAG files (e.g., `dag_initial_load_olap.py`).

3. **Run with Docker**:

   - Start the services:

     ```
     docker-compose up -d
     ```

   - Access the Airflow UI at `http://localhost:8080` (default credentials: airflow/airflow).

4. **Execute the DAGs**:

   - In the Airflow UI, enable and trigger the following DAGs in order:
     1. `create_ecommerce_tables_oltp`: Creates the OLTP tables.
     2. `initial_load_oltp`: Populates the OLTP database.
     3. `create_ecommerce_tables_olap`: Creates the OLAP tables.
     4. `register_purchases_and_clients`: Generates new purchases and clients for testing.
     5. `initial_load_olap`: Performs the initial load of the OLAP database.
     6. `incremental_load_olap`: Performs incremental loads into the OLAP database.

5. **Monitor Execution**:

   - Check the Airflow UI for task logs and status.
   - Verify the data in the OLTP and OLAP databases using a PostgreSQL client (e.g., pgAdmin). To confirm incremental loads, query the `fact_sales` table with the `created_at` column:

     ```
     SELECT purchase_id, created_at
     FROM fact_sales
     WHERE created_at > 'YYYY-MM-DD HH:MM:SS'
     ORDER BY created_at DESC;
     ```

---

## Lessons Learned

- **Schema Design**: Using Draw.io helped me plan the database structure effectively, ensuring the OLTP and OLAP databases met their respective goals (transactional vs. analytical).
- **ETL Orchestration**: Airflow's DAGs and XCom made it easy to manage the ETL pipeline, but I learned to be cautious with XCom for large datasets.
- **Data Generation**: Generating realistic Brazilian data (e.g., addresses, regions in Portuguese) added authenticity to the project.
- **Debugging Complex Issues**: Resolving timezone mismatches between Airflow and the OLTP database, handling `NaN` values in nullable columns, and adjusting the incremental load logic to be more robust significantly improved my problem-solving skills.
- **Importance of Logging**: Adding detailed logs at each stage of the pipeline (e.g., in `extract` and `load_facts`) was crucial for debugging and verifying the correct behavior of incremental loads.
- **Timezone Handling**: Learned the importance of aligning timestamps across systems, especially when working with databases and orchestration tools in different timezones.

---

## Future Improvements

- **Optimize XCom Usage**: For larger datasets, save intermediate data to an external storage (e.g., S3) and pass file paths via XCom.
- **Add Data Quality Checks**: Implement validation tasks to ensure data consistency before loading into the OLAP database (e.g., checking for missing foreign keys).
- **Expand Analytical Capabilities**: Integrate the OLAP database with a BI tool (e.g., Power BI, Tableau) to create dashboards for sales and inventory analysis.
- **Automate Data Generation**: Schedule the `register_purchases_and_clients` DAG to run periodically, simulating continuous e-commerce activity.
- **Monitoring and Alerts**: Add monitoring tasks to the DAG to alert on failures or unexpected behavior (e.g., no new records for an extended period).

---

## Contact

Feel free to reach out to me for questions or collaboration opportunities:

- **LinkedIn**: [Ruan Castro](https://www.linkedin.com/in/ruan-castro-900262183/)

---

# Banco de Dados de E-commerce - Pipeline ETL

## Visão Geral

Este projeto é uma demonstração das minhas habilidades como **Engenheiro de Dados**, desenvolvido como parte do meu portfólio para exibir minha experiência na construção de pipelines ETL (Extract, Transform, Load) completos. O objetivo foi projetar e implementar um pipeline de dados para uma plataforma fictícia de e-commerce brasileira, focada em gerenciar dados de estoque, vendas e clientes. O pipeline inclui um banco de dados **OLTP (Online Transaction Processing)** para dados operacionais e um banco de dados **OLAP (Online Analytical Processing)** para relatórios analíticos, com cargas inicial e incremental orquestradas usando o **Apache Airflow**.

O projeto abrange todo o ciclo de vida da engenharia de dados: desde o design do esquema até a geração de dados, criação de bancos de dados e processos ETL automatizados. Ele demonstra minha capacidade de trabalhar com bancos de dados relacionais, orquestrar fluxos de trabalho, lidar com transformações de dados e garantir escalabilidade e manutenção em um ambiente semelhante ao de produção.

---

## Estrutura do Projeto

O projeto está organizado nas seguintes pastas:

- **/config**: Arquivos de configuração (ex.: credenciais de banco de dados, variáveis de ambiente).
- **/dags**: DAGs do Apache Airflow para orquestrar o pipeline ETL.
  - `dag_create_ecommerce_tables_oltp.py`: Cria as tabelas do banco OLTP.
  - `dag_create_ecommerce_tables_olap.py`: Cria as tabelas do banco OLAP.
  - `dag_initial_load_oltp.py`: Popula o banco OLTP com dados iniciais.
  - `dag_initial_load_olap.py`: Realiza a carga inicial do banco OLAP.
  - `dag_incremental_load_olap.py`: Realiza cargas incrementais no banco OLAP.
  - `dag_register_purchases_and_clients.py`: Gera novos dados de compras e clientes para testar cargas incrementais.
  - **/utils**: Scripts auxiliares para o pipeline ETL.
    - `etl.py`: Contém a lógica ETL para cargas inicial e incremental.
    - `brazilian_address_complement.py`: Script auxiliar para gerar endereços brasileiros.
    - `customer_email.py`: Script auxiliar para gerar endereços de e-mail de clientes.
    - `ecommerce_starter.py`: Script para gerar dados iniciais de e-commerce (itens, categorias, lojas).
    - `phone_utils.py`: Script auxiliar para gerar números de telefone.
    - `store_email.py`: Script auxiliar para gerar endereços de e-mail de lojas.
- **/data**: Arquivos de dados usados para a população inicial.
  - `items_and_categories.csv`: Conjunto de dados inicial com itens e categorias para o banco OLTP.
- **/logs**: Diretório para armazenar logs (não mostrado no pipeline, mas recomendado para produção).
- **/plugins**: Plugins personalizados do Airflow (se houver, atualmente vazio).
- **/env**: Arquivos de configuração de ambiente.
  - `.gitignore`: Exclui arquivos sensíveis do controle de versão.
  - `docker-compose.yaml`: Configuração do Docker para executar o Airflow e os bancos PostgreSQL.
  - `Dockerfile`: Dockerfile para configurar o ambiente do Airflow.
- **E_commerce_dbs-OLAP.png**: Diagrama do esquema OLAP criado com Draw.io.
- **E_commerce_dbs-OLTP.png**: Diagrama do esquema OLTP criado com Draw.io.
- **README.md**: Este arquivo.
- **requirements.txt**: Dependências Python do projeto.

---

## Conceitos e Tecnologias Utilizadas

### 1. Design de Esquema com Draw.io

Para projetar os esquemas de banco de dados OLTP e OLAP, utilizei o **Draw.io**, uma ferramenta gratuita de diagramação. Isso me permitiu:

- **Visualizar Relacionamentos**: Definir claramente os relacionamentos entre tabelas (ex.: chaves estrangeiras, relações um-para-muitos).
- **Planejar para Escala**: Garantir que os esquemas fossem normalizados (OLTP) para eficiência transacional e desnormalizados (OLAP) para consultas analíticas.
- **Iterar Rapidamente**: Fazer ajustes no design do esquema antes da implementação.

Os diagramas resultantes são:

- **Esquema OLTP**:

  ![Esquema OLTP](E_commerce_dbs-OLTP.png)

- **Esquema OLAP**:

  ![Esquema OLAP](E_commerce_dbs-OLAP.png)

**Por que o Draw.io**? Escolhi o Draw.io porque é leve, gratuito e se integra bem a fluxos de documentação. Ele me permitiu compartilhar os designs de esquema de forma clara e profissional, o que é útil para o meu portfólio.

### 2. Criação de Banco de Dados com SQLAlchemy

Utilizei o **SQLAlchemy**, uma biblioteca ORM (Object-Relational Mapping) do Python, para definir e criar as tabelas de banco de dados para os bancos OLTP e OLAP. Isso foi feito nas seguintes DAGs:

- `dag_create_ecommerce_tables_oltp.py`: Define o esquema OLTP com tabelas como `customers`, `purchases`, `items`, `stores`, `sizes` e seus relacionamentos.
- `dag_create_ecommerce_tables_olap.py`: Define o esquema OLAP com uma estrutura de esquema em estrela, incluindo tabelas de dimensão e fatos.

**Por que o SQLAlchemy?**

- **Reutilização de Código**: O SQLAlchemy me permite definir esquemas como classes Python, tornando-os reutilizáveis em diferentes DAGs (ex.: para cargas inicial e incremental).
- **Restrições e Validação**: Pude adicionar facilmente restrições (ex.: `CheckConstraint` para regiões válidas, chaves estrangeiras) para garantir a integridade dos dados.
- **Portabilidade**: O SQLAlchemy abstrai a camada de banco de dados, facilitando a troca de motores de banco (ex.: de PostgreSQL para MySQL), se necessário.

### 3. População Inicial (OLTP) a partir de CSV

O banco OLTP foi inicialmente populado usando dados do arquivo `items_and_categories.csv`, que contém uma lista de itens e suas categorias. Além disso, criei scripts auxiliares para gerar dados realistas de e-commerce brasileiro:

- `ecommerce_starter.py`: Gera dados iniciais para itens, categorias e lojas.
- `brazilian_address_complement.py`: Gera endereços brasileiros com formatação adequada (ex.: cidade, estado, CEP).
- `customer_email.py` e `store_email.py`: Geram endereços de e-mail realistas para clientes e lojas.
- `phone_utils.py`: Gera números de telefone no formato brasileiro.

A população foi gerenciada pela DAG `dag_initial_load_oltp.py`, que:

1. Lê o arquivo CSV.
2. Usa os scripts auxiliares para gerar dados adicionais (ex.: endereços, e-mails).
3. Insere os dados no banco OLTP usando o SQLAlchemy.

**Por que CSV e Scripts Auxiliares?**

- **CSV para Simplicidade**: O arquivo CSV fornece uma maneira simples de inicializar o banco com dados iniciais, imitando um cenário real onde os dados podem vir de uma fonte externa.
- **Scripts Auxiliares para Realismo**: Gerar dados brasileiros realistas (ex.: endereços, e-mails) garante que o projeto reflita um cenário de e-commerce real, tornando-o mais relevante para o meu portfólio.

### 4. Geração de Dados Aleatórios para Clientes e Compras

Para simular a atividade contínua de e-commerce, criei a DAG `dag_register_purchases_and_clients.py`. Essa DAG:

- Gera novos clientes com nomes, e-mails, endereços e números de telefone aleatórios.
- Cria compras aleatórias para esses clientes, incluindo itens, quantidades e status de compra (ex.: "Pendente", "Entregue").
- Usa o módulo `random` do Python para introduzir variabilidade nos dados (ex.: seleção aleatória de itens, datas de compra).

**Por que Dados Aleatórios?**

- **Teste de Cargas Incrementais**: Dados gerados aleatoriamente me permitem testar o processo de carga incremental, simulando novas transações ao longo do tempo.
- **Realismo**: A aleatoriedade imita a natureza imprevisível das transações de e-commerce, tornando o pipeline mais realista.

### 5. Criação do Banco OLAP

O banco OLAP foi criado usando um esquema em estrela, que é ideal para consultas analíticas. O esquema inclui:

- **Tabelas de Dimensão**: `dim_customers`, `dim_items`, `dim_sizes`, `dim_stores`, `dim_time`.
- **Tabelas de Fatos**: `fact_sales` (para dados de vendas), `fact_inventory` (para dados de estoque).

A criação foi gerenciada pela DAG `dag_create_ecommerce_tables_olap.py`, que usa o SQLAlchemy para definir o esquema com restrições apropriadas (ex.: `CheckConstraint` para regiões, chaves estrangeiras).

**Por que um Esquema em Estrela?**

- **Desempenho Analítico**: Um esquema em estrela simplifica consultas analíticas (ex.: agregações, junções) e é otimizado para ferramentas como dashboards de BI.
- **Escalabilidade**: Separar dimensões e fatos permite consultas eficientes e facilita a evolução do esquema.

### 6. Cargas ETL Inicial e Incremental com Airflow

O pipeline ETL foi implementado usando o **Apache Airflow**, com duas DAGs principais para carregar dados no banco OLAP:

- `dag_initial_load_olap.py`: Realiza a carga inicial do banco OLAP, extraindo todos os dados do banco OLTP, transformando-os e carregando-os no banco OLAP.
- `dag_incremental_load_olap.py`: Realiza cargas incrementais, extraindo apenas dados novos ou atualizados (com base no timestamp `created_at`) desde a última execução, transformando-os e carregando-os no banco OLAP.

A lógica ETL está encapsulada no script `etl.py`, que define duas classes:

- `ETLInitial`: Gerencia a carga inicial, apagando e recriando tabelas, e depois carregando todos os dados.
- `ETLIncremental`: Gerencia cargas incrementais, filtrando dados novos e adicionando-os às tabelas existentes, usando `ON CONFLICT DO NOTHING` para evitar duplicatas.

**Por que o Airflow?**

- **Orquestração**: O Airflow me permite agendar e gerenciar o pipeline ETL, garantindo que as tarefas sejam executadas na ordem correta (ex.: extrair → transformar → carregar).
- **Monitoramento**: A interface do Airflow fornece visibilidade sobre a execução das tarefas, logs e falhas, o que é essencial para pipelines de produção.
- **Escalabilidade**: O Airflow pode escalar para lidar com fluxos de trabalho mais complexos à medida que o projeto cresce.

### 7. XCom para Comunicação entre Tarefas

Nas DAGs do Airflow (`dag_initial_load_olap.py` e `dag_incremental_load_olap.py`), utilizei o **XCom** (Cross-Communication) para passar dados entre tarefas:

- A tarefa `extract_task` extrai dados e os envia para o XCom com uma chave (ex.: `extracted_data`).
- A tarefa `transform_task` recupera os dados extraídos do XCom, transforma-os e envia o resultado com outra chave (ex.: `transformed_data`).
- A tarefa `load_task` recupera os dados transformados do XCom e os carrega no banco OLAP.

**Por que o XCom?**

- **Simplicidade**: O XCom elimina a necessidade de armazenamento externo (ex.: arquivos, bancos de dados) para passar dados entre tarefas, simplificando o pipeline.
- **Eficiência**: Ele permite que as tarefas se comuniquem diretamente dentro do ambiente do Airflow, reduzindo o overhead.

**Nuance**: O XCom não é adequado para conjuntos de dados muito grandes, pois armazena os dados no banco de metadados do Airflow. Em um ambiente de produção, eu ajustaria o pipeline para salvar grandes conjuntos de dados em um local externo (ex.: S3) e passar o caminho do arquivo via XCom.

### 8. Transformações de Dados

Durante o processo ETL, várias transformações foram aplicadas:

- **Normalização para Desnormalização**: O banco OLTP é normalizado (ex.: tabelas separadas para endereços, telefones). No banco OLAP, desnormalizei os dados em um esquema em estrela para consultas analíticas mais rápidas.
- **Mapeamento de Regiões**: Estados brasileiros foram mapeados para regiões (ex.: "SP" → "Sudeste") usando um dicionário no script `etl.py`. Ajustei as regiões para português (ex.: "Northeast" → "Nordeste") para refletir o contexto brasileiro.
- **Dimensão de Tempo**: Criei uma tabela `dim_time` com atributos como dia, mês, trimestre e ano para permitir análises baseadas em tempo.
- **Tratamento de Valores Nulos**: Ajustei a lógica de transformação para lidar com valores `NaN` em colunas que permitem nulos (ex.: `customer_id` em `fact_sales`), convertendo-os para `None` para inserção correta no banco OLAP.

**Por que Essas Transformações?**

- **Prontidão Analítica**: As transformações garantem que o banco OLAP esteja otimizado para relatórios (ex.: agregações por região, tempo).
- **Relevância Cultural**: Usar nomes de regiões em português alinha os dados ao contexto brasileiro, tornando-os mais significativos para stakeholders locais.
- **Integridade dos Dados**: O tratamento de valores `NaN` garante compatibilidade com o esquema do banco e evita erros de inserção.

### 9. Tratamento de Erros e Depuração

Ao longo do projeto, encontrei e resolvi vários problemas:

- **Inconsistência de Tamanho de String**: Corrigi um erro em que a coluna `size` no banco OLAP tinha uma restrição de tamanho (`VARCHAR(4)`) muito pequena para valores como "Pequeno". Aumentei para `VARCHAR(10)`.
- **Inconsistência de Nomenclatura de Regiões**: Corrigi uma incompatibilidade entre os nomes de regiões na lógica de transformação ("North East") e a restrição do esquema ("Northeast"), posteriormente ajustando para português ("Nordeste").
- **Erro de Valor Numérico Fora do Intervalo**: Encontrei um erro `psycopg2.errors.NumericValueOutOfRange` ao carregar dados em `fact_sales`. O problema foi causado por valores `NaN` na coluna `customer_id` (um campo `INTEGER` que permite nulos), que o PostgreSQL não conseguia processar. Resolvi isso convertendo `NaN` para `None` antes da inserção e reescrevi o método `load_facts` para usar o `Session` do SQLAlchemy, que lida melhor com campos que permitem nulos.
- **Inconsistência de Fuso Horário em Cargas Incrementais**: Tive problemas com a DAG de carga incremental (`dag_incremental_load_olap.py`) que falhava devido a uma incompatibilidade de fuso horário entre a DAG do Airflow (configurada em `America/Sao_Paulo`) e o banco OLTP (em `UTC`). Isso causava falhas nas comparações de `created_at`, resultando na não extração de novos registros. Corrigi isso convertendo explicitamente o `last_execution_date` para UTC antes de consultar o banco OLTP, garantindo uma extração incremental precisa.
- **Condição de Parada da Carga Incremental**: Inicialmente, o método `extract` em `ETLIncremental` lançava um erro (`ValueError: Nenhum novo registro encontrado para processar`) se não encontrasse novos registros em `purchases`, interrompendo a DAG. Isso era muito restritivo para um pipeline incremental. Modifiquei a lógica para registrar uma mensagem e continuar processando outras tabelas (ex.: `inventory`, `customers`), permitindo que a DAG fosse concluída com sucesso mesmo quando não havia novas compras.
- **Melhorias nos Logs**: Adicionei logs detalhados nos métodos `extract` e `load_facts` para rastrear o número de novos registros extraídos e carregados, bem como seus timestamps `created_at`, melhorando a visibilidade do comportamento do pipeline.

**Por que Isso Importa?**

- **Confiabilidade**: O tratamento adequado de erros garante que o pipeline seja robusto e possa se recuperar de falhas.
- **Habilidades de Depuração**: Destacar essas correções no meu portfólio mostra minha capacidade de solucionar problemas complexos, como incompatibilidades de fuso horário e erros de banco de dados, e implementar soluções eficazes.

### 10. Implantação com Docker

O projeto é conteinerizado usando **Docker** e **Docker Compose**:

- `Dockerfile`: Define o ambiente do Airflow com as dependências necessárias (ex.: SQLAlchemy, psycopg2).
- `docker-compose.yaml`: Configura serviços para o Airflow (webserver, scheduler, worker) e bancos PostgreSQL para OLTP e OLAP.

**Por que o Docker?**

- **Reprodutibilidade**: O Docker garante que o projeto possa rodar em qualquer máquina com o mesmo ambiente.
- **Portabilidade**: Facilita o compartilhamento e a implantação do projeto, uma habilidade essencial para um engenheiro de dados.
---

## Como Executar o Projeto

1. **Pré-requisitos**:

   - Docker e Docker Compose instalados.
   - Python 3.12+ (se executar sem Docker).
   - PostgreSQL (se executar sem Docker).
   - Airflow (se executar sem Docker).

2. **Configuração**:

   - Clone o repositório:

     ```
     git clone <url_do_repositorio>
     cd ECOMMERCY-DATABASE
     ```

   - Instale as dependências (se não usar Docker):

     ```
     pip install -r requirements.txt
     ```

   - Atualize as credenciais do banco em `OLTP_URL` e `OLAP_URL` nos arquivos das DAGs (ex.: `dag_initial_load_olap.py`).

3. **Executar com Docker**:

   - Inicie os serviços:

     ```
     docker-compose up -d
     ```

   - Acesse a interface do Airflow em `http://localhost:8080` (credenciais padrão: airflow/airflow).

4. **Executar as DAGs**:

   - Na interface do Airflow, ative e dispare as seguintes DAGs na ordem:
     1. `create_ecommerce_tables_oltp`: Cria as tabelas OLTP.
     2. `initial_load_oltp`: Popula o banco OLTP.
     3. `create_ecommerce_tables_olap`: Cria as tabelas OLAP.
     4. `register_purchases_and_clients`: Gera novas compras e clientes para testes.
     5. `initial_load_olap`: Realiza a carga inicial do banco OLAP.
     6. `incremental_load_olap`: Realiza cargas incrementais no banco OLAP.

5. **Monitorar a Execução**:

   - Verifique os logs e o status das tarefas na interface do Airflow.
   - Confirme os dados nos bancos OLTP e OLAP usando um cliente PostgreSQL (ex.: pgAdmin). Para verificar cargas incrementais, consulte a tabela `fact_sales` com a coluna `created_at`:

     ```
     SELECT purchase_id, created_at
     FROM fact_sales
     WHERE created_at > 'YYYY-MM-DD HH:MM:SS'
     ORDER BY created_at DESC;
     ```

---

## Lições Aprendidas

- **Design de Esquema**: Usar o Draw.io me ajudou a planejar a estrutura do banco de dados de forma eficaz, garantindo que os bancos OLTP e OLAP atendessem aos seus respectivos objetivos (transacional vs. analítico).
- **Orquestração ETL**: As DAGs e o XCom do Airflow facilitaram o gerenciamento do pipeline ETL, mas aprendi a ter cuidado com o XCom para grandes conjuntos de dados.
- **Geração de Dados**: Gerar dados brasileiros realistas (ex.: endereços, regiões em português) adicionou autenticidade ao projeto.
- **Depuração de Problemas Complexos**: Resolver incompatibilidades de fuso horário entre o Airflow e o banco OLTP, lidar com valores `NaN` em colunas que permitem nulos e ajustar a lógica de carga incremental para ser mais robusta melhorou significativamente minhas habilidades de resolução de problemas.
- **Importância dos Logs**: Adicionar logs detalhados em cada etapa do pipeline (ex.: em `extract` e `load_facts`) foi crucial para depurar e verificar o comportamento correto das cargas incrementais.
- **Gerenciamento de Fuso Horário**: Aprendi a importância de alinhar timestamps entre sistemas, especialmente ao trabalhar com bancos de dados e ferramentas de orquestração em fusos horários diferentes.

---

## Melhorias Futuras

- **Otimizar o Uso do XCom**: Para grandes conjuntos de dados, salvar dados intermediários em um armazenamento externo (ex.: S3) e passar os caminhos dos arquivos via XCom.
- **Adicionar Verificações de Qualidade de Dados**: Implementar tarefas de validação para garantir a consistência dos dados antes de carregar no banco OLAP (ex.: verificar chaves estrangeiras ausentes).
- **Expandir Capacidades Analíticas**: Integrar o banco OLAP com uma ferramenta de BI (ex.: Power BI, Tableau) para criar dashboards de análise de vendas e estoque.
- **Automatizar a Geração de Dados**: Agendar a DAG `register_purchases_and_clients` para rodar periodicamente, simulando atividade contínua de e-commerce.
- **Monitoramento e Alertas**: Adicionar tarefas de monitoramento à DAG para alertar sobre falhas ou comportamentos inesperados (ex.: ausência de novos registros por um período prolongado).

---

## Contato

Fique à vontade para me contatar para perguntas ou oportunidades de colaboração:

- **LinkedIn**: [Ruan Castro](https://www.linkedin.com/in/ruan-castro-900262183/)