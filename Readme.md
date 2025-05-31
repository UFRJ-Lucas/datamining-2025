# Datamining - Primeiros Passos

Este projeto utiliza Docker Compose para subir um banco de dados PostGIS e ferramentas auxiliares para análise de dados de GPS de ônibus.

## 1. Pré-requisitos

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)
- Python 3.8+ com `pip` (para rodar o ETL)

## 2. Instalando as Dependências do Python

Instale as dependências com o comando abaixo:

```bash
pip install -r requirements.txt
```

Se preferir use um ambiente virtual:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Subindo o Banco de Dados

No diretório do projeto, execute:

```bash
docker compose up -d
```

Isso irá iniciar os containers do PostGIS e do PgAdmin.

## 4. Rodando o ETL

Execute o script ETL para importar os  dados de GPS para o banco:

```bash
python etl/etl.py
```

O script irá processar os arquivos de dados e inserir no banco de dados do container.