# Datamining - Primeiros Passos

Este projeto utiliza Docker Compose para subir um banco de dados PostGIS e ferramentas auxiliares para análise de dados de GPS de ônibus.

## 1. Pré-requisitos

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)
- Python 3.8+ com `pip` (para rodar o ETL)

Verifique também a estrutura das pastas de dados em `dados/`:

```
dados/
    gps/
    gps_teste/
    gps_teste_final/
```

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

## 5. Executando o caderno de previsão

Com o banco de dados populado, já é possível executar `previsao_onibus.ipynb`. Este arquivo vai criar uma tabela com os buffers das linhas no banco, e gerar as previsões dos dados de teste final, que serão salvas na pasta `previsoes\`.

## 6. Submetendo as previsões

Após executar `previsao_onibus.ipynb`, basta executar o comando abaixo para enviar as previsões criadas para o endpoint do trabalho.

```bash
chmod +x enviar_previsoes.sh
./enviar_previsoes.sh
```