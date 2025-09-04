# Breweries Use Case — PoC

![Beer Icon](./docs/imgs/beer.png)

Solução de dados, robusta e escalável, seguindo a arquitetura medallion (Bronze–Silver–Gold) para processar dados da **Open Brewery DB**. O projeto utiliza componentes modernos de dados para orquestração, processamento distribuído, formato de tabela transacional e consulta interativa.

## Arquitetura

### Visão geral
- **Data Fabric** sobre **Lakehouse**
- Transações ACID em formatos de tabela
- Escalabilidade e confiabilidade para lotes e consultas interativas
- Virtualização/consulta unificada com Trino
- Fonte única da verdade e camadas de dados padronizadas

## On Going:
![Arquitetura Atual](./docs/imgs/current_arch.jpg)

## To Be:
![Arquitetura Alvo](./docs/imgs/to_be_arch.jpg)

### Componentes
- **Airflow**: orquestração de workflows
- **Spark**: processamento distribuído e execução de jobs
- **Apache Iceberg**: formato de tabela para data lake (ACID)
- **Nessie**: catálogo para versionamento de tabelas Iceberg
- **MinIO**: armazenamento compatível com S3
- **Trino**: engine SQL para exploração/BI
- **PostgreSQL**: metastore do Airflow

### Camadas de dados
- **Bronze**: ingestão bruta da API, mínima transformação
- **Silver**: limpeza, padronização e validação
- **Gold**: agregações analíticas e métricas de negócio

---

## Pré-requisitos
- Docker 20.10+
- Docker Compose 2.0+
- Git
- Recomendado: 8 GB RAM, 4 vCPUs, 20 GB de espaço livre

> Em Linux/MacOS, garanta permissão de execução: `chmod +x setup.sh`.

---

## Início rápido

```bash
# 1) Clone o repositório
git clone <URL-do-repositorio>
cd breweries_case

# 2) Execute o setup completo
./setup.sh
```

O `setup.sh` realiza automaticamente:
- build das imagens
- subida dos containers
- criação dos namespaces e tabelas Iceberg no catálogo Nessie

Após a execução, as interfaces ficam disponíveis em:
- Airflow UI: http://localhost:8080
- Spark Master UI: http://localhost:8081
- Spark History Server: http://localhost:18080
- Trino Web UI: http://localhost:8082
- MinIO Console: http://localhost:9001 (usuário: `admin`, senha: `admin123`)
- Nessie (API): http://localhost:19120

---

## Estrutura do projeto

```
breweries_case/
├─ docker-compose.yaml
├─ requirements.txt
├─ spark-defaults.conf
├─ setup
│  ├─ setup.sh
│  └─ create_tables_script.py
├─ docker/
│  ├─ airflow/
│  │  └─ Dockerfile.airflow
│  └─ spark/
│     └─ Dockerfile.spark
├─ dags/
│  └─ ... (DAGs do Airflow)
├─ src/
│  ├─ config/
│  ├─ processors/
│  └─ utils/
├─ trino/
│  └─ etc/
│     ├─ config.properties
│     └─ catalog/
│        ├─ iceberg.properties
│        └─ memory.properties
├─ scripts/
│  └─ minio-init.sh
└─ docs/
   └─ imgs -> Icons usados no README.md
```

> Volumes de dados, eventos e logs (por exemplo `events/`, `spark-logs/`, `data/`) são criados e montados via `docker-compose.yaml`.

---

## Operação do pipeline

### Via Airflow
1. Acesse **http://localhost:8080** (usuário: `admin`, senha: `admin`)
2. Localize a DAG `breweries_data_pipeline`
3. Ative a DAG e dispare uma execução manual (ou aguarde o agendamento)

### Via Trino (consulta interativa)
Exemplo usando o CLI dentro do container do Trino:

```bash
docker exec -it trino trino --server http://localhost:8080 --catalog iceberg

-- Schemas (camadas)
SHOW SCHEMAS LIKE '%_layer';

-- Tabelas por camada
SHOW TABLES FROM iceberg.bronze_layer;
SHOW TABLES FROM iceberg.silver_layer;
SHOW TABLES FROM iceberg.gold_layer;

-- Amostras
SELECT * FROM iceberg.silver_layer.tbl_silver_brewery LIMIT 10;
SELECT * FROM iceberg.gold_layer.tbl_gold_brewery_agg LIMIT 10;
```

> Se preferir usar um cliente SQL externo, configure o endpoint `http://localhost:8082` com o catálogo `iceberg`.

---

## Troubleshooting

1) **Containers não sobem**
```bash
docker compose down -v
docker compose up -d
```

2) **Permissões no Airflow (UID)**
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose down && docker compose up -d
```

3) **Tabelas não encontradas**
Reexecutar a criação após os serviços estarem de pé:
```bash
python3 ./create_tables_script.py
```

### Logs e diagnóstico
```bash
docker compose logs --tail=200

# Serviços específicos
docker logs airflow-webserver   --tail=200
docker logs airflow-scheduler   --tail=200
docker logs spark-master        --tail=200
docker logs spark-worker-1      --tail=200

# Recursos
docker stats
```

---

## Desenvolvimento

- Estilo: PEP 8/257, type hints, tratamento de erros, logs
- Airflow: tarefas idempotentes, parâmetros em variáveis/arquivos de configuração
- PySpark: schemas explícitos, particionamento coerente, manuseio de datas
- Testes: estrutura para unit/integration; fixtures para dados sintéticos
- Qualidade: linters e pre-commit

Exemplo de estrutura de testes:
```
tests/
├─ unit/
│  ├─ test_bronze.py
│  ├─ test_silver.py
│  └─ test_gold.py
└─ integration/
   └─ ...
```

---

## Roadmap

- Great Expectations para Data Quality
- Observabilidade e métricas (Grafana)
- Data Catalog (ex.: DataHub)
- CI/CD (GitHub Actions) com build/publish de imagens
- Evolução de schema e auditoria de alterações
- Testes de performance/carga
- Evolução da infraestrutura para um ambiente robusto de orquestração de containers (Kubernetes)

---

## Licença

MIT License

Copyright (c) 2025 Daniel Ferreira da Costa

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

