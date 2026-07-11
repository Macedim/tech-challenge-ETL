# Tech Challenge – Pipeline de Dados para o Indicador Nacional de Alfabetização

Pipeline de engenharia de dados construído em **Databricks + PySpark + Delta Lake**, sobre **Amazon S3**, para processar os resultados da avaliação de alfabetização de alunos brasileiros e transformá-los em indicadores confiáveis de acompanhamento das metas nacionais, estaduais e municipais de alfabetização (2024–2030).

---

## 1. Contexto do problema

O Brasil enfrenta um desafio estrutural histórico: garantir que todas as crianças estejam alfabetizadas na idade certa. Para acompanhar esse esforço, o poder público define metas de alfabetização por município, estado e para o país como um todo, e aplica avaliações padronizadas (com prova de Língua Portuguesa e cálculo de proficiência via TRI – Teoria de Resposta ao Item) para medir, ano a ano, o percentual de alunos alfabetizados.

O problema é que esses dados chegam **brutos, fragmentados e em múltiplas granularidades** — resultado por aluno, por item de prova, por município, por estado e metas planejadas separadamente — em arquivos CSV carregados por ano de referência. Sem um processo estruturado de ingestão, padronização e agregação, não é possível:

- comparar o desempenho de um município com sua própria meta e com a média do estado/país;
- identificar tendência de melhora ou piora ao longo dos anos;
- gerar uma base analítica confiável para dashboards, relatórios e modelos preditivos.

Este projeto resolve esse problema construindo um **pipeline de dados ponta a ponta**, que transforma os arquivos brutos da avaliação em uma camada analítica (Gold) pronta para consumo por áreas de negócio, BI e ciência de dados.

## 2. Desafio educacional e uso do indicador de alfabetização

O indicador central do projeto é o **`PC_ALUNO_ALFABETIZADO`** — o percentual de alunos alfabetizados em um recorte (município, estado ou país), calculado a partir da proficiência individual em Língua Portuguesa (`VL_MEDIA_LP`) e da flag de alfabetização.

Esse indicador foi escolhido como eixo do projeto porque:

- é **comparável ao longo do tempo** (permite ver evolução ano a ano por município/estado);
- é **acionável**: pode ser confrontado diretamente com as metas oficiais de alfabetização definidas até 2030, respondendo à pergunta "estamos no caminho certo?";
- permite **segmentação por rede de ensino** (federal, estadual, municipal, privada), essencial para orientar política pública, já que a maior parte do desafio de alfabetização está concentrada na rede pública municipal;
- serve de base para **detectar desigualdade educacional**, ao comparar a distribuição de alunos entre regiões do país.

A partir dele, o pipeline calcula, na camada Gold, métricas derivadas como:
- **Diferença em relação à meta** (`DIF_META_ALFABETIZACAO_BRASIL`, `DIF_META_ALFABETIZACAO_UF`, `DIF_META_ALFABETIZACAO_MUNICIPIO`)
- **Se a meta foi atingida** (`ATINGIU_META_BRASIL`, `ATINGIU_META_UF`, `ATINGIU_META_MUNICIPIO`)
- **Variação ano a ano** (`VARIACAO_ALFABETIZACAO`)
- **Tendência** (`TENDENCIA`: Melhorou / Piorou / Estável)
- **Classificação do município** por nível de desempenho (Nível 1 a 5 e Abaixo do Nível 1, baseado em faixas de `PC_ALUNO_ALFABETIZADO`)

## 3. Arquitetura proposta

O projeto segue a **arquitetura medallion (bronze → silver → gold)**, um padrão consolidado para pipelines de Data Lakehouse, executado em **Databricks** com armazenamento em **Amazon S3** e tabelas em formato **Delta Lake**.

| Camada | Papel | Formato | Localização |
|---|---|---|---|
| **Raw** | Arquivos originais (CSV) enviados pela fonte, particionados por ano | CSV | `s3://bucket/raw/` |
| **Bronze** | Dado raw ingerido, com tipagem correta, padronização de texto e chave técnica (hash) para deduplicação | Delta | `s3://bucket/bronze/` |
| **Silver** | Dado bronze enriquecido com regras de negócio (região, faixas de proficiência/alfabetização, validações de qualidade) | Delta | `s3://bucket/silver/` |
| **Gold** | Indicadores agregados e prontos para consumo: comparação com metas, análise de níveis, feature store para Machine Learning | Delta | `s3://bucket/gold/` |

### 3.1 Descrição da arquitetura da solução

#### Camada Bronze (Ingestão)

- **Ingestão em lote (batch)**: notebooks em `notebooks/bronze/raw_bz_*` leem os CSVs brutos por ano (`recursive_by_year=True`), aplicam:
  - **Casting de tipos**: conversão de colunas (ex.: `ano` string → `int`, `taxa_alfabetizacao` → `double`)
  - **Padronização de strings**: `trim()`, `upper()`, `initcap()` para padronizar valores textuais
  - **Chave técnica via hash**: geração de `SK_*` (surrogate keys) usando `sha2()` para permitir `MERGE` idempotente (upsert) e evitar duplicidade em reprocessamentos
  - **Auditoria**: adição de `DT_PROCESSAMENTO` e `TS_PROCESSAMENTO` em todas as linhas

- **Ingestão em streaming**: `notebooks/bronze/stream_novos_resultados.ipynb` usa **Databricks Auto Loader** (`cloudFiles`) para capturar continuamente novos arquivos de resultado assim que chegam em um diretório de staging no S3, escrevendo em modo `append` numa tabela Delta dedicada, com checkpoint para garantir exactly-once.

#### Camada Silver (Enriquecimento e Qualidade)

- Os notebooks em `notebooks/silver/` leem a Bronze e aplicam:
  - **Regras de negócio**: 
    - Classificação geográfica (ex.: mapear `SG_UF` para `CO_UF` e `REGIAO`)
    - Faixas de proficiência e alfabetização para análise por nível
    - Indicadores derivados (ex.: `QTD_METAS_DEFINIDAS`, `IN_POSSUI_META`, `IN_POSSUI_RESULTADO`)
  - **Validações de qualidade** (via `commons/validators.ipynb`):
    - `validate_primary_key`: garante unicidade da chave técnica
    - `validate_not_null`: verifica colunas obrigatórias
    - `validate_years`: garante existência de ano de referência
    - `validate_schema`: garante tipo de dados esperado
    - `validate_foreign_key`: valida integridade referencial com tabelas relacionadas

#### Camada Gold (Agregação Analítica)

- Os notebooks em `notebooks/gold/` cruzam tabelas Silver e produzem:
  - **`FT_INDICADOR_MUNICIPIO`**: indicador de alfabetização do município, comparado ao estado e país, com variação e tendência ano a ano
  - **`FT_INDICADOR_MUNICIPIO_META_VS_RESULTADO`**: resultado observado vs. meta oficial, com flags de meta atingida em cada nível
  - **`ANALISE_NIVEIS_MUNICIPIO`**: perfil de distribuição dos alunos entre níveis de proficiência
  - **`FT_MACHINE_LEARNING`**: base a nível de aluno, enriquecida com indicadores do município e do estado, pronta para treinar modelos

#### Módulo Commons (Compartilhado)

- **`config.ipynb`**: centraliza paths do lake (S3), nomes de tabelas, opções de leitura CSV (encoding, delimiter)
- **`readers.ipynb`**: função `read()` para leitura padronizada (suporta `recursive_by_year` para carregar múltiplos anos com um único comando)
- **`writers.ipynb`**: função `write_delta()` com suporte a `MERGE` (upsert) e `overwrite`, evitando duplicação de código
- **`validators.ipynb`**: funções reutilizáveis para validação de qualidade antes de cada escrita na Silver

### 3.2 Fluxo de dados

1. **Aquisição**: arquivos CSV oficiais (aluno, item de prova, município, estado, metas) chegam organizados por ano de referência no bucket S3 (`raw/`).

2. **Bronze**: cada notebook `raw_bz_*`:
   - Lê todos os anos disponíveis (ex.: `read(base_path=RAW_PATH, table_name="TS_ALUNO", recursive_by_year=True)`)
   - Faz casting de tipos (ex.: `col("ano").cast("int")`)
   - Padroniza texto (ex.: `initcap(trim(col("rede")))`)
   - Gera chave técnica: `sha2(concat_ws("|", col("ANO"), upper(col("REDE"))), 256)` → `SK_META_ALFABETIZACAO`
   - Escreve com `MERGE` pela chave técnica: `write_delta(..., merge_keys=["SK_*"])`
   - Garante idempotência em reprocessamentos

3. **Streaming paralelo**: novos resultados que chegam fora do ciclo de carga anual são capturados continuamente pelo Auto Loader e incorporados à Bronze quase em tempo real, sem competir com a carga batch.

4. **Silver**: 
   - Lê Bronze em formato Delta
   - Aplica regras de negócio (ex.: `when(col("SG_UF") == "RO", 11).when(col("SG_UF") == "AC", 12)...` para mapear UF → CO_UF)
   - Calcula indicadores (ex.: `col("META_2024").isNotNull().cast("int") + col("META_2025").isNotNull().cast("int") + ...` → `QTD_METAS_DEFINIDAS`)
   - Roda validações de qualidade antes de escrever
   - Se qualquer validação falhar, a escrita é interrompida (raise ValueError)

5. **Gold**: 
   - Lê tabelas Silver (TS_ALUNO, TS_MUNICIPIO, TS_ESTADO, METAS_*)
   - Faz JOINs para cruzar município × estado × metas
   - Calcula diferenças: `col("PC_ALUNO_ALFABETIZADO") - col("META_ALFABETIZACAO_BRASIL")`
   - Gera flags: `when(col("PC_ALUNO_ALFABETIZADO") >= col("META_ALFABETIZACAO_BRASIL"), "Sim").otherwise("Não")` → `ATINGIU_META_BRASIL`
   - Usa Window Functions para variação: `lag("PC_ALUNO_ALFABETIZADO").over(window_municipio)` → `VARIACAO_ALFABETIZACAO`
   - Gera classificação por nível: `when(col("PC_ALUNO_ALFABETIZADO") >= 80, "Nível 5").when(...).otherwise("Abaixo do Nível 1")`

6. **Consumo**: as tabelas Gold alimentam dashboards de BI, relatórios de acompanhamento de metas e modelos de Machine Learning.

### 3.3 Diagrama da pipeline

```
Raw (CSV)
  ├─ TS_ALUNO/2024, 2025, 2026...
  ├─ TS_ITEM/2024, 2025, 2026...
  ├─ TS_MUNICIPIO/2024, 2025, 2026...
  ├─ TS_ESTADO/2024, 2025, 2026...
  ├─ METAS/BRASIL, METAS/UF, METAS/MUNICIPIO
  └─ Novos_resultados (chegada contínua)
         ↓
    [raw_bz_*.ipynb + stream_novos_resultados.ipynb]
         ↓
Bronze (Delta)
  ├─ TS_ALUNO [SK_ALUNO, ANO_REFERENCIA, CO_MUNICIPIO, ...]
  ├─ TS_ITEM [SK_ITEM, ANO_REFERENCIA, CO_ITEM, ...]
  ├─ TS_MUNICIPIO [SK_TS_MUNICIPIO, ANO_REFERENCIA, CO_MUNICIPIO, ...]
  ├─ TS_ESTADO [SK_TS_ESTADO, ANO_REFERENCIA, CO_UF, ...]
  ├─ METAS_BR [SK_META_ALFABETIZACAO, ANO, ...]
  ├─ METAS_UF [SK_META_ESTADO, ANO, ...]
  ├─ METAS_MUNICIPIO [SK_META_MUNICIPIO, ANO, ...]
  └─ STREAM_NOVOS_RESULTADOS [SK_*, ANO_REFERENCIA, ...]
         ↓
    [bz_sv_*.ipynb + validators]
         ↓
Silver (Delta)
  ├─ TS_ALUNO [SK_ALUNO, ANO_REFERENCIA, CO_MUNICIPIO, REGIAO, IN_ALFABETIZADO, ...]
  ├─ TS_ITEM [SK_ITEM, ANO_REFERENCIA, FAIXA_DIFICULDADE, ...]
  ├─ TS_MUNICIPIO [SK_TS_MUNICIPIO, ANO_REFERENCIA, CO_MUNICIPIO, REGIAO, NO_MUNICIPIO_UF, FAIXA_ALFABETIZACAO, ...]
  ├─ TS_ESTADO [SK_TS_ESTADO, ANO_REFERENCIA, CO_UF, SG_UF, REGIAO, ...]
  ├─ METAS_BR [SK_META_ALFABETIZACAO, ANO_REFERENCIA, QTD_METAS_DEFINIDAS, IN_POSSUI_META, ...]
  ├─ METAS_UF [SK_META_ESTADO, ANO_REFERENCIA, CO_UF, REGIAO, IN_POSSUI_META, ...]
  └─ METAS_MUNICIPIO [SK_META_MUNICIPIO, ANO_REFERENCIA, CO_MUNICIPIO, IN_POSSUI_META, DS_NIVEL_ALFABETIZACAO, ...]
         ↓
    [gd_*.ipynb: joins + cálculos]
         ↓
Gold (Delta)
  ├─ FT_INDICADOR_MUNICIPIO
  │  └─ [CO_MUNICIPIO, ANO_REFERENCIA, CLASSIFICACAO, ORDEM_CLASSIFICACAO, ATINGIU_META_BRASIL/UF/MUNICIPIO, VARIACAO_ALFABETIZACAO, TENDENCIA, ...]
  ├─ FT_INDICADOR_MUNICIPIO_META_VS_RESULTADO
  │  └─ [CO_MUNICIPIO, ANO_REFERENCIA, META_ALFABETIZACAO_BRASIL/UF/MUNICIPIO, DIF_META_*, ...]
  ├─ ANALISE_NIVEIS_MUNICIPIO
  │  └─ [CO_MUNICIPIO, ANO_REFERENCIA, FAIXA_ALFABETIZACAO, QTD_ALUNOS, PCT_ALUNOS, ...]
  └─ FT_MACHINE_LEARNING
     └─ [SK_ALUNO, ANO_REFERENCIA, CO_MUNICIPIO, VL_MEDIA_LP, IN_ALFABETIZADO, PC_ALUNO_ALFABETIZADO, DIF_ALFABETIZACAO_ESTADO, ...]
         ↓
   BI / Dashboards / Modelos ML / Políticas Públicas
```

## 4. Tecnologias utilizadas

| Tecnologia | Onde é usada | Justificativa |
|---|---|---|
| **Databricks (notebooks PySpark)** | Todas as camadas | Ambiente gerenciado de Spark com suporte nativo a Delta Lake, Auto Loader e agendamento de jobs, reduzindo esforço de infraestrutura. |
| **Apache Spark / PySpark** | Transformações em todas as camadas | Processamento distribuído, necessário pelo volume de dados por aluno/item em escala nacional, com uma única API para batch e streaming. |
| **Delta Lake** | Bronze, Silver e Gold | Suporte a `MERGE` (upsert), ACID transactions, time travel e schema enforcement — essencial para reprocessamentos idempotentes e para dados que chegam por ano de referência. |
| **Amazon S3** | Armazenamento do Data Lake (raw/bronze/silver/gold) | Armazenamento de objetos barato, durável e desacoplado do processamento, permitindo escalar armazenamento e compute de forma independente. |
| **Databricks Auto Loader (`cloudFiles`)** | Ingestão de novos resultados | Ingestão incremental e eficiente de arquivos que chegam de forma contínua, sem necessidade de listar o diretório inteiro a cada execução, com checkpoint gerenciado. |
| **Structured Streaming** | `stream_novos_resultados.ipynb` | Permite ingestão near real-time de novos resultados sem impactar a janela de processamento batch. |
| **PySpark SQL** | Transformações, validações e cálculos | Linguagem SQL sobre Spark para transformações declarativas e expressivas, facilitando auditoria e manutenção do código. |

## 5. Estrutura do repositório

```
tech-challenge-ETL/
├── commons/                          # Módulos compartilhados entre os notebooks
│   ├── config.ipynb                  # Paths do lake (S3), nomes de tabelas, opções de leitura
│   ├── commons_imports.ipynb         # Imports centralizados + carrega os demais módulos
│   ├── readers.ipynb                 # Função read() para leitura padronizada do lake
│   ├── writers.ipynb                 # Função write_delta() com merge/overwrite
│   └── validators.ipynb              # Validações de qualidade de dado (PK, not null, anos, schema, FK)
│
├── notebooks/
│   ├── bronze/                       # Ingestão: raw -> bronze (casting, padronização, chave técnica)
│   │   ├── raw_bz_ts_aluno.ipynb     # Ingestão de dados por aluno
│   │   ├── raw_bz_ts_item.ipynb      # Ingestão de dados por item de prova
│   │   ├── raw_bz_municipio.ipynb    # Ingestão de agregação de município
│   │   ├── raw_bz_estado.ipynb       # Ingestão de agregação de estado
│   │   ├── raw_bz_metas.ipynb        # Ingestão de metas (Brasil, UF, Município)
│   │   └── stream_novos_resultados.ipynb   # Ingestão via Structured Streaming + Auto Loader
│   │
│   ├── silver/                       # Enriquecimento e qualidade: bronze -> silver
│   │   ├── bz_sv_ts_aluno.ipynb      # Transformação de alunos (regiões, indicadores)
│   │   ├── bz_sv_ts_item.ipynb       # Transformação de itens (faixas de dificuldade)
│   │   ├── bz_sv_ts_municipio.ipynb  # Transformação de municípios (regiões, faixas de desempenho)
│   │   ├── bz_sv_ts_estado.ipynb     # Transformação de estados (regiões, faixas de desempenho)
│   │   └── bz_sv_metas.ipynb         # Transformação de metas (indicadores, região, code UF)
│   │
│   └── gold/                         # Agregação analítica e indicadores: silver -> gold
│       ├── gd_indicador_municipio.ipynb    # Indicador de alfabetização por município (joins, variação, tendência)
│       ├── gd_metas_municipio.ipynb        # Análise resultado vs. meta por município
│       └── gd_machine_learning.ipynb       # Feature store para modelos preditivos
│
└── README.md                         # Este arquivo
```

### Descrição por camada:

#### Bronze
- Um notebook por entidade de origem
- Responsável por: tipagem de dados, padronização de texto, geração de chave técnica (hash)
- Escrita via `MERGE` pela chave técnica para garantir idempotência
- Exemplo: `raw_bz_metas.ipynb` carrega três tabelas (METAS_BR, METAS_UF, METAS_MUNICIPIO) e as escreve com `merge_keys=["SK_META_*"]`

#### Silver
- Um notebook por entidade (aluno, item, município, estado, metas)
- Responsável por: enriquecimento com regras de negócio, validação de qualidade
- Cada notebook valida antes de escrever (chave primária, not null, anos, schema, foreign keys)
- Exemplo: `bz_sv_metas.ipynb` mapeia `SG_UF` → `CO_UF`, calcula `QTD_METAS_DEFINIDAS`, e valida `SK_META_*` único

#### Gold
- Notebooks que cruzam múltiplas entidades Silver para gerar indicadores finais
- Exemplo: `gd_indicador_municipio.ipynb` faz JOINs entre município, estado, metas e calcula diferenças, atingimento e tendência
- Resultado é consumido diretamente por BI, dashboards e modelos

#### Commons
- Scripts reutilizáveis: configuração, leitura/escrita padronizadas, validações
- Reduz duplicação de código entre os 13+ notebooks

## 6. Decisões arquiteturais

### Batch vs. Streaming

A maior parte do pipeline é **batch**, pois a fonte principal (avaliação anual de alfabetização) é publicada em ciclos anuais, e o consumo (BI, metas, modelos) não exige latência de segundos.

Streaming foi adotado **apenas** para o cenário de novos resultados que podem chegar fora do ciclo oficial de carga, evitando esperar o próximo job batch para disponibilizá-los.

**Trade-off**: 
- ✅ Batch é mais barato (clusters efêmeros, processamento pontual)
- ✅ Streaming permite ingestão contínua de novos dados
- ⚠️ Streaming tem custo operacional (cluster sempre ativo ou trigger frequente); por isso foi isolado em um pipeline específico

### Data Lake vs. Data Warehouse

Optou-se por um **Data Lakehouse** (S3 + Delta Lake) em vez de um Data Warehouse tradicional.

**Trade-off**:
- ✅ DW dá performance de consulta melhor "pronta" e governança mais rígida
- ✅ Lakehouse permite guardar o dado raw e semiestruturado a **baixo custo**
- ✅ Schema pode ser aplicado progressivamente (bronze → silver → gold)
- ✅ Formato Delta oferece performance analítica e suporte a MERGE
- ⚠️ Lakehouse exige disciplina na limpeza de dados e validação

### Custo vs. Performance

- **Chaves técnicas via hash + `MERGE`** em vez de reescrever a tabela inteira: mais barato computacionalmente, evita duplicidade
- **Bronze/Silver mantém granularidade fina** (aluno, item) para rastreabilidade e auditoria
- **Gold é pré-agregada** por município/estado, otimizando custo e performance das consultas mais frequentes
- **Streaming restrito** a caso de uso específico (novos resultados), evitando custo de clusters sempre ativos

### Padronização de dados

- **Encoding ISO-8859-1 + delimiter ";"** para leitura de CSV: definido uma única vez em `config.ipynb`, reutilizado em todos os `read()`
- **Nomes de colunas padronizados** (SNAKE_CASE) em todas as camadas
- **Tipos de dados tipados** desde a Bronze (int, double, string, date, timestamp)
- **Surrogate keys via hash** para permitir upserts idempotentes e evitar duplicidade

## 7. Monitoramento e qualidade de dados

### Validações automatizadas

Na camada Silver, **antes de cada escrita**, rodam validações de qualidade (via `commons/validators.ipynb`):

- **`validate_primary_key(df, "SK_*")`**: garante que a chave técnica é única (sem duplicidade)
- **`validate_not_null(df, ["SK_*", "ANO_REFERENCIA"])`**: verifica colunas obrigatórias
- **`validate_years(df)`**: garante existência de ano de referência na faixa esperada
- **`validate_schema(df, expected_schema)`**: valida tipo de dados de cada coluna
- **`validate_foreign_key(df, reference_df, join_keys)`**: valida integridade referencial (ex.: CO_MUNICIPIO existe em TS_MUNICIPIO)

Se qualquer validação falhar, a escrita é interrompida com `raise ValueError`, evitando que dado inconsistente avance para camadas seguintes.

### Auditoria

- **`DT_PROCESSAMENTO`** (data) e **`TS_PROCESSAMENTO`** (timestamp) adicionados em todas as linhas
- Permite rastrear quando cada registro foi processado
- Facilita investigação de incidentes e reprodução de estados históricos

### Monitoramento operacional

- Jobs do Databricks agendados com alertas nativos de falha/sucesso
- Checkpoint do Structured Streaming permite acompanhar se a ingestão contínua está em dia ou atrasada
- Logs e métricas de execução disponíveis no Databricks UI

## 8. Aplicação em IA e análise avançada

A tabela **`FT_MACHINE_LEARNING`** (camada Gold) foi desenhada especificamente para suportar casos de uso de IA:

### Modelos de predição de alfabetização

- Dataset traz proficiência do aluno (`VL_MEDIA_LP`), diferença em relação à média do município e do estado
- Flags de posição relativa (`IN_ACIMA_MEDIA_MUNICIPIO/ESTADO`)
- Permite treinar modelos supervisionados (classificação) para prever probabilidade de um aluno **não** atingir nível esperado
- Possibilita intervenção pedagógica antecipada

### Análise de desigualdade educacional

- Cruzamento de região, rede de ensino e faixas de proficiência
- Quantifica gaps entre regiões (Norte/Nordeste vs. Sul/Sudeste) e entre redes (pública/privada)
- Identifica municípios com alta concentração em "defasagem extrema" para priorização de recursos

### Políticas públicas baseadas em dados

- Comparação sistemática entre resultado observado e meta oficial
- Permite simular cenários de investimento
- Direciona recursos públicos para municípios com maior distância da meta 2030
- Transforma avaliação em instrumento de gestão, não apenas de medição

## 9. Como executar o pipeline

### Pré-requisitos

- ✅ Workspace Databricks com PySpark 3.x
- ✅ S3 bucket configurado: `tech-challenge-etl-153372322872-us-east-2-an`
- ✅ IAM permissions para ler/escrever em S3
- ✅ Arquivos CSV brutos em `s3://bucket/raw/` organizados por tabela e ano

### Ordem de execução

#### 1. Ingestão (Bronze)
Executar em paralelo ou sequencialmente:
```
notebooks/bronze/raw_bz_metas.ipynb
notebooks/bronze/raw_bz_estado.ipynb
notebooks/bronze/raw_bz_municipio.ipynb
notebooks/bronze/raw_bz_ts_aluno.ipynb
notebooks/bronze/raw_bz_ts_item.ipynb
```

Opcional (execução contínua):
```
notebooks/bronze/stream_novos_resultados.ipynb
```

#### 2. Enriquecimento (Silver)
Executar em sequência (dependências):
```
notebooks/silver/bz_sv_metas.ipynb
notebooks/silver/bz_sv_ts_estado.ipynb
notebooks/silver/bz_sv_ts_municipio.ipynb
notebooks/silver/bz_sv_ts_aluno.ipynb
notebooks/silver/bz_sv_ts_item.ipynb
```

#### 3. Agregação analítica (Gold)
Executar após Silver estar completo:
```
notebooks/gold/gd_indicador_municipio.ipynb
notebooks/gold/gd_metas_municipio.ipynb
notebooks/gold/gd_machine_learning.ipynb
```

### Agendamento (Databricks Jobs)

1. **Job 1 (Bronze)**: executa todos os `raw_bz_*` em paralelo
   - Frequência: **diária** (ou conforme ciclo de carga da fonte)
   - Trigger: manualmente ou via webhook da fonte

2. **Job 2 (Silver)**: executa todos os `bz_sv_*` em sequência
   - Frequência: **diária** (após Job 1 completar)
   - Dependency: Job 1 completo

3. **Job 3 (Gold)**: executa todos os `gd_*` em sequência
   - Frequência: **diária** (após Job 2 completar)
   - Dependency: Job 2 completo

4. **Job 4 (Streaming)**: `stream_novos_resultados.ipynb`
   - Tipo: **continuous** (sempre ativo)
   - Cluster: job cluster efêmero com trigger a cada X minutos

### Exemplo: primeira carga manual

```python
# No Databricks notebook, executar:

# 1. Importar commons
%run "./commons/commons_imports"

# 2. Carregar bronze
%run "./notebooks/bronze/raw_bz_metas"
%run "./notebooks/bronze/raw_bz_estado"
# ... etc

# 3. Carregar silver
%run "./notebooks/silver/bz_sv_metas"
# ... etc

# 4. Carregar gold
%run "./notebooks/gold/gd_indicador_municipio"
# ... etc
```

## 10. Versionamento (Git)

O desenvolvimento segue um fluxo baseado em branches por funcionalidade:

- **`main`**: branch estável, reflete produção
- **`develop`**: branch de integração, reflete próxima release
- **Feature branches**: `feature/*` para novas funcionalidades (ex.: `feature/bronze-ingestao`, `feature/silver-validacoes`, `feature/gold-indicadores`)
- **Commits descritivos**: refletem evolução do pipeline (ex.: "feat: add SK_META_* para idempotência de upsert", "fix: validate_primary_key para TS_MUNICIPIO")
- **Pull Requests**: descrição clara do que mudou e por quê, permitindo revisão antes do merge

### Convenção de commit

```
feat: adiciona validação de foreign key em Silver
fix: corrige casting de tipo em bz_sv_metas
docs: atualiza README com exemplo de execução
refactor: simplifica função read() com suporte a formato automático
```

---

## 11. Próximos passos e melhorias futuras

- 🔄 Integração com **Apache Airflow** ou **Databricks Workflows** para orquestração complexa
- 📊 Dashboard de monitoramento em **Power BI** alimentado por tabelas Gold
- 🤖 Modelo preditivo baseado em `FT_MACHINE_LEARNING` para detecção antecipada de alunos em risco
- 🌐 API REST para consultas ad-hoc sobre indicadores municipais
- 📧 Alertas automáticos quando meta não é atingida em X% dos municípios
- 🔒 Integração com **Unity Catalog** para governança e controle de acesso

---

**Autor(es):** _(preencher com o(s) nome(s) do grupo)_  
**Curso/Turma:** _(preencher)_  
**Data:** _07/2026_  
**Atualizado em:** _11/07/2026_
