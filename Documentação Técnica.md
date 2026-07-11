# Documentação Técnica – Pipeline de Alfabetização (Bronze / Silver / Gold)

**Versão:** 1.3  
**Data:** Julho/2026  
**Projeto:** Tech Challenge – Pós-Tech em Artificial Intelligence for Data Scientists (FIAP)

---

## Sumário

1. [Visão Geral Técnica](#1-visão-geral-técnica)
2. [Configuração e Parâmetros](#2-configuração-e-parâmetros-commonsconfigipynb)
3. [Módulos Compartilhados](#3-módulos-compartilhados-commons)
4. [Camada Bronze](#4-camada-bronze)
5. [Camada Silver](#5-camada-silver)
6. [Camada Gold](#6-camada-gold)
7. [Ordem de Execução dos Jobs](#7-ordem-de-execução-dos-jobs)
8. [Regras de Qualidade de Dados](#8-regras-de-qualidade-de-dados)
9. [Requisitos Técnicos](#9-requisitos-técnicos)
10. [Manutenção e Troubleshooting](#10-manutenção-e-troubleshooting)

---

## 1. Visão Geral Técnica

| Componente | Especificação |
|---|---|
| **Motor de processamento** | Apache Spark (PySpark), executado em notebooks Databricks |
| **Armazenamento** | Amazon S3, bucket `tech-challenge-etl-153372322872-us-east-2-an` |
| **Formato das tabelas persistidas** | Delta Lake (Bronze, Silver, Gold) |
| **Formato dos arquivos de origem** | CSV, separador `;`, encoding `ISO-8859-1`, com header |
| **Orquestração** | Notebooks como unidades independentes; ordem segue dependência entre camadas (ver seção 7) |
| **Idempotência** | Bronze e Silver usam `MERGE` por chave técnica (hash SHA-256); Gold usa `overwrite` (reconstrução completa) |
| **Versionamento** | Git com branches por funcionalidade (`feature/*`) e Pull Requests para `main` |

---

## 2. Configuração e Parâmetros (`commons/config.ipynb`)

Centraliza todos os paths e nomes de tabela — nenhum notebook deve hardcodar path ou nome de tabela fora deste arquivo.

### 2.1 Paths do Data Lake

```python
S3_BUCKET   = "tech-challenge-etl-153372322872-us-east-2-an"
S3_BASE_PATH = f"s3://{S3_BUCKET}"

RAW_PATH    = f"{S3_BASE_PATH}/raw"
BRONZE_PATH = f"{S3_BASE_PATH}/bronze"
SILVER_PATH = f"{S3_BASE_PATH}/silver"
GOLD_PATH   = f"{S3_BASE_PATH}/gold"
```

### 2.2 Nomes de Tabela (Constantes)

| Constante | Valor | Camada |
|---|---|---|
| `TS_ALUNO` | `TS_ALUNO` | Bronze, Silver |
| `TS_ESTADO` | `TS_ESTADO` | Bronze, Silver |
| `TS_ITEM` | `TS_ITEM` | Bronze, Silver |
| `TS_MUNICIPIO` | `TS_MUNICIPIO` | Bronze, Silver |
| `METAS_BR` | `METAS/BRASIL` | Bronze, Silver |
| `METAS_UF` | `METAS/UF` | Bronze, Silver |
| `METAS_MUNICIPIO` | `METAS/MUNICIPIO` | Bronze, Silver |
| `INDICADOR_MUNICIPIO` | `FT_INDICADOR_MUNICIPIO` | Gold |
| `METAS_MUNICIPIO_META_VS_RESULTADO` | `FT_INDICADOR_MUNICIPIO_META_VS_RESULTADO` | Gold |
| `ANALISE_NIVEIS_MUNICIPIO` | `ANALISE_NIVEIS_MUNICIPIO` | Gold |
| `FT_MACHINE_LEARNING` | `FT_MACHINE_LEARNING` | Gold |

### 2.3 Opções de Leitura CSV

```python
CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "sep": ";",
    "encoding": "ISO-8859-1"
}
```

### 2.4 Mapas de Domínio (`commons/commons_imports.ipynb`)

```python
DEPENDENCIA_MAP = {1: "Federal", 2: "Estadual", 3: "Municipal", 4: "Privada"}
PRESENCA_MAP    = {True: "Presente", False: "Ausente"}
ALFABETIZADO_MAP = {True: "Sim", False: "Não"}
```

---

## 3. Módulos Compartilhados (`commons/`)

### 3.1 `commons_imports.ipynb`
- Importa bibliotecas do Spark (`functions`, `types`, `Window`)
- Define mapas de domínio (acima)
- Carrega, via `%run`, os demais módulos (`config`, `readers`, `writers`, `validators`)
- Todo notebook de camada começa com `%run "../../commons/commons_imports"`

### 3.2 `readers.ipynb`

```python
def get_available_years(path: str) -> list[str]:
    """
    Lista as pastas de ano disponíveis para uma tabela no lake.
    Exemplo de retorno: ['2022', '2023', '2024']
    """

def read(base_path: str, table_name: str, format: str = "csv",
         recursive_by_year: bool = False) -> DataFrame:
    """
    Lê uma tabela do Data Lake.
    
    Parâmetros:
    - format: "csv", "delta" ou "parquet"
    - recursive_by_year=True: lê todas as pastas de ano disponíveis
      (get_available_years) e faz unionByName (allowMissingColumns=True),
      adicionando a coluna ANO_REFERENCIA com o ano de cada partição.
    - recursive_by_year=False: lê diretamente base_path/table_name
    """
```

**Uso:** `recursive_by_year=True` apenas na ingestão Bronze (leitura de múltiplos anos de CSV). Nas camadas Silver/Gold: `format="delta"`, `recursive_by_year=False` (tabela Delta já contém histórico consolidado).

### 3.3 `writers.ipynb`

```python
def write_delta(df: DataFrame, base_path: str, table_name: str,
                 merge_keys: list[str] | None = None,
                 write_mode: str = "merge") -> None:
    """
    Escreve um DataFrame no formato Delta.
    
    Parâmetros:
    - write_mode="overwrite": sobrescreve a tabela inteira (overwriteSchema=True).
      Usado nas tabelas Gold, totalmente recalculadas a cada execução.
      
    - write_mode="merge" (default): faz upsert por merge_keys.
        - Se a tabela Delta não existe no path, cria via overwrite.
        - Se existe, executa DeltaTable.merge():
            whenMatchedUpdateAll() + whenNotMatchedInsertAll()
      Usado nas tabelas Bronze e Silver, garantindo idempotência.
    """
```

### 3.4 `validators.ipynb`

| Função | O que valida | Comportamento em falha |
|---|---|---|
| `validate_primary_key(df, primary_key)` | Unicidade da(s) coluna(s) informada(s) | `ValueError` com contagem de duplicados |
| `validate_not_null(df, columns)` | Ausência de nulos nas colunas obrigatórias | `ValueError` por coluna, informando contagem de nulos |
| `validate_foreign_key(fact_df, dimension_df, key)` | Integridade referencial via `leftanti` join | `ValueError` com contagem de chaves órfãs |
| `validate_years(df, year_column="ANO_REFERENCIA")` | Existência de pelo menos um ano distinto | `ValueError` se não houver nenhum ano |
| `validate_schema(df, expected_schema)` | Colunas faltantes/inesperadas em relação ao schema | `ValueError` listando colunas faltantes e/ou inesperadas |

**Comportamento:** todas as funções imprimem `✔ ... validation passed.` em caso de sucesso e interrompem a execução (`raise ValueError`) em caso de falha — não há gravação de dado inválido.

---

## 4. Camada Bronze

**Papel:** ingestão do dado bruto com:
- Casting de tipos
- Padronização de texto (`initcap`, `trim`, `upper`)
- Geração de chave técnica via hash SHA-256
- Colunas de auditoria (`DT_PROCESSAMENTO`, `TS_PROCESSAMENTO`)
- Escrita via `write_delta(..., write_mode="merge")`

### 4.1 `raw_bz_ts_aluno.ipynb` → `TS_ALUNO`

| Atributo | Especificação |
|---|---|
| **Origem** | `RAW_PATH/TS_ALUNO`, leitura recursiva por ano |
| **Casts principais** | `NU_ANO_AVALIACAO`, `CO_UF`, `TP_SERIE`, `TP_DEPENDENCIA`, `CO_MUNICIPIO` → `int`; `ID_ALUNO`, `ID_ESCOLA` → `long`; `IN_PRESENCA_LP`, `IN_PREENCHIMENTO_LP`, `IN_ALFABETIZADO` → `int`; `VL_PROFICIENCIA_LP` → `double` |
| **Padronização** | `NO_MUNICIPIO` em `initcap(trim(lower(...)))` |
| **Chave técnica** | `SK_ALUNO = sha2(concat_ws("|", NU_ANO_AVALIACAO, ID_ALUNO), 256)` |
| **Colunas selecionadas** | Chave técnica; chaves de negócio (`NU_ANO_AVALIACAO`, `ANO_REFERENCIA`, `ID_ALUNO`, `ID_ESCOLA`); localização (`CO_UF`, `SG_UF`, `CO_MUNICIPIO`, `NO_MUNICIPIO`, `TP_DEPENDENCIA`, `TP_SERIE`); prova (`IN_PRESENCA_LP`, `IN_PREENCHIMENTO_LP`, `CO_CADERNO_LP`, blocos 1-4); resultado (`VL_PESO_ALUNO_LP`, `VL_PROFICIENCIA_LP`, `IN_ALFABETIZADO`); auditoria |
| **Escrita** | `merge_keys=["SK_ALUNO"]` |

### 4.2 `raw_bz_ts_item.ipynb` → `TS_ITEM`

| Atributo | Especificação |
|---|---|
| **Origem** | `RAW_PATH/TS_ITEM`, leitura recursiva por ano |
| **Casts principais** | Identificadores (`NU_ANO_AVALIACAO`, `CO_UF`, `CO_BLOCO`, `NU_POSICAO`, `CO_ITEM`, `TP_SERIE`, `TP_DISCIPLINA`, `TP_RESPOSTA_ITEM`, `TP_MODELO_TRI`, `IN_ITEM_COMUM`) → `int`; Parâmetros TRI (`NU_PARAM_A`, `NU_PARAM_B`, `NU_PARAM_C`, `NU_PARAM_B1..B4`) → `double` |
| **Chave técnica** | `SK_ITEM = sha2(concat_ws("|", NU_ANO_AVALIACAO, CO_UF, SG_UF, CO_BLOCO, NU_POSICAO), 256)` |
| **Conteúdo** | Metadados do item de prova (descritor de habilidade, gabarito, parâmetros TRI) |
| **Escrita** | `merge_keys=["SK_ITEM"]` |

### 4.3 `raw_bz_municipio.ipynb` → `TS_MUNICIPIO`

| Atributo | Especificação |
|---|---|
| **Origem** | `RAW_PATH/TS_MUNICIPIO`, leitura recursiva por ano |
| **Casts principais** | `NU_ANO_AVALIACAO`, `CO_UF`, `CO_MUNICIPIO`, `TP_SERIE`, `ID_TIPO_REDE` → `int`; `PC_ALUNO_ALFABETIZADO`, `VL_MEDIA_LP`, `PC_ALUNO_NIVEL_0_LP` a `PC_ALUNO_NIVEL_8_LP` → `double` |
| **Chave técnica** | `SK_MUNICIPIO = sha2(concat_ws("|", NU_ANO_AVALIACAO, CO_MUNICIPIO, TP_SERIE, ID_TIPO_REDE), 256)` |
| **Conteúdo** | Indicador agregado por município/rede (`PC_ALUNO_ALFABETIZADO`, `VL_MEDIA_LP`, distribuição por nível 0–8) |
| **Escrita** | `merge_keys=["SK_MUNICIPIO"]` |

### 4.4 `raw_bz_estado.ipynb` → `TS_ESTADO`

- Estrutura análoga à de `TS_MUNICIPIO`, porém agregado por UF
- **Chave técnica:** `SK_ESTADO = sha2(concat_ws("|", NU_ANO_AVALIACAO, CO_UF, TP_SERIE, ID_TIPO_REDE), 256)`
- **Escrita:** `merge_keys=["SK_ESTADO"]`

### 4.5 `raw_bz_metas.ipynb` → `METAS_BR`, `METAS_MUNICIPIO`, `METAS_UF`

| Atributo | Especificação |
|---|---|
| **Origem** | Três arquivos distintos (Brasil, municípios, UF) |
| **Casts** | `ano`, `meta_alfabetizacao_2024` a `2030` → `double`/`int`; `percentual_participacao` → `double` |
| **Padronização** | `rede` em `initcap(trim(...))`; `sigla_uf` em `upper(trim(...))` |
| **Chave técnica** | Específica por origem: `SK_META_ALFABETIZACAO` (Brasil), `SK_META_MUNICIPIO`, `SK_META_ESTADO` |
| **Escrita** | `merge_keys` correspondente à chave técnica de cada uma |

### 4.6 `stream_novos_resultados.ipynb` (Ingestão via Streaming)

| Atributo | Especificação |
|---|---|
| **Fonte** | `RAW_PATH/STREAMING`, monitorada via **Databricks Auto Loader** (`cloudFiles`) |
| **Schema explícito** | `StructType([StructField("ANO_REFERENCIA", IntegerType()), StructField("CO_MUNICIPIO", IntegerType()), StructField("ID_TIPO_REDE", IntegerType()), StructField("PC_ALUNO_ALFABETIZADO", DoubleType()), StructField("VL_MEDIA_LP", DoubleType())])` |
| **Destino** | `BRONZE_PATH/STREAM_NOVOS_RESULTADOS`, modo `append` |
| **Checkpoint** | `S3_BASE_PATH/checkpoints/stream_novos_resultados` — garante exactly-once |
| **Execução** | `query.awaitTermination()` mantém job contínuo (dedicado, separado do batch) |

---

## 5. Camada Silver

**Papel:** aplicar regras de negócio (enriquecimento) e rodar validações de qualidade **antes** de gravar. Escrita via `write_delta(..., write_mode="merge")`, sempre seguida de validações.

### 5.1 `bz_sv_ts_aluno.ipynb` → `TS_ALUNO`

**Novas colunas:**
- `FAIXA_PROFICIENCIA`: `Não avaliado` (nulo) / `<650` / `650-699` / `700-742` / `743-799` / `800+`
- `IN_PARTICIPOU_AVALIACAO`: 1 se `IN_PRESENCA_LP=1` e `IN_PREENCHIMENTO_LP=1`
- `DS_PARTICIPACAO`: "Participou" / "Não participou"
- `DS_SITUACAO_AVALIACAO`: "Não Avaliado" / "Avaliado"
- Enriquecimento de localização/dependência (`REGIAO`, `DS_DEPENDENCIA`, `NO_MUNICIPIO_UF`, `DS_SERIE`)

**Validações:**
- Checagem manual de duplicidade de `SK_ALUNO`
- `validate_primary_key(["SK_ALUNO"])`
- `validate_not_null(["SK_ALUNO", "ID_ALUNO", "CO_UF"])`
- `validate_years()`

### 5.2 `bz_sv_ts_municipio.ipynb` → `TS_MUNICIPIO`

**Novas colunas:**
- `REGIAO`: classificação da UF (Norte/Nordeste/Centro-Oeste/Sudeste/Sul)
- `NO_MUNICIPIO_UF`: `concat_ws(" - ", NO_MUNICIPIO, SG_UF)`
- `FAIXA_MEDIA_LP`: `Não avaliado` / `<700` / `700-742` / `743-799` / `800+`
- `FAIXA_ALFABETIZACAO`: `Não informado` / `Muito Baixa` (<40%) / `Baixa` (<60%) / `Boa` (<80%) / `Excelente` (≥80%)
- `IN_POSSUI_DISTRIBUICAO_NIVEIS`: 1 se `PC_ALUNO_NIVEL_0_LP` não é nulo
- `ANO_CARGA` / `MES_CARGA`: extraídos de `TS_PROCESSAMENTO`

**Validações:** `validate_primary_key(["SK_MUNICIPIO"])`, `validate_not_null(["SK_MUNICIPIO"])`, `validate_years()`

### 5.3 `bz_sv_ts_estado.ipynb` → `TS_ESTADO`

Mesmas regras de `REGIAO`, `FAIXA_MEDIA_LP`, `FAIXA_ALFABETIZACAO`, `IN_POSSUI_DISTRIBUICAO_NIVEIS`, `ANO_CARGA`/`MES_CARGA`.

**Validações:** `validate_primary_key(["SK_ESTADO"])`, `validate_not_null(["SK_ESTADO"])`, `validate_years()`

### 5.4 `bz_sv_ts_item.ipynb` → `TS_ITEM`

**Novas colunas:**
- `REGIAO` (mesma regra por `SG_UF`)
- `FAIXA_DIFICULDADE_ITEM`: `Não informado` / `Muito Fácil` / `Fácil` / `Médio` / `Difícil` / `Muito Difícil` (baseado em `NU_PARAM_B`)
- `FAIXA_DISCRIMINACAO`: `Não informado` / `Muito Baixa` / `Baixa` / faixas seguintes (baseado em `NU_PARAM_A`)
- `FAIXA_ACERTO_AO_ACASO`, `IN_PARAMETROS_POLITOMICOS`, `QT_PARAMETROS_B`

**Validações:** `validate_primary_key(["SK_ITEM"])`, `validate_not_null(["SK_ITEM"])`, `validate_years()`

### 5.5 `bz_sv_metas.ipynb` → `METAS_BR`, `METAS_UF`, `METAS_MUNICIPIO`

**Novas colunas (para cada recorte):**
- `REDE`: padronizado com `initcap(trim(...))`
- `QTD_METAS_DEFINIDAS`: soma de `META_ALFABETIZACAO_2024..2030` não nulas (0 a 7)
- `IN_POSSUI_META`: 1 se `QTD_METAS_DEFINIDAS > 0`
- `IN_POSSUI_RESULTADO` (BR/Município): 1 se `TAXA_ALFABETIZACAO` não é nula
- `DS_NIVEL_ALFABETIZACAO` (Município): "Abaixo da Meta" / "Meta Atingida" / "Não Informado"
- UF: `CO_UF` mapeado a partir de `SG_UF` via tabela IBGE

**Validações:** `validate_primary_key` na chave técnica correspondente, `validate_not_null`, `validate_years` para cada DataFrame.

---

## 6. Camada Gold

**Papel:** cruzar tabelas Silver e calcular indicadores finais de consumo. Escrita via `write_delta(..., write_mode="overwrite")`.

### 6.1 `gd_indicador_municipio.ipynb` → `FT_INDICADOR_MUNICIPIO`

**Joins:**
1. `TS_MUNICIPIO` (Silver, filtrando `ID_TIPO_REDE` fora de `[0, 5]`)
2. Metas (Brasil/UF/Município) "despivotadas" via `stack()` (linhas por ano)
3. `TS_ESTADO` (Silver), casado por `ANO_REFERENCIA`, `CO_UF`, `ID_TIPO_REDE`

**Colunas calculadas:**
| Coluna | Fórmula |
|---|---|
| `DIF_META_ALFABETIZACAO_BRASIL/UF/MUNICIPIO` | `PC_ALUNO_ALFABETIZADO − META_ALFABETIZACAO_{...}` (arredondado a 2 casas) |
| `ATINGIU_META_BRASIL/UF/MUNICIPIO` | "Sim" se `PC_ALUNO_ALFABETIZADO ≥ META_...`, senão "Não" |
| `DIF_ALFABETIZACAO_ESTADO` | `PC_ALUNO_ALFABETIZADO − PC_ALUNO_ALFABETIZADO_ESTADO` |
| `DIF_MEDIA_LP_ESTADO` | `VL_MEDIA_LP − VL_MEDIA_LP_ESTADO` |
| `ACIMA_MEDIA_ESTADO` | "Sim" se `DIF_ALFABETIZACAO_ESTADO ≥ 0` |
| `VARIACAO_ALFABETIZACAO` / `VARIACAO_MEDIA_LP` | `lag()` sobre janela `Window.partitionBy("CO_MUNICIPIO","ID_TIPO_REDE").orderBy("ANO_REFERENCIA")` |
| `TENDENCIA` | "Melhorou" / "Piorou" / "Estável" conforme sinal de `VARIACAO_ALFABETIZACAO` |

### 6.2 `gd_metas_municipio.ipynb` → `FT_INDICADOR_MUNICIPIO_META_VS_RESULTADO` e `ANALISE_NIVEIS_MUNICIPIO`

**Base:** `TS_MUNICIPIO` (Silver) filtrado para `ID_TIPO_REDE = 3` (Municipal) + última meta disponível por município (`row_number()` sobre `Window.partitionBy("CO_MUNICIPIO").orderBy(ANO_REFERENCIA desc)`).

**Tabela `FT_INDICADOR_MUNICIPIO_META_VS_RESULTADO`:**
| Coluna | Fórmula |
|---|---|
| `META_ANO_AVALIACAO` | Seleciona dinamicamente coluna `META_ALFABETIZACAO_<ano>` correspondente ao `ANO_REFERENCIA` |
| `DIF_META_ALFABETIZACAO` | `PC_ALUNO_ALFABETIZADO − META_ANO_AVALIACAO` |
| `IN_META_ATINGIDA` | Se não há meta: `PC_ALUNO_ALFABETIZADO > 80`; senão: `PC_ALUNO_ALFABETIZADO ≥ META_ANO_AVALIACAO` |
| `DISTANCIA_META_2030` | `PC_ALUNO_ALFABETIZADO − META_ALFABETIZACAO_2030` |
| `IN_META_2030_ATINGIDA` | "Sim" se `DISTANCIA_META_2030 ≥ 0` |

**Tabela `ANALISE_NIVEIS_MUNICIPIO` (perfil de distribuição):**
| Coluna | Fórmula |
|---|---|
| `PC_PERFIL_EXTREMA_DEFASAGEM` | `PC_ALUNO_NIVEL_0_LP + PC_ALUNO_NIVEL_1_LP` |
| `PC_PERFIL_EM_DESENVOLVIMENTO` | `PC_ALUNO_NIVEL_2_LP + PC_ALUNO_NIVEL_3_LP` |
| `PC_PERFIL_LIMITROFE` | `PC_ALUNO_NIVEL_4_LP + PC_ALUNO_NIVEL_1_LP` |
| `PC_PERFIL_AVANCADO` | `PC_ALUNO_NIVEL_5_LP + PC_ALUNO_NIVEL_6_LP` |
| `PC_TAXA_EXCELENCIA` | `PC_ALUNO_NIVEL_7_LP + PC_ALUNO_NIVEL_8_LP` |
| **`INDICE_POLARIZACAO`** | `PC_TAXA_EXCELENCIA / (PC_PERFIL_EXTREMA_DEFASAGEM + 0.01)` |
| **`INDICE_RISCO_ESTRUTURAL`** | `((PC_ALUNO_NIVEL_0_LP×4) + (PC_ALUNO_NIVEL_1_LP×3) + (PC_ALUNO_NIVEL_2_LP×2) + (PC_ALUNO_NIVEL_3_LP×1)) / 400` (0 se denominador = 0) |

### 6.3 `gd_machine_learning.ipynb` → `FT_MACHINE_LEARNING`

**Base:** `TS_ALUNO` (Silver, filtrando `CO_MUNICIPIO` não nulo) + `TS_MUNICIPIO` + `TS_ESTADO` (joins por `ANO_REFERENCIA`, localização e dependência).

**Features derivadas (nível aluno):**
| Coluna | Fórmula |
|---|---|
| `DIF_MEDIA_MUNICIPIO` | `VL_PROFICIENCIA_LP − VL_MEDIA_LP` |
| `DIF_MEDIA_ESTADO` | `VL_PROFICIENCIA_LP − VL_MEDIA_LP_ESTADO` |
| `IN_ACIMA_MEDIA_MUNICIPIO/ESTADO` | 1 se `VL_PROFICIENCIA_LP ≥` média correspondente |
| `DIF_ALFABETIZACAO_MUNICIPIO` | `PC_ALUNO_ALFABETIZADO_ESTADO − PC_ALUNO_ALFABETIZADO` |
| `DESEMPENHO_RELATIVO` | "Muito Acima" (≥30), "Acima" (≥10), "Na Média" (≥−10), "Abaixo" (≥−30), "Muito Abaixo" (<−30) |
| `TARGET` | `IN_ALFABETIZADO` — variável-alvo para classificação |
| `IN_PROVA_VALIDA` | 1 se `IN_PRESENCA_LP=1` e `IN_PREENCHIMENTO_LP=1` |
| `IN_POSSUI_PROFICIENCIA` | 1 se `VL_PROFICIENCIA_LP` não é nulo |

---

## 7. Ordem de Execução dos Jobs

O pipeline **não** é auto-orquestrado — a ordem deve ser garantida pelo agendador de jobs do Databricks (ou orquestrador externo).

### 7.1 Batch (Sequencial por dependência)

**1. Bronze (podem rodar em paralelo entre si):**
- `raw_bz_ts_aluno`
- `raw_bz_ts_item`
- `raw_bz_municipio`
- `raw_bz_estado`
- `raw_bz_metas`

**2. Silver (cada notebook depende apenas do seu Bronze correspondente):**
- `bz_sv_ts_aluno`
- `bz_sv_ts_item`
- `bz_sv_ts_municipio`
- `bz_sv_ts_estado`
- `bz_sv_metas`

**3. Gold (depende de múltiplas tabelas Silver, deve rodar por último):**
- `gd_indicador_municipio` (depende de `TS_MUNICIPIO`, `TS_ESTADO`, `METAS_*`)
- `gd_metas_municipio` (depende de `TS_MUNICIPIO`, `METAS_MUNICIPIO`)
- `gd_machine_learning` (depende de `TS_ALUNO`, `TS_MUNICIPIO`, `TS_ESTADO`)

### 7.2 Streaming (Independente)
- `stream_novos_resultados` → job **contínuo e independente**, não faz parte da esteira batch diária

---

## 8. Regras de Qualidade de Dados

| Camada | Validação | Função |
|---|---|---|
| Silver (todas) | Unicidade da chave técnica (`SK_*`) | `validate_primary_key` |
| Silver (todas) | Colunas obrigatórias não nulas | `validate_not_null` |
| Silver (todas) | Existência de ao menos um `ANO_REFERENCIA` | `validate_years` |
| Silver `TS_ALUNO` | Checagem extra de duplicidade de `SK_ALUNO` | Verificação manual (`groupBy` + `raise Exception`) |
| Disponível (futuro) | Integridade referencial fato ↔ dimensão | `validate_foreign_key` |
| Disponível (futuro) | Conformidade de schema | `validate_schema` |

**Comportamento:** todas as validações são **bloqueantes** — uma falha interrompe o notebook antes da chamada a `write_delta`, garantindo que nenhum dado inválido chegue à Silver.

---

## 9. Requisitos Técnicos

| Requisito | Especificação |
|---|---|
| **Cluster Databricks** | Runtime com suporte a Delta Lake e Auto Loader (Databricks Runtime padrão já inclui ambos) |
| **Permissões AWS** | Leitura/escrita no bucket `S3_BUCKET` configurado |
| **Bibliotecas Python** | Nenhuma dependência externa além do que já vem no Databricks Runtime (`pyspark`, `delta-spark`) |
| **Versão Python** | 3.x |
| **Git** | Para versionamento e colaboração |

---

## 10. Manutenção e Troubleshooting

### 10.1 Como reprocessar um ano específico

1. Localize os arquivos CSV originais no bucket `raw/` para o ano desejado.
2. Execute o(s) notebook(s) Bronze correspondente(s) (ex.: `raw_bz_ts_aluno`).
3. O `MERGE` por chave técnica atualizará apenas os registros daquele ano.
4. Execute os notebooks Silver e Gold na ordem definida na seção 7.

### 10.2 Como adicionar uma nova fonte de dados

1. Adicione o novo path no `config.ipynb` (se necessário).
2. Crie um novo notebook na camada Bronze para ingestão.
3. Crie um notebook Silver correspondente para enriquecimento.
4. Atualize os notebooks Gold que dependem da nova fonte.

### 10.3 Como lidar com falhas de validação

1. Identifique a validação que falhou no log do notebook.
2. Verifique os dados de origem (arquivos CSV) para inconsistências.
3. Se necessário, corrija os dados e reprocesse a partir da Bronze.
4. As validações bloqueantes impedem que dados inválidos avancem para a Silver.

### 10.4 Monitoramento de Streaming

- Verifique o checkpoint em `S3_BASE_PATH/checkpoints/stream_novos_resultados`
- Monitore a tabela `BRONZE_PATH/STREAM_NOVOS_RESULTADOS` para confirmar que novos registros estão sendo inseridos
- Utilize os logs do job de streaming no Databricks para identificar erros de leitura

---

