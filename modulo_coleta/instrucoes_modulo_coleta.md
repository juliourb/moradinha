# Instruções para desenvolvimento do módulo de coleta de dados

> **Versão:** 2.1 | **Última atualização:** 2026-03-22
>
> **Este arquivo é um documento vivo.** O agente (Claude Code) deve atualizá-lo ao final de cada sessão de trabalho, registrando o estado atual de implementação, decisões tomadas, erros encontrados e próximos passos. Nenhuma seção deve ser apagada — use ~~tachado~~ para marcar itens obsoletos e adicione notas quando necessário.

---

## Contexto do projeto

Você está ajudando a desenvolver o **módulo de coleta de dados** de uma pesquisa de doutorado em Planejamento e Gestão do Território (PPGPGT – UFABC).

O tema do trabalho é a **estimação multidimensional de necessidades habitacionais** usando microsimulação espacial. O projeto maior constrói um índice espacial baseado na grade H3 (Uber) que serve como matriz de covariáveis territoriais para métodos de desagregação e estimação em pequenas áreas (SAE) no nível intraurbano.

O módulo de coleta é a **primeira etapa** do pipeline. Ele deve baixar, recortar e persistir todos os dados brutos necessários para **qualquer município brasileiro**, deixando-os prontos para etapas posteriores de processamento e análise.

---

## ⚠️ Reaproveitamento de código do projeto anterior

Existe um projeto anterior no ambiente `h3_jacarei` com notebooks e scripts parcialmente funcionais. Antes de implementar qualquer função nova:

1. **Consultar a pasta do projeto `h3_jacarei`** para verificar se já existe código equivalente
2. **Refatorar o que já funciona** em vez de reescrever do zero — especialmente para downloads FTP, tratamento de shapefiles IBGE e consultas OSMnx
3. **Focar exclusivamente no módulo de coleta** — não trazer lógica de estimação, modelagem ou H3 para este módulo
4. **Reportar ao pesquisador** quais trechos foram reaproveitados e quais foram reescritos, justificando a decisão

---

## 🔴 Protocolo de trabalho incremental — LEIA ANTES DE QUALQUER COISA

Este projeto avança **função por função, grupo por grupo**. As regras abaixo são inegociáveis e devem ser seguidas em toda sessão de trabalho.

### Regra 1 — Perguntar antes de implementar

Antes de escrever qualquer código, o agente deve fazer as perguntas necessárias para eliminar ambiguidades. Exemplos típicos:

- "Encontrei código equivalente em `h3_jacarei/notebooks/NB03`. Posso refatorá-lo ou preferes partir do zero?"
- "O campo `Cod_setor` neste CSV tem zeros à esquerda? Preciso confirmar o dtype antes de implementar o join."
- "Para este grupo, preferes salvar primeiro o arquivo bruto e só depois escrever no DuckDB, ou em uma etapa só?"
- "Qual município usar para o teste desta função?"

Nunca assumir. Sempre perguntar quando houver dúvida razoável.

### Regra 2 — Uma função por vez

O fluxo padrão de cada função nova é:

```
1. Verificar se há código reaproveitável no projeto h3_jacarei
2. Propor a abordagem em linguagem natural (com referência ao código existente se houver)
3. Aguardar confirmação ou ajuste do pesquisador
4. Escrever o código
5. Executar e mostrar o resultado do teste
6. Aguardar validação explícita ("ok", "aprovado", "pode seguir")
7. Só então avançar para a próxima função
```

**Nunca implementar dois grupos ao mesmo tempo. Nunca pular a etapa de validação.**

### Regra 3 — Testar com dados reais, município a escolha do pesquisador

Toda função nova deve ser testada com um município real informado pelo pesquisador na hora do teste. O teste deve:

- Mostrar o output resumido (shape, colunas, primeiras linhas ou contagem de registros)
- Verificar CRS EPSG:4674
- Confirmar arquivo salvo no caminho correto
- Confirmar tabela escrita no DuckDB

O módulo deve funcionar igualmente bem para qualquer município — **nenhuma lógica pode depender de um código IBGE específico**.

### Regra 4 — Atualizar este arquivo ao final de cada sessão

Ao encerrar uma sessão (ou quando o pesquisador disser "salva o contexto"), o agente deve:

1. Atualizar a seção **"Estado atual de implementação"** abaixo
2. Marcar funções concluídas com ✅
3. Registrar decisões de arquitetura em **"Log de decisões"**
4. Registrar erros e soluções em **"Log de erros conhecidos"**
5. Atualizar **"Próximos passos"**
6. Sincronizar o `CONTEXTO.md` do projeto se ele existir

---

## Estado atual de implementação

> *Esta seção é mantida pelo agente. Não editar manualmente.*

| Componente | Status | Observações |
|---|---|---|
| Estrutura de pacotes `moradinha/modulo_coleta/` | ⬜ Pendente | — |
| `utils/db_utils.py` | ✅ Concluído | Testado com 2701407 (Campo Alegre-AL). Geometria salva como WKB. |
| `utils/ibge_ftp.py` | ✅ Concluído | Testado com 2701407 (66 setores Campo Alegre-AL, CRS 4674). URL censo corrigida. |
| `utils/osmx.py` | ✅ Concluído | Refatorado do template. Colunas mistas (lista+int) convertidas para str antes do parquet. |
| `utils/raster_utils.py` | ✅ Concluído | clip_raster (rasterio) + zonal_stats_por_camada (rasterstats) + ler_tabela_espacial. |
| `grupo1_geometrias.py` | ✅ Concluído | Testado com 2701407 (1 quadrante: ID_58) e 2701803 (2 quadrantes: ID_57+ID_58). Concatenação e clip corretos. |
| `grupo2_censo.py` | ✅ Concluído | Testado com 2701407: domicilio01 (90 cols), domicilio02 (407 cols), responsavel01 (6 cols). 72 setores cada. |
| `grupo3_logradouros.py` | ✅ Concluído | Testado com 2701407: 14.966 endereços CNEFE, 1.682 faces, 3.294 eixos OSM. |
| `grupo4_luminosidade.py` | ✅ Concluído | Testado com 2701407 (Campo Alegre-AL). modo='tile_local'. TIF 54x56px. luminosidade_2022 (66 setores) + luminosidade_2022_grade200 (976 células). |
| `grupo5_pnadc.py` | ✅ Concluído | Testado com 2701407 (Campo Alegre-AL). V2001=3.25±0.05, n=4732, V1029=2717818 (RM). S01xxx ausentes em todo PNADc 2022 — documentado nos metadados. |
| `grupo6_extensoes.py` (stub) | ⬜ Pendente | — |
| `orquestrador.py` | 🟡 Em progresso | Implementação do mapa vetorial com luminosidade de grade 200m. |

---

## Log de decisões

> *Registrar aqui toda decisão de arquitetura ou design tomada durante o desenvolvimento.*

| Data | Decisão | Alternativa rejeitada | Motivo |
|---|---|---|---|
| 2026-03-22 | Arquivo criado | — | Versão inicial |
| 2026-03-22 | Geometria salva como WKB bytes no parquet antes de carregar no DuckDB | GeoParquet nativo (geopandas) | DuckDB spatial não suporta colunas geometry com CRS em storage < v1.5.0; WKB + ST_GeomFromWKB é compatível |
| 2026-03-22 | Ambiente conda: `anaconda3\envs\moradinha` (não miniconda3) | miniconda3\envs\moradinha | Ambiente instalado em anaconda3 |
| 2026-03-22 | URL real do censo agregado: `.../Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/` | URL da instrução original (`.../Resultados_do_Universo/...`) que retorna 404 | Estrutura real do FTP IBGE verificada em 2026-03-22 |
| 2026-03-22 | Arquivos do censo agregado são nacionais (BR), não por UF | Download por UF | IBGE disponibiliza apenas arquivos nacionais no novo endereço FTP |
| 2026-03-22 | `geobr.read_weighting_area()` usa parâmetro `code_weighting`, não `code_muni` | `code_muni` | Assinatura real da função geobr Python |
| 2026-03-22 | `domicilio02` usa coluna `setor` (não `Cod_setor`) para identificar setor | Assumir nome uniforme | Diferença real entre arquivos do FTP IBGE |
| 2026-03-22 | `responsavel01` está em pasta FTP separada: `.../Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/` — arquivo: `Agregados_por_setores_renda_responsavel_BR_csv.zip` | Mesmo FTP dos outros dois | Divulgação separada do IBGE (abril 2025) — 6 colunas: V06001–V06005 |
| 2026-03-22 | Grade estatística: cruzamento com BR500KM.zip (8 KB) para identificar quadrante, depois download do grade_id{N}.zip específico | Download da grade nacional inteira | Metodologia IBGE: grade organizada por quadrantes de 500km |
| 2026-03-22 | Faces de logradouro: URL 2022 — `.../censo_2022/base_de_faces_de_logradouros_versao_2022_censo_demografico/shp/{SIGLA}_faces_de_logradouros_2022_shp.zip` (sigla maiúscula) | URL 2021 com pasta minúscula (retorna 404) | Versão 2022 disponível e alinhada com o Censo 2022 |
| 2026-03-22 | OSMnx: colunas com valores mistos (int + lista de ints) convertidas para `str` inteiramente antes do parquet | Converter só os valores lista | PyArrow infere int64 na coluna, rejeita str ao encontrar lista serializada |
| 2026-03-22 | VIIRS VNL V2.2 não define nodata nos metadados do TIF — passar `nodata=0` explicitamente para `rasterstats` (0 = ausência de luz = nodata para este produto) | Deixar rasterstats adivinhar (-999) ou suprimir o warning | 0 é o valor semanticamente correto para o produto average_masked |
| 2026-03-22 | Tile VIIRS baixado manualmente (global ~500MB) — modo='tile_local' como padrão; modo='download' reservado para implementação futura com token EOG | Download automático em todas as chamadas | Tile global reutilizável para qualquer município; download único evita retrabalho |
| 2026-04-14 | Orquestrador usa a tabela `luminosidade_YYYY_grade200` unida à `grade_estatistica` para plotar luminosidade de fundo | Ler `luminosidade_YYYY_grade200` como GeoDataFrame direto | A tabela de estatísticas não contém geometria; é necessário fazer um join com a grade espacial |
| 2026-03-22 | `ler_tabela_espacial()` adicionado em raster_utils.py — lê tabela com geometria do DuckDB via ST_AsWKB → GeoDataFrame | Reimplementar em cada grupo | Função reutilizável para qualquer grupo que precise ler geometrias do banco |
| 2026-03-22 | PNADc: geobr `code_weighting` ≠ PNADc `V1029` — sistemas de codificação diferentes (13 dígitos vs 7 dígitos). Filtro feito por prefixo de 2 dígitos de V1029 (= código UF) em vez de usar geobr para obter V1029 | Correspondência direta geobr→V1029 | Mismatch confirmado em campo: code_weighting=2701407003002 vs V1029=2717818 |
| 2026-03-22 | Coluna UF do PNADc é haven_labelled — converter para integer diretamente produz NAs. Filtrar por UF usando prefixo de V1029: `substr(V1029, 1, 2) == cod_uf` | `as.integer(as.character(UF)) == cod_uf` | Abordagem UF via label falhou; prefixo de V1029 é robusto |
| 2026-03-22 | Tipo de município (capital/RM/interior) detectado via geobr::read_capitals + read_metro_area → mapeado para regex de V1023 → seleciona V1029 com V1023 correspondente | Hardcoded por UF | Dinâmico para qualquer município brasileiro |
| 2026-03-22 | S01xxx (S01007A, S01011C, etc.) NÃO existem em nenhum produto PNADc acessível via get_pnadc() — trimestral T1-T4/2022 e interview=1/5 confirmados. Pertencem ao "Módulo Habitação" publicado separadamente pelo IBGE, sem acesso padrão pelo pacote PNADcIBGE | Assumir presença nos dados | Ausência confirmada após download de todos os trimestres e visitas de 2022 |
| 2026-03-22 | Fallback S01xxx: tenta outros trimestres do mesmo ano → interview=5 → documenta ausência nos metadados sem erro fatal | Falhar a execução | Consistente com a regra: variáveis ausentes = aviso, não erro |

---

## Log de erros conhecidos

> *Registrar aqui erros encontrados e como foram resolvidos.*

| Data | Erro | Solução |
|---|---|---|
| 2026-04-14 | `luminosidade_YYYY_grade200` não contém geometria e não pode ser lida diretamente como GeoDataFrame | Unir a tabela de estatísticas à camada `grade_estatistica` com geometria antes de plotar |

---

## Próximos passos

> *Atualizado pelo agente ao final de cada sessão.*

- [x] Consultar projeto `h3_jacarei` e mapear código reaproveitável (sem DuckDB — nenhum código reaproveitável para este módulo)
- [x] Criar estrutura de diretórios do pacote
- [x] Implementar e testar `db_utils.py` (testado com 2701407)
- [x] Implementar e testar `utils/ibge_ftp.py` — refatorar funções do template notebook
- [x] Implementar e testar `grupo1_geometrias.py`
- [x] Implementar e testar `utils/raster_utils.py`
- [x] Implementar e testar `grupo4_luminosidade.py` (modo tile_local, testado com 2701407)
- [x] Implementar e testar `grupo5_pnadc.py` (fluxo R → CSV → Python, testado com 2701407)
- [ ] Implementar `grupo6_extensoes.py` (apenas stubs)
- [ ] Implementar `orquestrador.py`

---

## Decisões de arquitetura já tomadas

- **Linguagem:** Python 3
- **Ambiente conda:** `moradinha` (Windows, VS Code)
- **Banco de dados analítico:** DuckDB + extensão spatial
- **Formato vetorial:** GeoParquet (via geopandas)
- **Formato raster:** GeoTIFF (mantido como arquivo bruto; estatísticas zonais extraídas para o banco)
- **CRS padrão:** EPSG:4674 (SIRGAS 2000) para todos os dados vetoriais
- **Convenção de nomes de município:** `{sigla_uf}_{nome_municipio}` em minúsculas, ex: `al_maceio`, `sp_jacarei`
- **Artefato final por município:**
  - `data/raw/{municipio}/` — arquivos brutos organizados por grupo
  - `data/processed/{municipio}/{municipio}.duckdb` — banco analítico com tabelas nomeadas
  - `data/processed/{municipio}/metadata.json` — datas de coleta, URLs, versões

---

## Contexto técnico do ambiente

- Windows, conda `moradinha`
- Bibliotecas já instaladas: `geopandas`, `pandas`, `osmnx`, `shapely`, `h3`
- Bibliotecas a instalar: `duckdb`, `rasterio`, `rasterstats`, `requests`, `tqdm`
- Para PNADc: script R com pacote `PNADcIBGE` (chamado via `subprocess`) — ver Grupo 5
- Acesso a dados: FTP IBGE, API geobr (Python), OSMnx, eogdata.mines.edu (requer conta gratuita)

---

## Estrutura do módulo

```
moradinha/modulo_coleta/
├── __init__.py
├── orquestrador.py           # função principal coletar_municipio()
├── grupos/
│   ├── grupo1_geometrias.py
│   ├── grupo2_censo.py
│   ├── grupo3_logradouros.py
│   ├── grupo4_luminosidade.py
│   ├── grupo5_pnadc.py
│   └── grupo6_extensoes.py   # stubs para futuro
└── utils/
    ├── ibge_ftp.py           # helpers de download FTP e descompressão
    ├── osmx.py               # helpers de dados OSM (grafo, edges, nodes)
    ├── raster_utils.py       # clip e zonal stats
    └── db_utils.py           # conexão e escrita DuckDB
```

### Assinatura padrão de cada grupo

```python
def coletar_grupo_X(
    codigo_ibge: str,        # 7 dígitos, ex: "3524402"
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn: duckdb.DuckDBPyConnection,
    **kwargs
) -> dict:
    """
    Retorna dict com status e metadados:
    {"status": "ok" | "erro", "camadas": [...], "mensagem": "..."}
    """
```

### Função principal

```python
def coletar_municipio(
    codigo_ibge: str,
    grupos: list[int] = [1, 2, 3, 4, 5],
    base_dir: Path = Path("data")
) -> None
```

---

## Ordem de implementação recomendada

Seguir esta ordem para minimizar retrabalho e dependências cruzadas:

```
1.  utils/db_utils.py         → base de tudo; testar conexão e escrita
2.  utils/ibge_ftp.py         → necessário para grupos 1, 2 e 3
3.  grupo1_geometrias.py      → limite municipal é insumo dos demais grupos
4.  grupo2_censo.py           → dados tabulares; depende apenas de FTP
5.  utils/osmx.py             → necessário para grupo 3
6.  grupo3_logradouros.py     → migrar notebook h3_jacarei; não reescrever
7.  utils/raster_utils.py     → necessário para grupo 4
8.  grupo4_luminosidade.py    → requer token EOG; testar com tile já baixado se disponível
9.  grupo5_pnadc.py           → fluxo R→CSV→Python; menos dependências internas
10. grupo6_extensoes.py       → apenas stubs com docstrings
11. orquestrador.py           → somente após todos os grupos validados
```

---

## Grupos de dados a implementar

### Grupo 1 — Geometrias base (prioridade máxima)

| Camada | Fonte | Método | Tabela DuckDB |
|---|---|---|---|
| Limite municipal | geobr | `read_municipality(code_muni=int(codigo_ibge))` | `limite_municipal` |
| Setores censitários 2022 | geoftp.ibge.gov.br | download .gpkg por UF, filtro por `CD_MUN` | `setores_censitarios` |
| Grade estatística 200m | ftp.ibge.gov.br | download ZIP por UF, clip por bbox | `grade_estatistica` |
| Áreas de ponderação | geobr | `read_weighting_area(code_muni=int(codigo_ibge))` | `areas_ponderacao` |

URLs de referência:
- Setores: `https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022_preliminar/setores/gpkg/UF/{SIGLA}/{SIGLA}_Malha_Preliminar_2022.gpkg`
- Grade: `https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Grade_de_Estatisticas/`

**Atenção:** a sigla e código da UF devem ser derivados do `codigo_ibge` (primeiros 2 dígitos = código UF), nunca codificados como constantes.

---

### Grupo 2 — Dados tabulares do Censo 2022 (prioridade máxima)

| Arquivo | Fonte | Conteúdo | Tabela DuckDB |
|---|---|---|---|
| domicilio01 | `Agregados_por_Setores_Censitarios/UF_yyyymmdd.zip` | características físicas do domicílio | `censo_domicilio01` |
| domicilio02 | mesmo ZIP | acesso a serviços (água, esgoto, energia) | `censo_domicilio02` |
| responsavel01 | mesmo ZIP | renda e instrução do responsável | `censo_responsavel01` |

- Encoding: `latin-1`, separador: `;`
- Join com geometria via `Cod_setor` = `CD_SETOR`
- O nome exato do ZIP varia por UF e data de publicação — implementar busca dinâmica no FTP em vez de URL hardcoded
- URL base: `https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Resultados_do_Universo/Agregados_por_Setores_Censitarios/`

---

### Grupo 3 — Endereços e logradouros

| Camada | Fonte | Método | Tabela DuckDB |
|---|---|---|---|
| Endereços CNEFE | ftp.ibge.gov.br | download ZIP por UF, clip por polígono municipal | `enderecos_cnefe` |
| Faces de logradouro IBGE | geoftp.ibge.gov.br | download ZIP por UF, filtro por `CD_MUN` | `faces_logradouro` |
| Eixos viários OSM | OSMnx | `graph_from_polygon()` → edges como GeoDataFrame | `eixos_osm` |

Padrão de URLs (retirado do notebook `h3_jacarei` — manter compatibilidade):
```python
url_cnefe = (
    f"https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos"
    f"/Censo_Demografico_2022/Coordenadas_enderecos/UF/{codigo_uf}_{sigla_uf}.zip"
)
url_faces = (
    f"https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos"
    f"/malha_de_setores_censitarios/censo_2010/base_de_faces_de_logradouros_versao_2021"
    f"/{sigla_uf_lower}/{sigla_uf_lower}_faces_de_logradouros_2021.zip"
)
```

**Importante:** consultar o notebook do projeto `h3_jacarei` e refatorar o código existente — não reescrever do zero.

---

### Grupo 4 — Luminosidade noturna VIIRS VNL V2.2

**Fonte:** Earth Observation Group (EOG), Colorado School of Mines
**URL base:** `https://eogdata.mines.edu/nighttime_light/annual/v22/{ano}/`
**Arquivo a baixar:** `_average_masked.tif`
**Licença:** CC BY 4.0 — **requer registro gratuito e token de autenticação (EOG)**

```python
# 1. Download do tile que cobre a América do Sul
# 2. Clip pelo limite municipal
import rasterio
from rasterio.mask import mask as rio_mask

with rasterio.open(caminho_tif) as src:
    out_image, out_transform = rio_mask(
        src, [limite_geom.__geo_interface__], crop=True
    )

# 3. Estatísticas zonais por setor censitário e grade 200m
from rasterstats import zonal_stats

stats_setores = zonal_stats(
    setores_gdf,
    caminho_tif_recortado,
    stats=["mean", "median", "max", "std"],
    geojson_out=True
)

# 4. Salvar GeoTIFF recortado em data/raw/{municipio}/luminosidade/
# 5. Persistir estatísticas no DuckDB como tabela luminosidade_{ano}
```

**Anos a coletar:** 2022 (ano-base do Censo) e 2024 (mais recente disponível)

**Atenção:** arquivo global ~500MB — implementar `tqdm` para barra de progresso e verificar hash quando disponível. Documentar claramente que o token EOG é obrigatório.

---

### Grupo 5 — PNADc (Pesquisa Nacional por Amostra de Domicílios Contínua)

> ⚠️ **Limitação fundamental:** A PNADc **não tem representatividade municipal**. A menor unidade geográfica publicada é a **área de ponderação**. Para qualquer município, a área de ponderação correspondente deve ser identificada via `geobr::read_weighting_area()` e filtrada com precisão. **Nunca usar dados agregados para a UF inteira como proxy do município.**

#### 5.1 — Método de acesso

O fluxo adotado é: **R + `PNADcIBGE` → CSV → Python → DuckDB**

- R é mantido para extração e cálculo de estimativas porque o pacote `PNADcIBGE` trata o plano amostral complexo de forma robusta via `survey`
- Python consome o CSV exportado pelo R e persiste no DuckDB
- A integração ocorre via `subprocess.run(["Rscript", ...])`

**Pergunta obrigatória antes de implementar:** confirmar com o pesquisador se o R está instalado no ambiente `moradinha` e qual o caminho do executável `Rscript`.

#### 5.2 — Variáveis a coletar

| Variável | Descrição | Categoria |
|---|---|---|
| `V1028` | Peso do domicílio | **Obrigatório — plano amostral** |
| `Estrato` | Estrato da amostra | **Obrigatório — plano amostral** |
| `UPA` | Unidade Primária de Amostragem | **Obrigatório — plano amostral** |
| `V1029` | Código da área de ponderação | **Obrigatório — filtro geográfico** |
| `V1022` | Situação do domicílio (urbano/rural) | Caracterização territorial |
| `V1023` | Tipo de área (capital, resto da RM, etc.) | Caracterização territorial |
| `V2001` | Tipo de domicílio | Estrutura habitacional |
| `VD5008` | Renda domiciliar per capita | Renda |
| `S01007A` | Material predominante nas paredes externas | Adequação construtiva |
| `S01011C` | Cobertura do telhado | Adequação construtiva |
| `S01012A` | Abastecimento de água | Serviços básicos |
| `S01013` | Esgotamento sanitário | Serviços básicos |
| `S01017` | Destino do lixo | Serviços básicos |
| `S01019` | Número de banheiros | Adensamento / condições |

> **Nota:** as variáveis `S01xxx` pertencem ao suplemento habitacional e podem não estar disponíveis em todos os trimestres. Tratar ausência com aviso no log, não como erro fatal. O módulo deve registrar no `metadata.json` quais variáveis estavam ausentes para o trimestre coletado.

#### 5.3 — Fluxo de processamento

```python
import subprocess
from pathlib import Path

def coletar_grupo5_pnadc(
    codigo_ibge: str,
    limite_municipal,
    output_dir: Path,
    db_conn,
    ano: int = 2022,
    trimestre: int = 4,
    **kwargs
) -> dict:

    script_r = Path("moradinha/modulo_coleta/r_scripts/extrair_pnadc.R")
    output_pnadc = output_dir / "pnadc"
    output_pnadc.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [
            "Rscript", str(script_r),
            "--codigo_ibge", codigo_ibge,
            "--ano", str(ano),
            "--trimestre", str(trimestre),
            "--output_dir", str(output_pnadc)
        ],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        return {"status": "erro", "mensagem": result.stderr}

    import pandas as pd
    csv_path = output_pnadc / f"pnadc_{ano}T{trimestre}_estimativas.csv"
    df = pd.read_csv(csv_path)
    db_conn.execute("CREATE OR REPLACE TABLE pnadc_estimativas AS SELECT * FROM df")

    return {"status": "ok", "camadas": ["pnadc_estimativas"], "mensagem": "ok"}
```

#### 5.4 — Script R (estrutura mínima de referência)

```r
# extrair_pnadc.R
# Uso: Rscript extrair_pnadc.R --codigo_ibge 3524402 --ano 2022 --trimestre 4 --output_dir ...

suppressPackageStartupMessages({
  library(PNADcIBGE)
  library(survey)
  library(dplyr)
  library(geobr)
})

# --- parsear argumentos ---
args <- commandArgs(trailingOnly = TRUE)
# implementar parser simples para --chave valor

# --- identificar área de ponderação do município ---
# NUNCA filtrar pela UF inteira
areas_pond <- geobr::read_weighting_area(code_muni = as.integer(codigo_ibge))
cod_pond <- unique(areas_pond$code_weighting)

# --- baixar e preparar dados ---
vars_necessarias <- c(
  "V1028", "Estrato", "UPA", "V1029",
  "V1022", "V1023", "V2001", "VD5008",
  "S01007A", "S01011C", "S01012A", "S01013", "S01017", "S01019"
)

pnadc_raw <- get_pnadc(year = ano, quarter = trimestre, vars = vars_necessarias)

# --- definir plano amostral (OBRIGATÓRIO) ---
pnadc_design <- pnadc_design(pnadc_raw)

# --- filtrar pela área de ponderação ---
pnadc_local <- subset(pnadc_design, V1029 %in% cod_pond)

# --- calcular estimativas com variância correta ---
est_renda      <- svymean(~VD5008, design = pnadc_local, na.rm = TRUE)
est_habitacao  <- svymean(
  ~V2001 + S01007A + S01011C + S01012A + S01013 + S01017 + S01019,
  design = pnadc_local, na.rm = TRUE
)

# --- exportar ---
df_out <- rbind(
  data.frame(variavel = names(coef(est_renda)),    estimativa = coef(est_renda),    erro_padrao = SE(est_renda)),
  data.frame(variavel = names(coef(est_habitacao)), estimativa = coef(est_habitacao), erro_padrao = SE(est_habitacao))
)
write.csv(df_out,
          file.path(output_dir, paste0("pnadc_", ano, "T", trimestre, "_estimativas.csv")),
          row.names = FALSE)
```

#### 5.5 — Metadados obrigatórios do Grupo 5

```json
{
  "fonte": "IBGE PNADc",
  "ano": 2022,
  "trimestre": 4,
  "nivel_geografico": "area_de_ponderacao",
  "areas_ponderacao_usadas": ["..."],
  "variaveis_coletadas": ["V1028", "V1029", "VD5008", "S01007A", "S01011C", "S01012A", "S01013", "S01017", "S01019", "V2001", "V1022", "V1023"],
  "variaveis_ausentes": [],
  "metodo_estimacao": "svymean/svytotal via PNADcIBGE + survey",
  "aviso": "Estimativas válidas para a área de ponderação. NÃO representam o município isoladamente."
}
```

#### 5.6 — Tabelas DuckDB geradas pelo Grupo 5

| Tabela | Conteúdo |
|---|---|
| `pnadc_estimativas` | Estimativas pontuais + erro padrão por variável |
| `pnadc_metadados` | Trimestre, ano, áreas de ponderação, avisos |

---

## Requisitos de qualidade do código

1. **Idempotência:** verificar se o arquivo já existe antes de baixar novamente
2. **Logging:** `logging` com nível INFO; registrar URL, tamanho e tempo de cada download
3. **Tratamento de erros:** cada grupo captura exceções e retorna `{"status": "erro", "mensagem": "..."}` sem interromper outros grupos
4. **Metadados:** `metadata.json` com URL, data, hash MD5 e versão da fonte ao final de cada coleta
5. **Extensibilidade:** novos grupos adicionados via dicionário `GRUPOS_DISPONIVEIS` sem alterar código existente
6. **CRS padrão:** EPSG:4674 (SIRGAS 2000) antes de persistir qualquer dado vetorial
7. **Generalidade:** nenhuma constante de município, UF ou código IBGE específico fora dos testes — tudo derivado do `codigo_ibge` recebido como parâmetro
8. **Docstrings:** todos os parâmetros, retorno e fonte dos dados documentados

---

## Estrutura de diretórios esperada

```
data/
├── raw/
│   └── {sigla_uf}_{municipio}/     # ex: al_maceio, sp_jacarei, rj_petropolis
│       ├── geometria/
│       │   ├── limite_municipal.gpkg
│       │   ├── setores_censitarios.gpkg
│       │   ├── grade_estatistica.gpkg
│       │   └── areas_ponderacao.gpkg
│       ├── censo/
│       │   ├── domicilio01.csv
│       │   ├── domicilio02.csv
│       │   └── responsavel01.csv
│       ├── logradouros/
│       │   ├── enderecos_cnefe.gpkg
│       │   ├── faces_logradouro.gpkg
│       │   └── eixos_osm.gpkg
│       ├── luminosidade/
│       │   ├── viirs_2022_recortado.tif
│       │   └── viirs_2024_recortado.tif
│       ├── pnadc/
│       │   ├── pnadc_2022T4_estimativas.csv
│       │   └── pnadc_metadados.json
│       └── miscelanea/              # grupos futuros
│           ├── cnes.gpkg
│           ├── inep.gpkg
│           └── cadunico.geoparquet
└── processed/
    └── {sigla_uf}_{municipio}/
        ├── {municipio}.duckdb
        └── metadata.json
```

---

## Extensões futuras (não implementar agora — apenas stubs com docstrings)

- **Grupo 6a:** Aglomerados subnormais / favelas (`geobr::read_urban_concentrations()`)
- **Grupo 6b:** CNES — estabelecimentos de saúde (API DATASUS)
- **Grupo 6c:** INEP — escolas geocodificadas (dados.gov.br)
- **Grupo 6d:** MapBiomas — cobertura e uso do solo (GEE ou download por bbox)
- **Grupo 6e:** CadÚnico — famílias geocodificadas (requer convênio institucional)

---

## Exemplo de uso esperado

```python
from moradinha.modulo_coleta.orquestrador import coletar_municipio

# Qualquer município — o módulo deve funcionar sem configuração adicional
coletar_municipio(codigo_ibge="2704302", grupos=[1, 2, 3])        # Maceió - AL
coletar_municipio(codigo_ibge="3304557", grupos=[1, 2, 3, 4])     # Rio de Janeiro - RJ
coletar_municipio(codigo_ibge="1501402", grupos=[1, 2, 3, 4, 5])  # Belém - PA
```

---

## Checklist de validação por grupo

Antes de declarar qualquer grupo "concluído":

**Todos os grupos:**
- [ ] Executa sem erros para um município informado pelo pesquisador
- [ ] Executa sem erros para um segundo município diferente (teste de generalização)
- [ ] Output tem CRS EPSG:4674
- [ ] Arquivo salvo no caminho correto sob `data/raw/`
- [ ] Tabela escrita no DuckDB com nome correto
- [ ] `metadata.json` atualizado com URL, data e hash
- [ ] Idempotência verificada (duas execuções não duplicam dados)
- [ ] Docstring completa

**Grupo 5 (PNADc) adicionalmente:**
- [ ] Área de ponderação identificada dinamicamente (não hardcoded)
- [ ] Pesos amostrais aplicados em todas as estimativas
- [ ] Variáveis ausentes tratadas com aviso, não erro fatal
- [ ] Aviso de limitação geográfica gravado no `metadata.json`
- [ ] Script R executado via `subprocess` e saída verificada
