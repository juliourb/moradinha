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
| `grupo3_logradouros.py` | ✅ Concluído | 3 tabelas CNEFE + qualidade_geo. Testado com 2700300: 120.326 total, 96.193 residencial, 18.845 não-residencial, qualidade 99,97% alta. |
| `grupo4_luminosidade.py` | ✅ Concluído | Testado com 2701407 (Campo Alegre-AL). modo='tile_local'. TIF 54x56px. luminosidade_2022 (66 setores) + luminosidade_2022_grade200 (976 células). |
| `grupo5_pnadc.py` | 🟡 Em revisão | Será reescrito para PNADc Anual V1 + metodologia FJP 2021 (3 componentes em cascata). Versão trimestral preservada em `grupo5_pnadc.py.bak_trimestral`. |
| `grupo6_uso_solo_precariedade.py` | 🟡 Implementado | MapBiomas clip+zonal stats + FCU download+intersecção. Aguardando teste com 2700300. |
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
| 2026-04-21 | Grupo 5 reescrito em Python puro (sem R) usando requests + pandas.read_fwf + svy (Taylor linearization) | Manter R via subprocess | Smart App Control (SAC) do Windows 11 bloqueava DLLs do R, impedindo execução do script R |
| 2026-04-21 | Filtro de UF feito pela coluna `UF` (string "27") em vez de prefixo de V1029 | Prefixo de V1029 (como no R) | V1029 no arquivo de largura fixa tem zeros à esquerda ("002717818") — o prefixo leria "00" em vez de "27" |
| 2026-04-21 | geobr.read_capitals(as_sf=False) em vez de as_sf=True | as_sf=True (padrão) | Bug no geobr Python: read_capitals chama read_municipal_seat(show_progress=...) mas essa função não aceita esse parâmetro |
| 2026-04-21 | Dicionário PNADc obtido de Dicionario_e_input_20221031.zip (pasta Documentacao/) → extrai dicionario_PNADC_microdados_trimestral.xls | Download direto de .xls | IBGE empacota o dicionário dentro de um ZIP; não há .xls direto na pasta |
| 2026-04-21 | VD5008 é variável derivada: não existe no arquivo de microdados de largura fixa (confirmado via dicionário e arquivo .txt de input) | Tratá-la como qualquer outra variável | VD5008 é calculada pelo pacote PNADcIBGE; não integra o produto de microdados brutos |
| 2026-04-21 | samplics substituído por svy (pip install svy) para estimação com plano amostral | samplics | samplics 0.6.0 está arquivado; svy é o substituto mantido ativo (svylab.com) |
| 2026-04-25 | Migrar Grupo 5 da PNADc Trimestral para PNADc Anual Visita 1 | Manter Trimestral | A Trimestral não contém variáveis S01XXX de habitação. Apenas a Anual V1 (módulo Características Gerais dos Domicílios e Moradores) tem o módulo completo. FJP usa exclusivamente este produto. |
| 2026-04-25 | Adotar `V1032` como peso de domicílio em vez de `V1028` | Manter `V1028` | `V1028` é peso de pessoa; para estimativas de domicílio FJP usa `V1032` (peso com calibração). Réplicas `V1032001`–`V1032200` para erro padrão via Rao-Wu. |
| 2026-04-25 | Implementar metodologia FJP 2021 (3 componentes em cascata) no próprio Grupo 5 | Deixar cálculo do déficit para módulo separado | Grupo 5 deve entregar não apenas microdados da PNADc, mas também a estimativa de déficit pronta para a área de ponderação — é o produto final esperado deste grupo. |
| 2026-04-25 | Domicílios improvisados ficam fora do Grupo 5 | Tentar derivar via PNADc | A partir de 2016 o IBGE deixou de captar improvisados na PNADc. FJP usa CadÚnico para esse subcomponente. Documentar nos metadados; integração com CadÚnico fica para Grupo 6. |
| 2026-04-25 | Adensamento excessivo em alugados não entra no déficit (passou para inadequação na FJP 2021) | Manter como 4º componente | Mudança documentada no Relatório Metodológico FJP 2021. O déficit atual tem 3 componentes, não 4. |
| 2026-04-25 | V1029 (área de ponderação) não existe na PNADc Anual V1 2022 — domínio de estimação é (UF + V1023) | Usar V1029 como no produto Trimestral | Confirmado por inspeção do dicionário XLS: variáveis V10xx disponíveis são V1030, V1031, V1032, V1034. Sem V1029. A Anual V1 é representativa apenas para UF × tipo de área (capital/RM/interior). filtrar_area_ponderacao() usa V1023 como seletor; dominios_usados registra "V1023=2, V1023=3" nos metadados. |
| 2026-04-25 | Códigos S01XXX de 2022 diferem dos usados no plano original (baseado em versão anterior) | Manter códigos do plano | Confirmado via parsear_dicionario() + parsear_categorias(): S01005=cômodos, S01006=dormitórios, S01017=condição ocupação, S01019=aluguel. Constantes S01001_COMODO={3}, S01002_RUSTICO={3,5,6}, S01017_ALUGADO={3} hardcoded com comentário indicando que devem ser validadas ao mudar de ano. |
| 2026-04-26 | CNEFE separado em 3 tabelas: bruta + residencial + não-residencial | Filtragem só no consumo (modulo_estimacao) | Filtragem na coleta evita repetição de lógica em todas as etapas; tabelas filtradas servem como contrato claro entre módulos |
| 2026-04-26 | Coluna qualidade_geo derivada de NV_GEO_COORD adicionada em todas as tabelas CNEFE | Manter NV_GEO_COORD bruto | Categorização semântica facilita filtros downstream sem que o consumidor precise lembrar dos códigos numéricos |

---

## Log de erros conhecidos

> *Registrar aqui erros encontrados e como foram resolvidos.*

| Data | Erro | Solução |
| --- | --- | --- |
| 2026-04-14 | `luminosidade_YYYY_grade200` não contém geometria e não pode ser lida diretamente como GeoDataFrame | Unir a tabela de estatísticas à camada `grade_estatistica` com geometria antes de plotar |
| 2026-04-21 | SAC (Smart App Control) do Windows 11 bloqueava DLLs do R, impedindo execução via subprocess no ambiente conda moradinha | Reescrita do Grupo 5 em Python puro (sem R) |

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

#### 3.4 — Filtros aplicados ao CNEFE (decisão 2026-04-26)

O CNEFE bruto contém múltiplas espécies de endereços. Para evitar contaminação de
covariáveis habitacionais, três tabelas são geradas:

- `enderecos_cnefe`: bruta (todas as espécies, todas as qualidades de geocodificação)
- `enderecos_cnefe_residencial`: COD_ESPECIE = 1 (domicílios particulares apenas)
- `enderecos_cnefe_naoresidencial`: COD_ESPECIE ∈ {4,5,6,8} (escolas, saúde, outras
  finalidades, religiosos)

Coluna adicional `qualidade_geo` em todas as tabelas: `alta`/`media`/`baixa` baseada em
`NV_GEO_COORD`. Permite filtragem posterior quando a covariável depende de localização fina.

Domicílios coletivos (espécie 2) e construções (espécie 7) ficam apenas na tabela bruta.

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

### Grupo 5 — PNADc Anual Visita 1 + cálculo do déficit habitacional FJP

> ⚠️ **Limitação fundamental:** A PNADc não tem representatividade municipal. A menor unidade geográfica publicada é a **área de ponderação**. As estimativas geradas neste grupo valem para a área de ponderação inteira, não para o município isoladamente. Esse aviso deve constar nos metadados.

#### 5.1 — Por que PNADc Anual Visita 1 (e não Trimestral)

A PNADc visita cada domicílio 5 vezes em trimestres consecutivos. As características do domicílio (paredes, piso, água, esgoto, condição de ocupação, valor de aluguel) só são coletadas na **1ª visita**. As variáveis com prefixo `S01XXX` (módulo de habitação) só existem no produto da Anual V1, não no produto Trimestral. A FJP usa exclusivamente esse produto desde a metodologia 2021.

Em 2020 e 2021 não houve divulgação da Anual V1 por causa da pandemia (coleta apenas por telefone). A série tem hiato 2019 → 2022.

#### 5.2 — Método de acesso adotado

**Fluxo:** download direto do FTP IBGE em Python puro (sem R), seguindo o padrão dos demais grupos.

```
1. Baixar dicionário Anual V1 do ano alvo (XLS) → identificar posições e códigos
2. Baixar microdado fixed-width (TXT) do ano alvo
3. Ler com pandas.read_fwf usando widths do input SAS
4. Filtrar pela área de ponderação (V1029) que contém o município
5. Aplicar regras FJP 2021 (cascata de 3 componentes)
6. Estimar com plano amostral usando V1032 e replicações V1032001-V1032200
7. Persistir resultados no DuckDB
```

Justificativa de não usar R: SAC do Windows 11 bloqueava DLLs do R (decisão 2026-04-21 preservada). Plano amostral implementado via método de replicações Rao-Wu (Bootstrap RW).

#### 5.3 — Variáveis a coletar da PNADc Anual V1

**Atenção:** os códigos S01XXX podem variar ano a ano. O agente DEVE baixar o dicionário do ano alvo e confirmar cada código antes de implementar a leitura. A tabela abaixo é referência preliminar — todos os códigos S01XXX precisam validação no dicionário efetivamente baixado.

##### 5.3.1 Identificação e plano amostral (padrão estável entre anos)

| Código | Descrição | Uso |
|---|---|---|
| `Ano` | Ano de referência | Filtro |
| `UF` | UF (string 2 dígitos) | Filtro inicial |
| `UPA` | Unidade Primária de Amostragem | Plano amostral |
| `Estrato` | Estrato amostral | Plano amostral |
| `V1008` | Nº de seleção do domicílio | Chave de domicílio |
| `V1014` | Painel | Chave de domicílio |
| `V1022` | Tipo de situação (1=urbano, 2=rural) | Filtro de ônus excessivo |
| `V1023` | Tipo de área (capital, RM, RIDE, resto) | Estratificação |
| `V1029` | Código da área de ponderação | **Filtro geográfico crítico** |
| `V1032` | **Peso COM calibração** | **Peso principal de domicílio** |
| `V1032001`...`V1032200` | Pesos replicados (Rao-Wu) | Erros padrão / IC 95% |

##### 5.3.2 Pessoas e composição familiar

| Código | Descrição | Uso na FJP |
|---|---|---|
| `V2001` | Nº de pessoas no domicílio | Adensamento, per capita |
| `V2005` | Condição da pessoa no domicílio | Núcleos familiares |
| `V2007` | Sexo | Recortes |
| `V2009` | Idade na data de referência | Recortes |
| `V2010` | Cor ou raça | Recortes |

##### 5.3.3 Variáveis derivadas (módulo VDXXXX)

| Código | Descrição | Uso na FJP |
|---|---|---|
| `VD2002` | Condição na unidade doméstica | **Identifica núcleos secundários** |
| `VD2004` | Espécie da unidade doméstica (unipessoal, nuclear, extensa, composta) | **Crítica — coabitação** |
| `VD5007` | Renda habitual domiciliar (todas as fontes) | **Crítica — ônus excessivo, recortes de renda** |
| `VD5008` | Renda habitual domiciliar per capita | Recortes de pobreza |

##### 5.3.4 Características do domicílio (módulo S01XXX) — confirmar códigos no dicionário do ano

| Código de referência | Descrição esperada | Componente FJP |
|---|---|---|
| `S01001` | Tipo do domicílio (casa, apto, cômodo/cortiço, etc.) | Habitação Precária + Coabitação |
| `S01002` | Material predominante das paredes externas | Habitação Precária (rústicos) |
| `S01007` | Nº total de cômodos | Adensamento (inadequação) |
| `S01008` | Nº de cômodos servindo de dormitório | **Coabitação por adensamento** |
| `S01032` | Condição de ocupação (próprio, alugado, cedido) | **Crítica — ônus excessivo** |
| `S01033` | Valor mensal do aluguel | **Crítica — ônus excessivo** |

> 🔬 **Tarefa do agente antes de implementar a leitura**: baixar o dicionário do ano alvo, filtrar linhas com prefixo `S01` e gerar tabela mapeamento código → descrição → posição no fixed-width. Salvar em `data/raw/{municipio}/pnadc/dicionario_S01_{ano}.csv` para auditoria.

#### 5.4 — Lógica de cálculo do déficit habitacional FJP (cascata, 3 componentes)

Cada domicílio é avaliado em sequência. Se entrar em um componente, **não é mais avaliado nos seguintes** (anti-dupla-contagem).

```
PARA cada domicílio i com peso V1032[i]:

  COMPONENTE 1 — HABITAÇÃO PRECÁRIA
    flag_rustico: S01002 indica material não durável das paredes externas
    (taipa não revestida, madeira aproveitada, palha, outro)
    SE flag_rustico → componente = "habitacao_precaria" → próximo domicílio

    Domicílios improvisados: FORA DO ESCOPO (não vem da PNADc; vem do CadÚnico)
    → documentar nos metadados como subcomponente externo

  COMPONENTE 2 — COABITAÇÃO FAMILIAR
    flag_comodo: S01001 = cômodo/cortiço
    flag_coabit_adens: VD2004 ∈ {extensa, composta} E V2001/S01008 > 2
    SE flag_comodo OU flag_coabit_adens → componente = "coabitacao" → próximo domicílio

  COMPONENTE 3 — ÔNUS EXCESSIVO COM ALUGUEL URBANO
    Todos verdadeiros simultaneamente:
        V1022 = 1                        (urbano)
        S01001 indica domicílio durável  (casa OU apto, NÃO cômodo)
        flag_rustico = 0
        S01032 = código de "alugado"
        VD5007 / SM_VIGENTE ≤ 3          (renda ≤ 3 SM)
        S01033 / VD5007 ≥ 0,30           (gasta ≥30% com aluguel)
    SE todos verdadeiros → componente = "onus_excessivo"

  CASO CONTRÁRIO → componente = "nao_deficit"
```

#### 5.5 — Estimação com plano amostral (Rao-Wu Bootstrap)

```python
# Estimativa pontual usando V1032
total_deficit = sum(df.loc[df["componente"] != "nao_deficit", "V1032"])

# Erro padrão usando 200 réplicas
rep_totals = [
    sum(df.loc[df["componente"] != "nao_deficit", f"V1032{r:03d}"])
    for r in range(1, 201)
]

import numpy as np
var_estimada = (1/200) * sum((np.array(rep_totals) - total_deficit)**2)
erro_padrao  = np.sqrt(var_estimada)
ic_95_inf    = total_deficit - 1.96 * erro_padrao
ic_95_sup    = total_deficit + 1.96 * erro_padrao
cv           = erro_padrao / total_deficit  # suprimir se CV > 0.30
```

#### 5.6 — Tabelas DuckDB geradas pelo Grupo 5

| Tabela | Conteúdo |
|---|---|
| `pnadc_microdados_v1` | Microdados filtrados pela área de ponderação + coluna `componente` FJP |
| `pnadc_deficit_componentes` | Total estimado e erro padrão por componente (precária, coabitação, ônus, total) |
| `pnadc_deficit_recortes` | Estimativas por recortes (sexo do responsável, cor/raça, faixa de renda) |
| `pnadc_metadados` | Ano, área de ponderação, V1029 usado, variáveis ausentes, avisos, hash do dicionário |

#### 5.7 — Assinatura da função principal

```python
def coletar_grupo5(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn,
    ano: int = 2022,
    salario_minimo: float | None = None,  # None = usar default {2022: 1212.00, 2023: 1320.00}
    forcar: bool = False,
    **kwargs,
) -> dict
```

> ⚠️ **Salário mínimo:** para anos fora do dicionário interno `{2022: 1212.00, 2023: 1320.00}`, o parâmetro `salario_minimo` é obrigatório. O orquestrador deve ser configurado com o valor correto para o ano usado.

#### 5.8 — Metadados obrigatórios do Grupo 5

```json
{
  "fonte": "IBGE PNADc Anual Visita 1",
  "ano": 2022,
  "metodologia": "FJP 2021 (Relatório Metodológico — 3 componentes do déficit em cascata)",
  "metodologia_url": "https://repositorio.fjp.mg.gov.br/items/a79c5256-7329-443b-acad-30c5e9640bd8",
  "nivel_geografico": "area_de_ponderacao",
  "v1029_usados": ["..."],
  "hash_microdados_md5": "...",
  "hash_dicionario_md5": "...",
  "variaveis_coletadas": ["..."],
  "variaveis_ausentes": [],
  "subcomponentes_externos": ["habitacao_precaria/improvisados (CadÚnico, fora deste módulo)"],
  "metodo_estimacao": "Rao-Wu Bootstrap com 200 réplicas (V1032001-V1032200)",
  "salario_minimo_referencia": {"valor": 1212.00, "ano": 2022},
  "aviso_geografico": "Estimativas válidas para a área de ponderação. NÃO representam o município isoladamente."
}
```

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

## Repositórios e fontes oficiais por grupo

> Esta seção lista as URLs canônicas de cada fonte de dados. Sempre que uma URL falhar, consultar primeiro o repositório institucional da fonte antes de assumir mudança permanente.

### Grupo 1 — Geometrias base
- **geobr (Python)**: https://github.com/ipeaGIT/geobr — limites municipais, áreas de ponderação, regiões metropolitanas
- **Setores censitários 2022 (FTP IBGE)**: https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022_preliminar/setores/gpkg/UF/
- **Grade estatística 200m**: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Grade_de_Estatisticas/

### Grupo 2 — Censo 2022 (agregados por setor)
- **Domicílio01 / Domicílio02**: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Resultados_do_Universo/Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/
- **Responsável01 (renda do responsável)**: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Resultados_do_Universo/Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/
- **Documentação técnica**: https://www.ibge.gov.br/estatisticas/sociais/populacao/22827-censo-demografico-2022.html

### Grupo 3 — Endereços e logradouros
- **CNEFE 2022**: https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Coordenadas_enderecos/UF/
- **Faces de logradouros 2022**: https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/malha_de_setores_censitarios/censo_2022/base_de_faces_de_logradouros_versao_2022_censo_demografico/shp/
- **OSMnx**: https://github.com/gboeing/osmnx (consulta Overpass API)

### Grupo 4 — Luminosidade noturna VIIRS
- **EOG / Earth Observation Group**: https://eogdata.mines.edu/products/vnl/
- **VNL V2.2 anual**: https://eogdata.mines.edu/nighttime_light/annual/v22/
- **Documentação do produto**: https://eogdata.mines.edu/products/vnl/#annual_v2
- **Token EOG**: registro gratuito em https://eogdata.mines.edu/products/register/

### Grupo 5 — PNADc Anual Visita 1
- **Microdados (FTP IBGE)**: https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Anual/Microdados/Visita/Visita_1/
- **Dicionário e SAS input**: pasta `Documentacao/` no FTP acima — arquivo `dicionario_PNADC_microdados_{ano}_visita1_*.xls`
- **Página oficial PNADc**: https://www.ibge.gov.br/estatisticas/sociais/trabalho/17270-pnad-continua.html

### Metodologia FJP — referência canônica
- **Página de divulgação**: https://fjp.mg.gov.br/deficit-habitacional-no-brasil/
- **Relatório Metodológico 2021** (definições + fórmulas atuais): https://repositorio.fjp.mg.gov.br/items/a79c5256-7329-443b-acad-30c5e9640bd8
- **Nota Técnica FJP 1/2024 — "As voltas que o ônus dá"** (alternativas para ônus em escala municipal — diretamente relevante para a tese): http://www.bibliotecadigital.mg.gov.br/consulta/consultaDetalheDocumento.php?iCodDocumento=77618

### Grupo 6 — Extensões (futuras)
- **CadÚnico (microdados)**: depende de convênio institucional via Ministério do Desenvolvimento Social
- **CNES**: https://datasus.saude.gov.br/cnes/ (API + FTP)
- **INEP — Catálogo de Escolas**: https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/censo-escolar
- **MapBiomas**: https://brasil.mapbiomas.org/ (Coleção 9, GEE ou downloads por bbox)
- **Aglomerados Subnormais 2022 (IBGE)**: https://www.ibge.gov.br/geociencias/organizacao-do-territorio/tipologias-do-territorio/15788-aglomerados-subnormais.html

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
