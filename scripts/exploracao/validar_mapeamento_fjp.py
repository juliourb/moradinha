"""
validar_mapeamento_fjp.py

Cruza a especificação curada (mapeamento_fjp_censo2022.json) com o dicionário
local parseado (dicionario_censo2022_parsed.json) para preencher os campos
`codigo_censo` onde estão null.

Regras de match:
  - Busca OR case-insensitive das keywords_busca no campo `descricao`
  - Filtra na aba "Dicionário não PCT" por padrão (municípios gerais)
  - 1 match único → preenche automaticamente
  - 2–N matches → AMBIGUO (lista candidatos)
  - 0 matches → NAO_ENCONTRADO

NÃO inventa códigos. Marca ambíguos/não encontrados para revisão do pesquisador.

Uso:
    python scripts/exploracao/validar_mapeamento_fjp.py

Saídas:
    data/processed/dicionarios/mapeamento_fjp_censo2022_validado.json
    data/processed/dicionarios/relatorio_validacao_mapeamento.md
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
REF_JSON = ROOT / "data" / "raw" / "referencias" / "mapeamento_fjp_censo2022.json"
DIC_JSON = ROOT / "data" / "processed" / "dicionarios" / "dicionario_censo2022_parsed.json"
OUT_DIR  = ROOT / "data" / "processed" / "dicionarios"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ABA_PRINCIPAL = "Dicionário não PCT"


# ---------------------------------------------------------------------------
# Carregamento
# ---------------------------------------------------------------------------

def carregar_dicionario() -> list[dict]:
    with open(DIC_JSON, encoding="utf-8") as f:
        d = json.load(f)
    return [v for v in d["variaveis"] if v["aba"] == ABA_PRINCIPAL]


def buscar(variaveis: list[dict], keywords: list[str]) -> list[dict]:
    """Retorna todas as variáveis do dicionário cujas descrições casem com
    ALGUMA das keywords (OR, case-insensitive, substring)."""
    resultado = []
    for v in variaveis:
        desc_lower = v["descricao"].lower()
        for kw in keywords:
            if kw.lower() in desc_lower:
                resultado.append(v)
                break
    return resultado


# ---------------------------------------------------------------------------
# Traversal do JSON de especificação
# ---------------------------------------------------------------------------

Item = dict[str, Any]

def _itens_com_keywords(obj: Any, caminho: str = "") -> list[tuple[str, Item]]:
    """
    Percorre recursivamente o objeto e retorna todos os dicts que tenham
    "keywords_busca" (i.e., são itens pesquisáveis), junto com o caminho.
    """
    encontrados = []
    if isinstance(obj, dict):
        if "keywords_busca" in obj:
            encontrados.append((caminho, obj))
        else:
            for k, v in obj.items():
                encontrados.extend(_itens_com_keywords(v, f"{caminho}.{k}" if caminho else k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            encontrados.extend(_itens_com_keywords(v, f"{caminho}[{i}]"))
    return encontrados


# ---------------------------------------------------------------------------
# Validação
# ---------------------------------------------------------------------------

def validar(spec: dict, variaveis: list[dict]) -> tuple[dict, list[dict]]:
    """
    Percorre spec, resolve keywords para codigos, retorna (spec_preenchido, log_items).
    log_items: lista de dicts com resultado por item.
    """
    spec_out = copy.deepcopy(spec)
    log = []

    # Obtém referência para os mesmos objetos no spec_out
    itens = _itens_com_keywords(spec_out)

    for caminho, item in itens:
        rotulo = item.get("rotulo", caminho)
        keywords = item.get("keywords_busca", [])
        codigo_atual = item.get("codigo_censo")

        # Já preenchido: apenas validar descrição
        if codigo_atual and codigo_atual not in ("AMBIGUO", "NAO_ENCONTRADO"):
            matches = buscar(variaveis, keywords)
            codigos_match = [m["codigo"] for m in matches]
            if codigo_atual in codigos_match:
                status = "CONFIRMADO"
                # Preenche descricao_real_xlsx se for None
                if item.get("descricao_real_xlsx") is None:
                    match_exato = next(m for m in matches if m["codigo"] == codigo_atual)
                    item["descricao_real_xlsx"] = match_exato["descricao"]
                candidatos = [{"codigo": m["codigo"], "descricao": m["descricao"]} for m in matches]
            else:
                status = "CODIGO_CONFIRMADO_NAO_BATE_KEYWORDS"
                candidatos = [{"codigo": m["codigo"], "descricao": m["descricao"]} for m in matches]
            log.append({
                "caminho": caminho, "rotulo": rotulo, "status": status,
                "codigo_resolvido": codigo_atual,
                "n_matches_keywords": len(matches),
                "candidatos": candidatos[:10],
            })
            continue

        # Precisa resolver
        matches = buscar(variaveis, keywords)

        if len(matches) == 0:
            item["codigo_censo"] = "NAO_ENCONTRADO"
            status = "NAO_ENCONTRADO"
            codigo_resolvido = None
            candidatos = []

        elif len(matches) == 1:
            item["codigo_censo"] = matches[0]["codigo"]
            item["descricao_real_xlsx"] = matches[0]["descricao"]
            status = "PREENCHIDO"
            codigo_resolvido = matches[0]["codigo"]
            candidatos = [{"codigo": matches[0]["codigo"], "descricao": matches[0]["descricao"]}]

        else:
            item["codigo_censo"] = "AMBIGUO"
            status = "AMBIGUO"
            codigo_resolvido = None
            candidatos = [{"codigo": m["codigo"], "descricao": m["descricao"]} for m in matches]

        log.append({
            "caminho": caminho,
            "rotulo": rotulo,
            "status": status,
            "codigo_resolvido": codigo_resolvido,
            "n_matches_keywords": len(matches),
            "keywords_usadas": keywords,
            "candidatos": candidatos[:20],
        })

    return spec_out, log


# ---------------------------------------------------------------------------
# Relatório
# ---------------------------------------------------------------------------

def gerar_relatorio(log: list[dict]) -> str:
    total = len(log)
    confirmados  = [x for x in log if x["status"] in ("CONFIRMADO", "PREENCHIDO", "CODIGO_CONFIRMADO_NAO_BATE_KEYWORDS")]
    preenchidos  = [x for x in log if x["status"] == "PREENCHIDO"]
    ja_tinham    = [x for x in log if x["status"] in ("CONFIRMADO", "CODIGO_CONFIRMADO_NAO_BATE_KEYWORDS")]
    ambiguos     = [x for x in log if x["status"] == "AMBIGUO"]
    nao_enc      = [x for x in log if x["status"] == "NAO_ENCONTRADO"]

    linhas = [
        "# Relatório de Validação — Mapeamento FJP → Censo 2022\n",
        f"**Total de itens avaliados:** {total}  ",
        f"**Já tinham código (confirmados):** {len(ja_tinham)}  ",
        f"**Preenchidos automaticamente:** {len(preenchidos)}  ",
        f"**Ambíguos (revisão humana):** {len(ambiguos)}  ",
        f"**Não encontrados:** {len(nao_enc)}  ",
        "",
    ]

    if preenchidos:
        linhas += ["\n## Preenchidos automaticamente\n",
                   "| Rótulo | Código | Descrição real XLSX |",
                   "|---|---|---|"]
        for x in preenchidos:
            cands = x.get("candidatos", [])
            desc = cands[0]["descricao"] if cands else ""
            linhas.append(f"| {x['rotulo']} | {x['codigo_resolvido']} | {desc} |")

    if ja_tinham:
        linhas += ["\n## Já tinham código (confirmação)\n",
                   "| Rótulo | Código | Status |",
                   "|---|---|---|"]
        for x in ja_tinham:
            linhas.append(f"| {x['rotulo']} | {x['codigo_resolvido']} | {x['status']} |")

    if ambiguos:
        linhas += ["\n## Ambíguos — precisam revisão do pesquisador\n"]
        for x in ambiguos:
            linhas += [f"\n### {x['rotulo']}\n",
                       f"**Caminho:** `{x['caminho']}`  ",
                       f"**Keywords usadas:** {x.get('keywords_usadas', [])}  ",
                       f"**Candidatos ({x['n_matches_keywords']}):**\n",
                       "| Código | Descrição |",
                       "|---|---|"]
            for c in x["candidatos"]:
                desc = c["descricao"].replace("|", "\\|")
                linhas.append(f"| {c['codigo']} | {desc} |")

    if nao_enc:
        linhas += ["\n## Não encontrados — sugestão de keywords alternativas\n",
                   "| Rótulo | Caminho | Keywords tentadas |",
                   "|---|---|---|"]
        for x in nao_enc:
            kws = ", ".join(f'`{k}`' for k in x.get("keywords_usadas", []))
            linhas.append(f"| {x['rotulo']} | `{x['caminho']}` | {kws} |")

    return "\n".join(linhas)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Carregando arquivos...")
    with open(REF_JSON, encoding="utf-8") as f:
        spec = json.load(f)
    variaveis = carregar_dicionario()
    print(f"  Dicionário: {len(variaveis)} variáveis (aba '{ABA_PRINCIPAL}')")

    print("Validando mapeamento...")
    spec_out, log = validar(spec, variaveis)

    # Salva JSON validado
    out_json = OUT_DIR / "mapeamento_fjp_censo2022_validado.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(spec_out, f, ensure_ascii=False, indent=2)
    print(f"JSON validado salvo: {out_json}")

    # Gera e salva relatório
    relatorio = gerar_relatorio(log)
    out_md = OUT_DIR / "relatorio_validacao_mapeamento.md"
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(relatorio)
    print(f"Relatório salvo: {out_md}\n")

    # Imprime resumo no console
    total   = len(log)
    preench = sum(1 for x in log if x["status"] == "PREENCHIDO")
    confir  = sum(1 for x in log if x["status"] in ("CONFIRMADO", "CODIGO_CONFIRMADO_NAO_BATE_KEYWORDS"))
    ambig   = sum(1 for x in log if x["status"] == "AMBIGUO")
    nao_e   = sum(1 for x in log if x["status"] == "NAO_ENCONTRADO")

    print("=" * 60)
    print(f"  Total itens avaliados : {total}")
    print(f"  Ja tinham codigo      : {confir}")
    print(f"  Preenchidos auto      : {preench}")
    print(f"  Ambiguos              : {ambig}")
    print(f"  Nao encontrados       : {nao_e}")
    print("=" * 60)

    if ambig > 0:
        print("\nAMBIGUOS — requerem decisão do pesquisador:")
        for x in log:
            if x["status"] != "AMBIGUO":
                continue
            print(f"\n  [{x['rotulo']}] ({x['n_matches_keywords']} candidatos)")
            for c in x["candidatos"][:5]:
                print(f"    {c['codigo']} — {c['descricao'][:80]}")

    if nao_e > 0:
        print("\nNAO ENCONTRADOS:")
        for x in log:
            if x["status"] == "NAO_ENCONTRADO":
                print(f"  [{x['rotulo']}] keywords: {x.get('keywords_usadas', [])}")


if __name__ == "__main__":
    main()
