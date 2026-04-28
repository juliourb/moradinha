"""
parsear_dicionario_censo.py

Lê TODAS as abas relevantes do dicionário XLSX do Censo 2022 (Agregados por Setores
Censitários) e salva um JSON único + versão MD legível.

Uso:
    python scripts/exploracao/parsear_dicionario_censo.py

Saídas:
    data/processed/dicionarios/dicionario_censo2022_parsed.json
    data/processed/dicionarios/dicionario_censo2022_parsed.md
"""

from pathlib import Path
import json
import re
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
XLSX = ROOT / "fonte" / "dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx"
OUT_DIR = ROOT / "data" / "processed" / "dicionarios"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Abas que têm variáveis codificadas (Vxxxxx)
ABAS_VARIAVEIS = [
    "Dicionário Básico",
    "Dicionário não PCT",
    "Dicionário PCT - Indígenas",
    "Dicionário PCT - Quilombolas",
]


def _encontrar_coluna(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    """Retorna o nome da primeira coluna do df que casou com candidatos (case-insensitive)."""
    for col in df.columns:
        if col.strip().lower() in [c.lower() for c in candidatos]:
            return col
    return None


def parsear_aba(xls: pd.ExcelFile, aba: str) -> list[dict]:
    df = pd.read_excel(xls, sheet_name=aba, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    col_codigo = _encontrar_coluna(df, ["Variável", "variavel", "Variable", "Codigo", "codigo"])
    col_descricao = _encontrar_coluna(df, ["Descrição", "descricao", "Descricao", "Description"])
    col_tema = _encontrar_coluna(df, ["Tema", "tema", "Theme"])

    if col_codigo is None or col_descricao is None:
        print(f"  [AVISO] Aba '{aba}': colunas código/descrição não encontradas — pulando")
        print(f"          Colunas disponíveis: {list(df.columns)}")
        return []

    registros = []
    for _, row in df.iterrows():
        codigo = str(row[col_codigo]).strip() if pd.notna(row[col_codigo]) else ""
        descricao = str(row[col_descricao]).strip() if pd.notna(row[col_descricao]) else ""
        tema = str(row[col_tema]).strip() if col_tema and pd.notna(row[col_tema]) else ""

        # Manter só linhas com código no formato Vxxxxx ou Vxxxxxx
        if not re.match(r"^V\d{4,6}$", codigo, re.IGNORECASE):
            continue
        if not descricao:
            continue

        registros.append({
            "aba": aba,
            "codigo": codigo.upper(),
            "tema": tema,
            "descricao": descricao,
        })

    return registros


def main():
    print(f"Lendo {XLSX.name} ...")
    xls = pd.ExcelFile(XLSX)
    print(f"Abas encontradas: {xls.sheet_names}")

    todos = []
    abas_lidas = []
    for aba in ABAS_VARIAVEIS:
        if aba not in xls.sheet_names:
            print(f"  [AVISO] Aba '{aba}' não existe no arquivo — pulando")
            continue
        registros = parsear_aba(xls, aba)
        print(f"  Aba '{aba}': {len(registros)} variáveis")
        todos.extend(registros)
        abas_lidas.append(aba)

    # Checar duplicatas de código DENTRO da mesma aba (esperado em abas PCT vs não-PCT)
    codigos_nao_pct = {r["codigo"] for r in todos if r["aba"] == "Dicionário não PCT"}
    print(f"\nTotal de variáveis parseadas: {len(todos)}")
    print(f"Códigos únicos em 'não PCT': {len(codigos_nao_pct)}")

    saida = {
        "_meta": {
            "fonte": str(XLSX.relative_to(ROOT)),
            "n_variaveis_total": len(todos),
            "abas_lidas": abas_lidas,
        },
        "variaveis": todos,
    }

    # --- JSON ---
    json_path = OUT_DIR / "dicionario_censo2022_parsed.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(saida, f, ensure_ascii=False, indent=2)
    print(f"JSON salvo: {json_path}")

    # --- MD ---
    md_lines = ["# Dicionário Censo 2022 — Agregados por Setores Censitários\n"]
    md_lines.append(f"Fonte: `{XLSX.name}`\n")
    for aba in abas_lidas:
        regs = [r for r in todos if r["aba"] == aba]
        md_lines.append(f"\n## {aba} ({len(regs)} variáveis)\n")
        md_lines.append("| Código | Tema | Descrição |")
        md_lines.append("|---|---|---|")
        for r in regs:
            desc = r["descricao"].replace("|", "\\|")
            tema = r["tema"].replace("|", "\\|")
            md_lines.append(f"| {r['codigo']} | {tema} | {desc} |")

    md_path = OUT_DIR / "dicionario_censo2022_parsed.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
    print(f"MD  salvo: {md_path}")


if __name__ == "__main__":
    main()
