"""
listar_ftp_censo.py

Lista os arquivos disponíveis no diretório FTP do IBGE para os Agregados
por Setores Censitários do Censo 2022, para verificar se existem tabelas
adicionais além das já baixadas.

Saída: data/processed/dicionarios/ftp_censo_arquivos.txt
"""

from pathlib import Path
import urllib.request
import ssl

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "processed" / "dicionarios"
OUT.mkdir(parents=True, exist_ok=True)

URLS_VERIFICAR = [
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/",
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/",
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/",
]

lines = []
for url in URLS_VERIFICAR:
    lines.append(f"\n=== {url} ===\n")
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            content = resp.read().decode("utf-8", errors="replace")
        lines.append(content[:5000])  # primeiros 5000 chars do HTML
    except Exception as e:
        lines.append(f"ERRO: {e}\n")

out_file = OUT / "ftp_censo_arquivos.txt"
with open(out_file, "w", encoding="utf-8") as f:
    f.writelines(lines)

print(f"Salvo em: {out_file}")
for l in lines:
    print(l[:2000])
