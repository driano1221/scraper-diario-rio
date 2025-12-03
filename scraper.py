import asyncio
import shutil
import aiohttp
import aiofiles
import fitz  # PyMuPDF
import json
from pathlib import Path
from tqdm.asyncio import tqdm
import nest_asyncio

nest_asyncio.apply()

# --- CONFIGURAÇÕES ---
BASE_URL = "https://doweb.rio.rj.gov.br"
OUTPUT_DIR = Path("Diarios_Rio_PDF")
TEMP_DIR = OUTPUT_DIR / "temp_chunks"
HISTORY_FILE = Path("historico.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
MAX_CONCURRENT = 30
RETRIES = 3

# --- GESTÃO DE HISTÓRICO (MEMÓRIA DO ROBÔ) ---
def carregar_historico():
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, "r") as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def salvar_historico(novos_ids):
    historico_atual = carregar_historico()
    historico_atual.update(novos_ids)
    # Salva lista ordenada
    with open(HISTORY_FILE, "w") as f:
        json.dump(sorted(list(historico_atual)), f, indent=4)

# --- FUNÇÕES DE REDE E PDF (Mesmas da versão anterior otimizada) ---
async def fetch_with_retry(session, url, method="GET"):
    for attempt in range(RETRIES):
        try:
            async with session.request(method, url, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                if resp.status == 200:
                    return True if method == "HEAD" else await resp.read()
                elif resp.status == 404:
                    return None
                if resp.status >= 500: await asyncio.sleep(0.5)
        except: await asyncio.sleep(0.5)
    return None

async def get_page_count(session, ed_id):
    if not await fetch_with_retry(session, f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{ed_id}/1", "HEAD"):
        return 0
    low, high, last = 1, 3000, 0
    while low <= high:
        mid = (low + high) // 2
        if await fetch_with_retry(session, f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{ed_id}/{mid}", "HEAD"):
            last, low = mid, mid + 1
        else: high = mid - 1
    return last

async def download_page(session, sem, ed_id, page, dest):
    url = f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{ed_id}/{page}"
    async with sem:
        content = await fetch_with_retry(session, url)
        if content and content.startswith(b'%PDF'):
            async with aiofiles.open(dest, 'wb') as f: await f.write(content)
            return True
    return False

def fast_merge_pdfs(pdf_list, output_path):
    doc = fitz.open()
    for p in pdf_list:
        try:
            with fitz.open(p) as temp: doc.insert_pdf(temp)
        except: pass
    doc.save(output_path, garbage=0, deflate=False)
    doc.close()

async def process_edition(session, ed_id, sem, executor):
    print(f"📥 Baixando Edição {ed_id}...", end=" ")
    total = await get_page_count(session, ed_id)
    if not total:
        print("(Vazia)")
        return False

    temp_path = TEMP_DIR / str(ed_id)
    temp_path.mkdir(parents=True, exist_ok=True)
    
    tasks = [download_page(session, sem, ed_id, p, temp_path / f"{p:04d}.pdf") for p in range(1, total + 1)]
    results = [await t for t in tqdm(asyncio.as_completed(tasks), total=total, desc="Páginas", leave=False)]

    if not any(results):
        shutil.rmtree(temp_path, ignore_errors=True)
        return False

    final_pdf = OUTPUT_DIR / f"DO_Rio_{ed_id}.pdf"
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(executor, fast_merge_pdfs, sorted(temp_path.glob("*.pdf"), key=lambda x: int(x.stem)), final_pdf)
    
    shutil.rmtree(temp_path, ignore_errors=True)
    print("✅ OK")
    return True

# --- FLUXO PRINCIPAL ---
async def find_latest_binary(session):
    low, high, last = 7500, 9000, 0
    print(f"📡 Buscando última edição (Binária {low}-{high})...")
    while low <= high:
        mid = (low + high) // 2
        if await fetch_with_retry(session, f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{mid}/1", "HEAD"):
            last, low = mid, mid + 1
        else: high = mid - 1
    return last

async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    if TEMP_DIR.exists(): shutil.rmtree(TEMP_DIR)
    
    # Carrega memória
    ja_baixados = carregar_historico()
    print(f"💾 Histórico carregado: {len(ja_baixados)} edições já salvas.")

    async with aiohttp.ClientSession(headers=HEADERS, connector=aiohttp.TCPConnector(limit=0)) as session:
        latest = await find_latest_binary(session)
        if not latest: return

        # Define alvo: últimas 10 edições que NÃO estão no histórico
        target_ids = []
        curr = latest
        count = 0
        while count < 10 and curr > 6000:
            if curr not in ja_baixados:
                target_ids.append(curr)
                count += 1
            curr -= 1
        
        if not target_ids:
            print("✨ Nenhuma edição nova para baixar hoje.")
            return

        print(f"🎯 Alvo: {target_ids}")
        sem = asyncio.Semaphore(MAX_CONCURRENT)
        sucessos = []

        for ed_id in target_ids:
            # Verifica existência antes de baixar
            if await fetch_with_retry(session, f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{ed_id}/1", "HEAD"):
                if await process_edition(session, ed_id, sem, None):
                    sucessos.append(ed_id)
        
        # Salva o que conseguiu baixar no histórico
        if sucessos:
            salvar_historico(sucessos)
            print(f"💾 Histórico atualizado com: {sucessos}")

    if TEMP_DIR.exists(): shutil.rmtree(TEMP_DIR)

if __name__ == "__main__":
    asyncio.run(main())
