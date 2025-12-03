import asyncio
import shutil
import aiohttp
import aiofiles
import fitz  # PyMuPDF
from pathlib import Path
from tqdm.asyncio import tqdm
import nest_asyncio

# Aplica correção para loops (Jupyter/Interactive)
nest_asyncio.apply()

# --- CONFIGURAÇÕES ---
BASE_URL = "https://doweb.rio.rj.gov.br"
OUTPUT_DIR = Path("Diarios_Rio_PDF")
TEMP_DIR = OUTPUT_DIR / "temp_chunks"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Concorrência e Limites
MAX_CONCURRENT = 30
TIMEOUT_GLOBAL = aiohttp.ClientTimeout(total=45, connect=10)
RETRIES = 3

async def fetch_with_retry(session: aiohttp.ClientSession, url: str, method="GET") -> bytes | bool | None:
    """Requisição robusta com retries."""
    for attempt in range(RETRIES):
        try:
            async with session.request(method, url, timeout=TIMEOUT_GLOBAL) as resp:
                if resp.status == 200:
                    if method == "HEAD": return True
                    return await resp.read()
                elif resp.status == 404:
                    return None
                if resp.status >= 500:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
        except Exception:
            await asyncio.sleep(0.5 * (attempt + 1))
    return None

async def check_edition_exists(session, ed_id):
    """Verifica rápido se a edição existe (HEAD na página 1)."""
    url = f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{ed_id}/1"
    return await fetch_with_retry(session, url, method="HEAD")

async def find_latest_edition_binary(session: aiohttp.ClientSession) -> int:
    """
    BUSCA BINÁRIA: Encontra a maior edição válida em O(log N).
    Muito mais rápido que busca linear.
    """
    # Definimos um intervalo seguro.
    # low: uma edição antiga que sabemos que existe
    # high: uma edição futura que sabemos que NÃO existe
    low = 7500
    high = 9000 
    last_found = 0

    print(f"📡 Iniciando Busca Binária pela última edição (Intervalo: {low}-{high})...")
    
    with tqdm(total=(high - low), bar_format="{desc}") as pbar:
        while low <= high:
            mid = (low + high) // 2
            pbar.set_description_str(f"🔎 Testando edição {mid}...")
            
            exists = await check_edition_exists(session, mid)
            
            if exists:
                last_found = mid
                low = mid + 1  # Tenta achar uma maior
            else:
                high = mid - 1 # A edição alvo é menor
                
    if last_found > 0:
        print(f"✅ Última edição encontrada: {last_found}")
    else:
        print("❌ Nenhuma edição encontrada no intervalo.")
        
    return last_found

async def get_page_count(session: aiohttp.ClientSession, ed_id: int) -> int:
    """Busca binária para descobrir total de páginas da edição."""
    # Checa pág 1
    if not await check_edition_exists(session, ed_id):
        return 0

    low, high, last = 1, 3000, 0
    while low <= high:
        mid = (low + high) // 2
        # Verifica se página 'mid' existe
        url = f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{ed_id}/{mid}"
        if await fetch_with_retry(session, url, method="HEAD"):
            last = mid
            low = mid + 1
        else:
            high = mid - 1
    return last

async def download_page(session, sem, ed_id, page, dest):
    if dest.exists(): return True
    url = f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{ed_id}/{page}"
    async with sem:
        content = await fetch_with_retry(session, url)
        if content and isinstance(content, bytes) and content.startswith(b'%PDF'):
            async with aiofiles.open(dest, 'wb') as f:
                await f.write(content)
            return True
    return False

def fast_merge_pdfs(pdf_list, output_path):
    """Merge rápido sem recompressão."""
    doc = fitz.open()
    for p in pdf_list:
        try:
            with fitz.open(p) as temp:
                doc.insert_pdf(temp)
        except Exception: pass
    doc.save(output_path, garbage=0, deflate=False)
    doc.close()

async def process_edition(session, ed_id, sem, executor):
    final_pdf = OUTPUT_DIR / f"DO_Rio_{ed_id}.pdf"
    
    if final_pdf.exists() and final_pdf.stat().st_size > 1024:
        print(f"⏩ Edição {ed_id} já existe.")
        return True

    print(f"📥 Baixando Edição {ed_id}...", end=" ")
    total = await get_page_count(session, ed_id)
    if not total:
        print("(Não encontrada/Vazia)")
        return False

    temp_path = TEMP_DIR / str(ed_id)
    temp_path.mkdir(parents=True, exist_ok=True)

    tasks = [download_page(session, sem, ed_id, p, temp_path / f"{p:04d}.pdf") for p in range(1, total + 1)]
    
    # Executa downloads
    results = [await t for t in tqdm(asyncio.as_completed(tasks), total=total, desc="Páginas", leave=False)]

    if not any(results):
        shutil.rmtree(temp_path, ignore_errors=True)
        return False

    print("Unindo...", end=" ")
    loop = asyncio.get_running_loop()
    pages = sorted(temp_path.glob("*.pdf"), key=lambda x: int(x.stem))
    await loop.run_in_executor(executor, fast_merge_pdfs, pages, final_pdf)
    
    shutil.rmtree(temp_path, ignore_errors=True)
    print("✅ OK")
    return True

async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    if TEMP_DIR.exists(): shutil.rmtree(TEMP_DIR)
    
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        # 1. Busca Binária da última edição
        latest_id = await find_latest_edition_binary(session)
        
        if not latest_id: return

        # 2. Baixa as X últimas
        count = 0
        TARGET = 10
        curr_id = latest_id
        
        print(f"\n🚀 Baixando as {TARGET} últimas edições a partir de {latest_id}...")

        while count < TARGET and curr_id > 6000:
            # Verifica rápido antes de processar
            if await check_edition_exists(session, curr_id):
                if await process_edition(session, curr_id, sem, None):
                    count += 1
            curr_id -= 1

    if TEMP_DIR.exists(): shutil.rmtree(TEMP_DIR)
    print("\n✨ Concluído.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrompido.")