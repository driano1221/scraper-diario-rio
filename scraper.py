import os
import asyncio
import aiohttp
import aiofiles
import fitz  # PyMuPDF
from tqdm.asyncio import tqdm
import nest_asyncio
import shutil
import re
import json
from datetime import datetime

nest_asyncio.apply()

BASE_URL = "https://doweb.rio.rj.gov.br"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

MAX_CONCURRENT = 50
TIMEOUT_SECS = 60

# Arquivo para controlar edições já baixadas
CONTROLE_FILE = "edicoes_baixadas.json"

def carregar_edicoes_baixadas():
    """Carrega lista de edições já baixadas."""
    if os.path.exists(CONTROLE_FILE):
        with open(CONTROLE_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def salvar_edicoes_baixadas(edicoes):
    """Salva lista de edições baixadas."""
    with open(CONTROLE_FILE, 'w') as f:
        json.dump(sorted(list(edicoes)), f)

async def descobrir_ultima_edicao_site(session):
    """Extrai o número da última edição diretamente da página inicial."""
    print("🔍 Buscando última edição na página inicial...")
    
    try:
        async with session.get(BASE_URL) as resp:
            if resp.status == 200:
                html = await resp.text()
                
                match = re.search(r'let DADOS_ULTIMAS_EDICOES = ({.*?});', html)
                if match:
                    dados = json.loads(match.group(1))
                    if dados.get('itens') and len(dados['itens']) > 0:
                        for item in dados['itens']:
                            if item.get('suplemento') == 0:
                                ultima_edicao = int(item['id'])
                                print(f"✅ Última edição encontrada: {ultima_edicao}")
                                return ultima_edicao
                        
                        ultima_edicao = int(dados['itens'][0]['id'])
                        print(f"✅ Última edição encontrada: {ultima_edicao}")
                        return ultima_edicao
    except Exception as e:
        print(f"⚠️ Erro ao buscar página inicial: {e}")
    
    return None

async def descobrir_total_paginas(session, edicao_id):
    """Busca binária para descobrir total de páginas."""
    low, high = 1, 3000
    ultima = 0

    url_1 = f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{edicao_id}/1"
    try:
        async with session.head(url_1) as resp:
            if resp.status != 200: return 0
    except: return 0

    while low <= high:
        mid = (low + high) // 2
        url = f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{edicao_id}/{mid}"
        try:
            async with session.head(url, timeout=5) as resp:
                if resp.status == 200 and 'application/pdf' in resp.headers.get('Content-Type', ''):
                    ultima = mid
                    low = mid + 1
                else:
                    high = mid - 1
        except:
            high = mid - 1
    return ultima

async def baixar_pagina_pdf(session, sem, edicao_id, pagina, pasta_cache):
    """Baixa uma página PDF e salva no cache."""
    arquivo_pdf = f"{pasta_cache}/pag_{pagina:04d}.pdf"

    if os.path.exists(arquivo_pdf):
        return pagina, True

    url = f"{BASE_URL}/apifront/portal/edicoes/pdf_diario/{edicao_id}/{pagina}"
    
    async with sem:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    pdf_bytes = await resp.read()
                    
                    async with aiofiles.open(arquivo_pdf, 'wb') as f:
                        await f.write(pdf_bytes)
                    
                    return pagina, False
        except Exception:
            pass
            
    return pagina, None

def consolidar_pdfs(pasta_cache, total_paginas, arquivo_final):
    """Junta todos os PDFs individuais em um único arquivo PDF."""
    pdf_final = fitz.open()
    
    for p in range(1, total_paginas + 1):
        caminho_pdf = f"{pasta_cache}/pag_{p:04d}.pdf"
        
        if os.path.exists(caminho_pdf):
            try:
                pdf_temp = fitz.open(caminho_pdf)
                pdf_final.insert_pdf(pdf_temp)
                pdf_temp.close()
            except Exception as e:
                print(f"⚠️ Erro ao adicionar página {p}: {e}")
    
    pdf_final.save(arquivo_final)
    pdf_final.close()

async def processar_edicao(edicao_id):
    print(f"\n{'='*50}")
    print(f"🚀 PROCESSANDO EDIÇÃO {edicao_id}")
    
    pasta_raiz = "Diarios_Rio"
    pasta_cache = f"{pasta_raiz}/cache_{edicao_id}"
    os.makedirs(pasta_cache, exist_ok=True)
    
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, force_close=True)
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    
    async with aiohttp.ClientSession(headers=HEADERS, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECS), connector=connector) as session:
        
        print("📊 Verificando total de páginas...", end=" ")
        total = await descobrir_total_paginas(session, edicao_id)
        print(f"Total: {total} páginas")
        
        if total == 0:
            print("❌ Edição vazia ou erro de conexão.")
            return False

        print("⬇️ Iniciando download dos PDFs...")
        tasks = [
            baixar_pagina_pdf(session, sem, edicao_id, p, pasta_cache) 
            for p in range(1, total + 1)
        ]
        
        novos = 0
        cache = 0
        falhas = 0
        
        for coro in tqdm(asyncio.as_completed(tasks), total=total, unit="pág", desc="Progresso"):
            pag, status = await coro
            if status is True: cache += 1
            elif status is False: novos += 1
            else: falhas += 1

        print(f"\nResumo: {novos} baixados | {cache} cache | {falhas} falhas")

        print("📑 Gerando PDF final...")
        arquivo_final = f"{pasta_raiz}/DO_Rio_{edicao_id}_COMPLETO.pdf"
        
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, consolidar_pdfs, pasta_cache, total, arquivo_final)

        print(f"✅ SUCESSO! Arquivo: {os.path.abspath(arquivo_final)}")
        
        print("🗑️ Limpando cache...")
        shutil.rmtree(pasta_cache)
        
        return True

async def main():
    # Carrega edições já baixadas
    edicoes_baixadas = carregar_edicoes_baixadas()
    print(f"📚 Edições já baixadas: {len(edicoes_baixadas)}")
    
    connector = aiohttp.TCPConnector(limit=10, force_close=True)
    async with aiohttp.ClientSession(headers=HEADERS, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECS), connector=connector) as session:
        ultima_edicao = await descobrir_ultima_edicao_site(session)
    
    if not ultima_edicao:
        print("❌ Não foi possível determinar a última edição")
        return
    
    # Define range das 10 últimas
    primeira_edicao = max(7000, ultima_edicao - 9)
    edicoes = range(primeira_edicao, ultima_edicao + 1)
    
    # Filtra apenas edições novas
    edicoes_novas = [ed for ed in edicoes if ed not in edicoes_baixadas]
    
    if not edicoes_novas:
        print("\n✨ Nenhuma edição nova para baixar!")
        return
    
    print(f"\n📋 Total de edições novas: {len(edicoes_novas)}")
    print(f"📅 Edições: {min(edicoes_novas)} até {max(edicoes_novas)}")

    for ed in edicoes_novas:
        sucesso = await processar_edicao(ed)
        if sucesso:
            edicoes_baixadas.add(ed)
            salvar_edicoes_baixadas(edicoes_baixadas)
        
        print("⏳ Aguardando 1 segundo...")
        await asyncio.sleep(1)
    
    print(f"\n✅ CONCLUÍDO! {len(edicoes_novas)} edições novas baixadas")

if __name__ == "__main__":
    asyncio.run(main())
