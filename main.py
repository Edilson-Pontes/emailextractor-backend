import asyncio
import io
import re
import uuid
from typing import Dict, List

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# Armazenamento simples em memória
JOBS: Dict[str, Dict] = {}


def extract_domain(raw: str) -> str:
    """Normaliza a entrada (com ou sem http/https, com ou sem caminho)
    para obter apenas o dominio, ex.: 'https://www.exemplo.com.br/contato'
    -> 'www.exemplo.com.br'.
    """
    url = (raw or "").strip()
    if not url:
        return ""

    # Remove esquema se existir
    if url.startswith("http://"):
        url = url[len("http://") :]
    elif url.startswith("https://"):
        url = url[len("https://") :]

    # Mantem apenas host (ate a primeira barra ou ?)
    for sep in ["/", "?"]:
        if sep in url:
            url = url.split(sep, 1)[0]
    return url


def get_domain_for_whois(raw: str) -> str:
    """Retorna apenas o domínio a ser pesquisado no WHOIS/RDAP do registro.br."""
    return extract_domain(raw)


async def fetch_emails_for_domain(client: httpx.AsyncClient, raw_url: str, timeout: float = 15.0) -> List[str]:
    """Consulta o RDAP do registro.br (JSON) para obter os emails de contato.

    Exemplo de endpoint: https://rdap.registro.br/domain/exemplo.com.br
    """
    domain = get_domain_for_whois(raw_url)
    if not domain:
        return []

    # Pequeno mecanismo de retry para evitar falhas transitórias do serviço RDAP
    last_resp_text = ""
    for attempt in range(3):
        try:
            resp = await client.get(
                f"https://rdap.registro.br/domain/{domain}",
                timeout=timeout,
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    )
                },
            )
            last_resp_text = resp.text
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception:
            if attempt == 2:
                return []
            # espera pequena antes de tentar de novo
            await asyncio.sleep(0.5 * (attempt + 1))

    emails: set[str] = set()

    # 1) RDAP "bonitinho": emails em entities -> vcardArray
    try:
        entities = data.get("entities", [])
        for ent in entities:
            vcard = ent.get("vcardArray")
            if not isinstance(vcard, list) or len(vcard) < 2:
                continue
            for item in vcard[1]:
                # item: ["email", {"type": "work"}, "text", "email@exemplo.com"]
                if not isinstance(item, list) or len(item) < 4:
                    continue
                if item[0] == "email":
                    value = item[3]
                    if isinstance(value, str):
                        emails.update(EMAIL_REGEX.findall(value))
    except Exception:
        # se der algum problema de formato, apenas ignora essa parte
        pass

    # 2) Fallback agressivo: aplica regex em todo o JSON serializado
    try:
        emails.update(EMAIL_REGEX.findall(last_resp_text or ""))
    except Exception:
        pass

    # 3) Se ainda não achamos nada, tentar raspar a página HTML do WHOIS
    if not emails:
        try:
            html_resp = await client.get(
                "https://registro.br/tecnologia/ferramentas/whois",
                params={"search": domain},
                timeout=timeout,
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    )
                },
            )
            html_resp.raise_for_status()
            html_text = html_resp.text

            try:
                soup = BeautifulSoup(html_text, "html.parser")

                # Prioriza tabela de Contato (ID): linha com rotulo Email
                for row in soup.find_all("tr"):
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 2:
                        continue
                    label = cells[0].get_text(strip=True).upper()
                    if label == "EMAIL":
                        value_text = cells[1].get_text(separator=" ", strip=True)
                        if value_text:
                            emails.update(EMAIL_REGEX.findall(value_text))

                # Tambem considera <td class="cell-emails">...</td>
                for td in soup.select("td.cell-emails"):
                    cell_text = td.get_text(separator=" ", strip=True)
                    if cell_text:
                        emails.update(EMAIL_REGEX.findall(cell_text))

                # Fallback final no HTML inteiro
                if not emails:
                    emails.update(EMAIL_REGEX.findall(html_text or ""))
            except Exception:
                # Se der erro no parser, apenas ignora o HTML
                pass
        except Exception:
            # Se nao conseguirmos nem carregar a pagina HTML, seguimos sem email
            pass

    return sorted(emails)


async def process_job(job_id: str) -> None:
    job = JOBS[job_id]
    urls: List[str] = job["urls"]

    job["status"] = "processing"
    job["total_urls"] = len(urls)
    job["processed_urls"] = 0
    job["extracted_emails"] = 0
    job["results"] = []

    # Concorrência moderada para não sobrecarregar o serviço do registro.br
    concurrency = 10
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(limits=limits) as client:
        sem = asyncio.Semaphore(concurrency)

        async def handle_url(url: str) -> None:
            # Verifica se o job foi cancelado antes de processar a URL
            if job.get("status") == "cancelled":
                return
            async with sem:
                emails = await fetch_emails_for_domain(client, url)
                job["results"].append({"url": url, "emails": emails})
                job["processed_urls"] += 1
                job["extracted_emails"] += len(emails)

        tasks = [handle_url(u) for u in urls]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            if job.get("status") != "cancelled":
                job["status"] = "completed"
        except Exception as e:
            if job.get("status") != "cancelled":
                job["status"] = "failed"
                job["error"] = str(e)


@app.post("/api/extract")
async def extract_emails(file: UploadFile = File(...)):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Arquivo deve ser .xlsx")

    content = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(content))
    except Exception:
        raise HTTPException(status_code=400, detail="Não foi possível ler o Excel")

    if df.empty or df.shape[1] < 1:
        raise HTTPException(status_code=400, detail="Excel sem URLs na Coluna A")

    urls = df.iloc[:, 0].dropna().astype(str).tolist()
    if not urls:
        raise HTTPException(status_code=400, detail="Nenhuma URL encontrada na Coluna A")

    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "status": "pending",
        "urls": urls,
        "total_urls": len(urls),
        "processed_urls": 0,
        "extracted_emails": 0,
        "results": [],
        "error": None,
        "cancelled": False,
        "parent_job_id": None,
    }

    JOBS[job_id]["task"] = asyncio.create_task(process_job(job_id))

    return {"jobId": job_id}


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    return {
        "status": job["status"],
        "total_urls": job.get("total_urls", 0),
        "processed_urls": job.get("processed_urls", 0),
        "extracted_emails": job.get("extracted_emails", 0),
    }


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job["status"] in {"completed", "failed", "cancelled"}:
        raise HTTPException(status_code=400, detail="Job já finalizado")

    job["status"] = "cancelled"
    job["cancelled"] = True

    task = job.get("task")
    if isinstance(task, asyncio.Task):
        task.cancel()

    return {"status": "cancelled"}


@app.post("/api/jobs/{job_id}/complement")
async def complement_job(job_id: str):
    """Complementa um job existente processando repetidamente apenas as URLs
    que ainda não possuem email, até esgotar ou o usuário cancelar.

    É disparado quando o usuário clica em "Nova Extração".
    """
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    # Só permite complementar jobs que já finalizaram uma rodada
    if job["status"] not in {"completed", "failed"}:
        raise HTTPException(status_code=400, detail="Job ainda em processamento")

    # Vamos reutilizar o mesmo job, apenas enriquecendo os resultados
    job["status"] = "processing"

    # Loop: enquanto existirem domínios sem email e o usuário não cancelar
    while True:
        if job.get("status") == "cancelled":
            break

        missing_urls: List[str] = []
        for item in job.get("results", []):
            emails_list = item.get("emails") or []
            if not emails_list:
                missing_urls.append(item["url"])

        if not missing_urls:
            # Nada mais a complementar
            break

        # Atualiza total de URLs desta rodada de complemento (apenas informativo)
        job["total_urls"] = len(missing_urls)
        job["processed_urls"] = 0

        # Usa um cliente HTTP dedicado para esta rodada, bem mais lento/gentil
        # Concorrência MUITO baixa (1 por vez) e pausas longas para o WHOIS/RDAP
        concurrency = 1
        limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
        async with httpx.AsyncClient(limits=limits) as client:
            sem = asyncio.Semaphore(concurrency)

            async def handle_complement_url(url: str) -> None:
                if job.get("status") == "cancelled":
                    return
                async with sem:
                    # Espera ~3 segundos ANTES de consultar este dominio,
                    # para garantir um ritmo bem lento de chamadas
                    await asyncio.sleep(3.0)

                    # Consulta lenta: timeout maior dentro do fetch
                    emails = await fetch_emails_for_domain(client, url, timeout=20.0)

                    # Atualiza o registro correspondente no array de resultados
                    for item in job["results"]:
                        if item["url"] == url:
                            item["emails"] = emails
                            break

                    job["processed_urls"] += 1
                    if emails:
                        job["extracted_emails"] += 1

            tasks = [handle_complement_url(u) for u in missing_urls]
            await asyncio.gather(*tasks, return_exceptions=True)

        # Pausa entre rodadas completas de complemento
        await asyncio.sleep(1.0)

        # Após esta rodada, o loop volta ao início para ver se ainda há pendentes

    if job.get("status") != "cancelled":
        job["status"] = "completed"

    # Mantemos o mesmo jobId; o frontend continua acompanhando este job
    return {"jobId": job_id}


@app.get("/api/jobs/{job_id}/download")
async def download_job_result(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    # Se este job é um complemento, mescla resultados com o job pai
    parent_job_id = job.get("parent_job_id")
    if parent_job_id:
        parent = JOBS.get(parent_job_id)
        if not parent:
            raise HTTPException(status_code=404, detail="Job pai não encontrado")

        if job["status"] != "completed":
            raise HTTPException(status_code=400, detail="Job ainda não concluído")

        # Mapa URL -> lista de emails, começando pelos resultados originais
        email_map = {}
        for item in parent.get("results", []):
            email_map[item["url"]] = item.get("emails") or []

        # Resultados complementares sobrescrevem os originais (caso existam)
        for item in job.get("results", []):
            email_map[item["url"]] = item.get("emails") or []

        rows = []
        for url in parent.get("urls", []):
            emails_list = email_map.get(url) or []
            first_email = emails_list[0] if emails_list else ""
            rows.append({"URL": url, "Emails": first_email})
    else:
        if job["status"] != "completed":
            raise HTTPException(status_code=400, detail="Job ainda não concluído")

        rows = []
        for item in job["results"]:
            url = item["url"]
            emails_list = item.get("emails") or []
            first_email = emails_list[0] if emails_list else ""
            rows.append({"URL": url, "Emails": first_email})

    df = pd.DataFrame(rows)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Emails")

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="emails_{job_id}.xlsx"'
        },
    )
