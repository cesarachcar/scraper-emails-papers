import logging
import sys
import re
import asyncio
from datetime import datetime
from urllib.parse import urlparse
from OpenSSL import SSL
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.http_clients import ImpitHttpClient
from crawlee import Request
from otel_setup import tracer, pages_scraped_counter, emails_extracted_counter, pdf_processing_histogram, request_duration_histogram
import ssl
import pandas as pd
import certifi
import time
import settings
import pymupdf as fitz
import json


# --- Logging Config ---
logging.basicConfig(level=logging.INFO, filename=f"{__file__}.log", filemode="w",
                    format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# --- Logging Config Crawlee ---
crawlee_logger = logging.getLogger("crawlee")
crawlee_logger.propagate = True

class FormatadoCrawler:
    def __init__(self):
        self.email_registro_api = settings.EMAIL_REGISTRO_API
        self.regex_email = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

        # runtime state
        self.csv_writer = None
        self.csv_file = None
        self.csv_lock = asyncio.Lock()  # protege escrita concorrente

    # ---------------- utilidades ----------------
    def _processar_pdf(self, pdf_bytes: bytes, ordem_doi: int):
        """Processa o PDF: varre o texto procurando por emails"""
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        emails = []
        emails_encontrados = 0
        for page in doc:
            text = page.get_text() or ""
            for email in re.findall(self.regex_email, text):
                emails.append(email)
                emails_encontrados += 1
        logging.info(f"Extração PDF concluída - {emails_encontrados} e-mails (DOI {ordem_doi})")
        return emails

    async def _escrever_emails_csv(self, emails, doi):
        # escrita thread-safe
        async with self.csv_lock:
            for email in emails:
                self.csv_writer_pdf.writerow([email, doi])
            self.csv_file_pdf.flush()

    # ---------------- handlers do crawler ----------------
    async def handle_unpaywall(self, ctx: HttpCrawlingContext):
        """
        1) Recebe a resposta JSON da Unpaywall
        2) Filtra Elsevier
        3) Enfileira a URL do PDF (label='pdf') se existir
        """
        
        doi = ctx.request.user_data["doi"]
        ordem_doi = ctx.request.user_data["ordem_doi"]
        resp = ctx.http_response
        print(f'processando {ctx.request.url}')

        t0 = time.perf_counter()
        
        if resp.status_code == 404:
            ctx.log.info(f"[DOI {ordem_doi}] Página Unpaywall Vazia | DOI: {doi}")
            return
        
        try:
            raw = await resp.read()
            data = json.loads(raw)
        except Exception as e:
            ctx.log.info(f"[DOI {ordem_doi}] Falha ao decodificar JSON | DOI: {doi} | Erro: {e}")
            return
        
        best_loc = data.get("best_oa_location") or {}
        url_artigo = best_loc.get("url_for_pdf") or best_loc.get("url") or data.get("doi_url")
        if not url_artigo:
            ctx.log.info(f"[DOI {ordem_doi}] Nenhum PDF disponível | DOI: {doi}")
            return
        
        publisher = (data.get("publisher") or "")
        if "Elsevier" in publisher:
            async with self.csv_lock:
                self.csv_writer_elsevier.writerow([url_artigo, doi])
                self.csv_file_elsevier.flush()
            ctx.log.info(f"[DOI {ordem_doi}] URL Elsevier coletada | {url_artigo}")
            return
        
        else:
            await ctx.add_requests([
                Request.from_url(
                    url=url_artigo,
                    label="pdf",
                    user_data={"doi": doi, "ordem_doi": ordem_doi},
                )
            ], forefront=True)
            t1 = time.perf_counter()
            ctx.log.info(f"[DOI {ordem_doi}] Enfileirado PDF | {url_artigo} | Prep: {t1 - t0:.2f}s")
            return


    async def handle_pdf(self, ctx: HttpCrawlingContext):
        """Faz download do PDF e extrai emails."""
        doi = ctx.request.user_data["doi"]
        ordem_doi = ctx.request.user_data["ordem_doi"]

        inicio_total = time.perf_counter()

        emails_encontrados = 0
        status = None
        resp = ctx.http_response
        body = await resp.read()
        ct = (resp.headers.get("Content-Type") or "").lower()


        try:
            is_pdf = ("pdf" in ct) or (len(body) >= 5 and body[:5] == b"%PDF-") # verificação simples de PDF
            if resp.status_code == 200 and is_pdf:
                t0 = time.perf_counter()
                emails = self._processar_pdf(body, ordem_doi)
                t1 = time.perf_counter()

                emails_encontrados = len(emails)
                await self._escrever_emails_csv(emails, doi)
                status = "pdf normal"

                emails_extracted_counter.add(emails_encontrados, {"status": "ok"})
                pdf_processing_histogram.record(t1 - t0, {"status": "ok"})
            elif resp.status_code == 404:
                status = "página vazia"
                ctx.log.info(f'ERRO | [DOI {ordem_doi}] | Página vazia | {ctx.request.url}')
            elif resp.status_code == 403:
                status = f"requisição {resp.status_code}"
                ctx.log.info(f'ERRO | [DOI {ordem_doi}] | Requisição falhou | {ctx.request.url}')
            elif resp.status_code == 200:
                status = "não é pdf"
            else:
                status = f"requisição {resp.status_code}"

        except ssl.SSLCertVerificationError as e:
            # Com ImpitHttpClient isso é raro; se quiser implementar fallback custom, você pode fazê-lo aqui.
            status = "ssl verification error"

        except Exception as e:
            status = "processamento falhou"
            print(f'{e}')

        host_name = urlparse(ctx.request.url).hostname
        fim_total = time.perf_counter()
        request_duration_histogram.record(fim_total - inicio_total, {"handler": "pdf"})

        ctx.log.info(
            f'[DOI {ordem_doi}] | '
            f'Status: {status} | '
            f'Emails encontrados: {emails_encontrados or 0} | '
            f'Host: {host_name} | '
            f'Total: {fim_total - inicio_total:.2f}s | '
            f'DOI: {doi}'
        )
    
    # ---------------- orquestração ----------------
    async def main(self, caminho_planilha_doi: str, save_emails_pdf: str, save_urls_elsevier: str, limite_concorrencia: int = 10, amostra: int = 250):
        planilha_doi = pd.read_excel(caminho_planilha_doi)
        dois = planilha_doi["DOI"].sample(frac=1, random_state=42).reset_index(drop=True)

        import csv
        with open(save_emails_pdf, "w", newline="", encoding="utf-8") as f_pdf, \
             open(save_urls_elsevier, "w", newline="", encoding='utf-8') as f_elsevier:
            self.csv_file_pdf = f_pdf
            self.csv_writer_pdf = csv.writer(f_pdf)
            self.csv_writer_pdf.writerow(["emails", "doi"])
            self.csv_file_elsevier = f_elsevier
            self.csv_writer_elsevier = csv.writer(f_elsevier)
            self.csv_writer_elsevier.writerow(["urls elsevier", "doi"])

            # HTTP client com impersonation (Chrome por padrão), HTTP/3 habilitado
            http_client = ImpitHttpClient(
                browser="chrome",
                http3=True,
                verify=True,
            )

            crawler = HttpCrawler(
                http_client=http_client,
                max_request_retries=0
            )

            router = crawler.router

            @router.handler("unpaywall")
            async def _unpaywall(ctx: HttpCrawlingContext):
                await self.handle_unpaywall(ctx)

            @router.handler("pdf")
            async def _pdf(ctx: HttpCrawlingContext):
                await self.handle_pdf(ctx)

            # default handler só para log
            @router.default_handler
            async def _default(ctx: HttpCrawlingContext):
                ctx.log.info(f'Processing {ctx.request.url}')

            # Seeda a fila com as chamadas da Unpaywall
            start_requests = []
            base = settings.URL_BASE_UNPAYWALL
            email = self.email_registro_api

            for ordem_doi, doi in enumerate(dois.head(amostra), start=1):
                api_url = f"{base}{doi}?email={email}"
                start_requests.append(
                    Request.from_url(
                        url=api_url,
                        label="unpaywall",
                        user_data={"doi": doi, "ordem_doi": ordem_doi},
                    )
                )

            await crawler.run(start_requests)


# ---------------- Programa ----------------
if __name__ == "__main__":
    caminho_planilha_doi = settings.CAMINHO_PLANILHA_DOI
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_emails_pdf = fr"C:\Users\emails_coletados_pdf_{timestamp}.csv"
    save_urls_elsevier = fr"C:\Users\urls_coletadas_elsevier{timestamp}.csv"
    scrap = FormatadoCrawler()

    inicio_codigo = time.perf_counter()
    try:
        asyncio.run(scrap.main(caminho_planilha_doi, save_emails_pdf, save_urls_elsevier))
    except Exception as e:
        fim_codigo = time.perf_counter()
        logging.error(
            f'código interrompido: {e} '
            f'Tempo de código: {fim_codigo - inicio_codigo:.2f}s',
            exc_info=True
        )
    else:
        fim_codigo = time.perf_counter()
        logging.info(
            f'Código concluído '
            f'Tempo de código: {fim_codigo - inicio_codigo:.2f}s'
        )
