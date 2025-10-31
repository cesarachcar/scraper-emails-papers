import logging
import sys
import re
import asyncio
from urllib.parse import urlparse
from OpenSSL import SSL
from cryptography.hazmat.primitives import serialization
import ssl
import pandas as pd
import certifi
import PyPDF2 as pyf
import aiohttp
import socket
import time
import settings
import pymupdf as fitz

# --- Logging Config ---
logging.basicConfig(level=logging.INFO, filename=f"{__file__}.log", filemode="w",
                    format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)


class FormatadoAiohttp:
    def __init__(self):
        self.email_registro_api = settings.EMAIL_REGISTRO_API
        self.regex_email = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.headers = {
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "Accept-Language": "en-US,en;q=0.9",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
                        "Sec-Ch-Ua": "\"Chromium\";v=\"136\", \"Google Chrome\";v=\"136\", \"Not.A/Brand\";v=\"99\""
        }

    def _processar_pdf(self, pdf_bytes, ordem_doi: int):
        """Processa o PDF encontratado, varrendo o texto procurando por emails"""
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

    async def _processar_resposta(self, resp, writer, f, ordem_doi, doi, status_label):
        """Processa uma resposta HTTP, extrai e-mails se for PDF e escreve no CSV"""
        if resp.status == 200 and "pdf" in resp.headers.get("Content-Type", ""):
            pdf_bytes = await resp.read()
            inicio_processamento = time.perf_counter()
            emails = self._processar_pdf(pdf_bytes, ordem_doi)
            fim_processamento = time.perf_counter()
            for email in emails:
                writer.writerow([email, status_label, doi])
                f.flush()
            return len(emails), resp.url.host, inicio_processamento, fim_processamento, status_label
        elif resp.status == 404:
            logging.info(f'Página vazia (DOI {doi})')
            return 0, None, None, None, 'página vazia'
        else:
            logging.info(f'Não é PDF ou status inválido: {resp.status} DOI: {doi}')
            return 0, resp.url.host, None, None, "não é pdf" if resp.status == 200 else "inválido"
    
    async def _tratar_ssl_error(self, session, url, ordem_doi, writer, doi):
        """Tenta baixar a cadeia de certificados e refazer o download"""
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or 443
        output_pem = f'arquivos_pem/fullchain_{host}.pem'
        
        try:
            ctx = SSL.Context(SSL.TLS_CLIENT_METHOD) # cria conexão crua com OpenSSL
            sock = socket.create_connection((host, port))
            conn = SSL.Connection(ctx, sock)
            conn.set_tlsext_host_name(host.encode())
            conn.set_connect_state()
            conn.do_handshake()
            chain = conn.get_peer_cert_chain() # pega cadeia de certificados
            with open(output_pem, "wb") as f:
                for cert in chain:
                    f.write(cert.to_cryptography().public_bytes(serialization.Encoding.PEM))
            conn.close()
            sock.close()
            fallback_ssl = ssl.create_default_context(cafile=output_pem) # cria novo contexto SSL com o PEM baixado
            async with session.get(url, ssl=fallback_ssl, timeout=30, headers=self.headers) as resp2:
                return await self._processar_resposta(resp2, writer, f, ordem_doi, doi, "pdf fallback")
            
        except Exception as e2:
            logging.error(f'Fallback SSL falhou para DOI {ordem_doi}: {e2}', exc_info=True)
            return 0, None, None, None, "fallback falhou"

    async def _extracao_pdf(self, session, url: str, ordem_doi: int, writer, f, doi):
        """[Após url do artigo já ser adquirida] Baixa PDF com aiohttp e extrai e-mails em streaming"""
        inicio_total = time.perf_counter()
        inicio_processamento, fim_processamento, inicio_download, fim_download, emails_encontrados, host_name = None, None, None, None, None, None # só para não dar erro (no caso de cair no except e dar problema de variável não definida)
        status = 'desconhecido'
        try:
            inicio_download = time.perf_counter()
            async with session.get(url, ssl=self.ssl_context, timeout=30, headers=self.headers) as resp:
                fim_download = time.perf_counter()
                emails_encontrados, host_name, inicio_processamento, fim_processamento, status = await self._processar_resposta(resp, writer, f, ordem_doi, doi, 'pdf normal')
            
        except ssl.SSLCertVerificationError as e:
            logging.warning(f"Falha SSL no DOI {ordem_doi}: {e} — tentando fallback...")
            # ---- Fallback: baixar cadeia de certificados e tentar de novo ----
            inicio_download = time.perf_counter()
            emails_encontrados, host_name, inicio_processamento, fim_processamento, status = await self._tratar_ssl_error(session, url, ordem_doi, writer, f, doi)
            fim_download = time.perf_counter()

        except Exception as e:
            logging.error(f"Erro processando PDF (DOI {ordem_doi}): {e}", exc_info=True)
            status = 'processamento falhou'
        fim_total = time.perf_counter()
        logging.info(
            f'[DOI {ordem_doi}] | '
            f'Status: {status} | '
            f'Emails encontrados: {emails_encontrados or 0} | ' # se não houver emails encontrados assume o valor de 0
            f'Host: {host_name} | '
            f'Download: {f'{fim_download - inicio_download:.2f}s' if fim_download and inicio_download else 'Não baixou'} | '
            f'Processamento: {f'{fim_processamento - inicio_processamento:.2f}s' if fim_processamento and inicio_processamento else 'Não processou'} | ' 
            f'Total: {fim_total - inicio_total:.2f}s | '
            f'DOI: {doi}'
            )

    async def _extracao_principal(self, session, doi: str, ordem_doi: int, writer, f):
        """Consulta API Unpaywall e coleta e-mails de PDFs"""
        try:
            api_url = f"{settings.URL_BASE_UNPAYWALL}{doi}?email={self.email_registro_api}"
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            async with session.get(api_url, ssl=ssl_context, timeout=30) as resp1:
                if resp1.status == 404:
                    logging.info(f"Página vazia para DOI {doi}")
                    return
                data = await resp1.json()
                if 'Elsevier' in data.get("publisher", ""):
                    logging.info(f'[DOI {ordem_doi}] | Elsevier | DOI: {doi}')
                    return
                best_loc = data.get("best_oa_location", {})
                url_artigo = best_loc.get("url_for_pdf") or best_loc.get("url") if best_loc is not None else None
                if url_artigo:
                    await self._extracao_pdf(session, url_artigo, ordem_doi, writer, f, doi=doi)
        except Exception as e:
            logging.error(f"Erro processando DOI {doi}: {e}", exc_info=True)

    async def main(self, caminho_planilha_doi: str, save_path: str, limite_concorrencia: int = 10):
        """Percorre planilha de DOIs com concorrência controlada"""
        planilha_doi = pd.read_excel(caminho_planilha_doi)
        dois = planilha_doi["DOI"].sample(frac=1, random_state=42).reset_index(drop=True)  # embaralha a planilha, para que as requisições não sejam sobrecarregadas em um mesmo host (quando se extrai a planilha boa parte dos hosts vêm seguidos)
        sem = asyncio.Semaphore(limite_concorrencia)

        import csv
        with open(save_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["emails", "origem", "ordem doi"])  # cabeçalho

            async with aiohttp.ClientSession() as session:
                tasks = []
                for ordem_doi, doi in enumerate(dois.head(50), start=1):
                    async def bound_task(d=doi, o=ordem_doi):
                        async with sem:
                            await self._extracao_principal(session, d, o, writer, f)
                    tasks.append(bound_task())

                await asyncio.gather(*tasks)


# ---------------- Programa ----------------
if __name__ == "__main__":
    caminho_planilha_doi = settings.CAMINHO_PLANILHA_DOI
    save_path = r"C:\Users\cesar\Downloads\emails_coletados_async.csv"
    scrap = FormatadoAiohttp()

    inicio_codigo = time.perf_counter()
    try:
        asyncio.run(scrap.main(caminho_planilha_doi, save_path))
    except Exception as e:
        fim_codigo = time.perf_counter()
        logging.error(f'código interrompido: {e}'
                      f'Tempo de código: {fim_codigo - inicio_codigo:.2f}s',
                      exc_info=True
        )
        asyncio.get_event_loop().close()
    else:
        fim_codigo = time.perf_counter()
        logging.info(f'Código concluído'
                     f'Tempo de código: {fim_codigo - inicio_codigo:.2f}s'
        )
        asyncio.get_event_loop().close()