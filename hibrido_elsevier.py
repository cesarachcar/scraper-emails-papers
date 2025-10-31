import re
import csv
import asyncio
import pandas as pd
from datetime import timedelta
from pathlib import Path
from crawlee.browsers import BrowserPool, PlaywrightBrowserController, PlaywrightBrowserPlugin
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext, PlaywrightPreNavCrawlingContext
from crawlee import Request, ConcurrencySettings
from datetime import timedelta
from crawlee.fingerprint_suite import (
    DefaultFingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)
from camoufox import AsyncNewBrowser
from typing_extensions import override

import logging
logging.getLogger("crawlee").setLevel(logging.INFO)

class CamoufoxPlugin(PlaywrightBrowserPlugin):
    """Example browser plugin that uses Camoufox browser,
    but otherwise keeps the functionality of PlaywrightBrowserPlugin.
    """

    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        return PlaywrightBrowserController(
            browser=await AsyncNewBrowser(
                self._playwright, **self._browser_launch_options
            ),
            # Increase, if camoufox can handle it in your use case.
            max_open_pages_per_browser=1,
            # This turns off the crawlee header_generation. Camoufox has its own.
            header_generator=None,
        )


class ExtracaoElsevier:
    def __init__(self):
        self.regex = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
        self.csv_writer = None
        self.csv_file = None
        self.csv_lock = asyncio.Lock()

    # Handler padrão do router
    async def handle_elsevier(self, ctx: PlaywrightCrawlingContext):
        print('chamou a função -------------------------------------------------------------------------------')
        page = ctx.page
        #await page.wait_for_timeout(30000) # isso não está funcionando, ou seja não está nem chamando a função, ou o problema está dentro da prórpia função/contexto

        await page.screenshot(path=r"debug\debug_camoufox.png")
        await page.get_by_role("button", name="Accept all cookies").click()
        #await page.get_by_role("button", name="Close").click()

        # Localiza ícones de envelope
        side_bar_list = await page.locator("svg.icon-envelope").all()
        print(side_bar_list)
        for side_bar in side_bar_list:
            for tentativa in range(5):
                try:
                    try:    
                        await side_bar.click(timeout=3000)
                    except:
                        await page.get_by_role("button", name="Close Author panel").click(timeout=3000)
                    try: # Tenta localizar e extrair email
                        email = await page.locator(f"text=/{self.regex.pattern}/").inner_text()
                        async with self.csv_lock:
                            self.csv_writer.writerow([email, ctx.request.user_data["doi"]])
                            self.csv_file.flush()
                        print("Email encontrado:", email)
                    except Exception:
                        print("Nenhum email encontrado nesse painel.")
                    await page.get_by_role("button", name="Close Author panel").click(timeout=3000)
                    break
                except:
                    try:
                        await page.get_by_role("button", name="Close").click()
                        print("Fechou popup e vai tentar de novo.")
                    except Exception:
                        print("Não achou popup para fechar.")
            


    async def main(self, folder_path_data, folder_path_emails):
        fingerprint_generator = DefaultFingerprintGenerator(
            header_options=HeaderGeneratorOptions(browsers=['chrome']),
        )

        crawler = PlaywrightCrawler(
            max_request_retries=0,
            browser_pool=BrowserPool(plugins=[CamoufoxPlugin(browser_launch_options={"headless": True})]),
            concurrency_settings=ConcurrencySettings(
                min_concurrency=2,
                desired_concurrency=3,
                max_concurrency=3,        # mantenha baixo com sites sensíveis
                # max_tasks_per_minute=30, # opcional p/ “ritmar” requisições
            ),
            #request_handler_timeout=timedelta(seconds=120) # tempo máximo do handler
        )

        for arquivo in folder_path_data.iterdir():
            if arquivo.suffix != ".csv":
                continue
            
            dados = pd.read_csv(arquivo)

            processed_name = arquivo.stem.replace('coletadas', 'processadas') + arquivo.suffix
            save_path_emails = folder_path_emails / processed_name
            with open(save_path_emails, "w", newline="", encoding="utf-8") as f:
                self.csv_file = f
                self.csv_writer = csv.writer(f)
                self.csv_writer.writerow(['emails', 'doi'])

                router = crawler.router

                @router.handler("elsevier")
                async def _elsevier(ctx: PlaywrightCrawlingContext):
                    print("chamou a função -------------------------------------------------------------------------------")
                    await self.handle_elsevier(ctx)

                request_list = []

                for ordem_doi, row in dados.iterrows(): # Enfileira a URL inicial
                    if pd.isna(row['urls elsevier']) or pd.isna(row['doi']):
                        continue
                    url = row['urls elsevier']
                    doi = row['doi']
                    request_list.append(
                        Request.from_url(
                            url=url,
                            label="elsevier",
                            user_data={'doi': doi, 'ordem doi': ordem_doi}
                        )
                    )
                    print(f"[DOI {ordem_doi}] Enfileirado Elsevier | {url}")

                await crawler.run(request_list)


if __name__ == "__main__":
    folder_path_data = Path(r"C:\Users\cesar\Desktop\Scrap_emails_data\elsevier\enfileirados_elsevier")
    folder_path_emails = Path(r"C:\Users\cesar\Desktop\Scrap_emails_data\elsevier\processados_elsevier")
    scrap = ExtracaoElsevier()

    asyncio.run(scrap.main(folder_path_data, folder_path_emails))