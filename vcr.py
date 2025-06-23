import asyncio
import logging
import re
import sys
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import psutil
import pyodbc
from playwright.async_api import async_playwright, Browser, BrowserContext, Page


def print_with_flush(msg):
    """Print message with immediate flush"""
    print(msg, flush=True)


class CompanyInfo:
    def __init__(self):
        self.name: str = ""
        self.address: str = ""
        self.cvr_number: str = ""
        self.status: str = ""
        self.company_type: str = ""


class CvrScraper:
    PAGE_SIZE = 100
    DEFAULT_TIMEOUT = 30000
    MAX_RETRIES = 3
    CONCURRENT_DAYS = 5
    CONCURRENT_PAGES = 8
    BATCH_COMMIT_SIZE = 100

    def __init__(self):
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._logger = logging.getLogger(__name__)
        self._cvr_cache = set()
        self._company_buffer = []
        self._last_request_time = 0

    async def get_last_created_date(self) -> datetime:
        """Get the most recent created_at date from the database"""
        try:
            conn = pyodbc.connect(
                "DRIVER={SQL Server};"
                "SERVER=DESKTOP-VPRC2OF;"
                "DATABASE=cvr_database;"
                "Trusted_Connection=yes;"
                "TrustServerCertificate=yes;"
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(created_at) AS last_created_at
                FROM [cvr_database].[dbo].[companies];
            """)
            result = cursor.fetchone()
            conn.close()

            if not result or not result[0]:
                raise Exception("No created_at date found in database")

            last_created_at = result[0]  # Assuming result[0] is a string

            # Convert string to datetime (handling up to 7 microsecond digits)
            last_created_at = datetime.strptime(last_created_at[:26], '%Y-%m-%d %H:%M:%S.%f')

            return last_created_at  # ✅ Now always a string

        except Exception as e:
            self._logger.error(f"Database error getting last created date: {str(e)}")
            raise

    @staticmethod
    def build_url(page_index: int, start_date: datetime) -> str:
        """Build search URL with proper date encoding"""
        date_formatted = start_date.strftime("%d%%2F%m%%2F%Y")
        return (
            f"https://datacvr.virk.dk/soegeresultater?"
            f"sideIndex={page_index}&enhedstype=virksomhed&virksomhedsstatus=aktive&"
            f"startdatoFra={date_formatted}&startdatoTil={date_formatted}&"
            f"size={CvrScraper.PAGE_SIZE}"  # Fixed here
        )

    @asynccontextmanager
    async def get_page(self) -> Page:
        """Context manager for browser pages"""
        page = await self._context.new_page()
        try:
            yield page
        finally:
            await page.close()

    async def initialize_browser(self):
        """Initialize Playwright browser instance"""
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.firefox.launch(
            headless=False,
            slow_mo=200,
            timeout=30000
        )
        self._context = await self._browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            java_script_enabled=True
        )

    async def process_date_range(self, start_date: datetime, end_date: datetime):
        """Process a range of dates"""
        current_date = start_date
        while current_date <= end_date:
            await self.process_single_date(current_date)
            current_date += timedelta(days=1)

    async def process_single_date(self, date: datetime):
        """Process all pages for a single date"""
        try:
            self._logger.info(f"Processing date: {date.strftime('%d/%m/%Y')}")
            async with self.get_page() as page:
                url = self.build_url(0, date)
                await page.goto(url, wait_until="networkidle")

                total_companies = await self.get_total_companies_count(page)
                if not total_companies:
                    return

                total_pages = (total_companies + self.PAGE_SIZE - 1) // self.PAGE_SIZE
                self._logger.info(f"Found {total_companies} companies across {total_pages} pages")

                for page_index in range(total_pages):
                    await self.process_page(page_index, date)

        except Exception as e:
            self._logger.error(f"Error processing date {date}: {str(e)}")
            await self.take_debug_screenshot(page)

    async def process_page(self, page_index: int, date: datetime):
        """Process a single results page"""
        try:
            async with self.get_page() as page:
                url = self.build_url(page_index, date)
                self._logger.info(f"Processing page {page_index} - {url}")

                await page.goto(url, wait_until="networkidle")
                await self.wait_for_content(page)

                rows = await page.query_selector_all("div.row[data-v-295c8b5e]")
                companies = await self.process_rows(rows)

                if companies:
                    await self.save_companies(companies)

        except Exception as e:
            self._logger.error(f"Error processing page {page_index}: {str(e)}")
            await self.take_debug_screenshot(page)

    async def process_rows(self, rows) -> List[CompanyInfo]:
        """Process all company rows on a page"""
        companies = []
        for row in rows:
            try:
                company = await self.extract_company_info(row)
                if company.cvr_number and company.cvr_number not in self._cvr_cache:
                    self.clean_company_data(company)
                    companies.append(company)
            except Exception as e:
                self._logger.error(f"Error processing row: {str(e)}")
        return companies

    async def extract_company_info(self, row) -> CompanyInfo:
        """Extract company information from a row"""
        company = CompanyInfo()

        try:
            name_element = await row.query_selector("span.bold.value")
            company.name = await name_element.inner_text() if name_element else ""

            cvr_element = await row.query_selector("div:has(> div:text-is('CVR-nummer:'))")
            company.cvr_number = (await cvr_element.inner_text()).replace("CVR-nummer:",
                                                                          "").strip() if cvr_element else ""

            address_element = await row.query_selector("div[class*='col-12'] div")
            company.address = await address_element.inner_text() if address_element else ""

            status_element = await row.query_selector("div:has(> div:text-is('Status:'))")
            company.status = (await status_element.inner_text()).replace("Status:",
                                                                         "").strip() if status_element else ""

            type_element = await row.query_selector("div:has(> div:text-is('Virksomhedsform:'))")
            company.company_type = (await type_element.inner_text()).replace("Virksomhedsform:",
                                                                             "").strip() if type_element else ""

        except Exception as e:
            self._logger.warning(f"Partial data extraction error: {str(e)}")

        return company

    async def get_total_companies_count(self, page: Page) -> Optional[int]:
        """Get total number of companies for current search"""
        try:
            content = await page.content()
            match = re.search(r"Vi fandt ([\d\.]+) resultater", content)
            if match:
                return int(match.group(1).replace(".", ""))
            return None
        except Exception as e:
            self._logger.error(f"Error getting company count: {str(e)}")
            return None

    async def save_companies(self, companies: List[CompanyInfo]):
        """Save companies to database using MERGE to handle duplicates"""
        if not companies:
            return

        conn = None
        try:
            conn = pyodbc.connect(
                "DRIVER={SQL Server};"
                "SERVER=DESKTOP-VPRC2OF;"
                "DATABASE=cvr_database;"
                "Trusted_Connection=yes;"
                "TrustServerCertificate=yes;"
            )
            cursor = conn.cursor()

            # Prepare batch data
            data = [
                (
                    company.cvr_number,
                    company.name,
                    company.address,
                    company.status,
                    company.company_type
                )
                for company in companies
            ]

            merge_sql = """
            MERGE INTO companies AS target
            USING (VALUES (?, ?, ?, ?, ?)) AS source 
                (cvr_number, name, address, status, company_type)
            ON target.cvr_number = source.cvr_number
            WHEN NOT MATCHED THEN
                INSERT (cvr_number, name, address, status, company_type)
                VALUES (source.cvr_number, source.name, source.address, source.status, source.company_type);
            """

            for row in data:
                cursor.execute(merge_sql, row)
                self._cvr_cache.add(row[0])  # Add CVR to cache after successful merge

            conn.commit()
            self._logger.info(f"Processed {len(companies)} companies using MERGE")

        except Exception as e:
            if conn:
                conn.rollback()
            self._logger.error(f"Database error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    @staticmethod
    def clean_company_data(company: CompanyInfo):
        """Clean and normalize company data"""
        company.address = ' '.join(company.address.split())
        company.cvr_number = company.cvr_number.strip()

    async def take_debug_screenshot(self, page: Page):
        """Take debug screenshot on error"""
        try:
            screenshot_dir = Path.home() / "cvr_screenshots"
            screenshot_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = screenshot_dir / f"error_{timestamp}.png"
            await page.screenshot(path=str(path), full_page=True)
            self._logger.info(f"Saved debug screenshot to {path}")
        except Exception as e:
            self._logger.error(f"Failed to take screenshot: {str(e)}")

    async def wait_for_content(self, page: Page):
        """Wait for page content to load"""
        try:
            await page.wait_for_selector("div.row[data-v-295c8b5e]", state="visible", timeout=self.DEFAULT_TIMEOUT)
        except Exception as e:
            self._logger.error("Timeout waiting for content")
            await self.take_debug_screenshot(page)
            raise

    async def dispose_resources(self):
        """Clean up all resources"""
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as e:
            self._logger.error(f"Error during cleanup: {str(e)}")


async def main_scraper():
    """Main scraping workflow"""
    scraper = CvrScraper()
    try:
        await scraper.initialize_browser()

        # Get start date from database (already formatted as a tuple)
        # start_date = await scraper.get_last_created_date()
        start_date = datetime(1900,12,30)

        # Use current date as end date and format as tuple (YYYY, M, D)
        now = datetime.now()
        end_date = now

        print_with_flush(f"Starting scrape from {start_date} to {end_date}")

        await scraper.process_date_range(
            start_date=start_date,  # This is a datetime object
            end_date=end_date  # This is a string
        )
    except Exception as e:
        print_with_flush(f"Error: {str(e)}")
        raise
    finally:
        await scraper.dispose_resources()


def main():
    """Entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )

    try:
        asyncio.run(main_scraper())
    except KeyboardInterrupt:
        print_with_flush("\nOperation cancelled by user")
    except Exception as e:
        print_with_flush(f"Critical error: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()