import os
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sec_edgar_downloader import Downloader

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_recent_8k_filings(
    ticker: str,
    start_date: str = None,
    end_date: str = None
) -> list[str]:
    try:
        company_name = os.getenv("SEC_COMPANY_NAME", "MyCompany")
        email_address = os.getenv("SEC_EMAIL_ADDRESS", "user@example.com")
        logger.info(f"SEC 다운로더 초기화: {company_name} {email_address}")

        today = datetime.now().strftime("%Y-%m-%d")
        if end_date is None:
            end_date = today
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        logger.info(f"검색 기간: {start_date}부터 {end_date}까지")

        # 티커별 폴더로 다운로드
        download_folder = f"sec_data/{ticker.upper()}"
        os.makedirs(download_folder, exist_ok=True)

        dl = Downloader(
            company_name=company_name,
            email_address=email_address,
            download_folder=download_folder
        )
        time.sleep(0.2)
        dl.get("8-K", ticker, after=start_date, before=end_date, include_amends=True)
        time.sleep(0.3)

        # 다운로드된 모든 txt 파일 읽기
        docs = []
        for root, _, files in os.walk(download_folder):
            for fname in files:
                if fname.lower().endswith(".txt"):
                    file_path = os.path.join(root, fname)
                    try:
                        with open(file_path, encoding="utf-8", errors="ignore") as f:
                            content = f.read().strip()
                        if len(content) > 1000:
                            docs.append(content)
                    except Exception:
                        continue

        logger.info(f"총 {len(docs)}개의 8-K 공시 로드 완료")
        return docs

    except Exception as e:
        logger.error(f"8-K 다운로드 중 오류: {e}")
        return []
