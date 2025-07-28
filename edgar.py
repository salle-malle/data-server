import os
import logging
import time
import chardet
import gzip
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sec_edgar_downloader import Downloader

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_recent_8k_filings(
    ticker: str,
    start_date: str = None,
    end_date: str = None,
    recent_days: int = 1,
    status: bool = True  # status 파라미터 추가
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

        if status:
            cutoff_timestamp = (datetime.now() - timedelta(days=recent_days)).timestamp()
        else:
            cutoff_timestamp = None  # 모든 파일 읽기

        docs = []
        for root, _, files in os.walk(download_folder):
            for fname in files:
                if fname.lower().endswith(".txt"):
                    file_path = os.path.join(root, fname)
                    mtime = os.path.getmtime(file_path)
                    if cutoff_timestamp is not None and mtime < cutoff_timestamp:
                        logger.debug(f"오래된 파일 스킵: {fname}")
                        continue

                    try:
                        with open(file_path, "rb") as f:
                            raw = f.read()

                        # GZIP 압축 해제 (SEC 파일에 종종 포함됨)
                        if raw[:2] == b"\x1f\x8b":
                            try:
                                raw = gzip.decompress(raw)
                                logger.debug(f"GZIP 압축 해제: {fname}")
                            except Exception as e:
                                logger.warning(f"GZIP 압축 해제 실패 ({fname}): {e}")

                        # 제어문자 제거 (ASCII 범위 외 문자)
                        raw_clean = re.sub(rb"[^\x09\x0A\x0D\x20-\x7E]", b" ", raw)

                        # 인코딩 감지 및 디코딩
                        enc_info = chardet.detect(raw_clean)
                        encoding = enc_info["encoding"] or "utf-8"
                        confidence = enc_info.get("confidence", 0)

                        logger.debug(f"파일 {fname}: 인코딩={encoding}, 신뢰도={confidence:.2f}")

                        content = raw_clean.decode(encoding, errors="replace").strip()

                        if len(content) > 1000:
                            docs.append(content)
                            logger.debug(f"파일 로드 성공: {fname} ({len(content):,}자)")
                        else:
                            logger.debug(f"파일 너무 짧음, 스킵: {fname} ({len(content)}자)")

                    except Exception as e:
                        logger.warning(f"파일 읽기 실패 ({fname}): {e}")
                        continue

        if status:
            logger.info(f"총 {len(docs)}개의 최근 8-K 공시 로드 완료 (최근 {recent_days}일 내)")
        else:
            logger.info(f"총 {len(docs)}개의 8-K 공시 로드 완료 (기간 무관)")
        return docs

    except Exception as e:
        logger.error(f"8-K 다운로드 중 오류: {e}")
        return []
