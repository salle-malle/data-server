from sec_edgar_downloader import Downloader
from datetime import datetime, timedelta
import os
import logging
import time
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_recent_8k_filings(ticker: str, max_files: int = 3, days_back: int = 30) -> list[str]:
    """
    SEC Fair Access Policy를 준수하는 8-K 공시 다운로드
    비용 절약을 위해 기본값을 3개 파일, 30일로 설정
    """
    try:
        # 환경 변수에서 필수 정보 로드
        company_name = os.getenv("SEC_COMPANY_NAME", "MyCompany")
        email_address = os.getenv("SEC_EMAIL_ADDRESS", "user@example.com")
        
        logger.info(f"SEC 다운로더 초기화: {company_name} {email_address}")
        
        # SEC 5.0+ 버전 호환 생성자
        dl = Downloader(
            company_name=company_name,
            email_address=email_address,
            download_folder="sec_data"
        )
        
        # 날짜 필터 설정
        after_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        logger.info(f"검색 기간: {after_date} 이후, 최대 {max_files}개 파일")
        
        # SEC Fair Access Policy 준수를 위한 지연
        time.sleep(0.1)
        
        # 8-K 공시 다운로드 (5.0+ 버전: amount -> limit)
        dl.get("8-K", ticker, limit=max_files, after=after_date, include_amends=False)
        
        # 파일 읽기
        base_path = f"sec_data/sec-edgar-filings/{ticker.upper()}/8-K"
        docs = []
        collected = 0
        
        if not os.path.exists(base_path):
            logger.warning(f"경로가 존재하지 않습니다: {base_path}")
            return []
        
        for root, _, files in os.walk(base_path):
            for fname in sorted(files, reverse=True):
                if fname.endswith(".txt"):
                    file_path = os.path.join(root, fname)
                    try:
                        with open(file_path, encoding="utf-8", errors="ignore") as f:
                            content = f.read()
                            # 최소 1000자 이상인 의미있는 문서만 처리
                            if len(content.strip()) > 1000:
                                docs.append(content)
                                collected += 1
                                logger.info(f"파일 로드 완료: {fname} ({len(content)} 문자)")
                                if collected >= max_files:
                                    return docs
                    except Exception as e:
                        logger.warning(f"파일 읽기 오류: {fname}, {str(e)}")
                        continue
        
        logger.info(f"총 {len(docs)}개의 8-K 공시를 로드했습니다.")
        return docs
        
    except Exception as e:
        logger.error(f"8-K 공시 다운로드 중 오류 발생: {str(e)}")
        
        # 403 오류 특별 처리
        if "403" in str(e):
            logger.error("SEC Fair Access Policy 위반:")
            logger.error("1. .env 파일에 SEC_COMPANY_NAME과 SEC_EMAIL_ADDRESS 설정")
            logger.error("2. 유효한 이메일 주소와 회사명 사용")
            logger.error("3. 1-2시간 후 다시 시도")
        
        return []
