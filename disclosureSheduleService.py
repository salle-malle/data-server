import schedule
import threading
import time
import logging
import pytz
import asyncio
from datetime import datetime, timedelta
from db import fetch_all, execute_query
from analyzer import analyze_8k
from edgar import fetch_recent_8k_filings
from croniter import croniter

logger = logging.getLogger(__name__)


def schedule_disclosure_job():
    """
    FastAPI startup 이벤트에서 호출하여 스케줄러 작동 시작.
    schedule.run_pending()을 별도 데몬 스레드에서 주기 실행.
    """
    CRON_EXPRESSION = "0 30 6 * * *"
    CRON_TIMEZONE = "Asia/Seoul"

    def run_at_cron():
        now = datetime.now(pytz.timezone(CRON_TIMEZONE))
        base = now.replace(second=0, microsecond=0)
        cron = croniter(CRON_EXPRESSION, base - timedelta(minutes=1))
        next_run = cron.get_next(datetime)
        if base == next_run:
            logger.info("스케줄러 조건 일치: 6:30 - read_stock_list 실행")
            read_stock_list()

    schedule.every().minute.do(run_at_cron)
    

    def run_scheduler():
        while True:
            try:
                schedule.run_pending()
            except Exception as e:
                logger.error(f"Schedule run_pending 에러: {e}")
            time.sleep(1)

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("스케줄러 스레드 시작됨")


def run_async_func(coro):
    """
    별도 스레드에서 asyncio 함수 안전 실행용 헬퍼.
    이벤트 루프 없으면 생성 후 실행.
    FastAPI/uvicorn 환경에서는 이미 실행 중인 루프에 run_until_complete를 호출하면 RuntimeError 발생 가능.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        # FastAPI 환경에서는 이 분기가 항상 실행될 수 있음.
        # ensure_future는 현재 스레드의 루프에만 안전함.
        return asyncio.ensure_future(coro)
    else:
        return loop.run_until_complete(coro)


def read_stock_list():
    """
    DB에서 stock 목록 조회 후 각 ticker로 비동기 analyze_8k_job 실행
    """
    select_sql = "SELECT * FROM stock"
    try:
        stocks = fetch_all(select_sql)
    except Exception as e:
        logger.error(f"stock 목록 조회 실패: {e}")
        return

    tickers = [stock.get("stock_id") for stock in stocks if stock.get("stock_id")]

    logger.info(f"총 {len(tickers)}종목 대상 analyze_8k_job 실행 시작")

    for ticker in tickers:
        try:
            run_async_func(analyze_8k_job(ticker))
        except Exception as e:
            logger.error(f"{ticker}에 대해 analyze_8k_job 실행 실패: {e}")


async def analyze_8k_job(ticker: str):
    """
    실제 8-K 공시 분석 및 DB 저장 비동기 작업
    """
    today = datetime.now(pytz.timezone("Asia/Seoul")).date()
    yesterday = today - timedelta(days=1)

    try:
        logger.info(f"📌 분석 시작 - ticker: {ticker}, 기간: {yesterday} ~ {yesterday}")

        docs = fetch_recent_8k_filings(ticker, yesterday, yesterday, status=True)
        logger.info(f"📄 {ticker} - 수집된 문서 수: {len(docs)}")

        if not docs:
            logger.info(f"{ticker} - 분석할 문서가 없습니다.")
            return

        analysis_results = analyze_8k(docs)
        logger.info(f"{ticker} - 분석 완료")

        clean_results = [{k: v for k, v in r.items() if k != "_meta"} for r in analysis_results]

        insert_sql = """
            INSERT INTO disclosure
            (disclosure_date, created_at, updated_at, disclosure_summary, disclosure_title, stock_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        for result in clean_results:
            try:
                execute_query(insert_sql, (
                    yesterday,
                    datetime.now(pytz.timezone("Asia/Seoul")),
                    datetime.now(pytz.timezone("Asia/Seoul")),
                    result.get("narrative"),
                    result.get("title"),
                    ticker
                ))
            except Exception as e:
                logger.error(f"{ticker} - DB 저장 중 오류: {e}")

        logger.info(f"{ticker} - DB 저장 완료")

    except Exception as e:
        logger.exception(f"❌ analyze_8k_job 처리 중 오류: {e}")
