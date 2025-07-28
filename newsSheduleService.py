import schedule
import threading
import time
import logging
import pytz
import asyncio
from datetime import datetime
from db import fetch_all, execute_query
from services import crawl_and_process_news

logger = logging.getLogger(__name__)

def schedule_news_job():
    CRON_TIMEZONE = "Asia/Seoul"
    TARGET_HOUR = 7
    TARGET_MINUTE = 0

    def run_at_target_time():
        now = datetime.now(pytz.timezone(CRON_TIMEZONE))
        if now.hour == TARGET_HOUR and now.minute == TARGET_MINUTE:
            read_stock_list()

    schedule.every().minute.do(run_at_target_time)

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

def read_stock_list():
    select_sql = "SELECT * FROM stock"
    try:
        stocks = fetch_all(select_sql)
    except Exception as e:
        logger.error(f"stock 목록 조회 실패: {e}")
        return

    tickers = [stock.get("stock_id") for stock in stocks if stock.get("stock_id")]
    logger.info(f"총 {len(tickers)}종목 대상 뉴스 데이터 실행 시작")

    for ticker in tickers:
        try:
            run_async_func(crawl_news_job(ticker))
        except Exception as e:
            logger.error(f"{ticker}에 대해 crawl_news_job 실행 실패: {e}")

def run_async_func(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        return asyncio.ensure_future(coro)
    else:
        return loop.run_until_complete(coro)

async def crawl_news_job(ticker: str):
    try:
        logger.info(f"📌 뉴스 크롤링 시작 - ticker: {ticker}")
        results = await crawl_and_process_news([ticker])
        logger.info(f"📄 {ticker} - 수집된 문서 수: {sum(len(v) for v in results.values())}")

        insert_sql = """
            INSERT INTO news (
                created_at,
                updated_at,
                news_content,
                news_date,
                news_image,
                news_title,
                news_uri,
                stock_id
            ) VALUES (
                NOW(), NOW(), %s, %s, %s, %s, %s, %s
            )
        """
        for ticker_key, articles in results.items():
            logger.info(f"티커: {ticker_key}, 기사 수: {len(articles)}")
            for idx, article in enumerate(articles, 1):
                logger.info(f"  [{idx}] 제목: {article.newsTitle}, 날짜: {article.newsDate}, URI: {article.newsUri}")
                execute_query(
                    insert_sql,
                    (
                        article.newsContent,
                        str(article.newsDate),
                        article.newsImage,
                        article.newsTitle,
                        article.newsUri,
                        ticker_key
                    )
                )
    except Exception as e:
        logger.exception("❌ 뉴스 크롤링 중 오류 발생")
