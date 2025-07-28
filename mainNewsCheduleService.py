import pytz
import schedule
import threading
import time
import logging
import asyncio
from datetime import datetime
from db import execute_query
from services import crawl_yahoo_stock_market_news

logger = logging.getLogger(__name__)

def schedule_main_news_job():
    CRON_TIMEZONE = "Asia/Seoul"
    TARGET_HOUR = 5
    TARGET_MINUTE =0

    def run_at_target_time():
        now = datetime.now(pytz.timezone(CRON_TIMEZONE))
        if now.hour == TARGET_HOUR and now.minute == TARGET_MINUTE:
            schedule_yahoo_stock_market_news()

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

def schedule_yahoo_stock_market_news():
    try:
        logger.info("📌 메인 뉴스 크롤링 시작")
        results = crawl_yahoo_stock_market_news()
        logger.info(f"📄 메인 뉴스 크롤링 완료 - 수집된 문서 수: {sum(len(v) for v in results.values())}")
        print(results)
        insert_sql = """
            INSERT INTO main_news (
                news_content,
                news_title,
                news_uri,
                news_date
            ) VALUES (
                %s, %s, %s, %s
            )
        """

        for result_list in results.values():
            for result in result_list:
                execute_query(
                    insert_sql,
                    (
                        result.newsContent,
                        result.newsTitle,
                        result.newsUri,
                        str(result.newsDate),
                    )
                )

    except Exception as e:
        logger.exception("❌ 메인 뉴스 크롤링 중 오류 발생")
