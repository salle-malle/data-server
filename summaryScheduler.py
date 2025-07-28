
import schedule
import threading
import time
import logging
import pytz
from datetime import datetime
from summaryService import generate_summary_for_today_news

def schedule_today_summary_job():
    # 테스트용: 1분에 한 번씩 실행
    def run_job():
        try:
            generate_summary_for_today_news()
        except Exception as e:
            logging.error(f"generate_summary_for_today_news 실행 에러: {e}")

    schedule.every(1).minutes.do(run_job)

    def run_scheduler():
        while True:
            try:
                schedule.run_pending()
            except Exception as e:
                logging.error(f"Schedule run_pending 에러: {e}")
            time.sleep(1)

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logging.info("스케줄러 스레드 시작됨")
