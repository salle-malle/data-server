import schedule
import threading
import time
import logging
import pytz
from datetime import datetime
from summaryService import generate_summary_for_today_news

def schedule_today_summary_job():
    KST = pytz.timezone("Asia/Seoul")

    def run_job():
        try:
            generate_summary_for_today_news()
        except Exception as e:
            logging.error(f"generate_summary_for_today_news 실행 에러: {e}")

    def schedule_kst_daily(hour, minute, job_func):
        """
        한국 시간 기준으로 매일 특정 시각에 job_func 실행
        """
        def wrapper():
            now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
            now_kst = now_utc.astimezone(KST)
            if now_kst.hour == hour and now_kst.minute == minute:
                job_func()
        schedule.every(1).minutes.do(wrapper)

    schedule_kst_daily(19, 55, run_job)

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
