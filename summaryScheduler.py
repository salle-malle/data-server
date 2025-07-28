import schedule
import threading
import time
import logging
import pytz
from datetime import datetime
from summaryService import generate_summary_for_today_news
from threading import Lock

def schedule_today_summary_job():
    KST = pytz.timezone("Asia/Seoul")
    logger = logging.getLogger(__name__)
    job_lock = Lock()
    last_run_time = [None]  # 리스트로 감싸야 nonlocal처럼 쓰기 가능

    def run_job():
        if not job_lock.acquire(blocking=False):
            logger.warning("⚠️ 요약 작업이 이미 실행 중입니다. 중복 실행 방지됨.")
            return
        try:
            logger.info("✅ 요약 스케줄러 실행 시작: %s", datetime.now(KST))
            generate_summary_for_today_news()
            logger.info("✅ 오늘 뉴스 요약 완료")
        except Exception as e:
            logger.error(f"❌ generate_summary_for_today_news 실행 에러: {e}")
        finally:
            job_lock.release()

    def wrapper():
        now_kst = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(KST)
        target_hour, target_minute = 0, 38

        if now_kst.hour == target_hour and now_kst.minute == target_minute:
            # 마지막 실행이 60초 이내면 skip
            if last_run_time[0] and (datetime.now(KST) - last_run_time[0]).total_seconds() < 60:
                return
            last_run_time[0] = datetime.now(KST)
            run_job()

    # 매 10초마다 실행 체크
    schedule.every(10).seconds.do(wrapper)

    def run_scheduler():
        while True:
            try:
                schedule.run_pending()
            except Exception as e:
                logger.error(f"❌ Schedule run_pending 에러: {e}")
            time.sleep(1)

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("📅 요약 스케줄러 스레드 시작됨")
