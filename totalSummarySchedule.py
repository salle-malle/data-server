import schedule
import time
import logging
import pytz
from datetime import datetime
from totalSummaryService import generate_total_summary_for_all_members
from threading import Lock

def schedule_total_summary_job():
    KST = pytz.timezone("Asia/Seoul")
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    has_run_today = {"date": None}
    job_lock = Lock()
    last_run_time = [None]

    def run_job():
        # 실행 중이면 중복 방지
        if not job_lock.acquire(blocking=False):
            logger.warning("⚠️ 총평 작업이 이미 실행 중입니다. 중복 실행 방지됨.")
            return

        now_kst = datetime.now(KST)
        today_str = now_kst.strftime("%Y-%m-%d")

        if has_run_today["date"] == today_str:
            logger.info("🔁 이미 오늘 실행됨. 중복 실행 방지.")
            job_lock.release()
            return

        try:
            logger.info("✅ 총평 요약 스케줄러 실행: %s", now_kst)
            generate_total_summary_for_all_members()
            has_run_today["date"] = today_str
            last_run_time[0] = now_kst
            logger.info("✅ 총평 요약 생성 완료")
        except Exception as e:
            logger.error(f"❌ 총평 요약 생성 중 오류 발생: {e}")
        finally:
            job_lock.release()

    def schedule_kst_daily(hour, minute, job_func):
        def wrapper():
            now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
            now_kst = now_utc.astimezone(KST)

            # 1분 안에 여러 번 실행되는 것 방지
            if now_kst.hour == hour and now_kst.minute == minute:
                if last_run_time[0] and (now_kst - last_run_time[0]).total_seconds() < 60:
                    return
                job_func()

        schedule.every(10).seconds.do(wrapper)

    # ✅ 원하는 시간 설정 (예: 23:24)
    # 24시는 존재하지 않으므로, 자정(00시 5분)에 실행하려면 0으로 설정해야 합니다.
    schedule_kst_daily(1, 20, run_job)
    logger.info("⏰ 총평 요약 스케줄러 시작됨 (KST 기준 매일 23:24 실행)")

    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logger.error(f"❌ Schedule run_pending 에러: {e}")
        time.sleep(1)

if __name__ == "__main__":
    schedule_total_summary_job()
