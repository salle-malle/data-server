import schedule
import threading
import time
import logging
import pytz
from datetime import datetime
from totalSummaryService import generate_total_summary_for_all_members

def schedule_total_summary_job():
    KST = pytz.timezone("Asia/Seoul")
    logger = logging.getLogger(__name__)

    def run_job():
        try:
            logger.info("총평 요약 스케줄러 실행: %s", datetime.now(KST))
            generate_total_summary_for_all_members()
            logger.info("총평 요약 생성 완료")
        except Exception as e:
            logger.error(f"총평 요약 생성 중 오류 발생: {e}")

    # 테스트용: 매 분마다 실행
    schedule.every(1).minutes.do(run_job)
    logger.info("총평 요약 스케줄러가 시작되었습니다. (테스트용: 매 분마다 실행)")

    def run_scheduler():
        while True:
            try:
                schedule.run_pending()
            except Exception as e:
                logger.error(f"Schedule run_pending 에러: {e}")
            time.sleep(1)

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("총평 요약 스케줄러 스레드 시작됨")

if __name__ == "__main__":
    schedule_total_summary_job()
    while True:
        time.sleep(60)
