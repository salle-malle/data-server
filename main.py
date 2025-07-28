import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from services import crawl_and_process_news, crawl_yahoo_stock_market_news
from schemas import CrawlingRequest, CrawlingResponse
from disclosureSheduleService import schedule_disclosure_job
from contextlib import asynccontextmanager
from newsSheduleService import schedule_news_job
from mainNewsCheduleService import schedule_main_news_job
from summaryScheduler import schedule_today_summary_job
from totalSummarySchedule import schedule_total_summary_job
from disclosureInitScheduleService import schedule_disclosure_init_job
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 서버가 여러 번 재시작되거나 lifespan이 여러 번 호출될 수 있으므로,
# 한 번만 실행되도록 플래그를 사용
_disclosure_once_flag = False
_disclosure_once_lock = asyncio.Lock()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("✅ FastAPI lifespan 진입: 스케줄러 및 초기화 작업 시작")
    try:
        logger.info("스케줄러 등록 시작")
        schedule_disclosure_job()
        logger.info("공시 스케줄러 등록 완료")
        schedule_news_job()
        logger.info("뉴스 스케줄러 등록 완료")
        schedule_main_news_job()
        logger.info("메인 뉴스 스케줄러 등록 완료")
        schedule_today_summary_job()
        logger.info("오늘 요약 스케줄러 등록 완료")
        schedule_total_summary_job()
        logger.info("총평 요약 스케줄러 등록 완료")
        schedule_disclosure_init_job()
        logger.info("init 스케줄러 등록 완료")
    except Exception as e:
        logger.exception(f"lifespan 내 스케줄러 등록 중 오류 발생", exc_info=True)
    yield

app = FastAPI(title="SEC 8-K 분석기", version="2.0.0", lifespan=lifespan)

class AnalysisRequest(BaseModel):
    ticker: str
    start_date: str = None
    end_date: str = None

class AnalysisResponse(BaseModel):
    ticker: str
    total_filings: int
    results: List[Dict[str, Any]]
    total_cost_usd: float
    status: str

@app.get("/health")
async def health_check():
    logger.info("/health 엔드포인트 호출됨")
    return {"status": "healthy"}

@app.post("/crawl-news", response_model=CrawlingResponse, deprecated=True)
async def crawl_stock_news(req: CrawlingRequest):
    logger.warning("/crawl-news 엔드포인트는 더 이상 지원되지 않습니다.")
    raise HTTPException(status_code=410, detail="이 엔드포인트는 더 이상 지원되지 않습니다.")
