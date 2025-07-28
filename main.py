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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI 인스턴스를 여러 개 만들어 각각 lifespan을 다르게 등록하면 각각의 앱에서만 해당 스케줄러가 동작합니다.
# 하지만 일반적으로 하나의 FastAPI 앱에서 여러 스케줄러를 모두 등록하려면, 아래처럼 각각의 스케줄러를 한 lifespan에서 모두 호출해야 합니다.
# 만약 각각의 스케줄러를 독립적으로 관리하고 싶다면, 별도의 프로세스나 서비스로 분리하는 것이 일반적입니다.
# FastAPI의 lifespan은 앱 단위로 하나만 등록할 수 있습니다.

# 예시: 하나의 앱에서 여러 스케줄러를 모두 등록
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("이벤트 등록")
    schedule_disclosure_job()
    schedule_news_job()
    schedule_main_news_job()
    schedule_today_summary_job()
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
    return {"status": "healthy"}

@app.post("/crawl-news", response_model=CrawlingResponse, deprecated=True)
async def crawl_stock_news(req: CrawlingRequest):
    # results = crawl_and_process_news(req.tickers)/
    # if not results:
        # raise HTTPException(status_code=404, detail="뉴스를 가져올 수 없습니다.")
    # return CrawlingResponse(crawl_results=results)
    raise HTTPException(status_code=410, detail="이 엔드포인트는 더 이상 지원되지 않습니다.")

