import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from services import crawl_and_process_news, crawl_yahoo_stock_market_news
from schemas import CrawlingRequest, CrawlingResponse
from disclosureSheduleService import schedule_disclosure_job
from contextlib import asynccontextmanager
from newsSheduleService import schedule_news_job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("이벤트 등록")
    schedule_disclosure_job()
    yield
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("이벤트 등록")
    schedule_news_job()
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


@app.get("/market/crawl_news", response_model=CrawlingResponse)
async def crawl_market_crawl_news():
    results = crawl_yahoo_stock_market_news()
    print(results)
    if not results:
        raise HTTPException(status_code=404, detail="미국 증시 뉴스를 가져올 수 없습니다.")
    
    return CrawlingResponse(crawl_results=results)