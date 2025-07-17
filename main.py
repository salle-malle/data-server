import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any

from edgar import fetch_recent_8k_filings
from analyzer import analyze_8k
from schemas import CrawlingRequest, CrawlingResponse
from services import crawl_and_process_news

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="SEC 8-K 분석기", version="2.0.0")

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

@app.post("/analyze-8k", response_model=AnalysisResponse)
async def analyze_8k_endpoint(req: AnalysisRequest):
    if any(c.isdigit() for c in req.ticker):
        return AnalysisResponse(
            ticker=req.ticker.upper(),
            total_filings=0,
            results=[],
            total_cost_usd=0.0,
            status="skipped"
        )

    ticker = req.ticker.upper()
    try:
        docs = fetch_recent_8k_filings(ticker, req.start_date, req.end_date)
        if not docs:
            return AnalysisResponse(
                ticker=ticker,
                total_filings=0,
                results=[],
                total_cost_usd=0.0,
                status="no filings found"
            )

        analysis_results = analyze_8k(docs)
        total_cost = sum(r.get("_meta", {}).get("cost_usd", 0) for r in analysis_results)
        clean_results = [{k: v for k, v in r.items() if k != "_meta"} for r in analysis_results]

        return AnalysisResponse(
            ticker=ticker,
            total_filings=len(docs),
            results=clean_results,
            total_cost_usd=round(total_cost, 4),
            status="success"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/crawl-news", response_model=CrawlingResponse)
async def crawl_stock_news(req: CrawlingRequest):
    results = crawl_and_process_news(req.tickers)
    if not results:
        raise HTTPException(status_code=404, detail="뉴스를 가져올 수 없습니다.")
    return CrawlingResponse(crawl_results=results)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
