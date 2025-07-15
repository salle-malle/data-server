from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from edgar import fetch_recent_8k_filings
from analyzer import analyze_8k
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SEC 8-K LLM 분석기",
    description="SEC 8-K 공시를 자동으로 수집하고 한국어로 요약하는 API (비용 최적화)",
    version="2.0.0"
)

class AnalysisRequest(BaseModel):
    ticker: str
    max_files: int = 3      # 비용 절약을 위해 기본값 3개
    days_back: int = 30     # 30일로 확장

class AnalysisResponse(BaseModel):
    ticker: str
    total_filings: int
    results: List[Dict[str, Any]]
    total_cost_usd: float
    status: str

@app.post("/analyze-8k", response_model=AnalysisResponse)
async def analyze_8k_endpoint(req: AnalysisRequest):
    """
    8-K 공시 분석 API (비용 최적화)
    """
    try:
        logger.info(f"분석 시작: {req.ticker}")
        
        # 8-K 공시 다운로드
        docs = fetch_recent_8k_filings(
            ticker=req.ticker,
            max_files=req.max_files,
            days_back=req.days_back
        )
        
        if not docs:
            raise HTTPException(
                status_code=404,
                status="no_filings",
                detail=f"{req.ticker}의 최근 {req.days_back}일 간 8-K 공시를 찾을 수 없습니다."
            )
        
        # LLM 분석 수행
        analysis_results = analyze_8k(docs)
        
        # 총 비용 계산
        total_cost = sum(result.get("_meta", {}).get("cost_usd", 0) for result in analysis_results)
        
        # 메타데이터 제거 (사용자에게는 숨김)
        clean_results = []
        for result in analysis_results:
            clean_result = {k: v for k, v in result.items() if k != "_meta"}
            clean_results.append(clean_result)
        
        response = AnalysisResponse(
            ticker=req.ticker.upper(),
            total_filings=len(docs),
            results=clean_results,
            total_cost_usd=round(total_cost, 4),
            status="success"
        )
        
        logger.info(f"분석 완료: {req.ticker}, 결과: {len(clean_results)}개, 비용: ${total_cost:.4f}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"분석 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")

@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    return {"status": "healthy", "message": "SEC 8-K 분석기가 정상 작동 중입니다."}

@app.get("/cost-info")
async def cost_info():
    """비용 정보 제공"""
    return {
        "model": "gpt-4o-mini",
        "input_cost_per_1k_tokens": "$0.00015",
        "output_cost_per_1k_tokens": "$0.00060",
        "estimated_cost_per_request": "$0.005-0.015",
        "optimization": "청크 크기 최적화, 중요 섹션만 추출, 문서당 최대 3개 청크"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
