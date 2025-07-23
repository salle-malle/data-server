# schemas.py

from pydantic import BaseModel
from typing import List, Dict, Optional


class CrawlingRequest(BaseModel):
    """
    뉴스 크롤링 요청을 위한 모델
    """
    tickers: List[str]


class NewsArticle(BaseModel):
    """
    개별 뉴스 기사 정보를 담는 모델
    """
    newsTitle: str
    newsUri: str
    newsContent: str
    newsDate: Optional[str] = None  # str | None 과 동일
    newsImage: Optional[str] = None


class CrawlingResponse(BaseModel):
    """
    크롤링 결과 응답을 위한 모델
    """
    crawl_results: Dict[str, List[NewsArticle]]
