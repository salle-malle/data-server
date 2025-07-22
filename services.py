from datetime import datetime
from zoneinfo import ZoneInfo
import yfinance as yf
from newspaper import Article, Config
import logging
import time
from typing import List, Dict
import requests
from bs4 import BeautifulSoup

from schemas import NewsArticle

logger = logging.getLogger(__name__)

def crawl_and_process_news(tickers: List[str]) -> Dict[str, List[NewsArticle]]:
    all_results: Dict[str, List[NewsArticle]] = {}

    for ticker_symbol in tickers:
        try:
            logger.info(f"'{ticker_symbol}' 뉴스 크롤링 시작...")
            ticker = yf.Ticker(ticker_symbol)
            news_list = ticker.get_news()
            
            crawled_articles: List[NewsArticle] = []
            for news_item in news_list[:5]:
                try:
                    url = news_item.get('link') or news_item.get('content', {}).get('canonicalUrl', {}).get('url')
                    pub_date = news_item.get('providerPublishTime') or news_item.get('content', {}).get('pubDate')

                    if not url or not pub_date:
                        continue

                    # pub_date 가 ISO 형식 문자열일 수도 있고, UNIX timestamp일 수도 있어서 분기
                    if isinstance(pub_date, int) or isinstance(pub_date, float):
                        utc_time = datetime.fromtimestamp(pub_date, ZoneInfo("UTC"))
                    else:
                        utc_time = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))

                    kst_time = utc_time.astimezone(ZoneInfo("Asia/Seoul"))
                    date_kst = kst_time.strftime('%Y-%m-%d %H:%M:%S')

                    url = news_item['content']['canonicalUrl']['url']
                    image = news_item['content']['thumbnail']['originalUrl']
                    pub_date = news_item['content']['pubDate']
                    utc_time = datetime.datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                    kst_zone = zoneinfo.ZoneInfo("Asia/Seoul")
                    kst_time = utc_time.astimezone(kst_zone)
                    date_kst = kst_time.strftime('%Y-%m-%d %H:%M:%S.%f')
                    article = Article(url, language='en')
                    article.download()
                    article.parse()

                    crawled_articles.append(NewsArticle(
                        newsTitle=article.title,
                        newsUri=url,
                        newsContent=article.text,
                        newsDate=date_kst,
                        newsImage=image,
                    ))

                    logger.info(f"  - 성공: {article.title}")
                except Exception as e:
                    logger.error(f"  - 기사 처리 중 오류 발생 (URL: {url}): {e}")
                    continue

            all_results[ticker_symbol] = crawled_articles
            logger.info(f"'{ticker_symbol}' 뉴스 크롤링 완료. {len(crawled_articles)}개 기사 수집.")
        except Exception as e:
            logger.error(f"'{ticker_symbol}' 처리 중 오류 발생: {e}", exc_info=True)
            all_results[ticker_symbol] = []
        time.sleep(1)

    return all_results

def crawl_yahoo_stock_market_news(limit: int = 10) -> Dict[str, List[NewsArticle]]:
    """Yahoo Finance 주요 증시뉴스 페이지에서 직접 크롤링"""
    all_results: Dict[str, List[NewsArticle]] = {}
    
    try:
        # Yahoo Finance 주요 증시뉴스 페이지
        url = "https://finance.yahoo.com/topic/stock-market-news/"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
        
        # 페이지 HTML 가져오기
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        crawled_articles: List[NewsArticle] = []
        seen_urls = set()
        
        # Yahoo Finance 뉴스 링크 찾기 (여러 패턴 시도)
        article_selectors = [
            "a[href*='/news/']",  # /news/ 경로 포함
            "h3 a[href*='finance.yahoo.com']",  # 제목 링크
            ".js-content-viewer",  # Yahoo Finance 콘텐츠 뷰어
            "[data-module='FinanceStream'] a"  # 금융 스트림 모듈
        ]
        
        for selector in article_selectors:
            if len(crawled_articles) >= limit:
                break
                
            links = soup.select(selector)
            for link in links:
                if len(crawled_articles) >= limit:
                    break
                    
                href = link.get('href')
                if not href:
                    continue
                    
                # 상대 URL을 절대 URL로 변환
                if href.startswith('/'):
                    article_url = f"https://finance.yahoo.com{href}"
                elif href.startswith('http'):
                    article_url = href
                else:
                    continue
                
                # Yahoo Finance 뉴스만 필터링 및 중복 제거 + 광고성 URL 제외
                if (article_url in seen_urls or 
                    'finance.yahoo.com/news/' not in article_url or
                    article_url.endswith('/news/') or  # 뉴스 홈페이지 제외
                    'sign-up-for-yahoo-finances-morning-brief' in article_url):  # 뉴스레터 가입 페이지 제외
                    continue
                    
                seen_urls.add(article_url)
                
                try:
                    # newspaper3k 설정
                    config = Config()
                    config.browser_user_agent = headers["User-Agent"]
                    config.request_timeout = 10
                    
                    # 기사 본문 추출
                    article = Article(article_url, language='en', config=config)
                    article.download()
                    article.parse()
                    
                    # 제목과 본문이 있는지 확인
                    if not article.title or not article.text:
                        logger.warning(f"  - 제목 또는 본문 없음: {article_url}")
                        continue
                    
                    # 발행일 처리
                    if article.publish_date:
                        try:
                            # timezone이 없으면 UTC로 가정
                            if article.publish_date.tzinfo is None:
                                utc_time = article.publish_date.replace(tzinfo=ZoneInfo("UTC"))
                            else:
                                utc_time = article.publish_date.astimezone(ZoneInfo("UTC"))
                            kst_time = utc_time.astimezone(ZoneInfo("Asia/Seoul"))
                            date_kst = kst_time.strftime('%Y-%m-%d %H:%M:%S')
                        except Exception:
                            # 날짜 파싱 실패시 현재 시간 사용
                            now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
                            date_kst = now_kst.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # 발행일이 없으면 현재 시간 사용
                        now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
                        date_kst = now_kst.strftime('%Y-%m-%d %H:%M:%S')
                    
                    crawled_articles.append(NewsArticle(
                        newsTitle=article.title,
                        newsUri=article_url,
                        newsContent=article.text,
                        newsDate=date_kst
                    ))
                    
                    logger.info(f"  - 성공: {article.title}")
                    time.sleep(0.8)  # 요청 간격 조절
                    
                except Exception as e:
                    logger.warning(f"  - 기사 처리 실패 (URL: {article_url}): {e}")
                    continue
        
        all_results["yahoo_finance_market_news"] = crawled_articles
        logger.info(f"Yahoo Finance 주요 증시뉴스 크롤링 완료: {len(crawled_articles)}개 기사 수집")
        
    except Exception as e:
        logger.error(f"Yahoo Finance 주요 증시뉴스 크롤링 중 오류: {e}", exc_info=True)
        all_results["yahoo_finance_market_news"] = []
    
    return all_results
