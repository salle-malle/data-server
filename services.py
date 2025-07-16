# services.py

import datetime
import zoneinfo
import yfinance as yf
from newspaper import Article
import logging
import requests  
import time     

from schemas import NewsArticle
from typing import List, Dict

logger = logging.getLogger(__name__)

def crawl_and_process_news(tickers: List[str]) -> Dict[str, List[NewsArticle]]:
    all_results: Dict[str, List[NewsArticle]] = {}
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })

    for ticker_symbol in tickers:
        try:
            logger.info(f"'{ticker_symbol}' 뉴스 크롤링 시작...")
            ticker = yf.Ticker(ticker_symbol)
            
            logger.info(f"  - 성공: {ticker}")
            news_list = ticker.get_news()
            # with open("news-list.json", 'w') as f:
            #     f.write(json.dumps(news_list, indent=2))
            # logger.info(f"  - 성공: {str(news_list)}")
            
            
            crawled_articles: List[NewsArticle] = []
            for news_item in news_list[:5]:
                try:
                    url = news_item['content']['canonicalUrl']['url']
                    pub_date = news_item['content']['pubDate']
                    utc_time = datetime.datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                    kst_zone = zoneinfo.ZoneInfo("Asia/Seoul")
                    kst_time = utc_time.astimezone(kst_zone)
                    date_kst = kst_time.strftime('%Y-%m-%d %H:%M:%S.%f')
                    article = Article(url, language='en')
                    article.download()
                    article.parse()

                    crawled_articles.append(NewsArticle(
                        news_title=article.title,
                        source_uri=url,
                        news_content=article.text,
                        date=date_kst  # 변환된 문자열 날짜를 저장
                    ))
                    # logger.info({article.publish_date})
                    logger.info(f"  - 성공: {article.title}")
                except Exception as e:
                    logger.error(f"  - 기사 처리 중 오류 발생 (URL: {news_item.get('link')}): {e}")
                    continue
            
            all_results[ticker_symbol] = crawled_articles
            logger.info(f"'{ticker_symbol}' 뉴스 크롤링 완료. {len(crawled_articles)}개 기사 수집.")

        except Exception as e:
            # 에러 메시지를 더 자세히 로깅
            logger.error(f"'{ticker_symbol}' Ticker 처리 중 심각한 오류 발생: {e}", exc_info=True)
            all_results[ticker_symbol] = []
        
        # 5. 서버에 부담을 줄이기 위해 각 티커 처리 후 잠시 대기
        time.sleep(1)

    return all_results
