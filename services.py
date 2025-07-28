from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict
import time
import logging
import requests
from bs4 import BeautifulSoup
from newspaper import Article, Config
import yfinance as yf

from schemas import NewsArticle

logger = logging.getLogger(__name__)

def now_kst():
    return datetime.now(ZoneInfo("Asia/Seoul"))

async def crawl_and_process_news(tickers: List[str]) -> Dict[str, List[NewsArticle]]:
    results: Dict[str, List[NewsArticle]] = {}
    symbol_map = {"BRK/B": "BRK-B", "BRK.A": "BRK-A"}

    for ticker in tickers:
        try:
            yf_symbol = symbol_map.get(ticker, ticker)
            news_items = yf.Ticker(yf_symbol).get_news()

            articles: List[NewsArticle] = []
            for item in news_items[:5]:
                url = item.get("content", {}).get("canonicalUrl", {}).get("url")
                image = item.get("content", {}).get("thumbnail", {}).get("originalUrl")
                pub_date = item.get("content", {}).get("pubDate") or item.get("providerPublishTime")

                if not url or not pub_date:
                    continue

                try:
                    # pub_date를 UTC로 해석 후 KST로 변환
                    dt = datetime.fromtimestamp(pub_date, ZoneInfo("UTC")) if isinstance(pub_date, (int, float)) \
                        else datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                    date_kst = dt.astimezone(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')

                    article = Article(url, language='en')
                    article.download()
                    article.parse()

                    # createAt, updateAt 모두 한국 시간으로 보장
                    create_at = now_kst().strftime('%Y-%m-%d %H:%M:%S')
                    update_at = create_at

                    articles.append(NewsArticle(
                        newsTitle=article.title,
                        newsUri=url,
                        newsContent=article.text,
                        newsDate=date_kst,
                        newsImage=image or "",
                        createAt=create_at,
                        updateAt=update_at
                    ))
                    logger.info(f"  - 성공: {article.title}")
                except Exception as e:
                    logger.error(f"  - 기사 처리 중 오류 발생 (URL: {url}): {e}")
            results[ticker] = articles
        except Exception as e:
            logger.error(f"'{ticker}' 처리 중 오류 발생: {e}", exc_info=True)
            results[ticker] = []
        time.sleep(1)

    return results


def crawl_yahoo_stock_market_news(limit: int = 10) -> Dict[str, List[NewsArticle]]:
    url = "https://finance.yahoo.com/topic/stock-market-news/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        logger.error(f"Yahoo Finance 페이지 로딩 실패: {e}", exc_info=True)
        return {"yahoo_finance_market_news": []}

    articles: List[NewsArticle] = []
    seen_urls = set()
    selectors = [
        "a[href*='/news/']", 
        "h3 a[href*='finance.yahoo.com']", 
        ".js-content-viewer", 
        "[data-module='FinanceStream'] a"
    ]

    config = Config()
    config.browser_user_agent = headers["User-Agent"]
    config.request_timeout = 10

    for selector in selectors:
        for link in soup.select(selector):
            if len(articles) >= limit:
                break
            href = link.get("href")
            if not href:
                continue
            full_url = href if href.startswith("http") else f"https://finance.yahoo.com{href}"
            if (full_url in seen_urls or 'finance.yahoo.com/news/' not in full_url or
                    full_url.endswith("/news/") or 'sign-up-for-yahoo-finances-morning-brief' in full_url):
                continue
            seen_urls.add(full_url)

            try:
                article = Article(full_url, language='en', config=config)
                article.download()
                article.parse()

                if not article.title or not article.text:
                    continue

                publish_dt = article.publish_date or datetime.now(ZoneInfo("UTC"))
                if publish_dt.tzinfo is None:
                    publish_dt = publish_dt.replace(tzinfo=ZoneInfo("UTC"))
                kst_dt = publish_dt.astimezone(ZoneInfo("Asia/Seoul"))

                articles.append(NewsArticle(
                    newsTitle=article.title,
                    newsUri=full_url,
                    newsContent=article.text,
                    newsDate=kst_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    newsImage=article.top_image or ""
                ))
                logger.info(f"  - 성공: {article.title}")
                time.sleep(0.8)
            except Exception as e:
                logger.warning(f"  - 기사 처리 실패 (URL: {full_url}): {e}")

    logger.info(f"Yahoo Finance 주요 증시뉴스 크롤링 완료: {len(articles)}개 기사 수집")
    return {"yahoo_finance_market_news": articles}
