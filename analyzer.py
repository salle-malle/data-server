import json, logging, re, time, chardet, gzip
from datetime import datetime, date
from bs4 import BeautifulSoup, UnicodeDammit
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import tiktoken

# 1. 공통 설정 -------------------------------------------------------------
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, max_tokens=1200)
enc = tiktoken.encoding_for_model("gpt-4o-mini")

prompt = ChatPromptTemplate.from_template("""
당신은 SEC 8-K 공시를 분석하는 **금융 애널리스트**입니다. 
투자자와 비즈니스 전문가가 이해하기 쉽도록 **맥락과 함의를 포함한 상세 분석**을 제공하세요.

공시일: {filing_date}
문서 내 형식으로 출력하세요:

{{
  "title": "공시 핵심 내용을 반영한 구체적인 한국어 제목 (50자 이내)",
  "narrative": "공시 내용을 스토리텔링 방식으로 설명한 2-3개 단락 (각 단락 4-5문장, 연결어 사용으로 흐름 유지)",
  "filing_date": "{filing_date}"
}}

중요 지침:
- narrative는 **연결어와 맥락**을 사용해 스토리처럼 서술
- 단순 번역이 아닌 **비즈니스 맥락과 함의** 포함
- 수치나 날짜는 정확히 인용, 추측 금지
- 모든 내용은 자연스러운 한국어로 작성
""")

# 2. 핵심 키워드 세트 - 확장 및 가중치 추가 ------------------------------
CRITICAL_KEYWORDS = {
    "merger_acquisition": {
        "keywords": ["merger", "acquisition", "agreement", "definitive", "purchase", "sale", "disposition", "divestiture"],
        "weight": 1.5
    },
    "executive_changes": {
        "keywords": ["resign", "appointment", "terminate", "ceo", "cfo", "director", "president", "officer", "retire"],
        "weight": 1.3
    },
    "financial_events": {
        "keywords": ["earnings", "revenue", "loss", "dividend", "bankruptcy", "impairment", "writedown", "restructuring"],
        "weight": 1.4
    },
    "material_agreements": {
        "keywords": ["material definitive agreement", "joint venture", "partnership", "contract", "license"],
        "weight": 1.2
    },
    "legal_regulatory": {
        "keywords": ["lawsuit", "settlement", "regulatory", "compliance", "investigation", "sec", "doj"],
        "weight": 1.1
    }
}

ALL_KEYWORDS = []
for category, info in CRITICAL_KEYWORDS.items():
    for keyword in info["keywords"]:
        ALL_KEYWORDS.append((keyword.lower(), info["weight"]))

ALL_KEYWORDS.sort(key=lambda x: len(x[0]), reverse=True)

# 3. 유틸 함수들 ----------------------------------------------------------
def build_flexible_item_pattern(major: int, minor: int) -> str:
    return (
        rf"(?i)item[\s.\xa0]*{major}[\s.\xa0]*[\.:,]?\s*{minor}"
        r".*?(?=item[\s.\xa0]*\d+[\s.\xa0]*[\.:,]?\s*\d+|signature|$)"
    )

def extract_financial_numbers(text: str) -> list[str]:
    patterns = [
        r'\$[\d,]+(?:\.\d{2})?(?:\s*(?:million|billion|trillion|백만|억|조))?',
        r'[\d,]+(?:\.\d{2})?\s*(?:million|billion|trillion|백만|억|조)\s*(?:달러|dollars?)',
        r'[\d,]+(?:\.\d{2})?\s*(?:shares?|주식|주)',
        r'[\d,]+(?:\.\d{2})?\s*(?:percent|%)',
    ]
    
    numbers = []
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        numbers.extend(matches)
    
    return list(set(numbers))

# ★★★ 핵심: 안전한 BeautifulSoup 파싱 함수 (ParserRejectedMarkup 오류 방지) ★★★
def safe_soup(raw_html) -> BeautifulSoup:
    """
    SEC 8-K 문서의 손상된 HTML/바이너리 데이터로 인한 ParserRejectedMarkup 오류 방지
    - gzip 압축 해제
    - 제어문자 제거 
    - 인코딩 자동 감지
    - html.parser → lxml → html5lib 순서로 폴백
    """
    try:
        # bytes 타입 처리
        if isinstance(raw_html, bytes):
            # GZIP 압축 해제 (SEC 파일에 종종 포함됨)
            if raw_html[:2] == b"\x1f\x8b":
                try:
                    raw_html = gzip.decompress(raw_html)
                    logger.debug("GZIP 압축 해제 완료")
                except Exception as e:
                    logger.warning(f"GZIP 압축 해제 실패: {e}")
            
            # 제어문자 제거 (ASCII 범위 외 문자)
            cleaned = re.sub(rb"[^\x09\x0A\x0D\x20-\x7E]", b" ", raw_html)
            
            # 인코딩 감지 및 디코딩
            enc_info = chardet.detect(cleaned)
            encoding = enc_info["encoding"] or "utf-8"
            confidence = enc_info.get("confidence", 0)
            
            logger.debug(f"감지된 인코딩: {encoding} (신뢰도: {confidence:.2f})")
            
            # 안전한 디코딩
            text = cleaned.decode(encoding, errors="replace")
        else:
            # str 타입 처리
            text = str(raw_html)
            # 유니코드 제어문자 제거
            text = re.sub(r"[^\x09\x0A\x0D\x20-\x7E]", " ", text)
        
        # 다중 파서 시도 (관대함 순서: html5lib > lxml > html.parser)
        for parser in ("html5lib", "lxml", "html.parser"):
            try:
                soup = BeautifulSoup(text, parser)
                logger.debug(f"파싱 성공: {parser}")
                return soup
            except Exception as e:
                logger.debug(f"{parser} 파싱 실패: {e}")
                continue
        
        # 마지막 수단: UnicodeDammit 사용
        logger.warning("모든 파서 실패, UnicodeDammit 사용")
        return BeautifulSoup(UnicodeDammit(text).unicode_markup, "html.parser")
        
    except Exception as e:
        logger.error(f"safe_soup 완전 실패: {e}")
        return BeautifulSoup("", "html.parser")

class EnhancedDateExtractor:
    def __init__(self, default_date: str = None):
        self.default_date = default_date or date.today().strftime("%Y-%m-%d")
        
        self.date_patterns = [
            r"(?i)date\s+of\s+report\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?i)date\s+of\s+earliest\s+event\s+reported\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?i)filing\s+date\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
            r"(\d{4}-\d{2}-\d{2})",
            r"(\d{1,2}/\d{1,2}/\d{4})",
            r"(\d{1,2}-\d{1,2}-\d{4})",
            r"\((\w+\s+\d{1,2},?\s+\d{4})\)",
            r"\((\d{1,2}/\d{1,2}/\d{4})\)",
            r"\((\d{4}-\d{2}-\d{2})\)",
            r"(?i)(?:on|dated?|as\s+of|effective)\s+(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?i)(?:on|dated?|as\s+of|effective)\s+(\d{1,2}/\d{1,2}/\d{4})",
            r"(?i)current\s+report\s+.*?(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?i)form\s+8-k\s+.*?(\w+\s+\d{1,2},?\s+\d{4})",
        ]
        
        self.date_formats = [
            "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y",
            "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y", "%d-%m-%Y",
        ]
    
    def extract_date_from_html(self, raw_html) -> str:
        """★★★ 핵심: safe_soup 사용으로 안전한 날짜 추출 ★★★"""
        
        # 안전한 파싱 사용
        soup = safe_soup(raw_html)
        
        # SEC-HEADER에서 날짜 찾기
        header_date = self._extract_from_sec_header(soup)
        if header_date:
            return header_date
        
        # 텍스트에서 정규식 패턴 매칭
        plain_text = soup.get_text(" ")
        
        # 상위 2000자에서 우선 검색
        header_text = plain_text[:2000]
        date_found = self._extract_with_patterns(header_text)
        if date_found:
            return date_found
        
        # 전체 텍스트에서 검색
        date_found = self._extract_with_patterns(plain_text)
        if date_found:
            return date_found
        
        # HTML 태그에서 추출
        date_found = self._extract_from_html_tags(soup)
        if date_found:
            return date_found
        
        # 기본값 반환
        logger.warning("날짜 추출 실패, 기본값 사용: %s", self.default_date)
        return self.default_date
    
    def _extract_from_sec_header(self, soup: BeautifulSoup) -> str:
        header_patterns = [
            r"<SEC-HEADER>.*?</SEC-HEADER>",
            r"SEC-HEADER.*?(?=<|$)",
            r"CONFORMED\s+PERIOD\s+OF\s+REPORT[:\s]+(\d{8})",
            r"FILED\s+AS\s+OF\s+DATE[:\s]+(\d{8})",
        ]
        
        text = str(soup)
        for pattern in header_patterns:
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            if matches:
                for match in matches:
                    if isinstance(match, str) and match.isdigit() and len(match) == 8:
                        return self._format_date(match, "%Y%m%d")
                    
                    date_found = self._extract_with_patterns(match)
                    if date_found:
                        return date_found
        
        return None
    
    def _extract_with_patterns(self, text: str) -> str:
        for pattern in self.date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                for match in matches:
                    formatted_date = self._format_date(match.strip())
                    if formatted_date:
                        return formatted_date
        return None
    
    def _extract_from_html_tags(self, soup: BeautifulSoup) -> str:
        date_tags = ['time', 'date', 'span', 'div', 'p']
        date_attrs = ['datetime', 'date', 'data-date']
        
        for tag_name in date_tags:
            for tag in soup.find_all(tag_name):
                for attr in date_attrs:
                    if tag.get(attr):
                        formatted_date = self._format_date(tag.get(attr))
                        if formatted_date:
                            return formatted_date
                
                if tag.string:
                    formatted_date = self._format_date(tag.string.strip())
                    if formatted_date:
                        return formatted_date
        
        return None
    
    def _format_date(self, date_str: str, known_format: str = None) -> str:
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        if known_format:
            try:
                return datetime.strptime(date_str, known_format).strftime("%Y-%m-%d")
            except ValueError:
                pass
        
        if date_str.isdigit() and len(date_str) == 8:
            try:
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except:
                pass
        
        for fmt in self.date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                if parsed_date.year > datetime.now().year + 5:
                    continue
                if parsed_date.year < 1990:
                    continue
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        return None

class Smart8KExtractor:
    def __init__(self, default_date: str = None):
        self.date_extractor = EnhancedDateExtractor(default_date)
        
        self.item_patterns = {
            "ITEM_1_01": {
                "pattern": build_flexible_item_pattern(1, 1),
                "importance": 8,
                "korean_name": "중요 계약 체결",
                "keywords": ["agreement", "contract", "definitive", "material"]
            },
            "ITEM_1_02": {
                "pattern": build_flexible_item_pattern(1, 2),
                "importance": 7,
                "korean_name": "계약 종료",
                "keywords": ["termination", "terminate", "end", "expire"]
            },
            "ITEM_2_01": {
                "pattern": build_flexible_item_pattern(2, 1),
                "importance": 9,
                "korean_name": "자산 인수/매각",
                "keywords": ["acquisition", "merger", "purchase", "sale", "dispose", "acquire"]
            },
            "ITEM_2_02": {
                "pattern": build_flexible_item_pattern(2, 2),
                "importance": 8,
                "korean_name": "실적 발표",
                "keywords": ["earnings", "revenue", "results", "financial", "quarter", "fiscal"]
            },
            "ITEM_3_02": {
                "pattern": build_flexible_item_pattern(3, 2),
                "importance": 6,
                "korean_name": "주식 매각",
                "keywords": ["sale", "equity", "stock", "shares"]
            },
            "ITEM_4_01": {
                "pattern": build_flexible_item_pattern(4, 1),
                "importance": 6,
                "korean_name": "감사인 변경",
                "keywords": ["auditor", "accountant", "change"]
            },
            "ITEM_5_02": {
                "pattern": build_flexible_item_pattern(5, 2),
                "importance": 9,
                "korean_name": "임원 변경",
                "keywords": ["appoint", "resign", "retire", "ceo", "cfo", "president", "officer", "director"]
            },
            "ITEM_7_01": {
                "pattern": build_flexible_item_pattern(7, 1),
                "importance": 7,
                "korean_name": "규제 절차",
                "keywords": ["regulatory", "proceeding", "investigation"]
            },
            "ITEM_8_01": {
                "pattern": build_flexible_item_pattern(8, 1),
                "importance": 5,
                "korean_name": "기타 중요 사건",
                "keywords": ["other", "event", "material"]
            },
            "ITEM_9_01": {
                "pattern": build_flexible_item_pattern(9, 1),
                "importance": 4,
                "korean_name": "재무제표 및 전시물",
                "keywords": ["financial", "statements", "exhibits"]
            }
        }

    def smart_filter(self, raw_html) -> dict:
        """★★★ 핵심: safe_soup 사용으로 안전한 필터링 ★★★"""
        soup = safe_soup(raw_html)
        
        # 불필요한 태그 제거
        for tag in soup.find_all(["header", "footer", "nav", "script", "style"]):
            tag.decompose()
        
        full_text = soup.get_text(separator=" ")
        
        # 테이블 정보 수집
        tables = []
        for tbl in soup.find_all("table"):
            txt = tbl.get_text(" ", strip=True)
            if any(kw in txt.lower() for kw in [
                "merger", "agreement", "officer", "director", "shares", "vote", 
                "financial", "revenue", "earnings", "dividend", "debt"
            ]):
                tables.append(txt)
        
        # 강조된 텍스트 수집
        emphasizes = []
        for em in soup.find_all(["b", "strong", "em", "u"]):
            txt = em.get_text(" ", strip=True)
            if len(txt) > 10:
                emphasizes.append(txt)
        
        return {
            "full_text": full_text,
            "tables": tables[:3],
            "emphasized": emphasizes[:15]
        }

    def extract_filing_date(self, raw_html) -> str:
        """★★★ 핵심: 안전한 날짜 추출 ★★★"""
        return self.date_extractor.extract_date_from_html(raw_html)

    def extract_important_content(self, text: str) -> str:
        important_parts = []
        
        # Item 패턴 매칭
        for code, cfg in sorted(
            self.item_patterns.items(),
            key=lambda x: x[1]["importance"],
            reverse=True
        ):
            matches = re.findall(cfg["pattern"], text, re.DOTALL)
            if matches:
                block = matches[0].strip()
                if len(block) > 80:
                    important_parts.append(f"[{cfg['korean_name']}]\n{block[:1500]}")
        
        # 키워드 기반 중요 문장
        sentences = re.split(r"[.!?]+", text)
        weighted_sentences = []
        
        for sent in sentences:
            sent = sent.strip()
            if len(sent) < 40:
                continue
            
            total_weight = 0
            for keyword, weight in ALL_KEYWORDS:
                if keyword in sent.lower():
                    total_weight += weight
            
            if total_weight >= 1.5:
                weighted_sentences.append((sent, total_weight))
        
        weighted_sentences.sort(key=lambda x: x[1], reverse=True)
        for sent, weight in weighted_sentences[:12]:
            important_parts.append(sent)
        
        # 수치 정보 추가
        financial_numbers = extract_financial_numbers(text)
        if financial_numbers:
            numbers_text = "주요 수치: " + ", ".join(financial_numbers[:10])
            important_parts.append(numbers_text)
        
        # 통합 및 토큰 제한
        combined = "\n\n".join(important_parts[:15])
        
        if len(enc.encode(combined)) > 4500:
            combined = combined[:12000]
        
        return combined

def simplify_8k_results(results):
    simplified = []
    
    for result in results:
        simplified_item = {
            "title": result.get("title", "제목 없음"),
            "narrative": result.get("narrative", "내용 없음"),
            "filing_date": result.get("filing_date", "날짜 정보 없음")
        }
        simplified.append(simplified_item)
    
    return simplified

def analyze_8k(docs: list[str], max_cost: float = 8.0, default_date: str = None) -> list[dict]:
    """
    ★★★ 핵심: SEC 8-K 문서 분석 (ParserRejectedMarkup 오류 완전 해결) ★★★
    """
    if not docs:
        return []

    if default_date is None:
        default_date = date.today().strftime("%Y-%m-%d")

    extractor = Smart8KExtractor(default_date)
    results, total_cost = [], 0.0

    for idx, raw in enumerate(docs):
        logger.info("문서 %s 분석 중 (안전 파싱 버전)...", idx + 1)
        
        # 토큰 비용 계산
        est_tokens = len(enc.encode(raw[:15_000])) * 0.4
        est_dollars = est_tokens / 1_000 * 0.00075
        if total_cost + est_dollars > max_cost:
            logger.warning("예산 초과 예상, 문서 %s 스킵", idx + 1)
            continue

        # ★★★ 핵심: 안전한 날짜 추출 ★★★
        filing_date = extractor.extract_filing_date(raw)
        logger.info("추출된 날짜: %s", filing_date)
        
        # 날짜 형식 검증
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', filing_date):
            logger.warning("날짜 형식 불일치, 기본값 사용: %s", default_date)
            filing_date = default_date
        
        # ★★★ 핵심: 안전한 필터링 ★★★
        filtered = extractor.smart_filter(raw)
        
        # 중요 컨텐츠 추출
        important_content = extractor.extract_important_content(filtered["full_text"])
        
        # 테이블 정보 추가
        if filtered["tables"]:
            table_content = "\n".join(filtered["tables"])
            important_content += f"\n\n[테이블 정보]\n{table_content[:1500]}"
        
        # 강조된 텍스트 추가
        if filtered["emphasized"]:
            emphasized_content = "\n".join(filtered["emphasized"][:10])
            important_content += f"\n\n[강조된 내용]\n{emphasized_content}"

        # API 호출 전 딜레이
        if idx > 0:
            logger.info("API Rate Limit 방지를 위해 8초 대기...")
            time.sleep(8)

        # 429 오류 재시도 로직
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = (prompt | llm | StrOutputParser()).invoke({
                    "filing_date": filing_date,
                    "full_content": important_content
                })
                break
                
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "rate" in error_msg.lower():
                    logger.warning(
                        "429 오류 발생. %s초 후 재시도 (%d/%d)",
                        retry_delay, attempt + 1, max_retries
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    if attempt == max_retries - 1:
                        logger.error("최대 재시도 횟수 초과. 문서 %s 스킵", idx + 1)
                        response = None
                else:
                    logger.error("API 호출 실패: %s", e)
                    response = None
                    break

        if response is None:
            continue

        try:
            record = json.loads(response)
            record_date = record.get("filing_date", filing_date)
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', record_date):
                record["filing_date"] = filing_date
                logger.info("응답 날짜 형식 보정: %s", filing_date)
            
            record.update({
                "document_index": idx + 1,
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "analysis_version": "v5_safe_parsing_complete",
                "content_length": len(important_content)
            })
            
            # 비용 계산
            token_in = len(enc.encode(important_content))
            token_out = len(enc.encode(response))
            cost = token_in * 0.00015 / 1_000 + token_out * 0.00060 / 1_000
            total_cost += cost
            
            results.append(record)
            logger.info("문서 %s 분석 완료 (비용 +$%.4f, 토큰: %d)", idx + 1, cost, token_in)
            
        except json.JSONDecodeError as e:
            logger.error("JSON 파싱 실패 - 문서 %s: %s", idx + 1, e)
            continue

        if total_cost >= max_cost:
            logger.info("예산 소진, 추가 문서 중단")
            break

    logger.info("분석 완료: %s건, 총 비용 $%.4f", len(results), total_cost)

    if not results:
        return [{
            "title": "분석 가능한 중요 공시 내용 없음",
            "narrative": "제공된 문서에서 투자자나 비즈니스 관점에서 중요한 정보를 찾을 수 없었습니다. 문서 형식이나 내용에 문제가 있거나, 중요도가 낮은 기술적 공시일 가능성이 있습니다.",
            "filing_date": default_date
        }]

    return simplify_8k_results(results)

if __name__ == "__main__":
    docs = ["테스트 문서"]
    results = analyze_8k(docs, default_date="2025-07-27")
    
    for result in results:
        print(f"제목: {result['title']}")
        print(f"내용: {result['narrative']}")
        print(f"날짜: {result['filing_date']}")
        print("-" * 50)
