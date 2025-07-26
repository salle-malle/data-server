import json, logging, re, time
from datetime import datetime, date
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import tiktoken

# 1. 공통 설정 -------------------------------------------------------------
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# max_tokens 증가로 더 상세한 요약 생성
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, max_tokens=1200)
enc = tiktoken.encoding_for_model("gpt-4o-mini")

# 개선된 프롬프트 - 투자자 관점과 상세 맥락 추가
prompt = ChatPromptTemplate.from_template("""
당신은 SEC 8-K 공시를 분석하는 **금융 애널리스트**입니다. 
투자자와 비즈니스 전문가가 이해하기 쉽도록 **맥락과 함의를 포함한 상세 분석**을 제공하세요.

공시일: {filing_date}
문서 내용: {full_content}

다음 **완벽하게** JSON 형식으로 출력하세요:

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

# 가중치가 적용된 키워드 리스트 생성
ALL_KEYWORDS = []
for category, info in CRITICAL_KEYWORDS.items():
    for keyword in info["keywords"]:
        ALL_KEYWORDS.append((keyword.lower(), info["weight"]))

# 길이 순으로 정렬 (긴 키워드가 먼저 매칭되도록)
ALL_KEYWORDS.sort(key=lambda x: len(x[0]), reverse=True)

# 3. 유틸 함수들 ----------------------------------------------------------
def build_flexible_item_pattern(major: int, minor: int) -> str:
    return (
        rf"(?i)item[\s.\xa0]*{major}[\s.\xa0]*[\.:,]?\s*{minor}"
        r".*?(?=item[\s.\xa0]*\d+[\s.\xa0]*[\.:,]?\s*\d+|signature|$)"
    )

def extract_financial_numbers(text: str) -> list[str]:
    """텍스트에서 재무 관련 수치 추출"""
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
    
    return list(set(numbers))  # 중복 제거

class EnhancedDateExtractor:
    def __init__(self, default_date: str = None):
        """
        Args:
            default_date: 날짜 추출 실패 시 사용할 기본 날짜 (YYYY-MM-DD 형식)
                         None이면 오늘 날짜 사용
        """
        self.default_date = default_date or date.today().strftime("%Y-%m-%d")
        
        # SEC 8-K 특화 날짜 패턴 (우선순위 순)
        self.date_patterns = [
            # 1. SEC 헤더 형식
            r"(?i)date\s+of\s+report\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?i)date\s+of\s+earliest\s+event\s+reported\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?i)filing\s+date\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
            
            # 2. 표준 날짜 형식
            r"(\d{4}-\d{2}-\d{2})",                    # 2025-01-15
            r"(\d{1,2}/\d{1,2}/\d{4})",                # 1/15/2025
            r"(\d{1,2}-\d{1,2}-\d{4})",                # 1-15-2025
            
            # 3. 괄호 안의 날짜
            r"\((\w+\s+\d{1,2},?\s+\d{4})\)",          # (January 15, 2025)
            r"\((\d{1,2}/\d{1,2}/\d{4})\)",            # (1/15/2025)
            r"\((\d{4}-\d{2}-\d{2})\)",                # (2025-01-15)
            
            # 4. 문맥상 날짜
            r"(?i)(?:on|dated?|as\s+of|effective)\s+(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?i)(?:on|dated?|as\s+of|effective)\s+(\d{1,2}/\d{1,2}/\d{4})",
            
            # 5. 8-K 특화 패턴
            r"(?i)current\s+report\s+.*?(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?i)form\s+8-k\s+.*?(\w+\s+\d{1,2},?\s+\d{4})",
        ]
        
        # 날짜 형식 패턴 (파싱용)
        self.date_formats = [
            "%B %d, %Y",     # January 15, 2025
            "%b %d, %Y",     # Jan 15, 2025
            "%B %d %Y",      # January 15 2025
            "%b %d %Y",      # Jan 15 2025
            "%Y-%m-%d",      # 2025-01-15
            "%m/%d/%Y",      # 1/15/2025
            "%m-%d-%Y",      # 1-15-2025
            "%d/%m/%Y",      # 15/1/2025
            "%d-%m-%Y",      # 15-1-2025
        ]
    
    def extract_date_from_html(self, raw_html: str) -> str:
        """HTML에서 날짜 추출 (다중 전략)"""
        
        # 전략 1: HTML 메타데이터에서 추출
        soup = BeautifulSoup(raw_html, "html.parser")
        
        # SEC-HEADER에서 날짜 찾기
        header_date = self._extract_from_sec_header(soup)
        if header_date:
            return header_date
        
        # 전략 2: 텍스트에서 정규식 패턴 매칭
        plain_text = soup.get_text(" ")
        
        # 상위 2000자에서 우선 검색 (헤더 부분)
        header_text = plain_text[:2000]
        date_found = self._extract_with_patterns(header_text)
        if date_found:
            return date_found
        
        # 전체 텍스트에서 검색 (헤더에서 찾지 못한 경우)
        date_found = self._extract_with_patterns(plain_text)
        if date_found:
            return date_found
        
        # 전략 3: 특정 HTML 태그에서 추출
        date_found = self._extract_from_html_tags(soup)
        if date_found:
            return date_found
        
        # 전략 4: 기본값 반환
        logger.warning("날짜 추출 실패, 기본값 사용: %s", self.default_date)
        return self.default_date
    
    def _extract_from_sec_header(self, soup: BeautifulSoup) -> str:
        """SEC-HEADER에서 날짜 추출"""
        # SEC-HEADER 섹션 찾기
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
                    # 8자리 숫자 날짜 형식 (YYYYMMDD)
                    if isinstance(match, str) and match.isdigit() and len(match) == 8:
                        return self._format_date(match, "%Y%m%d")
                    
                    # 일반 날짜 패턴 검색
                    date_found = self._extract_with_patterns(match)
                    if date_found:
                        return date_found
        
        return None
    
    def _extract_with_patterns(self, text: str) -> str:
        """정규식 패턴으로 날짜 추출"""
        for pattern in self.date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                for match in matches:
                    formatted_date = self._format_date(match.strip())
                    if formatted_date:
                        return formatted_date
        return None
    
    def _extract_from_html_tags(self, soup: BeautifulSoup) -> str:
        """HTML 태그에서 날짜 추출"""
        # 날짜 관련 태그들 검색
        date_tags = ['time', 'date', 'span', 'div', 'p']
        date_attrs = ['datetime', 'date', 'data-date']
        
        for tag_name in date_tags:
            for tag in soup.find_all(tag_name):
                # 속성에서 날짜 찾기
                for attr in date_attrs:
                    if tag.get(attr):
                        formatted_date = self._format_date(tag.get(attr))
                        if formatted_date:
                            return formatted_date
                
                # 태그 내용에서 날짜 찾기
                if tag.string:
                    formatted_date = self._format_date(tag.string.strip())
                    if formatted_date:
                        return formatted_date
        
        return None
    
    def _format_date(self, date_str: str, known_format: str = None) -> str:
        """날짜 문자열을 YYYY-MM-DD 형식으로 변환"""
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        # 알려진 형식이 있으면 직접 파싱
        if known_format:
            try:
                return datetime.strptime(date_str, known_format).strftime("%Y-%m-%d")
            except ValueError:
                pass
        
        # 8자리 숫자 (YYYYMMDD) 처리
        if date_str.isdigit() and len(date_str) == 8:
            try:
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except:
                pass
        
        # 다양한 형식으로 시도
        for fmt in self.date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                # 미래 날짜 체크 (너무 미래면 제외)
                if parsed_date.year > datetime.now().year + 5:
                    continue
                # 너무 과거 날짜 체크 (1990년 이전이면 제외)
                if parsed_date.year < 1990:
                    continue
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        return None

# 5. 개선된 Smart8KExtractor -----------------------------------------------
class Smart8KExtractor:
    def __init__(self, default_date: str = None):
        """
        Args:
            default_date: 날짜 추출 실패 시 기본값 (YYYY-MM-DD)
        """
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

    def smart_filter(self, raw_html: str) -> dict:
        soup = BeautifulSoup(raw_html, "html.parser")
        
        # 불필요한 태그 제거
        for tag in soup.find_all(["header", "footer", "nav", "script", "style"]):
            tag.decompose()
        
        full_text = soup.get_text(separator=" ")
        
        # 테이블 정보 - 더 포괄적으로 수집
        tables = []
        for tbl in soup.find_all("table"):
            txt = tbl.get_text(" ", strip=True)
            # 더 많은 중요 키워드 포함
            if any(kw in txt.lower() for kw in [
                "merger", "agreement", "officer", "director", "shares", "vote", 
                "financial", "revenue", "earnings", "dividend", "debt"
            ]):
                tables.append(txt)
        
        # 강조된 텍스트 수집
        emphasizes = []
        for em in soup.find_all(["b", "strong", "em", "u"]):
            txt = em.get_text(" ", strip=True)
            if len(txt) > 10:  # 너무 짧은 텍스트 제외
                emphasizes.append(txt)
        
        return {
            "full_text": full_text,
            "tables": tables[:3],  # 3개로 증가
            "emphasized": emphasizes[:15]  # 15개로 증가
        }

    def extract_filing_date(self, raw_html: str) -> str:
        """강화된 날짜 추출"""
        return self.date_extractor.extract_date_from_html(raw_html)

    def extract_important_content(self, text: str) -> str:
        """더 풍부한 맥락 정보를 추출"""
        important_parts = []
        
        # 1) Item 패턴 매칭 - 더 긴 블록 허용
        for code, cfg in sorted(
            self.item_patterns.items(),
            key=lambda x: x[1]["importance"],
            reverse=True
        ):
            matches = re.findall(cfg["pattern"], text, re.DOTALL)
            if matches:
                block = matches[0].strip()
                if len(block) > 80:  # 최소 길이 80자로 증가
                    # 블록 크기를 1500자로 증가
                    important_parts.append(f"[{cfg['korean_name']}]\n{block[:1500]}")
        
        # 2) 키워드 기반 중요 문장 - 가중치 적용
        sentences = re.split(r"[.!?]+", text)
        weighted_sentences = []
        
        for sent in sentences:
            sent = sent.strip()
            if len(sent) < 40:  # 최소 길이 40자로 감소
                continue
            
            # 가중치 계산
            total_weight = 0
            for keyword, weight in ALL_KEYWORDS:
                if keyword in sent.lower():
                    total_weight += weight
            
            if total_weight >= 1.5:  # 가중치 기준으로 필터링
                weighted_sentences.append((sent, total_weight))
        
        # 가중치 순으로 정렬하여 상위 문장 선택
        weighted_sentences.sort(key=lambda x: x[1], reverse=True)
        for sent, weight in weighted_sentences[:12]:  # 12개로 증가
            important_parts.append(sent)
        
        # 3) 수치 정보 추가
        financial_numbers = extract_financial_numbers(text)
        if financial_numbers:
            numbers_text = "주요 수치: " + ", ".join(financial_numbers[:10])
            important_parts.append(numbers_text)
        
        # 4) 통합 및 토큰 제한
        combined = "\n\n".join(important_parts[:15])  # 15개 섹션으로 증가
        
        # 토큰 수 제한을 4500으로 증가
        if len(enc.encode(combined)) > 4500:
            combined = combined[:12000]  # 문자 수 제한도 증가
        
        return combined

# 6. 간소화 함수 ----------------------------------------------------------
def simplify_8k_results(results):
    """
    analyze_8k 함수의 결과에서 title, narrative, filing_date만 추출
    """
    simplified = []
    
    for result in results:
        simplified_item = {
            "title": result.get("title", "제목 없음"),
            "narrative": result.get("narrative", "내용 없음"),
            "filing_date": result.get("filing_date", "날짜 정보 없음")
        }
        simplified.append(simplified_item)
    
    return simplified

# 7. 📌 강화된 분석 함수 (날짜 보장) ----------------------------------------
def analyze_8k(docs: list[str], max_cost: float = 8.0, default_date: str = None) -> list[dict]:
    """
    SEC 8-K 문서 분석 (날짜 추출 강화)
    
    Args:
        docs: 분석할 문서 리스트
        max_cost: 최대 비용 한도
        default_date: 날짜 추출 실패 시 기본값 (YYYY-MM-DD)
    
    Returns:
        간소화된 분석 결과 리스트
    """
    if not docs:
        return []

    # 기본값 설정 (오늘 날짜)
    if default_date is None:
        default_date = date.today().strftime("%Y-%m-%d")

    extractor = Smart8KExtractor(default_date)
    results, total_cost = [], 0.0

    for idx, raw in enumerate(docs):
        logger.info("문서 %s 분석 중 (개선된 버전)...", idx + 1)
        
        # 토큰 비용 재계산 (더 많은 토큰 사용)
        est_tokens = len(enc.encode(raw[:15_000])) * 0.4  # 추정 비율 증가
        est_dollars = est_tokens / 1_000 * 0.00075
        if total_cost + est_dollars > max_cost:
            logger.warning("예산 초과 예상, 문서 %s 스킵", idx + 1)
            continue

        # 📌 강화된 날짜 추출
        filing_date = extractor.extract_filing_date(raw)
        logger.info("추출된 날짜: %s", filing_date)
        
        # 날짜 형식 검증 (YYYY-MM-DD)
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', filing_date):
            logger.warning("날짜 형식 불일치, 기본값 사용: %s", default_date)
            filing_date = default_date
        
        filtered = extractor.smart_filter(raw)
        
        # 더 풍부한 컨텍스트 추출
        important_content = extractor.extract_important_content(filtered["full_text"])
        
        # 테이블 정보 추가 (더 많은 정보)
        if filtered["tables"]:
            table_content = "\n".join(filtered["tables"])
            important_content += f"\n\n[테이블 정보]\n{table_content[:1500]}"  # 1500자로 증가
        
        # 강조된 텍스트 추가
        if filtered["emphasized"]:
            emphasized_content = "\n".join(filtered["emphasized"][:10])
            important_content += f"\n\n[강조된 내용]\n{emphasized_content}"

        # API 호출 전 딜레이
        if idx > 0:
            logger.info("API Rate Limit 방지를 위해 3초 대기...")
            time.sleep(3)  # 3초로 증가

        # 429 오류 재시도 로직
        max_retries = 5
        retry_delay = 2  # 초기 딜레이 2초로 증가
        
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
                "analysis_version": "v4_enhanced_date",  # 버전 정보 추가
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

    # 📌 개선된 빈 결과 처리 (날짜 보장)
    if not results:
        return [{
            "title": "분석 가능한 중요 공시 내용 없음",
            "narrative": "제공된 문서에서 투자자나 비즈니스 관점에서 중요한 정보를 찾을 수 없었습니다. 문서 형식이나 내용에 문제가 있거나, 중요도가 낮은 기술적 공시일 가능성이 있습니다.",
            "filing_date": default_date
        }]

    return simplify_8k_results(results)

# 8. 사용 예시 -------------------------------------------------------------
if __name__ == "__main__":
    # 예시 사용법
    docs = ["문서1 내용", "문서2 내용", "문서3 내용"]
    default_date = "None"
    results = analyze_8k(docs, default_date=default_date)
    
    # 결과 출력
    for result in results:
        print(f"제목: {result['title']}")
        print(f"내용: {result['narrative']}")
        print(f"날짜: {result['filing_date']}")  # 무조건 YYYY-MM-DD 형식
        print("-" * 50)