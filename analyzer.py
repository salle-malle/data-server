"""
analyzer.py

SEC 8-K 문서를 안정적으로 파싱·요약하기 위한 분석기
- BeautifulSoup ParserRejectedMarkup 오류 방지(제어문 제거 + 다중 파서 폴백)
- gzip · 잘못된 인코딩 자동 처리
- LangChain-OpenAI를 이용해 한국어 요약(JSON 포맷) 생성
"""

import json
import logging
import re
import time
import gzip
from datetime import datetime, date

import chardet
from bs4 import BeautifulSoup, UnicodeDammit
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import tiktoken

# ────────────────────────────── 1. 공통 설정 ──────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, max_tokens=1200)
enc = tiktoken.encoding_for_model("gpt-4o-mini")

PROMPT = ChatPromptTemplate.from_template(
    """
당신은 SEC 8-K 공시를 분석하는 **금융 애널리스트**입니다. 
투자자와 비즈니스 전문가가 이해하기 쉽도록 **맥락과 함의를 포함한 상세 분석**을 제공하세요.

공시일: {filing_date}
문서 내용: {full_content}

다음 JSON 형식으로 출력하세요:

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
"""
)

# ─────────────────────── 2. 키워드 및 정규식 설정 ────────────────────────
CRITICAL_KEYWORDS = {
    "merger_acquisition": {
        "keywords": [
            "merger",
            "acquisition",
            "agreement",
            "definitive",
            "purchase",
            "sale",
            "disposition",
            "divestiture",
        ],
        "weight": 1.5,
    },
    "executive_changes": {
        "keywords": [
            "resign",
            "appointment",
            "terminate",
            "ceo",
            "cfo",
            "director",
            "president",
            "officer",
            "retire",
        ],
        "weight": 1.3,
    },
    "financial_events": {
        "keywords": [
            "earnings",
            "revenue",
            "loss",
            "dividend",
            "bankruptcy",
            "impairment",
            "writedown",
            "restructuring",
        ],
        "weight": 1.4,
    },
    "material_agreements": {
        "keywords": [
            "material definitive agreement",
            "joint venture",
            "partnership",
            "contract",
            "license",
        ],
        "weight": 1.2,
    },
    "legal_regulatory": {
        "keywords": [
            "lawsuit",
            "settlement",
            "regulatory",
            "compliance",
            "investigation",
            "sec",
            "doj",
        ],
        "weight": 1.1,
    },
}

ALL_KEYWORDS = [
    (kw.lower(), info["weight"]) for info in CRITICAL_KEYWORDS.values() for kw in info["keywords"]
]
ALL_KEYWORDS.sort(key=lambda x: len(x[0]), reverse=True)  # 긴 키워드 우선

# ────────────────────────── 3. 헬퍼 / 유틸 함수 ──────────────────────────
def build_item_pattern(major: int, minor: int) -> str:
    return (
        rf"(?i)item[\s.\xa0]*{major}[\s.\xa0]*[\.:,]?\s*{minor}"
        r".*?(?=item[\s.\xa0]*\d+[\s.\xa0]*[\.:,]?\s*\d+|signature|$)"
    )


def extract_financial_numbers(text: str) -> list[str]:
    patterns = [
        r"\$[\d,]+(?:\.\d{2})?(?:\s*(?:million|billion|trillion|백만|억|조))?",
        r"[\d,]+(?:\.\d{2})?\s*(?:million|billion|trillion|백만|억|조)\s*(?:달러|dollars?)",
        r"[\d,]+(?:\.\d{2})?\s*(?:shares?|주식|주)",
        r"[\d,]+(?:\.\d{2})?\s*(?:percent|%)",
    ]
    out: list[str] = []
    for p in patterns:
        out.extend(re.findall(p, text, flags=re.IGNORECASE))
    return list(set(out))


def safe_soup(raw_html) -> BeautifulSoup:
    """
    ParserRejectedMarkup 방지용 안전 파서.
    bytes/str 모두 입력 가능하며,
    - gzip 해제
    - 제어문자 제거
    - 인코딩 자동 감지
    - html.parser → lxml → html5lib 폴백
    """
    try:
        # ───── bytes 처리 ─────
        if isinstance(raw_html, bytes):
            if raw_html.startswith(b"\x1f\x8b"):
                try:
                    raw_html = gzip.decompress(raw_html)
                except Exception:
                    pass
            cleaned = re.sub(rb"[^\x09\x0A\x0D\x20-\x7E]", b" ", raw_html)
            enc_guess = chardet.detect(cleaned)["encoding"] or "utf-8"
            text = cleaned.decode(enc_guess, errors="replace")
        else:
            # ───── str 처리 ─────
            text = re.sub(r"[^\x09\x0A\x0D\x20-\x7E]", " ", str(raw_html))

        for parser in ("html.parser", "lxml", "html5lib"):
            try:
                return BeautifulSoup(text, parser)
            except Exception:
                continue

        # 마지막 수단
        return BeautifulSoup(UnicodeDammit(text).unicode_markup, "html.parser")
    except Exception as e:
        logger.error("safe_soup 실패: %s", e)
        return BeautifulSoup("", "html.parser")


# ──────────────────────── 4. 날짜 추출 로직 클래스 ────────────────────────
class EnhancedDateExtractor:
    def __init__(self, default_date: str | None = None):
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
            "%B %d, %Y",
            "%b %d, %Y",
            "%B %d %Y",
            "%b %d %Y",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%d/%m/%Y",
            "%d-%m-%Y",
        ]

    def extract_date(self, raw_html) -> str:
        soup = safe_soup(raw_html)

        # 1) SEC-HEADER 영역 우선
        header_date = self._from_sec_header(soup)
        if header_date:
            return header_date

        text = soup.get_text(" ")
        if (date_found := self._with_patterns(text[:2000])) :
            return date_found
        if (date_found := self._with_patterns(text)) :
            return date_found
        if (date_found := self._from_tags(soup)) :
            return date_found

        logger.warning("날짜 추출 실패, 기본값 반환: %s", self.default_date)
        return self.default_date

    # ───── 내부 helper ─────
    def _with_patterns(self, txt: str) -> str | None:
        for pat in self.date_patterns:
            for m in re.findall(pat, txt, flags=re.IGNORECASE):
                if (fd := self._format(m)):
                    return fd
        return None

    def _from_sec_header(self, soup: BeautifulSoup) -> str | None:
        patterns = [
            r"<SEC-HEADER>.*?</SEC-HEADER>",
            r"SEC-HEADER.*?(?=<|$)",
            r"CONFORMED\s+PERIOD\s+OF\s+REPORT[:\s]+(\d{8})",
            r"FILED\s+AS\s+OF\s+DATE[:\s]+(\d{8})",
        ]
        text = str(soup)
        for p in patterns:
            for m in re.findall(p, text, flags=re.DOTALL | re.IGNORECASE):
                if isinstance(m, str) and m.isdigit() and len(m) == 8:
                    return self._format(m, "%Y%m%d")
                if (fd := self._with_patterns(m)):
                    return fd
        return None

    def _from_tags(self, soup: BeautifulSoup) -> str | None:
        for tag in soup.find_all(["time", "date", "span", "div", "p"]):
            for attr in ["datetime", "date", "data-date"]:
                if tag.get(attr) and (fd := self._format(tag[attr])):
                    return fd
            if tag.string and (fd := self._format(tag.string.strip())):
                return fd
        return None

    def _format(self, ds: str, known: str | None = None) -> str | None:
        ds = ds.strip()
        try:
            if known:
                return datetime.strptime(ds, known).strftime("%Y-%m-%d")
            if ds.isdigit() and len(ds) == 8:
                return f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
            for fmt in self.date_formats:
                try:
                    dt = datetime.strptime(ds, fmt)
                    if 1990 <= dt.year <= datetime.now().year + 5:
                        return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        except Exception:
            pass
        return None


# ────────────────────── 5. Smart8KExtractor 클래스 ──────────────────────
class Smart8KExtractor:
    def __init__(self, default_date: str | None = None):
        self.date_extractor = EnhancedDateExtractor(default_date)
        # Item 패턴 정의
        self.item_patterns: dict[str, dict] = {
            "ITEM_1_01": {
                "pattern": build_item_pattern(1, 1),
                "importance": 8,
                "korean": "중요 계약 체결",
            },
            "ITEM_1_02": {
                "pattern": build_item_pattern(1, 2),
                "importance": 7,
                "korean": "계약 종료",
            },
            "ITEM_2_01": {
                "pattern": build_item_pattern(2, 1),
                "importance": 9,
                "korean": "자산 인수/매각",
            },
            "ITEM_2_02": {
                "pattern": build_item_pattern(2, 2),
                "importance": 8,
                "korean": "실적 발표",
            },
            "ITEM_3_02": {
                "pattern": build_item_pattern(3, 2),
                "importance": 6,
                "korean": "주식 매각",
            },
            "ITEM_4_01": {
                "pattern": build_item_pattern(4, 1),
                "importance": 6,
                "korean": "감사인 변경",
            },
            "ITEM_5_02": {
                "pattern": build_item_pattern(5, 2),
                "importance": 9,
                "korean": "임원 변경",
            },
            "ITEM_7_01": {
                "pattern": build_item_pattern(7, 1),
                "importance": 7,
                "korean": "규제 절차",
            },
            "ITEM_8_01": {
                "pattern": build_item_pattern(8, 1),
                "importance": 5,
                "korean": "기타 중요 사건",
            },
            "ITEM_9_01": {
                "pattern": build_item_pattern(9, 1),
                "importance": 4,
                "korean": "재무제표 및 전시물",
            },
        }

    # ───── 핵심 메서드 ─────
    def extract_filing_date(self, raw_html) -> str:
        return self.date_extractor.extract_date(raw_html)

    def smart_filter(self, raw_html) -> dict:
        soup = safe_soup(raw_html)
        for t in soup.find_all(["header", "footer", "nav", "script", "style"]):
            t.decompose()
        full_text = soup.get_text(" ")

        # 테이블 / 강조 텍스트 추출
        tables = [
            tbl.get_text(" ", strip=True)
            for tbl in soup.find_all("table")
            if any(
                kw in tbl.get_text(" ", strip=True).lower()
                for kw in [
                    "merger",
                    "agreement",
                    "officer",
                    "director",
                    "shares",
                    "vote",
                    "financial",
                    "revenue",
                    "earnings",
                    "dividend",
                    "debt",
                ]
            )
        ][:3]

        emphasizes = [
            em.get_text(" ", strip=True)
            for em in soup.find_all(["b", "strong", "em", "u"])
            if len(em.get_text(strip=True)) > 10
        ][:15]

        return {"full_text": full_text, "tables": tables, "emphasized": emphasizes}

    def extract_important(self, text: str) -> str:
        important: list[str] = []

        # 1) Item 블록
        for code, cfg in sorted(
            self.item_patterns.items(), key=lambda x: x[1]["importance"], reverse=True
        ):
            ms = re.findall(cfg["pattern"], text, flags=re.DOTALL)
            if ms:
                blk = ms[0].strip()
                if len(blk) > 80:
                    important.append(f"[{cfg['korean']}]\n{blk[:1500]}")

        # 2) 키워드 가중치 문장
        weighted: list[tuple[str, float]] = []
        for sent in re.split(r"[.!?]+", text):
            sent = sent.strip()
            if len(sent) < 40:
                continue
            score = sum(w for kw, w in ALL_KEYWORDS if kw in sent.lower())
            if score >= 1.5:
                weighted.append((sent, score))
        for s, _ in sorted(weighted, key=lambda x: x[1], reverse=True)[:12]:
            important.append(s)

        # 3) 재무 수치
        if (nums := extract_financial_numbers(text)):
            important.append("주요 수치: " + ", ".join(nums[:10]))

        combined = "\n\n".join(important[:15])
        if len(enc.encode(combined)) > 4500:
            combined = combined[:12000]
        return combined


# ─────────────────────── 6. 결과 간소화 헬퍼 ───────────────────────
def simplify(results: list[dict]) -> list[dict]:
    return [
        {
            "title": r.get("title", "제목 없음"),
            "narrative": r.get("narrative", "내용 없음"),
            "filing_date": r.get("filing_date", "날짜 정보 없음"),
        }
        for r in results
    ]


# ───────────────────────── 7. 메인 분석 함수 ──────────────────────────
def analyze_8k(
    docs: list[str],
    max_cost: float = 8.0,
    default_date: str | None = None,
) -> list[dict]:
    if not docs:
        return []

    default_date = default_date or date.today().strftime("%Y-%m-%d")
    extractor = Smart8KExtractor(default_date)

    results: list[dict] = []
    total_cost = 0.0

    for idx, raw in enumerate(docs, start=1):
        logger.info("🔍 문서 %d 분석 중...", idx)

        # 예산 체크
        est_tokens = len(enc.encode(raw[:15_000])) * 0.4
        est_dollars = est_tokens / 1000 * 0.00075
        if total_cost + est_dollars > max_cost:
            logger.warning("예산 초과 예상, 문서 %d 스킵", idx)
            continue

        filing_date = extractor.extract_filing_date(raw)
        if not re.match(r"\d{4}-\d{2}-\d{2}", filing_date):
            filing_date = default_date

        filt = extractor.smart_filter(raw)
        important = extractor.extract_important(filt["full_text"])

        if filt["tables"]:
            important += f"\n\n[테이블]\n{'\n'.join(filt['tables'])[:1500]}"
        if filt["emphasized"]:
            important += f"\n\n[강조]\n{'\n'.join(filt['emphasized'])}"

        # Rate limit 휴식
        if idx > 1:
            time.sleep(8)

        # OpenAI 호출 (재시도)
        attempt, response = 0, None
        while attempt < 5 and response is None:
            try:
                response = (
                    PROMPT | llm | StrOutputParser()
                ).invoke({"filing_date": filing_date, "full_content": important})
            except Exception as e:
                if "429" in str(e):
                    delay = 2 * (2**attempt)
                    logger.warning("429 재시도 %d회차, %ds 후 재시도", attempt + 1, delay)
                    time.sleep(delay)
                    attempt += 1
                else:
                    logger.error("OpenAI 오류: %s", e)
                    break

        if response is None:
            continue

        # JSON 파싱 및 메타 추가
        try:
            rec = json.loads(response)
        except json.JSONDecodeError:
            logger.error("JSON 파싱 실패, 문서 %d 스킵", idx)
            continue

        if not re.match(r"\d{4}-\d{2}-\d{2}", rec.get("filing_date", "")):
            rec["filing_date"] = filing_date

        token_in = len(enc.encode(important))
        token_out = len(enc.encode(response))
        cost = token_in * 0.00015 / 1000 + token_out * 0.00060 / 1000
        total_cost += cost

        rec.update(
            {
                "document_index": idx,
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "analysis_version": "v4_safe",
                "_meta": {"cost_usd": round(cost, 6)},
            }
        )

        results.append(rec)
        logger.info("✅ 문서 %d 완료 (비용 $%.4f)", idx, cost)

        if total_cost >= max_cost:
            logger.info("💰 예산 소진, 루프 종료")
            break

    if not results:
        return [
            {
                "title": "분석 가능한 공시 없음",
                "narrative": "투자 가치가 높은 정보를 발견하지 못했습니다.",
                "filing_date": default_date,
            }
        ]

    return simplify(results)


# ─────────────────────────── 8. 테스트 코드 ────────────────────────────
if __name__ == "__main__":
    sample_docs = ["<html><body><p>Sample 8-K content</p></body></html>"]
    res = analyze_8k(sample_docs)
    print(json.dumps(res, ensure_ascii=False, indent=2))
