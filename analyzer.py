"""
alyzer.py  (SEC 8-K 분석기 · Docker 안전판)

- gzip/인코딩/제어문자 문제를 모두 해결하는 safe_soup() 탑재
- BeautifulSoup 직접 호출 구문을 전부 safe_soup() 로 교체
- 날짜 추출 메서드 이름 유지( extract_date_from_html ) → 기존 코드와 100% 호환
"""

import json, logging, re, time, gzip
from datetime import datetime, date
from typing import List, Dict, Any

import chardet
from bs4 import BeautifulSoup, UnicodeDammit
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import tiktoken

# ─────────────────────── 0. 공통 설정 ───────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, max_tokens=1200)
enc = tiktoken.encoding_for_model("gpt-4o-mini")

PROMPT = ChatPromptTemplate.from_template(
    """
당신은 SEC 8-K 공시를 분석하는 **금융 애널리스트**입니다.

공시일: {filing_date}
문서 내용: {full_content}

다음 JSON 형식으로 출력하세요:

{{
  "title": "공시 핵심 내용을 반영한 구체적인 한국어 제목 (50자 이내)",
  "narrative": "공시 내용을 스토리텔링 방식으로 설명한 2-3개 단락 (각 단락 4-5문장, 연결어 사용으로 흐름 유지)",
  "filing_date": "{filing_date}"
}}
"""
)

# ─────────────────────── 1. 유틸 ───────────────────────
def safe_soup(raw_html) -> BeautifulSoup:
    """ParserRejectedMarkup 방지용 다중 파서 + 인코딩 정리"""
    try:
        if isinstance(raw_html, bytes):
            if raw_html[:2] == b"\x1f\x8b":         # gzip 헤더
                raw_html = gzip.decompress(raw_html)
            raw_html = re.sub(rb"[^\x09\x0A\x0D\x20-\x7E]", b" ", raw_html)
            enc_guess = chardet.detect(raw_html)["encoding"] or "utf-8"
            text = raw_html.decode(enc_guess, errors="replace")
        else:
            text = re.sub(r"[^\x09\x0A\x0D\x20-\x7E]", " ", str(raw_html))

        for parser in ("html.parser", "lxml", "html5lib"):
            try:
                return BeautifulSoup(text, parser)
            except Exception:
                continue
        return BeautifulSoup(UnicodeDammit(text).unicode_markup, "html.parser")
    except Exception as e:
        logger.error("safe_soup 실패: %s", e)
        return BeautifulSoup("", "html.parser")

def build_item_pattern(major: int, minor: int) -> str:
    return (
        rf"(?i)item[\s.\xa0]*{major}[\s.\xa0]*[\.:,]?\s*{minor}"
        r".*?(?=item[\s.\xa0]*\d+[\s.\xa0]*[\.:,]?\s*\d+|signature|$)"
    )

def extract_financial_numbers(text: str) -> List[str]:
    pats = [
        r"\$[\d,]+(?:\.\d{2})?(?:\s*(?:million|billion|trillion|백만|억|조))?",
        r"[\d,]+(?:\.\d{2})?\s*(?:million|billion|trillion|백만|억|조)\s*(?:달러|dollars?)",
        r"[\d,]+(?:\.\d{2})?\s*(?:shares?|주식|주)",
        r"[\d,]+(?:\.\d{2})?\s*(?:percent|%)",
    ]
    out = []
    for p in pats:
        out.extend(re.findall(p, text, flags=re.IGNORECASE))
    return list(set(out))

# ─────────────────────── 2. 날짜 추출기 ───────────────────────
class EnhancedDateExtractor:
    date_patterns = [
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
    date_formats = [
        "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y",
        "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y", "%d-%m-%Y",
    ]

    def __init__(self, default_date: str | None = None):
        self.default = default_date or date.today().strftime("%Y-%m-%d")

    def _format(self, s: str, known: str | None = None) -> str | None:
        s = s.strip()
        try:
            if known:
                return datetime.strptime(s, known).strftime("%Y-%m-%d")
            if s.isdigit() and len(s) == 8:
                return f"{s[:4]}-{s[4:6]}-{s[6:]}"
            for fmt in self.date_formats:
                try:
                    d = datetime.strptime(s, fmt)
                    if 1990 <= d.year <= datetime.now().year + 5:
                        return d.strftime("%Y-%m-%d")
                except ValueError:
                    pass
        except Exception:
            pass
        return None

    def _sec_header(self, soup: BeautifulSoup) -> str | None:
        pats = [
            r"<SEC-HEADER>.*?</SEC-HEADER>",
            r"SEC-HEADER.*?(?=<|$)",
            r"CONFORMED\s+PERIOD\s+OF\s+REPORT[:\s]+(\d{8})",
            r"FILED\s+AS\s+OF\s+DATE[:\s]+(\d{8})",
        ]
        txt = str(soup)
        for p in pats:
            for m in re.findall(p, txt, flags=re.DOTALL | re.IGNORECASE):
                if isinstance(m, str) and m.isdigit() and len(m) == 8:
                    return self._format(m, "%Y%m%d")
                if (fd := self._with_patterns(m)):
                    return fd
        return None

    def _with_patterns(self, text: str) -> str | None:
        for pat in self.date_patterns:
            for m in re.findall(pat, text, flags=re.IGNORECASE):
                if (fd := self._format(m)):
                    return fd
        return None

    def _from_tags(self, soup: BeautifulSoup) -> str | None:
        for tag in soup.find_all(["time", "date", "span", "div", "p"]):
            for attr in ("datetime", "date", "data-date"):
                if tag.get(attr) and (fd := self._format(tag[attr])):
                    return fd
            if tag.string and (fd := self._format(tag.string)):
                return fd
        return None

    # 기존 코드 호환: extract_date_from_html
    def extract_date_from_html(self, raw_html) -> str:
        soup = safe_soup(raw_html)
        return (
            self._sec_header(soup)
            or self._with_patterns(soup.get_text(" "))
            or self._from_tags(soup)
            or self.default
        )

# ─────────────────────── 3. 핵심 Extractor ───────────────────────
class Smart8KExtractor:
    def __init__(self, default_date: str | None = None):
        self.date_extractor = EnhancedDateExtractor(default_date)
        self.item_patterns = {
            "ITEM_1_01": ("중요 계약 체결", build_item_pattern(1, 1), 8),
            "ITEM_1_02": ("계약 종료", build_item_pattern(1, 2), 7),
            "ITEM_2_01": ("자산 인수/매각", build_item_pattern(2, 1), 9),
            "ITEM_2_02": ("실적 발표", build_item_pattern(2, 2), 8),
            "ITEM_3_02": ("주식 매각", build_item_pattern(3, 2), 6),
            "ITEM_4_01": ("감사인 변경", build_item_pattern(4, 1), 6),
            "ITEM_5_02": ("임원 변경", build_item_pattern(5, 2), 9),
            "ITEM_7_01": ("규제 절차", build_item_pattern(7, 1), 7),
            "ITEM_8_01": ("기타 중요 사건", build_item_pattern(8, 1), 5),
            "ITEM_9_01": ("재무제표 및 전시물", build_item_pattern(9, 1), 4),
        }

    def extract_filing_date(self, raw_html) -> str:
        return self.date_extractor.extract_date_from_html(raw_html)

    def smart_filter(self, raw_html) -> Dict[str, Any]:
        soup = safe_soup(raw_html)
        for t in soup.find_all(["script", "style", "nav", "header", "footer"]):
            t.decompose()
        full = soup.get_text(" ")
        tables = [
            tbl.get_text(" ", strip=True)
            for tbl in soup.find_all("table")
            if any(
                kw in tbl.get_text(" ", strip=True).lower()
                for kw in ["merger", "agreement", "officer", "director", "shares",
                           "vote", "financial", "revenue", "earnings", "dividend", "debt"]
            )
        ][:3]
        emphasizes = [
            em.get_text(" ", strip=True) for em in soup.find_all(["b", "strong", "u", "em"])
            if len(em.get_text(strip=True)) > 10
        ][:15]
        return {"full": full, "tables": tables, "emph": emphasizes}

    def important_content(self, text: str) -> str:
        parts: List[str] = []

        # Item 블록
        for name, pat, imp in sorted(
            self.item_patterns.values(), key=lambda x: x[2], reverse=True
        ):
            ms = re.findall(pat, text, flags=re.DOTALL)
            if ms:
                blk = ms[0].strip()
                if len(blk) > 80:
                    parts.append(f"[{name}]\n{blk[:1500]}")

        # 가중치 문장
        sent_scores = []
        for s in re.split(r"[.!?]+", text):
            s = s.strip()
            if len(s) < 40:
                continue
            score = sum(w for kw, w in ALL_KEYWORDS if kw in s.lower())
            if score >= 1.5:
                sent_scores.append((score, s))
        for _, s in sorted(sent_scores, reverse=True)[:12]:
            parts.append(s)

        # 재무 수치
        nums = extract_financial_numbers(text)
        if nums:
            parts.append("주요 수치: " + ", ".join(nums[:10]))

        combined = "\n\n".join(parts[:15])
        if len(enc.encode(combined)) > 4500:
            combined = combined[:12000]
        return combined

# ─────────────────────── 4. 분석 함수 ───────────────────────
def analyze_8k(
    docs: List[str],
    max_cost: float = 8.0,
    default_date: str | None = None,
) -> List[Dict]:
    if not docs:
        return []

    default_date = default_date or date.today().strftime("%Y-%m-%d")
    ext = Smart8KExtractor(default_date)

    total_cost = 0.0
    output: List[Dict] = []

    for idx, raw in enumerate(docs, start=1):
        logger.info("문서 %d 분석 중 (Docker-safe 버전)...", idx)

        est_tokens = len(enc.encode(raw[:15000])) * 0.4
        if total_cost + est_tokens / 1000 * 0.00075 > max_cost:
            logger.warning("예산 초과 예상, 문서 %d 스킵", idx)
            continue

        filing_date = ext.extract_filing_date(raw)
        if not re.match(r"\d{4}-\d{2}-\d{2}", filing_date):
            filing_date = default_date

        filt = ext.smart_filter(raw)
        important = ext.important_content(filt["full"])
        if filt["tables"]:
            important += "\n\n[테이블]\n" + "\n".join(filt["tables"])[:1500]
        if filt["emph"]:
            important += "\n\n[강조]\n" + "\n".join(filt["emph"])

        if idx > 1:
            time.sleep(8)

        retries, response = 0, None
        while retries < 5 and response is None:
            try:
                response = (PROMPT | llm | StrOutputParser()).invoke(
                    {"filing_date": filing_date, "full_content": important}
                )
            except Exception as e:
                if "429" in str(e):
                    delay = 2 ** (retries + 1)
                    logger.warning("429 재시도 %d회차, %ds 대기", retries + 1, delay)
                    time.sleep(delay)
                    retries += 1
                else:
                    logger.error("OpenAI 오류: %s", e)
                    break

        if response is None:
            continue

        try:
            rec = json.loads(response)
        except json.JSONDecodeError:
            logger.error("JSON 파싱 실패, 문서 %d 스킵", idx)
            continue

        rec["filing_date"] = (
            rec.get("filing_date") if re.match(r"\d{4}-\d{2}-\d{2}", rec.get("filing_date", "")) else filing_date
        )

        token_in = len(enc.encode(important))
        token_out = len(enc.encode(response))
        cost = token_in * 0.00015 / 1000 + token_out * 0.0006 / 1000
        total_cost += cost

        rec.update(
            {
                "document_index": idx,
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "analysis_version": "v5_docker_safe",
                "_meta": {"cost_usd": round(cost, 6)},
            }
        )
        output.append(rec)
        logger.info("✅ 문서 %d 완료 (비용 $%.4f)", idx, cost)

        if total_cost >= max_cost:
            logger.info("예산 소진, 루프 중단")
            break

    if not output:
        return [{
            "title": "분석 가능한 공시 없음",
            "narrative": "중요 정보가 없거나 문서 형식이 비정상입니다.",
            "filing_date": default_date
        }]

    # 간소화
    return [
        {
            "title": r.get("title", "제목 없음"),
            "narrative": r.get("narrative", "내용 없음"),
            "filing_date": r.get("filing_date", default_date),
        }
        for r in output
    ]

# ─────────────────────── 5. CLI 테스트 ───────────────────────
if __name__ == "__main__":
    sample = ["<SEC-HEADER>\nFILED AS OF DATE 20250726\n</SEC-HEADER><body>Test 8-K filing.</body>"]
    print(json.dumps(analyze_8k(sample), indent=2, ensure_ascii=False))
