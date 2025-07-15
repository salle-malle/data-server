import json
import logging
import re
from datetime import datetime
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
from bs4 import BeautifulSoup
import tiktoken

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, max_tokens=400)
enc = tiktoken.encoding_for_model("gpt-4o-mini")

# 개선된 한국어 프롬프트 - 더 구체적인 지시
prompt = ChatPromptTemplate.from_template("""
당신은 SEC 8-K 공시 문서의 내용을 한국어로 정리하는 역할입니다.

Item 유형: {item_type}
중요도: {importance}/10
공시일: {filing_date}
내용: {text}

다음 JSON 형식으로 요약하세요. 단, 반드시 **텍스트에 명시된 사실만 사용**하고, 절대로 내용을 상상하거나 추론하지 마세요.

{{
    "title": "텍스트에 명시된 사실 기반의 한국어 제목 (허위 생성 금지)",
    "summary": ["텍스트에서 발췌한 주요 문장 3개"],
    "event_type": "텍스트에 명시된 사건 유형 (한국어로 표현)",
    "tone": "positive/negative/neutral (사실에 근거하여 판단, 없으면 neutral)"
}}

아래 사항을 반드시 지키세요:
- 텍스트에 없는 정보는 절대 포함하지 마세요.
- 고유명사(회사명, 인명 등)는 반드시 **텍스트에서 명시된 것만** 사용하세요.
- 추론, 요약, 재구성, 일반화 금지 — 있는 그대로 중요한 문장을 뽑아 정리하세요.
- 한국어로만 작성하세요.
""")

class Smart8KExtractor:
    """SEC 8-K 핵심 내용 추출기 (날짜 추출 포함)"""
    
    def __init__(self):
        # SEC 8-K Item별 중요도 및 패턴
        self.item_patterns = {
            'ITEM_5_02': {
                'pattern': r'(?i)item\s+5\.02[^0-9].*?(?=item\s+\d+\.\d+|signature|$)',
                'importance': 9,
                'korean_name': '임원 변경',
                'keywords': ['appoint', 'resign', 'retire', 'CEO', 'CFO', 'President', 'officer', 'director']
            },
            'ITEM_2_02': {
                'pattern': r'(?i)item\s+2\.02[^0-9].*?(?=item\s+\d+\.\d+|signature|$)',
                'importance': 8,
                'korean_name': '실적 발표',
                'keywords': ['earnings', 'revenue', 'results', 'financial', 'quarter', 'fiscal']
            },
            'ITEM_2_01': {
                'pattern': r'(?i)item\s+2\.01[^0-9].*?(?=item\s+\d+\.\d+|signature|$)',
                'importance': 7,
                'korean_name': '자산 인수/매각',
                'keywords': ['acquisition', 'merger', 'purchase', 'sale', 'dispose', 'acquire']
            },
            'ITEM_1_01': {
                'pattern': r'(?i)item\s+1\.01[^0-9].*?(?=item\s+\d+\.\d+|signature|$)',
                'importance': 6,
                'korean_name': '중요 계약 체결',
                'keywords': ['agreement', 'contract', 'definitive', 'material']
            },
            'ITEM_1_02': {
                'pattern': r'(?i)item\s+1\.02[^0-9].*?(?=item\s+\d+\.\d+|signature|$)',
                'importance': 6,
                'korean_name': '계약 종료',
                'keywords': ['termination', 'terminate', 'end', 'expire']
            },
            'ITEM_4_01': {
                'pattern': r'(?i)item\s+4\.01[^0-9].*?(?=item\s+\d+\.\d+|signature|$)',
                'importance': 5,
                'korean_name': '감사인 변경',
                'keywords': ['auditor', 'accountant', 'change']
            },
            'ITEM_8_01': {
                'pattern': r'(?i)item\s+8\.01[^0-9].*?(?=item\s+\d+\.\d+|signature|$)',
                'importance': 4,
                'korean_name': '기타 중요 사건',
                'keywords': ['other', 'event', 'material']
            }
        }
        
        # 날짜 추출 패턴들
        self.date_patterns = [
            # Date of Report 패턴 (가장 일반적)
            r'(?i)date\s+of\s+report\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})',
            # Filing Date 패턴
            r'(?i)filing\s+date\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})',
            # Report Date 패턴  
            r'(?i)report\s+date\s*[:\(]?\s*(\w+\s+\d{1,2},?\s+\d{4})',
            # 괄호 안의 날짜 패턴
            r'\((\w+\s+\d{1,2},?\s+\d{4})\)',
            # YYYY-MM-DD 형식
            r'(\d{4}-\d{2}-\d{2})',
            # MM/DD/YYYY 형식
            r'(\d{1,2}/\d{1,2}/\d{4})',
            # 문서 상단의 표준 날짜 패턴
            r'(?i)(?:on|dated?|as of)\s+(\w+\s+\d{1,2},?\s+\d{4})'
        ]
    
    def extract_filing_date(self, doc: str) -> str:
        """SEC 8-K 문서에서 공시 날짜 추출"""
        # HTML 태그 제거
        soup = BeautifulSoup(doc, "html.parser")
        text = soup.get_text()
        
        # 문서 상단 2000자에서 날짜 찾기 (헤더 정보가 보통 위에 있음)
        header_text = text[:2000]
        
        for pattern in self.date_patterns:
            matches = re.findall(pattern, header_text)
            if matches:
                date_str = matches[0]
                
                # 날짜 형식 정규화
                normalized_date = self.normalize_date(date_str)
                if normalized_date:
                    logger.info(f"공시 날짜 추출 성공: {normalized_date}")
                    return normalized_date
        
        # 날짜를 찾지 못한 경우 전체 문서에서 다시 시도
        logger.warning("헤더에서 날짜를 찾지 못함. 전체 문서에서 재시도...")
        
        for pattern in self.date_patterns:
            matches = re.findall(pattern, text)
            if matches:
                date_str = matches[0]
                normalized_date = self.normalize_date(date_str)
                if normalized_date:
                    logger.info(f"공시 날짜 추출 성공 (전체검색): {normalized_date}")
                    return normalized_date
        
        logger.warning("공시 날짜를 찾을 수 없음")
        return "날짜 정보 없음"
    
    def normalize_date(self, date_str: str) -> str:
        """다양한 날짜 형식을 YYYY-MM-DD로 정규화"""
        try:
            # 일반적인 형식들 시도
            date_formats = [
                "%B %d, %Y",     # January 15, 2025
                "%B %d %Y",      # January 15 2025
                "%b %d, %Y",     # Jan 15, 2025
                "%b %d %Y",      # Jan 15 2025
                "%Y-%m-%d",      # 2025-01-15
                "%m/%d/%Y",      # 01/15/2025
                "%m-%d-%Y",      # 01-15-2025
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str.strip(), fmt)
                    return parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            # 숫자만 있는 경우 처리 (예: 20250115)
            if date_str.isdigit() and len(date_str) == 8:
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]
                return f"{year}-{month}-{day}"
            
        except Exception as e:
            logger.warning(f"날짜 정규화 실패: {date_str}, 오류: {e}")
        
        return None

    def clean_and_extract_items(self, doc: str) -> list:
        """HTML 정리 후 중요한 Item들만 추출"""
        # 1. HTML 태그 제거
        soup = BeautifulSoup(doc, "html.parser")
        cleaned = soup.get_text()
        
        # 2. 기본 정리
        cleaned = re.sub(r'\s+', ' ', cleaned)  # 연속 공백 제거
        
        extracted_items = []
        
        # 3. Item별 패턴 매칭 (중요도 순으로)
        sorted_items = sorted(self.item_patterns.items(), 
                             key=lambda x: x[1]['importance'], reverse=True)
        
        for item_type, config in sorted_items:
            matches = re.findall(config['pattern'], cleaned, re.DOTALL)
            
            if matches:
                content = matches[0].strip()
                
                # 너무 짧으면 스킵
                if len(content) < 200:
                    continue
                
                # 키워드 밀도 계산 (관련성 확인)
                keyword_count = sum(1 for keyword in config['keywords'] 
                                  if keyword.lower() in content.lower())
                
                # 관련 키워드가 있거나 중요도가 높으면 포함
                if keyword_count > 0 or config['importance'] >= 7:
                    extracted_items.append({
                        'item_type': item_type,
                        'korean_name': config['korean_name'],
                        'importance': config['importance'],
                        'content': self.extract_key_sentences(content, config['keywords']),
                        'keyword_density': keyword_count
                    })
        
        return extracted_items
    
    def extract_key_sentences(self, content: str, keywords: list) -> str:
        """키워드 기반으로 핵심 문장들만 추출"""
        sentences = re.split(r'[.!?]+', content)
        
        # 중요한 문장들 선별
        important_sentences = []
        keyword_sentences = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) < 20:  # 너무 짧은 문장 제외
                continue
            
            # 키워드 포함 문장
            if any(keyword.lower() in sentence.lower() for keyword in keywords):
                keyword_sentences.append(sentence)
            
            # 일반적으로 중요한 패턴
            if any(pattern in sentence.lower() for pattern in [
                'announced', 'appointed', 'effective', 'pursuant to', 
                'million', 'billion', 'agreement', 'company'
            ]):
                important_sentences.append(sentence)
        
        # 키워드 문장 우선, 그 다음 중요 문장
        selected = keyword_sentences[:3] + important_sentences[:2]
        
        # 중복 제거 및 길이 제한
        unique_sentences = []
        total_length = 0
        
        for sentence in selected:
            if sentence not in unique_sentences and total_length < 1500:
                unique_sentences.append(sentence)
                total_length += len(sentence)
        
        return '. '.join(unique_sentences[:4]) + '.'

def analyze_8k(docs: list[str]) -> list[dict]:
    """
    개선된 8-K 분석 (핵심 내용 위주 + 날짜 추출)
    """
    if not docs:
        return []
    
    extractor = Smart8KExtractor()
    results = []
    total_cost = 0.0
    
    for doc_idx, doc in enumerate(docs):
        logger.info(f"문서 {doc_idx + 1} 핵심 추출 중...")
        
        try:
            # 0. 공시 날짜 추출
            filing_date = extractor.extract_filing_date(doc)
            
            # 1. 중요한 Item들만 추출
            extracted_items = extractor.clean_and_extract_items(doc)
            
            if not extracted_items:
                logger.warning(f"문서 {doc_idx + 1}: 중요한 Item을 찾을 수 없음")
                continue
            
            # 2. 각 Item을 LLM으로 요약 (중요도 순)
            for item in extracted_items[:2]:  # 상위 2개만 처리
                try:
                    # 토큰 수 계산
                    content = item['content']
                    token_count = len(enc.encode(content))
                    
                    if token_count > 1200:  # 너무 길면 자르기
                        content = content[:1000]
                        token_count = len(enc.encode(content))
                    
                    # LLM 요청 (날짜 정보 포함)
                    chain = prompt | llm | StrOutputParser()
                    response = chain.invoke({
                        "item_type": item['korean_name'],
                        "importance": item['importance'],
                        "filing_date": filing_date,
                        "text": content
                    })
                    
                    # 응답 파싱
                    try:
                        result = json.loads(response)
                        
                        # 메타데이터 추가 (날짜 정보 포함)
                        result.update({
                            "filing_date": filing_date,  # 공시 날짜 추가
                            "item_type_code": item['item_type'],
                            "importance_score": item['importance'],
                            "keyword_density": item['keyword_density']
                        })
                        
                        # 비용 계산
                        input_cost = token_count * 0.00015 / 1000
                        output_cost = len(enc.encode(response)) * 0.00060 / 1000
                        total_cost += (input_cost + output_cost)
                        
                        results.append(result)
                        logger.info(f"{item['korean_name']} 요약 완료 (중요도: {item['importance']}, 날짜: {filing_date})")
                        
                    except json.JSONDecodeError:
                        logger.error(f"{item['korean_name']} JSON 파싱 실패")
                        continue
                
                except Exception as e:
                    logger.error(f"{item['korean_name']} 처리 오류: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"문서 {doc_idx + 1} 전체 오류: {e}")
            continue
    
    logger.info(f"핵심 추출 완료: {len(results)}개, 비용: ${total_cost:.4f}")
    
    if not results:
        return [{
            "title": "중요한 공시 내용 없음",
            "summary": ["분석 가능한 중요한 Item 섹션을 찾을 수 없습니다."],
            "event_type": "정보 부족",
            "tone": "neutral",
            "filing_date": "날짜 정보 없음",
            "importance_score": 0
        }]
    
    # 중요도순 정렬
    results.sort(key=lambda x: x.get('filing_date', ''), reverse=True)
    return results
