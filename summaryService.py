import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from db import execute_query, fetch_all
import openai
import pytz
import pymysql
import time

load_dotenv()
logging.basicConfig(level=logging.INFO)

# 한국 시간대 객체
KST = pytz.timezone("Asia/Seoul")

# OpenAI 클라이언트 생성
def get_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise Exception("OPENAI_API_KEY가 .env에 설정되어 있지 않습니다.")
    return openai.OpenAI(api_key=api_key)

# 뉴스 요약 생성
def summarize(content, stock_name):
    prompt = (
        "⚠️ 너는 이제부터 전문 투자 뉴스 요약가야. 아래 영어 기사들을 읽고 **반드시 '한국어'로** 요약해야 해.\n\n"
        f"이번 뉴스는 `{stock_name}` 기업에 대한 내용이야. 이 기업과 관련된 흐름에 초점을 맞춰서 요약해.\n\n"
        "요약은 **마크다운 형식**으로 다음 기준을 따라:\n"
        "1. 소제목은 `###`로 시작하고, 핵심 주제를 중심으로 작성해.\n"
        "2. 각 소제목 아래에 1~3줄로 문단을 구성하고, 불필요한 결론이나 서론은 생략해.\n"
        "3. 과도한 숫자 예측, 투자 조언, 기사 링크는 포함하지 마.\n"
        "4. 전체 소제목은 2~3개 정도만 사용해.\n\n"
        "⛔ *요약 결과가 영어일 경우, 응답은 무효 처리되며 평가에 반영되지 않아.*\n\n"
        "아래는 영어 기사 내용이야. **무조건 한국어로 요약해줘**:\n\n"
        f"{content}"
    )

    params = {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.5,
    }
    retry = 0
    while True:
        try:
            completion = get_client().chat.completions.create(**params)
            return completion.choices[0].message.content.strip()
        except Exception as e:
            retry += 1
            if retry > 5:
                logging.error(f"OpenAI 요약 요청 5회 이상 실패: {e}")
                raise
            logging.warning(f"OpenAI 요약 요청 실패({retry}회): {e}, 3초 후 재시도")
            time.sleep(3)

# 투자 성향별 코멘트 생성
def generate_commentary(summary_content, investment_type_name):
    description = {
        "안정형": "안정형(원금 보전 최우선)",
        "보수형": "보수형(소폭의 수익 추구, 낮은 위험)",
        "적극형": "적극형(수익과 성장을 위해 일정 수준의 위험 감수)",
        "공격형": "공격형(최대 수익 추구, 높은 위험 감수)"
    }.get(investment_type_name, "일반 투자자")

    prompt = (
        f"사용자는 아래 뉴스 요약을 이미 읽었어. 이 요약을 바탕으로, {description} 투자 성향을 가진 사용자에게 도움이 될 만한 짧은 코멘트를 해줘."
        "- 너무 자세한 설명보다는, 요약 내용을 투자자 입장에서 어떻게 받아들이면 좋을지 한 문장 정도의 조언이나 인사이트를 줘."
        "- 존댓말로 작성해줘. 투자 도우미처럼 말해줘."
        "- 최대 200자 이내로 간결하게 작성해줘."
        "- 형식은 자연스러운 서술형 문장 한 문장으로 해줘."
        f"뉴스 요약:{summary_content}"
    )

    params = {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 1000
    }
    # 동기적으로 요청 (재시도 및 딜레이는 유지)
    retry = 0
    while True:
        try:
            completion = get_client().chat.completions.create(**params)
            return completion.choices[0].message.content.strip()
        except Exception as e:
            retry += 1
            if retry > 5:
                logging.error(f"OpenAI 코멘트 요청 5회 이상 실패: {e}")
                raise
            logging.warning(f"OpenAI 코멘트 요청 실패({retry}회): {e}, 3초 후 재시도")
            time.sleep(3)

# 하나의 종목에 대해 요약 및 저장 처리
def summarize_and_save(content, stock, image_url):
    summary_text = summarize(content, stock['stock_name'])
    try:
        # 한국 시간으로 현재 시간 생성
        now_kst = datetime.now(KST).replace(tzinfo=None)
        insert_sql = """
            INSERT INTO summary (
                created_at,
                updated_at,
                news_content,
                news_image,
                stock_id
            ) VALUES (
                %s, %s, %s, %s, %s
            )
        """
        
        summary_id = execute_query(insert_sql, (now_kst, now_kst, summary_text, image_url, stock['stock_id']))
        
        if not summary_id:
            logging.error("❌ summary insert 후 id를 못받았음")
            return None
        
    except Exception as e:
        logging.error(f"summary insert 실패: {e}")
        return None

    # 투자 성향별 첨언 저장
    try:
        investment_types = fetch_all("SELECT * FROM investment_type")
        logging.info(f"investment_type 쿼리 결과: {investment_types}")
    except Exception as e:
        logging.error(f"investment_type 조회 실패: {e}")
        investment_types = []

    saved_comments = []
    for investment_type in investment_types:
        comment = generate_commentary(summary_text, investment_type['investment_name'])
        try:
            execute_query(
                """
                INSERT INTO investment_type_news_comment (
                    summary_id,
                    investment_id,
                    investment_type_news_content,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (summary_id, investment_type['id'], comment, now_kst, now_kst)
            )
            saved_comments.append({
                "investment_type_id": investment_type['id'],
                "investment_type_news_content": comment
            })
        except Exception as e:
            logging.error(f"investment_type_news_comment insert 실패: {e}")

    # 종목 보유 회원 조회 및 스냅샷 저장
    try:
        holding_members = fetch_all("""
            SELECT m.*, it.id AS investment_type_id
            FROM member m
            JOIN member_stock ms ON m.id = ms.member_id
            LEFT JOIN investment_type it ON m.investment_type_id = it.id
            WHERE ms.stock_id = %s
        """, (stock['stock_id'],))
    except Exception as e:
        logging.error(f"member 조회 실패: {e}")
        holding_members = []

    # investment_type_news_comment의 id를 가져오려면, comment 저장 시 insert 후 id를 받아야 합니다.
    # execute_query가 insert 후 id를 반환한다고 가정하고 아래처럼 수정합니다.

    comment_id_map = {}  # investment_type_id -> comment row id

    for investment_type in investment_types:
        comment = generate_commentary(summary_text, investment_type['investment_name'])
        try:
            comment_id = execute_query(
                """
                INSERT INTO investment_type_news_comment (
                    summary_id,
                    investment_id,
                    investment_type_news_content,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (summary_id, investment_type['id'], comment, now_kst, now_kst)
            )
            saved_comments.append({
                "investment_type_id": investment_type['id'],
                "investment_type_news_content": comment
            })
            if comment_id:
                comment_id_map[investment_type['id']] = comment_id
        except Exception as e:
            logging.error(f"investment_type_news_comment insert 실패: {e}")

    for member in holding_members:
        type_id = member.get('investment_type_id')
        if not type_id:
            continue
        comment_id = comment_id_map.get(type_id)
        if not comment_id:
            continue
        try:
            # 한국 시간으로 현재 시간 생성
            now_kst = datetime.now(KST).replace(tzinfo=None)
            execute_query(
                """
                INSERT INTO member_stock_snapshot (
                    created_at,
                    updated_at,
                    member_id,
                    investment_type_news_comment_id
                ) VALUES (%s, %s, %s, %s)
                """,
                (now_kst, now_kst, member['id'], comment_id)
            )
        except Exception as e:
            logging.error(f"member_stock_snapshot insert 실패: {e}")

    return summary_text

def generate_summary_for_today_news():
    try:
        now_kst = datetime.now(KST)
        start = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)

        # DB에 직접 쿼리 날리기 (pymysql 사용)
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")
        db_port = int(os.getenv("DB_PORT", "3306"))

        logging.info(f"오늘 뉴스 조회 범위 (KST): {start} ~ {end}")

        today_news = []
        try:
            conn = pymysql.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                db=db_name,
                port=db_port,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            with conn.cursor() as cursor:
                start_str = start.strftime('%Y-%m-%d %H:%M:%S')
                end_str = end.strftime('%Y-%m-%d %H:%M:%S')
                sql = f"""
                    SELECT n.*, s.stock_name
                    FROM news n
                    LEFT JOIN stock s ON n.stock_id = s.stock_id
                    WHERE n.created_at >= '{start_str}' AND n.created_at < '{end_str}'
                """
                logging.info(f"실행 쿼리(직접 문자열): {sql}")
                cursor.execute(sql)
                today_news = cursor.fetchall()
            conn.close()
        except Exception as e:
            logging.error(f"pymysql 직접 쿼리 실패: {e}")
            return

        if not today_news or len(today_news) == 0:
            logging.warning("오늘 날짜 기준으로 가져온 뉴스가 없습니다.")
            return
        logging.info(f"오늘 날짜 기준 뉴스 가져오기 완료: {len(today_news)}건")
    except Exception as e:
        logging.error(f"news 조회 실패: {e}")
        return

    # 종목별로 그룹핑
    news_by_stock = {}
    for news in today_news:
        stock_id = news.get('stock_id')
        if not stock_id:
            continue
        if stock_id not in news_by_stock:
            news_by_stock[stock_id] = {
                'stock': {
                    'stock_id': stock_id,
                    'stock_name': news.get('stock_name') or "Unknown"
                },
                'news_list': []
            }
        news_by_stock[stock_id]['news_list'].append(news)

    if not news_by_stock:
        logging.warning("오늘 날짜 기준으로 종목별 뉴스가 없습니다.")
        return

    # 각 종목에 대해 summarize_and_save 호출 (동기적으로 순차 처리)
    for stock_id, grouped in news_by_stock.items():
        stock = grouped['stock']
        news_list = grouped['news_list']
        combined_content = "\n\n".join(n['news_content'] for n in news_list if n.get('news_content'))
        image_url = next((n.get('news_image') for n in news_list if n.get('news_image')), None)
        if not combined_content:
            logging.info(f"[요약 스킵] 종목: {stock['stock_name']} ({stock['stock_id']}) - 뉴스 내용 없음")
            continue
        logging.info(f"[요약 시작] 종목: {stock['stock_name']} ({stock['stock_id']})")
        summarize_and_save(combined_content, stock, image_url)
        time.sleep(5)
