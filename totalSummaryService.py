import os
import logging
from datetime import datetime, timedelta, date
from typing import Optional
import time

from regex import P
from db import fetch_all, execute_query
import pytz
import openai

_logger = logging.getLogger(__name__)

# 한국 시간대 객체
KST = pytz.timezone("Asia/Seoul")

def get_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise Exception("OPENAI_API_KEY가 .env에 설정되어 있지 않습니다.")
    return openai.OpenAI(api_key=api_key)

def summarize_total(content: str) -> str:
    prompt = (
        "다음은 오늘의 투자 뉴스 요약 모음입니다. 이 내용을 바탕으로 3~5개의 핵심 요약을 만들어 주세요.\n\n"
        "각 요약은 다음과 같은 형식으로 구성해 주세요:\n\n"
        "[이모지] 제목\n"
        "한 줄설명 \n\n"
        "예시:\n"
        "📉 기술주 약세\n"
        "금리 인상 우려로 기술주 중심의 하락세가 나타났습니다.\n\n"
        "📈 반도체 강세\n"
        "AI 수요 확대에 따라 엔비디아 등 반도체 종목이 상승했습니다.\n\n"
        "💡 투자 코멘트\n"
        "단기적인 시장 변동성에 대비해 포트폴리오 리밸런싱이 필요합니다.\n\n"
        "상승, 하락, 전반적인 투자 코멘트, 투자 조언 으로 구성해주세요. 이모지는 적절히 📉📈💡🔥 같은 걸 활용해 주셔도 좋습니다.\n"
        "각 문단별로 \\n\\n로 구분해주세요.\n\n"
        + content
    )
    params = {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.5,
        "max_tokens": 500,
    }
    retry = 0
    while True:
        try:
            completion = get_client().chat.completions.create(**params)
            return completion.choices[0].message.content.strip()
        except Exception as e:
            retry += 1
            if retry > 5:
                _logger.error(f"OpenAI 요약 요청 5회 이상 실패: {e}")
                return "요약 생성에 실패했습니다."
            _logger.warning(f"OpenAI 요약 요청 실패({retry}회): {e}, 3초 후 재시도")
            time.sleep(3)

def generate_total_summary_for_all_members():
    # 테이블명을 소문자로 변경 (MySQL 등에서 대소문자 구분 문제 방지)
    members = fetch_all("SELECT * FROM member")
    today = date.today()

    for member in members:
        try:
            member_id = member['id'] if isinstance(member, dict) else getattr(member, 'id', None)
            if not member_id:
                continue

            snapshots = fetch_all("SELECT * FROM member_stock_snapshot WHERE member_id = %s", (member_id,))
            today_snapshots = []

            for s in snapshots:
                created_at = s.get('created_at') if isinstance(s, dict) else getattr(s, 'created_at', None)
                if created_at is not None and not isinstance(created_at, datetime):
                    try:
                        created_at = datetime.fromisoformat(str(created_at))
                    except Exception:
                        continue
                if created_at is not None and created_at.date() == today:
                    today_snapshots.append(s)

            if not today_snapshots:
                continue

            comment_ids = []
            for s in today_snapshots:
                comment_id = s.get('investment_type_news_comment_id') if isinstance(s, dict) else getattr(s, 'investment_type_news_comment_id', None)
                if comment_id:
                    comment_ids.append(comment_id)

            if not comment_ids:
                continue

            format_strings = ','.join(['%s'] * len(comment_ids))
            comments = fetch_all(
                f"SELECT * FROM investment_type_news_comment WHERE id IN ({format_strings})",
                tuple(comment_ids)
            )

            comment_map = {
                c['id'] if isinstance(c, dict) else getattr(c, 'id', None): c
                for c in comments
            }

            merged = "\n".join(
                comment_map.get(cid, {}).get('investment_type_news_content', '') if isinstance(comment_map.get(cid, {}), dict)
                else getattr(comment_map.get(cid, {}), 'investment_type_news_content', '')
                for cid in comment_ids
            )

            if not merged.strip():
                continue

            # 너무 많이 요청 보내지 않도록 안전하게 3초 지연
            time.sleep(3)

            summary = summarize_total(merged)
            _logger.info(f"🧾 Member ID: {member_id}, Total Summary:\n{summary}")

            now_kst = datetime.now(KST).replace(tzinfo=None)

            insert_sql = """
                INSERT INTO total_summary (
                    created_at,
                    updated_at,
                    member_id,
                    total_content
                ) VALUES (
                    %s, %s, %s, %s
                )
            """
            totalsummary_id = execute_query(insert_sql, (now_kst, now_kst, member_id, summary))

            formatted_date = f"{today.year}년 {today.month}월 {today.day}일"
            stock_count = len(today_snapshots)
            title = f"{formatted_date} 총평 요약 도착!"
            content = f"{stock_count}개 종목에 대한 총평 요약이 도착했어요. 확인해보세요!"

            _logger.info("🔔 알림 생성: %s", title)

            insert_sql = """
                INSERT INTO notification (
                    created_at,
                    updated_at,
                    notification_title,
                    member_id,
                    notification_content,
                    notification_is_read,
                    notification_type
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """
            execute_query(insert_sql, (now_kst, now_kst, title, member_id, content, False, "SUMMARY_COMPLETE"))

        except Exception as e:
            _logger.error(f"회원 {member_id} 처리 중 에러: {e}")
            # 다음 멤버를 위해 1초 대기
            time.sleep(1)