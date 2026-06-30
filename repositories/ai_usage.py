"""AI usage events + rolling usage summary.

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db


async def insert_ai_usage_event(
    *,
    google_sub: str | None,
    feature: str,
    model: str,
    model_profile: str | None,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cost_usd: float = 0.0,
    latency_ms: int | None = None,
    ok: bool = True,
    error: str | None = None,
):
    db = await get_db()
    await db.execute(
        """
        INSERT INTO ai_usage_events
            (ts, google_sub, feature, model, model_profile, input_tokens, output_tokens, cost_usd, latency_ms, ok, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().isoformat(),
            google_sub,
            feature,
            model,
            model_profile,
            int(input_tokens or 0),
            int(output_tokens or 0),
            float(cost_usd or 0),
            latency_ms,
            1 if ok else 0,
            (error or "")[:500] if error else None,
        ),
    )
    await db.commit()


async def summarize_ai_usage(days: int = 30) -> dict:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT feature, model, model_profile,
               COUNT(*) AS calls,
               SUM(input_tokens) AS input_tokens,
               SUM(output_tokens) AS output_tokens,
               SUM(cost_usd) AS cost_usd,
               SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS errors,
               AVG(latency_ms) AS avg_latency_ms,
               MAX(ts) AS latest_at
        FROM ai_usage_events
        WHERE ts >= datetime('now', ?)
        GROUP BY feature, model, model_profile
        ORDER BY cost_usd DESC, calls DESC
        """,
        (f"-{int(days)} days",),
    )
    by_feature = [dict(row) for row in await cursor.fetchall()]
    cursor = await db.execute(
        """
        SELECT ts, feature, model, model_profile, input_tokens, output_tokens, latency_ms, ok, error
        FROM ai_usage_events
        ORDER BY ts DESC, id DESC
        LIMIT 50
        """,
    )
    recent = [dict(row) for row in await cursor.fetchall()]
    return {"days": days, "by_feature": by_feature, "recent": recent}


async def get_daily_cost_usd(google_sub: str | None) -> float:
    """특정 사용자(또는 전체)의 오늘(KST 기준 date 접두사) 누적 USD 비용.

    예산 캡(ST-11) 사전 체크용. ``google_sub`` 가 None 이면 전체 합계를 반환한다
    (사이트 전체 한도 검사). KST 날짜 경계는 표의 ts 가 ISO 문자열이므로
    date 접두사 비교로 처리한다 — SQLite 의 datetime('now') 는 UTC 기반이라
    KST 당일 범위를 명시해야 일/월 한도가 사용자 체감과 일치한다.

    인덱스: idx_ai_usage_feature_ts 가 (feature, ts) 를 covering 하지만, 이
    쿼리는 google_sub·ts 만 쓰므로 ts 인덱스(idx_ai_usage_ts)를 탄다.
    """
    db = await get_db()
    # KST 오늘 YYYY-MM-DD. tzinfo 없는 datetime.now() 는 런타임 로컬 시간인데,
    # 배포 서버(라즈베리파이) 타임존이 KST 이므로 그대로 써도 당일 경계가 맞다.
    today_prefix = datetime.now().strftime("%Y-%m-%d")
    if google_sub is None:
        cursor = await db.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) AS total FROM ai_usage_events "
            "WHERE ts >= ? AND ts < ?",
            (f"{today_prefix}T00:00:00", f"{today_prefix}T23:59:59"),
        )
    else:
        cursor = await db.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) AS total FROM ai_usage_events "
            "WHERE google_sub = ? AND ts >= ? AND ts < ?",
            (google_sub, f"{today_prefix}T00:00:00", f"{today_prefix}T23:59:59"),
        )
    row = await cursor.fetchone()
    return float(row[0] if row is not None else 0)
