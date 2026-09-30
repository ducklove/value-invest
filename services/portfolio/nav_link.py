"""정산 기준(price_basis) 경계를 넘는 NAV 연결 — 읽기 전용, 저장 행은 바꾸지 않는다.

정규장 정산(regular_close_v1)의 첫 날은 NAV 1,000 으로 새 구간을 시작한다
(docs/regular-close-settlement.md). 저장된 NAV 는 구간마다 척도가 달라 그대로
이으면 경계에서 가짜 등락이 생기고, 새 구간만 보여 주면 과거 이력이 사라진다.

그래서 조회 시점에 체인 링크한다. 경계마다 이전 구간 마지막 행 d0, 새 구간 첫
행 d1 에 대해

    r = (V(d1) − netCF) / V(d0) − 1        (전환일 수익률, Today 카드와 같은 정의)
    k = RN(d1) / (RN(d0) × (1 + r))        (RN = return_nav = nav × return_factor)

를 구하고, 이전 구간의 NAV 계열 값(nav, return_nav, distribution_per_unit)에
k 를 곱한다. netCF 는 d0 정산 이후 ~ d1 정산까지 반영된 입출금(입금 +,
출금·분배금 −)이며 Today 기준선과 같은 조회(get_cashflows_created_after)를 쓴다.
경계가 여럿이면 뒤에서부터 곱해 모든 구간을 최신 구간 척도로 맞춘다.

원화 금액(total_value, total_invested)과 좌수(total_units)는 실제 값이라 바꾸지
않는다. 연결된 행에서 nav × total_units 는 평가액과 다르므로 소비자는 금액을
NAV × 좌수로 다시 계산하지 말아야 한다. 연결된 행은 ``linked=True``,
``nav_link_factor``, ``raw_nav`` 를 갖는다. 평가액이 없거나 0 이라 연결할 수
없는 경계가 있으면 그 이전 행은 예전처럼 숨기고 경고를 남긴다.

입출금 내역의 ``nav_at_time``(좌수 발행 NAV)도 같은 k 로 환산한다
(``link_cashflows``). 발행 NAV 는 반영 정산일(``applied_snapshot_date``, 없으면
``date``)이 속한 구간의 척도이므로 그 구간의 누적 k 를 곱한다. ``units_change`` 는
실제 좌수라 그대로다. 연결된 행은 ``raw_nav_at_time``, ``nav_link_factor`` 를 갖고,
연결할 수 없는 구간의 행은 ``nav_at_time=None`` + ``nav_link_unavailable=True``.
"""

from __future__ import annotations

import logging
import math

from repositories import snapshots
from repositories.db import read_snapshot
from services.portfolio.time_windows import settlement_marker_seconds

logger = logging.getLogger(__name__)

LEGACY_BASIS = "legacy_latest"


def signed_cashflow(row: dict) -> float:
    """입금 +, 출금·분배금 −, 그 외 0 — Today 기준선(previous_day)과 같은 부호 규칙."""
    amount = float(row.get("amount") or 0)
    kind = row.get("type")
    if kind == "deposit":
        return amount
    if kind in {"withdrawal", "distribution"}:
        return -amount
    return 0.0


def _return_nav(row: dict) -> float | None:
    value = row.get("return_nav")
    if value is None and row.get("nav") is not None:
        value = row["nav"] * (row.get("return_factor") or 1)
    return value


def _cutoff(row: dict) -> str:
    if row.get("cashflow_cutoff_at"):
        return row["cashflow_cutoff_at"]
    if (row.get("price_basis") or LEGACY_BASIS) == LEGACY_BASIS:
        return row["date"] + "T20:00:00"
    return settlement_marker_seconds(row["date"])


def transition_return(prev: dict, nxt: dict, net_cashflow: float) -> float | None:
    """d0 정산 → d1 정산 사이의 입출금 차감 평가액 수익률."""
    v0, v1 = prev.get("total_value"), nxt.get("total_value")
    if v0 is None or v1 is None or not v0 > 0:
        return None
    return (v1 - net_cashflow) / v0 - 1


def link_factor(prev: dict, nxt: dict, net_cashflow: float) -> float | None:
    """이전 구간 NAV 를 새 구간 척도로 옮기는 배수 k. 계산할 수 없으면 None."""
    r = transition_return(prev, nxt, net_cashflow)
    rn0, rn1 = _return_nav(prev), _return_nav(nxt)
    if r is None or 1 + r <= 0 or not rn0 or rn0 <= 0 or not rn1 or rn1 <= 0:
        return None
    k = rn1 / (rn0 * (1 + r))
    return k if math.isfinite(k) and k > 0 else None


def find_boundaries(rows: list[dict]) -> list[tuple[dict, dict]]:
    """날짜 오름차순 행에서 기준이 바뀌는 인접 쌍."""
    return [(a, b) for a, b in zip(rows, rows[1:])
            if (a.get("price_basis") or LEGACY_BASIS) != (b.get("price_basis") or LEGACY_BASIS)]


def _linked_row(row: dict, factor: float) -> dict:
    out = dict(row)
    out["raw_nav"] = row["nav"]
    out["nav"] = row["nav"] * factor
    out["return_nav"] = out["nav"] * (row.get("return_factor") or 1)
    if row.get("distribution_per_unit"):
        out["distribution_per_unit"] = row["distribution_per_unit"] * factor
    out["linked"] = True
    out["nav_link_factor"] = factor
    return out


def apply_links(rows: list[dict], factors: dict[str, float | None]) -> list[dict]:
    """``factors``: 새 구간 첫 행 날짜 → k (None = 연결 불가).

    경계 날짜보다 앞선 행에는 그 뒤 모든 경계의 k 를 곱한다. 연결 불가 경계를
    만나면 그 이전 행은 모두 제외한다.
    """
    starts = sorted(factors, reverse=True)
    out: list[dict] = []
    cumulative, idx = 1.0, 0
    for row in reversed(rows):
        while idx < len(starts) and row["date"] < starts[idx]:
            k = factors[starts[idx]]
            if k is None:
                out.reverse()
                return out
            cumulative *= k
            idx += 1
        out.append(_linked_row(row, cumulative) if idx else row)
    out.reverse()
    return out


def cumulative_factor(day: str, factors: dict[str, float | None]) -> float | None:
    """``day`` 가 속한 구간을 최신 구간 척도로 옮기는 누적 k — ``apply_links`` 와 같은 규칙.

    뒤에 경계가 없으면 1.0, 뒤에 연결할 수 없는 경계가 있으면 None.
    """
    cumulative = 1.0
    for start in sorted(factors, reverse=True):
        if day >= start:
            break
        k = factors[start]
        if k is None:
            return None
        cumulative *= k
    return cumulative


def link_cashflow_rows(rows: list[dict], factors: dict[str, float | None]) -> list[dict]:
    """입출금 행의 발행 NAV(``nav_at_time``)를 최신 구간 척도로 환산한 사본."""
    if not factors:
        return rows
    out: list[dict] = []
    for row in rows:
        nav = row.get("nav_at_time")
        day = row.get("applied_snapshot_date") or row.get("date")
        if nav is None or not day or not any(day < start for start in factors):
            out.append(row)
            continue
        factor = cumulative_factor(day, factors)
        linked = dict(row)
        linked["raw_nav_at_time"] = nav
        if factor is None:
            linked["nav_at_time"] = None
            linked["nav_link_unavailable"] = True
        else:
            linked["nav_at_time"] = nav * factor
            linked["nav_link_factor"] = factor
        out.append(linked)
    return out


async def boundary_net_cashflow(user: str, prev: dict, nxt: dict) -> float:
    """d0 정산 이후 ~ d1 정산까지 잔고에 반영된 순입출금(입금 +)."""
    rows = await snapshots.get_cashflows_created_after(user, settlement_marker_seconds(prev["date"]))
    end_cutoff = _cutoff(nxt)
    total = 0.0
    for row in rows:
        applied = row.get("applied_snapshot_date")
        if applied:
            if applied > nxt["date"]:
                continue
        elif str(row.get("created_at") or "") > end_cutoff:
            continue
        total += signed_cashflow(row)
    return total


async def _factors(user: str, pairs: list[tuple[dict, dict]]) -> dict[str, float | None]:
    factors: dict[str, float | None] = {}
    for prev, nxt in pairs:
        if not prev or not nxt:
            continue
        net = await boundary_net_cashflow(user, prev, nxt)
        k = link_factor(prev, nxt, net)
        if k is None:
            logger.warning(
                "NAV basis boundary %s→%s (%s→%s) cannot be linked for user=%s; hiding earlier rows",
                prev["date"], nxt["date"], prev.get("price_basis"), nxt.get("price_basis"), user[:8],
            )
        factors[nxt["date"]] = k
    return factors


@read_snapshot()
async def get_nav_history(user: str, *, include_legacy: bool = False) -> list[dict]:
    """수익률·차트용 NAV 이력 — 모든 소비자의 단일 진입점.

    기본값은 기준 경계를 연결한 전체 이력(이전 구간 행은 ``linked=True``).
    ``include_legacy=True`` 는 연결하지 않은 저장 원자료 전체(감사용)다.
    """
    rows = await snapshots.get_raw_nav_history(user)
    if include_legacy:
        return rows
    pairs = find_boundaries(rows)
    if not pairs:
        return rows
    return apply_links(rows, await _factors(user, pairs))


@read_snapshot()
async def link_snapshot(user: str, snapshot: dict | None) -> dict | None:
    """한 스냅샷(MTD/YTD 기준점 등)을 최신 구간 척도로 환산한다.

    뒤에 기준 경계가 없으면 그대로, 연결할 수 없으면 None.
    """
    if not snapshot or not snapshot.get("date"):
        return snapshot
    pairs = await snapshots.get_basis_boundaries(user, after_date=snapshot["date"])
    if not pairs:
        return snapshot
    linked = apply_links([snapshot], await _factors(user, pairs))
    return linked[0] if linked else None


@read_snapshot()
async def link_cashflows(user: str, rows: list[dict]) -> list[dict]:
    """입출금 내역의 ``nav_at_time`` 을 연결된 NAV 이력과 같은 척도로 맞춘다(저장 행 불변)."""
    if not any(row.get("nav_at_time") is not None for row in rows):
        return rows
    pairs = await snapshots.get_basis_boundaries(user)
    if not pairs:
        return rows
    return link_cashflow_rows(rows, await _factors(user, pairs))
