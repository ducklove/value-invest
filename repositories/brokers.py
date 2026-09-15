"""사용자별 NH 자격증명과 계좌 연결. 외부 응답에는 비밀을 포함하지 않는다."""

import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

import aiosqlite

from repositories.broker_secrets import BrokerError, account_fingerprint, decrypt, encrypt
from repositories.db import get_db, transaction


def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


async def store_credential(user: str, app_key: str, app_secret: str) -> str:
    digest = fingerprint(app_key)
    async with transaction() as db:
        row = await (await db.execute("SELECT credential_id,google_sub FROM broker_credentials WHERE key_fingerprint=?", (digest,))).fetchone()
        if row and row["google_sub"] != user:
            raise BrokerError("이 앱키는 다른 연결에 등록돼 있습니다. 본인 명의의 앱키를 사용해 주세요.")
        cid = row["credential_id"] if row else str(uuid4())
        if row:
            old = await get_credential(user, cid)
            if old["app_key"] == app_key and old["app_secret"] == app_secret:
                return cid
            linked = await (await db.execute("SELECT 1 FROM broker_account_links WHERE credential_id=? LIMIT 1", (cid,))).fetchone()
            if linked:
                raise BrokerError("연결 중인 앱키의 시크릿이 다릅니다. 기존 연결을 해제한 뒤 변경해 주세요.")
        sealed = encrypt(json.dumps({"app_key": app_key, "app_secret": app_secret}))
        await db.execute(
            "INSERT INTO broker_credentials (credential_id,google_sub,secret_ciphertext,key_fingerprint,created_at) VALUES (?,?,?,?,?) "
            "ON CONFLICT(credential_id) DO UPDATE SET secret_ciphertext=excluded.secret_ciphertext,token_ciphertext=NULL,token_expires_at=NULL",
            (cid, user, sealed, digest, datetime.now(timezone.utc).isoformat()),
        )
        return cid


async def get_credential(user: str, cid: str) -> dict:
    db = await get_db()
    row = await (await db.execute("SELECT * FROM broker_credentials WHERE google_sub=? AND credential_id=?", (user, cid))).fetchone()
    if not row:
        raise BrokerError("등록된 나무 앱키를 찾을 수 없습니다.")
    data = dict(row)
    data.update(json.loads(decrypt(data.pop("secret_ciphertext"))))
    data["token"] = decrypt(data["token_ciphertext"]) if data["token_ciphertext"] else None
    return data


async def save_token(user: str, cid: str, token: str, expires_at: float):
    async with transaction() as db:
        await db.execute("UPDATE broker_credentials SET token_ciphertext=?,token_expires_at=? WHERE google_sub=? AND credential_id=?",
                         (encrypt(token), expires_at, user, cid))


async def link_account(user: str, aid: str, cid: str, account_no: str, environment: str, include_overseas: bool = True):
    from repositories.account_holdings import require_account
    async with transaction() as db:
        await require_account(user, aid)
        await get_credential(user, cid)
        existing = await (await db.execute("SELECT 1 FROM broker_account_links WHERE account_id=?", (aid,))).fetchone()
        if existing:
            raise BrokerError("이미 연결된 계좌입니다. 기존 연결을 해제한 뒤 연결해 주세요.")
        holdings = await (await db.execute("SELECT 1 FROM account_holdings WHERE google_sub=? AND account_id=? LIMIT 1", (user, aid))).fetchone()
        if holdings:
            raise BrokerError("잔고가 없는 계좌에 연결해 주세요. 기존 수동 잔고의 중복·덮어쓰기를 방지합니다.")
        try:
            await db.execute("INSERT INTO broker_account_links (account_id,google_sub,credential_id,account_ciphertext,account_fingerprint,account_mask,environment,include_overseas) VALUES (?,?,?,?,?,?,?,?)",
                             (aid, user, cid, encrypt(account_no), account_fingerprint(account_no), "•••••••" + account_no[-4:], environment, int(include_overseas)))
        except aiosqlite.IntegrityError as exc:
            raise BrokerError("이미 연동한 NH 계좌입니다.") from exc


async def get_link(user: str, aid: str) -> dict:
    db = await get_db()
    row = await (await db.execute("SELECT * FROM broker_account_links WHERE google_sub=? AND account_id=?", (user, aid))).fetchone()
    if not row:
        raise BrokerError("NH 연결 계좌를 찾을 수 없습니다.")
    result = dict(row)
    result["account_no"] = decrypt(result.pop("account_ciphertext"))
    return result


async def disconnect(user: str, aid: str):
    """잔고를 보존하고 수동 계좌로 전환한다. 마지막 연결의 키·토큰도 삭제한다."""
    async with transaction() as db:
        link = await get_link(user, aid)
        await db.execute("DELETE FROM broker_account_links WHERE google_sub=? AND account_id=?", (user, aid))
        await db.execute("DELETE FROM broker_credentials WHERE google_sub=? AND credential_id=? AND NOT EXISTS "
                         "(SELECT 1 FROM broker_account_links WHERE credential_id=?)", (user, link["credential_id"], link["credential_id"]))


async def list_links() -> list[dict]:
    db = await get_db()
    rows = await (await db.execute("SELECT google_sub,account_id,credential_id,environment,last_sync_at FROM broker_account_links")).fetchall()
    return [dict(r) for r in rows]
