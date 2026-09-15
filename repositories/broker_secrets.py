"""증권사 비밀의 저장 형식. DB와 분리된 암호화 키를 사용한다."""

import base64
import hashlib
import hmac
import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from core.errors import AppError


class BrokerError(AppError):
    status_code = 400


def _key() -> bytes:
    configured = os.environ.get("BROKER_ENCRYPTION_KEY")
    if configured:
        key = configured.encode()
    else:
        from repositories.db import DB_PATH
        path = Path(os.environ.get("BROKER_KEY_FILE") or str(Path(DB_PATH).parent / ".broker-encryption.key"))
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            key = path.read_bytes()
        else:
            key = Fernet.generate_key()
            with os.fdopen(fd, "wb") as stream:
                stream.write(key)
    try:
        Fernet(key)
        return key
    except (ValueError, TypeError) as exc:
        raise BrokerError("증권사 암호화 키 설정을 확인해 주세요.") from exc


def cipher() -> Fernet:
    return Fernet(_key())


def account_fingerprint(value: str) -> str:
    # 계좌번호는 탐색 공간이 작으므로 DB만으로 대입할 수 없는 키 기반 지문을 쓴다.
    return hmac.new(base64.urlsafe_b64decode(_key()), b"nh-account:" + value.encode(), hashlib.sha256).hexdigest()


def encrypt(value: str) -> str:
    return cipher().encrypt(value.encode()).decode()


def decrypt(value: str) -> str:
    try:
        return cipher().decrypt(value.encode()).decode()
    except (InvalidToken, UnicodeError) as exc:
        raise BrokerError("증권사 암호화 키가 일치하지 않습니다. 서버 키를 복구해 주세요.") from exc
