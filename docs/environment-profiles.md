# 설정과 환경 프로파일

## 단일 소스: `.env`

설정과 시크릿은 **프로젝트 루트의 `.env` 한 파일**에만 둔다. 로컬·운영 모두 같은
파일 하나를 쓴다.

우선순위는 두 단계뿐이다.

1. **프로세스 환경변수** — systemd `Environment=`, 쉘 export, 테스트의 `patch.dict`
2. **`.env`** — 위에서 비어 있는 키만 채운다 (`load_dotenv(override=False)`)

`core.config.load_environment()`가 앱 시작 시(그리고 배치/테스트 진입점에서) 한 번
로드한다. 새 키는 `.env.example`에 먼저 문서화한 뒤 `.env`에 채운다.

## 프로파일

프로파일은 별도 파일이 아니라 `.env` 안의 `VALUE_INVEST_ENV` 값으로 구분한다.

- `development`: 무거운 배치 루프를 끄고(`*_INTERVAL_S=0`), 로컬 브라우저 CORS를
  허용하고, `/docs`를 연다.
- `production`: systemd timer 기준으로 운영하고 공개 도메인만 CORS에 둔다.
- 기본값은 `production`이다. env를 명시하지 않던 기존 배포 호환 정책이다.

## 사용법

로컬:

```powershell
Copy-Item .env.example .env
# .env 에서 VALUE_INVEST_ENV=development 로 바꾸고 시크릿을 채운다.
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

운영(라즈베리파이):

```bash
cp .env.example .env    # 최초 1회. 이후 키 추가는 .env 를 직접 편집.
chmod 600 .env
sudo systemctl restart value-invest.service
```

`deploy/value-invest.service`는 `EnvironmentFile=-.../.env` 한 줄만 두고 값을
중복 정의하지 않는다.

## 단일화 이전 파일들 (2026-08-12 제거)

`.env.<프로필>`, `.kis.env`, `keys.txt`, 그리고 유닛 파일의 중복
`Environment=` 라인은 더 이상 읽지 않는다.

- 남아 있는 파일이 감지되면 `core.config`가 **파일 이름만** WARNING으로 남긴다
  (값은 절대 로그에 찍지 않는다). 경고가 보이면 값을 `.env`로 옮기고 파일을 지운다.
- 서버 체크아웃은 `deploy/migrate_env_to_single_file.sh`가 배포 중(재시작 전)
  자동으로 병합한다. 옛 로드 순서를 그대로 재현해
  `.kis.env` > `.env.<프로필>` > `.env` > `keys.txt` 우선순위로 합치고, 원본은
  `.config-migrated-<timestamp>/`에 백업한 뒤 삭제한다. 이미 정리된 체크아웃에서는
  아무 일도 하지 않는다.

## 마이그레이션 원칙

- 새 설정은 코드 기본값에만 흩뿌리지 말고 `.env.example`에 먼저 문서화한다.
- 새 모듈은 `core.config.get_settings()`를 통해 app-level 설정을 읽는다.
- 시크릿은 import 시점에 고정하지 말고 **호출 시점에** `os.getenv`로 읽는다.
  `main.py`가 앱 팩토리를 먼저 import 하므로 모듈 전역에 굳히면 `.env` 값이
  반영되지 않는다(`auth_service.session_secret()`, `dart_client.api_key()` 참고).
