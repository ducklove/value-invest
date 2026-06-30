# 개발/운영 환경 분리

## 프로필

- `VALUE_INVEST_ENV=development`: 로컬 개발용. 무거운 배치 루프는 기본 비활성화하고 로컬 브라우저 CORS를 허용한다.
- `VALUE_INVEST_ENV=production`: 실서비스용. systemd timer를 기준으로 운영하고 공개 도메인만 CORS에 둔다.
- 기본값은 `production`이다. 기존 서버가 별도 env 없이 동작하던 것을 깨지 않기 위한 호환 정책이다.

## 파일 로딩 순서

1. `.env`
2. `.env.<VALUE_INVEST_ENV>`
3. `.kis.env` *(legacy)*
4. `keys.txt` *(legacy)*

`.kis.env`는 기존 운영 호환을 위해 override를 유지한다. `keys.txt`는 마지막 fallback이며 이미 존재하는 환경변수는 덮어쓰지 않는다.

### Legacy 경로 deprecation (3·4단계)

`.kis.env`와 `keys.txt`는 레거시다. 운영 호환성 때문에 당장 제거하지 못하지만
(예: `deploy/value-invest.service`가 `.kis.env`를 아직 참조), 프로필 env
(`.env.<profile>`)을 단일 소스로 수렴하기 위해 2026-06-30부터 **deprecation
경고**를 로깅한다.

- `load_environment()`가 `.kis.env`·`keys.txt`에서 **적용한 키 이름**을
  `core.config` 로거(WARNING 레벨)에 기록한다. **값은 절대 로그에 찍지 않는다.**
- 목표: 경고에 나타난 키를 `.env.<profile>`로 옮긴 뒤, legacy 파일에서
  제거하고, 빈 파일이 되면 삭제한다.
- 운영자가 감사를 마친 뒤 경고를 끄려면 `SILENCE_LEGACY_CONFIG_WARNINGS=1`
  환경변수를 설정한다(기본은 경고). 단, 이 플래그는 경고만 끌 뿐 로드 동작은
  그대로 유지한다.


## 권장 사용법

로컬:

```powershell
Copy-Item .env.example .env
Copy-Item .env.development.example .env.development
$env:VALUE_INVEST_ENV = "development"
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

운영:

```bash
cp .env.production.example .env.production
# Fill non-secret production settings in .env.production.
# Keep real secrets in systemd env, .kis.env, or another untracked secret source.
sudo systemctl restart value-invest.service
```

## 마이그레이션 원칙

- 새 설정은 코드 기본값에 흩뿌리지 말고 `.env.*.example`에 먼저 문서화한다.
- 새 모듈은 `core.config.get_settings()`를 통해 app-level 설정을 읽는다.
- 외부 client의 timeout/base URL은 다음 단계에서 `services/*`로 옮기며 import-time 전역 설정을 줄인다.

