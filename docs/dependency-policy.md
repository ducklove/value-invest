# Dependency Policy

## Python

- Runtime dependencies live in `requirements.txt`.
- Test/lint/type-check dependencies live in `requirements-dev.txt`, which must include `-r requirements.txt`.
- Every direct Python dependency must have both a lower bound and an upper bound, for example `httpx>=0.28.1,<1.0`.
- 정확한 버전과 해시는 `requirements.lock`·`requirements-dev.lock`에 고정한다. 직접 의존성을 추가하거나 범위를 바꾸면 두 잠금 파일도 함께 갱신한다. Windows의 `pip freeze`로 대체하지 않는다.
- When a dependency is intentionally capped because of a known compatibility issue, keep the reason next to the requirement as a comment.

## JavaScript

- JavaScript test dependencies are installed with `npm ci` from `package-lock.json`.
- `package-lock.json` is the source of truth for resolved JS dependency versions.

## Enforcement

- `tests/test_dependency_policy.py` checks that direct Python requirements are bounded and that CI/deploy keep using the locked install paths.
- `.github/workflows/ci.yml`과 `deploy/deploy.sh`는 `--require-hashes -r requirements-dev.lock`으로 Python 환경을 설치한다. 잠금 파일의 직접 의존성 누락·버전 범위 불일치도 테스트로 검사한다.
- JavaScript 설치는 `npm ci`를 사용한다.

```sh
uv pip compile --no-python-downloads --universal --python-version 3.11 --generate-hashes --output-file requirements.lock requirements.txt
uv pip compile --no-python-downloads --universal --python-version 3.11 --generate-hashes --constraint requirements.lock --output-file requirements-dev.lock requirements-dev.txt
```
