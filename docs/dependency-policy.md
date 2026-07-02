# Dependency Policy

## Python

- Runtime dependencies live in `requirements.txt`.
- Test/lint/type-check dependencies live in `requirements-dev.txt`, which must include `-r requirements.txt`.
- Every direct Python dependency must have both a lower bound and an upper bound, for example `httpx>=0.28.1,<1.0`.
- Exact pins are avoided for runtime requirements until a Linux/Python 3.11 lock is generated in CI or production, because local Windows `pip freeze` can include unrelated and platform-specific packages.
- When a dependency is intentionally capped because of a known compatibility issue, keep the reason next to the requirement as a comment.

## JavaScript

- JavaScript test dependencies are installed with `npm ci` from `package-lock.json`.
- `package-lock.json` is the source of truth for resolved JS dependency versions.

## Enforcement

- `tests/test_dependency_policy.py` checks that direct Python requirements are bounded and that CI/deploy keep using the locked install paths.
- `.github/workflows/ci.yml` installs Python from `requirements-dev.txt` and JavaScript from `npm ci`.
- `deploy/deploy.sh` installs Python from `requirements-dev.txt` and JavaScript from `npm ci` when Node is available.
