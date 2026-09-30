#!/usr/bin/env bash
# 연계 프로젝트의 '데이터' 파일만 업스트림에서 당겨온다 (운영 서버 정기 실행).
#
# 왜 따로 도는가: deploy/deploy.sh 는 value-invest 체크아웃만 origin/master 로
# 맞춘다. hodling-value·gold_gap 같은 형제 저장소는 각자의 스케줄로 갱신되므로,
# integrations.py 가 로컬 파일로 읽는 스냅샷(지분가치/금·비트코인 괴리)은
# 배포와 무관하게 오래된 채로 남는다.
#
# 왜 git pull 이 아닌가: config.json 은 /admin.html 이 이 서버 위에서 직접
# 편집하는 파일이라 워크트리가 dirty 한 게 정상이고, 업스트림으로 덮으면
# 관리자 편집이 날아간다. 그래서 merge/pull 대신 서버가 읽는 데이터 파일만
# `git show <upstream>:<path>` 로 덮어쓴다 — 인덱스도 config.json 도 건드리지
# 않는다.
set -uo pipefail

ROOT="${LINKED_PROJECTS_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

# "디렉터리후보(|로 구분)[@원격브랜치]:업스트림에서 가져올 파일(,로 구분)"
# @원격브랜치가 없으면 추적 브랜치(없으면 origin/HEAD·main·master)를 쓴다.
# gold_gap 의 data.json 은 master 에 없다 — Actions 가 orphan `data` 브랜치에만
# 커밋하고 Pages 로 배포한다. 그래서 origin/data 를 명시한다.
PROJECTS=(
  "hodling-value|holding_value:current.json"
  "gold_gap@origin/data:data.json"
)

resolve_upstream() {
  local repo="$1" ref
  ref="$(git -C "$repo" rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)"
  if [[ -n "$ref" ]] && git -C "$repo" rev-parse --verify --quiet "$ref" >/dev/null; then
    printf '%s' "$ref"
    return 0
  fi
  for ref in origin/HEAD origin/main origin/master; do
    if git -C "$repo" rev-parse --verify --quiet "$ref" >/dev/null; then
      printf '%s' "$ref"
      return 0
    fi
  done
  return 1
}

status=0
for entry in "${PROJECTS[@]}"; do
  spec="${entry%%:*}"
  files="${entry#*:}"
  dirs="${spec%%@*}"
  pinned=""
  if [[ "$spec" == *@* ]]; then
    pinned="${spec#*@}"
  fi

  repo=""
  IFS='|' read -ra candidates <<<"$dirs"
  for name in "${candidates[@]}"; do
    if [[ -d "$ROOT/$name/.git" ]]; then
      repo="$ROOT/$name"
      break
    fi
  done
  if [[ -z "$repo" ]]; then
    echo "skip: $dirs — 체크아웃이 없습니다 (원격 폴백 사용)"
    continue
  fi

  if ! git -C "$repo" fetch --prune --quiet origin; then
    echo "warn: $repo fetch 실패"
    status=1
    continue
  fi

  if [[ -n "$pinned" ]]; then
    # single-branch 클론이면 기본 fetch 가 그 브랜치를 받지 않는다 — 명시적으로 받는다.
    branch="${pinned#origin/}"
    git -C "$repo" fetch --quiet origin "+refs/heads/$branch:refs/remotes/origin/$branch" 2>/dev/null || true
    if ! git -C "$repo" rev-parse --verify --quiet "$pinned" >/dev/null; then
      echo "warn: $repo $pinned 브랜치를 찾지 못했습니다"
      status=1
      continue
    fi
    upstream="$pinned"
  elif ! upstream="$(resolve_upstream "$repo")"; then
    echo "warn: $repo 업스트림 브랜치를 찾지 못했습니다"
    status=1
    continue
  fi

  IFS=',' read -ra paths <<<"$files"
  for path in "${paths[@]}"; do
    if ! git -C "$repo" cat-file -e "$upstream:$path" 2>/dev/null; then
      echo "skip: $repo/$path — $upstream 에 없습니다"
      continue
    fi
    if git -C "$repo" show "$upstream:$path" >"$repo/$path.sync.tmp" 2>/dev/null; then
      mv "$repo/$path.sync.tmp" "$repo/$path"
      echo "ok:   $repo/$path <- $upstream"
    else
      rm -f "$repo/$path.sync.tmp"
      echo "warn: $repo/$path 갱신 실패"
      status=1
    fi
  done
done

exit "$status"
