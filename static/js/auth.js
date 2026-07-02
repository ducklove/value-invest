function isCompactMobileViewport() {
  return window.matchMedia('(max-width: 900px), (max-height: 520px) and (max-width: 1180px)').matches;
}

async function loadAuthConfig() {
  if (!hasApiConfiguration()) {
    authConfig = { enabled: false, googleClientId: '' };
    return authConfig;
  }

  try {
    authConfig = await apiFetchJson('/api/auth/config');
  } catch (error) {
    // 네트워크 오류는 "설정 안 됨"의 근거가 아니다 — 이미 받아둔 설정이
    // 있으면 유지해 일시적 연결 불안정이 로그인 UI를 끄지 않게 한다.
    if (!authConfig) {
      authConfig = { enabled: false, googleClientId: '' };
    }
  }
  return authConfig;
}

async function loadCurrentUser() {
  if (!hasApiConfiguration()) {
    currentUser = null;
    return currentUser;
  }

  try {
    const data = await apiFetchJson('/api/auth/me');
    currentUser = data.user || null;
  } catch (error) {
    if (error?.status && error.status < 500) {
      currentUser = null;
    }
    // 네트워크 오류/타임아웃 — 인증 여부를 알 수 없는 상태. 여기서 null로
    // 덮으면 DDNS 순단 같은 일시 장애마다 로그인이 풀린 것처럼 깜빡인다.
  }
  return currentUser;
}

async function syncAuthState(options = {}) {
  const { refreshRecentList = false, refreshPreference = false } = options;
  await loadCurrentUser();
  renderAuthState();
  if (refreshPreference) {
    await refreshActivePreference();
  }
  if (refreshRecentList) {
    await loadRecentList();
  }
}

function switchTab(tab) {
  activeTab = tab;
  document.querySelectorAll('.sidebar-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });
  loadRecentList();
}

function updateSidebarTabs() {
  const tabs = document.getElementById('sidebarTabs');
  if (!tabs) return;
  tabs.style.display = currentUser ? 'flex' : 'none';
}

function buildGoogleLoginUri() {
  return buildApiUrl('/api/auth/google/callback');
}

function buildLoginPageUrl() {
  const path = `/login?return_to=${encodeURIComponent(window.location.pathname + window.location.search + window.location.hash)}`;
  if (IS_GITHUB_PAGES_SITE && API_BASE_URL) {
    return `${API_BASE_URL.replace(/\/$/, '')}${path}`;
  }
  return path;
}

function normalizeUserPreference(preference) {
  return {
    is_starred: Boolean(preference?.is_starred),
    note: preference?.note || '',
    updated_at: preference?.updated_at || null,
  };
}

function setPreferenceStatus(message = '', tone = '') {
  const status = document.getElementById('preferenceStatus');
  if (!status) return;
  status.className = 'personalization-status';
  if (tone) {
    status.classList.add(tone);
  }
  status.textContent = message;
}

function renderUserPreference() {
  const panel = document.getElementById('personalizationPanel');
  const favoriteBtn = document.getElementById('favoriteBtn');
  const saveNoteBtn = document.getElementById('saveNoteBtn');
  const note = document.getElementById('preferenceNote');
  if (!panel || !favoriteBtn || !saveNoteBtn || !note) return;

  if (!activeStockCode) {
    panel.style.display = 'none';
    return;
  }

  panel.style.display = 'block';
  currentUserPreference = normalizeUserPreference(currentUserPreference);
  const isLoggedIn = Boolean(currentUser);

  favoriteBtn.classList.toggle('active', currentUserPreference.is_starred);
  favoriteBtn.textContent = currentUserPreference.is_starred ? '관심중' : '관심종목';
  favoriteBtn.disabled = !isLoggedIn || preferenceSaving;
  saveNoteBtn.disabled = !isLoggedIn || preferenceSaving;
  note.disabled = !isLoggedIn || preferenceSaving;
  note.value = currentUserPreference.note || '';

  if (!isLoggedIn) {
    setPreferenceStatus('로그인하면 관심종목과 개인 메모를 저장할 수 있습니다.', 'warning');
  } else if (!document.getElementById('preferenceStatus').textContent) {
    setPreferenceStatus('관심종목에 추가하면 관심 목록 탭에서 모아볼 수 있습니다.');
  }
}

async function refreshActivePreference() {
  if (!activeStockCode || !currentUser) {
    currentUserPreference = normalizeUserPreference(null);
    renderUserPreference();
    return;
  }

  try {
    const data = await apiFetchJson(`/api/preferences/${activeStockCode}`, {
      errorMessage: '개인화 설정을 불러오지 못했습니다.',
    });
    currentUserPreference = normalizeUserPreference(data.user_preference);
    setPreferenceStatus('');
  } catch (error) {
    currentUserPreference = normalizeUserPreference(null);
    setPreferenceStatus(error.message || '개인화 설정을 불러오지 못했습니다.', 'warning');
  }
  renderUserPreference();
}

async function saveUserPreference(changes, successMessage) {
  if (!currentUser || !activeStockCode || preferenceSaving) return;
  preferenceSaving = true;
  renderUserPreference();
  setPreferenceStatus('저장 중...');

  try {
    const data = await apiFetchJson(`/api/preferences/${activeStockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(changes),
      errorMessage: '개인화 설정을 저장하지 못했습니다.',
    });
    currentUserPreference = normalizeUserPreference(data.user_preference);
    renderUserPreference();
    setPreferenceStatus(successMessage, 'saved');
    loadRecentList();
  } catch (error) {
    renderUserPreference();
    setPreferenceStatus(error.message || '개인화 설정을 저장하지 못했습니다.', 'warning');
  } finally {
    preferenceSaving = false;
    renderUserPreference();
  }
}

async function toggleFavorite() {
  if (!currentUser) {
    renderUserPreference();
    return;
  }
  const nextValue = !normalizeUserPreference(currentUserPreference).is_starred;
  currentUserPreference = { ...normalizeUserPreference(currentUserPreference), is_starred: nextValue };
  renderUserPreference();
  await saveUserPreference(
    { is_starred: nextValue },
    nextValue ? '관심종목에 추가했습니다.' : '관심종목에서 제거했습니다.',
  );
}

async function autoStarCurrentStock() {
  if (!currentUser || !activeStockCode) return;
  try {
    const data = await apiFetchJson(`/api/preferences/${activeStockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ is_starred: true }),
      fallback: null,
    });
    if (data) {
      currentUserPreference = normalizeUserPreference(data.user_preference);
      renderUserPreference();
    }
  } catch (e) { console.warn(e); }
}

async function savePreferenceNote() {
  if (!currentUser) {
    renderUserPreference();
    return;
  }
  const note = document.getElementById('preferenceNote').value;
  currentUserPreference = { ...normalizeUserPreference(currentUserPreference), note };
  await saveUserPreference({ note }, '메모를 저장했습니다.');
}

function userHasPassword(user = currentUser) {
  return Boolean(user?.password_set);
}

function userHasGoogleLink(user = currentUser) {
  return Boolean(user?.google_linked);
}

function openProfileModal() {
  if (!currentUser) {
    window.location.href = buildLoginPageUrl();
    return;
  }
  const modal = document.getElementById('profileModal');
  if (!modal) return;
  const blankAvatar = 'data:image/gif;base64,R0lGODlhAQABAAAAACw=';
  const passwordSet = userHasPassword();
  const googleLinked = userHasGoogleLink();

  document.getElementById('profileAvatar').src = currentUser.picture || blankAvatar;
  document.getElementById('profileName').textContent = currentUser.name || currentUser.email || '사용자';
  document.getElementById('profileEmail').textContent = currentUser.email || '';

  const googleBadge = document.getElementById('profileGoogleBadge');
  googleBadge.textContent = googleLinked ? 'Google 연결됨' : 'Google 미연결';
  googleBadge.classList.toggle('active', googleLinked);

  const passwordBadge = document.getElementById('profilePasswordBadge');
  passwordBadge.textContent = passwordSet ? 'ID/PW 사용 가능' : 'ID/PW 미등록';
  passwordBadge.classList.toggle('active', passwordSet);

  document.getElementById('profilePasswordTitle').textContent = passwordSet ? '비밀번호 변경' : '비밀번호 등록';
  document.getElementById('profileCurrentPasswordGroup').style.display = passwordSet ? 'grid' : 'none';
  document.getElementById('profileCurrentPassword').required = passwordSet;
  document.getElementById('profileCurrentPassword').value = '';
  document.getElementById('profileNewPassword').value = '';
  document.getElementById('profileConfirmPassword').value = '';
  setProfilePasswordStatus('');

  openManagedModal(modal, {
    initialFocus: '#profileNewPassword',
    onEscape: closeProfileModal,
  });
}

function closeProfileModal() {
  const modal = document.getElementById('profileModal');
  if (modal) closeManagedModal(modal);
}

function setProfilePasswordStatus(message, tone = '') {
  const status = document.getElementById('profilePasswordStatus');
  if (!status) return;
  status.className = 'profile-status';
  if (tone) status.classList.add(tone);
  status.textContent = message || '';
}

async function saveProfilePassword(event) {
  event.preventDefault();
  if (!currentUser) return;
  const currentPassword = document.getElementById('profileCurrentPassword').value;
  const newPassword = document.getElementById('profileNewPassword').value;
  const confirmPassword = document.getElementById('profileConfirmPassword').value;
  const saveBtn = document.getElementById('profilePasswordSaveBtn');

  if (newPassword.length < 8) {
    setProfilePasswordStatus('비밀번호는 8자 이상이어야 합니다.', 'error');
    return;
  }
  if (newPassword !== confirmPassword) {
    setProfilePasswordStatus('새 비밀번호 확인이 일치하지 않습니다.', 'error');
    return;
  }

  saveBtn.disabled = true;
  setProfilePasswordStatus('저장 중...');
  try {
    const data = await apiFetchJson('/api/auth/me/password', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        current_password: currentPassword,
        new_password: newPassword,
      }),
      errorMessage: '비밀번호를 저장하지 못했습니다.',
    });
    currentUser = data.user || { ...currentUser, password_set: true };
    renderAuthState();
    openProfileModal();
    setProfilePasswordStatus(userHasPassword() ? '비밀번호를 저장했습니다.' : '비밀번호가 등록되었습니다.', 'success');
    showToast('비밀번호를 저장했습니다.', 'success');
  } catch (error) {
    setProfilePasswordStatus(error.message || '비밀번호를 저장하지 못했습니다.', 'error');
  } finally {
    saveBtn.disabled = false;
  }
}

function scheduleGoogleButtonRender() {
  if (googleButtonRetryTimer !== null) return;
  googleButtonRetryTimer = window.setTimeout(() => {
    googleButtonRetryTimer = null;
    googleButtonRetryCount += 1;
    renderGoogleButton();
  }, 300);
}

async function submitGoogleCredential(credential) {
  const data = await apiFetchJson('/api/auth/google', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ credential }),
    errorMessage: 'Google 로그인에 실패했습니다.',
  });
  currentUser = data.user || null;
  return data;
}

async function handleGoogleCredentialResponse(response) {
  const credential = response?.credential;
  if (!credential) {
    showToast('Google 로그인 토큰을 받지 못했습니다.');
    return;
  }

  try {
    await submitGoogleCredential(credential);
    renderAuthState();
    await refreshActivePreference();
    await loadRecentList();
    trackEvent('login_success', { provider: 'google', method: 'popup' });
  } catch (error) {
    trackEvent('login_error', { provider: 'google', reason: 'popup_callback' });
    showToast(error.message || 'Google 로그인에 실패했습니다.');
  }
}

function renderGoogleButton() {
  const container = document.getElementById('googleSignInButton');
  if (!container) return;

  const googleEnabled = Boolean(authConfig?.googleEnabled ?? authConfig?.googleClientId);
  if (currentUser || !googleEnabled || !authConfig?.googleClientId) {
    container.innerHTML = '';
    return;
  }

  if (IS_GITHUB_PAGES_SITE && API_BASE_URL) {
    container.innerHTML = `<a class="auth-login-link" href="${escapeHtml(buildLoginPageUrl())}">서버 버전에서 로그인</a>`;
    return;
  }

  if (!window.google?.accounts?.id) {
    if (googleButtonRetryCount >= 5) {
      container.innerHTML = `<a class="auth-login-link" href="${escapeHtml(buildLoginPageUrl())}">로그인 페이지에서 계속</a>`;
      return;
    }
    scheduleGoogleButtonRender();
    return;
  }

  if (!googleAuthInitialized) {
    window.google.accounts.id.initialize({
      client_id: authConfig.googleClientId,
      auto_select: false,
      callback: handleGoogleCredentialResponse,
    });
    googleAuthInitialized = true;
  }

  container.innerHTML = '';
  window.google.accounts.id.renderButton(container, {
    theme: 'outline',
    size: 'large',
    shape: 'pill',
    text: 'signin_with',
    width: 280,
    locale: 'ko',
  });
}

function updateMobileAuthChrome() {
  const mobileAuth = document.getElementById('mobileAuthStatus');
  const mobileLogin = document.getElementById('mobileLoginBtn');
  if (!mobileAuth) return;
  const compact = isCompactMobileViewport();
  if (currentUser && compact) {
    document.body.classList.add('mobile-auth');
    mobileAuth.style.display = 'flex';
    document.getElementById('mobileAuthAvatar').src = currentUser.picture || '';
    document.getElementById('mobileAuthName').textContent = currentUser.name || currentUser.email;
    if (mobileLogin) mobileLogin.style.display = 'none';
  } else {
    document.body.classList.remove('mobile-auth');
    mobileAuth.style.display = 'none';
    // 비로그인 모바일에서는 사이드바가 숨겨지므로 헤더의 로그인 버튼이 유일한 진입점.
    if (mobileLogin) {
      const showLogin = compact && !currentUser && Boolean(authConfig?.enabled);
      mobileLogin.style.display = showLogin ? 'inline-flex' : 'none';
      if (showLogin) mobileLogin.href = buildLoginPageUrl();
    }
  }
  if (typeof pfSyncMobileFixedView === 'function') pfSyncMobileFixedView();
}

function renderAuthState() {
  const statusTitle = document.getElementById('authStatusTitle');
  const statusDetail = document.getElementById('authStatusDetail');
  const authUser = document.getElementById('authUser');
  const loginLink = document.getElementById('authLoginLink');
  const avatar = document.getElementById('authAvatar');
  const name = document.getElementById('authUserName');
  const email = document.getElementById('authUserEmail');

  updateSidebarTabs();
  updateAnalyticsAuthState();

  if (currentUser) {
    statusTitle.textContent = '내 계정으로 최근 분석을 저장 중입니다';
    statusDetail.textContent = '최근 검색, 관심종목, 개인 메모가 내 계정 기준으로 저장됩니다. 사용자를 누르면 프로필을 열 수 있습니다.';
    authUser.style.display = 'grid';
    loginLink.style.display = 'none';
    avatar.src = currentUser.picture || 'data:image/gif;base64,R0lGODlhAQABAAAAACw=';
    name.textContent = currentUser.name || currentUser.email;
    email.textContent = currentUser.email || '';
  } else if (authConfig?.enabled) {
    statusTitle.textContent = '로그인해 최근 분석을 저장하세요';
    statusDetail.textContent = IS_GITHUB_PAGES_SITE
      ? 'GitHub Pages에서는 서버 버전으로 이동해 로그인한 뒤 개인화 기능을 사용할 수 있습니다.'
      : '이메일/비밀번호 또는 Google로 로그인하면 최근 검색, 관심종목, 개인 메모를 내 계정 기준으로 관리할 수 있습니다.';
    authUser.style.display = 'none';
    loginLink.href = buildLoginPageUrl();
    loginLink.textContent = '로그인 / 가입';
    loginLink.style.display = 'inline-flex';
  } else {
    statusTitle.textContent = '로그인 시스템이 아직 설정되지 않았습니다';
    statusDetail.textContent = '서버 설정이 완료되면 계정별 최근 분석 저장을 사용할 수 있습니다.';
    authUser.style.display = 'none';
    loginLink.style.display = 'none';
  }

  renderGoogleButton();

  updateMobileAuthChrome();
  if (typeof _setFilingReviewAdminAction === 'function') {
    _setFilingReviewAdminAction(activeStockCode || '', {});
  }
}

async function logout() {
  try {
    await apiFetch('/api/auth/logout', { method: 'POST' });
  } catch (e) { console.warn(e); } finally {
    if (window.google?.accounts?.id) {
      window.google.accounts.id.disableAutoSelect();
    }
    currentUser = null;
    closeProfileModal();
    renderAuthState();
    refreshActivePreference();
    trackEvent('logout', { provider: 'account' });
    loadRecentList();
  }
}

function consumeAuthRedirectResult() {
  const url = new URL(window.location.href);
  const auth = url.searchParams.get('auth');
  const authError = url.searchParams.get('auth_error');
  if (!auth && !authError) return;

  url.searchParams.delete('auth');
  url.searchParams.delete('auth_error');
  window.history.replaceState({}, '', url.toString());

  if (auth === 'success') {
    trackEvent('login_success', { provider: 'google', method: 'redirect' });
    return;
  }

  trackEvent('login_error', { provider: 'google', reason: authError || 'unknown' });
  if (authError === 'csrf') {
    showToast('Google 로그인 보안 검증에 실패했습니다. 다시 시도해 주세요.');
  } else if (authError === 'not_configured') {
    showToast('서버 설정이 아직 완료되지 않았습니다.');
  } else {
    showToast('Google 로그인에 실패했습니다.');
  }
}

async function initAuth() {
  await loadAuthConfig();
  await syncAuthState({ refreshPreference: true });
  consumeAuthRedirectResult();
}
