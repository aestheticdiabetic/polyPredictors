/**
 * Polymarket Whale Copier - Frontend Application
 * Single-file vanilla JS for the dashboard SPA.
 */

'use strict';

// ============================================================
// State
// ============================================================
const State = {
  session:           null,
  mode:              'SIMULATION',
  runtimeHours:      null,   // null = manual
  ledgerPage:        1,
  ledgerStatus:      'all',
  ledgerMode:        'SIMULATION',  // scoped to the active tab (SIMULATION or REAL)
  ledgerSort:        'default',  // 'default' = newest first; 'close_asc' = soonest close first
  ledgerSince:       null,       // ISO string — set when "Sessions" tab is active
  ledgerUntil:       null,       // ISO string — upper bound for session filter
  latestSession:     null,       // cached session object for the banner
  // Session browser
  sessionList:       [],         // [{id, mode, started_at, ...}] ordered newest-first
  sessionListPage:   1,
  sessionListTotal:  0,
  viewedSessionId:   null,       // null = latest; int = specific session
  signalsPage:       1,
  signalsTab:        'all',   // 'all' | 'win' | 'loss' | 'open'
  signalsGroupPage:  1,       // client-side page for grouped signal rows
  timerInterval:     null,
  pollInterval:      null,
  ledgerInterval:    null,
  lastActivityMaxId: 0,    // Fix #6: dirty-check — skip re-render when unchanged
};

// Fix #7: in-flight guard — prevents overlapping concurrent fetches per endpoint
const _inFlight = new Set();

// ============================================================
// DOM refs (populated after DOMContentLoaded)
// ============================================================
const $ = id => document.getElementById(id);
let DOM = {};

function cacheDom() {
  DOM = {
    // Header
    modeBadge:       $('mode-badge'),
    sessionTimer:    $('session-timer'),
    timerVal:        $('timer-value'),

    // Controls
    modeSimBtn:      $('mode-sim-btn'),
    modeRealBtn:     $('mode-real-btn'),
    startBtn:        $('start-btn'),
    stopBtn:         $('stop-btn'),
    balanceValue:    $('balance-value'),

    // Stats
    statBets:        $('stat-bets'),
    statCapital:     $('stat-capital'),
    statSimCapital:  $('stat-sim-capital'),
    statWinRate:     $('stat-winrate'),
    statPnl:         $('stat-pnl'),
    statDuration:    $('stat-duration'),

    // Whales
    whaleTableBody:  $('whale-table-body'),
    addWhaleAddr:    $('add-whale-addr'),
    addWhaleAlias:   $('add-whale-alias'),
    addWhaleBtn:     $('add-whale-btn'),
    discoverBtn:     $('discover-btn'),

    // Activity
    activityFeed:    $('activity-feed'),

    // Ledger
    ledgerTableBody: $('ledger-table-body'),
    ledgerPagination:$('ledger-pagination'),
    ledgerInfo:      $('ledger-info'),
    sessionBanner:   $('session-banner'),

    // Signals
    signalsTableBody:  $('signals-table-body'),
    signalsPagination: $('signals-pagination'),
    signalsInfo:       $('signals-info'),

    // Modal
    modalOverlay:    $('modal-overlay'),
    modalBody:       $('modal-body'),
    modalClose:      $('modal-close'),

    // Toast
    toastContainer:  $('toast-container'),
  };
}

// ============================================================
// Init
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  cacheDom();
  bindEvents();
  setMode('SIMULATION');
  setRuntime(null); // Manual by default

  // Initial load
  fetchStatus();
  fetchWhales();
  fetchActivity();
  fetchLedger();
  fetchSignals();

  // Polling
  State.pollInterval = setInterval(() => {
    fetchStatus();
    if (State.session && State.session.is_active) {
      fetchActivity();
    }
  }, 5000);

  State.ledgerInterval = setInterval(() => {
    if (State.session && State.session.is_active) {
      fetchLedger();
      fetchSignals();
    }
  }, 10000);
});

// ============================================================
// Event binding
// ============================================================
function bindEvents() {
  DOM.modeSimBtn.addEventListener('click', () => setMode('SIMULATION'));
  DOM.modeRealBtn.addEventListener('click', () => setMode('REAL'));

  DOM.startBtn.addEventListener('click', startSession);
  DOM.stopBtn.addEventListener('click', stopSession);

  DOM.addWhaleBtn.addEventListener('click', addWhale);
  DOM.addWhaleAddr.addEventListener('keydown', e => { if (e.key === 'Enter') addWhale(); });

  DOM.discoverBtn.addEventListener('click', openDiscoverModal);
  DOM.modalClose.addEventListener('click', closeModal);
  DOM.modalOverlay.addEventListener('click', e => {
    if (e.target === DOM.modalOverlay) closeModal();
  });

  // Runtime selector buttons
  document.querySelectorAll('.runtime-selector button').forEach(btn => {
    btn.addEventListener('click', () => {
      const val = btn.dataset.hours;
      setRuntime(val === 'manual' ? null : parseFloat(val));
      document.querySelectorAll('.runtime-selector button').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    });
  });

  // Ledger filter tabs
  document.querySelectorAll('.filter-tab[data-status]').forEach(tab => {
    tab.addEventListener('click', async () => {
      const s = tab.dataset.status;
      document.querySelectorAll('.filter-tab[data-status]').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      State.ledgerPage = 1;

      if (s === 'session') {
        State.ledgerStatus = 'all';
        State.ledgerSort = 'default';
        updateSortButton();
        State.viewedSessionId = null;
        await fetchAndShowSessionBrowser();
      } else {
        State.ledgerSince = null;
        State.ledgerUntil = null;
        State.latestSession = null;
        if (DOM.sessionBanner) DOM.sessionBanner.style.display = 'none';
        State.ledgerStatus = s;
        State.ledgerSort = s === 'open' ? 'close_asc' : 'default';
        updateSortButton();
        fetchLedger();
      }
    });
  });

  // Refresh profiles button
  const refreshBtn = $('refresh-profiles-btn');
  if (refreshBtn) refreshBtn.addEventListener('click', refreshProfiles);
}

// ============================================================
// Mode & Runtime
// ============================================================
function setMode(mode) {
  State.mode = mode;
  State.ledgerMode = mode;  // ledger/signals/stats now scoped to the viewed tab
  State.ledgerPage = 1;
  if (mode === 'SIMULATION') {
    DOM.modeSimBtn.classList.add('active-sim');
    DOM.modeRealBtn.classList.remove('active-real');
  } else {
    DOM.modeSimBtn.classList.remove('active-sim');
    DOM.modeRealBtn.classList.add('active-real');
  }
  // Re-fetch everything scoped to the new mode
  fetchModeStats();
  fetchLedger();
  fetchSignals();
  // For REAL tab: show live wallet balance immediately (even before a session starts)
  if (mode === 'REAL') fetchWalletBalance();
}

async function fetchWalletBalance() {
  try {
    const data = await api('GET', '/api/wallet/balance');
    DOM.balanceValue.textContent = formatUSDC(data.balance);
  } catch (err) {
    // Credentials not configured or request failed — leave balance as-is
  }
}

function setRuntime(hours) {
  State.runtimeHours = hours;
}

// ============================================================
// Session
// ============================================================
async function startSession() {
  if (DOM.startBtn.disabled) return;

  setButtonLoading(DOM.startBtn, true);
  try {
    const resp = await api('POST', '/api/session/start', {
      mode: State.mode,
      runtime_hours: State.runtimeHours,
    });

    State.session = resp.session;
    State.lastActivityMaxId = 0; // force activity re-render for new session
    updateSessionUI();
    startTimer();
    fetchLedger();
    fetchWhales();
    showToast('Session started in ' + State.mode + ' mode', 'success');
  } catch (err) {
    showToast(err.message || 'Failed to start session', 'error');
  } finally {
    setButtonLoading(DOM.startBtn, false);
  }
}

async function stopSession() {
  if (DOM.stopBtn.disabled) return;

  setButtonLoading(DOM.stopBtn, true);
  try {
    const resp = await api('POST', `/api/session/stop?mode=${State.mode}`);
    State.session = resp.session;
    updateSessionUI();
    stopTimer();
    fetchLedger();
    showToast('Session stopped', 'info');
  } catch (err) {
    showToast(err.message || 'Failed to stop session', 'error');
  } finally {
    setButtonLoading(DOM.stopBtn, false);
  }
}

// ============================================================
// Status polling
// ============================================================
async function fetchStatus() {
  if (_inFlight.has('status')) return;
  _inFlight.add('status');
  try {
    const data = await api('GET', '/api/status');
    // Exclude HEDGE_SIM sessions from the standard dashboard — they run on /hedge
    State.session = (data.session && data.session.mode !== 'HEDGE_SIM') ? data.session : null;
    updateSessionUI();
    await fetchModeStats();
  } catch (err) {
    // Silently fail on status polls
  } finally {
    _inFlight.delete('status');
  }
}

async function fetchModeStats() {
  try {
    const stats = await api('GET', `/api/ledger/stats?mode=${State.mode}`);
    updateStats(stats);
    // REAL mode: always show live wallet balance (session_balance is 0 before first session)
    // SIMULATION: use session balance from stats
    if (State.mode === 'REAL') {
      fetchWalletBalance();
    } else {
      DOM.balanceValue.textContent = formatUSDC(stats.session_balance ?? 0);
    }
  } catch (err) {
    // Silently fail
  }
}

function updateSessionUI() {
  const s = State.session;
  const isActive = s && s.is_active;

  // Mode badge
  if (!s) {
    DOM.modeBadge.textContent = 'IDLE';
    DOM.modeBadge.className = 'mode-badge idle';
  } else if (s.mode === 'SIMULATION') {
    DOM.modeBadge.textContent = 'SIMULATION';
    DOM.modeBadge.className = 'mode-badge simulation';
  } else {
    DOM.modeBadge.textContent = 'REAL';
    DOM.modeBadge.className = 'mode-badge real';
  }

  // Buttons (balance is driven by fetchModeStats, not the session object)
  DOM.startBtn.disabled = isActive;
  DOM.stopBtn.disabled = !isActive;

  if (!isActive) stopTimer();
}

function updateStats(stats) {
  DOM.statBets.textContent  = stats.placed || 0;
  if (DOM.statCapital)    DOM.statCapital.textContent    = '$' + (stats.capital_at_risk     || 0).toFixed(2);
  if (DOM.statSimCapital) DOM.statSimCapital.textContent = '$' + (stats.sim_capital_at_risk || 0).toFixed(2);
  DOM.statWinRate.textContent = (stats.win_rate_pct || 0).toFixed(1) + '%';

  const pnl = stats.total_pnl_usdc || 0;
  DOM.statPnl.textContent = formatPnl(pnl);
  DOM.statPnl.className = 'stat-value ' + (pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : '');
}

// ============================================================
// Timer
// ============================================================
function startTimer() {
  stopTimer();
  State.timerInterval = setInterval(tickTimer, 1000);
  tickTimer();
}

function stopTimer() {
  if (State.timerInterval) {
    clearInterval(State.timerInterval);
    State.timerInterval = null;
  }
}

function tickTimer() {
  const s = State.session;
  if (!s || !s.is_active || !s.started_at) {
    DOM.timerVal.textContent = '00:00:00';
    DOM.statDuration.textContent = '00:00:00';
    return;
  }
  const started = new Date(s.started_at + (s.started_at.endsWith('Z') ? '' : 'Z'));
  const elapsed = Math.floor((Date.now() - started.getTime()) / 1000);
  const fmt = formatDuration(elapsed);
  DOM.timerVal.textContent = fmt;
  DOM.statDuration.textContent = fmt;
}

function formatDuration(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  return [h, m, s].map(n => String(n).padStart(2, '0')).join(':');
}

// ============================================================
// Whales
// ============================================================
async function fetchWhales() {
  try {
    const data = await api('GET', '/api/whales');
    renderWhaleTable(data.whales);
  } catch (err) {
    console.error('fetchWhales error:', err);
  }
}

function renderWhaleTable(whales) {
  if (!whales || whales.length === 0) {
    DOM.whaleTableBody.innerHTML = `
      <tr>
        <td colspan="5">
          <div class="empty-state">
            <div class="empty-icon">🐋</div>
            <p>No whales tracked. Add one below or discover from the leaderboard.</p>
          </div>
        </td>
      </tr>`;
    return;
  }

  DOM.whaleTableBody.innerHTML = whales.map(w => `
    <tr>
      <td class="whale-alias-cell">${escHtml(w.alias)}</td>
      <td class="whale-addr mono">${formatAddress(w.address)}</td>
      <td class="mono">${w.avg_bet_size_usdc > 0 ? '$' + w.avg_bet_size_usdc.toFixed(0) : '—'}</td>
      <td>
        <label class="toggle-switch">
          <input type="checkbox" ${w.is_active ? 'checked' : ''}
            onchange="toggleWhale('${w.address}', this.checked)">
          <span class="toggle-slider"></span>
        </label>
      </td>
      <td>
        <button class="btn btn-danger btn-sm" onclick="removeWhale('${w.address}')">Remove</button>
      </td>
    </tr>
  `).join('');
}

async function addWhale() {
  const address = DOM.addWhaleAddr.value.trim();
  const alias   = DOM.addWhaleAlias.value.trim();
  if (!address) { showToast('Enter a wallet address', 'warning'); return; }

  setButtonLoading(DOM.addWhaleBtn, true);
  try {
    await api('POST', '/api/whales', { address, alias });
    DOM.addWhaleAddr.value = '';
    DOM.addWhaleAlias.value = '';
    await fetchWhales();
    showToast('Whale added: ' + formatAddress(address), 'success');
  } catch (err) {
    showToast(err.message || 'Failed to add whale', 'error');
  } finally {
    setButtonLoading(DOM.addWhaleBtn, false);
  }
}

async function removeWhale(address) {
  if (!confirm(`Remove whale ${formatAddress(address)}?`)) return;
  try {
    await api('DELETE', `/api/whales/${address}`);
    await fetchWhales();
    showToast('Whale removed', 'info');
  } catch (err) {
    showToast(err.message || 'Failed to remove whale', 'error');
  }
}

async function toggleWhale(address, active) {
  try {
    await api('PATCH', `/api/whales/${address}/toggle`);
    showToast((active ? 'Activated' : 'Deactivated') + ' ' + formatAddress(address), 'info');
    await fetchWhales();
  } catch (err) {
    showToast(err.message || 'Toggle failed', 'error');
    await fetchWhales(); // revert UI
  }
}

async function refreshProfiles() {
  const btn = $('refresh-profiles-btn');
  setButtonLoading(btn, true);
  try {
    await api('POST', '/api/whales/refresh-profiles');
    await fetchWhales();
    showToast('Risk profiles refreshed', 'success');
  } catch (err) {
    showToast(err.message || 'Refresh failed', 'error');
  } finally {
    setButtonLoading(btn, false);
  }
}

// ============================================================
// Discover modal
// ============================================================
async function openDiscoverModal() {
  DOM.modalBody.innerHTML = '<div style="text-align:center;padding:30px;color:var(--text-muted)">Loading leaderboard...</div>';
  DOM.modalOverlay.classList.add('visible');

  try {
    const data = await api('GET', '/api/leaderboard?limit=50');
    renderLeaderboard(data.leaderboard);
  } catch (err) {
    DOM.modalBody.innerHTML = `<p style="color:var(--danger);padding:20px">Failed to load: ${escHtml(err.message)}</p>`;
  }
}

function renderLeaderboard(entries) {
  if (!entries || entries.length === 0) {
    DOM.modalBody.innerHTML = '<p style="color:var(--text-muted);padding:20px">No leaderboard data available.</p>';
    return;
  }

  DOM.modalBody.innerHTML = entries.map(e => `
    <div class="leaderboard-item">
      <div class="leaderboard-info">
        <div class="leaderboard-alias">${escHtml(e.alias)}</div>
        <div class="leaderboard-addr">${formatAddress(e.address)}</div>
      </div>
      <div class="leaderboard-stats">
        <div class="leaderboard-stat">
          <div class="lbl">P&L</div>
          <div class="val ${e.pnl_usdc >= 0 ? 'text-success' : 'text-danger'}">${formatPnl(e.pnl_usdc)}</div>
        </div>
        <div class="leaderboard-stat">
          <div class="lbl">Volume</div>
          <div class="val">${formatUSDC(e.volume_usdc)}</div>
        </div>
      </div>
      ${e.already_tracked
        ? '<span class="badge badge-open">Tracking</span>'
        : `<button class="btn btn-sm btn-primary" onclick="addFromLeaderboard('${escHtml(e.address)}', '${escHtml(e.alias)}', this)">Add</button>`
      }
    </div>
  `).join('');
}

async function addFromLeaderboard(address, alias, btn) {
  btn.disabled = true;
  btn.textContent = '...';
  try {
    await api('POST', '/api/whales', { address, alias });
    btn.textContent = 'Added';
    btn.className = 'btn btn-sm badge-open';
    await fetchWhales();
    showToast('Added: ' + escHtml(alias), 'success');
  } catch (err) {
    btn.disabled = false;
    btn.textContent = 'Add';
    showToast(err.message || 'Failed to add', 'error');
  }
}

function closeModal() {
  DOM.modalOverlay.classList.remove('visible');
}

// ============================================================
// Activity Feed
// ============================================================
async function fetchActivity() {
  if (_inFlight.has('activity')) return;
  _inFlight.add('activity');
  try {
    const data = await api('GET', '/api/activity?mode=standard');
    // Fix #6: skip full DOM re-render when feed hasn't changed
    if (data.max_id === State.lastActivityMaxId) return;
    State.lastActivityMaxId = data.max_id;
    renderActivity(data.activity);
  } catch (err) {
    console.error('fetchActivity error:', err);
  } finally {
    _inFlight.delete('activity');
  }
}

function renderActivity(items) {
  if (!items || items.length === 0) {
    DOM.activityFeed.innerHTML = '<div class="activity-feed-empty">Waiting for whale activity...</div>';
    return;
  }

  DOM.activityFeed.innerHTML = items.map(item => {
    const isSkipped = item.status === 'SKIPPED';
    const isExit   = item.side === 'SELL';
    const dotClass = isExit ? 'exited' : isSkipped ? 'skipped' : 'copied';
    const actionLabel = isExit ? 'EXITED' : isSkipped ? 'SKIPPED' : 'COPIED';
    const badgeClass = isExit ? 'badge-open' : isSkipped ? 'badge-skip' : 'badge-win';

    const time = item.opened_at
      ? new Date(item.opened_at + (item.opened_at.endsWith('Z') ? '' : 'Z')).toLocaleTimeString()
      : '—';

    return `
      <div class="activity-item">
        <span class="activity-dot ${dotClass}"></span>
        <div class="activity-content">
          <div class="activity-market">${escHtml(truncate(item.question || item.market_id, 55))}</div>
          <div class="activity-meta">
            <b>${escHtml(item.whale_alias || formatAddress(item.whale_address))}</b>
            &bull; ${item.outcome || '?'} &bull; ${time}
          </div>
        </div>
        <div class="activity-action">
          <span class="badge ${badgeClass}">${actionLabel}</span>
          ${item.size_usdc > 0 ? `<span class="activity-size">$${item.size_usdc.toFixed(2)}</span>` : ''}
        </div>
      </div>`;
  }).join('');
}

// ============================================================
// Ledger
// ============================================================
async function fetchLedger() {
  if (_inFlight.has('ledger')) return;
  _inFlight.add('ledger');
  try {
    const params = new URLSearchParams({
      page: State.ledgerPage,
      limit: 50,
      status: State.ledgerStatus,
      mode: State.ledgerMode,
      sort: State.ledgerSort,
    });
    if (State.ledgerSince) params.set('since', State.ledgerSince);
    if (State.ledgerUntil) params.set('until', State.ledgerUntil);
    const data = await api('GET', `/api/ledger?${params}`);
    renderLedger(data.bets, data.pagination);
  } catch (err) {
    console.error('fetchLedger error:', err);
  } finally {
    _inFlight.delete('ledger');
  }
}

function renderLedger(bets, pagination) {
  if (!bets || bets.length === 0) {
    DOM.ledgerTableBody.innerHTML = `
      <tr>
        <td colspan="10">
          <div class="empty-state">
            <div class="empty-icon">📋</div>
            <p>No bets yet. Start a session and add whales to track.</p>
          </div>
        </td>
      </tr>`;
    renderPagination(pagination);
    return;
  }

  DOM.ledgerTableBody.innerHTML = bets.map(bet => {
    const rowClass = {
      CLOSED_WIN:     'row-win',
      CLOSED_LOSS:    'row-loss',
      OPEN:           'row-open',
      SKIPPED:        'row-skip',
      CLOSED_NEUTRAL: '',
    }[bet.status] || '';

    const badgeClass = {
      CLOSED_WIN:     'badge-win',
      CLOSED_LOSS:    'badge-loss',
      OPEN:           'badge-open',
      SKIPPED:        'badge-skip',
      CLOSED_NEUTRAL: 'badge-neutral',
      PENDING:        'badge-neutral',
    }[bet.status] || 'badge-neutral';

    const statusLabel = {
      CLOSED_WIN:     'WIN',
      CLOSED_LOSS:    'LOSS',
      OPEN:           'OPEN',
      SKIPPED:        'SKIP',
      CLOSED_NEUTRAL: 'NEUTRAL',
      PENDING:        'PENDING',
    }[bet.status] || bet.status;

    const time = bet.opened_at
      ? new Date(bet.opened_at + (bet.opened_at.endsWith('Z') ? '' : 'Z')).toLocaleString()
      : '—';

    const pnlHtml = bet.pnl_usdc != null
      ? `<span class="${bet.pnl_usdc > 0 ? 'pnl-positive' : bet.pnl_usdc < 0 ? 'pnl-negative' : 'pnl-zero'}">${formatPnl(bet.pnl_usdc)}</span>`
      : '<span class="text-muted">—</span>';

    const actionDir = bet.side === 'SELL' ? 'EXIT' : 'BUY ' + (bet.outcome || '');

    const whaleLabel = escHtml(bet.whale_alias || formatAddress(bet.whale_address));

    const closesHtml = formatClosesIn(bet.market_close_at, bet.status, bet.market_category);

    return `
      <tr class="${rowClass}">
        <td class="mono text-muted" style="font-size:0.75rem">${time}</td>
        <td title="${escHtml(bet.whale_address)}">${whaleLabel}</td>
        <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(bet.question)}">${escHtml(truncate(bet.question || bet.market_id, 40))}</td>
        <td><span class="badge ${bet.mode === 'SIMULATION' ? 'badge-sim' : 'badge-real'}">${actionDir}</span>${bet.followed_signal_id ? ' <span class="badge badge-signal" title="Placed by follow-add-signals (signal #' + bet.followed_signal_id + ')">SIG+</span>' : ''}</td>
        <td class="mono">${bet.price_at_entry ? bet.price_at_entry.toFixed(3) : '—'}</td>
        <td class="mono">${bet.size_usdc > 0 ? '$' + bet.size_usdc.toFixed(2) : '—'}</td>
        <td>${pnlHtml}</td>
        <td class="mono" style="font-size:0.8rem">${closesHtml}</td>
        <td><span class="badge ${badgeClass}">${statusLabel}</span></td>
        <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text-muted);font-size:0.75rem"
            title="${escHtml(bet.close_reason || bet.skip_reason || '')}">${escHtml(truncate(bet.close_reason || bet.skip_reason || '', 30))}</td>
      </tr>`;
  }).join('');

  renderPagination(pagination);
}

function renderPagination(pagination) {
  if (!pagination) return;
  const { page, pages, total, limit } = pagination;
  const start = (page - 1) * limit + 1;
  const end   = Math.min(page * limit, total);

  DOM.ledgerInfo.textContent = total > 0 ? `Showing ${start}–${end} of ${total}` : 'No results';

  let html = '';
  html += `<button onclick="changeLedgerPage(${page - 1})" ${page <= 1 ? 'disabled' : ''}>&lsaquo;</button>`;

  // Show at most 7 page buttons
  const lo = Math.max(1, page - 3);
  const hi = Math.min(pages, page + 3);
  if (lo > 1) html += `<button onclick="changeLedgerPage(1)">1</button><span class="pagination-info">...</span>`;
  for (let i = lo; i <= hi; i++) {
    html += `<button onclick="changeLedgerPage(${i})" class="${i === page ? 'active' : ''}">${i}</button>`;
  }
  if (hi < pages) html += `<span class="pagination-info">...</span><button onclick="changeLedgerPage(${pages})">${pages}</button>`;

  html += `<button onclick="changeLedgerPage(${page + 1})" ${page >= pages ? 'disabled' : ''}>&rsaquo;</button>`;
  DOM.ledgerPagination.innerHTML = html;
}

function changeLedgerPage(page) {
  if (page < 1) return;
  State.ledgerPage = page;
  fetchLedger();
}

// ============================================================
// Session Banner — delegates to session_browser.js
// ============================================================
async function fetchAndShowSessionBanner() {
  State.viewedSessionId = null;
  await fetchAndShowSessionBrowser();
}

// ============================================================
// Signals
// ============================================================
let _cachedSignals = [];

async function fetchSignals() {
  if (_inFlight.has('signals')) return;
  _inFlight.add('signals');
  try {
    const [data, stats] = await Promise.all([
      api('GET', `/api/signals?page=1&limit=2000&mode=${State.mode}`),
      api('GET', `/api/signals/stats?mode=${State.mode}`),
    ]);
    _cachedSignals = data.signals || [];
    renderSignalsWidget(stats);
    renderSignalsGrouped(_cachedSignals, State.signalsTab, State.signalsGroupPage);
  } catch (err) {
    console.error('fetchSignals error:', err);
  } finally {
    _inFlight.delete('signals');
  }
}

function setSignalsTab(tab) {
  State.signalsTab = tab;
  State.signalsGroupPage = 1;
  const container = $('sig-filter-tabs');
  if (container) {
    container.querySelectorAll('.filter-tab').forEach(btn => btn.classList.remove('active'));
    const idx = ['all','win','loss','open'].indexOf(tab);
    if (idx >= 0) container.querySelectorAll('.filter-tab')[idx].classList.add('active');
  }
  renderSignalsGrouped(_cachedSignals, tab, 1);
}

function changeSignalsPage(page) {
  if (page < 1) return;
  State.signalsGroupPage = page;
  renderSignalsGrouped(_cachedSignals, State.signalsTab, page);
}

const SIGNALS_PER_PAGE = 20;

function renderSignalsWidget(stats) {
  const widget = $('signals-verdict-widget');
  if (!widget) return;

  if (!stats || stats.total_signals === 0) {
    widget.style.display = 'none';
    return;
  }

  widget.style.display = '';

  // Header: verdict banner
  const header = $('signals-verdict-header');
  const verdictCfg = {
    follow:             { bg: 'rgba(0,200,100,0.12)', color: 'var(--success)', icon: '✅', text: 'Worth Following — these double-downs have been profitable' },
    avoid:              { bg: 'rgba(220,50,50,0.10)',  color: 'var(--danger)',  icon: '⚠️', text: 'Caution — following these has historically lost money' },
    neutral:            { bg: 'rgba(255,200,0,0.10)',  color: 'var(--warning)', icon: '⚖️', text: 'Mixed Results — no strong edge either way' },
    insufficient_data:  { bg: 'rgba(255,255,255,0.04)', color: 'var(--text-muted)', icon: 'ℹ️', text: 'Awaiting more resolved signals to make a recommendation' },
  };
  const cfg = verdictCfg[stats.verdict] || verdictCfg.insufficient_data;
  header.style.background = cfg.bg;
  header.style.color = cfg.color;
  header.innerHTML = `<span>${cfg.icon}</span> Should I follow these signals? &nbsp;<span style="font-weight:400">${cfg.text}</span>`;

  // Stat cells
  $('sig-stat-total').textContent    = stats.total_signals;
  $('sig-stat-resolved').textContent = stats.resolved_signals > 0
    ? `${stats.resolved_signals} (${stats.profitable}W / ${stats.losing}L)`
    : stats.resolved_signals;

  const wr = $('sig-stat-winrate');
  wr.textContent = stats.win_rate_pct != null ? stats.win_rate_pct.toFixed(1) + '%' : '—';
  wr.style.color = stats.win_rate_pct > 55 ? 'var(--success)' : stats.win_rate_pct < 45 ? 'var(--danger)' : '';

  const pnlEl = $('sig-stat-pnl');
  pnlEl.textContent = stats.total_pnl_usdc != null ? formatPnl(stats.total_pnl_usdc) : '—';
  pnlEl.style.color = stats.total_pnl_usdc > 0 ? 'var(--success)' : stats.total_pnl_usdc < 0 ? 'var(--danger)' : '';

  const avgEl = $('sig-stat-avg');
  avgEl.textContent = stats.avg_pnl_per_signal != null ? formatPnl(stats.avg_pnl_per_signal) : '—';
  avgEl.style.color = stats.avg_pnl_per_signal > 0 ? 'var(--success)' : stats.avg_pnl_per_signal < 0 ? 'var(--danger)' : '';

  $('sig-stat-open').textContent = stats.open_signals;
}

function renderSignalsGrouped(signals, tab, page) {
  page = page || 1;
  const tbody = DOM.signalsTableBody;
  if (!tbody) return;

  // Filter by tab (one row per signal)
  let rows = (signals || []).slice();
  if (tab === 'win')  rows = rows.filter(s => s.bet_status === 'CLOSED_WIN');
  if (tab === 'loss') rows = rows.filter(s => s.bet_status === 'CLOSED_LOSS');
  if (tab === 'open') rows = rows.filter(s => s.bet_status === 'OPEN' || s.bet_status === 'PENDING');

  const total = rows.length;
  const pages = Math.max(1, Math.ceil(total / SIGNALS_PER_PAGE));
  const safePage = Math.min(page, pages);
  const start = (safePage - 1) * SIGNALS_PER_PAGE;
  rows = rows.slice(start, start + SIGNALS_PER_PAGE);

  // Render pagination
  if (DOM.signalsInfo) {
    const end = Math.min(start + SIGNALS_PER_PAGE, total);
    DOM.signalsInfo.textContent = total > 0 ? `Showing ${start + 1}–${end} of ${total}` : '';
  }
  if (DOM.signalsPagination) {
    if (pages <= 1) {
      DOM.signalsPagination.innerHTML = '';
    } else {
      let ph = '';
      ph += `<button onclick="changeSignalsPage(${safePage - 1})" ${safePage <= 1 ? 'disabled' : ''}>&lsaquo;</button>`;
      for (let i = 1; i <= pages; i++) {
        ph += `<button onclick="changeSignalsPage(${i})" class="${i === safePage ? 'active' : ''}">${i}</button>`;
      }
      ph += `<button onclick="changeSignalsPage(${safePage + 1})" ${safePage >= pages ? 'disabled' : ''}>&rsaquo;</button>`;
      DOM.signalsPagination.innerHTML = ph;
    }
  }

  if (rows.length === 0) {
    const labels = { win: 'successful', loss: 'failed', open: 'open', all: '' };
    tbody.innerHTML = `<tr><td colspan="7"><div class="empty-state"><p>No ${labels[tab] || ''} double-down signals yet.</p></div></td></tr>`;
    return;
  }

  const STATUS_BADGE = {
    CLOSED_WIN:     '<span class="badge badge-win">WIN</span>',
    CLOSED_LOSS:    '<span class="badge badge-loss">LOSS</span>',
    CLOSED_NEUTRAL: '<span class="badge badge-neutral">NEUTRAL</span>',
    OPEN:           '<span class="badge badge-open">OPEN</span>',
    PENDING:        '<span class="badge badge-neutral">PENDING</span>',
  };

  tbody.innerHTML = rows.map(s => {
    const statusBadge = STATUS_BADGE[s.bet_status]
      || `<span class="badge badge-neutral">${escHtml(s.bet_status || '?')}</span>`;

    let pnlHtml;
    if (s.bet_status === 'OPEN' || s.bet_status === 'PENDING') {
      pnlHtml = '<span class="text-muted">pending…</span>';
    } else if (s.hypothetical_pnl_usdc == null) {
      pnlHtml = '<span class="text-muted">—</span>';
    } else {
      const cls = s.hypothetical_pnl_usdc > 0 ? 'pnl-positive' : s.hypothetical_pnl_usdc < 0 ? 'pnl-negative' : 'pnl-zero';
      pnlHtml = `<span class="${cls}">${formatPnl(s.hypothetical_pnl_usdc)}</span>`;
    }

    const investment   = s.suggested_add_usdc ?? s.whale_additional_usdc;
    const market       = escHtml(truncate(s.bet_question || `Bet #${s.copied_bet_id}`, 50));
    const whaleLabel   = escHtml(s.whale_alias || s.whale_address || '—');
    const lastTs       = s.last_addition_at || s.timestamp;
    const ts           = lastTs ? new Date(lastTs).toLocaleDateString() : '—';
    const count        = s.addition_count || 1;
    const countBadge   = count > 1
      ? ` <span style="font-size:0.75rem;color:var(--text-muted)">×${count}</span>`
      : '';

    return `
      <tr>
        <td title="${escHtml(s.bet_question || '')}">${market}</td>
        <td style="font-size:0.8rem;color:var(--text-muted)" title="${escHtml(s.whale_address || '')}">${whaleLabel}</td>
        <td class="mono text-muted">$${s.whale_additional_usdc.toFixed(2)}${countBadge}</td>
        <td class="mono">$${investment.toFixed(2)}</td>
        <td class="mono">${s.price.toFixed(4)}</td>
        <td>${statusBadge}</td>
        <td>${pnlHtml}</td>
      </tr>`;
  }).join('');
}

// Whale analysis moved to /whales page

// ============================================================
// Helpers
// ============================================================
async function api(method, url, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' },
  };
  if (body !== undefined) opts.body = JSON.stringify(body);

  const resp = await fetch(url, opts);
  if (!resp.ok) {
    let detail = `HTTP ${resp.status}`;
    try {
      const err = await resp.json();
      detail = err.detail || err.message || detail;
    } catch (_) {}
    throw new Error(detail);
  }
  return resp.json();
}

function toggleLedgerSort() {
  State.ledgerSort = State.ledgerSort === 'close_asc' ? 'default' : 'close_asc';
  State.ledgerPage = 1;
  updateSortButton();
  fetchLedger();
}

function updateSortButton() {
  const btn = $('ledger-sort-btn');
  if (!btn) return;
  const isOpen = State.ledgerStatus === 'open';
  btn.style.display = isOpen ? '' : 'none';
  if (State.ledgerSort === 'close_asc') {
    btn.textContent = '⏱ Soonest First';
    btn.classList.add('active-sim');
  } else {
    btn.textContent = '🕒 Newest First';
    btn.classList.remove('active-sim');
  }
}

const LIVE_SPORT_CATEGORIES = new Set([
  'Soccer', 'Basketball', 'Tennis', 'eSports',
  'American Football', 'Baseball', 'Hockey',
]);

function formatClosesIn(marketCloseAt, status, marketCategory) {
  if (!marketCloseAt) return '<span class="text-muted">—</span>';

  // Treat stored value as UTC (naive UTC from backend)
  const closeStr = marketCloseAt.endsWith('Z') ? marketCloseAt : marketCloseAt + 'Z';
  const closeMs = new Date(closeStr).getTime();
  const nowMs = Date.now();
  const diffMs = closeMs - nowMs;
  const diffSec = Math.floor(diffMs / 1000);

  if (diffSec < 0) {
    // Market trading window has closed. For live sports the endDate is the
    // game START / trading-close time — the oracle may not settle for hours
    // after play ends. Show "Pending" so the bet isn't mis-labelled EXPIRED
    // while the game is still in progress.
    if (status === 'OPEN') {
      if (LIVE_SPORT_CATEGORIES.has(marketCategory)) {
        return '<span style="color:var(--warning);font-weight:600">Pending</span>';
      }
      return '<span style="color:var(--danger);font-weight:600">EXPIRED</span>';
    }
    return '<span class="text-muted">closed</span>';
  }

  const days  = Math.floor(diffSec / 86400);
  const hours = Math.floor((diffSec % 86400) / 3600);
  const mins  = Math.floor((diffSec % 3600) / 60);

  let label;
  if (days > 0)       label = `${days}d ${hours}h`;
  else if (hours > 0) label = `${hours}h ${mins}m`;
  else                label = `${mins}m`;

  // Highlight near-expiry (< 1h)
  const style = diffSec < 3600 ? 'color:var(--warning);font-weight:600' : '';
  return style ? `<span style="${style}">${label}</span>` : label;
}

function formatAddress(addr) {
  if (!addr || addr.length < 12) return addr || '—';
  return addr.slice(0, 6) + '...' + addr.slice(-4);
}

function formatUSDC(amount) {
  if (amount == null) return '—';
  return '$' + Number(amount).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatPnl(pnl) {
  if (pnl == null) return '—';
  const sign = pnl >= 0 ? '+' : '-';
  return sign + '$' + Math.abs(pnl).toFixed(2);
}

function truncate(str, len) {
  if (!str) return '';
  return str.length > len ? str.slice(0, len) + '…' : str;
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function setButtonLoading(btn, loading) {
  if (!btn) return;
  if (loading) {
    btn._originalHTML = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>';
  } else {
    btn.disabled = false;
    if (btn._originalHTML) btn.innerHTML = btn._originalHTML;
  }
}

function showToast(message, type = 'info') {
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  DOM.toastContainer.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transition = 'opacity 0.3s';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}
