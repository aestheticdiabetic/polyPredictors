/**
 * Polymarket Whale Copier - Frontend Application
 * Single-file vanilla JS for the dashboard SPA.
 */

'use strict';

// ============================================================
// State
// ============================================================
const State = {
  session:       null,
  mode:          'SIMULATION',
  runtimeHours:  null,   // null = manual
  ledgerPage:    1,
  ledgerStatus:  'all',
  ledgerMode:    'all',
  signalsPage:   1,
  timerInterval: null,
  pollInterval:  null,
  ledgerInterval: null,
};

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

    // Signals
    signalsTableBody:$('signals-table-body'),

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
    tab.addEventListener('click', () => {
      State.ledgerStatus = tab.dataset.status;
      State.ledgerPage = 1;
      document.querySelectorAll('.filter-tab[data-status]').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      fetchLedger();
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
  if (mode === 'SIMULATION') {
    DOM.modeSimBtn.classList.add('active-sim');
    DOM.modeRealBtn.classList.remove('active-real');
  } else {
    DOM.modeSimBtn.classList.remove('active-sim');
    DOM.modeRealBtn.classList.add('active-real');
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
    const resp = await api('POST', '/api/session/stop');
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
  try {
    const data = await api('GET', '/api/status');
    State.session = data.session;
    updateSessionUI();

    // Update stats from ledger stats
    const stats = await api('GET', '/api/ledger/stats');
    updateStats(stats);
  } catch (err) {
    // Silently fail on status polls
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

  // Balance
  const balance = s ? s.current_balance_usdc : 200;
  DOM.balanceValue.textContent = formatUSDC(balance);

  // Buttons
  DOM.startBtn.disabled = isActive;
  DOM.stopBtn.disabled = !isActive;

  if (!isActive) stopTimer();
}

function updateStats(stats) {
  DOM.statBets.textContent  = stats.placed || 0;
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
  try {
    const data = await api('GET', '/api/activity');
    renderActivity(data.activity);
  } catch (err) {
    console.error('fetchActivity error:', err);
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
  try {
    const params = new URLSearchParams({
      page: State.ledgerPage,
      limit: 50,
      status: State.ledgerStatus,
      mode: State.ledgerMode,
    });
    const data = await api('GET', `/api/ledger?${params}`);
    renderLedger(data.bets, data.pagination);
  } catch (err) {
    console.error('fetchLedger error:', err);
  }
}

function renderLedger(bets, pagination) {
  if (!bets || bets.length === 0) {
    DOM.ledgerTableBody.innerHTML = `
      <tr>
        <td colspan="9">
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

    return `
      <tr class="${rowClass}">
        <td class="mono text-muted" style="font-size:0.75rem">${time}</td>
        <td class="whale-addr mono">${formatAddress(bet.whale_address)}</td>
        <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(bet.question)}">${escHtml(truncate(bet.question || bet.market_id, 40))}</td>
        <td><span class="badge ${bet.mode === 'SIMULATION' ? 'badge-sim' : 'badge-real'}">${actionDir}</span></td>
        <td class="mono">${bet.price_at_entry ? bet.price_at_entry.toFixed(3) : '—'}</td>
        <td class="mono">${bet.size_usdc > 0 ? '$' + bet.size_usdc.toFixed(2) : '—'}</td>
        <td>${pnlHtml}</td>
        <td><span class="badge ${badgeClass}">${statusLabel}</span></td>
        <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text-muted);font-size:0.75rem"
            title="${escHtml(bet.skip_reason || '')}">${escHtml(bet.skip_reason ? truncate(bet.skip_reason, 30) : '')}</td>
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
// Signals
// ============================================================
async function fetchSignals() {
  try {
    const data = await api('GET', `/api/signals?page=${State.signalsPage}&limit=20`);
    renderSignals(data.signals);
  } catch (err) {
    console.error('fetchSignals error:', err);
  }
}

function renderSignals(signals) {
  const tbody = DOM.signalsTableBody;
  if (!tbody) return;

  if (!signals || signals.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="6">
          <div class="empty-state">
            <p>No add-to-position signals yet.</p>
          </div>
        </td>
      </tr>`;
    return;
  }

  tbody.innerHTML = signals.map(s => {
    const time = new Date(s.timestamp + (s.timestamp.endsWith('Z') ? '' : 'Z')).toLocaleString();
    const hypo = s.hypothetical_pnl_usdc != null
      ? formatPnl(s.hypothetical_pnl_usdc)
      : '—';
    return `
      <tr>
        <td class="mono text-muted" style="font-size:0.75rem">${time}</td>
        <td class="mono text-muted">#${s.copied_bet_id}</td>
        <td class="mono">$${s.whale_additional_usdc.toFixed(2)}</td>
        <td class="mono">${s.price.toFixed(4)}</td>
        <td>${hypo}</td>
        <td style="color:var(--text-muted);font-size:0.75rem">${escHtml(s.note || '')}</td>
      </tr>`;
  }).join('');
}

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
  const sign = pnl >= 0 ? '+' : '';
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
