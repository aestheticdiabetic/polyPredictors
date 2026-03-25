/**
 * Session Browser — per-session stats and navigation
 */

'use strict';

async function fetchAndShowSessionBrowser() {
  if (!DOM.sessionBanner) return;
  DOM.sessionBanner.textContent = '';
  const loading = document.createElement('div');
  loading.style.cssText = 'padding:12px 16px;color:var(--text-muted);font-size:0.8rem';
  loading.textContent = 'Loading sessions\u2026';
  DOM.sessionBanner.appendChild(loading);
  DOM.sessionBanner.style.display = 'block';

  try {
    const listData = await api('GET', `/api/sessions/list?mode=${State.mode}&limit=200`);
    State.sessionList = listData.sessions || [];

    if (State.sessionList.length === 0) {
      DOM.sessionBanner.textContent = '';
      const empty = document.createElement('div');
      empty.style.cssText = 'padding:12px 16px;color:var(--text-muted);font-size:0.8rem';
      empty.textContent = 'No sessions found.';
      DOM.sessionBanner.appendChild(empty);
      fetchLedger();
      return;
    }

    if (!State.viewedSessionId) {
      State.viewedSessionId = State.sessionList[0].id;
    }
    await loadSessionById(State.viewedSessionId);
  } catch (err) {
    console.error('fetchAndShowSessionBrowser error:', err);
    DOM.sessionBanner.textContent = '';
    const errEl = document.createElement('div');
    errEl.style.cssText = 'padding:12px 16px;color:var(--danger);font-size:0.8rem';
    errEl.textContent = 'Failed to load sessions.';
    DOM.sessionBanner.appendChild(errEl);
    fetchLedger();
  }
}

async function loadSessionById(sessionId) {
  State.viewedSessionId = sessionId;
  try {
    const data = await api('GET', `/api/sessions/${sessionId}/stats`);
    const s = data.session;
    if (!s) return;
    State.latestSession = s;
    State.ledgerSince = s.started_at;
    State.ledgerUntil = s.stopped_at || null;
    renderSessionBrowser(s);
    fetchLedger();
  } catch (err) {
    console.error('loadSessionById error:', err);
  }
}

function _makeBtn(text, enabled, onClick) {
  const btn = document.createElement('button');
  btn.textContent = text;
  btn.disabled = !enabled;
  btn.style.cssText = [
    'background:none',
    'border:1px solid var(--border)',
    `color:${enabled ? 'var(--text)' : 'var(--text-muted)'}`,
    'border-radius:4px',
    'padding:2px 8px',
    `cursor:${enabled ? 'pointer' : 'default'}`,
    'font-size:0.8rem',
  ].join(';');
  if (enabled) btn.addEventListener('click', onClick);
  return btn;
}

function _makeStatCell(label, value, color) {
  const cell = document.createElement('div');
  cell.className = 'signal-stat-cell';
  const lbl = document.createElement('div');
  lbl.className = 'signal-stat-label';
  lbl.textContent = label;
  const val = document.createElement('div');
  val.className = 'signal-stat-value';
  val.textContent = value;
  if (color) val.style.color = color;
  cell.append(lbl, val);
  return cell;
}

function renderSessionBrowser(s) {
  if (!DOM.sessionBanner) return;
  DOM.sessionBanner.textContent = '';

  const list = State.sessionList;
  const idx = list.findIndex(x => x.id === s.id);
  const hasOlder = idx < list.length - 1;
  const hasNewer = idx > 0;

  const startedStr = s.started_at
    ? new Date(s.started_at.endsWith('Z') ? s.started_at : s.started_at + 'Z').toLocaleString()
    : '\u2014';

  let durationStr = '\u2014';
  if (s.duration_seconds != null) {
    const h = Math.floor(s.duration_seconds / 3600);
    const m = Math.floor((s.duration_seconds % 3600) / 60);
    durationStr = h > 0 ? `${h}h ${m}m` : `${m}m`;
  }

  const pnl = s.total_pnl_usdc || 0;
  const pnlColor = pnl > 0 ? 'var(--success)' : pnl < 0 ? 'var(--danger)' : 'var(--text-muted)';
  const pnlStr = (pnl >= 0 ? '+$' : '-$') + Math.abs(pnl).toFixed(2);

  // --- Nav row ---
  const navRow = document.createElement('div');
  navRow.style.cssText = 'padding:8px 12px;background:var(--bg-secondary);display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--border);flex-wrap:wrap';

  const idLabel = document.createElement('span');
  idLabel.style.cssText = 'font-weight:600;font-size:0.85rem';
  idLabel.textContent = `Session #${s.id}`;

  const modeBadge = document.createElement('span');
  const modeColor = s.mode === 'SIMULATION' ? 'var(--warning)' : 'var(--danger)';
  const modeBg    = s.mode === 'SIMULATION' ? 'var(--warning-dim)' : 'var(--danger-dim)';
  modeBadge.style.cssText = `background:${modeBg};color:${modeColor};padding:2px 8px;border-radius:10px;font-size:0.72rem;font-weight:700`;
  modeBadge.textContent = s.mode;

  const statusBadge = document.createElement('span');
  statusBadge.style.cssText = `font-weight:600;font-size:0.78rem;color:${s.is_active ? 'var(--success)' : 'var(--text-muted)'}`;
  statusBadge.textContent = s.is_active ? 'ACTIVE' : 'STOPPED';

  const timeLabel = document.createElement('span');
  timeLabel.style.cssText = 'font-size:0.76rem;color:var(--text-muted)';
  timeLabel.textContent = `${startedStr} \u2014 ${durationStr}`;

  const counter = document.createElement('span');
  counter.style.cssText = 'margin-left:auto;font-size:0.72rem;color:var(--text-muted)';
  counter.textContent = `${idx + 1} of ${list.length}`;

  navRow.append(
    _makeBtn('\u2190 Older', hasOlder, () => navigateSession('older')),
    idLabel, modeBadge, statusBadge, timeLabel, counter,
    _makeBtn('Newer \u2192', hasNewer, () => navigateSession('newer')),
  );

  // --- Stats grid ---
  const statsGrid = document.createElement('div');
  statsGrid.style.cssText = 'display:grid;grid-template-columns:repeat(6,1fr)';
  statsGrid.append(
    _makeStatCell('Placed',   String(s.total_bets_placed),          null),
    _makeStatCell('Wins',     String(s.total_wins),                  'var(--success)'),
    _makeStatCell('Losses',   String(s.total_losses),                'var(--danger)'),
    _makeStatCell('Win Rate', s.win_rate_pct.toFixed(1) + '%',       null),
    _makeStatCell('Duration', durationStr,                           null),
    _makeStatCell('P&L',      pnlStr,                                pnlColor),
  );

  DOM.sessionBanner.append(navRow, statsGrid);

  // --- Per-whale table ---
  if (s.whales && s.whales.length > 0) {
    const section = document.createElement('div');
    section.style.cssText = 'padding:10px 16px 8px;border-top:1px solid var(--border)';

    const sectionHeader = document.createElement('div');
    sectionHeader.style.cssText = 'font-size:0.72rem;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.04em;margin-bottom:6px';
    sectionHeader.textContent = 'Per Whale';

    const wrapper = document.createElement('div');
    wrapper.style.overflowX = 'auto';

    const table = document.createElement('table');
    table.style.cssText = 'width:100%;border-collapse:collapse';

    const thead = document.createElement('thead');
    const hRow = document.createElement('tr');
    const cols = [
      { label: 'Whale',   align: 'left' },
      { label: 'Bets',    align: 'center' },
      { label: 'W',       align: 'center' },
      { label: 'L',       align: 'center' },
      { label: 'Win%',    align: 'center' },
      { label: 'Open',    align: 'center' },
      { label: 'P&L',     align: 'right' },
    ];
    cols.forEach(({ label, align }) => {
      const th = document.createElement('th');
      th.style.cssText = `padding:4px 10px;font-size:0.7rem;font-weight:600;color:var(--text-muted);text-transform:uppercase;text-align:${align}`;
      th.textContent = label;
      hRow.appendChild(th);
    });
    thead.appendChild(hRow);

    const tbody = document.createElement('tbody');
    s.whales.forEach(w => {
      const tr = document.createElement('tr');
      tr.style.cssText = 'border-top:1px solid var(--border);font-size:0.8rem';

      const wPnl = w.total_pnl_usdc || 0;
      const wPnlColor = wPnl > 0 ? 'var(--success)' : wPnl < 0 ? 'var(--danger)' : 'var(--text-muted)';
      const wPnlStr = (wPnl >= 0 ? '+$' : '-$') + Math.abs(wPnl).toFixed(2);
      const wr = w.win_rate_pct != null ? w.win_rate_pct.toFixed(1) + '%' : '\u2014';

      const tdDefs = [
        { text: w.whale_alias || w.whale_address.slice(0, 10), style: 'padding:6px 10px;font-family:monospace;font-size:0.75rem', title: w.whale_address },
        { text: String(w.followed),   style: 'padding:6px 10px;text-align:center' },
        { text: String(w.wins),       style: 'padding:6px 10px;text-align:center;color:var(--success)' },
        { text: String(w.losses),     style: 'padding:6px 10px;text-align:center;color:var(--danger)' },
        { text: wr,                   style: 'padding:6px 10px;text-align:center' },
        { text: String(w.open),       style: 'padding:6px 10px;text-align:center' },
        { text: wPnlStr,              style: `padding:6px 10px;text-align:right;font-weight:600;color:${wPnlColor}` },
      ];
      tdDefs.forEach(({ text, style, title }) => {
        const td = document.createElement('td');
        td.style.cssText = style;
        td.textContent = text;
        if (title) td.title = title;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

    table.append(thead, tbody);
    wrapper.appendChild(table);
    section.append(sectionHeader, wrapper);
    DOM.sessionBanner.appendChild(section);
  }

  DOM.sessionBanner.style.display = 'block';
}

function navigateSession(direction) {
  const list = State.sessionList;
  const idx = list.findIndex(x => x.id === State.viewedSessionId);
  if (idx === -1) return;
  const nextIdx = direction === 'older' ? idx + 1 : idx - 1;
  if (nextIdx < 0 || nextIdx >= list.length) return;
  loadSessionById(list[nextIdx].id);
}
