/* ===== NBParlay – PrizePicks-style UI ===== */

let allPlayers = [];
let todaysPlayers = [];
let currentFilter = 'all';
let selectedDates = [];
let availableDates = [];
let countdownInterval = null;

/* ---------- Bootstrap ---------- */
setTimeout(() => {
    if (allPlayers.length === 0 && todaysPlayers.length === 0) {
        console.warn('Safety: players not loaded – forcing load');
        loadPlayerData();
    }
}, 2500);

document.addEventListener('DOMContentLoaded', () => {
    loadPlayerData();

    // Filter pills
    document.querySelectorAll('.pill[data-filter]').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.pill[data-filter]').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentFilter = btn.dataset.filter;
            renderCards();
        });
    });

    // Search
    const search = document.getElementById('playerSearch');
    if (search) search.addEventListener('input', () => renderCards());

    // Modal backdrop close
    const overlay = document.getElementById('modalOverlay');
    if (overlay) overlay.addEventListener('click', e => { if (e.target === overlay) closeModal(); });

    // ESC to close
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

    // Auto-refresh every 30s
    setInterval(() => loadPlayerData(), 30000);

    // Start countdown ticker
    countdownInterval = setInterval(updateCountdowns, 1000);
});

/* ---------- Data Loading ---------- */
async function loadPlayerData() {
    try {
        const [projRes, todayRes] = await Promise.all([
            fetch('/api/player-projections').then(r => r.json()),
            fetch('/api/todays-players').then(r => r.json())
        ]);

        allPlayers = projRes.projections || projRes.players || [];
        todaysPlayers = todayRes.players || [];

        // Enrich allPlayers with today-status
        const todayIds = new Set(todaysPlayers.map(p => p.name));
        allPlayers.forEach(p => { p._playsToday = todayIds.has(p.name); });

        // Build available dates
        const dateSet = new Set();
        allPlayers.forEach(p => {
            if (p.game_time) {
                const d = p.game_time.split('T')[0];
                if (d) dateSet.add(d);
            }
        });
        availableDates = [...dateSet].sort();
        selectedDates = [...availableDates]; // default: all selected

        renderCards();
        updateFooter();
    } catch (err) {
        console.error('Data load error:', err);
        const grid = document.getElementById('gamesContainer');
        if (grid) grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠️</div><h2>Failed to load data</h2><p>' + escapeHtml(err.message) + '</p></div>';
    }
}

/* ---------- Render ---------- */
function renderCards() {
    const grid = document.getElementById('gamesContainer');
    if (!grid) return;

    const q = (document.getElementById('playerSearch')?.value || '').toLowerCase().trim();

    let list = allPlayers.filter(p => {
        // text search
        if (q) {
            const hay = (p.name + ' ' + (p.team || '') + ' ' + (p.position || '')).toLowerCase();
            if (!hay.includes(q)) return false;
        }
        // filter
        if (currentFilter === 'upcoming') return p._playsToday;
        if (currentFilter === 'live') return p._playsToday;
        if (currentFilter === 'injured') return p.injury_status && p.injury_status !== 'healthy' && p.injury_status !== '';
        return true;
    });

    if (list.length === 0) {
        grid.innerHTML = '<div class="empty-state"><div class="empty-icon">🔍</div><h2>No players found</h2><p>Try a different filter or search</p></div>';
        return;
    }

    grid.innerHTML = list.map(p => createPlayerCard(p)).join('');
}

/* ---------- Card Builder ---------- */
function createPlayerCard(p) {
    const initials = (p.name || '?').split(' ').map(w => w[0]).join('').slice(0, 2);

    // Find top-3 stats for compact card (PTS, REB, AST preferred)
    const projections = p.projections || [];
    const statOrder = ['PTS', 'REB', 'AST', 'PRA', '3PM', 'STL'];
    const sorted = [...projections].sort((a, b) => statOrder.indexOf(a.stat_type) - statOrder.indexOf(b.stat_type));
    const top3 = sorted.slice(0, 3);

    // Injury badge
    let injuryBadge = '';
    if (p.injury_status && p.injury_status !== 'healthy' && p.injury_status !== '') {
        const cls = (p.injury_status || '').toLowerCase().replace(/\s+/g, '-');
        injuryBadge = '<span class="injury-badge ' + cls + '">' + escapeHtml(p.injury_status) + '</span>';
    }

    // Game/matchup
    const matchup = p.game ? escapeHtml(p.game) : (p.opponent ? escapeHtml((p.team || '') + ' vs ' + p.opponent) : '');
    const gameTime = p.game_datetime || p.game_time || '';

    const injuredClass = (p.injury_status && p.injury_status !== 'healthy' && p.injury_status !== '') ? ' injured' : '';

    const idx = allPlayers.indexOf(p);

    let html = '<div class="player-card' + injuredClass + '" onclick="openModal(' + idx + ')">';
    html += '<div class="card-top">';
    html += '  <div class="player-avatar">' + escapeHtml(initials) + '</div>';
    html += '  <div class="player-info">';
    html += '    <div class="player-name">' + escapeHtml(p.name || 'Unknown') + '</div>';
    html += '    <div class="player-meta">';
    if (p.position) html += '<span class="pos-badge">' + escapeHtml(p.position) + '</span>';
    html += escapeHtml(p.team || '');
    if (p.number) html += ' #' + escapeHtml(String(p.number));
    html += '</div></div>';
    if (injuryBadge) html += injuryBadge;
    html += '</div>';

    // Game row
    html += '<div class="card-game">';
    html += '  <span class="matchup">' + matchup + '</span>';
    html += '  <span class="game-countdown" data-datetime="' + escapeHtml(gameTime) + '">' + formatCountdown(gameTime) + '</span>';
    html += '</div>';

    // Stats row
    html += '<div class="card-stats">';
    top3.forEach(s => {
        const conf = s.confidence || 0;
        const cc = confClass(conf);
        html += '<div class="mini-stat">';
        html += '  <div class="mini-stat-value">' + (s.projected_value != null ? Number(s.projected_value).toFixed(1) : '-') + '</div>';
        html += '  <div class="mini-stat-label">' + escapeHtml(s.stat_type || '?') + '</div>';
        html += '  <div class="mini-stat-conf ' + cc + '">' + Number(conf).toFixed(0) + '/10</div>';
        html += '</div>';
    });
    // If fewer than 3, fill gaps
    for (let i = top3.length; i < 3; i++) {
        html += '<div class="mini-stat"><div class="mini-stat-value">-</div><div class="mini-stat-label">-</div><div class="mini-stat-conf">-</div></div>';
    }
    html += '</div></div>';
    return html;
}

/* ---------- Modal ---------- */
function openModal(idx) {
    const p = allPlayers[idx];
    if (!p) return;

    const overlay = document.getElementById('modalOverlay');
    const card = document.getElementById('modalCard');
    if (!overlay || !card) return;

    const initials = (p.name || '?').split(' ').map(w => w[0]).join('').slice(0, 2);
    const matchup = p.game ? escapeHtml(p.game) : (p.opponent ? escapeHtml((p.team || '') + ' vs ' + p.opponent) : 'TBD');
    const gameTime = p.game_datetime || p.game_time || '';

    let injuryBadge = '';
    if (p.injury_status && p.injury_status !== 'healthy' && p.injury_status !== '') {
        const cls = (p.injury_status || '').toLowerCase().replace(/\s+/g, '-');
        injuryBadge = '<span class="injury-badge ' + cls + '">' + escapeHtml(p.injury_status) + '</span>';
    }

    let injuryDetailHTML = '';
    if (p.injury_detail) {
        injuryDetailHTML = '<div class="modal-injury-row">' + injuryBadge +
            '<div class="modal-injury-detail">' + escapeHtml(p.injury_detail) + '</div></div>';
    }

    // All stat rows
    const projections = p.projections || [];
    const statOrder = ['PTS', 'REB', 'AST', 'PRA', '3PM', 'STL'];
    const sorted = [...projections].sort((a, b) => statOrder.indexOf(a.stat_type) - statOrder.indexOf(b.stat_type));

    let statsHTML = '';
    sorted.forEach(s => {
        const conf = s.confidence || 0;
        const cc = confClass(conf);
        const pct = Math.min(conf * 10, 100);
        const fillClass = conf >= 7 ? 'high' : conf >= 4 ? 'med' : 'low';
        statsHTML += '<div class="stat-row">';
        statsHTML += '  <div class="stat-name">' + escapeHtml(s.stat_type || '') + '</div>';
        statsHTML += '  <div class="stat-value">' + (s.projected_value != null ? Number(s.projected_value).toFixed(1) : '-') + '</div>';
        statsHTML += '  <div class="stat-conf-wrap">';
        statsHTML += '    <div class="conf-bar"><div class="conf-bar-fill ' + fillClass + '" style="width:' + pct + '%"></div></div>';
        statsHTML += '    <span class="conf-num ' + cc + '">' + Number(conf).toFixed(0) + '</span>';
        statsHTML += '  </div>';
        statsHTML += '</div>';
    });

    let html = '<div class="modal-header">';
    html += '  <div class="player-name">' + escapeHtml(p.name || 'Unknown') + '</div>';
    html += '  <div class="player-meta">';
    if (p.position) html += '<span class="pos-badge">' + escapeHtml(p.position) + '</span>';
    html += escapeHtml(p.team || '');
    if (p.number) html += ' #' + escapeHtml(String(p.number));
    if (p.height) html += ' · ' + escapeHtml(p.height);
    html += '</div>';
    html += '  <div class="modal-game-row">';
    html += '    <span class="modal-matchup">' + matchup + '</span>';
    html += '    <span class="modal-countdown" data-datetime="' + escapeHtml(gameTime) + '">' + formatCountdown(gameTime) + '</span>';
    html += '  </div>';
    if (injuryDetailHTML) html += injuryDetailHTML;
    html += '</div>';
    html += '<div class="modal-stats">' + statsHTML + '</div>';
    html += '<div class="modal-close-area"><button class="modal-close-btn" onclick="closeModal()">Close</button></div>';

    card.innerHTML = html;
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';
}

function closeModal() {
    const overlay = document.getElementById('modalOverlay');
    if (overlay) overlay.classList.remove('open');
    document.body.style.overflow = '';
}

/* ---------- Countdown ---------- */
function formatCountdown(dtStr) {
    if (!dtStr) return 'TBD';
    const target = new Date(dtStr);
    if (isNaN(target)) return 'TBD';
    const now = new Date();
    const diff = target - now;
    if (diff <= 0) return 'LIVE';

    const d = Math.floor(diff / 86400000);
    const h = Math.floor((diff % 86400000) / 3600000);
    const m = Math.floor((diff % 3600000) / 60000);
    const s = Math.floor((diff % 60000) / 1000);

    if (d > 0) return d + 'd ' + h + 'h';
    if (h > 0) return h + 'h ' + m + 'm';
    return m + 'm ' + s + 's';
}

function updateCountdowns() {
    document.querySelectorAll('.game-countdown, .modal-countdown').forEach(el => {
        const dt = el.getAttribute('data-datetime');
        if (!dt) return;
        const text = formatCountdown(dt);
        el.textContent = text;
        if (text === 'LIVE') {
            el.classList.add('live-now');
        } else {
            el.classList.remove('live-now');
        }
    });
}

/* ---------- Helpers ---------- */
function confClass(val) {
    if (val >= 7) return 'conf-high';
    if (val >= 4) return 'conf-med';
    return 'conf-low';
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.appendChild(document.createTextNode(String(str)));
    return div.innerHTML;
}

function updateFooter() {
    const timeEl = document.getElementById('lastUpdate');
    const countEl = document.getElementById('playerCount');
    if (timeEl) timeEl.textContent = new Date().toLocaleTimeString();
    if (countEl) countEl.textContent = allPlayers.length + ' players';
}
