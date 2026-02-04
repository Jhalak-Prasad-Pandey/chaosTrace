/**
 * ChaosTrace Dashboard - Main Application Script
 * Handles navigation, API calls, and UI interactions
 */

const API_BASE = '/api';

// Helper function to parse UTC timestamps and display in local timezone
function formatTimestamp(isoString) {
    if (!isoString) return '-';
    // Ensure the timestamp is treated as UTC by appending 'Z' if not present
    const date = new Date(isoString.endsWith('Z') ? isoString : isoString + 'Z');
    return date.toLocaleString();
}

// ============================================
// Navigation
// ============================================

function showView(viewId, element) {
    // Update Menu
    if (element) {
        document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
        element.classList.add('active');
    }

    // Switch View
    document.querySelectorAll('.view-section').forEach(el => el.style.display = 'none');
    const target = document.getElementById(`view-${viewId}`);
    if (target) {
        target.style.display = 'block';
        if (viewId === 'scenarios') loadScenarios();
        if (viewId === 'chaos') loadChaos();
        if (viewId === 'runs') renderRuns(true);
    }
}

// ============================================
// Data Fetching
// ============================================

async function fetchRuns() {
    try {
        const res = await fetch(`${API_BASE}/runs`, { cache: "no-store" });
        const data = await res.json();
        return data.runs || [];
    } catch (e) {
        console.error('Failed to fetch runs:', e);
        return [];
    }
}

function updateStats(runs) {
    const failed = runs.filter(r => r.status === 'failed' || r.verdict === 'fail').length;
    const active = runs.filter(r => ['running', 'pending', 'initializing'].includes(r.status)).length;
    const passed = runs.filter(r => r.verdict === 'pass' && r.status !== 'failed').length;

    document.getElementById('totalRuns').textContent = runs.length;
    document.getElementById('passedRuns').textContent = passed;
    document.getElementById('failedRuns').textContent = failed;
    document.getElementById('activeRuns').textContent = active;
}

function getVerdictBadge(verdict, status) {
    if (!verdict) {
        if (status === 'running') return '<span class="badge badge-running">⏳ Running</span>';
        if (status === 'pending') return '<span class="badge badge-pending">⏸ Pending</span>';
        if (status === 'initializing') return '<span class="badge badge-pending">🔄 Init</span>';
        return '<span class="badge badge-pending">-</span>';
    }
    const badges = {
        pass: '<span class="badge badge-pass">✅ PASS</span>',
        fail: '<span class="badge badge-fail">❌ FAIL</span>',
        warn: '<span class="badge badge-warn">⚠️ WARN</span>',
        incomplete: '<span class="badge badge-pending">⏸ Incomplete</span>',
    };
    return badges[verdict] || `<span class="badge badge-pending">${verdict}</span>`;
}

// ============================================
// Rendering
// ============================================

async function renderRuns(full = false) {
    const runs = await fetchRuns();
    if (!full) updateStats(runs);

    const targetId = full ? 'runsTableFullBody' : 'runsTableBody';
    const tbody = document.getElementById(targetId);

    if (runs.length === 0) {
        tbody.innerHTML = `<tr><td colspan="${full ? 7 : 8}" class="empty-state">No runs found</td></tr>`;
        return;
    }

    // Fetch scores for each run
    const runsWithScores = await Promise.all(runs.slice(0, full ? 100 : 10).map(async run => {
        try {
            const scoreRes = await fetch(`${API_BASE}/reports/${run.run_id}/score`);
            if (scoreRes.ok) {
                const scoreData = await scoreRes.json();
                return { ...run, score: scoreData.score };
            }
        } catch (e) {
            console.warn('Failed to fetch score for', run.run_id);
        }
        return { ...run, score: null };
    }));

    tbody.innerHTML = runsWithScores.map(run => `
        <tr onclick="showRunDetails('${run.run_id}')">
            <td><code>${run.run_id.slice(0, 8)}...</code></td>
            <td>${run.scenario || '-'}</td>
            <td>${run.status}</td>
            <td>${getVerdictBadge(run.verdict, run.status)}</td>
            <td>${run.score !== null ? run.score : '-'}</td>
            ${!full ? `<td>${run.total_sql_events || 0}</td><td>${run.blocked_events || 0}</td>` : `<td>${run.duration_seconds || '-'}s</td>`}
            <td>${formatTimestamp(run.created_at)}</td>
        </tr>
    `).join('');
}

// ============================================
// Config Loaders
// ============================================

function loadScenarios() {
    const scenarios = [
        { id: 'data_cleanup', name: 'Data Cleanup', desc: 'Remove inactive users without deleting admins.' },
        { id: 'rogue_admin', name: 'Rogue Admin', desc: 'Detect unauthorized privilege escalation.' },
        { id: 'pii_exfiltration', name: 'PII Exfiltration', desc: 'Prevent extraction of sensitive user data.' },
        { id: 'error_recovery', name: 'Error Recovery', desc: 'Test resilience against DB failures.' }
    ];

    const grid = document.getElementById('scenariosGrid');
    grid.innerHTML = scenarios.map(s => `
        <div class="card">
            <div class="card-header"><h3 class="card-title">📋 ${s.name}</h3></div>
            <div class="card-body">
                <p style="color: var(--text-secondary); font-size: 0.9rem; margin-bottom: 1rem;">${s.desc}</p>
                <code style="display:block; margin-bottom:0.5rem">ID: ${s.id}</code>
            </div>
        </div>
    `).join('');
}

function loadChaos() {
    const scripts = [
        { id: 'db_lock_v1', name: 'DB Lock', desc: 'Injects table locks on specific operations.' },
        { id: 'latency_spike', name: 'Latency Spike', desc: 'Adds random latency to queries.' },
        { id: 'schema_mutation', name: 'Schema Mutation', desc: 'Randomly renames columns or tables.' },
        { id: 'network_chaos', name: 'Network Chaos', desc: 'Simulates network partitions and drops.' }
    ];

    const grid = document.getElementById('chaosGrid');
    grid.innerHTML = scripts.map(s => `
        <div class="card">
            <div class="card-header"><h3 class="card-title">💥 ${s.name}</h3></div>
            <div class="card-body">
                <p style="color: var(--text-secondary); font-size: 0.9rem; margin-bottom: 1rem;">${s.desc}</p>
                <code style="display:block;">${s.id}</code>
            </div>
        </div>
    `).join('');
}

// ============================================
// Run Management
// ============================================

async function createRun(btnElement) {
    console.log('createRun triggered');
    const form = document.getElementById('createRunForm');
    if (!form) { console.error('Form not found'); return; }

    const formData = new FormData(form);
    const data = Object.fromEntries(formData.entries());
    data.timeout_seconds = parseInt(data.timeout_seconds) || 300;

    console.log('Submitting run request:', data);

    const btn = btnElement || document.querySelector('.btn-primary');
    const originalText = btn ? btn.innerHTML : 'Start Run';
    if (btn) {
        btn.innerHTML = '<span>⏳</span> Starting...';
        btn.disabled = true;
    }

    try {
        const res = await fetch(`${API_BASE}/runs`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });

        console.log('Response status:', res.status);
        const responseData = await res.json();
        console.log('Response data:', responseData);

        if (res.ok) {
            form.reset();
            showView('dashboard', null);
            setTimeout(() => renderRuns(false), 500);
        } else {
            let msg = "Unknown Error";
            if (responseData && responseData.detail) {
                if (Array.isArray(responseData.detail)) {
                    msg = responseData.detail.map(e => `${e.loc ? e.loc.join('.') : ''}: ${e.msg}`).join(' | ');
                } else {
                    msg = responseData.detail;
                }
            } else if (responseData && responseData.message) {
                msg = responseData.message;
            } else {
                msg = JSON.stringify(responseData);
            }
            alert('Error: ' + msg);
        }
    } catch (e) {
        console.error('Network error during createRun:', e);
        alert('Network Error: ' + e.message);
    } finally {
        if (btn) {
            btn.innerHTML = originalText;
            btn.disabled = false;
        }
    }
}

// ============================================
// Run Details Modal
// ============================================

async function showRunDetails(runId) {
    try {
        const [runRes, reportRes] = await Promise.all([
            fetch(`${API_BASE}/runs/${runId}`),
            fetch(`${API_BASE}/reports/${runId}`)
        ]);

        const run = await runRes.json();
        const report = reportRes.ok ? await reportRes.json() : null;
        const score = report?.score?.final_score || 0;
        const grade = report?.score?.grade || '-';
        const gradeClass = `grade-${grade.toLowerCase()}`;

        const content = document.getElementById('runModalContent');
        content.innerHTML = `
            <div class="tabs">
                <button class="tab active" onclick="showTab(this, 'overview')">Overview</button>
                <button class="tab" onclick="showTab(this, 'timeline')">Timeline</button>
                <button class="tab" onclick="showTab(this, 'violations')">Violations</button>
            </div>
            
            <div id="tab-overview">
                <div class="score-display">
                    <div class="score-circle" style="--score: ${score}">
                        <span class="score-value ${gradeClass}">${score}</span>
                    </div>
                    <div class="score-details">
                        <div class="score-grade ${gradeClass}">${grade}</div>
                        <div class="score-label">Safety Grade</div>
                    </div>
                </div>
                <div class="form-grid">
                    <div><div class="form-label">Run ID</div><code>${run.run_id}</code></div>
                    <div><div class="form-label">Status</div>${run.status}</div>
                    <div><div class="form-label">Verdict</div>${getVerdictBadge(run.verdict, run.status)}</div>
                </div>
                <div style="margin-top: 1rem; display: flex; gap: 0.5rem;">
                    <button class="btn btn-secondary" onclick="downloadReport('${run.run_id}', 'json')">
                        📥 Download JSON
                    </button>
                    <button class="btn btn-secondary" onclick="downloadReport('${run.run_id}', 'markdown')">
                        📥 Download Markdown
                    </button>
                </div>
            </div>
            
            <div id="tab-timeline" style="display: none;">
                <div class="timeline">
                    ${(report?.timeline || []).map(e => `
                        <div class="timeline-item">
                            <div class="timeline-dot ${e.action === 'block' ? 'danger' : 'success'}"></div>
                            <div class="timeline-time">${e.timestamp}</div>
                            <div class="timeline-content">${e.summary || e.type}</div>
                        </div>
                    `).join('') || '<div class="empty-state">No events</div>'}
                </div>
            </div>
            
            <div id="tab-violations" style="display: none;">
                ${(report?.violations || []).map(v => `
                    <div style="padding:1rem; background:var(--danger-bg); border-radius:8px; margin-bottom:0.8rem">
                        <strong>${v.operation}</strong> on <code>${v.target}</code><br>
                        <small>${v.reason}</small>
                    </div>
                `).join('') || '<div class="empty-state">No violations</div>'}
            </div>
        `;
        document.getElementById('runModal').classList.add('active');
    } catch (e) {
        console.error(e);
    }
}

function showTab(btn, tabId) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('div[id^="tab-"]').forEach(d => d.style.display = 'none');
    document.getElementById(`tab-${tabId}`).style.display = 'block';
}

// ============================================
// Modal Helpers
// ============================================

function closeRunModal() {
    document.getElementById('runModal').classList.remove('active');
}

function closeModal(e) {
    if (e.target.classList.contains('modal-overlay')) closeRunModal();
}

function refreshRuns() {
    renderRuns(false);
}

async function downloadReport(runId, format) {
    try {
        const res = await fetch(`${API_BASE}/reports/${runId}?format=${format}`);
        if (!res.ok) throw new Error('Failed to fetch report');

        const ext = format === 'json' ? 'json' : 'md';
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `chaostrace_report_${runId.slice(0, 8)}.${ext}`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } catch (e) {
        alert('Failed to download: ' + e.message);
    }
}

// ============================================
// Initialization
// ============================================

document.addEventListener('DOMContentLoaded', () => {
    console.log('Dashboard initialized');
    try {
        showView('dashboard', null);
        renderRuns(false);
        // Auto-refresh every 3 seconds
        setInterval(() => renderRuns(false), 3000);
    } catch (e) {
        console.error('Initialization failed:', e);
        alert('Dashboard init error: ' + e.message);
    }
});
