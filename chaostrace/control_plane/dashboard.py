"""
Enhanced Dashboard HTML Template

Modern, feature-rich dashboard with:
- Real-time updates via polling/WebSocket
- Detailed run views
- Interactive timeline
- Score breakdown visualization
"""

ENHANCED_DASHBOARD_HTML = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ChaosTrace Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0a0f;
            --bg-secondary: #12121a;
            --bg-tertiary: #1a1a24;
            --bg-card: #16161f;
            --text-primary: #ffffff;
            --text-secondary: #8b8b9e;
            --text-muted: #5a5a6e;
            --accent: #6366f1;
            --accent-light: #818cf8;
            --accent-glow: rgba(99, 102, 241, 0.25);
            --success: #22c55e;
            --success-bg: rgba(34, 197, 94, 0.1);
            --warning: #f59e0b;
            --warning-bg: rgba(245, 158, 11, 0.1);
            --danger: #ef4444;
            --danger-bg: rgba(239, 68, 68, 0.1);
            --border: #2a2a3a;
            --border-light: #3a3a4a;
            --gradient-1: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
            --gradient-2: linear-gradient(135deg, #22c55e 0%, #16a34a 100%);
            --gradient-3: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
        }
        
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            line-height: 1.5;
        }
        
        /* Layout */
        .app-container {
            display: flex;
            min-height: 100vh;
        }
        
        .sidebar {
            width: 260px;
            background: var(--bg-secondary);
            border-right: 1px solid var(--border);
            padding: 1.5rem;
            display: flex;
            flex-direction: column;
        }
        
        .main-content {
            flex: 1;
            padding: 2rem;
            overflow-y: auto;
        }
        
        /* Sidebar */
        .logo {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            margin-bottom: 2rem;
        }
        
        .logo-icon {
            width: 42px;
            height: 42px;
            background: var(--gradient-1);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.5rem;
            box-shadow: 0 4px 20px var(--accent-glow);
        }
        
        .logo-text {
            font-size: 1.25rem;
            font-weight: 700;
            background: linear-gradient(135deg, #fff 0%, #a0a0b0 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .nav-section {
            margin-bottom: 1.5rem;
        }
        
        .nav-label {
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: var(--text-muted);
            margin-bottom: 0.75rem;
            padding-left: 0.75rem;
        }
        
        .nav-item {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            padding: 0.75rem 1rem;
            border-radius: 10px;
            color: var(--text-secondary);
            text-decoration: none;
            font-size: 0.9rem;
            font-weight: 500;
            transition: all 0.2s ease;
            cursor: pointer;
        }
        
        .nav-item:hover, .nav-item.active {
            background: rgba(99, 102, 241, 0.1);
            color: var(--accent-light);
        }
        
        .nav-item.active {
            background: rgba(99, 102, 241, 0.15);
        }
        
        .nav-icon { font-size: 1.1rem; }
        
        .nav-spacer { flex: 1; }
        
        /* Stats Cards */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.25rem;
            margin-bottom: 2rem;
        }
        
        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.25rem;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        
        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: var(--gradient-1);
            opacity: 0;
            transition: opacity 0.3s ease;
        }
        
        .stat-card:hover::before { opacity: 1; }
        
        .stat-card:hover {
            border-color: var(--accent);
            transform: translateY(-2px);
            box-shadow: 0 8px 32px var(--accent-glow);
        }
        
        .stat-icon {
            width: 44px;
            height: 44px;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.25rem;
            margin-bottom: 1rem;
        }
        
        .stat-icon.purple { background: rgba(99, 102, 241, 0.15); }
        .stat-icon.green { background: var(--success-bg); }
        .stat-icon.red { background: var(--danger-bg); }
        .stat-icon.yellow { background: var(--warning-bg); }
        
        .stat-value {
            font-size: 2rem;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }
        
        .stat-label {
            color: var(--text-secondary);
            font-size: 0.85rem;
        }
        
        /* Cards and Sections */
        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            margin-bottom: 1.5rem;
        }
        
        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1.25rem 1.5rem;
            border-bottom: 1px solid var(--border);
        }
        
        .card-title {
            font-size: 1rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .card-body { padding: 1.5rem; }
        
        /* Buttons */
        .btn {
            padding: 0.625rem 1.25rem;
            border-radius: 10px;
            border: none;
            cursor: pointer;
            font-weight: 600;
            font-size: 0.875rem;
            transition: all 0.2s ease;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .btn-primary {
            background: var(--gradient-1);
            color: white;
        }
        
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 24px var(--accent-glow);
        }
        
        .btn-secondary {
            background: var(--bg-tertiary);
            color: var(--text-primary);
            border: 1px solid var(--border);
        }
        
        .btn-secondary:hover {
            border-color: var(--accent);
            background: rgba(99, 102, 241, 0.1);
        }
        
        .btn-sm {
            padding: 0.375rem 0.75rem;
            font-size: 0.8rem;
        }
        
        /* Forms */
        .form-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1rem;
        }
        
        .form-group { margin-bottom: 0.5rem; }
        
        .form-label {
            display: block;
            font-size: 0.8rem;
            font-weight: 500;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }
        
        .form-input, .form-select {
            width: 100%;
            padding: 0.75rem 1rem;
            border-radius: 10px;
            border: 1px solid var(--border);
            background: var(--bg-tertiary);
            color: var(--text-primary);
            font-size: 0.9rem;
            transition: all 0.2s ease;
        }
        
        .form-input:focus, .form-select:focus {
            outline: none;
            border-color: var(--accent);
            box-shadow: 0 0 0 3px var(--accent-glow);
        }
        
        /* Table */
        .table-container { overflow-x: auto; }
        
        .data-table {
            width: 100%;
            border-collapse: collapse;
        }
        
        .data-table th {
            text-align: left;
            padding: 1rem 1.25rem;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            border-bottom: 1px solid var(--border);
        }
        
        .data-table td {
            padding: 1rem 1.25rem;
            font-size: 0.9rem;
            border-bottom: 1px solid var(--border);
        }
        
        .data-table tbody tr {
            transition: background 0.2s ease;
            cursor: pointer;
        }
        
        .data-table tbody tr:hover {
            background: rgba(99, 102, 241, 0.05);
        }
        
        /* Badges */
        .badge {
            display: inline-flex;
            align-items: center;
            padding: 0.25rem 0.75rem;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        
        .badge-pass { background: var(--success-bg); color: var(--success); }
        .badge-fail { background: var(--danger-bg); color: var(--danger); }
        .badge-warn { background: var(--warning-bg); color: var(--warning); }
        .badge-pending { background: rgba(99, 102, 241, 0.1); color: var(--accent); }
        .badge-running { 
            background: rgba(99, 102, 241, 0.1); 
            color: var(--accent);
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        
        /* Score Display */
        .score-display {
            display: flex;
            align-items: center;
            gap: 2rem;
            padding: 1.5rem;
            background: var(--bg-tertiary);
            border-radius: 12px;
            margin-bottom: 1.5rem;
        }
        
        .score-circle {
            width: 120px;
            height: 120px;
            border-radius: 50%;
            background: conic-gradient(var(--accent) calc(var(--score) * 3.6deg), var(--bg-primary) 0);
            display: flex;
            align-items: center;
            justify-content: center;
            position: relative;
        }
        
        .score-circle::before {
            content: '';
            position: absolute;
            width: 100px;
            height: 100px;
            background: var(--bg-tertiary);
            border-radius: 50%;
        }
        
        .score-value {
            position: relative;
            z-index: 1;
            font-size: 2rem;
            font-weight: 700;
        }
        
        .grade-a { color: var(--success); }
        .grade-b { color: #84cc16; }
        .grade-c { color: var(--warning); }
        .grade-d { color: #fb923c; }
        .grade-f { color: var(--danger); }
        
        .score-details { flex: 1; }
        
        .score-grade {
            font-size: 3rem;
            font-weight: 700;
            line-height: 1;
            margin-bottom: 0.5rem;
        }
        
        .score-label {
            color: var(--text-secondary);
            font-size: 0.9rem;
        }
        
        /* Timeline */
        .timeline {
            position: relative;
            padding-left: 2rem;
        }
        
        .timeline::before {
            content: '';
            position: absolute;
            left: 0.5rem;
            top: 0;
            bottom: 0;
            width: 2px;
            background: var(--border);
        }
        
        .timeline-item {
            position: relative;
            padding-bottom: 1.5rem;
        }
        
        .timeline-dot {
            position: absolute;
            left: -1.5rem;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: var(--accent);
            border: 3px solid var(--bg-card);
        }
        
        .timeline-dot.success { background: var(--success); }
        .timeline-dot.danger { background: var(--danger); }
        .timeline-dot.warning { background: var(--warning); }
        
        .timeline-time {
            font-size: 0.75rem;
            color: var(--text-muted);
            margin-bottom: 0.25rem;
        }
        
        .timeline-content {
            background: var(--bg-tertiary);
            padding: 0.75rem 1rem;
            border-radius: 8px;
            font-size: 0.9rem;
        }
        
        /* Modal */
        .modal-overlay {
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.7);
            backdrop-filter: blur(4px);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 1000;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s ease;
        }
        
        .modal-overlay.active {
            opacity: 1;
            visibility: visible;
        }
        
        .modal {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 20px;
            width: 90%;
            max-width: 800px;
            max-height: 85vh;
            overflow: hidden;
            transform: scale(0.95);
            transition: transform 0.3s ease;
        }
        
        .modal-overlay.active .modal {
            transform: scale(1);
        }
        
        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1.5rem;
            border-bottom: 1px solid var(--border);
        }
        
        .modal-title { font-size: 1.25rem; font-weight: 600; }
        
        .modal-close {
            width: 36px;
            height: 36px;
            border-radius: 10px;
            border: none;
            background: var(--bg-tertiary);
            color: var(--text-primary);
            cursor: pointer;
            font-size: 1.25rem;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s ease;
        }
        
        .modal-close:hover {
            background: var(--danger-bg);
            color: var(--danger);
        }
        
        .modal-body {
            padding: 1.5rem;
            overflow-y: auto;
            max-height: calc(85vh - 80px);
        }
        
        /* Tabs */
        .tabs {
            display: flex;
            gap: 0.5rem;
            border-bottom: 1px solid var(--border);
            margin-bottom: 1.5rem;
        }
        
        .tab {
            padding: 0.75rem 1.25rem;
            border: none;
            background: none;
            color: var(--text-secondary);
            font-size: 0.9rem;
            font-weight: 500;
            cursor: pointer;
            position: relative;
            transition: color 0.2s ease;
        }
        
        .tab:hover { color: var(--text-primary); }
        
        .tab.active {
            color: var(--accent);
        }
        
        .tab.active::after {
            content: '';
            position: absolute;
            bottom: -1px;
            left: 0;
            right: 0;
            height: 2px;
            background: var(--accent);
        }
        
        /* Empty State */
        .empty-state {
            text-align: center;
            padding: 4rem 2rem;
            color: var(--text-secondary);
        }
        
        .empty-icon { font-size: 4rem; margin-bottom: 1rem; }
        .empty-title { font-size: 1.25rem; font-weight: 600; margin-bottom: 0.5rem; color: var(--text-primary); }
        .empty-text { font-size: 0.9rem; max-width: 400px; margin: 0 auto; }
        
        /* Code */
        code {
            font-family: 'JetBrains Mono', 'Fira Code', monospace;
            background: var(--bg-tertiary);
            padding: 0.125rem 0.375rem;
            border-radius: 4px;
            font-size: 0.85em;
        }
        
        /* Responsive */
        @media (max-width: 768px) {
            .sidebar { display: none; }
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
        }
    </style>
</head>
<body>
    <div class="app-container">
        <!-- Sidebar -->
        <aside class="sidebar">
            <div class="logo">
                <div class="logo-icon">🌀</div>
                <span class="logo-text">ChaosTrace</span>
            </div>
            
            <nav class="nav-section">
                <div class="nav-label">Main</div>
                <a class="nav-item active" onclick="showView('dashboard', this)">
                    <span class="nav-icon">📊</span> Dashboard
                </a>
                <a class="nav-item" onclick="showView('runs', this)">
                    <span class="nav-icon">🧪</span> Test Runs
                </a>
                <a class="nav-item" onclick="showView('scenarios', this)">
                    <span class="nav-icon">📋</span> Scenarios
                </a>
            </nav>
            
            <nav class="nav-section">
                <div class="nav-label">Configuration</div>
                <a class="nav-item" onclick="showView('policies', this)">
                    <span class="nav-icon">🛡️</span> Policies
                </a>
                <a class="nav-item" onclick="showView('chaos', this)">
                    <span class="nav-icon">💥</span> Chaos Scripts
                </a>
            </nav>
            
            <div class="nav-spacer"></div>
            
            <nav class="nav-section">
                <a class="nav-item" href="/docs" target="_blank">
                    <span class="nav-icon">📚</span> API Docs
                </a>
            </nav>
        </aside>
        
        <!-- Main Content -->
        <main class="main-content">
            <!-- Dashboard View -->
            <div id="view-dashboard" class="view-section">
                <h1 style="font-size: 1.5rem; margin-bottom: 1.5rem;">Dashboard</h1>
                
                <!-- Stats -->
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-icon purple">📊</div>
                        <div class="stat-value" id="totalRuns">0</div>
                        <div class="stat-label">Total Runs</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon green">✅</div>
                        <div class="stat-value" id="passedRuns" style="color: var(--success)">0</div>
                        <div class="stat-label">Passed</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon red">❌</div>
                        <div class="stat-value" id="failedRuns" style="color: var(--danger)">0</div>
                        <div class="stat-label">Failed</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon yellow">⏳</div>
                        <div class="stat-value" id="activeRuns" style="color: var(--warning)">0</div>
                        <div class="stat-label">Active</div>
                    </div>
                </div>
                
                <!-- Create Run Card -->
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">🚀 Create New Run</h2>
                    </div>
                    <div class="card-body">
                        <form id="createRunForm" class="form-grid">
                            <div class="form-group">
                                <label class="form-label">Agent Type</label>
                                <select name="agent_type" class="form-select" required>
                                    <option value="python">Python Script</option>
                                    <option value="openai">OpenAI Agent</option>
                                    <option value="langchain">LangChain Agent</option>
                                    <option value="custom">Custom Agent</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Agent Entry Point</label>
                                <input type="text" name="agent_entry" class="form-input" placeholder="examples/cleanup_agent.py" required>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Scenario</label>
                                <select name="scenario" class="form-select" required>
                                    <option value="data_cleanup">Data Cleanup</option>
                                    <option value="rogue_admin">Rogue Admin</option>
                                    <option value="data_migration">Data Migration</option>
                                    <option value="pii_exfiltration">PII Exfiltration</option>
                                    <option value="privilege_escalation">Privilege Escalation</option>
                                    <option value="error_recovery">Error Recovery</option>
                                    <option value="bulk_operations">Bulk Operations</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Policy Profile</label>
                                <select name="policy_profile" class="form-select">
                                    <option value="strict">Strict</option>
                                    <option value="permissive">Permissive</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Chaos Script</label>
                                <select name="chaos_profile" class="form-select">
                                    <option value="">None</option>
                                    <option value="db_lock_v1">DB Lock</option>
                                    <option value="latency_spike">Latency Spike</option>
                                    <option value="schema_mutation">Schema Mutation</option>
                                    <option value="network_chaos">Network Chaos</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Timeout (seconds)</label>
                                <input type="number" name="timeout_seconds" class="form-input" value="300" min="30" max="3600">
                            </div>
                        </form>
                        <div style="margin-top: 1rem;">
                            <button class="btn btn-primary" onclick="createRun(this)">
                                <span>▶</span> Start Run
                            </button>
                        </div>
                    </div>
                </div>
                
                <!-- Recent Runs -->
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">📋 Recent Runs</h2>
                        <button class="btn btn-secondary btn-sm" onclick="refreshRuns()">
                            <span>🔄</span> Refresh
                        </button>
                    </div>
                    <div class="card-body" style="padding: 0;">
                        <div class="table-container">
                            <table class="data-table">
                                <thead>
                                    <tr>
                                        <th>Run ID</th>
                                        <th>Scenario</th>
                                        <th>Status</th>
                                        <th>Verdict</th>
                                        <th>Score</th>
                                        <th>SQL Events</th>
                                        <th>Blocked</th>
                                        <th>Created</th>
                                    </tr>
                                </thead>
                                <tbody id="runsTableBody">
                                    <tr>
                                        <td colspan="8">
                                            <div class="empty-state">
                                                <div class="empty-icon">🧪</div>
                                                <div class="empty-title">No test runs yet</div>
                                                <div class="empty-text">Create your first run to start testing your AI agent</div>
                                            </div>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Runs View -->
            <div id="view-runs" class="view-section" style="display: none;">
                <h1 style="font-size: 1.5rem; margin-bottom: 1.5rem;">Test Runs</h1>
                <div class="card">
                    <div class="card-body" style="padding: 0;">
                        <div class="table-container">
                            <table class="data-table">
                                <thead>
                                    <tr>
                                        <th>Run ID</th>
                                        <th>Scenario</th>
                                        <th>Status</th>
                                        <th>Verdict</th>
                                        <th>Score</th>
                                        <th>Duration</th>
                                        <th>Created</th>
                                    </tr>
                                </thead>
                                <tbody id="runsTableFullBody"></tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Scenarios View -->
            <div id="view-scenarios" class="view-section" style="display: none;">
                <h1 style="font-size: 1.5rem; margin-bottom: 1.5rem;">Scenarios</h1>
                <div class="form-grid" id="scenariosGrid">
                    <!-- Populated by JS -->
                </div>
            </div>

            <!-- Policies View -->
            <div id="view-policies" class="view-section" style="display: none;">
                <h1 style="font-size: 1.5rem; margin-bottom: 1.5rem;">Policies</h1>
                <div class="form-grid" id="policiesGrid">
                    <div class="card">
                        <div class="card-header"><h3 class="card-title">🛡️ Strict</h3></div>
                        <div class="card-body"><pre><code>Blocks DDL, Grants, and Unsafe DML.</code></pre></div>
                    </div>
                    <div class="card">
                        <div class="card-header"><h3 class="card-title">⚠️ Permissive</h3></div>
                        <div class="card-body"><pre><code>Allows most operations, logs warnings.</code></pre></div>
                    </div>
                </div>
            </div>

            <!-- Chaos View -->
            <div id="view-chaos" class="view-section" style="display: none;">
                <h1 style="font-size: 1.5rem; margin-bottom: 1.5rem;">Chaos Scripts</h1>
                <div class="form-grid" id="chaosGrid">
                    <!-- Populated by JS -->
                </div>
            </div>

        </main>
    </div>
    
    <!-- Run Details Modal -->
    <div id="runModal" class="modal-overlay" onclick="closeModal(event)">
        <div class="modal" onclick="event.stopPropagation()">
            <div class="modal-header">
                <h2 class="modal-title">Run Details</h2>
                <button class="modal-close" onclick="closeRunModal()">×</button>
            </div>
            <div class="modal-body" id="runModalContent">
                <!-- Content loaded dynamically -->
            </div>
        </div>
    </div>
    
    <script>
        const API_BASE = '/api';
        
        // Helper function to parse UTC timestamps and display in local timezone
        function formatTimestamp(isoString) {
            if (!isoString) return '-';
            // Ensure the timestamp is treated as UTC by appending 'Z' if not present
            const date = new Date(isoString.endsWith('Z') ? isoString : isoString + 'Z');
            return date.toLocaleString();
        }
        
        // Navigation Logic
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
                if (viewId === 'runs') renderRuns(true); // Render full table
            }
        }

        // Data Fetching
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
        
        async function renderRuns(full = false) {
            const runs = await fetchRuns();
            if (!full) updateStats(runs);
            
            const targetId = full ? 'runsTableFullBody' : 'runsTableBody';
            const tbody = document.getElementById(targetId);
            
            if (runs.length === 0) {
                tbody.innerHTML = `<tr><td colspan="${full ? 7 : 8}" class="empty-state">No runs found</td></tr>`;
                return;
            }
            
            tbody.innerHTML = runs.slice(0, full ? 100 : 10).map(run => `
                <tr onclick="showRunDetails('${run.run_id}')">
                    <td><code>${run.run_id.slice(0, 8)}...</code></td>
                    <td>${run.scenario || '-'}</td>
                    <td>${run.status}</td>
                    <td>${getVerdictBadge(run.verdict, run.status)}</td>
                    <td>${run.score || '-'}</td>
                    ${!full ? `<td>${run.total_sql_events || 0}</td><td>${run.blocked_events || 0}</td>` : `<td>${run.duration_seconds || '-'}s</td>`}
                    <td>${formatTimestamp(run.created_at)}</td>
                </tr>
            `).join('');
        }
        
        // Mock Data Loaders for Config Views (Would be API calls in full version)
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

        function closeRunModal() { document.getElementById('runModal').classList.remove('active'); }
        function closeModal(e) { if(e.target.classList.contains('modal-overlay')) closeRunModal(); }
        function refreshRuns() { renderRuns(false); }
        
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

        // Initial Load
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
    </script>
</body>
</html>
'''

