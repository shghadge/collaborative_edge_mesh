/* ═══════════════════════════════════════════════════════════
   Edge Mesh Observatory — Dashboard JS
   ═══════════════════════════════════════════════════════════ */

// --- State ---
let nodes = [];
let liveMetrics = {};
let eventLog = [];
let charts = {};
let pollInterval = null;
const POLL_MS = 3000;

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    refreshAll();
    pollInterval = setInterval(pollData, POLL_MS);
});

// --- API Helpers ---
async function api(method, path, body = null) {
    const opts = {
        method,
        headers: { 'Content-Type': 'application/json' },
    };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(path, opts);
    const data = await res.json();
    if (!res.ok) {
        const msg = data.message || data.detail?.message || JSON.stringify(data);
        throw new Error(msg);
    }
    return data;
}

function toast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => {
        el.classList.add('out');
        setTimeout(() => el.remove(), 300);
    }, 3500);
}

// --- Refresh ---
async function refreshAll() {
    await Promise.allSettled([
        fetchNodes(),
        fetchLiveMetrics(),
        fetchTimeseries(),
    ]);
}

async function pollData() {
    await Promise.allSettled([
        fetchNodes(),
        fetchLiveMetrics(),
    ]);
}

// --- Nodes ---
async function fetchNodes() {
    try {
        nodes = await api('GET', '/nodes');
        renderNodes();
        renderTopology();
        updateNodeSelector();
        document.getElementById('stat-node-count').textContent = nodes.length;
    } catch (e) {
        // Docker may not be ready
    }
}

function renderNodes() {
    const container = document.getElementById('node-list');
    if (!nodes.length) {
        container.innerHTML = '<div class="empty-state">No nodes running</div>';
        return;
    }

    container.innerHTML = nodes.map(n => {
        const statusClass = n.isolated ? 'isolated' : (n.status === 'running' ? 'running' : 'stopped');
        const statusLabel = n.isolated ? 'isolated' : n.status;
        return `
            <div class="node-item">
                <span class="node-dot ${statusClass}"></span>
                <span class="node-name">${n.name}</span>
                <span class="node-id">${n.node_id} · ${statusLabel}</span>
                <div class="node-actions">
                    ${n.isolated
                ? `<button class="btn btn-sm btn-success" onclick="healNode('${n.node_id}')">Heal</button>`
                : `<button class="btn btn-sm btn-warning" onclick="isolateNode('${n.node_id}')">Isolate</button>`
            }
                    <button class="btn btn-sm" onclick="removeNode('${n.node_id}')">✕</button>
                </div>
            </div>
        `;
    }).join('');
}

function updateNodeSelector() {
    const select = document.getElementById('event-target');
    const current = select.value;
    select.innerHTML = '<option value="">Any node</option>' +
        nodes.map(n => `<option value="${n.node_id}" ${n.node_id === current ? 'selected' : ''}>${n.name} (${n.node_id})</option>`).join('');
}

// --- Node Actions ---
async function createNode() {
    try {
        const result = await api('POST', '/nodes');
        toast(`Created ${result.container || result.node_id}`, 'success');
        await fetchNodes();
    } catch (e) {
        toast(`Create failed: ${e.message}`, 'error');
    }
}

async function createNodesBatch(count) {
    try {
        const result = await api('POST', `/nodes/batch?count=${count}`);
        toast(`Created ${result.created_count} nodes`, 'success');
        await fetchNodes();
    } catch (e) {
        toast(`Batch create failed: ${e.message}`, 'error');
    }
}

async function removeNode(nodeId) {
    try {
        await api('DELETE', `/nodes/${nodeId}`);
        toast(`Removed ${nodeId}`, 'info');
        await fetchNodes();
    } catch (e) {
        toast(`Remove failed: ${e.message}`, 'error');
    }
}

async function isolateNode(nodeId) {
    try {
        await api('POST', `/nodes/${nodeId}/partition`);
        toast(`Isolated ${nodeId}`, 'info');
        await fetchNodes();
    } catch (e) {
        toast(`Isolate failed: ${e.message}`, 'error');
    }
}

async function healNode(nodeId) {
    try {
        await api('DELETE', `/nodes/${nodeId}/partition`);
        toast(`Healed ${nodeId}`, 'success');
        await fetchNodes();
    } catch (e) {
        toast(`Heal failed: ${e.message}`, 'error');
    }
}

async function splitBrain() {
    try {
        const result = await api('POST', '/partition/split-brain');
        toast('Split brain created', 'info');
        await fetchNodes();
    } catch (e) {
        toast(`Split brain failed: ${e.message}`, 'error');
    }
}

async function healAll() {
    try {
        await api('POST', '/partition/heal-all');
        toast('All nodes healed', 'success');
        await fetchNodes();
    } catch (e) {
        toast(`Heal all failed: ${e.message}`, 'error');
    }
}

async function triggerPoll() {
    try {
        await api('POST', '/gateway/poll');
        toast('Poll triggered', 'info');
        await fetchLiveMetrics();
    } catch (e) {
        toast(`Poll failed: ${e.message}`, 'error');
    }
}

// --- Event Sending ---
async function sendEvent(e) {
    e.preventDefault();
    const btn = document.getElementById('send-event-btn');
    btn.disabled = true;

    const type = document.getElementById('event-type').value;
    const value = document.getElementById('event-value').value;
    const category = document.getElementById('event-category').value;
    const targetNode = document.getElementById('event-target').value;
    const location = document.getElementById('event-location').value;
    const operation = document.getElementById('event-operation').value;

    // Parse value (number or string)
    let parsedValue = value;
    if (!isNaN(value) && value.trim() !== '') {
        parsedValue = parseFloat(value);
    }

    try {
        const url = targetNode
            ? `/event/send?target_node=${encodeURIComponent(targetNode)}`
            : '/event/send';

        const result = await api('POST', url, {
            type,
            value: parsedValue,
            category,
            location: location || undefined,
            operation: operation || undefined,
        });

        // Add to local event log
        eventLog.unshift({
            timestamp: new Date().toISOString(),
            node: result.target_node || result.node_id,
            type,
            category,
            state_size_bytes: result.state_size_bytes,
            merkle_root: result.merkle_root_after,
            tracking_id: result.tracking_id,
        });
        renderEventLog();

        // Show result
        const resultEl = document.getElementById('event-result');
        resultEl.classList.remove('hidden', 'error');
        resultEl.textContent = `✓ Sent to ${result.target_node} | tracking: ${result.tracking_id?.substring(0, 8)}... | state: ${result.state_size_bytes || '?'} bytes`;

        toast(`Event sent to ${result.target_node}`, 'success');
    } catch (e) {
        const resultEl = document.getElementById('event-result');
        resultEl.classList.remove('hidden');
        resultEl.classList.add('error');
        resultEl.textContent = `✕ Failed: ${e.message}`;
        toast(`Event send failed: ${e.message}`, 'error');
    } finally {
        btn.disabled = false;
    }
}

// --- Live Metrics ---
async function fetchLiveMetrics() {
    try {
        liveMetrics = await api('GET', '/metrics/live');
        renderLiveMetrics();
        renderPerNodeStats();
    } catch (e) {
        // Gateway may not be ready
    }
}

function renderLiveMetrics() {
    const m = liveMetrics;

    // Convergence time
    const convTime = m.convergence_time_seconds;
    document.getElementById('mv-convergence').textContent =
        convTime !== null && convTime !== undefined ? `${convTime}s` : '—';
    document.getElementById('stat-convergence-time').textContent =
        convTime !== null && convTime !== undefined ? `${convTime}s` : '—';

    // Data loss
    const dl = m.data_loss_percent;
    document.getElementById('mv-data-loss').textContent =
        dl !== null && dl !== undefined ? `${dl}%` : '—';

    // Gossip
    document.getElementById('mv-gossip').textContent =
        (m.total_gossip_messages_sent || 0).toLocaleString();
    document.getElementById('stat-gossip-count').textContent =
        m.total_gossip_messages_sent || 0;

    // State size
    const totalSize = m.total_state_size_bytes || 0;
    document.getElementById('mv-state-size').textContent = formatBytes(totalSize);

    // Network status
    const isDivergent = m.is_divergent;
    const statusEl = document.getElementById('mv-divergence');
    const detailEl = document.getElementById('mv-divergence-detail');
    const headerStatusEl = document.getElementById('stat-network-status');

    if (isDivergent) {
        statusEl.textContent = 'Divergent';
        statusEl.style.color = 'var(--accent-red)';
        detailEl.textContent = `Diverged for ${m.divergence_duration_seconds || 0}s`;
        headerStatusEl.innerHTML = '<span class="status-dot divergent"></span> Divergent';
    } else {
        statusEl.textContent = 'Converged';
        statusEl.style.color = 'var(--accent-green)';
        detailEl.textContent = 'All Merkle roots match';
        headerStatusEl.innerHTML = '<span class="status-dot converged"></span> OK';
    }

    // Update badge
    document.getElementById('metrics-update-badge').textContent =
        `Poll #${m.polls_completed || 0}`;
}

function renderPerNodeStats() {
    const tbody = document.getElementById('per-node-tbody');
    const perNode = liveMetrics.per_node || [];

    if (!perNode.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">Waiting for data...</td></tr>';
        return;
    }

    tbody.innerHTML = perNode.map(n => `
        <tr>
            <td>
                <span class="node-dot ${n.isolated ? 'isolated' : 'running'}" style="display:inline-block;vertical-align:middle;margin-right:4px;width:6px;height:6px;"></span>
                ${n.node_id || n.name}
            </td>
            <td>${n.event_count ?? '—'}</td>
            <td>${n.gossip_sent ?? '—'}</td>
            <td>${n.peers_discovered ?? '—'}</td>
            <td>${n.state_size_bytes ? formatBytes(n.state_size_bytes) : '—'}</td>
            <td>${n.merkle_root || '—'}</td>
        </tr>
    `).join('');
}

// --- Event Log ---
function renderEventLog() {
    const tbody = document.getElementById('event-log-tbody');
    const badge = document.getElementById('event-log-count');
    badge.textContent = `${eventLog.length} event${eventLog.length !== 1 ? 's' : ''}`;

    if (!eventLog.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">No events sent yet</td></tr>';
        return;
    }

    tbody.innerHTML = eventLog.slice(0, 50).map(ev => `
        <tr>
            <td>${formatTime(ev.timestamp)}</td>
            <td>${ev.node}</td>
            <td>${ev.type}</td>
            <td>${ev.category}</td>
            <td>${ev.state_size_bytes ? formatBytes(ev.state_size_bytes) : '—'}</td>
            <td>${ev.merkle_root ? ev.merkle_root.substring(0, 12) : '—'}</td>
        </tr>
    `).join('');
}

// --- Charts ---
function initCharts() {
    const defaults = Chart.defaults;
    defaults.color = '#8892a4';
    defaults.borderColor = 'rgba(255, 255, 255, 0.04)';
    defaults.font.family = "'Inter', system-ui, sans-serif";
    defaults.font.size = 10;

    const commonOpts = {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 400 },
        plugins: {
            legend: { display: false },
        },
        scales: {
            x: {
                grid: { color: 'rgba(255, 255, 255, 0.03)' },
                ticks: { maxTicksLimit: 8, font: { size: 9 } },
            },
            y: {
                grid: { color: 'rgba(255, 255, 255, 0.03)' },
                beginAtZero: true,
                ticks: { font: { size: 9 } },
            },
        },
    };

    charts.convergence = new Chart(
        document.getElementById('chart-convergence').getContext('2d'),
        {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Convergence Time (s)',
                    data: [],
                    borderColor: '#63b3ed',
                    backgroundColor: 'rgba(99, 179, 237, 0.1)',
                    borderWidth: 2,
                    pointRadius: 3,
                    pointBackgroundColor: '#63b3ed',
                    fill: true,
                    tension: 0.3,
                }],
            },
            options: { ...commonOpts },
        }
    );

    charts.stateSize = new Chart(
        document.getElementById('chart-state-size').getContext('2d'),
        {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'State Size (bytes)',
                    data: [],
                    borderColor: '#b794f4',
                    backgroundColor: 'rgba(183, 148, 244, 0.1)',
                    borderWidth: 2,
                    pointRadius: 3,
                    pointBackgroundColor: '#b794f4',
                    fill: true,
                    tension: 0.3,
                }],
            },
            options: { ...commonOpts },
        }
    );

    charts.nodeCount = new Chart(
        document.getElementById('chart-node-count').getContext('2d'),
        {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Active Nodes',
                    data: [],
                    borderColor: '#68d391',
                    backgroundColor: 'rgba(104, 211, 145, 0.1)',
                    borderWidth: 2,
                    pointRadius: 3,
                    pointBackgroundColor: '#68d391',
                    fill: true,
                    tension: 0.3,
                    stepped: 'after',
                }],
            },
            options: { ...commonOpts },
        }
    );
}

async function fetchTimeseries() {
    try {
        const [convergeData, sizeData, countData] = await Promise.all([
            api('GET', '/metrics/timeseries?metric=convergence_time&limit=50'),
            api('GET', '/metrics/timeseries?metric=state_size_bytes&limit=50'),
            api('GET', '/metrics/timeseries?metric=node_count&limit=50'),
        ]);

        updateChart(charts.convergence, convergeData);
        updateChart(charts.stateSize, sizeData);
        updateChart(charts.nodeCount, countData);
    } catch (e) {
        // Gateway or timeseries not ready
    }
}

function updateChart(chart, data) {
    if (!Array.isArray(data)) return;
    // Data comes in reverse chronological order
    const reversed = [...data].reverse();
    chart.data.labels = reversed.map(d => formatTime(d.timestamp));
    chart.data.datasets[0].data = reversed.map(d => d.value);
    chart.update('none');
}

// --- Topology Canvas ---
function renderTopology() {
    const canvas = document.getElementById('topology-canvas');
    const container = canvas.parentElement;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;

    // Read CSS layout dimensions BEFORE modifying the canvas
    const rect = container.getBoundingClientRect();
    const w = rect.width - 32;   // subtract padding
    const h = rect.height - 32;

    if (w <= 0 || h <= 0) return;

    // Set canvas buffer size (high-res) and CSS display size
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    canvas.style.width = w + 'px';
    canvas.style.height = h + 'px';
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    ctx.clearRect(0, 0, w, h);

    if (!nodes.length) {
        ctx.fillStyle = '#5a6478';
        ctx.font = '13px Inter, system-ui, sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText('Create nodes to see the topology', w / 2, h / 2);
        return;
    }

    // Calculate positions in a circle
    const cx = w / 2;
    const cy = h / 2;
    const radius = Math.min(w, h) * 0.35;
    const positions = nodes.map((_, i) => {
        const angle = (i * 2 * Math.PI / nodes.length) - Math.PI / 2;
        return {
            x: cx + radius * Math.cos(angle),
            y: cy + radius * Math.sin(angle),
        };
    });

    // Draw connections
    for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
            const eitherIsolated = nodes[i].isolated || nodes[j].isolated;

            ctx.beginPath();
            ctx.moveTo(positions[i].x, positions[i].y);
            ctx.lineTo(positions[j].x, positions[j].y);

            if (eitherIsolated) {
                ctx.strokeStyle = 'rgba(252, 129, 129, 0.15)';
                ctx.setLineDash([4, 4]);
            } else {
                ctx.strokeStyle = 'rgba(99, 179, 237, 0.2)';
                ctx.setLineDash([]);
            }
            ctx.lineWidth = 1;
            ctx.stroke();
            ctx.setLineDash([]);
        }
    }

    // Draw nodes
    nodes.forEach((node, i) => {
        const pos = positions[i];
        const isIsolated = node.isolated;

        // Glow
        const gradient = ctx.createRadialGradient(pos.x, pos.y, 0, pos.x, pos.y, 24);
        gradient.addColorStop(0, isIsolated
            ? 'rgba(252, 129, 129, 0.25)'
            : 'rgba(99, 179, 237, 0.25)');
        gradient.addColorStop(1, 'rgba(0,0,0,0)');
        ctx.fillStyle = gradient;
        ctx.beginPath();
        ctx.arc(pos.x, pos.y, 24, 0, Math.PI * 2);
        ctx.fill();

        // Node circle
        ctx.beginPath();
        ctx.arc(pos.x, pos.y, 10, 0, Math.PI * 2);
        ctx.fillStyle = isIsolated ? '#fc8181' : '#63b3ed';
        ctx.fill();

        // Inner dot
        ctx.beginPath();
        ctx.arc(pos.x, pos.y, 3, 0, Math.PI * 2);
        ctx.fillStyle = isIsolated ? '#fed7d7' : '#bee3f8';
        ctx.fill();

        // Label
        ctx.fillStyle = '#8892a4';
        ctx.font = '10px Inter, system-ui, sans-serif';
        ctx.textAlign = 'center';
        const label = node.node_id || node.name || '';
        ctx.fillText(label.length > 12 ? label.substring(0, 12) + '…' : label, pos.x, pos.y + 24);
    });
}

// Re-render topology on window resize
let resizeTimer;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(renderTopology, 150);
});

// --- Utilities ---
function formatBytes(bytes) {
    if (bytes == null || isNaN(bytes)) return '—';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function formatTime(isoString) {
    if (!isoString) return '—';
    try {
        const d = new Date(isoString);
        if (isNaN(d.getTime())) return isoString.substring(11, 19);
        return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch {
        return isoString.substring(11, 19);
    }
}

// --- Evaluation Suite ---
let evalPollTimer = null;

async function runEvaluation() {
    const btn = document.getElementById('btn-run-eval');
    const nodeCountsRaw = document.getElementById('eval-node-counts').value;
    const eventsPerNode = parseInt(document.getElementById('eval-events-per-node').value) || 5;

    const nodeCounts = nodeCountsRaw.split(',').map(s => parseInt(s.trim())).filter(n => n > 0);
    if (!nodeCounts.length) {
        toast('Enter at least one node count', 'error');
        return;
    }

    btn.classList.add('running');
    document.getElementById('btn-download-eval').disabled = true;
    document.getElementById('eval-progress-wrap').classList.remove('hidden');
    document.getElementById('eval-progress-fill').style.width = '0%';
    document.getElementById('eval-step-label').textContent = 'Starting...';
    document.getElementById('eval-status-badge').textContent = 'Running';
    document.getElementById('eval-status-badge').className = 'badge badge-warning';

    try {
        await api('POST', '/evaluation/run', {
            node_counts: nodeCounts,
            events_per_node: eventsPerNode,
        });
        toast('Evaluation started', 'success');
        startEvalPolling();
    } catch (e) {
        toast(`Failed to start: ${e.message}`, 'error');
        btn.classList.remove('running');
    }
}

function startEvalPolling() {
    if (evalPollTimer) clearInterval(evalPollTimer);
    evalPollTimer = setInterval(pollEvalStatus, 2000);
}

async function pollEvalStatus() {
    try {
        const status = await api('GET', '/evaluation/status');

        // Update progress bar
        const pct = status.total_steps > 0
            ? Math.round((status.progress / status.total_steps) * 100)
            : 0;
        document.getElementById('eval-progress-fill').style.width = pct + '%';
        document.getElementById('eval-step-label').textContent = status.current_step || '...';

        // Update badge
        if (status.running) {
            document.getElementById('eval-status-badge').textContent =
                `Step ${status.progress + 1}/${status.total_steps}`;
        }

        // If done, stop polling and fetch results
        if (!status.running) {
            clearInterval(evalPollTimer);
            evalPollTimer = null;

            const btn = document.getElementById('btn-run-eval');
            btn.classList.remove('running');

            if (status.error) {
                document.getElementById('eval-status-badge').textContent = 'Error';
                document.getElementById('eval-status-badge').className = 'badge badge-error';
                toast(`Evaluation error: ${status.error}`, 'error');
            } else {
                document.getElementById('eval-progress-fill').style.width = '100%';
                document.getElementById('eval-status-badge').textContent = 'Complete';
                document.getElementById('eval-status-badge').className = 'badge badge-success';
                toast('Evaluation complete!', 'success');
            }

            if (status.has_csv) {
                document.getElementById('btn-download-eval').disabled = false;
            }

            // Fetch and display results
            await fetchEvalResults();
        }
    } catch (e) {
        // Silently retry
    }
}

async function fetchEvalResults() {
    try {
        const data = await api('GET', '/evaluation/results');
        renderEvalResults(data.results || []);
        if (data.has_csv) {
            document.getElementById('btn-download-eval').disabled = false;
        }
    } catch (e) {
        // No results yet
    }
}

function renderEvalResults(results) {
    const tbody = document.getElementById('eval-results-tbody');
    if (!results.length) {
        tbody.innerHTML = '<tr><td colspan="8" class="empty-cell">No evaluation data — click Run Evaluation to start</td></tr>';
        return;
    }

    const scenarioLabels = {
        baseline_convergence: { label: 'Baseline', cls: 'eval-tag-baseline' },
        partition_heal: { label: 'Partition', cls: 'eval-tag-partition' },
        scaling_events: { label: 'Scaling', cls: 'eval-tag-scaling' },
    };

    tbody.innerHTML = results.map(r => {
        const s = scenarioLabels[r.scenario] || { label: r.scenario, cls: '' };
        const rowCls = r.is_converged ? 'eval-row-converged' : 'eval-row-diverged';
        const convTime = r.convergence_time_s != null ? `${r.convergence_time_s.toFixed(3)}s` : '—';
        const dataLoss = r.data_loss_pct != null ? `${r.data_loss_pct}%` : '—';
        const convergedIcon = r.is_converged ? '✅' : '❌';

        return `
            <tr class="${rowCls}">
                <td><span class="eval-scenario-tag ${s.cls}">${s.label}</span></td>
                <td>${r.node_count}</td>
                <td>${r.events_sent}</td>
                <td>${convTime}</td>
                <td>${dataLoss}</td>
                <td>${r.gossip_messages_sent}</td>
                <td>${formatBytes(r.total_state_size_bytes)}</td>
                <td>${convergedIcon}</td>
            </tr>
        `;
    }).join('');
}

function downloadEvaluation() {
    window.open('/evaluation/download', '_blank');
}

// Periodically refresh charts (every 15s)
setInterval(fetchTimeseries, 15000);
