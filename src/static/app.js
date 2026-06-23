/* ═══════════════════════════════════════════════════════════
   Edge Mesh Observatory — Dashboard JS
   ═══════════════════════════════════════════════════════════ */

// --- State ---
let nodes = [];
let gateways = [];
let liveMetrics = {};
let eventLog = [];
let charts = {};
let pollInterval = null;
let ws = null;
let wsConnected = false;
const POLL_MS = 10000; // Fallback poll (slower since WS handles real-time)
let lastTimeseriesFetchMs = 0;

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    refreshAll();
    connectWebSocket();
    // Keep live metrics fresh even with WebSocket; use full polling only as fallback.
    pollInterval = setInterval(() => {
        fetchLiveMetrics();
        if (!wsConnected) {
            pollData();
        }
    }, POLL_MS);
});

// --- WebSocket ---
function connectWebSocket() {
    try {
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        ws = new WebSocket(`${protocol}://${window.location.host}/ws`);

        ws.onopen = () => {
            wsConnected = true;
            console.log('WebSocket connected');
        };

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (data.type === 'update') {
                    if (data.nodes) {
                        nodes = data.nodes;
                        renderNodes();
                        renderTopology();
                        updateEventTargetSelector();
                    }
                    if (data.gateways) {
                        gateways = data.gateways;
                        renderGateways();
                        updateGatewaySelector();
                    }
                    if (data.metrics) {
                        liveMetrics = data.metrics;
                        renderMetrics();
                        updateCharts();
                    }
                }
            } catch (e) {
                console.warn('WebSocket message parse error', e);
            }
        };

        ws.onclose = () => {
            wsConnected = false;
            console.log('WebSocket disconnected, retrying in 3s...');
            setTimeout(connectWebSocket, 3000);
        };

        ws.onerror = () => {
            wsConnected = false;
            ws.close();
        };
    } catch (e) {
        wsConnected = false;
        console.warn('WebSocket not available, using polling fallback');
    }
}

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
        fetchGateways(),
        fetchNodes(),
        fetchLiveMetrics(),
        fetchTimeseries(),
    ]);
}

async function pollData() {
    await Promise.allSettled([
        fetchGateways(),
        fetchNodes(),
        fetchLiveMetrics(),
    ]);
}

// --- Gateways ---
async function fetchGateways() {
    try {
        gateways = await api('GET', '/gateways');
        renderGateways();
        updateGatewaySelector();
    } catch (e) {
        // Docker may not be ready
    }
}

function renderGateways() {
    const container = document.getElementById('gateway-list');
    if (!container) return;

    if (!gateways.length) {
        container.innerHTML = '<div class="empty-state">No gateways running</div>';
        return;
    }

    container.innerHTML = gateways.map(g => {
        const assignedCount = g.assigned_nodes ? g.assigned_nodes.length : 0;
        return `
            <div class="node-item">
                <span class="node-dot running"></span>
                <span class="node-name" style="color: #ed8936;">${g.name}</span>
                <span class="node-id">port ${g.host_port} &middot; ${assignedCount} nodes</span>
                <div class="node-actions">
                    <button class="btn btn-sm btn-outline" onclick="removeGateway('${g.name}')">✕</button>
                </div>
            </div>
        `;
    }).join('');
}

async function createGateway() {
    try {
        const result = await api('POST', '/gateways');
        toast(`Created ${result.gateway_id || result.container}`, 'success');
        await fetchGateways();
        await fetchNodes();
    } catch (e) {
        toast(`Create failed: ${e.message}`, 'error');
    }
}

async function removeGateway(gatewayId) {
    try {
        await api('DELETE', `/gateways/${gatewayId}`);
        toast(`Removed ${gatewayId}`, 'info');
        await fetchGateways();
        await fetchNodes();
    } catch (e) {
        toast(`Remove failed: ${e.message}`, 'error');
    }
}

// --- Nodes ---
async function fetchNodes() {
    try {
        nodes = await api('GET', '/nodes');
        renderNodes();
        renderTopology();
        updateNodeSelector();
        const statEl = document.getElementById('stat-node-count');
        if (statEl) statEl.textContent = nodes.length;
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

    const activeGateways = gateways || [];

    container.innerHTML = nodes.map(n => {
        const statusClass = n.isolated ? 'isolated' : (n.status === 'running' ? 'running' : 'stopped');
        const statusLabel = n.isolated ? 'isolated' : n.status;
        const gwLabel = n.assigned_gateway ? ` &middot; ${n.assigned_gateway}` : '';

        const optionsHtml = ['<option value="">Auto (Round-Robin)</option>', ...activeGateways.map(g => `<option value="${g.name}" ${g.name === n.assigned_gateway ? 'selected' : ''}>${g.name}</option>`)].join('');

        return `
            <div class="node-item">
                <span class="node-dot ${statusClass}"></span>
                <span class="node-name">${n.name}</span>
                <span class="node-id">${n.node_id} &middot; ${statusLabel}${gwLabel}</span>
                <div class="node-actions" style="display: flex; gap: 6px; align-items: center;">
                    <select onchange="changeNodeGateway('${n.node_id}', this.value)" style="font-size: 11px; padding: 2px 4px; border-radius: 4px; border: 1px solid var(--border-color); background: var(--surface); color: var(--text-secondary);">
                        ${optionsHtml}
                    </select>
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
    if (!select) return;
    const current = select.value;
    select.innerHTML = '<option value="">Any node</option>' +
        nodes.map(n => `<option value="${n.node_id}" ${n.node_id === current ? 'selected' : ''}>${n.name} (${n.node_id})</option>`).join('');
}

function updateGatewaySelector() {
    const selects = document.querySelectorAll('.gateway-selector');
    selects.forEach(select => {
        const current = select.value;
        let html = '<option value="">Auto (Round-Robin)</option>';
        if (gateways) {
            gateways.forEach(g => {
                html += `<option value="${g.name}" ${g.name === current ? 'selected' : ''}>${g.name}</option>`;
            });
        }
        select.innerHTML = html;
    });
}

// --- Node Actions ---
async function createNode() {
    try {
        const gwSelect = document.getElementById('gateway-selector');
        const query = gwSelect && gwSelect.value ? `?assigned_gateway=${encodeURIComponent(gwSelect.value)}` : '';
        const result = await api('POST', `/nodes${query}`);
        toast(`Created ${result.container || result.node_id}`, 'success');
        await fetchNodes();
    } catch (e) {
        toast(`Create failed: ${e.message}`, 'error');
    }
}

async function createNodesBatch(count) {
    try {
        const gwSelect = document.getElementById('gateway-selector');
        const query = gwSelect && gwSelect.value ? `&assigned_gateway=${encodeURIComponent(gwSelect.value)}` : '';
        const result = await api('POST', `/nodes/batch?count=${count}${query}`);
        toast(`Created ${result.created_count} nodes`, 'success');
        await fetchNodes();
    } catch (e) {
        toast(`Batch create failed: ${e.message}`, 'error');
    }
}

async function changeNodeGateway(nodeId, gatewayName) {
    try {
        await api('PUT', `/nodes/${nodeId}/gateway`, { assigned_gateway: gatewayName });
        toast(`Reassigned ${nodeId} to ${gatewayName || 'Auto'}`, 'info');
        await fetchNodes();
    } catch (e) {
        toast(`Reassign failed: ${e.message}`, 'error');
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

function renderMetrics() {
    renderLiveMetrics();
    renderPerNodeStats();
}

function renderLiveMetrics() {
    const m = liveMetrics;

    // Convergence time
    const convTime = m.convergence_time_seconds;
    const convergenceEl = document.getElementById('mv-convergence');
    const headerConvergenceEl = document.getElementById('stat-convergence-time');
    if (convergenceEl) {
        convergenceEl.textContent =
            convTime !== null && convTime !== undefined ? `${convTime}s` : '—';
    }
    if (headerConvergenceEl) {
        headerConvergenceEl.textContent =
            convTime !== null && convTime !== undefined ? `${convTime}s` : '—';
    }

    // Data loss
    const dl = m.data_loss_percent;
    const dataLossEl = document.getElementById('mv-data-loss');
    if (dataLossEl) {
        dataLossEl.textContent = dl !== null && dl !== undefined ? `${dl}%` : '—';
    }

    // Gossip
    const gossipEl = document.getElementById('mv-gossip');
    const headerGossipEl = document.getElementById('stat-gossip-count');
    if (gossipEl) {
        gossipEl.textContent = (m.total_gossip_messages_sent || 0).toLocaleString();
    }
    if (headerGossipEl) {
        headerGossipEl.textContent = m.total_gossip_messages_sent || 0;
    }

    // State size
    const totalSize = m.total_state_size_bytes || 0;
    const stateSizeEl = document.getElementById('mv-state-size');
    if (stateSizeEl) {
        stateSizeEl.textContent = formatBytes(totalSize);
    }

    // Confidence-aware status and coverage
    const knownStatusRaw = (m.known_status || 'unknown_partial').toLowerCase();
    const knownStatusText =
        knownStatusRaw === 'known_converged' ? 'Known Converged' :
        knownStatusRaw === 'known_diverged' ? 'Known Diverged' :
        'Insufficient Coverage';
    const coveragePct = m.coverage_percent !== null && m.coverage_percent !== undefined
        ? Number(m.coverage_percent)
        : 0;
    const knownStatusEl = document.getElementById('mv-known-status');
    const coverageEl = document.getElementById('mv-coverage');
    if (knownStatusEl) {
        knownStatusEl.textContent = knownStatusText;
    }
    if (coverageEl) {
        coverageEl.textContent = `${coveragePct.toFixed(1)}%`;
    }

    // Network status
    const knownStatus = knownStatusRaw;
    const isDivergent = knownStatus === 'known_diverged';
    const statusEl = document.getElementById('mv-divergence');
    const detailEl = document.getElementById('mv-divergence-detail');
    const headerStatusEl = document.getElementById('stat-network-status');
    const coverageText = `${coveragePct.toFixed(1)}% coverage`;

    if (!statusEl || !detailEl || !headerStatusEl) {
        return;
    }

    const divergenceCard = document.getElementById('mc-divergence');
    const knownStatusCard = document.getElementById('mc-known-status');
    if (divergenceCard) {
        divergenceCard.classList.remove('status-converged', 'status-diverged', 'status-partial');
    }
    if (knownStatusCard) {
        knownStatusCard.classList.remove('status-converged', 'status-diverged', 'status-partial');
    }

    if (knownStatus === 'unknown_partial') {
        statusEl.textContent = 'Insufficient Coverage';
        statusEl.style.color = '#d69e2e';
        detailEl.textContent = coverageText;
        headerStatusEl.innerHTML = '<span class="status-dot partial"></span> Partial';
        if (divergenceCard) divergenceCard.classList.add('status-partial');
        if (knownStatusCard) knownStatusCard.classList.add('status-partial');
    } else if (isDivergent) {
        statusEl.textContent = 'Divergent';
        statusEl.style.color = 'var(--accent-red)';
        detailEl.textContent = `Diverged for ${m.divergence_duration_seconds || 0}s · ${coverageText}`;
        headerStatusEl.innerHTML = '<span class="status-dot divergent"></span> Divergent';
        if (divergenceCard) divergenceCard.classList.add('status-diverged');
        if (knownStatusCard) knownStatusCard.classList.add('status-diverged');
    } else {
        statusEl.textContent = 'Converged';
        statusEl.style.color = 'var(--accent-green)';
        detailEl.textContent = `Known converged · ${coverageText}`;
        headerStatusEl.innerHTML = '<span class="status-dot converged"></span> OK';
        if (divergenceCard) divergenceCard.classList.add('status-converged');
        if (knownStatusCard) knownStatusCard.classList.add('status-converged');
    }

    // Update badge
    const updateBadge = document.getElementById('metrics-update-badge');
    if (updateBadge) {
        updateBadge.textContent = `Poll #${m.polls_completed || 0}`;
    }
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

function updateCharts() {
    const mode = (liveMetrics.observability_mode || '').toLowerCase();
    const minIntervalMs =
        mode === 'off' ? 60000 :
        mode === 'light' ? 20000 :
        5000;
    const now = Date.now();
    if (now - lastTimeseriesFetchMs < minIntervalMs) {
        return;
    }
    lastTimeseriesFetchMs = now;
    fetchTimeseries();
}

// --- D3 Topology Canvas ---
let simulation = null;
let d3Nodes = [];
let d3Links = [];
let lastTopologyFingerprint = '';

function renderTopology() {
    const svgEl = document.getElementById('topology-canvas');
    if (!svgEl) return;
    const container = svgEl.parentElement;
    const rect = container.getBoundingClientRect();
    const w = rect.width;
    const h = rect.height;

    if (w <= 0 || h <= 0) return;

    const svg = d3.select('#topology-canvas')
        .attr('width', w)
        .attr('height', h);

    if (!nodes.length && (!gateways || !gateways.length)) {
        svg.selectAll('*').remove();
        svg.append('text')
            .attr('x', w / 2)
            .attr('y', h / 2)
            .attr('text-anchor', 'middle')
            .attr('fill', '#5a6478')
            .style('font-family', 'Inter, system-ui, sans-serif')
            .style('font-size', '13px')
            .text('Create nodes to see the topology');
        lastTopologyFingerprint = '';
        return;
    }

    // Prepare data
    const activeGateways = gateways || [];
    const newNodes = [];
    const newLinks = [];
    const nodeById = new Map();

    activeGateways.forEach(gw => {
        const n = { id: gw.name, type: 'gateway', label: gw.name };
        newNodes.push(n);
        nodeById.set(n.id, n);
    });

    nodes.forEach(node => {
        const n = { id: node.name, type: 'node', label: node.node_id || node.name, isolated: node.isolated };
        newNodes.push(n);
        nodeById.set(n.id, n);
    });

    // links
    nodes.forEach(node => {
        if (node.assigned_gateway && nodeById.has(node.assigned_gateway) && !node.isolated) {
            newLinks.push({ source: node.name, target: node.assigned_gateway, type: 'gateway-link' });
        }
    });


    for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
            const sameGateway =
                nodes[i].assigned_gateway &&
                nodes[j].assigned_gateway &&
                nodes[i].assigned_gateway === nodes[j].assigned_gateway;
            if (!sameGateway) continue;

            const eitherIsolated = nodes[i].isolated || nodes[j].isolated;
            if (!eitherIsolated) {
                newLinks.push({ source: nodes[i].name, target: nodes[j].name, type: 'mesh-link' });
            } else {
                newLinks.push({ source: nodes[i].name, target: nodes[j].name, type: 'isolated-link' });
            }
        }
    }

    // Build a fingerprint of the current graph structure to detect changes
    const nodeFingerprint = newNodes.map(n => `${n.id}:${n.type}:${n.isolated || false}`).sort().join('|');
    const linkFingerprint = newLinks.map(l => `${l.source}-${l.target}:${l.type}`).sort().join('|');
    const currentFingerprint = nodeFingerprint + '##' + linkFingerprint;
    const graphChanged = currentFingerprint !== lastTopologyFingerprint;
    lastTopologyFingerprint = currentFingerprint;

    // Merge with existing for stable layout
    const oldNodeById = new Map(d3Nodes.map(d => [d.id, d]));
    d3Nodes = newNodes.map(n => {
        const old = oldNodeById.get(n.id);
        if (old) {
            n.x = old.x;
            n.y = old.y;
            n.vx = old.vx;
            n.vy = old.vy;
            n.fx = old.fx;
            n.fy = old.fy;
        }
        return n;
    });
    d3Links = newLinks;

    svg.selectAll('*').remove();

    const defs = svg.append('defs');
    const glowFilter = defs.append('filter').attr('id', 'glow');
    glowFilter.append('feGaussianBlur').attr('stdDeviation', '4').attr('result', 'coloredBlur');
    const feMerge = glowFilter.append('feMerge');
    feMerge.append('feMergeNode').attr('in', 'coloredBlur');
    feMerge.append('feMergeNode').attr('in', 'SourceGraphic');

    const g = svg.append('g');

    if (!simulation) {
        simulation = d3.forceSimulation()
            .force('charge', d3.forceManyBody().strength(d => d.type === 'gateway' ? -600 : -250))
            .force('center', d3.forceCenter(w / 2, h / 2))
            .force('collision', d3.forceCollide().radius(d => d.type === 'gateway' ? 40 : 25));
    } else {
        simulation.force('center', d3.forceCenter(w / 2, h / 2));
    }

    simulation.nodes(d3Nodes);
    simulation.force('link', d3.forceLink(d3Links).id(d => d.id).distance(d => {
        if (d.type === 'gateway-link') return 80;
        return 140;
    }).strength(d => {
        if (d.type === 'isolated-link') return 0.01;
        if (d.type === 'gateway-link') return 0.8;
        return 0.1;
    }));

    // Only re-energize the simulation when the graph structure actually changed
    if (graphChanged) {
        simulation.alpha(0.3).restart();
    } else {
        // Just nudge to redraw without drifting
        simulation.alpha(0).restart();
    }

    // Draw links
    const link = g.append('g')
        .selectAll('line')
        .data(d3Links)
        .join('line')
        .attr('stroke', d => {
            if (d.type === 'isolated-link') return 'rgba(252, 129, 129, 0.15)';
            if (d.type === 'gateway-link') return 'rgba(237, 137, 54, 0.5)';
            return 'rgba(99, 179, 237, 0.2)';
        })
        .attr('stroke-dasharray', d => {
            if (d.type === 'isolated-link' || d.type === 'gateway-link') return '4,4';
            return null;
        });

    // Draw nodes
    const node = g.append('g')
        .selectAll('g')
        .data(d3Nodes)
        .join('g')
        .call(d3.drag()
            .on('start', dragstarted)
            .on('drag', dragged)
            .on('end', dragended)
        );

    // Gateway style
    const gatewaysG = node.filter(d => d.type === 'gateway');
    gatewaysG.append('rect')
        .attr('x', -15).attr('y', -15).attr('width', 30).attr('height', 30)
        .attr('fill', 'rgba(237, 137, 54, 0.2)');
    gatewaysG.append('rect')
        .attr('x', -8).attr('y', -8).attr('width', 16).attr('height', 16)
        .attr('fill', '#ed8936');
    gatewaysG.append('rect')
        .attr('x', -2).attr('y', -2).attr('width', 4).attr('height', 4)
        .attr('fill', '#fbd38d');

    // Edge node style
    const edgesG = node.filter(d => d.type === 'node');

    edgesG.append('circle')
        .attr('r', 10)
        .attr('fill', d => d.isolated ? '#fc8181' : '#63b3ed')
        .style('filter', 'url(#glow)');
    edgesG.append('circle')
        .attr('r', 10)
        .attr('fill', 'none')
        .attr('stroke', d => d.isolated ? 'rgba(252, 129, 129, 0.3)' : 'rgba(99, 179, 237, 0.3)')
        .attr('stroke-width', 8);
    edgesG.append('circle')
        .attr('r', 3)
        .attr('fill', d => d.isolated ? '#fed7d7' : '#bee3f8');

    node.append('text')
        .attr('y', 25)
        .attr('text-anchor', 'middle')
        .attr('fill', d => d.type === 'gateway' ? '#bdc4d1' : '#8892a4')
        .style('font-family', 'Inter, system-ui, sans-serif')
        .style('font-size', d => d.type === 'gateway' ? '11px' : '10px')
        .style('font-weight', d => d.type === 'gateway' ? 'bold' : 'normal')
        .text(d => d.label.length > 12 ? d.label.substring(0, 12) + '…' : d.label);

    simulation.on('tick', () => {
        // Enforce bounding box constraints
        node.attr('transform', d => {
            const r = d.type === 'gateway' ? 20 : 10;
            d.x = Math.max(r, Math.min(w - r, d.x));
            d.y = Math.max(r, Math.min(h - r, d.y));
            return `translate(${d.x},${d.y})`;
        });

        link
            .attr('x1', d => d.source.x)
            .attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x)
            .attr('y2', d => d.target.y);
    });

    function dragstarted(event) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        event.subject.fx = event.subject.x;
        event.subject.fy = event.subject.y;
    }

    function dragged(event) {
        event.subject.fx = event.x;
        event.subject.fy = event.y;
    }

    function dragended(event) {
        if (!event.active) simulation.alphaTarget(0);
        event.subject.fx = null;
        event.subject.fy = null;
    }
}

// Re-render topology on window resize
let resizeTimer;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(renderTopology, 150);
});

function toggleFullscreen() {
    const topologyPanel = document.getElementById('topology-panel');
    topologyPanel.classList.toggle('fullscreen');

    setTimeout(() => {
        if (simulation) {
            simulation.alpha(0.3).restart();
        }
        renderTopology();
    }, 10);
}

function toggleAccordion(headerEl) {
    const panel = headerEl.closest('.accordion-panel');
    if (panel) {
        panel.classList.toggle('open');
        if (panel.classList.contains('open') && panel.id === 'charts-panel') {
            setTimeout(() => {
                Object.values(charts).forEach((chart) => {
                    if (chart && typeof chart.resize === 'function') {
                        chart.resize();
                    }
                });
            }, 10);
        }
    }
}

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
    const partitionSplitsRaw = document.getElementById('eval-partition-splits').value;

    const nodeCounts = nodeCountsRaw.split(',').map(s => parseInt(s.trim())).filter(n => n > 0);
    const partitionSplits = partitionSplitsRaw.split(',').map(s => parseFloat(s.trim())).filter(n => n > 0 && n < 1);
    if (!nodeCounts.length) {
        toast('Enter at least one node count', 'error');
        return;
    }
    if (!partitionSplits.length) {
        toast('Enter at least one partition split between 0 and 1', 'error');
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
            partition_splits: partitionSplits,
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
        tbody.innerHTML = '<tr><td colspan="12" class="empty-cell">No evaluation data — click Run Evaluation to start</td></tr>';
        return;
    }

    const scenarioLabels = {
        baseline: { label: 'Baseline', cls: 'eval-tag-baseline' },
        baseline_convergence: { label: 'Baseline', cls: 'eval-tag-baseline' },
        partition_heal: { label: 'Partition', cls: 'eval-tag-partition' },
        high_load: { label: 'High Load', cls: 'eval-tag-scaling' },
        scaling_events: { label: 'Scaling', cls: 'eval-tag-scaling' },
    };

    tbody.innerHTML = results.map(r => {
        const s = scenarioLabels[r.scenario] || { label: r.scenario, cls: '' };
        const status = (r.known_status || '').toLowerCase();
        const rowCls =
            status === 'known_converged' ? 'eval-row-converged' :
            status === 'known_diverged' ? 'eval-row-diverged' :
            '';
        const convTime = r.convergence_time_s != null ? `${Number(r.convergence_time_s).toFixed(3)}s` : '—';
        const dataLoss = r.data_loss_count != null ? `${r.data_loss_count} (${r.data_loss_pct ?? 0}%)` : '—';
        const staleness = r.gateway_staleness_seconds != null ? `${Number(r.gateway_staleness_seconds).toFixed(3)}s` : '—';
        const writeSuccess = r.write_success_rate != null ? `${(Number(r.write_success_rate) * 100).toFixed(1)}%` : '—';
        const writes = `${r.accepted_events ?? '—'}/${r.attempted_events ?? '—'}`;
        const statusLabel =
            status === 'known_converged' ? 'Known Converged' :
            status === 'known_diverged' ? 'Known Diverged' :
            'Insufficient Coverage';
        const statusCls =
            status === 'known_converged' ? 'eval-status-converged' :
            status === 'known_diverged' ? 'eval-status-diverged' :
            'eval-status-partial';

        return `
            <tr class="${rowCls}">
                <td><span class="eval-scenario-tag ${s.cls}">${s.label}</span></td>
                <td>${r.partition_split || '—'}</td>
                <td>${r.node_count}</td>
                <td>${writes}</td>
                <td>${writeSuccess}</td>
                <td>${convTime}</td>
                <td>${dataLoss}</td>
                <td>${staleness}</td>
                <td>${formatBytes(r.sync_bytes_total)}</td>
                <td>${r.repair_pull_count ?? '—'}</td>
                <td>${formatBytes(r.total_state_size_bytes)}</td>
                <td><span class="eval-status-tag ${statusCls}">${statusLabel}</span></td>
            </tr>
        `;
    }).join('');
}

function downloadEvaluation() {
    window.open('/evaluation/download', '_blank');
}

// Periodically refresh charts (every 15s)
setInterval(fetchTimeseries, 15000);
