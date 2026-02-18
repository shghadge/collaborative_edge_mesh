const API_BASE = window.location.origin;

// State
let nodes = [];
let logs = [];
let isDivergent = false;
let status = "SYNCED";
let runtimeMetrics = {};
let lastActionResult = null;
let operationPending = false;
let fetchInFlight = false;

// Canvas
const canvas = document.getElementById("topologyCanvas");
const ctx = canvas.getContext("2d");

// Init
function init() {
    resizeCanvas();
    window.addEventListener("resize", resizeCanvas);

    // Initial fetch
    fetchData();

    // Poll every 2s
    setInterval(fetchData, 2000);
}

function resizeCanvas() {
    const parent = canvas.parentElement;
    canvas.width = parent.clientWidth;
    canvas.height = parent.clientHeight;
}

// --- API Calls ---

async function fetchData() {
    if (fetchInFlight) {
        return;
    }
    fetchInFlight = true;
    try {
        const [nodesRes, statusRes, divRes, metricsRes, runtimeRes] = await Promise.all([
            fetch(`${API_BASE}/nodes`).then(r => r.json()),
            fetch(`${API_BASE}/gateway/status`).then(r => r.json()),
            fetch(`${API_BASE}/gateway/divergence`).then(r => r.json()),
            fetch(`${API_BASE}/gateway/metrics?name=merge_time_ms&limit=1`).then(r => r.json()),
            fetch(`${API_BASE}/gateway/runtime-metrics`).then(r => r.json())
        ]);

        nodes = nodesRes;
        isDivergent = divRes.is_divergent;
        logs = divRes.log;
        runtimeMetrics = runtimeRes.runtime_metrics || {};

        // Check convergence status
        status = isDivergent ? "DIVERGENT" : "SYNCED";

        // Update Stats Bar
        document.getElementById("stat-nodes").innerText = nodes.length;
        document.getElementById("stat-status").innerText = status;
        document.getElementById("stat-status").className = `value status-badge ${isDivergent ? 'status-divergent' : 'status-synced'}`;
        document.getElementById("stat-retries").innerText = runtimeMetrics.http_retries ?? 0;

        if (metricsRes.length > 0) {
            document.getElementById("stat-merge").innerText = metricsRes[0].value.toFixed(2) + "ms";
        } else {
            document.getElementById("stat-merge").innerText = (runtimeMetrics.last_merge_duration_ms ?? 0) + "ms";
        }

        // Render UI
        renderTopology();
        renderNodeList();
        renderRuntimeMetrics();
        renderLogs();
        renderActionResult();

    } catch (e) {
        console.error("Poll failed", e);
    } finally {
        fetchInFlight = false;
    }
}

async function apiAction(url, options = {}) {
    const response = await fetch(url, options);
    const data = await response.json();
    if (!response.ok) {
        const message = data.message || data.detail || `Request failed (${response.status})`;
        throw new Error(message);
    }
    return data;
}

function setActionResult(payload) {
    lastActionResult = payload;
    renderActionResult();
}

function showLoading(text = "Running operation...") {
    operationPending = true;
    setControlsDisabled(true);
    const overlay = document.getElementById("loadingOverlay");
    const loadingText = document.getElementById("loadingText");
    if (loadingText) loadingText.innerText = text;
    if (overlay) overlay.classList.remove("hidden");
}

function hideLoading() {
    operationPending = false;
    setControlsDisabled(false);
    const overlay = document.getElementById("loadingOverlay");
    if (overlay) overlay.classList.add("hidden");
}

function setControlsDisabled(disabled) {
    const controls = document.querySelectorAll("button, input");
    controls.forEach((element) => {
        if (element.id === "loadingOverlay") {
            return;
        }
        element.disabled = disabled;
    });
}

function showToast(message, level = "success", ttlMs = 3500) {
    const container = document.getElementById("toastContainer");
    if (!container) return;

    const toast = document.createElement("div");
    toast.className = `toast ${level}`;
    toast.innerText = message;
    container.appendChild(toast);

    setTimeout(() => {
        toast.remove();
    }, ttlMs);
}

function summarizeAction(payload) {
    if (!payload) return "No action details";
    const action = payload.action || "operation";
    const status = payload.status || "unknown";

    if (action === "split_brain_then_heal") {
        return `${action}: ${status} • converged=${payload.converged}`;
    }

    if (action === "bootstrap_events_convergence") {
        return `${action}: ${status} • events ok=${payload.successful_events ?? 0}/${(payload.successful_events ?? 0) + (payload.failed_events ?? 0)}`;
    }

    if (action === "create_nodes_batch") {
        return `${action}: ${status} • created=${payload.created_count}/${payload.requested}`;
    }

    return `${action}: ${status} • ${payload.message || "completed"}`;
}

async function runActionWithLoading(actionFn, loadingText) {
    if (operationPending) {
        showToast("Another operation is already running", "warning", 3000);
        return;
    }

    showLoading(loadingText);
    showToast("Operation started...", "warning", 1800);
    try {
        const result = await actionFn();
        setActionResult(result);
        const level = result?.status === "failed" ? "error" : (result?.status === "partial" ? "warning" : "success");
        showToast(summarizeAction(result), level);
    } catch (error) {
        const err = { status: "error", message: error.message, action: "operation" };
        setActionResult(err);
        showToast(error.message, "error", 5000);
    } finally {
        hideLoading();
        fetchData();
    }
}

async function createNode() {
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/nodes`, { method: "POST" }),
        "Creating node..."
    );
}

async function createNodeBatch() {
    const count = Number(document.getElementById("batchCount")?.value || 1);
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/nodes/batch?count=${count}`, { method: "POST" }),
        `Creating ${count} nodes...`
    );
}

async function removeNode(id) {
    if (!confirm(`Delete ${id}?`)) return;
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/nodes/${id}`, { method: "DELETE" }),
        `Removing ${id}...`
    );
}

async function isolateNode(id) {
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/nodes/${id}/partition`, { method: "POST" }),
        `Isolating ${id}...`
    );
}

async function healNode(id) {
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/nodes/${id}/partition`, { method: "DELETE" }),
        `Healing ${id}...`
    );
}

async function healAll() {
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/partition/heal-all`, { method: "POST" }),
        "Healing all partitions..."
    );
}

async function splitBrain() {
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/partition/split-brain`, { method: "POST" }),
        "Creating split-brain partition..."
    );
}

async function runSplitBrainHealScenario() {
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/scenarios/split-brain-heal?isolate_seconds=6&verify_polls=2`, {
            method: "POST",
        }),
        "Running split-brain then heal scenario..."
    );
}

async function runBootstrapConvergeScenario() {
    return runActionWithLoading(
        () => apiAction(`${API_BASE}/scenarios/bootstrap-converge?create_nodes=1&events_per_node=2&verify_polls=3`, {
            method: "POST",
        }),
        "Running bootstrap & convergence scenario..."
    );
}

function fetchLogs() {
    fetchData();
}

// --- Rendering ---

function renderTopology() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    const centerX = canvas.width / 2;
    const centerY = canvas.height / 2;
    const radius = Math.min(centerX, centerY) - 60;

    // Calculate positions
    const angleStep = (2 * Math.PI) / nodes.length;
    nodes.forEach((node, i) => {
        node.x = centerX + radius * Math.cos(i * angleStep - Math.PI / 2);
        node.y = centerY + radius * Math.sin(i * angleStep - Math.PI / 2);
    });

    // Draw connections (mesh = everyone connected to everyone)
    for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
            const n1 = nodes[i];
            const n2 = nodes[j];

            // Color based on status
            // If EITHER node is isolated, draw red line
            const isPartitioned = n1.isolated || n2.isolated;

            ctx.beginPath();
            ctx.moveTo(n1.x, n1.y);
            ctx.lineTo(n2.x, n2.y);
            ctx.strokeStyle = isPartitioned ? "rgba(248, 81, 73, 0.4)" : "rgba(56, 139, 253, 0.2)";
            ctx.lineWidth = isPartitioned ? 1 : 2;
            ctx.stroke();
        }
    }

    // Draw nodes
    nodes.forEach(node => {
        ctx.beginPath();
        ctx.arc(node.x, node.y, 25, 0, 2 * Math.PI);
        ctx.fillStyle = "#161b22";
        ctx.fill();
        ctx.lineWidth = 3;

        // Border color: Orange if isolated, Green if running
        const borderColor = node.isolated ? "#d29922" : (node.status === "running" ? "#3fb950" : "#8b949e");
        ctx.strokeStyle = borderColor;
        ctx.stroke();

        // Label
        ctx.fillStyle = "#c9d1d9";
        ctx.font = "12px Inter";
        ctx.textAlign = "center";
        ctx.fillText(node.name.replace("edge-", ""), node.x, node.y + 40);

        // ID inside
        ctx.fillStyle = "#8b949e";
        ctx.font = "10px JetBrains Mono";
        ctx.fillText(node.id.substring(0, 4), node.x, node.y + 4);
    });
}

function renderNodeList() {
    const list = document.getElementById("nodeList");
    list.innerHTML = "";

    nodes.forEach(node => {
        const item = document.createElement("div");
        const isDynamic = node.managed; // created via API
        const nodeId = node.name.replace("edge-", "");

        item.className = `node-item ${node.isolated ? 'isolated' : ''}`;
        item.innerHTML = `
            <div class="node-info">
                <span class="node-name">${node.name} ${node.isolated ? '(ISOLATED)' : ''}</span>
                <span class="node-ip">${node.id} • ${node.status} • ${node.url || 'n/a'}</span>
            </div>
            <div class="actions">
                ${!node.isolated ?
                `<button class="btn btn-sm btn-warning" onclick="isolateNode('${nodeId}')">Isolate</button>` :
                `<button class="btn btn-sm btn-success" onclick="healNode('${nodeId}')">Heal</button>`
            }
                ${isDynamic ? `<button class="btn btn-sm btn-danger" onclick="removeNode('${nodeId}')">×</button>` : ''}
            </div>
        `;
        list.appendChild(item);
    });
}

function renderRuntimeMetrics() {
    document.getElementById("metric-polls").innerText = runtimeMetrics.polls_completed ?? 0;
    document.getElementById("metric-http-success").innerText = runtimeMetrics.total_http_success ?? 0;
    document.getElementById("metric-http-failures").innerText = runtimeMetrics.total_http_failures ?? 0;
    document.getElementById("metric-convergence").innerText = runtimeMetrics.total_convergence_events ?? 0;
    document.getElementById("metric-div-duration").innerText = `${runtimeMetrics.divergence_duration_seconds ?? 0}s`;
    document.getElementById("metric-reachable").innerText = runtimeMetrics.last_reachable_nodes ?? 0;
}

function renderActionResult() {
    const panel = document.getElementById("actionResult");
    if (!panel) return;

    if (!lastActionResult) {
        panel.innerText = "No actions yet.";
        return;
    }

    panel.innerText = summarizeAction(lastActionResult);
}

function renderLogs() {
    const tbody = document.getElementById("logBody");
    tbody.innerHTML = "";

    logs.slice(0, 10).forEach(entry => {
        const tr = document.createElement("tr");
        const date = new Date(entry.timestamp).toLocaleTimeString();
        tr.innerHTML = `
            <td>${date}</td>
            <td class="${entry.is_divergent ? 'log-row-divergent' : 'log-row-synced'}">
                ${entry.is_divergent ? 'DIVERGENT' : 'SYNCED'}
            </td>
            <td>
                <pre style="margin:0; font-size:10px;">${JSON.stringify(entry.merkle_roots, null, 2)}</pre>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

init();
