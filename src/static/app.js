const API_BASE = window.location.origin;

// State
let nodes = [];
let logs = [];
let isDivergent = false;
let status = "SYNCED";

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
    try {
        const [nodesRes, statusRes, divRes, metricsRes] = await Promise.all([
            fetch(`${API_BASE}/nodes`).then(r => r.json()),
            fetch(`${API_BASE}/gateway/status`).then(r => r.json()),
            fetch(`${API_BASE}/gateway/divergence`).then(r => r.json()),
            fetch(`${API_BASE}/gateway/metrics?name=merge_time_ms&limit=1`).then(r => r.json())
        ]);

        nodes = nodesRes;
        isDivergent = divRes.is_divergent;
        logs = divRes.log;

        // Check convergence status
        status = isDivergent ? "DIVERGENT" : "SYNCED";

        // Update Stats Bar
        document.getElementById("stat-nodes").innerText = nodes.length;
        document.getElementById("stat-status").innerText = status;
        document.getElementById("stat-status").className = `value status-badge ${isDivergent ? 'status-divergent' : 'status-synced'}`;

        if (metricsRes.length > 0) {
            document.getElementById("stat-merge").innerText = metricsRes[0].value.toFixed(2) + "ms";
        }

        // Render UI
        renderTopology();
        renderNodeList();
        renderLogs();

    } catch (e) {
        console.error("Poll failed", e);
    }
}

async function createNode() {
    await fetch(`${API_BASE}/nodes`, { method: "POST" });
    fetchData(); // instant refresh
}

async function removeNode(id) {
    if (!confirm(`Delete ${id}?`)) return;
    await fetch(`${API_BASE}/nodes/${id}`, { method: "DELETE" });
    fetchData();
}

async function isolateNode(id) {
    await fetch(`${API_BASE}/nodes/${id}/partition`, { method: "POST" });
    fetchData();
}

async function healNode(id) {
    await fetch(`${API_BASE}/nodes/${id}/partition`, { method: "DELETE" });
    fetchData();
}

async function healAll() {
    await fetch(`${API_BASE}/partition/heal-all`, { method: "POST" });
    fetchData();
}

async function splitBrain() {
    await fetch(`${API_BASE}/partition/split-brain`, { method: "POST" });
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
                <span class="node-ip">${node.id} • ${node.status}</span>
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
