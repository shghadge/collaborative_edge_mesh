(() => {
    "use strict";

    const STATUS_POLL_MS = 1000;

    const labels = {
        syncMode: {
            full_state: "Full state",
            optimized_full_state: "Optimized full state",
            naive_key_delta: "Naive key delta",
            true_delta: "True delta",
        },
        scenario: {
            baseline: "Baseline",
            partition_heal: "Partition and heal",
            high_load: "High load",
        },
        network: {
            clean_lan: "Clean LAN",
            constrained_wifi: "Constrained Wi-Fi",
            degraded: "Degraded",
            severe: "Severe",
        },
    };

    const elements = {
        form: document.getElementById("experiment-form"),
        fieldset: document.getElementById("config-fields"),
        runButton: document.getElementById("run-button"),
        runGuard: document.getElementById("run-guard"),
        formErrors: document.getElementById("form-errors"),
        caseId: document.getElementById("case-id"),
        nodeCount: document.getElementById("node-count"),
        repetitions: document.getElementById("repetitions"),
        scenarioSeed: document.getElementById("scenario-seed"),
        syncMode: document.getElementById("sync-mode"),
        scenario: document.getElementById("scenario"),
        networkProfile: document.getElementById("network-profile"),
        eventsPerNode: document.getElementById("events-per-node"),
        offeredRate: document.getElementById("offered-rate"),
        offeredRateField: document.getElementById("offered-rate-field"),
        partitionFields: document.getElementById("partition-fields"),
        partitionSplits: document.getElementById("partition-splits"),
        partitionDuration: document.getElementById("partition-duration"),
        partitionEvents: document.getElementById("partition-events"),
        eventConcurrency: document.getElementById("event-concurrency"),
        dockerCpus: document.getElementById("docker-cpus"),
        dockerMemory: document.getElementById("docker-memory"),
        resourceMode: document.getElementById("resource-mode"),
        resourceProfile: document.getElementById("resource-profile"),
        resourcePreview: document.getElementById("resource-preview"),
        resourcePreviewCounts: document.getElementById("resource-preview-counts"),
        resourcePreviewTotal: document.getElementById("resource-preview-total"),
        convergenceCutoff: document.getElementById("convergence-cutoff"),
        pollInterval: document.getElementById("poll-interval"),
        retentionField: document.getElementById("retention-field"),
        eventRetention: document.getElementById("event-retention"),
        retentionHelp: document.getElementById("retention-help"),
        deltaLogField: document.getElementById("delta-log-field"),
        deltaLogLimit: document.getElementById("delta-log-limit"),
        reviewNarrative: document.getElementById("review-narrative"),
        expectedRows: document.getElementById("expected-rows"),
        reviewList: document.getElementById("review-list"),
        preflightState: document.getElementById("preflight-state"),
        apiState: document.getElementById("api-state"),
        apiStateLabel: document.getElementById("api-state-label"),
        runState: document.getElementById("run-state"),
        activeRunId: document.getElementById("active-run-id"),
        currentStep: document.getElementById("current-step"),
        progressTrack: document.getElementById("progress-track"),
        progressBar: document.getElementById("progress-bar"),
        progressCount: document.getElementById("progress-count"),
        progressRepetition: document.getElementById("progress-repetition"),
        progressScenario: document.getElementById("progress-scenario"),
        progressResults: document.getElementById("progress-results"),
        runMessage: document.getElementById("run-message"),
        resultsPanel: document.getElementById("results-panel"),
        resultContext: document.getElementById("result-context"),
        metricGrid: document.getElementById("metric-grid"),
        rowCount: document.getElementById("row-count"),
        resultsBody: document.getElementById("results-body"),
        downloadCsv: document.getElementById("download-csv"),
    };

    const state = {
        running: false,
        pollTimer: null,
        runId: null,
        latestStatus: null,
    };

    function setApiState(mode, text) {
        elements.apiState.classList.remove("is-ready", "is-running", "is-error");
        if (mode) elements.apiState.classList.add(`is-${mode}`);
        elements.apiStateLabel.textContent = text;
    }


    function updateConditionals(changedControl = null) {
        const partitionSelected = elements.scenario.value === "partition_heal";
        const fullStateSelected = elements.syncMode.value === "full_state";
        const trueDeltaSelected = elements.syncMode.value === "true_delta";
        const heterogeneousResources = elements.resourceMode.value === "heterogeneous";

        elements.partitionFields.hidden = !partitionSelected;
        elements.partitionSplits.disabled = !partitionSelected;
        elements.partitionDuration.disabled = !partitionSelected;
        elements.partitionEvents.disabled = !partitionSelected;

        elements.offeredRateField.hidden = partitionSelected;
        elements.offeredRate.disabled = partitionSelected;

        if (fullStateSelected) {
            if (elements.eventRetention.value !== "0") {
                elements.eventRetention.dataset.retainedValue = elements.eventRetention.value;
            }
            elements.eventRetention.value = "0";
            elements.eventRetention.disabled = true;
            elements.retentionHelp.textContent = "Full state keeps the complete event history.";
        } else {
            if (elements.eventRetention.disabled) {
                elements.eventRetention.value = elements.eventRetention.dataset.retainedValue || "100";
            }
            elements.eventRetention.disabled = false;
            elements.retentionHelp.textContent =
                elements.syncMode.value === "optimized_full_state"
                    ? "Optimized full state needs a value above 0."
                    : "Use 0 to keep all event IDs.";
        }

        elements.deltaLogField.hidden = !trueDeltaSelected;
        elements.deltaLogLimit.disabled = !trueDeltaSelected;

        if (changedControl === elements.resourceMode) {
            if (heterogeneousResources) {
                elements.dockerCpus.dataset.uniformValue = elements.dockerCpus.value;
                elements.dockerMemory.dataset.uniformValue = elements.dockerMemory.value;
                elements.dockerCpus.value = "0.5";
                elements.dockerMemory.value = "512m";
            } else {
                elements.dockerCpus.value = elements.dockerCpus.dataset.uniformValue || "1";
                elements.dockerMemory.value = elements.dockerMemory.dataset.uniformValue || "512m";
            }
        }
        elements.resourceProfile.disabled = !heterogeneousResources;
        elements.dockerCpus.disabled = heterogeneousResources;
        elements.dockerMemory.disabled = heterogeneousResources;
        elements.resourcePreview.hidden = !heterogeneousResources;
        if (heterogeneousResources) {
            const nodeCount = Number(elements.nodeCount.value);
            if (Number.isSafeInteger(nodeCount) && nodeCount > 0 && nodeCount % 5 === 0) {
                const blocks = nodeCount / 5;
                elements.resourcePreviewCounts.textContent =
                    `${blocks * 2} low · ${blocks * 2} medium · ${blocks} high`;
                elements.resourcePreviewTotal.textContent =
                    `Total: ${nodeCount * 0.5} CPU · ${nodeCount * 512} MiB`;
            } else {
                elements.resourcePreviewCounts.textContent = "Use 5, 10, 15, or another multiple of 5";
                elements.resourcePreviewTotal.textContent = "Each five-node block uses 2.5 CPU · 2560 MiB";
            }
        }
    }

    function clearInvalidStates() {
        elements.form.querySelectorAll('[aria-invalid="true"]').forEach((control) => {
            control.removeAttribute("aria-invalid");
        });
    }

    function collectConfiguration() {
        clearInvalidStates();
        const errors = [];

        const addError = (control, message) => {
            errors.push({ control, message });
            control?.setAttribute("aria-invalid", "true");
        };

        const requiredInteger = (control, label, minimum = null) => {
            if (control.value.trim() === "") {
                addError(control, `${label} is required.`);
                return null;
            }
            const value = Number(control.value);
            if (!Number.isSafeInteger(value)) {
                addError(control, `${label} must be an integer.`);
                return null;
            }
            if (minimum !== null && value < minimum) {
                addError(control, `${label} must be ${minimum} or greater.`);
                return null;
            }
            return value;
        };

        const positiveNumber = (control, label) => {
            const value = Number(control.value);
            if (!Number.isFinite(value) || value <= 0) {
                addError(control, `${label} must be greater than 0.`);
                return null;
            }
            return value;
        };

        const optionalPositiveNumber = (control, label) => {
            if (control.value.trim() === "") return null;
            return positiveNumber(control, label);
        };

        const caseId = elements.caseId.value.trim();
        if (!caseId) addError(elements.caseId, "Case name is required.");

        const nodeCount = requiredInteger(elements.nodeCount, "Nodes", 1);
        const resourceMode = elements.resourceMode.value;
        const resourceProfile =
            resourceMode === "heterogeneous" ? elements.resourceProfile.value : null;
        if (
            resourceMode === "heterogeneous" &&
            nodeCount !== null &&
            nodeCount % 5 !== 0
        ) {
            addError(
                elements.nodeCount,
                "Mixed edge devices require 5, 10, 15, or another multiple of 5 nodes.",
            );
        }
        const repetitions = requiredInteger(elements.repetitions, "Runs", 1);
        const scenarioSeed = requiredInteger(elements.scenarioSeed, "First seed");
        const eventsPerNode = requiredInteger(elements.eventsPerNode, "Events per node", 1);
        const eventConcurrency = requiredInteger(elements.eventConcurrency, "Concurrent requests", 1);
        const convergenceWait = positiveNumber(elements.convergenceCutoff, "Convergence timeout");
        const pollInterval = positiveNumber(elements.pollInterval, "Sample interval");
        const dockerCpus =
            resourceMode === "heterogeneous"
                ? 0.5
                : positiveNumber(elements.dockerCpus, "CPU limit");

        const dockerMemory =
            resourceMode === "heterogeneous"
                ? "512m"
                : elements.dockerMemory.value.trim().toLowerCase();
        const memoryPattern = /^(?:\d+(?:\.\d+)?|\.\d+)(?:[kmgt]i?b?|b)$/i;
        if (
            resourceMode === "uniform" &&
            (!memoryPattern.test(dockerMemory) || Number.parseFloat(dockerMemory) <= 0)
        ) {
            addError(elements.dockerMemory, "Memory must be a positive Docker size such as 512m or 1g.");
        }

        const syncMode = elements.syncMode.value;
        const scenario = elements.scenario.value;
        const networkProfile = elements.networkProfile.value;
        const isPartition = scenario === "partition_heal";

        let eventRetention = 0;
        if (syncMode !== "full_state") {
            eventRetention = requiredInteger(elements.eventRetention, "Event IDs kept", 0);
            if (syncMode === "optimized_full_state" && eventRetention !== null && eventRetention <= 0) {
                addError(elements.eventRetention, "Optimized full state needs an event ID value above 0.");
            }
        }

        let deltaLogLimit = 1000;
        if (syncMode === "true_delta") {
            deltaLogLimit = requiredInteger(elements.deltaLogLimit, "Delta records kept", 0);
        }

        const offeredRate = isPartition
            ? null
            : optionalPositiveNumber(elements.offeredRate, "Offered rate");

        let partitionSplits = [];
        let partitionDuration = null;
        let partitionEvents = null;
        if (isPartition) {
            const rawParts = elements.partitionSplits.value.split(",");
            if (!elements.partitionSplits.value.trim() || rawParts.some((part) => !part.trim())) {
                addError(elements.partitionSplits, "Enter one or more comma-separated partition splits.");
            } else {
                partitionSplits = rawParts.map((part) => Number(part.trim()));
                if (partitionSplits.some((split) => !Number.isFinite(split) || split <= 0 || split >= 1)) {
                    addError(elements.partitionSplits, "Every partition split must be a decimal greater than 0 and less than 1.");
                }
            }
            partitionDuration = positiveNumber(elements.partitionDuration, "Failure time");
            partitionEvents = requiredInteger(elements.partitionEvents, "Events per node during failure", 0);
        }

        const expectedRows =
            Number.isSafeInteger(repetitions) && repetitions > 0
                ? repetitions * (isPartition ? partitionSplits.length : 1)
                : 0;

        const payload = {
            case_id: caseId,
            node_counts: nodeCount === null ? [] : [nodeCount],
            sync_mode: syncMode,
            repetitions,
            scenario_seed: scenarioSeed,
            scenarios: [scenario],
            events_per_node: eventsPerNode,
            partition_splits: isPartition ? partitionSplits : [],
            network_profile: networkProfile,
            convergence_wait_seconds: convergenceWait,
            poll_interval_seconds: pollInterval,
            event_concurrency: eventConcurrency,
            event_id_retention_limit: syncMode === "full_state" ? 0 : eventRetention,
            delta_log_limit: deltaLogLimit,
            docker_cpus: dockerCpus,
            docker_memory: dockerMemory,
            resource_mode: resourceMode,
            resource_profile: resourceProfile,
        };

        if (!isPartition) payload.offered_rate_per_second = offeredRate;
        if (isPartition) {
            payload.partition_wait_seconds = partitionDuration;
            payload.partition_events_per_node = partitionEvents;
        }

        return {
            errors,
            expectedRows,
            isPartition,
            payload,
        };
    }

    function reviewItem(term, description) {
        const wrapper = document.createElement("div");
        const dt = document.createElement("dt");
        const dd = document.createElement("dd");
        dt.textContent = term;
        dd.textContent = description;
        wrapper.append(dt, dd);
        return wrapper;
    }

    function renderReview() {
        const config = collectConfiguration();
        const payload = config.payload;
        const nodeText = payload.node_counts[0] || "—";
        const repetitionText = payload.repetitions || "—";
        const eventText = payload.events_per_node || "—";
        const scenarioName = labels.scenario[payload.scenarios[0]] || payload.scenarios[0];
        const networkName = labels.network[payload.network_profile] || payload.network_profile;
        const syncName = labels.syncMode[payload.sync_mode] || payload.sync_mode;

        const workloadDescription = config.isPartition
            ? `${eventText} before + ${payload.partition_events_per_node ?? "—"} during fault per node`
            : payload.scenarios[0] === "high_load"
              ? `${eventText} base / ${Number(eventText) * 3 || "—"} executed per node`
              : `${eventText} per node`;

        elements.reviewNarrative.textContent =
            `${repetitionText} separate ${Number(repetitionText) === 1 ? "run" : "runs"} will test ` +
            `${scenarioName.toLowerCase()} across ${nodeText} ${Number(nodeText) === 1 ? "node" : "nodes"} ` +
            `using ${syncName.toLowerCase()} sync on ${networkName}.`;
        elements.expectedRows.textContent = String(config.expectedRows);

        const injection = config.isPartition
            ? "Unpaced"
            : payload.offered_rate_per_second
              ? `${payload.offered_rate_per_second} events/s`
              : `Unpaced · ${payload.event_concurrency ?? "—"} concurrent`;
        const retention =
            payload.sync_mode === "full_state"
                ? "All event IDs"
                : `${payload.event_id_retention_limit ?? "—"} event IDs`;
        const delta = payload.sync_mode === "true_delta" ? ` · ${payload.delta_log_limit} delta records` : "";
        const partition = `Split ${payload.partition_splits.join(", ")} · ${payload.partition_wait_seconds ?? "—"}s`;
        const resourceDescription =
            payload.resource_mode === "heterogeneous"
                ? `Mixed · balanced edge devices · ${Number(nodeText) * 0.5} CPU total · ${Number(nodeText) * 512} MiB total`
                : `Same per node · ${payload.docker_cpus ?? "—"} CPU · ${payload.docker_memory || "—"}`;

        const summaryItems = [
            reviewItem(
                "Batch",
                `${payload.case_id || "Not named"} · ${nodeText} ${Number(nodeText) === 1 ? "node" : "nodes"} · ${repetitionText} ${Number(repetitionText) === 1 ? "run" : "runs"}`,
            ),
            reviewItem("Test", `${syncName} · ${scenarioName} · ${networkName}`),
            reviewItem("Load", `${workloadDescription} · ${injection}`),
            reviewItem(
                "Resources",
                `${resourceDescription} · ${payload.convergence_wait_seconds ?? "—"}s timeout · ${payload.poll_interval_seconds ?? "—"}s sample`,
            ),
        ];
        if (config.isPartition) summaryItems.push(reviewItem("Failure", partition));
        if (payload.sync_mode !== "full_state") {
            summaryItems.push(reviewItem("Data", `${retention}${delta}`));
        }
        elements.reviewList.replaceChildren(...summaryItems);

        elements.preflightState.classList.toggle("has-errors", config.errors.length > 0);
        elements.preflightState.textContent = config.errors.length
            ? `${config.errors.length} settings ${config.errors.length === 1 ? "issue" : "issues"} to fix before running.`
            : `Ready · ${config.expectedRows} ${config.expectedRows === 1 ? "row" : "rows"} expected from this setup.`;

        return config;
    }

    function renderFormErrors(errors) {
        if (!errors.length) {
            elements.formErrors.hidden = true;
            elements.formErrors.replaceChildren();
            return;
        }

        const title = document.createElement("strong");
        title.textContent = "The evaluation did not start. Resolve these settings:";
        const list = document.createElement("ul");
        errors.forEach(({ message }) => {
            const item = document.createElement("li");
            item.textContent = message;
            list.appendChild(item);
        });
        elements.formErrors.replaceChildren(title, list);
        elements.formErrors.hidden = false;
    }

    function handleConfigurationChange(event) {
        updateConditionals(event.target);
        renderFormErrors([]);
        renderReview();
    }

    function setControlsLocked(locked) {
        state.running = locked;
        elements.fieldset.disabled = locked;
        elements.runButton.disabled = locked;
        elements.runButton.querySelector("span").textContent = locked ? "Run in progress" : "Run evaluation";
        elements.runGuard.textContent = locked
            ? "Settings are locked until this batch finishes."
            : "Settings lock while the batch is running.";
        if (!locked) updateConditionals();
    }

    async function request(method, path, body = null) {
        const options = {
            method,
            headers: { Accept: "application/json" },
        };
        if (body !== null) {
            options.headers["Content-Type"] = "application/json";
            options.body = JSON.stringify(body);
        }

        const response = await fetch(path, options);
        let data = null;
        try {
            data = await response.json();
        } catch (_error) {
            data = null;
        }

        if (!response.ok) {
            const error = new Error(
                data?.message || data?.detail?.message || `Request failed with status ${response.status}.`,
            );
            error.status = response.status;
            throw error;
        }
        return data;
    }

    function clearPollTimer() {
        if (state.pollTimer !== null) {
            window.clearTimeout(state.pollTimer);
            state.pollTimer = null;
        }
    }

    function schedulePoll(delay = STATUS_POLL_MS) {
        clearPollTimer();
        state.pollTimer = window.setTimeout(pollStatus, delay);
    }

    function resetResults() {
        elements.resultsPanel.hidden = true;
        elements.metricGrid.replaceChildren();
        elements.resultsBody.replaceChildren();
        elements.resultContext.textContent = "";
        elements.rowCount.textContent = "";
        setDownloadAvailable(false);
    }

    function setDownloadAvailable(available) {
        elements.downloadCsv.classList.toggle("is-disabled", !available);
        elements.downloadCsv.setAttribute("aria-disabled", String(!available));
        if (available) {
            elements.downloadCsv.href = "/evaluation/download";
        } else {
            elements.downloadCsv.removeAttribute("href");
        }
    }

    async function startEvaluation(event) {
        event.preventDefault();
        const config = renderReview();
        renderFormErrors(config.errors);

        if (config.errors.length) {
            config.errors[0].control?.focus();
            return;
        }

        try {
            const activeStatus = await request("GET", "/evaluation/status");
            if (activeStatus.running) {
                handleStatus(activeStatus, "Another console already started this batch. Attached to its progress.");
                schedulePoll();
                return;
            }

            resetResults();
            setControlsLocked(true);
            setApiState("running", "Evaluation running");
            elements.runState.className = "run-state running";
            elements.runState.textContent = "Starting";
            elements.runMessage.className = "run-message";
            elements.runMessage.textContent = "Preparing fresh containers for the batch.";

            const started = await request("POST", "/evaluation/run", config.payload);
            state.runId = started.run_id;
            handleStatus({
                running: true,
                run_id: started.run_id,
                current_step: "Starting",
                progress: 0,
                completed_steps: 0,
                total_steps: started.total_steps,
                current_repetition: 0,
                total_repetitions: started.repetitions,
                current_scenario: started.scenarios?.[0],
                results_count: 0,
                has_csv: false,
                error: null,
            });
            schedulePoll(350);
        } catch (error) {
            if (error.status === 409) {
                try {
                    const status = await request("GET", "/evaluation/status");
                    handleStatus(status, "Another evaluation started first. Attached to its progress.");
                    if (status.running) schedulePoll();
                    return;
                } catch (_statusError) {
                    // Fall through to the original conflict message.
                }
            }

            setControlsLocked(false);
            setApiState("error", "Runner unavailable");
            elements.runState.className = "run-state error";
            elements.runState.textContent = "Not started";
            elements.runMessage.className = "run-message is-error";
            elements.runMessage.textContent = error.message;
        }
    }

    function handleStatus(status, messageOverride = null) {
        state.latestStatus = status;
        state.runId = status.run_id || state.runId;

        const completed = Number(status.completed_steps ?? status.progress ?? 0);
        const total = Number(status.total_steps ?? 0);
        const percent = total > 0 ? Math.min(100, Math.max(0, (completed / total) * 100)) : 0;
        const runError = status.error ? String(status.error) : "";

        elements.activeRunId.textContent = status.run_id || "Not started";
        elements.currentStep.textContent = status.current_step || (status.running ? "Preparing" : "Waiting for a configuration");
        elements.progressBar.style.width = `${percent}%`;
        elements.progressTrack.setAttribute("aria-valuenow", String(Math.round(percent)));
        elements.progressCount.textContent = `${completed} / ${total}`;
        elements.progressRepetition.textContent = status.total_repetitions
            ? `${status.current_repetition || 0} / ${status.total_repetitions}`
            : "—";
        elements.progressScenario.textContent = status.current_scenario
            ? labels.scenario[status.current_scenario] || status.current_scenario
            : "—";
        elements.progressResults.textContent = String(status.results_count ?? 0);
        elements.runMessage.className = "run-message";

        if (status.running) {
            setControlsLocked(true);
            setApiState("running", "Evaluation running");
            elements.runState.className = "run-state running";
            elements.runState.textContent = "Running";
            elements.runMessage.textContent = messageOverride || "The runner is executing this condition. Configuration remains locked.";
            return;
        }

        setControlsLocked(false);
        if (runError) {
            setApiState("error", "Run failed");
            elements.runState.className = "run-state error";
            elements.runState.textContent = "Error";
            elements.runMessage.className = "run-message is-error";
            elements.runMessage.textContent = runError;
        } else if (status.has_csv) {
            setApiState("ready", "Results ready");
            elements.runState.className = "run-state complete";
            elements.runState.textContent = "Complete";
            elements.runMessage.textContent = messageOverride || "The batch completed and its CSV is ready.";
        } else {
            setApiState("ready", "Runner ready");
            elements.runState.className = "run-state idle";
            elements.runState.textContent = "Idle";
            elements.runMessage.textContent = messageOverride || "No batch is running.";
        }
    }

    async function pollStatus() {
        clearPollTimer();
        try {
            const status = await request("GET", "/evaluation/status");
            handleStatus(status);
            if (status.running) {
                schedulePoll();
            } else if (status.has_csv) {
                await loadResults();
            }
        } catch (error) {
            setApiState("error", "Status unavailable");
            elements.runMessage.className = "run-message is-error";
            elements.runMessage.textContent = `Progress check failed: ${error.message}`;
            if (state.running) schedulePoll(2500);
        }
    }

    function numericValues(rows, key) {
        return rows
            .map((row) => {
                if (row[key] === null || row[key] === undefined || row[key] === "") return null;
                const value = Number(row[key]);
                return Number.isFinite(value) ? value : null;
            })
            .filter((value) => value !== null);
    }

    function isTrue(value) {
        return value === true || value === 1 || String(value).toLowerCase() === "true";
    }

    function sum(values) {
        return values.reduce((total, value) => total + value, 0);
    }

    function percentile(values, proportion) {
        if (!values.length) return null;
        const sorted = [...values].sort((a, b) => a - b);
        if (sorted.length === 1) return sorted[0];
        const index = (sorted.length - 1) * proportion;
        const lower = Math.floor(index);
        const upper = Math.ceil(index);
        const weight = index - lower;
        return sorted[lower] * (1 - weight) + sorted[upper] * weight;
    }

    function formatDecimal(value, digits = 2) {
        if (value === null || !Number.isFinite(value)) return "—";
        return new Intl.NumberFormat(undefined, {
            maximumFractionDigits: digits,
            minimumFractionDigits: 0,
        }).format(value);
    }

    function formatInteger(value) {
        if (value === null || !Number.isFinite(value)) return "—";
        return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value);
    }

    function formatBytes(value) {
        if (value === null || !Number.isFinite(value)) return "—";
        const units = ["B", "KiB", "MiB", "GiB", "TiB"];
        let amount = Math.max(0, value);
        let unitIndex = 0;
        while (amount >= 1024 && unitIndex < units.length - 1) {
            amount /= 1024;
            unitIndex += 1;
        }
        return `${formatDecimal(amount, amount >= 100 ? 0 : amount >= 10 ? 1 : 2)} ${units[unitIndex]}`;
    }

    function metricCard(label, value, detail) {
        const card = document.createElement("article");
        card.className = "metric-card";
        const labelElement = document.createElement("span");
        labelElement.className = "metric-label";
        labelElement.textContent = label;
        const valueElement = document.createElement("strong");
        valueElement.className = "metric-value";
        valueElement.textContent = value;
        const detailElement = document.createElement("span");
        detailElement.className = "metric-detail";
        detailElement.textContent = detail;
        card.append(labelElement, valueElement, detailElement);
        return card;
    }

    function renderSummary(rows) {
        const rowTotal = rows.length;
        const timedConvergence = rows.filter((row) => isTrue(row.converged_within_timeout)).length;
        const semanticConvergence = rows.filter((row) => isTrue(row.semantic_state_converged)).length;

        const acceptedTracked = sum(numericValues(rows, "accepted_tracking_ids_count"));
        const fullyObserved = sum(numericValues(rows, "fully_observed_tracking_ids_count"));
        const coverageValues = numericValues(rows, "tracking_coverage_percent");
        const coverage = acceptedTracked > 0
            ? (fullyObserved / acceptedTracked) * 100
            : percentile(coverageValues, 0.5);
        const missingCount = sum(numericValues(rows, "missing_tracking_ids_count"));

        const lastNodeTimes = numericValues(rows, "time_to_last_node_s");
        const lastNodeMedian = percentile(lastNodeTimes, 0.5);
        const lastNodeQ1 = percentile(lastNodeTimes, 0.25);
        const lastNodeQ3 = percentile(lastNodeTimes, 0.75);

        const rowLatencyP95 = numericValues(rows, "write_latency_p95_ms");
        const latencyMedian = percentile(rowLatencyP95, 0.5);
        const latencyQ1 = percentile(rowLatencyP95, 0.25);
        const latencyQ3 = percentile(rowLatencyP95, 0.75);
        const syncByteValues = numericValues(rows, "sync_bytes_total");
        const syncBytesMedian = percentile(syncByteValues, 0.5);
        const syncBytesQ1 = percentile(syncByteValues, 0.25);
        const syncBytesQ3 = percentile(syncByteValues, 0.75);
        const bytesPerEventValues = numericValues(rows, "bytes_per_accepted_event");
        const bytesPerEventMedian = percentile(bytesPerEventValues, 0.5);
        const cpuPeakValues = numericValues(rows, "cpu_peak_percent");
        const memoryPeakValues = numericValues(rows, "memory_peak_mib");
        const cpuPeakMedian = percentile(cpuPeakValues, 0.5);
        const memoryPeakMedian = percentile(memoryPeakValues, 0.5);

        elements.metricGrid.replaceChildren(
            metricCard(
                "Convergence",
                `${timedConvergence} / ${rowTotal} timed`,
                `${semanticConvergence} / ${rowTotal} semantically exact`,
            ),
            metricCard(
                "Event coverage",
                coverage === null ? "—" : `${formatDecimal(coverage, 1)}%`,
                `${formatInteger(missingCount)} tracking IDs missing`,
            ),
            metricCard(
                "Time to last node",
                lastNodeMedian === null ? "—" : `${formatDecimal(lastNodeMedian, 3)} s`,
                lastNodeMedian === null
                    ? "No complete propagation samples"
                    : `IQR ${formatDecimal(lastNodeQ1, 3)}–${formatDecimal(lastNodeQ3, 3)} s`,
            ),
            metricCard(
                "Write latency p95",
                latencyMedian === null ? "—" : `${formatDecimal(latencyMedian, 2)} ms`,
                latencyMedian === null
                    ? "No write-latency samples"
                    : `Median per-run p95 · IQR ${formatDecimal(latencyQ1, 2)}–${formatDecimal(latencyQ3, 2)} ms`,
            ),
            metricCard(
                "Sync traffic",
                syncBytesMedian === null ? "—" : formatBytes(syncBytesMedian),
                syncBytesMedian === null
                    ? "No synchronization samples"
                    : `Median per run · IQR ${formatBytes(syncBytesQ1)}–${formatBytes(syncBytesQ3)}`,
            ),
            metricCard(
                "Traffic / event",
                bytesPerEventMedian === null ? "—" : formatBytes(bytesPerEventMedian),
                "Median per-run bytes per accepted event",
            ),
            metricCard(
                "CPU peak",
                cpuPeakMedian === null ? "—" : `${formatDecimal(cpuPeakMedian, 2)}%`,
                "Median of per-run node peaks",
            ),
            metricCard(
                "Memory peak",
                memoryPeakMedian === null ? "—" : `${formatDecimal(memoryPeakMedian, 2)} MiB`,
                "Median of per-run node peaks",
            ),
        );
    }

    function tableCell(value, className = "") {
        const cell = document.createElement("td");
        cell.textContent = value;
        if (className) cell.className = className;
        return cell;
    }

    function booleanCell(value) {
        const positive = isTrue(value);
        return tableCell(positive ? "Yes" : "No", positive ? "boolean-yes" : "boolean-no");
    }

    function renderRows(rows) {
        if (!rows.length) {
            const row = document.createElement("tr");
            const cell = tableCell("The completed batch did not return result rows.", "empty-row");
            cell.colSpan = 14;
            row.appendChild(cell);
            elements.resultsBody.replaceChildren(row);
            return;
        }

        const fragment = document.createDocumentFragment();
        rows.forEach((result) => {
            const row = document.createElement("tr");
            const timeToLast = result.time_to_last_node_s;
            row.append(
                tableCell(result.run_id || "—"),
                tableCell(result.case_id || "—"),
                tableCell(String(result.repetition ?? "—")),
                tableCell(labels.scenario[result.scenario] || result.scenario || "—"),
                tableCell(String(result.node_count ?? "—")),
                tableCell(formatInteger(Number(result.accepted_events))),
                tableCell(
                    result.tracking_coverage_percent === "" || result.tracking_coverage_percent == null
                        ? "—"
                        : `${formatDecimal(Number(result.tracking_coverage_percent), 1)}%`,
                ),
                booleanCell(result.converged_within_timeout),
                booleanCell(result.semantic_state_converged),
                tableCell(timeToLast === "" || timeToLast == null ? "—" : `${formatDecimal(Number(timeToLast), 3)} s`),
                tableCell(
                    result.write_latency_p95_ms === "" || result.write_latency_p95_ms == null
                        ? "—"
                        : `${formatDecimal(Number(result.write_latency_p95_ms), 2)} ms`,
                ),
                tableCell(formatBytes(Number(result.sync_bytes_total))),
                tableCell(
                    result.cpu_peak_percent === "" || result.cpu_peak_percent == null
                        ? "—"
                        : `${formatDecimal(Number(result.cpu_peak_percent), 2)}%`,
                ),
                tableCell(
                    result.memory_peak_mib === "" || result.memory_peak_mib == null
                        ? "—"
                        : `${formatDecimal(Number(result.memory_peak_mib), 2)} MiB`,
                ),
            );
            fragment.appendChild(row);
        });
        elements.resultsBody.replaceChildren(fragment);
    }

    async function loadResults() {
        try {
            const data = await request("GET", "/evaluation/results");
            const rows = Array.isArray(data.results) ? data.results : [];
            renderSummary(rows);
            renderRows(rows);
            elements.resultsPanel.hidden = false;
            elements.rowCount.textContent = `${rows.length} ${rows.length === 1 ? "row" : "rows"}`;

            const first = rows[0] || {};
            const caseLabel = first.case_id ? `Case ${first.case_id}` : "Unlabeled case";
            const runLabel = first.run_id || state.runId || "completed run";
            elements.resultContext.textContent = `${caseLabel} · ${runLabel}`;
            setDownloadAvailable(Boolean(data.has_csv));
            setApiState("ready", "Results ready");
        } catch (error) {
            setDownloadAvailable(false);
            setApiState("error", "Results unavailable");
            elements.runMessage.className = "run-message is-error";
            elements.runMessage.textContent = `The run completed, but results could not be loaded: ${error.message}`;
        }
    }

    async function restoreRunnerState() {
        try {
            const status = await request("GET", "/evaluation/status");
            handleStatus(status);
            if (status.running) {
                schedulePoll(350);
            } else if (status.has_csv) {
                await loadResults();
            }
        } catch (error) {
            setApiState("error", "Runner unavailable");
            elements.runMessage.className = "run-message is-error";
            elements.runMessage.textContent = `Runner status could not be loaded: ${error.message}`;
        }
    }

    elements.eventRetention.dataset.retainedValue = "100";
    elements.form.addEventListener("input", handleConfigurationChange);
    elements.form.addEventListener("change", handleConfigurationChange);
    elements.form.addEventListener("submit", startEvaluation);
    elements.downloadCsv.addEventListener("click", (event) => {
        if (elements.downloadCsv.getAttribute("aria-disabled") === "true") event.preventDefault();
    });

    updateConditionals();
    renderReview();
    resetResults();
    restoreRunnerState();
})();
