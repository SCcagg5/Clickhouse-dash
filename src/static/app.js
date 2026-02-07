(() => {
  "use strict";

  const queryTextAreaElement = document.getElementById("queryTextArea");
  const runButtonElement = document.getElementById("runButton");
  const cancelButtonElement = document.getElementById("cancelButton");

  const queryIdentifierTextElement = document.getElementById("queryIdentifierText");
  const queryStatusTextElement = document.getElementById("queryStatusText");

  const elapsedSecondsTextElement = document.getElementById("elapsedSecondsText");
  const progressPercentTextElement = document.getElementById("progressPercentText");
  const readRowsTextElement = document.getElementById("readRowsText");
  const readBytesTextElement = document.getElementById("readBytesText");
  const rowsPerSecondTextElement = document.getElementById("rowsPerSecondText");
  const bytesPerSecondTextElement = document.getElementById("bytesPerSecondText");
  const memoryTextElement = document.getElementById("memoryText");
  const centralProcessingUnitTextElement = document.getElementById("centralProcessingUnitText");
  const threadCountTextElement = document.getElementById("threadCountText");

  const logsContainerElement = document.getElementById("logsContainer");

  let activeQueryIdentifier = null;
  let activeEventSource = null;

  function setQueryStatus(statusText) {
    queryStatusTextElement.textContent = statusText;
  }

  function setQueryIdentifier(queryIdentifier) {
    queryIdentifierTextElement.textContent = queryIdentifier || "-";
  }

  function clearMetrics() {
    elapsedSecondsTextElement.textContent = "-";
    progressPercentTextElement.textContent = "-";
    readRowsTextElement.textContent = "-";
    readBytesTextElement.textContent = "-";
    rowsPerSecondTextElement.textContent = "-";
    bytesPerSecondTextElement.textContent = "-";
    memoryTextElement.textContent = "-";
    centralProcessingUnitTextElement.textContent = "-";
    threadCountTextElement.textContent = "-";
  }

  function clearLogs() {
    logsContainerElement.innerHTML = "";
  }

  function appendLogLine(lineText) {
    const logLineElement = document.createElement("div");
    logLineElement.className = "logLine";
    logLineElement.textContent = lineText;
    logsContainerElement.appendChild(logLineElement);
    logsContainerElement.scrollTop = logsContainerElement.scrollHeight;
  }

  function formatNumber(value) {
    if (value === null || value === undefined) return "-";
    if (!Number.isFinite(value)) return "-";
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }

  function formatSeconds(value) {
    if (value === null || value === undefined) return "-";
    if (!Number.isFinite(value)) return "-";
    return `${value.toFixed(2)} s`;
  }

  function formatBytes(value) {
    if (value === null || value === undefined) return "-";
    if (!Number.isFinite(value)) return "-";
    const absoluteValue = Math.max(0, value);
    const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let unitIndex = 0;
    let scaledValue = absoluteValue;
    while (scaledValue >= 1024 && unitIndex < units.length - 1) {
      scaledValue = scaledValue / 1024;
      unitIndex++;
    }
    if (unitIndex === 0) {
      return `${scaledValue.toFixed(0)} ${units[unitIndex]}`;
    }
    return `${scaledValue.toFixed(2)} ${units[unitIndex]}`;
  }

  function closeActiveStream() {
    if (activeEventSource) {
      activeEventSource.close();
      activeEventSource = null;
    }
  }

  async function createQuery(queryText) {
    const response = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql: queryText })
    });

    const responseBody = await response.json().catch(() => ({}));
    if (!response.ok) {
      const messageText = responseBody && responseBody.message ? responseBody.message : `Request failed with status ${response.status}`;
      throw new Error(messageText);
    }
    return responseBody;
  }

  async function requestCancellation(queryIdentifier) {
    const response = await fetch("/api/query/cancel", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query_id: queryIdentifier })
    });

    const responseBody = await response.json().catch(() => ({}));
    if (!response.ok) {
      const messageText = responseBody && responseBody.message ? responseBody.message : `Cancel failed with status ${response.status}`;
      throw new Error(messageText);
    }
    return responseBody;
  }

  function startStream(streamUrl) {
    closeActiveStream();

    const eventSource = new EventSource(streamUrl);
    activeEventSource = eventSource;

    eventSource.addEventListener("meta", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;
      setQueryStatus("running");
      cancelButtonElement.disabled = false;
      appendLogLine(`connected: query_id=${payload.query_id}`);
    });

    eventSource.addEventListener("progress", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;

      elapsedSecondsTextElement.textContent = formatSeconds(payload.elapsed_seconds);

      if (payload.percent_known) {
        progressPercentTextElement.textContent = `${formatNumber(payload.percent)} %`;
      } else {
        progressPercentTextElement.textContent = "indeterminate";
      }

      readRowsTextElement.textContent = formatNumber(payload.read_rows);
      readBytesTextElement.textContent = formatBytes(payload.read_bytes);

      // Prefer total rates (stable). We also have instant rates, but we keep UI compact.
      rowsPerSecondTextElement.textContent = `${formatNumber(payload.rows_per_second)} rows/s`;
      bytesPerSecondTextElement.textContent = `${formatBytes(payload.bytes_per_second)}/s`;
    });

    eventSource.addEventListener("resource", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;

      const currentMemoryText = payload.memory_current_bytes === null ? "-" : formatBytes(payload.memory_current_bytes);
      const peakMemoryText = payload.memory_peak_bytes === null ? "-" : formatBytes(payload.memory_peak_bytes);
      memoryTextElement.textContent = `${currentMemoryText} / ${peakMemoryText}`;

      centralProcessingUnitTextElement.textContent = `${formatNumber(payload.central_processing_unit_core_percent_total)} % / ${formatNumber(payload.central_processing_unit_core_percent_instant)} %`;

      threadCountTextElement.textContent = `${formatNumber(payload.thread_count_current)} / ${formatNumber(payload.thread_count_peak)}`;
    });

    eventSource.addEventListener("log", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;
      appendLogLine(payload.line);
    });

    eventSource.addEventListener("error", (event) => {
      // Some browsers also trigger "error" for connection issues. We only show JSON if present.
      const payload = safelyParseJson(event.data);
      if (payload) {
        appendLogLine(`error: ${payload.message}`);
      } else {
        appendLogLine("error: stream connection issue");
      }
      setQueryStatus("error");
    });

    eventSource.addEventListener("done", (event) => {
      const payload = safelyParseJson(event.data);
      if (payload) {
        appendLogLine(`done: status=${payload.status} elapsed=${formatSeconds(payload.elapsed_seconds)}`);
        setQueryStatus(payload.status);
      } else {
        appendLogLine("done");
        setQueryStatus("done");
      }
      cancelButtonElement.disabled = true;
      closeActiveStream();
    });

    eventSource.onopen = () => {
      // No-op: we use the meta event for "connected".
    };

    eventSource.onerror = () => {
      // Keep the connection; the server may still be alive. The explicit error SSE event will close on done.
    };
  }

  function safelyParseJson(text) {
    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  }

  async function handleRun() {
    const queryText = (queryTextAreaElement.value || "").trim();
    if (!queryText) {
      appendLogLine("Please write a query first.");
      return;
    }

    clearLogs();
    clearMetrics();
    setQueryStatus("starting...");
    cancelButtonElement.disabled = true;
    runButtonElement.disabled = true;

    try {
      const responsePayload = await createQuery(queryText);

      activeQueryIdentifier = responsePayload.query_id;
      setQueryIdentifier(activeQueryIdentifier);

      setQueryStatus("connecting...");
      startStream(responsePayload.stream_url);
    } catch (error) {
      setQueryStatus("error");
      appendLogLine(`error: ${error.message || String(error)}`);
    } finally {
      runButtonElement.disabled = false;
    }
  }

  async function handleCancel() {
    if (!activeQueryIdentifier) return;

    cancelButtonElement.disabled = true;
    setQueryStatus("canceling...");

    try {
      await requestCancellation(activeQueryIdentifier);
      appendLogLine("cancel requested");
    } catch (error) {
      appendLogLine(`cancel error: ${error.message || String(error)}`);
      cancelButtonElement.disabled = false;
    }
  }

  runButtonElement.addEventListener("click", () => {
    void handleRun();
  });

  cancelButtonElement.addEventListener("click", () => {
    void handleCancel();
  });

  // A reasonable default query for first run.
  queryTextAreaElement.value = queryTextAreaElement.value || "SELECT count() AS rows FROM system.numbers LIMIT 100000000";
})();
