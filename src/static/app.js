(() => {
  "use strict";

  const queryTextAreaElement = document.getElementById("queryTextArea");
  const runButtonElement = document.getElementById("runButton");
  const cancelButtonElement = document.getElementById("cancelButton");
  const clearButtonElement = document.getElementById("clearButton");
  const themeSelectElement = document.getElementById("themeSelect");

  const queryIdentifierTextElement = document.getElementById("queryIdentifierText");
  const queryStatusTextElement = document.getElementById("queryStatusText");
  const resultColumnsTextElement = document.getElementById("resultColumnsText");
  const errorBannerElement = document.getElementById("errorBanner");

  const elapsedSecondsTextElement = document.getElementById("elapsedSecondsText");
  const progressPercentTextElement = document.getElementById("progressPercentText");
  const readCountersTextElement = document.getElementById("readCountersText");
  const readRateTextElement = document.getElementById("readRateText");

  const cpuTextElement = document.getElementById("cpuText");
  const cpuMaxTextElement = document.getElementById("cpuMaxText");
  const memoryTextElement = document.getElementById("memoryText");
  const memoryMaxTextElement = document.getElementById("memoryMaxText");
  const threadTextElement = document.getElementById("threadText");
  const threadMaxTextElement = document.getElementById("threadMaxText");

  const resultTableHeadElement = document.getElementById("resultTableHead");
  const resultTableBodyElement = document.getElementById("resultTableBody");

  const THEME_STORAGE_KEY = "chdash.theme";

  function applyTheme(mode) {
    const root = document.documentElement;
    if (mode === "light" || mode === "dark") {
      root.setAttribute("data-theme", mode);
    } else {
      root.removeAttribute("data-theme");
    }
  }

  function getSavedThemeMode() {
    const v = localStorage.getItem(THEME_STORAGE_KEY);
    if (v === "light" || v === "dark" || v === "system") return v;
    return "system";
  }

  function setSavedThemeMode(mode) {
    localStorage.setItem(THEME_STORAGE_KEY, mode);
  }

  let currentThemeMode = getSavedThemeMode();
  applyTheme(currentThemeMode);

  if (themeSelectElement) {
    themeSelectElement.value = currentThemeMode;
    themeSelectElement.addEventListener("change", () => {
      currentThemeMode = themeSelectElement.value;
      setSavedThemeMode(currentThemeMode);
      applyTheme(currentThemeMode);
    });
  }

  const themeMedia = window.matchMedia ? window.matchMedia("(prefers-color-scheme: light)") : null;
  if (themeMedia && typeof themeMedia.addEventListener === "function") {
    themeMedia.addEventListener("change", () => {
      if (currentThemeMode === "system") applyTheme("system");
    });
  } else if (themeMedia && typeof themeMedia.addListener === "function") {
    themeMedia.addListener(() => {
      if (currentThemeMode === "system") applyTheme("system");
    });
  }

  let activeQueryIdentifier = null;
  let activeEventSource = null;

  let resultColumns = [];
  let pendingRows = [];
  let scheduledFlush = false;

  const flushBatchSize = 400;

  function setStatus(text) {
    queryStatusTextElement.textContent = text;
  }

  function setQueryIdentifier(text) {
    queryIdentifierTextElement.textContent = text || "-";
  }

  function setError(message) {
    if (!message) {
      errorBannerElement.hidden = true;
      errorBannerElement.textContent = "";
      return;
    }
    errorBannerElement.hidden = false;
    errorBannerElement.textContent = message;
  }

  function safelyParseJson(text) {
    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  }

  function formatCompactNumber(value) {
  if (value === null || value === undefined) return "-";
  if (!Number.isFinite(value)) return "-";

  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";

  const units = ["", "k", "M", "B", "T", "P"];
  let unitIndex = 0;
  let scaled = abs;

  while (scaled >= 1000 && unitIndex < units.length - 1) {
    scaled /= 1000;
    unitIndex++;
  }

  const formatted =scaled.toFixed(2).replace(/\.0$/, "");

  return `${sign}${formatted} ${units[unitIndex]}`.trim();
}


  function formatNumber(value) {
    if (value === null || value === undefined) return "-";
    if (!Number.isFinite(value)) return "-";
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }

  function formatSeconds(value) {
    if (value === null || value === undefined) return "-";
    if (!Number.isFinite(value)) return "-";
    return `${value.toFixed(3)} s`;
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
    if (unitIndex === 0) return `${scaledValue.toFixed(0)} ${units[unitIndex]}`;
    return `${scaledValue.toFixed(2)} ${units[unitIndex]}`;
  }

  function closeActiveStream() {
    if (activeEventSource) {
      activeEventSource.close();
      activeEventSource = null;
    }
  }

  function clearMetrics() {
    elapsedSecondsTextElement.textContent = "-";
    progressPercentTextElement.textContent = "-";
    readCountersTextElement.textContent = "-";
    readRateTextElement.textContent = "-";

    cpuTextElement.textContent = "-";
    cpuMaxTextElement.textContent = "-";
    memoryTextElement.textContent = "-";
    memoryMaxTextElement.textContent = "-";
    threadTextElement.textContent = "-";
    threadMaxTextElement.textContent = "-";

    setError("");
  }

  function clearResults() {
    resultColumns = [];
    pendingRows = [];
    scheduledFlush = false;

    resultTableHeadElement.innerHTML = "";
    resultTableBodyElement.innerHTML = "";
    resultColumnsTextElement.textContent = "-";
  }

  function setResultMeta(columns) {
    resultColumns = Array.isArray(columns) ? columns : [];

    const headRow = document.createElement("tr");
    for (const columnName of resultColumns) {
      const th = document.createElement("th");
      th.textContent = String(columnName ?? "");
      headRow.appendChild(th);
    }

    resultTableHeadElement.innerHTML = "";
    resultTableHeadElement.appendChild(headRow);

    resultColumnsTextElement.textContent = `${resultColumns.length} column(s)`;
  }

  function enqueueRowForRender(row) {
    pendingRows.push(row);
    scheduleFlush();
  }

  function scheduleFlush() {
    if (scheduledFlush) return;
    scheduledFlush = true;
    requestAnimationFrame(flushPendingRows);
  }

  function flushPendingRows() {
    scheduledFlush = false;
    if (pendingRows.length === 0) return;

    const fragment = document.createDocumentFragment();
    const toRender = Math.min(flushBatchSize, pendingRows.length);

    for (let i = 0; i < toRender; i++) {
      const row = pendingRows.shift();
      const tr = document.createElement("tr");

      if (Array.isArray(row)) {
        for (let columnIndex = 0; columnIndex < resultColumns.length; columnIndex++) {
          const td = document.createElement("td");
          const value = row[columnIndex] === undefined || row[columnIndex] === null ? "" : String(row[columnIndex]);
          td.textContent = value;
          tr.appendChild(td);
        }
      } else {
        const td = document.createElement("td");
        td.textContent = String(row);
        tr.appendChild(td);
      }

      fragment.appendChild(tr);
    }

    resultTableBodyElement.appendChild(fragment);

    if (pendingRows.length > 0) {
      scheduleFlush();
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

  function updateProgress(payload) {
    elapsedSecondsTextElement.textContent = formatSeconds(payload.elapsed_seconds);

    if (payload.percent_known) {
      progressPercentTextElement.textContent = `${formatNumber(payload.percent)} %`;
    } else {
      progressPercentTextElement.textContent = "indeterminate";
    }

    readCountersTextElement.textContent =
      `${formatCompactNumber(payload.read_rows)} rows · ${formatBytes(payload.read_bytes)}`;
  }

  function updateResource(payload) {
    const rowsPerSecondInst = payload.rows_per_second_inst;
    const bytesPerSecondInst = payload.bytes_per_second_inst;
    readRateTextElement.textContent =
      `${formatCompactNumber(rowsPerSecondInst)} rows/s · ${formatBytes(bytesPerSecondInst)}/s`;

    const cpuInst = payload.cpu_percent_inst;
    const cpuInstMax = payload.cpu_percent_inst_max;
    cpuTextElement.textContent = `${formatNumber(cpuInst)}%`;
    cpuMaxTextElement.textContent = `max: ${formatNumber(cpuInstMax)}%`;

    const memInst = payload.memory_bytes_inst;
    const memMax = payload.memory_bytes_inst_max;
    memoryTextElement.textContent = memInst === null ? "-" : formatBytes(memInst);
    memoryMaxTextElement.textContent = `max: ${memMax === null ? "-" : formatBytes(memMax)}`;

    const tInst = payload.thread_count_inst;
    const tMax = payload.thread_count_inst_max;
    threadTextElement.textContent = `${formatNumber(tInst)}`;
    threadMaxTextElement.textContent = `max: ${formatNumber(tMax)}`;
  }

  function startStream(streamUrl) {
    closeActiveStream();

    const eventSource = new EventSource(streamUrl);
    activeEventSource = eventSource;

    eventSource.addEventListener("meta", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;
      setStatus("running");
      cancelButtonElement.disabled = false;
      setError("");
    });

    eventSource.addEventListener("progress", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;
      updateProgress(payload);
    });

    eventSource.addEventListener("resource", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;
      updateResource(payload);
    });

    eventSource.addEventListener("result_meta", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;

      clearResults();
      setResultMeta(payload.columns);
    });

    eventSource.addEventListener("result_rows", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;

      const rows = Array.isArray(payload.rows) ? payload.rows : [];
      for (const row of rows) {
        enqueueRowForRender(row);
      }
    });

    eventSource.addEventListener("error", (event) => {
      const payload = safelyParseJson(event.data);
      if (payload && payload.message) {
        setError(payload.message);
        setStatus("error");
      }
    });

    eventSource.addEventListener("done", (event) => {
      const payload = safelyParseJson(event.data);
      if (payload) {
        setStatus(String(payload.status || "done"));
        elapsedSecondsTextElement.textContent = formatSeconds(payload.elapsed_seconds);
        if (payload.message) setError(payload.message);
      } else {
        setStatus("done");
      }

      cancelButtonElement.disabled = true;
      closeActiveStream();
    });

    eventSource.addEventListener("keepalive", () => {});
    eventSource.onerror = () => {};
  }

  async function handleRun() {
    const queryText = (queryTextAreaElement.value || "").trim();
    if (!queryText) {
      setError("Please write a query first.");
      return;
    }

    setError("");
    clearMetrics();
    clearResults();

    setStatus("starting…");
    cancelButtonElement.disabled = true;
    runButtonElement.disabled = true;

    try {
      const responsePayload = await createQuery(queryText);
      activeQueryIdentifier = responsePayload.query_id;
      setQueryIdentifier(activeQueryIdentifier);

      setStatus("connecting…");
      startStream(responsePayload.stream_url);
    } catch (error) {
      setStatus("error");
      setError(error && error.message ? error.message : String(error));
      cancelButtonElement.disabled = true;
      closeActiveStream();
    } finally {
      runButtonElement.disabled = false;
    }
  }

  async function handleCancel() {
    if (!activeQueryIdentifier) return;

    cancelButtonElement.disabled = true;
    setStatus("canceling…");

    try {
      await requestCancellation(activeQueryIdentifier);
    } catch (error) {
      setError(error && error.message ? error.message : String(error));
      cancelButtonElement.disabled = false;
    }
  }

  function handleClear() {
    closeActiveStream();
    activeQueryIdentifier = null;

    setQueryIdentifier("");
    setStatus("idle");

    clearMetrics();
    clearResults();

    cancelButtonElement.disabled = true;
    runButtonElement.disabled = false;
    setError("");
  }

  function loadDefaultQueryIfEmpty() {
    if ((queryTextAreaElement.value || "").trim() !== "") return;
    queryTextAreaElement.value = "SELECT toString(number) AS dd, 100000000 AS test FROM numbers(10000000000) LIMIT 100";
  }

  runButtonElement.addEventListener("click", handleRun);
  cancelButtonElement.addEventListener("click", handleCancel);
  clearButtonElement.addEventListener("click", handleClear);

  loadDefaultQueryIfEmpty();
})();
