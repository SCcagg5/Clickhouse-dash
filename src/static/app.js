(() => {
  "use strict";

  // ----------------------------
  // DOM
  // ----------------------------
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

  // Support 2 variantes: progressCard (bg) OU progressBar (fill)
  const progressCardElement = document.getElementById("progressCard");
  const progressBarElement = document.getElementById("progressBar");
  const progressBarFillElement = document.getElementById("progressBarFill");

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

  const readChartCanvas = document.getElementById("readChart");
  const cpuChartCanvas = document.getElementById("cpuChart");
  const memoryChartCanvas = document.getElementById("memoryChart");
  const threadChartCanvas = document.getElementById("threadChart");

  const THEME_STORAGE_KEY = "chdash.theme";

  // ----------------------------
  // Helpers
  // ----------------------------
  function setText(el, value) {
    if (el) el.textContent = value;
  }

  function asFiniteNumber(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
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

    const formatted = unitIndex === 0 ? scaled.toFixed(0) : scaled.toFixed(2).replace(/\.0$/, "");
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
      scaledValue /= 1024;
      unitIndex++;
    }

    if (unitIndex === 0) return `${scaledValue.toFixed(0)} ${units[unitIndex]}`;
    return `${scaledValue.toFixed(2)} ${units[unitIndex]}`;
  }

  function getCssVar(name, fallback = "") {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name);
    return v && v.trim() ? v.trim() : fallback;
  }

  // ----------------------------
  // Theme
  // ----------------------------
  function applyTheme(mode) {
    const root = document.documentElement;
    if (mode === "light" || mode === "dark") root.setAttribute("data-theme", mode);
    else root.removeAttribute("data-theme");
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

  // ----------------------------
  // Progress visual (bar ou card)
  // ----------------------------
  function setProgressVisual(percentKnown, percent) {
    // progressCard (CSS var --p)
    if (progressCardElement) {
      if (!percentKnown) {
        progressCardElement.classList.add("is-indeterminate");
        progressCardElement.style.removeProperty("--p");
      } else {
        progressCardElement.classList.remove("is-indeterminate");
        const clamped = Math.max(0, Math.min(100, Number(percent) || 0));
        progressCardElement.style.setProperty("--p", String(clamped / 100));
      }
    }

    // progressBar (fill width)
    if (progressBarElement && progressBarFillElement) {
      if (!percentKnown) {
        progressBarElement.classList.add("progressBar--indeterminate");
        progressBarFillElement.style.width = "0%";
      } else {
        progressBarElement.classList.remove("progressBar--indeterminate");
        const clamped = Math.max(0, Math.min(100, Number(percent) || 0));
        progressBarFillElement.style.width = `${clamped}%`;
      }
    }
  }

  // ----------------------------
  // Charts (sparkline)
  // ----------------------------
  const series = {
    readBytes: [],
    cpu: [],
    memBytes: [],
    threads: [],
  };

  const MAX_POINTS = 240;
  const EPS_T = 1e-6;

  // Push monotone: garantit une série triée par t
  function pushPointMonotone(arr, t, v) {
    if (!Number.isFinite(t) || !Number.isFinite(v)) return;
    const n = arr.length;
    if (n === 0) {
      arr.push({ t, v });
      return;
    }
    const last = arr[n - 1];
    if (t < last.t - EPS_T) {
      // On refuse les retours arrière (cause principale des graph “bizarres”)
      return;
    }
    if (Math.abs(t - last.t) <= EPS_T) {
      // même timestamp → on remplace la valeur (dédup propre)
      last.v = v;
      return;
    }
    arr.push({ t, v });
    if (arr.length > MAX_POINTS) arr.splice(0, arr.length - MAX_POINTS);
  }

  function prepareCanvas(canvas) {
    if (!canvas) return null;
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;

    const w = Math.max(1, Math.floor(rect.width * dpr));
    const h = Math.max(1, Math.floor(rect.height * dpr));

    if (canvas.width !== w || canvas.height !== h) {
      canvas.width = w;
      canvas.height = h;
    }

    const ctx = canvas.getContext("2d");
    if (!ctx) return null;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    return { ctx, w, h };
  }

  function quantile(values, q) {
    if (!Array.isArray(values) || values.length === 0) return null;
    const sorted = values.slice().sort((a, b) => a - b);
    const idx = Math.max(0, Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * q)));
    return sorted[idx];
  }

  function computeAutoMax(points, opts) {
    // opts.autoMaxQuantile ex: 0.98 → ignore les spikes extrêmes (CPU 1073%...)
    const q = opts.autoMaxQuantile;
    if (!q || !Array.isArray(points) || points.length < 2) return null;
    const vals = [];
    for (const p of points) if (Number.isFinite(p.v)) vals.push(p.v);
    if (vals.length === 0) return null;
    const qv = quantile(vals, q);
    if (qv == null) return null;
    return qv * (opts.autoMaxPadFactor ?? 1.10);
  }

  function drawSparkline(canvas, points, opts = {}) {
    const prepared = prepareCanvas(canvas);
    if (!prepared) return;
    const { ctx, w, h } = prepared;

    ctx.clearRect(0, 0, w, h);

    const pad = Math.round(h * 0.10);
    const topReserved = Math.round(h * 0.46); // garde la zone texte tranquille
    const x0 = pad;
    const y0 = topReserved;
    const x1 = w - pad;
    const y1 = h - pad;

    const border = getCssVar("--border", "rgba(148,163,184,0.14)");
    ctx.globalAlpha = 1;
    ctx.strokeStyle = border;
    ctx.lineWidth = 1;
    ctx.strokeRect(Math.floor(x0) + 0.5, Math.floor(y0) + 0.5, Math.floor(x1 - x0), Math.floor(y1 - y0));

    if (!Array.isArray(points) || points.length < 2) return;

    // points sont monotones (grâce à pushPointMonotone)
    const tMin = points[0].t;
    const tMax = points[points.length - 1].t;
    const tSpan = Math.max(1e-6, tMax - tMin);

    const vMin = opts.min ?? 0;

    // Max: soit fixe (opts.max), soit auto (quantile), soit max brut
    let vMax = null;

    if (opts.max != null && Number.isFinite(opts.max)) {
      vMax = opts.max;
    } else {
      const auto = computeAutoMax(points, opts);
      if (auto != null && Number.isFinite(auto)) vMax = auto;
      else {
        let m = -Infinity;
        for (const p of points) if (Number.isFinite(p.v)) m = Math.max(m, p.v);
        vMax = Number.isFinite(m) ? m : (vMin + 1);
      }
    }

    const minMax = opts.minMax ?? null; // ex: CPU au moins 100
    if (minMax != null && Number.isFinite(minMax)) vMax = Math.max(vMax, minMax);

    if (!Number.isFinite(vMax) || vMax <= vMin) vMax = vMin + 1;

    const line = opts.lineColor || getCssVar("--accentBorder", "#2563eb");
    const fillAlpha = opts.fillAlpha ?? 0.12;

    function X(t) { return x0 + ((t - tMin) / tSpan) * (x1 - x0); }
    function Y(v) {
      const vv = opts.clampMax ? Math.min(v, vMax) : v;
      return y1 - ((vv - vMin) / (vMax - vMin)) * (y1 - y0);
    }

    // area fill
    ctx.beginPath();
    ctx.moveTo(X(points[0].t), y1);
    for (const p of points) ctx.lineTo(X(p.t), Y(p.v));
    ctx.lineTo(X(points[points.length - 1].t), y1);
    ctx.closePath();
    ctx.globalAlpha = fillAlpha;
    ctx.fillStyle = line;
    ctx.fill();

    // line
    ctx.beginPath();
    ctx.moveTo(X(points[0].t), Y(points[0].v));
    for (let i = 1; i < points.length; i++) ctx.lineTo(X(points[i].t), Y(points[i].v));
    ctx.globalAlpha = 0.90;
    ctx.strokeStyle = line;
    ctx.lineWidth = 2;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.stroke();

    // glow soft
    ctx.globalAlpha = 0.16;
    ctx.lineWidth = 6;
    ctx.stroke();
  }

  let chartsScheduled = false;
  function scheduleChartsRender() {
    if (chartsScheduled) return;
    chartsScheduled = true;
    requestAnimationFrame(() => {
      chartsScheduled = false;
      renderCharts();
    });
  }

  function renderCharts() {
    drawSparkline(readChartCanvas, series.readBytes, { min: 0 });

    // CPU: ignore spikes extrêmes (quantile) + clamp visuel
    drawSparkline(cpuChartCanvas, series.cpu, {
      min: 0,
      autoMaxQuantile: 0.98, // évite de scaler sur un spike isolé (ex: 1073%)
      autoMaxPadFactor: 1.10,
      minMax: 100,
      clampMax: true,
    });

    drawSparkline(memoryChartCanvas, series.memBytes, { min: 0 });
    drawSparkline(threadChartCanvas, series.threads, { min: 0 });
  }

  window.addEventListener("resize", scheduleChartsRender);
  const themeObserver = new MutationObserver(() => scheduleChartsRender());
  themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });

  // ----------------------------
  // Query state
  // ----------------------------
  let activeQueryIdentifier = null;
  let activeEventSource = null;

  let resultColumns = [];
  let pendingRows = [];
  let scheduledFlush = false;
  const flushBatchSize = 400;

  // Synchronisation temps:
  // resource events n'ont parfois pas elapsed_seconds → on réutilise le dernier elapsed connu
  let latestElapsedSeconds = 0;

  // Samples mode
  let hasSamplesStream = false;
  let lastSampleT = -Infinity;

  // Pour éviter le flash final à 0 (resource frame)
  let hasNonZeroResourceFrame = false;

  function closeActiveStream() {
    if (activeEventSource) {
      activeEventSource.close();
      activeEventSource = null;
    }
  }

  function setStatus(text) {
    setText(queryStatusTextElement, text);
  }

  function setQueryIdentifier(text) {
    setText(queryIdentifierTextElement, text || "-");
  }

  function setError(message) {
    if (!errorBannerElement) return;
    if (!message) {
      errorBannerElement.hidden = true;
      errorBannerElement.textContent = "";
      return;
    }
    errorBannerElement.hidden = false;
    errorBannerElement.textContent = message;
  }

  function clearCharts() {
    series.readBytes.length = 0;
    series.cpu.length = 0;
    series.memBytes.length = 0;
    series.threads.length = 0;

    hasSamplesStream = false;
    lastSampleT = -Infinity;
    hasNonZeroResourceFrame = false;

    setProgressVisual(true, 0);
    scheduleChartsRender();
  }

  function clearMetrics() {
    setText(elapsedSecondsTextElement, "-");
    setText(progressPercentTextElement, "-");
    setText(readCountersTextElement, "-");
    setText(readRateTextElement, "-");

    setText(cpuTextElement, "-");
    setText(cpuMaxTextElement, "-");
    setText(memoryTextElement, "-");
    setText(memoryMaxTextElement, "-");
    setText(threadTextElement, "-");
    setText(threadMaxTextElement, "-");

    setError("");
    clearCharts();
  }

  function clearResults() {
    resultColumns = [];
    pendingRows = [];
    scheduledFlush = false;

    if (resultTableHeadElement) resultTableHeadElement.innerHTML = "";
    if (resultTableBodyElement) resultTableBodyElement.innerHTML = "";
    setText(resultColumnsTextElement, "-");
  }

  function setResultMeta(columns) {
    resultColumns = Array.isArray(columns) ? columns : [];
    const headRow = document.createElement("tr");
    for (const columnName of resultColumns) {
      const th = document.createElement("th");
      th.textContent = String(columnName ?? "");
      headRow.appendChild(th);
    }
    if (resultTableHeadElement) {
      resultTableHeadElement.innerHTML = "";
      resultTableHeadElement.appendChild(headRow);
    }
    setText(resultColumnsTextElement, `${resultColumns.length} column(s)`);
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
    if (!resultTableBodyElement) return;

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
    if (pendingRows.length > 0) scheduleFlush();
  }

  // ----------------------------
  // Backend calls
  // ----------------------------
  async function createQuery(queryText) {
    const response = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql: queryText })
    });

    const responseBody = await response.json().catch(() => ({}));
    if (!response.ok) {
      const messageText = responseBody && responseBody.message
        ? responseBody.message
        : `Request failed with status ${response.status}`;
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
      const messageText = responseBody && responseBody.message
        ? responseBody.message
        : `Cancel failed with status ${response.status}`;
      throw new Error(messageText);
    }
    return responseBody;
  }

  // ----------------------------
  // SSE payload handling
  // ----------------------------
  function appendSamples(samples) {
    if (!Array.isArray(samples) || samples.length === 0) return;

    // IMPORTANT: trier par elapsed_seconds pour rester monotone
    const sorted = samples
      .map(s => ({ s, t: asFiniteNumber(s.elapsed_seconds) }))
      .filter(x => x.t != null)
      .sort((a, b) => a.t - b.t);

    for (const { s, t } of sorted) {
      // dédup temporel (fenêtre glissante côté serveur)
      if (t <= lastSampleT + EPS_T) continue;

      const readBytes = asFiniteNumber(s.read_bytes);
      const cpuInst = asFiniteNumber(s.cpu_percent_inst);
      const memInst = asFiniteNumber(s.memory_bytes_inst);
      const thrInst = asFiniteNumber(s.thread_count_inst);

      if (readBytes != null) pushPointMonotone(series.readBytes, t, readBytes);
      if (cpuInst != null) pushPointMonotone(series.cpu, t, cpuInst);
      if (memInst != null) pushPointMonotone(series.memBytes, t, memInst);
      if (thrInst != null) pushPointMonotone(series.threads, t, thrInst);

      lastSampleT = t;
      latestElapsedSeconds = Math.max(latestElapsedSeconds, t);
    }

    scheduleChartsRender();
  }

  function updateProgress(payload) {
    const t = asFiniteNumber(payload.elapsed_seconds);
    if (t != null) latestElapsedSeconds = Math.max(latestElapsedSeconds, t);

    setText(elapsedSecondsTextElement, formatSeconds(payload.elapsed_seconds));

    if (payload.percent_known) {
      setText(progressPercentTextElement, `${formatNumber(payload.percent)} %`);
      setProgressVisual(true, payload.percent);
    } else {
      setText(progressPercentTextElement, "indeterminate");
      setProgressVisual(false, 0);
    }

    setText(
      readCountersTextElement,
      `${formatCompactNumber(payload.read_rows)} rows · ${formatBytes(payload.read_bytes)}`
    );

    // Fallback graph si pas de stream samples
    if (!hasSamplesStream) {
      const tt = t != null ? t : latestElapsedSeconds;
      const rb = asFiniteNumber(payload.read_bytes);
      if (rb != null) pushPointMonotone(series.readBytes, tt, rb);
      scheduleChartsRender();
    }
  }

  function updateResource(payload) {
    const rowsPerSecondInst = asFiniteNumber(payload.rows_per_second_inst) ?? 0;
    const bytesPerSecondInst = asFiniteNumber(payload.bytes_per_second_inst) ?? 0;
    const cpuInst = asFiniteNumber(payload.cpu_percent_inst) ?? 0;
    const cpuInstMax = asFiniteNumber(payload.cpu_percent_inst_max);
    const memInst = asFiniteNumber(payload.memory_bytes_inst);
    const memMax = asFiniteNumber(payload.memory_bytes_inst_max);
    const tInst = asFiniteNumber(payload.thread_count_inst) ?? 0;
    const tMax = asFiniteNumber(payload.thread_count_inst_max);

    // évite le “flash” de fin à 0 : si on a déjà vu du non-zéro, on ignore un frame tout à 0
    const zeroFrame =
      rowsPerSecondInst === 0 &&
      bytesPerSecondInst === 0 &&
      cpuInst === 0 &&
      tInst === 0 &&
      (memInst === null || memInst === 0);

    if (zeroFrame && hasNonZeroResourceFrame) {
      return;
    }
    if (!zeroFrame) hasNonZeroResourceFrame = true;

    setText(
      readRateTextElement,
      `${formatCompactNumber(rowsPerSecondInst)} rows/s · ${formatBytes(bytesPerSecondInst)}/s`
    );

    setText(cpuTextElement, `${formatNumber(cpuInst)}%`);
    setText(cpuMaxTextElement, `max: ${cpuInstMax == null ? "-" : formatNumber(cpuInstMax)}%`);

    setText(memoryTextElement, memInst === null ? "-" : formatBytes(memInst));
    setText(memoryMaxTextElement, `max: ${memMax === null ? "-" : formatBytes(memMax)}`);

    setText(threadTextElement, `${formatNumber(tInst)}`);
    setText(threadMaxTextElement, `max: ${tMax == null ? "-" : formatNumber(tMax)}`);

    // Fallback graph si pas de stream samples
    if (!hasSamplesStream) {
      const t = asFiniteNumber(payload.elapsed_seconds);
      const tt = (t != null) ? t : latestElapsedSeconds;

      pushPointMonotone(series.cpu, tt, cpuInst);
      if (memInst != null) pushPointMonotone(series.memBytes, tt, memInst);
      pushPointMonotone(series.threads, tt, tInst);

      scheduleChartsRender();
    }
  }

  function startStream(streamUrl) {
    closeActiveStream();

    hasSamplesStream = false;
    lastSampleT = -Infinity;
    hasNonZeroResourceFrame = false;
    latestElapsedSeconds = 0;

    const eventSource = new EventSource(streamUrl);
    activeEventSource = eventSource;

    eventSource.addEventListener("meta", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;
      setStatus("running");
      if (cancelButtonElement) cancelButtonElement.disabled = false;
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

    // NEW: samples (mesures intermédiaires Go)
    eventSource.addEventListener("samples", (event) => {
      const payload = safelyParseJson(event.data);
      if (!payload) return;

      const samples = Array.isArray(payload.samples) ? payload.samples : [];
      if (samples.length === 0) return;

      // 1er lot samples → on bascule en mode samples et on repart propre (évite mélange progress+samples out-of-order)
      if (!hasSamplesStream) {
        hasSamplesStream = true;
        series.readBytes.length = 0;
        series.cpu.length = 0;
        series.memBytes.length = 0;
        series.threads.length = 0;
        lastSampleT = -Infinity;
      }

      appendSamples(samples);
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
      for (const row of rows) enqueueRowForRender(row);
    });

    // Attention: "error" en SSE peut être un event custom OU l'event réseau.
    // Ici on supporte le custom (data JSON), sinon on affiche un message générique.
    eventSource.addEventListener("error", (event) => {
      const payload = event && event.data ? safelyParseJson(event.data) : null;
      if (payload && payload.message) {
        setError(payload.message);
        setStatus("error");
      }
    });

    eventSource.addEventListener("done", (event) => {
      const payload = safelyParseJson(event.data);
      if (payload) {
        setStatus(String(payload.status || "done"));
        setText(elapsedSecondsTextElement, formatSeconds(payload.elapsed_seconds));
        if (payload.message) setError(payload.message);

        // si on finit sans % connu, on stoppe l'indeterminate
        if (!payload.percent_known) setProgressVisual(true, 100);
      } else {
        setStatus("done");
      }

      if (cancelButtonElement) cancelButtonElement.disabled = true;
      closeActiveStream();
    });

    eventSource.addEventListener("keepalive", () => {});
    eventSource.onerror = () => {
      // si tu veux afficher quelque chose en cas de coupure réseau, fais-le ici
    };
  }

  // ----------------------------
  // UI actions
  // ----------------------------
  async function handleRun() {
    const queryText = (queryTextAreaElement?.value || "").trim();
    if (!queryText) {
      setError("Please write a query first.");
      return;
    }

    setError("");
    clearMetrics();
    clearResults();

    setStatus("starting…");
    if (cancelButtonElement) cancelButtonElement.disabled = true;
    if (runButtonElement) runButtonElement.disabled = true;

    try {
      const responsePayload = await createQuery(queryText);
      activeQueryIdentifier = responsePayload.query_id;
      setQueryIdentifier(activeQueryIdentifier);

      setStatus("connecting…");
      startStream(responsePayload.stream_url);
    } catch (error) {
      setStatus("error");
      setError(error && error.message ? error.message : String(error));
      if (cancelButtonElement) cancelButtonElement.disabled = true;
      closeActiveStream();
    } finally {
      if (runButtonElement) runButtonElement.disabled = false;
    }
  }

  async function handleCancel() {
    if (!activeQueryIdentifier) return;

    if (cancelButtonElement) cancelButtonElement.disabled = true;
    setStatus("canceling…");

    try {
      await requestCancellation(activeQueryIdentifier);
    } catch (error) {
      setError(error && error.message ? error.message : String(error));
      if (cancelButtonElement) cancelButtonElement.disabled = false;
    }
  }

  function handleClear() {
    closeActiveStream();
    activeQueryIdentifier = null;

    setQueryIdentifier("");
    setStatus("idle");

    clearMetrics();
    clearResults();

    if (cancelButtonElement) cancelButtonElement.disabled = true;
    if (runButtonElement) runButtonElement.disabled = false;
    setError("");
  }

  function loadDefaultQueryIfEmpty() {
    if ((queryTextAreaElement?.value || "").trim() !== "") return;
    queryTextAreaElement.value =
      "SELECT number % 10 AS index, count() FROM numbers(10000000000) GROUP BY index";
  }

  runButtonElement?.addEventListener("click", handleRun);
  cancelButtonElement?.addEventListener("click", handleCancel);
  clearButtonElement?.addEventListener("click", handleClear);

  loadDefaultQueryIfEmpty();
  scheduleChartsRender();
})();
