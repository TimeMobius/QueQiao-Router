(function () {
    var API = "/dashboard/api/records";
    var LOG_API = "/dashboard/api/error-log";
    var state = { errors: false, cursor: null, stack: [], limit: 20, total: 0, totalExact: true, nextCursor: null, logCursor: null, logStack: [], logNext: null, loading: false, seq: 0 };

    var ICON = {
        tokens: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M7 4v16M4 7l3-3 3 3M17 20V4M14 17l3 3 3-3"></path></svg>',
        tools: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"></path></svg>',
        latency: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"></circle><path d="M12 7v5l3.2 1.9"></path></svg>',
        ttft: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2 4.5 13.5H11l-1 8.5 9.5-11.5H13l1-8.5z"></path></svg>',
        info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"></circle><path d="M12 16v-5M12 8h.01"></path></svg>',
        text: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6h16M4 11h16M4 16h9"></path></svg>',
        body: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16v16H4z"></path><path d="M8 8h8M8 12h8M8 16h5"></path></svg>'
    };

    var $ = function (id) { return document.getElementById(id); };
    var esc = function (v) {
        return String(v === null || v === undefined ? "" : v).replace(/[&<>"']/g, function (c) {
            return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
        });
    };
    var dash = function (v) { return (v === null || v === undefined || v === "") ? "-" : v; };
    var fmtMs = function (v) {
        if (v === null || v === undefined) return null;
        var n = Number(v);
        if (!isFinite(n)) return null;
        return n >= 1000 ? (n / 1000).toFixed(2) + "s" : Math.round(n) + "ms";
    };
    var fmtTokens = function (v) {
        if (v === null || v === undefined) return null;
        var n = Number(v);
        if (!isFinite(n)) return null;
        if (n >= 1e6) return (n / 1e6).toFixed(2) + "M";
        if (n >= 1000) return (n / 1000).toFixed(1) + "k";
        return String(n);
    };
    var clientText = function (it) {
        if (it.clientName) return it.clientName + (it.clientVersion ? " " + it.clientVersion : "");
        return it.userAgent || null;
    };
    var shortType = function (t) {
        if (!t) return null;
        if (t.indexOf("chat") === 0) return "chat";
        if (t.indexOf("completion") === 0) return "text";
        if (t.indexOf("embedding") === 0) return "embed";
        return t.split(".")[0];
    };
    var statusClass = function (s, err) {
        if (s === null || s === undefined) return err ? "status-5xx" : "";
        if (s >= 500) return "status-5xx";
        if (s >= 400) return "status-4xx";
        if (s >= 300) return "status-3xx";
        if (s >= 200) return "status-2xx";
        return "";
    };
    var localToMs = function (value) {
        if (!value) return null;
        var t = new Date(value).getTime();
        return isFinite(t) ? t : null;
    };
    var shortTime = function (t) {
        if (!t) return null;
        var s = String(t);
        var m = s.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/);
        return m ? m[1] + " " + m[2] : s;
    };
    function metric(cls, icon, value, title) {
        if (value === null || value === undefined) return "";
        return '<span class="metric ' + cls + '" title="' + esc(title) + '">' + icon + "<span>" + esc(value) + "</span></span>";
    }
    function sub(line) {
        return line ? '<span class="cell-sub">' + esc(line) + "</span>" : "";
    }

    var SCOPE_KEYS = ["q", "model", "ip", "apikey", "client", "session_id", "parent_session_id", "request_id"];

    function msToLocalInput(ms) {
        var d = new Date(Number(ms));
        if (!isFinite(d.getTime())) return "";
        var pad = function (n) { return n < 10 ? "0" + n : String(n); };
        return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
            "T" + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
    }

    function hasActiveFilters() {
        return !!$("searchInput").value.trim() || !!$("fromInput").value || !!$("toInput").value;
    }

    function buildParams() {
        var p = new URLSearchParams();
        if (state.errors) p.set("errors", "1");
        var search = $("searchInput").value.trim();
        if (search) p.set($("scopeSelect").value, search);
        var from = localToMs($("fromInput").value);
        var to = localToMs($("toInput").value);
        if (from !== null) p.set("from", String(from));
        if (to !== null) p.set("to", String(to));
        p.set("limit", String(state.limit));
        if (state.cursor) p.set("cursor", state.cursor);
        return p;
    }

    function syncUrl() {
        var p = new URLSearchParams();
        if (state.errors) p.set("errors", "1");
        var search = $("searchInput").value.trim();
        if (search) p.set($("scopeSelect").value, search);
        var from = localToMs($("fromInput").value);
        var to = localToMs($("toInput").value);
        if (from !== null) p.set("from", String(from));
        if (to !== null) p.set("to", String(to));
        p.set("limit", String(state.limit));
        var qs = p.toString();
        history.replaceState(null, "", location.pathname + (qs ? "?" + qs : ""));
    }

    function updateScopeUi() {
        var labels = {
            q: "请输入关键词", model: "请输入模型名（前缀匹配）", ip: "请输入请求IP（前缀匹配）",
            apikey: "请输入完整 API Key（精确匹配）", client: "请输入客户端",
            session_id: "请输入会话ID", parent_session_id: "请输入父会话ID", request_id: "请输入请求ID"
        };
        var scope = $("scopeSelect").value;
        $("searchInput").placeholder = labels[scope] || "请输入关键词";
        if (scope === "model") {
            $("searchInput").setAttribute("list", "modelOptions");
        } else {
            $("searchInput").removeAttribute("list");
        }
        updateShortHint();
    }

    function updateShortHint() {
        var h = $("searchHint");
        var len = $("searchInput").value.trim().length;
        h.hidden = state.errors || $("scopeSelect").value !== "q" || len === 0 || len >= 3;
    }

    function showError(msg) {
        var b = $("errorBanner");
        b.innerHTML = "";
        var label = document.createElement("span");
        label.textContent = msg;
        var retry = document.createElement("button");
        retry.type = "button";
        retry.className = "banner-retry";
        retry.textContent = "重试";
        retry.addEventListener("click", function () {
            if (state.errors) { loadErrorLog(true); } else { load(true); }
        });
        b.appendChild(label);
        b.appendChild(retry);
        b.classList.add("show");
    }
    function clearError() {
        var b = $("errorBanner");
        b.classList.remove("show");
        b.innerHTML = "";
    }
    function setRows(html) { $("tableBody").innerHTML = html; }

    function skeletonRows() {
        var cell = '<td><span class="skel"></span></td>';
        var row = '<tr class="skel-row">' + new Array(10).join(cell) + "</tr>";
        return new Array(9).join(row);
    }
    function skeletonLog() {
        var entry = '<div class="log-entry skel-entry"><span class="skel"></span><span class="skel short"></span></div>';
        return new Array(6).join(entry);
    }

    // Disable paging controls while a request is in flight so rapid clicks cannot
    // let an older response land after a newer one.
    function setBusy(busy) {
        state.loading = busy;
        $("refreshBtn").disabled = busy;
        $("prevBtn").disabled = busy;
        $("nextBtn").disabled = busy;
        var table = $("tableScroll");
        if (table) table.setAttribute("aria-busy", busy ? "true" : "false");
        var log = $("logList");
        if (log) log.setAttribute("aria-busy", busy ? "true" : "false");
    }

    function renderItems(items) {
        if (!items.length) {
            if (hasActiveFilters()) {
                setRows('<tr class="state-row"><td colspan="9"><span class="no-match">无匹配结果</span>' +
                    '<button type="button" class="btn-ghost" id="clearFilterBtn">清除筛选</button></td></tr>');
                var clearBtn = $("clearFilterBtn");
                if (clearBtn) clearBtn.addEventListener("click", clearFilters);
            } else {
                setRows('<tr class="state-row"><td colspan="9">暂无数据</td></tr>');
            }
            return;
        }
        var html = items.map(function (it) {
            var type = shortType(it.type);
            var usage =
                metric("tokens", ICON.tokens, fmtTokens(it.totalTokens),
                    "总 Token（" + dash(it.promptTokens) + " 提问 / " + dash(it.completionTokens) + " 回复）") +
                metric("tools", ICON.tools, it.toolCount, "工具调用数");
            var timing =
                metric("latency", ICON.latency, fmtMs(it.latencyMs), "总时延") +
                metric("ttft", ICON.ttft, fmtMs(it.ttftMs), "首字时延 TTFT");
            var preview = it.promptPreview
                ? '<div class="preview-text">' + esc(it.promptPreview) + '</div><button type="button" class="expand-link" data-open="' + esc(it.id) + '">展开全部</button>'
                : '<span class="empty-cell">-</span>';
            return "<tr>" +
                '<td class="time col-time" title="' + esc(dash(it.time)) + '">' + esc(dash(shortTime(it.time))) + "</td>" +
                '<td class="col-model"><div class="model-cell"><span class="model-name" title="' + esc(it.model) + '">' + esc(dash(it.model)) + "</span>" +
                    (type ? '<span class="pill">' + esc(type) + "</span>" : "") + "</div></td>" +
                '<td class="col-status"><span class="status ' + statusClass(it.status, it.error) + '"><span class="dot"></span>' + esc(dash(it.status)) + "</span></td>" +
                '<td class="col-client"><span class="cell-main" title="' + esc(dash(clientText(it))) + '">' + esc(dash(clientText(it))) + "</span>" + sub(it.ip) + "</td>" +
                '<td class="col-session"><span class="cell-main" title="' + esc(dash(it.sessionId)) + '">' + esc(dash(it.sessionId)) + "</span>" + sub(it.requestId) + "</td>" +
                '<td class="col-usage">' + (usage || '<span class="empty-cell">-</span>') + "</td>" +
                '<td class="col-latency">' + (timing || '<span class="empty-cell">-</span>') + "</td>" +
                '<td class="preview col-preview">' + preview + "</td>" +
                '<td class="col-actions"><button type="button" class="link" data-open="' + esc(it.id) + '">查看详情</button></td>' +
                "</tr>";
        }).join("");
        setRows(html);
        updateScrollHint();
    }

    function renderPager() {
        $("totalText").textContent = "共 " + state.total + (state.totalExact === false ? "+" : "") + " 条";
        $("pageText").textContent = "第 " + (state.stack.length + 1) + " 页";
        $("prevBtn").disabled = state.stack.length === 0;
        $("nextBtn").disabled = !state.nextCursor;
    }

    function applyView() {
        var file = state.errors;
        document.querySelector(".filters").classList.toggle("file-mode", file);
        $("tableWrap").hidden = file;
        $("logWrap").hidden = !file;
        $("advancedFilters").hidden = file;
        if (!file) updateScrollHint();
    }

    function updateScrollHint() {
        var wrap = $("tableWrap");
        var el = $("tableScroll");
        if (!wrap || !el || wrap.hidden) return;
        var scrollable = el.scrollWidth > el.clientWidth + 1;
        wrap.classList.toggle("is-scrollable", scrollable);
        wrap.classList.toggle("at-end", !scrollable || el.scrollLeft + el.clientWidth >= el.scrollWidth - 1);
    }

    function clearFilters() {
        $("searchInput").value = "";
        $("searchCounter").textContent = "0/" + $("searchInput").maxLength;
        $("scopeSelect").value = "q";
        $("fromInput").value = "";
        $("toInput").value = "";
        $("rangePreset").value = "";
        $("advancedFilters").open = false;
        updateScopeUi();
        load(true);
    }

    function renderLogPager() {
        $("pageText").textContent = "第 " + (state.logStack.length + 1) + " 页";
        $("prevBtn").disabled = state.logStack.length === 0;
        $("nextBtn").disabled = !state.logNext;
    }

    function logTime(e) {
        var m = String(e.time || "").match(/(\d{2}:\d{2}:\d{2})/);
        return m ? m[1] : (e.time || "");
    }
    function logEntryHtml(e) {
        if (e.kind === "unparsed") {
            return '<div class="log-entry"><pre class="log-raw">' + esc(e.raw) + "</pre></div>";
        }
        var badge = e.kind === "http_error"
            ? '<span class="status ' + statusClass(e.status, e.error) + '"><span class="dot"></span>' + esc(dash(e.status)) + "</span>"
            : '<span class="log-kind ' + (e.kind === "stream_interrupted" ? "stream" : "other") + '">' +
              (e.kind === "stream_interrupted" ? "流式中断" : "其他") + "</span>";
        var path = e.method ? e.method + " " + (e.path || "") : (e.path || "-");
        var meta = [];
        if (e.ip) meta.push("客户端 " + e.ip);
        if (e.backend) meta.push("后端 " + e.backend);
        if (e.latency) meta.push("耗时 " + e.latency);
        if (e.model && e.model !== "-") meta.push("模型 " + e.model);
        if (e.api_key && e.api_key !== "-") meta.push("Key " + e.api_key);
        if (e.user_agent) meta.push(e.user_agent);
        var details = "";
        if (e.request_body) {
            details += '<details class="log-detail"><summary>请求体</summary><pre>' + esc(pretty(e.request_body)) + "</pre></details>";
        }
        details += '<details class="log-detail"><summary>原始行</summary><pre>' + esc(e.raw) + "</pre></details>";
        return '<div class="log-entry">' +
            '<div class="log-head">' + badge +
                '<span class="log-time">' + esc(logTime(e)) + "</span>" +
                '<span class="log-path">' + esc(path) + "</span>" +
            "</div>" +
            (e.error ? '<div class="log-err">' + esc(e.error) + "</div>" : "") +
            (meta.length ? '<div class="log-meta-line">' + esc(meta.join(" · ")) + "</div>" : "") +
            details +
        "</div>";
    }
    function renderLogEntries(entries, file) {
        if (!entries.length) {
            $("logList").innerHTML = '<p class="hint">' + (file ? "错误日志暂无内容" : "未找到错误日志文件") + "</p>";
            return;
        }
        $("logList").innerHTML = entries.map(logEntryHtml).join("");
    }

    function loadErrorLog(reset) {
        if (reset) { state.logCursor = null; state.logStack = []; }
        var seq = ++state.seq;
        setBusy(true);
        syncUrl();
        clearError();
        $("logList").innerHTML = skeletonLog();
        var p = new URLSearchParams();
        p.set("limit", String(state.limit));
        if (state.logCursor !== null) p.set("before", String(state.logCursor));
        fetch(LOG_API + "?" + p.toString())
            .then(function (r) {
                if (r.status === 404) return { file: null, entries: [], nextBefore: null };
                if (!r.ok) throw new Error("HTTP " + r.status);
                return r.json();
            })
            .then(function (d) {
                if (seq !== state.seq) return;
                state.logNext = d.nextBefore || null;
                $("logMeta").textContent = d.file
                    ? d.file + "（从文件末尾向前分页，每页 " + state.limit + " 条）"
                    : "未找到错误日志文件";
                $("totalText").textContent = d.file || "-";
                renderLogEntries(d.entries || [], d.file);
                setBusy(false);
                renderLogPager();
                if (window.QQShell) { QQShell.setStatus('ok'); QQShell.setUpdated(new Date()); }
            })
            .catch(function (e) {
                if (seq !== state.seq) return;
                $("logList").innerHTML = "";
                $("logMeta").textContent = "";
                showError("加载错误日志失败：" + e.message);
                setBusy(false);
                renderLogPager();
                if (window.QQShell) QQShell.setStatus('down', '记录加载失败');
            });
    }

    function load(reset) {
        if (reset) { state.cursor = null; state.stack = []; }
        var seq = ++state.seq;
        setBusy(true);
        syncUrl();
        clearError();
        setRows(skeletonRows());
        fetch(API + "?" + buildParams().toString())
            .then(function (r) {
                if (!r.ok) throw new Error("HTTP " + r.status);
                return r.json();
            })
            .then(function (data) {
                if (seq !== state.seq) return;
                state.total = data.total || 0;
                state.totalExact = data.totalExact !== false;
                state.nextCursor = data.nextCursor || null;
                renderItems(data.items || []);
                setBusy(false);
                renderPager();
                if (window.QQShell) { QQShell.setStatus('ok'); QQShell.setUpdated(new Date()); }
            })
            .catch(function (e) {
                if (seq !== state.seq) return;
                setRows('<tr class="state-row"><td colspan="9">加载失败</td></tr>');
                showError("加载记录失败：" + e.message);
                setBusy(false);
                renderPager();
                if (window.QQShell) QQShell.setStatus('down', '记录加载失败');
            });
    }

    function loadFacets() {
        fetch(API + "/facets").then(function (r) { return r.json(); }).then(function (d) {
            $("modelOptions").innerHTML = (d.models || []).map(function (m) {
                return '<option value="' + esc(m) + '"></option>';
            }).join("");
        }).catch(function () {});
    }

    var lastFocused = null;

    function focusables(root) {
        var sel = 'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), ' +
            'textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
        return Array.prototype.filter.call(root.querySelectorAll(sel), function (el) {
            return el.offsetParent !== null || el === document.activeElement;
        });
    }

    function setShellInert(on) {
        [document.querySelector(".app-header"), document.querySelector(".app-main")].forEach(function (el) {
            if (!el) return;
            if ("inert" in el) el.inert = on;
            el.classList.toggle("is-inert", on);
        });
    }

    function openDrawer(trigger) {
        lastFocused = trigger || document.activeElement;
        $("drawer").classList.add("open");
        $("backdrop").classList.add("open");
        setShellInert(true);
        var close = $("drawerClose");
        if (close) close.focus();
    }
    function closeDrawer() {
        if (!$("drawer").classList.contains("open")) return;
        $("drawer").classList.remove("open");
        $("backdrop").classList.remove("open");
        setShellInert(false);
        if (lastFocused && typeof lastFocused.focus === "function") lastFocused.focus();
        lastFocused = null;
    }

    function group(title, icon, grid) {
        return '<div class="group"><div class="group-title">' + icon + esc(title) + "</div>" + grid + "</div>";
    }
    function metaRow(label, value) {
        return "<dt>" + esc(label) + "</dt><dd>" + esc(dash(value)) + "</dd>";
    }
    function maskKey(v) {
        if (!v) return "";
        var s = String(v);
        if (s.length <= 8) return s.charAt(0) + "••••";
        return s.slice(0, 8) + "••••" + s.slice(-4);
    }
    function apiKeyRow(v) {
        if (!v) return metaRow("API Key", null);
        return '<dt>API Key</dt><dd><span class="mono" id="apiKeyVal" data-shown="0" data-full="' +
            esc(v) + '" data-masked="' + esc(maskKey(v)) + '">' + esc(maskKey(v)) +
            '</span> <span class="link" id="apiKeyToggle">显示</span></dd>';
    }
    function section(title, icon, text, open) {
        if (!text) return "";
        return '<details class="section"' + (open ? " open" : "") + "><summary>" + icon + esc(title) +
            "</summary><pre>" + esc(text) + "</pre></details>";
    }
    function pretty(raw) {
        if (!raw) return "";
        try { return JSON.stringify(JSON.parse(raw), null, 2); } catch (e) { return raw; }
    }

    function renderDetail(d) {
        var html =
            group("请求信息", ICON.info, '<dl class="meta-grid">' +
                metaRow("记录 ID", d.id) +
                metaRow("请求时间", d.time) +
                metaRow("类型", d.type) +
                metaRow("模型", d.model) +
                metaRow("状态", d.status) +
                metaRow("后端", d.backend) +
                metaRow("方法 / 端点", (d.method || "") + " " + (d.endpoint || "")) +
                metaRow("结束原因", d.finishReason) +
                metaRow("错误", d.error) +
                metaRow("重试次数", d.retryCount) +
            "</dl>") +
            group("客户端", ICON.text, '<dl class="meta-grid">' +
                metaRow("客户端", clientText(d)) +
                metaRow("User-Agent", d.userAgent) +
                metaRow("服务 IP", d.ip) +
                metaRow("会话 ID", d.sessionId) +
                metaRow("父会话 ID", d.parentSessionId) +
                metaRow("请求 ID", d.requestId) +
                apiKeyRow(d.apiKey) +
            "</dl>") +
            group("用量与耗时", ICON.tokens, '<dl class="meta-grid">' +
                metaRow("总 Token", d.totalTokens) +
                metaRow("提问 Token", d.promptTokens) +
                metaRow("回复 Token", d.completionTokens) +
                metaRow("消息轮数", d.messageCount) +
                metaRow("工具调用", d.toolCount) +
                metaRow("助手消息", d.assistantCount) +
                metaRow("工具结果", d.toolResultCount) +
                metaRow("图片数", d.imageCount) +
                metaRow("总时延", fmtMs(d.latencyMs)) +
                metaRow("TTFT", fmtMs(d.ttftMs)) +
                metaRow("上游耗时", fmtMs(d.upstreamMs)) +
                metaRow("流式耗时", fmtMs(d.streamMs)) +
                metaRow("请求体积", d.requestBytes) +
                metaRow("响应体积", d.responseBytes) +
            "</dl>");
        html += section("Prompt", ICON.text, d.prompt, true);
        html += section("RequestTail", ICON.body, d.requestTail);
        html += section("Answer", ICON.text, d.answer);
        html += section("ToolNames", ICON.tools, d.toolNames);
        html += '<div id="bodyArea"></div>';
        html += d.hasPayload
            ? '<button class="drawer-btn" id="loadBodyBtn">' + ICON.body + "加载完整正文</button>"
            : '<p class="hint">该记录无压缩正文（历史数据或正文为空）</p>';
        $("drawerBody").innerHTML = html;
        var btn = $("loadBodyBtn");
        if (btn) btn.addEventListener("click", function () { loadBody(d.id, btn); });
        var keyToggle = $("apiKeyToggle");
        if (keyToggle) keyToggle.addEventListener("click", function () {
            var el = $("apiKeyVal");
            var shown = el.getAttribute("data-shown") === "1";
            el.textContent = shown ? el.getAttribute("data-masked") : el.getAttribute("data-full");
            el.setAttribute("data-shown", shown ? "0" : "1");
            keyToggle.textContent = shown ? "显示" : "隐藏";
        });
    }

    function loadBody(id, btn) {
        btn.disabled = true;
        btn.textContent = "加载中…";
        fetch(API + "/" + id + "?include=body").then(function (r) { return r.json(); }).then(function (d) {
            $("bodyArea").innerHTML =
                section("完整请求体", ICON.body, pretty(d.request), true) +
                section("完整响应体", ICON.body, pretty(d.response)) +
                section("请求头", ICON.text, pretty(d.headers));
            btn.remove();
        }).catch(function (e) {
            btn.disabled = false;
            btn.innerHTML = ICON.body + "加载完整正文";
            showError("加载正文失败：" + e.message);
        });
    }

    function openDetail(id, trigger) {
        $("drawerTitle").textContent = "请求详情 #" + id;
        $("drawerBody").innerHTML = '<p class="hint">加载中…</p>';
        openDrawer(trigger);
        fetch(API + "/" + id).then(function (r) {
            if (!r.ok) throw new Error("HTTP " + r.status);
            return r.json();
        }).then(renderDetail).catch(function (e) {
            $("drawerBody").innerHTML = '<p style="color:var(--danger)">加载详情失败：' + esc(e.message) + "</p>";
        });
    }

    function setMode(errors, doLoad) {
        state.errors = errors;
        $("modeToggle").querySelectorAll("button").forEach(function (b) {
            b.classList.toggle("active", (b.getAttribute("data-errors") === "1") === errors);
        });
        applyView();
        updateShortHint();
        if (doLoad) {
            if (errors) { loadErrorLog(true); } else { load(true); }
        }
    }

    $("modeToggle").addEventListener("click", function (e) {
        var btn = e.target.closest("button");
        if (!btn) return;
        setMode(btn.getAttribute("data-errors") === "1", true);
    });

    $("tableBody").addEventListener("click", function (e) {
        var el = e.target.closest("[data-open]");
        if (el) openDetail(el.getAttribute("data-open"), el);
    });

    $("refreshBtn").addEventListener("click", function () {
        if (state.errors) { loadErrorLog(true); } else { load(true); }
    });
    $("prevBtn").addEventListener("click", function () {
        if (state.errors) {
            if (!state.logStack.length) return;
            state.logCursor = state.logStack.pop();
            loadErrorLog(false);
            return;
        }
        if (!state.stack.length) return;
        state.cursor = state.stack.pop();
        load(false);
    });
    $("nextBtn").addEventListener("click", function () {
        if (state.errors) {
            if (!state.logNext) return;
            state.logStack.push(state.logCursor);
            state.logCursor = state.logNext;
            loadErrorLog(false);
            return;
        }
        if (!state.nextCursor) return;
        state.stack.push(state.cursor);
        state.cursor = state.nextCursor;
        load(false);
    });
    $("sizeSelect").addEventListener("change", function () {
        state.limit = parseInt(this.value, 10) || 20;
        if (state.errors) { loadErrorLog(true); } else { load(true); }
    });

    var searchTimer = null;
    $("searchInput").addEventListener("input", function () {
        $("searchCounter").textContent = this.value.length + "/" + this.maxLength;
        updateShortHint();
        if (state.errors) return;
        if (searchTimer) clearTimeout(searchTimer);
        searchTimer = setTimeout(function () { searchTimer = null; load(true); }, 300);
    });
    $("searchInput").addEventListener("keydown", function (e) {
        if (e.key !== "Enter") return;
        if (searchTimer) { clearTimeout(searchTimer); searchTimer = null; }
        if (!state.errors) load(true);
    });
    $("scopeSelect").addEventListener("change", updateScopeUi);

    $("rangePreset").addEventListener("change", function () {
        if (this.value === "custom") { $("advancedFilters").open = true; return; }
        var map = { "1h": 3600e3, "24h": 86400e3, "7d": 604800e3 };
        if (!this.value) {
            $("fromInput").value = "";
            $("toInput").value = "";
        } else {
            $("fromInput").value = msToLocalInput(Date.now() - map[this.value]);
            $("toInput").value = "";
        }
        if (!state.errors) load(true);
    });
    ["fromInput", "toInput"].forEach(function (id) {
        $(id).addEventListener("change", function () {
            $("rangePreset").value = "custom";
            if (!state.errors) load(true);
        });
    });

    $("tableScroll").addEventListener("scroll", updateScrollHint);
    window.addEventListener("resize", updateScrollHint);
    if (window.ResizeObserver) new ResizeObserver(updateScrollHint).observe($("tableScroll"));

    $("drawer").addEventListener("keydown", function (e) {
        if (e.key !== "Tab") return;
        var f = focusables(this);
        if (!f.length) return;
        var first = f[0];
        var last = f[f.length - 1];
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    });
    $("drawerClose").addEventListener("click", closeDrawer);
    $("backdrop").addEventListener("click", closeDrawer);
    document.addEventListener("keydown", function (e) { if (e.key === "Escape") closeDrawer(); });

    function hydrateFromUrl() {
        var p = new URLSearchParams(location.search);
        var scope = null;
        SCOPE_KEYS.forEach(function (k) { if (scope === null && p.get(k) !== null) scope = k; });
        if (scope) {
            $("scopeSelect").value = scope;
            $("searchInput").value = p.get(scope) || "";
            $("searchCounter").textContent = $("searchInput").value.length + "/" + $("searchInput").maxLength;
        }
        var from = parseInt(p.get("from"), 10);
        var to = parseInt(p.get("to"), 10);
        if (isFinite(from)) $("fromInput").value = msToLocalInput(from);
        if (isFinite(to)) $("toInput").value = msToLocalInput(to);
        if ($("fromInput").value || $("toInput").value) {
            $("advancedFilters").open = true;
            $("rangePreset").value = "custom";
        }
        var limit = parseInt(p.get("limit"), 10);
        if ([20, 50, 100, 200].indexOf(limit) >= 0) {
            state.limit = limit;
            $("sizeSelect").value = String(limit);
        }
        updateScopeUi();
        return p.get("errors") === "1";
    }

    var startErrors = hydrateFromUrl();
    setMode(startErrors, false);
    loadFacets();
    if (startErrors) { loadErrorLog(true); } else { load(true); }
})();
