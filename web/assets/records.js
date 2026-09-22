(function () {
    var API = "/dashboard/api/records";
    var LOG_API = "/dashboard/api/error-log";
    var state = { errors: false, cursor: null, stack: [], limit: 20, total: 0, totalExact: true, nextCursor: null, logCursor: null, logStack: [], logNext: null, loading: false, seq: 0, relationSeq: 0, detail: null, urlFilters: {} };

    var ICON = {
        tokens: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M7 4v16M4 7l3-3 3 3M17 20V4M14 17l3 3 3-3"></path></svg>',
        rounds: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M21 11.5a8.4 8.4 0 0 1-9 8.4 8.9 8.9 0 0 1-3.8-.9L3 21l1.9-5A8.4 8.4 0 0 1 12 3.1a8.4 8.4 0 0 1 9 8.4z"></path></svg>',
        tools: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"></path></svg>',
        latency: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"></circle><path d="M12 7v5l3.2 1.9"></path></svg>',
        ttft: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2 4.5 13.5H11l-1 8.5 9.5-11.5H13l1-8.5z"></path></svg>',
        info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"></circle><path d="M12 16v-5M12 8h.01"></path></svg>',
        text: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6h16M4 11h16M4 16h9"></path></svg>',
        body: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16v16H4z"></path><path d="M8 8h8M8 12h8M8 16h5"></path></svg>',
        back: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M19 12H5"></path><path d="M12 19l-7-7 7-7"></path></svg>'
    };

    var $ = function (id) { return document.getElementById(id); };
    var esc = function (v) {
        return String(v === null || v === undefined ? "" : v).replace(/[&<>"']/g, function (c) {
            return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
        });
    };
    var dash = function (v) { return (v === null || v === undefined || v === "") ? "-" : v; };
    var num = function (v) { var n = Number(v); return isFinite(n) ? n : 0; };
    var full = function (v) { return num(v).toLocaleString(); };
    var compact = function (v) {
        var n = num(v), abs = Math.abs(n);
        if (abs >= 1e9) return (n / 1e9).toFixed(1).replace(/\.0$/, "") + "B";
        if (abs >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
        if (abs >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, "") + "k";
        return full(n);
    };
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
    var fmtBytes = function (v) {
        var n = Number(v);
        if (!isFinite(n) || n <= 0) return null;
        if (n >= 1048576) return (n / 1048576).toFixed(2) + " MB";
        if (n >= 1024) return (n / 1024).toFixed(1) + " KB";
        return n + " B";
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
    function metricRow(parts) {
        var body = parts.filter(Boolean).join("");
        return body ? '<span class="metric-row">' + body + "</span>" : "";
    }
    function sub(line) {
        return line ? '<span class="cell-sub">' + esc(line) + "</span>" : "";
    }

    var SCOPE_KEYS = ["q", "model", "ip", "apikey", "client", "session_id", "parent_session_id", "request_id"];
    var URL_FILTER_KEYS = ["model", "ip", "apikey", "type", "backend", "status", "client", "q", "session_id", "parent_session_id", "request_id"];
    // 会话时间线 / 调用树使用独立的宽松时间窗口（近 90 天），
    // 绝不覆盖用户在筛选区选择的 from/to；后端缺省只查当前库，跨月会话会被截断，故必须显式带 from/to。
    var RELATION_WINDOW_MS = 90 * 24 * 3600 * 1000;
    var TIMELINE_MAX_PAGES = 5;
    var TIMELINE_MAX_ITEMS = 2500;
    var TIMELINE_PAGE_SIZE = 500;
    var TREE_MAX_DEPTH = 5;
    var TREE_MAX_NODES = 200;
    var TREE_QUERY_SIZE = 500;

    function msToLocalInput(ms) {
        var d = new Date(Number(ms));
        if (!isFinite(d.getTime())) return "";
        var pad = function (n) { return n < 10 ? "0" + n : String(n); };
        return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
            "T" + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
    }

    function hasActiveFilters() {
        if ($("searchInput").value.trim() || $("fromInput").value || $("toInput").value) return true;
        for (var key in state.urlFilters) {
            if (state.urlFilters[key]) return true;
        }
        return false;
    }

    function buildParams() {
        var p = new URLSearchParams();
        if (state.errors) p.set("errors", "1");
        URL_FILTER_KEYS.forEach(function (key) {
            if (state.urlFilters[key]) p.set(key, state.urlFilters[key]);
        });
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
        URL_FILTER_KEYS.forEach(function (key) {
            if (state.urlFilters[key]) p.set(key, state.urlFilters[key]);
        });
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
        } else if (scope === "client") {
            $("searchInput").setAttribute("list", "clientOptions");
        } else {
            $("searchInput").removeAttribute("list");
        }
        $("searchField").classList.toggle("has-datalist", scope === "model" || scope === "client");
        updateShortHint();
        syncSearchClear();
    }

    function updateShortHint() {
        var h = $("searchHint");
        var len = $("searchInput").value.trim().length;
        h.hidden = state.errors || $("scopeSelect").value !== "q" || len === 0 || len >= 3;
    }

    // 清除按钮仅在有内容时出现；点击只清空并聚焦，检索仍由回车/查询按钮显式触发。
    function syncSearchClear() {
        var clear = $("searchClear");
        if (!clear) return;
        clear.hidden = state.errors || $("searchInput").value.length === 0;
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
                metricRow([
                    metric("rounds", ICON.rounds, it.messageCount, "消息轮数"),
                    metric("tools", ICON.tools, it.toolCount, "工具调用数")
                ]) +
                metricRow([
                    metric("tokens", ICON.tokens, fmtTokens(it.totalTokens),
                        "总 Token（" + dash(it.promptTokens) + " 提问 / " + dash(it.completionTokens) + " 回复）")
                ]);
            var timing =
                metric("latency", ICON.latency, fmtMs(it.latencyMs), "总时延") +
                metric("ttft", ICON.ttft, fmtMs(it.ttftMs), "首字时延 TTFT");
            var shardAttr = it.shard ? ' data-shard="' + esc(it.shard) + '"' : "";
            var preview = it.promptPreview
                ? '<div class="preview-text">' + esc(it.promptPreview) + '</div><button type="button" class="expand-link" data-open="' + esc(it.id) + '"' + shardAttr + '>展开全部</button>'
                : '<span class="empty-cell">-</span>';
            return "<tr>" +
                '<td class="time col-time" title="' + esc(dash(it.time)) + '">' + esc(dash(shortTime(it.time))) + "</td>" +
                '<td class="col-model"><div class="model-cell"><span class="model-name" title="' + esc(it.model) + '">' + esc(dash(it.model)) + "</span>" +
                    (type ? '<span class="pill">' + esc(type) + "</span>" : "") + "</div></td>" +
                '<td class="col-status"><span class="status ' + statusClass(it.status, it.error) + '"><span class="dot"></span>' + esc(dash(it.status)) + "</span></td>" +
                '<td class="col-client"><span class="cell-main" title="' + esc(dash(clientText(it))) + '">' + esc(dash(clientText(it))) + "</span>" + sub(it.ip) + "</td>" +
                '<td class="col-session"><span class="cell-main" title="' + esc(dash(it.sessionId)) + '">' + esc(dash(it.sessionId)) + "</span>" + sub(it.requestId) + "</td>" +
                '<td class="col-usage">' + (usage ? '<span class="metrics">' + usage + "</span>" : '<span class="empty-cell">-</span>') + "</td>" +
                '<td class="col-latency">' + (timing || '<span class="empty-cell">-</span>') + "</td>" +
                '<td class="preview col-preview">' + preview + "</td>" +
                '<td class="col-actions"><button type="button" class="link" data-open="' + esc(it.id) + '"' + shardAttr + '>查看详情</button></td>' +
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
        state.urlFilters = {};
        $("searchInput").value = "";
        $("searchCounter").textContent = "0/" + $("searchInput").maxLength;
        $("scopeSelect").value = "q";
        $("fromInput").value = "";
        $("toInput").value = "";
        $("rangePreset").value = "";
        $("advancedFilters").open = false;
        updateScopeUi();
        syncSearchClear();
        load(true);
    }

    function renderLogPager() {
        $("pageText").textContent = "第 " + (state.logStack.length + 1) + " 页";
        $("prevBtn").disabled = state.logStack.length === 0;
        $("nextBtn").disabled = !state.logNext;
    }

    // 日志文件按天轮转，行内只显示时分秒，日期在标题栏的文件名里。
    // 正则必须锚定日期前缀，否则 "…/2026:14:49" 中的 "26:14:49" 会先被匹配到。
    function logTime(e) {
        var s = String(e.time || "");
        var m = s.match(/^\d{2}\/[A-Za-z]{3}\/\d{4}:(\d{2}:\d{2}:\d{2})/) ||
            s.match(/(\d{2}:\d{2}:\d{2})\s+[+-]\d{4}/);
        return m ? m[1] : s;
    }
    var LOG_ERROR_PREVIEW_CHARS = 600;
    function errorBlock(e) {
        var s = String(e.error || "");
        if (!s) return "";
        var sizeNote = e.error_truncated
            ? "（服务端已截断：原始 " + (fmtBytes(e.error_bytes) || e.error_bytes) + "，完整内容见「原始行」）"
            : "";
        if (s.length <= LOG_ERROR_PREVIEW_CHARS && !sizeNote) {
            return '<div class="log-err">' + esc(s) + "</div>";
        }
        var summary = "展开完整错误信息" + (sizeNote || "（" + s.length + " 字符）");
        return '<div class="log-err">' + esc(s.slice(0, LOG_ERROR_PREVIEW_CHARS)) + "…</div>" +
            '<details class="log-detail log-detail-err"><summary>' + esc(summary) + "</summary>" +
            '<pre class="log-raw">' + esc(s) + "</pre></details>";
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
                '<span class="log-time" title="' + esc(e.time || "") + '">' + esc(logTime(e)) + "</span>" +
                '<span class="log-path">' + esc(path) + "</span>" +
            "</div>" +
            errorBlock(e) +
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
            $("clientOptions").innerHTML = (d.clients || []).map(function (c) {
                return '<option value="' + esc(c) + '"></option>';
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
                metaRow("请求体积", fmtBytes(d.requestBytes)) +
                metaRow("响应体积", fmtBytes(d.responseBytes)) +
            "</dl>");
        html += '<div class="drawer-actions">' +
            '<button type="button" class="drawer-btn" id="timelineBtn"' +
                (d.sessionId ? "" : ' disabled title="该记录没有会话 ID"') + ">" +
                ICON.latency + "会话时间线</button>" +
            '<button type="button" class="drawer-btn" id="treeBtn">' + ICON.tools + "调用树</button>" +
            "</div>";
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
        var timelineBtn = $("timelineBtn");
        if (timelineBtn && !timelineBtn.disabled) timelineBtn.addEventListener("click", function () {
            loadTimeline(d.sessionId);
        });
        var treeBtn = $("treeBtn");
        if (treeBtn) treeBtn.addEventListener("click", function () { loadCallTree(d); });
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
        var shard = state.detail && state.detail.shard ? state.detail.shard : null;
        var url = API + "/" + encodeURIComponent(id) + "?include=body" +
            (shard ? "&shard=" + encodeURIComponent(shard) : "");
        fetch(url).then(function (r) { return r.json(); }).then(function (d) {
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

    function openDetail(id, trigger, shard) {
        $("drawerTitle").textContent = "请求详情 #" + id;
        $("drawerBody").innerHTML = '<p class="hint">加载中…</p>';
        openDrawer(trigger);
        state.detail = null;
        var url = API + "/" + encodeURIComponent(id) + (shard ? "?shard=" + encodeURIComponent(shard) : "");
        fetch(url).then(function (r) {
            if (!r.ok) throw new Error("HTTP " + r.status);
            return r.json();
        }).then(function (d) {
            if (shard && !d.shard) d.shard = shard;
            state.detail = d;
            renderDetail(d);
        }).catch(function (e) {
            $("drawerBody").innerHTML = '<p style="color:var(--danger)">加载详情失败：' + esc(e.message) + "</p>";
        });
    }

    function relationQuery(field, value, limit, cursor) {
        var p = new URLSearchParams();
        var now = Date.now();
        p.set("from", String(now - RELATION_WINDOW_MS));
        p.set("to", String(now));
        p.set(field, value);
        p.set("limit", String(limit));
        if (cursor) p.set("cursor", cursor);
        return p;
    }

    function fetchRecords(params) {
        return fetch(API + "?" + params.toString()).then(function (r) {
            if (!r.ok) throw new Error("HTTP " + r.status);
            return r.json();
        });
    }

    function relationView(title, icon) {
        $("drawerBody").innerHTML =
            '<div class="drawer-actions"><button type="button" class="drawer-btn" id="relationBack">' +
            ICON.back + "返回详情</button></div>" +
            '<div class="group"><div class="group-title">' + icon + esc(title) + "</div>" +
            '<div id="relationBody"></div></div>';
        var back = $("relationBack");
        if (back) back.addEventListener("click", function () {
            if (state.detail) renderDetail(state.detail);
        });
    }

    function setRelationBody(html) {
        var el = $("relationBody");
        if (el) el.innerHTML = html;
    }

    function relationFailText(e) {
        return '<p class="tree-loading">加载失败：' + esc(e.message || e) + "</p>" +
            '<p><button type="button" class="drawer-btn" id="relationRetry">重试</button></p>';
    }

    function timelineSummaryItem(label, value) {
        return '<div class="timeline-summary-item"><span class="timeline-summary-label">' + esc(label) +
            '</span><span class="timeline-summary-value" title="' + esc(value) + '">' + esc(value) + "</span></div>";
    }

    function renderTimeline(items, sessionId, truncated) {
        if (!items.length) {
            setRelationBody('<p class="tree-loading">该会话暂无记录</p>');
            return;
        }
        items.sort(function (a, b) {
            return (num(a.timeMs) - num(b.timeMs)) || (num(a.id) - num(b.id));
        });
        var totalTokens = 0;
        var errors = 0;
        items.forEach(function (it) {
            totalTokens += num(it.totalTokens);
            if (num(it.status) >= 400 || it.error) errors++;
        });
        var first = items[0];
        var last = items[items.length - 1];
        var summary = '<div class="timeline-summary">' +
            timelineSummaryItem("请求数", full(items.length)) +
            timelineSummaryItem("总 Token", compact(totalTokens)) +
            timelineSummaryItem("错误数", full(errors)) +
            timelineSummaryItem("首个请求", shortTime(first.time) || "-") +
            timelineSummaryItem("最后请求", shortTime(last.time) || "-") +
            "</div>";
        var list = items.map(function (it) {
            var rid = it.requestId || "";
            var shardAttr = it.shard ? ' data-shard="' + esc(it.shard) + '"' : "";
            return '<button type="button" class="timeline-item" data-open="' + esc(it.id) + '"' + shardAttr + ">" +
                '<span class="timeline-time" title="' + esc(dash(it.time)) + '">' + esc(dash(shortTime(it.time))) + "</span>" +
                '<span class="timeline-main">' +
                    '<span class="timeline-model" title="' + esc(dash(it.model)) + '">' + esc(dash(it.model)) + "</span>" +
                    '<span class="timeline-meta">' +
                        "<span>Token " + esc(dash(fmtTokens(it.totalTokens))) + "</span>" +
                        "<span>时延 " + esc(fmtMs(it.latencyMs) || "-") + "</span>" +
                        (rid ? '<span class="timeline-request" title="' + esc(rid) + '">' + esc(rid) + "</span>" : "") +
                    "</span>" +
                "</span>" +
                '<span class="timeline-status"><span class="status ' + statusClass(it.status, it.error) +
                    '"><span class="dot"></span>' + esc(dash(it.status)) + "</span></span>" +
            "</button>";
        }).join("");
        setRelationBody(
            (truncated ? '<p class="tree-notice">已达上限，结果可能不完整</p>' : "") +
            '<p class="hint" style="margin-top:0">会话 ID：<span class="mono">' + esc(sessionId) +
                "</span> · 共 " + full(items.length) + " 条 · 按时间升序</p>" +
            summary + '<div class="timeline-list">' + list + "</div>"
        );
    }

    function loadTimeline(sessionId) {
        var seq = ++state.relationSeq;
        relationView("会话时间线", ICON.latency);
        setRelationBody('<p class="tree-loading">加载中…</p>');
        var collected = [];
        var seen = {};
        var pages = 0;
        var truncated = false;

        function nextPage(cursor) {
            pages++;
            return fetchRecords(relationQuery("session_id", sessionId, TIMELINE_PAGE_SIZE, cursor)).then(function (d) {
                if (seq !== state.relationSeq) return null;
                (d.items || []).forEach(function (it) {
                    // session_id 为子串 LIKE，S1 会误命中 S10，必须按全等二次过滤。
                    if (String(it.sessionId) !== String(sessionId)) return;
                    if (collected.length >= TIMELINE_MAX_ITEMS) { truncated = true; return; }
                    if (seen[it.id]) return;
                    seen[it.id] = true;
                    collected.push(it);
                });
                var next = d.nextCursor || null;
                if (next && pages < TIMELINE_MAX_PAGES && collected.length < TIMELINE_MAX_ITEMS) return nextPage(next);
                if (next) truncated = true;
                return null;
            });
        }

        nextPage(null).then(function () {
            if (seq !== state.relationSeq) return;
            renderTimeline(collected, sessionId, truncated);
        }).catch(function (e) {
            if (seq !== state.relationSeq) return;
            setRelationBody(relationFailText(e));
            var retry = $("relationRetry");
            if (retry) retry.addEventListener("click", function () { loadTimeline(sessionId); });
        });
    }

    function fetchChildren(record) {
        var queries = [];
        if (record.sessionId) queries.push({ value: record.sessionId, basis: "session" });
        if (record.requestId) queries.push({ value: record.requestId, basis: "request" });
        if (!queries.length) return Promise.resolve([]);
        var merged = {};
        var order = [];
        var chain = Promise.resolve();
        queries.forEach(function (q) {
            chain = chain.then(function () {
                return fetchRecords(relationQuery("parent_session_id", q.value, TREE_QUERY_SIZE)).then(function (d) {
                    (d.items || []).forEach(function (it) {
                        // parent_session_id 同样是子串 LIKE，需按全等二次过滤后再合并去重。
                        if (String(it.parentSessionId) !== String(q.value)) return;
                        if (merged[it.id]) return;
                        merged[it.id] = true;
                        order.push({ record: it, basis: q.basis });
                    });
                });
            });
        });
        return chain.then(function () { return order; });
    }

    function treeNodeHtml(node) {
        var it = node.record;
        var label = it.sessionId || it.requestId || ("#" + it.id);
        var rid = it.requestId || "";
        var shardAttr = it.shard ? ' data-shard="' + esc(it.shard) + '"' : "";
        var basis = node.basis === "session" ? "依据 会话链接"
            : node.basis === "request" ? "依据 请求链接" : "";
        var children = "";
        if (node.children && node.children.length) {
            children = '<div class="call-tree-children">' + node.children.map(treeNodeHtml).join("") + "</div>";
        }
        return '<div class="call-tree-node"><details open>' +
            '<summary class="call-tree-summary">' +
                '<span class="status ' + statusClass(it.status, it.error) + '"><span class="dot"></span>' +
                    esc(dash(it.status)) + "</span>" +
                '<span class="call-tree-content">' +
                    '<span class="call-tree-line">' +
                        '<button type="button" class="link call-tree-model" data-open="' + esc(it.id) + '"' +
                            shardAttr + ' title="' + esc(label) + '">' + esc(label) + "</button>" +
                        (basis ? '<span class="call-tree-basis">' + esc(basis) + "</span>" : "") +
                    "</span>" +
                    '<span class="call-tree-meta">' +
                        "<span>" + esc(dash(it.model)) + "</span>" +
                        (fmtMs(it.latencyMs) ? "<span>时延 " + esc(fmtMs(it.latencyMs)) + "</span>" : "") +
                        (it.totalTokens !== null && it.totalTokens !== undefined
                            ? "<span>Token " + esc(compact(it.totalTokens)) + "</span>" : "") +
                        "<span>" + esc(dash(shortTime(it.time))) + "</span>" +
                        (rid ? '<span class="call-tree-request" title="' + esc(rid) + '">' + esc(rid) + "</span>" : "") +
                    "</span>" +
                "</span>" +
            "</summary>" + children +
        "</details></div>";
    }

    function renderTree(rootNode, loadingDepth, truncated, done) {
        var html = truncated ? '<p class="tree-notice">已达上限，结果可能不完整</p>' : "";
        if (loadingDepth !== null && loadingDepth !== undefined) {
            html += '<p class="tree-loading">正在加载第 ' + (loadingDepth + 1) + " 层…</p>";
        }
        html += '<div class="call-tree">' + treeNodeHtml(rootNode) + "</div>";
        if (done) html += '<p class="hint">点击节点标题可查看该条详情。</p>';
        setRelationBody(html);
    }

    function loadCallTree(root) {
        var seq = ++state.relationSeq;
        relationView("调用树", ICON.tools);
        var visited = {};
        visited[root.id] = true;
        var rootNode = { record: root, basis: null, children: [], depth: 0 };
        var all = [rootNode];
        var truncated = false;

        function nextLevel(level, depth) {
            if (!level.length) return Promise.resolve();
            // 深度上限：根为第 1 层（depth=0），达到 TREE_MAX_DEPTH 层即停止并提示。
            if (depth + 1 >= TREE_MAX_DEPTH) { truncated = true; return Promise.resolve(); }
            renderTree(rootNode, depth, truncated, false);
            var next = [];
            var chain = Promise.resolve();
            level.forEach(function (node) {
                chain = chain.then(function () {
                    if (seq !== state.relationSeq || truncated) return;
                    return fetchChildren(node.record).then(function (children) {
                        if (seq !== state.relationSeq) return;
                        node.children = [];
                        children.forEach(function (c) {
                            if (visited[c.record.id]) return;
                            if (all.length >= TREE_MAX_NODES) { truncated = true; return; }
                            visited[c.record.id] = true;
                            var child = { record: c.record, basis: c.basis, children: [], depth: depth + 1 };
                            node.children.push(child);
                            all.push(child);
                            next.push(child);
                        });
                    });
                });
            });
            return chain.then(function () {
                if (seq !== state.relationSeq || truncated) return;
                if (!next.length) return;
                return nextLevel(next, depth + 1);
            });
        }

        nextLevel([rootNode], 0).then(function () {
            if (seq !== state.relationSeq) return;
            renderTree(rootNode, null, truncated, true);
        }).catch(function (e) {
            if (seq !== state.relationSeq) return;
            setRelationBody(relationFailText(e));
            var retry = $("relationRetry");
            if (retry) retry.addEventListener("click", function () { loadCallTree(root); });
        });
    }

    function setMode(errors, doLoad) {
        state.errors = errors;
        $("modeToggle").querySelectorAll("button").forEach(function (b) {
            b.classList.toggle("active", (b.getAttribute("data-errors") === "1") === errors);
        });
        applyView();
        updateShortHint();
        syncSearchClear();
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
        if (el) openDetail(el.getAttribute("data-open"), el, el.getAttribute("data-shard") || null);
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

    $("searchInput").addEventListener("input", function () {
        $("searchCounter").textContent = this.value.length + "/" + this.maxLength;
        updateShortHint();
        syncSearchClear();
    });
    $("searchInput").addEventListener("keydown", function (e) {
        if (e.key !== "Enter") return;
        // 中文输入法组合态下回车用于选词，不应触发检索。
        if (e.isComposing || e.keyCode === 229) return;
        if (!state.errors) load(true);
    });
    $("searchClear").addEventListener("click", function () {
        $("searchInput").value = "";
        $("searchCounter").textContent = "0/" + $("searchInput").maxLength;
        updateShortHint();
        syncSearchClear();
        $("searchInput").focus();
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

    $("drawerBody").addEventListener("click", function (e) {
        var el = e.target.closest("[data-open]");
        if (!el) return;
        e.preventDefault();
        e.stopPropagation();
        openDetail(el.getAttribute("data-open"), el, el.getAttribute("data-shard") || null);
    });

    function hydrateFromUrl() {
        var p = new URLSearchParams(location.search);
        state.urlFilters = {};
        ["type", "backend", "status"].forEach(function (key) {
            var v = p.get(key);
            if (v !== null && v !== "") state.urlFilters[key] = v;
        });
        var searchScope = null;
        var searchValue = "";
        SCOPE_KEYS.forEach(function (key) {
            var v = p.get(key);
            if (v === null) return;
            if (searchScope === null) {
                searchScope = key;
                searchValue = v;
            } else if (!state.urlFilters[key]) {
                // 同一 URL 携带多个检索维度时，非主维度落入隐藏筛选，避免深链参数被丢弃。
                state.urlFilters[key] = v;
            }
        });
        var scope = p.get("scope");
        var scopeValue = p.get("value");
        if (scope && scopeValue !== null) {
            if (SCOPE_KEYS.indexOf(scope) >= 0) {
                if (searchScope === null) { searchScope = scope; searchValue = scopeValue; }
            } else if (URL_FILTER_KEYS.indexOf(scope) >= 0 && !state.urlFilters[scope]) {
                state.urlFilters[scope] = scopeValue;
            }
        }
        if (searchScope !== null) {
            $("scopeSelect").value = searchScope;
            $("searchInput").value = searchValue;
            $("searchCounter").textContent = searchValue.length + "/" + $("searchInput").maxLength;
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
