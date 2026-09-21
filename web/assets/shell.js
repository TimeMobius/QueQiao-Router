/* QueQiao Router — shared application shell runtime.
 * Owns theme persistence, nav state, and the window.QQShell page API.
 * Framework-free ES5-compatible. */
(function () {
    'use strict';

    var THEME_KEY = 'queqiao_dashboard_theme';
    var THEME_MODES = ['system', 'light', 'dark'];
    var media = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
    var handlers = [];

    function systemTheme() {
        return media && media.matches ? 'dark' : 'light';
    }

    function savedMode() {
        var stored = null;
        try { stored = localStorage.getItem(THEME_KEY); } catch (e) { stored = null; }
        return THEME_MODES.indexOf(stored) >= 0 ? stored : 'system';
    }

    function effectiveTheme(m) {
        return m === 'system' ? systemTheme() : m;
    }

    function current() {
        return effectiveTheme(savedMode());
    }

    function apply(m) {
        var theme = effectiveTheme(m);
        document.documentElement.setAttribute('data-theme', theme);
        return theme;
    }

    function emit(theme) {
        for (var i = 0; i < handlers.length; i++) {
            try { handlers[i](theme); } catch (e) { /* one bad handler must not break the rest */ }
        }
    }

    function setMode(m) {
        if (THEME_MODES.indexOf(m) < 0) m = 'system';
        try { localStorage.setItem(THEME_KEY, m); } catch (e) { /* private mode */ }
        var theme = apply(m);
        var select = document.getElementById('themeSelect');
        if (select) select.value = m;
        emit(theme);
        return theme;
    }

    function onThemeChange(fn) {
        if (typeof fn !== 'function') return function () {};
        handlers.push(fn);
        fn(current()); // invoke immediately with the current effective theme
        return function () { // unsubscribe
            var i = handlers.indexOf(fn);
            if (i >= 0) handlers.splice(i, 1);
        };
    }

    function defaultStatusText(state) {
        if (state === 'ok') return '系统运行中';
        if (state === 'down') return '指标不可用';
        return '检测中';
    }

    function setStatus(state, text) {
        var next = state === 'ok' || state === 'down' ? state : 'unknown';
        var badge = document.getElementById('appStatus');
        var label = document.getElementById('appStatusText');
        if (badge) badge.setAttribute('data-state', next);
        if (label) label.textContent = text != null ? text : defaultStatusText(next);
    }

    function pad2(n) { return n < 10 ? '0' + n : String(n); }

    function setUpdated(date) {
        var el = document.getElementById('appUpdated');
        if (!el) return;
        var d = date instanceof Date ? date : new Date();
        el.textContent = '最后更新 ' + pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
        el.setAttribute('datetime', d.toISOString());
    }

    function markNav() {
        var path = location.pathname.replace(/\/+$/, '') || '/';
        var links = document.querySelectorAll('.app-nav a');
        for (var i = 0; i < links.length; i++) {
            var href = (links[i].getAttribute('href') || '').replace(/\/+$/, '') || '/';
            if (href === path) {
                links[i].setAttribute('aria-current', 'page');
            } else {
                links[i].removeAttribute('aria-current');
            }
        }
    }

    function init() {
        var select = document.getElementById('themeSelect');
        var mode = savedMode();
        apply(mode);

        if (select) {
            select.value = mode;
            select.addEventListener('change', function () { setMode(this.value); });
        }

        if (media) {
            var onSystemChange = function () {
                if (savedMode() === 'system') {
                    apply('system');
                    emit(current());
                }
            };
            if (media.addEventListener) media.addEventListener('change', onSystemChange);
            else if (media.addListener) media.addListener(onSystemChange);
        }

        markNav();
    }

    window.QQShell = {
        current: current,
        onThemeChange: onThemeChange,
        setStatus: setStatus,
        setUpdated: setUpdated,
        setMode: setMode
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
