        function updateChartsTheme(theme) {
            const isDark = theme === 'dark';
            const textColor = isDark ? '#94a3b8' : '#64748b';
            const axisLineColor = isDark ? '#334155' : '#e2e8f0';
            const splitLineColor = isDark ? '#1e293b' : '#f1f5f9';
            const tooltipBg = isDark ? 'rgba(30, 41, 59, 0.95)' : 'rgba(255, 255, 255, 0.95)';
            const borderColor = isDark ? '#334155' : '#e2e8f0';

            const chartThemeOption = {
                textStyle: { color: textColor },
                backgroundColor: 'transparent'
            };

            Object.values(charts).forEach(chart => {
                if (!chart) return;
                const opts = chart.getOption();
                const baseOpt = Array.isArray(opts) ? opts[0] : opts;
                if (baseOpt) {

                    // Update tooltip
                    if (baseOpt.tooltip) {
                        baseOpt.tooltip.backgroundColor = tooltipBg;
                        baseOpt.tooltip.borderColor = borderColor;
                        baseOpt.tooltip.textStyle = { color: isDark ? '#f1f5f9' : '#334155' };
                    }

                    // Update legend
                    if (baseOpt.legend) {
                        baseOpt.legend.textStyle = { color: textColor };
                    }

                    // Update xAxis
                    if (baseOpt.xAxis) {
                        const xAxisArr = Array.isArray(baseOpt.xAxis) ? baseOpt.xAxis : [baseOpt.xAxis];
                        xAxisArr.forEach(ax => {
                            if (ax.axisLine) ax.axisLine.lineStyle = { color: axisLineColor };
                            if (ax.axisLabel) ax.axisLabel.color = textColor;
                            if (ax.splitLine) ax.splitLine.lineStyle = { color: splitLineColor };
                            if (ax.nameTextStyle) ax.nameTextStyle.color = textColor;
                        });
                    }

                    // Update yAxis
                    if (baseOpt.yAxis) {
                        const yAxisArr = Array.isArray(baseOpt.yAxis) ? baseOpt.yAxis : [baseOpt.yAxis];
                        yAxisArr.forEach(ax => {
                            if (ax.axisLine) ax.axisLine.lineStyle = { color: axisLineColor };
                            if (ax.axisLabel) ax.axisLabel.color = textColor;
                            if (ax.splitLine) ax.splitLine.lineStyle = { color: splitLineColor };
                            if (ax.nameTextStyle) ax.nameTextStyle.color = textColor;
                        });
                    }

                    // Update dataZoom
                    if (baseOpt.dataZoom) {
                        baseOpt.dataZoom.forEach(dz => {
                            if (dz.borderColor) dz.borderColor = borderColor;
                        });
                    }

                    chart.setOption(baseOpt);
                }
            });
        }

        // Global state
        let charts = {};
        let currentHistogramData = null;  // 当前 histogram 数据
        let historicalData = {
            activeRequests: [],
            timestamps: [],
            tpsByModel: {}  // { model: [tps1, tps2, ...] }
        };
        let currentModelList = [];
        const expandedModels = new Set();

        const METRICS_API = '/metrics';
        const REFRESH_INTERVAL = 5000;
        const MAX_HISTORY = 720;

        // Color palette
        const colors = {
            primary: '#3b82f6',
            success: '#10b981',
            warning: '#f59e0b',
            danger: '#ef4444',
            purple: '#8b5cf6',
            cyan: '#06b6d4',
            pink: '#ec4899',
            indigo: '#6366f1'
        };

        // Dynamic model color assignment - each model gets a consistent, distinct color
        const MODEL_PALETTE = [
            '#3b82f6', '#8b5cf6', '#10b981', '#f59e0b',
            '#ef4444', '#06b6d4', '#ec4899', '#f97316',
            '#6366f1', '#14b8a6', '#84cc16', '#a855f7',
            '#e11d48', '#0ea5e9', '#d946ef', '#22c55e',
            '#eab308', '#38bdf8', '#c084fc', '#fb923c'
        ];
        const assignedModelColors = {};
        let modelColorIndex = 0;

        function getModelColor(model) {
            if (!assignedModelColors[model]) {
                assignedModelColors[model] = MODEL_PALETTE[modelColorIndex % MODEL_PALETTE.length];
                modelColorIndex++;
            }
            return assignedModelColors[model];
        }

        // Toggle a model's backend detail rows, keeping the summary row visible.
        // Single-backend models have no expansion control and are never affected.
        function toggleModelExpansion(modelIndex) {
            const model = currentModelList[modelIndex];
            if (!model || model.backendCount <= 1) return;

            if (expandedModels.has(model.model)) {
                expandedModels.delete(model.model);
            } else {
                expandedModels.add(model.model);
            }
            renderModelTable(currentModelList);
        }

        // Render the model performance table. Summary rows are always shown;
        // backend detail rows only render for expanded multi-backend models.
        function renderModelTable(modelList) {
            const tbody = document.querySelector('#modelTable tbody');
            const formatCount = value => Math.round(value).toLocaleString();
            const renderSuccessRate = rate => {
                const boundedRate = Math.min(100, Math.max(0, rate));
                const color = boundedRate >= 90 ? 'green' : boundedRate >= 70 ? 'yellow' : 'red';
                return `
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <div class="progress-bar" style="width: 60px;">
                            <div class="fill ${color}" style="width: ${boundedRate}%"></div>
                        </div>
                        ${boundedRate.toFixed(1)}%
                    </div>`;
            };

            tbody.innerHTML = modelList.map((m, modelIndex) => {
                const mc = getModelColor(m.model);
                const isExpandable = m.backendCount > 1;
                const isExpanded = isExpandable && expandedModels.has(m.model);
                const modelLabel = isExpandable ? `
                    <button type="button" class="model-expand-button" data-model-index="${modelIndex}"
                        aria-expanded="${isExpanded}" aria-label="${isExpanded ? '收起' : '展开'} 后端详情">
                        <span class="model-expand-icon" aria-hidden="true">${isExpanded ? '−' : '+'}</span>
                        <span class="model-badge" style="background: ${mc}22; color: ${mc}; border-left: 3px solid ${mc};">${m.model}</span>
                    </button>` : `
                    <span class="model-badge" style="background: ${mc}22; color: ${mc}; border-left: 3px solid ${mc};">${m.model}</span>`;
                const summaryRow = `
                    <tr class="model-summary-row">
                        <td>
                            ${modelLabel}
                            <span class="model-meta">${m.backendCount} 个后端 · 模型合计</span>
                        </td>
                        <td><strong>${formatCount(m.total)}</strong></td>
                        <td><strong>${formatCount(m.active)}</strong></td>
                        <td>${renderSuccessRate(m.successRate)}</td>
                        <td>${m.latency > 0 ? m.latency.toFixed(2) + 's' : '-'}</td>
                        <td>${m.ttft > 0 ? m.ttft.toFixed(2) + 's' : '-'}</td>
                        <td>${m.tps > 0 ? m.tps.toFixed(1) : '-'}</td>
                    </tr>`;
                const backendRows = isExpanded ? m.backends.map(backend => `
                    <tr class="backend-row">
                        <td>
                            <span class="backend-label">${backend.backend}</span>
                            <span class="backend-meta">后端独立统计</span>
                        </td>
                        <td><strong>${formatCount(backend.total)}</strong></td>
                        <td>${formatCount(backend.active)}</td>
                        <td>${renderSuccessRate(backend.successRate)}</td>
                        <td>${backend.latency > 0 ? backend.latency.toFixed(2) + 's' : '-'}</td>
                        <td>${backend.ttft > 0 ? backend.ttft.toFixed(2) + 's' : '-'}</td>
                        <td>${backend.tps > 0 ? backend.tps.toFixed(1) : '-'}</td>
                    </tr>`).join('') : '';
                return summaryRow + backendRows;
            }).join('');
        }

        function lightenColor(hex, amount) {
            // Lighten a hex color by blending with white
            const num = parseInt(hex.replace('#', ''), 16);
            const r = Math.min(255, (num >> 16) + Math.round(255 * amount));
            const g = Math.min(255, ((num >> 8) & 0x00FF) + Math.round(255 * amount));
            const b = Math.min(255, (num & 0x0000FF) + Math.round(255 * amount));
            return `rgb(${r},${g},${b})`;
        }

        // Parse Prometheus metrics
        function parseMetrics(text) {
            const lines = text.split('\n');
            const metrics = {};

            lines.forEach(line => {
                if (!line || line.startsWith('#')) return;

                let name, labelsStr, value;

                // 匹配带大括号的指标
                const withLabels = line.match(/^(\w+){([^}]+)}\s+([\d.]+)/);
                if (withLabels) {
                    [, name, labelsStr, value] = withLabels;
                } else {
                    // 匹配不带大括标的指标 (如 histogram 的 _sum, _count)
                    const withoutLabels = line.match(/^(\w+)\s+([\d.]+)/);
                    if (!withoutLabels) return;
                    [, name, value] = withoutLabels;
                    labelsStr = '';
                }

                const labels = {};
                if (labelsStr) {
                    labelsStr.split(',').forEach(label => {
                        const [k, v] = label.split('=');
                        if (k && v) labels[k] = v.replace(/"/g, '');
                    });
                }

                if (!metrics[name]) metrics[name] = [];
                metrics[name].push({ labels, value: parseFloat(value) });
            });

            return metrics;
        }

        function calculateWeightedModelPerformance(backends, metric) {
            const weightedAverage = weightKey => {
                const weightedBackends = backends.filter(backend =>
                    Number.isFinite(backend[metric]) &&
                    Number.isFinite(backend[weightKey]) &&
                    backend[weightKey] > 0
                );
                const weightTotal = weightedBackends.reduce(
                    (sum, backend) => sum + backend[weightKey], 0
                );
                return weightTotal > 0
                    ? weightedBackends.reduce(
                        (sum, backend) => sum + backend[weightKey] * backend[metric], 0
                    ) / weightTotal
                    : null;
            };

            const activeWeighted = weightedAverage('active');
            if (activeWeighted !== null) return activeWeighted;

            const totalWeighted = weightedAverage('total');
            if (totalWeighted !== null) return totalWeighted;

            const positiveMetrics = backends
                .map(backend => backend[metric])
                .filter(value => Number.isFinite(value) && value > 0);
            return positiveMetrics.length > 0
                ? positiveMetrics.reduce((sum, value) => sum + value, 0) / positiveMetrics.length
                : 0;
        }

        // Process metrics into usable data
        function processMetrics(metrics) {
            const data = {
                activeRequests: {},      // 按 model 累加
                totalRequests: {},       // 按 model 累加 success/failed
                errors: {},              // 按 model 累加
                successRate: {},         // 按 model 计算
                latency: {},             // 按 backend performance 加权平均
                tokens: {},              // 按 model 累加
                tps: {},                 // 按 backend performance 加权平均
                ttft: {},                // 按 backend performance 加权平均
                backendStats: {},        // { model: { backend: { ... } } }
                histograms: {}           // { metricName: { model: { buckets: {}, sum: 0, count: 0 } } }
            };

            function getBackendStats(model, backend) {
                if (!data.backendStats[model]) data.backendStats[model] = {};
                if (!data.backendStats[model][backend]) {
                    data.backendStats[model][backend] = {
                        active: 0,
                        success: 0,
                        failed: 0,
                        latencySum: 0,
                        latencyCount: 0,
                        ttft: 0,
                        tps: 0
                    };
                }
                return data.backendStats[model][backend];
            }

            // 识别 histogram 指标的 base name (去掉 _bucket, _sum, _count 后缀)
            const histogramMetrics = {};
            Object.keys(metrics).forEach(name => {
                if (name.endsWith('_bucket') || name.endsWith('_sum') || name.endsWith('_count')) {
                    const baseName = name.replace(/_bucket|_sum|_count$/, '');
                    if (!histogramMetrics[baseName]) histogramMetrics[baseName] = true;
                }
            });

            // 处理每个 histogram
            Object.keys(histogramMetrics).forEach(baseName => {
                const bucketKey = baseName + '_bucket';
                const sumKey = baseName + '_sum';
                const countKey = baseName + '_count';

                if (!metrics[bucketKey] && !metrics[sumKey] && !metrics[countKey]) return;

                data.histograms[baseName] = {};

                const isTokenDist = baseName === 'gpt_api_token_distribution';

                // 按 model 聚合 bucket 数据（修复：累加多个backend的相同model数据）
                if (metrics[bucketKey]) {
                    metrics[bucketKey].forEach(m => {
                        const model = m.labels.model || 'unknown';
                        const le = m.labels.le;
                        const type = m.labels.type || 'unknown';

                        const key = isTokenDist ? `${model}::${type}` : model;

                        if (!data.histograms[baseName][key]) {
                            data.histograms[baseName][key] = { buckets: {}, sum: 0, count: 0 };
                        }
                        if (le !== undefined) {
                            if (isTokenDist) {
                                const bucketKey2 = `type="${type}",le="${le}"`;
                                data.histograms[baseName][key].buckets[bucketKey2] = 
                                    (data.histograms[baseName][key].buckets[bucketKey2] || 0) + m.value;
                            } else {
                                data.histograms[baseName][key].buckets[le] = 
                                    (data.histograms[baseName][key].buckets[le] || 0) + m.value;
                            }
                        }
                    });
                }

                // 聚合 sum 和 count（修复：累加多个backend的相同model数据）
                if (metrics[sumKey]) {
                    metrics[sumKey].forEach(m => {
                        const model = m.labels.model || 'unknown';
                        if (!data.histograms[baseName][model]) {
                            data.histograms[baseName][model] = { buckets: {}, sum: 0, count: 0 };
                        }
                        data.histograms[baseName][model].sum += m.value;
                    });
                }
                if (metrics[countKey]) {
                    metrics[countKey].forEach(m => {
                        const model = m.labels.model || 'unknown';
                        if (!data.histograms[baseName][model]) {
                            data.histograms[baseName][model] = { buckets: {}, sum: 0, count: 0 };
                        }
                        data.histograms[baseName][model].count += m.value;
                    });
                }
            });

            // Active requests - 同时按 model 和 backend 统计
            if (metrics.gpt_api_active_requests) {
                metrics.gpt_api_active_requests.forEach(m => {
                    const model = m.labels.model || 'unknown';
                    const backend = m.labels.backend || 'unknown';
                    const backendData = getBackendStats(model, backend);
                    backendData.active += m.value;
                    if (!data.activeRequests[model]) data.activeRequests[model] = 0;
                    data.activeRequests[model] += m.value;
                });
            }

            // Total requests - 同时按 model 和 backend 统计
            if (metrics.gpt_api_requests_total) {
                metrics.gpt_api_requests_total.forEach(m => {
                    const model = m.labels.model || 'unknown';
                    const backend = m.labels.backend || 'unknown';
                    const status = m.labels.status;
                    const backendData = getBackendStats(model, backend);
                    if (!data.totalRequests[model]) data.totalRequests[model] = { success: 0, failed: 0 };
                    const isSuccess = /^(2|3)\d\d$/.test(status);
                    if (isSuccess) {
                        data.totalRequests[model].success += m.value;
                        backendData.success += m.value;
                    } else {
                        data.totalRequests[model].failed += m.value;
                        backendData.failed += m.value;
                    }
                });
            }

            // Errors - 按 model 累加
            if (metrics.gpt_api_errors_total) {
                metrics.gpt_api_errors_total.forEach(m => {
                    const model = m.labels.model;
                    const errorType = m.labels.error_type;
                    if (!data.errors[model]) data.errors[model] = {};
                    if (!data.errors[model][errorType]) data.errors[model][errorType] = 0;
                    data.errors[model][errorType] += m.value;
                });
            }

            // Success rate (1m) - 按 model 累加后计算
            if (metrics.gpt_api_success_rate_1m) {
                metrics.gpt_api_success_rate_1m.forEach(m => {
                    const model = m.labels.model;
                    if (!data.successRate[model]) data.successRate[model] = { total: 0, count: 0 };
                    data.successRate[model].total += m.value * 100;
                    data.successRate[model].count += 1;
                });
                // 计算平均值
                Object.keys(data.successRate).forEach(model => {
                    data.successRate[model] = data.successRate[model].total / data.successRate[model].count;
                });
            }

            // 如果 totalRequests 有数据，覆盖 successRate（更准确）
            Object.keys(data.totalRequests).forEach(model => {
                const req = data.totalRequests[model];
                const total = req.success + req.failed;
                if (total > 0) {
                    data.successRate[model] = (req.success / total) * 100;
                }
            });

            // Latency - 按 backend 统计 sum 和 count，model 值稍后统一计算
            if (metrics.gpt_api_latency_seconds_count && metrics.gpt_api_latency_seconds_sum) {
                metrics.gpt_api_latency_seconds_count.forEach(m => {
                    const model = m.labels.model || 'unknown';
                    const backend = m.labels.backend || 'unknown';
                    const sumMetric = metrics.gpt_api_latency_seconds_sum.find(s => 
                        s.labels.backend === m.labels.backend && s.labels.model === m.labels.model
                    );
                    if (sumMetric && m.value > 0) {
                        const backendData = getBackendStats(model, backend);
                        backendData.latencySum += sumMetric.value;
                        backendData.latencyCount += m.value;
                    }
                });
            }

            // Tokens - 按 model 累加
            if (metrics.gpt_api_tokens_total) {
                metrics.gpt_api_tokens_total.forEach(m => {
                    const model = m.labels.model;
                    if (!data.tokens[model]) data.tokens[model] = { prompt: 0, completion: 0 };
                    if (m.labels.type === 'prompt') data.tokens[model].prompt += m.value;
                    if (m.labels.type === 'completion') data.tokens[model].completion += m.value;
                });
            }

            // TPS - 同时按 model 和 backend 统计
            if (metrics.gpt_api_tps_1m_avg) {
                metrics.gpt_api_tps_1m_avg.forEach(m => {
                    const model = m.labels.model || 'unknown';
                    const backend = m.labels.backend || 'unknown';
                    getBackendStats(model, backend).tps += m.value;
                });
            }

            // TTFT - 同时按 model 和 backend 取最大值
            if (metrics.gpt_api_ttft_1m_max) {
                metrics.gpt_api_ttft_1m_max.forEach(m => {
                    const model = m.labels.model || 'unknown';
                    const backend = m.labels.backend || 'unknown';
                    const backendData = getBackendStats(model, backend);
                    backendData.ttft = Math.max(backendData.ttft, m.value);
                });
            }

            Object.entries(data.backendStats).forEach(([model, backendStats]) => {
                const backends = Object.values(backendStats).map(stats => ({
                    active: stats.active,
                    total: stats.success + stats.failed,
                    latency: stats.latencyCount > 0 ? stats.latencySum / stats.latencyCount : 0,
                    ttft: stats.ttft,
                    tps: stats.tps
                }));
                data.latency[model] = calculateWeightedModelPerformance(backends, 'latency');
                data.ttft[model] = calculateWeightedModelPerformance(backends, 'ttft');
                data.tps[model] = calculateWeightedModelPerformance(backends, 'tps');
            });

            return data;
        }

        // Initialize charts - 增强交互版本
        function initCharts() {
            // Model Distribution Pie - 交互式饼图
            charts.modelDistribution = echarts.init(document.getElementById('modelDistribution'));
            charts.modelDistribution.setOption({
                tooltip: { 
                    trigger: 'item', 
                    formatter: '{b}<br/>请求数: {c} ({d}%)',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;'
                },
                legend: { 
                    orient: 'vertical', 
                    right: 10, 
                    top: 'center', 
                    textStyle: { color: '#64748b' },
                    selectedMode: true
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        saveAsImage: { title: '保存', name: 'model_distribution' },
                        dataView: { title: '数据', readOnly: true, lang: ['数据视图', '关闭', '刷新'] }
                    }
                },
                series: [{
                    type: 'pie',
                    radius: ['35%', '55%'],
                    center: ['30%', '50%'],
                    avoidLabelOverlap: true,
                    itemStyle: { 
                        borderRadius: 8, 
                        borderColor: '#fff', 
                        borderWidth: 2,
                        shadowBlur: 10,
                        shadowColor: 'rgba(0,0,0,0.1)'
                    },
                    label: { show: false },
                    emphasis: {
                        scale: true,
                        scaleSize: 10,
                        itemStyle: {
                            shadowBlur: 20,
                            shadowColor: 'rgba(0,0,0,0.3)'
                        },
                        label: { show: true, fontSize: 14, fontWeight: 'bold' }
                    },
                    select: {
                        itemStyle: { shadowBlur: 20, shadowColor: 'rgba(0,0,0,0.3)' }
                    },
                    data: []
                }]
            });

            // 点击事件
            charts.modelDistribution.on('click', function(params) {
                showNotification(`点击模型: ${params.name}<br/>请求量: ${params.value}`);
            });

            // Active Requests Trend - 交互式折线图
            charts.activeRequestsTrend = echarts.init(document.getElementById('activeRequestsTrend'));
            charts.activeRequestsTrend.setOption({
                tooltip: { 
                    trigger: 'axis',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;',
                    axisPointer: { type: 'cross', crossStyle: { color: '#999' } }
                },
                legend: { 
                    textStyle: { color: '#64748b' }, 
                    top: 0,
                    selectedMode: true
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        magicType: { type: ['line', 'bar', 'stack'], title: { line: '折线', bar: '柱状', stack: '堆叠' } },
                        restore: { title: '重置' },
                        saveAsImage: { title: '保存', name: 'active_requests' }
                    }
                },
                dataZoom: [
                    { type: 'slider', show: true, xAxisIndex: 0, start: 80, end: 100, height: 20, bottom: 5, borderColor: '#e2e8f0', fillerColor: 'rgba(59,130,246,0.2)' },
                    { type: 'inside', xAxisIndex: 0 }
                ],
                grid: { left: 50, right: 20, top: 60, bottom: 50 },
                xAxis: { 
                    type: 'category', 
                    data: [], 
                    axisLine: { lineStyle: { color: '#e2e8f0' } }, 
                    axisLabel: { color: '#94a3b8' },
                    axisTick: { show: true }
                },
                yAxis: { 
                    type: 'value', 
                    axisLine: { show: false }, 
                    splitLine: { lineStyle: { color: '#f1f5f9' } }, 
                    axisLabel: { color: '#94a3b8' }
                },
                series: []
            });

            charts.activeRequestsTrend.on('click', function(params) {
                showNotification(`点击: ${params.seriesName}<br/>时间: ${params.name}<br/>活跃请求: ${params.value}`);
            });

            // Latency Chart - 可排序的柱状图
            charts.latencyChart = echarts.init(document.getElementById('latencyChart'));
            charts.latencyChart.setOption({
                tooltip: { 
                    trigger: 'axis',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;'
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        saveAsImage: { title: '保存', name: 'latency' }
                    }
                },
                grid: { left: 70, right: 20, top: 20, bottom: 30 },
                xAxis: { 
                    type: 'category', 
                    data: [], 
                    axisLine: { lineStyle: { color: '#e2e8f0' } }, 
                    axisLabel: { color: '#94a3b8', rotate: 15, interval: 0 },
                    axisTick: { show: false }
                },
                yAxis: { 
                    type: 'value', 
                    name: 's',
                    nameTextStyle: { color: '#94a3b8' },
                    axisLine: { show: false }, 
                    splitLine: { lineStyle: { color: '#f1f5f9' } }, 
                    axisLabel: { color: '#94a3b8' }
                },
                series: [{
                    type: 'bar',
                    barWidth: '50%',
                    itemStyle: { 
                        borderRadius: [6, 6, 0, 0],
                        shadowBlur: 5,
                        shadowColor: 'rgba(0,0,0,0.1)'
                    },
                    emphasis: {
                        itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' }
                    },
                    data: []
                }]
            });

            charts.latencyChart.on('click', function(params) {
                showNotification(`模型: ${params.name}<br/>平均延迟: ${params.value} s`);
            });

            // TTFT Chart - similar to latency but with pink/purple theme
            charts.ttftChart = echarts.init(document.getElementById('ttftChart'));
            charts.ttftChart.setOption({
                tooltip: { 
                    trigger: 'axis',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;'
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        saveAsImage: { title: '保存', name: 'ttft' }
                    }
                },
                grid: { left: 70, right: 20, top: 20, bottom: 30 },
                xAxis: { 
                    type: 'category', 
                    data: [], 
                    axisLine: { lineStyle: { color: '#e2e8f0' } }, 
                    axisLabel: { color: '#94a3b8', rotate: 15, interval: 0 },
                    axisTick: { show: false }
                },
                yAxis: { 
                    type: 'value', 
                    name: 's',
                    nameTextStyle: { color: '#94a3b8' },
                    axisLine: { show: false }, 
                    splitLine: { lineStyle: { color: '#f1f5f9' } }, 
                    axisLabel: { color: '#94a3b8' }
                },
                series: [{
                    type: 'bar',
                    barWidth: '50%',
                    itemStyle: { 
                        borderRadius: [6, 6, 0, 0],
                        shadowBlur: 5,
                        shadowColor: 'rgba(236, 72, 153, 0.2)',
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: '#ec4899' },
                            { offset: 1, color: '#f472b6' }
                        ])
                    },
                    emphasis: {
                        itemStyle: { shadowBlur: 10, shadowColor: 'rgba(236, 72, 153, 0.4)' }
                    },
                    data: []
                }]
            });

            charts.ttftChart.on('click', function(params) {
                showNotification(`模型: ${params.name}<br/>TTFT: ${params.value} s`);
            });

            // Success Rate - 带阈值颜色
            charts.successRateChart = echarts.init(document.getElementById('successRateChart'));
            charts.successRateChart.setOption({
                tooltip: { 
                    trigger: 'axis',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;',
                    formatter: function(params) {
                        const val = params[0];
                        let status = val.value >= 90 ? '优秀' : val.value >= 70 ? '警告' : '危险';
                        return `<strong>${val.name}</strong><br/>成功率: ${val.value.toFixed(1)}%<br/>状态: ${status}`;
                    }
                },
                visualMap: {
                    show: false,
                    pieces: [
                        { gt: 90, lte: 100, color: colors.success },
                        { gt: 70, lte: 90, color: colors.warning },
                        { gt: 0, lte: 70, color: colors.danger }
                    ]
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        saveAsImage: { title: '保存', name: 'success_rate' }
                    }
                },
                grid: { left: 70, right: 20, top: 20, bottom: 30 },
                xAxis: { 
                    type: 'category', 
                    data: [], 
                    axisLine: { lineStyle: { color: '#e2e8f0' } }, 
                    axisLabel: { color: '#94a3b8', rotate: 15, interval: 0 },
                    axisTick: { show: false }
                },
                yAxis: { 
                    type: 'value', 
                    max: 100,
                    axisLine: { show: false }, 
                    splitLine: { lineStyle: { color: '#f1f5f9' } }, 
                    axisLabel: { color: '#94a3b8', formatter: '{value}%' },
                    name: '%',
                    nameTextStyle: { color: '#94a3b8' }
                },
                series: [{
                    type: 'bar',
                    barWidth: '50%',
                    itemStyle: { 
                        borderRadius: [6, 6, 0, 0],
                        shadowBlur: 5,
                        shadowColor: 'rgba(0,0,0,0.1)'
                    },
                    emphasis: {
                        itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' }
                    },
                    markLine: {
                        symbol: ['none', 'none'],
                        label: { show: true, position: 'end', formatter: '{b}' },
                        data: [
                            { yAxis: 90, name: '优秀线', lineStyle: { color: colors.success, type: 'dashed' } },
                            { yAxis: 70, name: '警告线', lineStyle: { color: colors.warning, type: 'dashed' } }
                        ]
                    },
                    data: []
                }]
            });

            charts.successRateChart.on('click', function(params) {
                const status = params.value >= 90 ? '优秀' : params.value >= 70 ? '警告' : '危险';
                showNotification(`模型: ${params.name}<br/>成功率: ${params.value.toFixed(1)}%<br/>状态: ${status}`);
            });

            // Error Distribution - 南丁格尔玫瑰图
            charts.errorDistribution = echarts.init(document.getElementById('errorDistribution'));
            charts.errorDistribution.setOption({
                tooltip: { 
                    trigger: 'item',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;'
                },
                legend: { 
                    orient: 'vertical', 
                    right: 10, 
                    top: 'center', 
                    textStyle: { color: '#64748b' }
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        saveAsImage: { title: '保存', name: 'errors' },
                        dataView: { title: '数据', readOnly: true }
                    }
                },
                series: [{
                    type: 'pie',
                    radius: ['25%', '45%'],
                    center: ['30%', '50%'],
                    roseType: 'radius',
                    data: [],
                    itemStyle: { 
                        borderRadius: 6, 
                        borderColor: '#fff', 
                        borderWidth: 2,
                        shadowBlur: 10,
                        shadowColor: 'rgba(0,0,0,0.1)'
                    },
                    emphasis: {
                        scale: true,
                        scaleSize: 10,
                        itemStyle: {
                            shadowBlur: 20,
                            shadowColor: 'rgba(0,0,0,0.3)'
                        },
                        label: { show: true, fontSize: 12, fontWeight: 'bold' }
                    },
                    label: {
                        show: true,
                        formatter: '{b}: {c}',
                        color: '#64748b'
                    }
                }]
            });

            charts.errorDistribution.on('click', function(params) {
                showNotification(`错误: ${params.name}<br/>数量: ${params.value}`);
            });

            // Token Chart - 堆叠柱状图（每个模型一个柱）+ 对数刻度
            charts.tokenChart = echarts.init(document.getElementById('tokenChart'));
            charts.tokenChart.setOption({
                tooltip: { 
                    trigger: 'axis',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;',
                    axisPointer: { type: 'cross', crossStyle: { color: '#999' } },
                    formatter: function(params) {
                        let result = `<strong>${params[0].name}</strong><br/>`;
                        params.forEach(p => {
                            const val = p.value;
                            let displayVal = val >= 1000000 ? (val/1000000).toFixed(2) + 'M' : 
                                           val >= 1000 ? (val/1000).toFixed(1) + 'K' : val;
                            result += `${p.marker} ${p.seriesName}: ${displayVal}<br/>`;
                        });
                        const total = params.reduce((sum, p) => sum + p.value, 0);
                        const displayTotal = total >= 1000000 ? (total/1000000).toFixed(2) + 'M' : 
                                           total >= 1000 ? (total/1000).toFixed(1) + 'K' : total;
                        result += `<hr style="margin: 4px 0; border:none; border-top:1px solid #e2e8f0;">`;
                        result += `合计: ${displayTotal}`;
                        return result;
                    }
                },
                legend: { 
                    textStyle: { color: '#64748b' }, 
                    top: 0
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        magicType: { type: ['line', 'bar'], title: { line: '折线', bar: '柱状' } },
                        restore: { title: '重置' },
                        saveAsImage: { title: '保存', name: 'tokens' }
                    }
                },
                grid: { left: 100, right: 20, top: 60, bottom: 30 },
                xAxis: { 
                    type: 'category', 
                    data: [], 
                    axisLine: { lineStyle: { color: '#e2e8f0' } }, 
                    axisLabel: { color: '#94a3b8', rotate: 15 },
                    axisTick: { show: false }
                },
                yAxis: { 
                    type: 'log', 
                    min: 1,  // 最小值设为1，避免log(0)
                    axisLine: { show: false }, 
                    splitLine: { lineStyle: { color: '#f1f5f9' } }, 
                    axisLabel: { 
                        color: '#94a3b8',
                        formatter: function(value) {
                            if (value >= 1000000) return (value/1000000).toFixed(0) + 'M';
                            if (value >= 1000) return (value/1000).toFixed(0) + 'K';
                            return value;
                        }
                    },
                    name: 'Tokens (对数刻度)',
                    nameTextStyle: { color: '#64748b', padding: [0, 0, 0, 20] }
                },
                series: []
            });

            charts.tokenChart.on('click', function(params) {
                showNotification(`模型: ${params.name}<br/>类型: ${params.seriesName}<br/>数量: ${params.value.toLocaleString()}`);
            });

            // TPS Trend - 带缩放
            charts.tpsTrend = echarts.init(document.getElementById('tpsTrend'));
            charts.tpsTrend.setOption({
                tooltip: { 
                    trigger: 'axis',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;'
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        magicType: { type: ['line', 'bar', 'stack'], title: { line: '折线', bar: '柱状', stack: '堆叠' } },
                        restore: { title: '重置' },
                        saveAsImage: { title: '保存', name: 'tps_trend' }
                    }
                },
                dataZoom: [
                    { type: 'slider', show: true, xAxisIndex: 0, start: 80, end: 100, height: 20, bottom: 5, borderColor: '#e2e8f0', fillerColor: 'rgba(139,92,246,0.2)' },
                    { type: 'inside', xAxisIndex: 0 }
                ],
                grid: { left: 50, right: 20, top: 40, bottom: 50 },
                xAxis: { 
                    type: 'category', 
                    data: [], 
                    axisLine: { lineStyle: { color: '#e2e8f0' } }, 
                    axisLabel: { color: '#94a3b8' },
                    axisTick: { show: true }
                },
                yAxis: { 
                    type: 'value', 
                    axisLine: { show: false }, 
                    splitLine: { lineStyle: { color: '#f1f5f9' } }, 
                    axisLabel: { color: '#94a3b8' },
                    name: 'Tokens/s',
                    nameTextStyle: { color: '#94a3b8' }
                },
                series: [{
                    type: 'line',
                    smooth: true,
                    symbol: 'circle',
                    symbolSize: 8,
                    showSymbol: false,
                    itemStyle: { 
                        color: colors.purple,
                        borderColor: '#fff',
                        borderWidth: 2
                    },
                    lineStyle: { width: 3 },
                    areaStyle: { 
                        opacity: 0.4,
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: 'rgba(139, 92, 246, 0.5)' },
                            { offset: 1, color: 'rgba(139, 92, 246, 0.05)' }
                        ])
                    },
                    emphasis: {
                        showSymbol: true,
                        itemStyle: {
                            borderWidth: 3,
                            shadowBlur: 10,
                            shadowColor: 'rgba(139,92,246,0.5)'
                        }
                    },
                    data: []
                }]
            });

            charts.tpsTrend.on('click', function(params) {
                showNotification(`时间: ${params.name}<br/>TPS: ${params.value.toFixed(1)}`);
            });

            // Histogram Chart - 展示直方图分布（柱状图）
            charts.histogramChart = echarts.init(document.getElementById('histogramChart'));
            charts.histogramChart.setOption({
                tooltip: { 
                    trigger: 'axis',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;',
                    formatter: function(params) {
                        const p = params[0];
                        return `<strong>${p.name}</strong><br/>请求数: ${p.value}`;
                    }
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        saveAsImage: { title: '保存', name: 'histogram' }
                    }
                },
                grid: { left: 60, right: 30, top: 30, bottom: 40 },
                xAxis: { 
                    type: 'category', 
                    data: [],
                    axisLine: { lineStyle: { color: '#e2e8f0' } }, 
                    axisLabel: { color: '#94a3b8', rotate: 45 },
                    axisTick: { show: false }
                },
                yAxis: { 
                    type: 'value', 
                    name: '请求数',
                    nameTextStyle: { color: '#94a3b8' },
                    axisLine: { show: false }, 
                    splitLine: { lineStyle: { color: '#f1f5f9' } }, 
                    axisLabel: { color: '#94a3b8' }
                },
                series: [{
                    type: 'bar',
                    barWidth: '80%',
                    data: [],
                    itemStyle: { 
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: '#3b82f6' },
                            { offset: 1, color: '#60a5fa' }
                        ]),
                        borderRadius: [4, 4, 0, 0]
                    },
                    emphasis: {
                        itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' }
                    }
                }]
            });

            charts.tokenDistChart = echarts.init(document.getElementById('tokenDistChart'));
            charts.tokenDistChart.setOption({
                tooltip: { 
                    trigger: 'axis',
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    textStyle: { color: '#334155' },
                    extraCssText: 'box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-radius: 8px;',
                    formatter: function(params) {
                        const p = params[0];
                        return `<strong>${p.name}</strong><br/>请求数: ${p.value}`;
                    }
                },
                legend: { 
                    textStyle: { color: '#64748b' }, 
                    top: 0,
                    data: ['Prompt', 'Completion']
                },
                toolbox: {
                    right: 10,
                    top: 0,
                    feature: {
                        saveAsImage: { title: '保存', name: 'token_distribution' }
                    }
                },
                grid: { left: 60, right: 30, top: 50, bottom: 40 },
                xAxis: { 
                    type: 'category', 
                    data: [],
                    axisLine: { lineStyle: { color: '#e2e8f0' } }, 
                    axisLabel: { color: '#94a3b8', rotate: 15, interval: 0 },
                    axisTick: { show: false }
                },
                yAxis: { 
                    type: 'value', 
                    name: '请求数',
                    nameTextStyle: { color: '#94a3b8' },
                    axisLine: { show: false }, 
                    splitLine: { lineStyle: { color: '#f1f5f9' } }, 
                    axisLabel: { color: '#94a3b8' }
                },
                series: [
                    {
                        name: 'Prompt',
                        type: 'bar',
                        barWidth: '40%',
                        data: [],
                        itemStyle: { 
                            color: colors.primary,
                            borderRadius: [4, 4, 0, 0]
                        },
                        emphasis: {
                            itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' }
                        }
                    },
                    {
                        name: 'Completion',
                        type: 'bar',
                        barWidth: '40%',
                        data: [],
                        itemStyle: { 
                            color: colors.purple,
                            borderRadius: [4, 4, 0, 0]
                        },
                        emphasis: {
                            itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' }
                        }
                    }
                ]
            });

            document.getElementById('tokenDistModel').addEventListener('change', function() {
                updateTokenDistChart();
            });

            // Histogram 下拉框事件绑定
            document.getElementById('histogramMetric').addEventListener('change', function() {
                updateHistogramChart();
            });
            document.getElementById('histogramModel').addEventListener('change', function() {
                updateHistogramChart();
            });

            // Window resize handler
            window.addEventListener('resize', () => {
                Object.values(charts).forEach(chart => chart.resize());
            });
        }

        // 通知提示函数
        function showNotification(message) {
            const notification = document.createElement('div');
            const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
            notification.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                background: ${isDark ? '#1e293b' : 'white'};
                border: 1px solid ${isDark ? '#334155' : '#e2e8f0'};
                border-radius: 8px;
                padding: 16px 20px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.15);
                z-index: 10000;
                max-width: 300px;
                font-size: 13px;
                color: ${isDark ? '#f1f5f9' : '#334155'};
                animation: slideIn 0.3s ease;
            `;
            notification.innerHTML = message;
            document.body.appendChild(notification);

            setTimeout(() => {
                notification.style.animation = 'slideOut 0.3s ease';
                setTimeout(() => notification.remove(), 300);
            }, 3000);
        }

        // 添加动画样式
        const style = document.createElement('style');
        style.textContent = `
            @keyframes slideIn {
                from { transform: translateX(100px); opacity: 0; }
                to { transform: translateX(0); opacity: 1; }
            }
            @keyframes slideOut {
                from { transform: translateX(0); opacity: 1; }
                to { transform: translateX(100px); opacity: 0; }
            }
        `;
        document.head.appendChild(style);

        // Update charts with data
        function updateCharts(data) {
            const now = new Date();
            const timeStr = now.toLocaleTimeString('zh-CN');
            if (window.QQShell) QQShell.setUpdated(now);

            // 计算 stats
            let totalReq = 0;
            let totalSuccess = 0;
            const modelNames = new Set([
                ...Object.keys(data.backendStats),
                ...Object.keys(data.totalRequests),
                ...Object.keys(data.activeRequests)
            ]);
            const modelList = [...modelNames].sort().map(model => {
                const reqData = data.totalRequests[model] || { success: 0, failed: 0 };
                const success = reqData.success;
                const failed = reqData.failed;
                totalReq += success + failed;
                totalSuccess += success;

                const backendEntries = Object.entries(data.backendStats[model] || {})
                    .sort(([a], [b]) => a.localeCompare(b));
                const backends = backendEntries.length > 0 ? backendEntries.map(([backend, stats]) => {
                    const backendTotal = stats.success + stats.failed;
                    return {
                        backend,
                        active: stats.active,
                        total: backendTotal,
                        successRate: backendTotal > 0 ? stats.success / backendTotal * 100 : 0,
                        latency: stats.latencyCount > 0 ? stats.latencySum / stats.latencyCount : 0,
                        ttft: stats.ttft,
                        tps: stats.tps
                    };
                }) : [{
                    backend: 'unknown',
                    active: data.activeRequests[model] || 0,
                    total: success + failed,
                    successRate: success + failed > 0 ? success / (success + failed) * 100 : 0,
                    latency: data.latency[model] || 0,
                    ttft: data.ttft[model] || 0,
                    tps: data.tps[model] || 0
                }];

                return {
                    model,
                    total: success + failed,
                    active: data.activeRequests[model] || 0,
                    backendCount: backends.length,
                    successRate: success + failed > 0 ? success / (success + failed) * 100 : (data.successRate[model] || 0),
                    latency: data.latency[model] || 0,
                    ttft: data.ttft[model] || 0,
                    tps: data.tps[model] || 0,
                    backends
                };
            });

            const overallSuccessRate = totalReq > 0 ? (totalSuccess / totalReq * 100).toFixed(1) : 0;
            const avgLatency = Object.values(data.latency).reduce((a, b) => a + b, 0) / Math.max(Object.keys(data.latency).length, 1);
            // 计算平均 TTFT
            const ttftModels = Object.keys(data.ttft);
            const avgTtft = ttftModels.length > 0 ? Object.values(data.ttft).reduce((a, b) => a + b, 0) / ttftModels.length : 0;
            // 只对有活跃请求的模型 TPS 进行加权平均
            let totalWeightedTps = 0;
            let totalActive = 0;
            Object.keys(data.activeRequests).forEach(model => {
                const active = data.activeRequests[model] || 0;
                const tps = data.tps[model] || 0;
                if (active > 0) {
                    totalWeightedTps += tps * active;
                    totalActive += active;
                }
            });
            const avgTps = totalActive > 0 ? totalWeightedTps / totalActive : 0;

            // Update stat cards
            document.getElementById('totalRequests').textContent = totalReq.toLocaleString();
            document.getElementById('overallSuccess').textContent = overallSuccessRate + '%';
            document.getElementById('avgLatency').textContent = avgLatency.toFixed(2) + 's';
            document.getElementById('avgTtft').textContent = avgTtft.toFixed(2) + 's';
            document.getElementById('avgTps').textContent = avgTps.toFixed(0) + '/s';

            // 保存当前 histogram 数据并更新下拉框
            currentHistogramData = data;
            if (data.histograms) {
                const metricSelect = document.getElementById('histogramMetric');
                const modelSelect = document.getElementById('histogramModel');

                // 获取所有可用的 histogram 指标（过滤掉tps和token_distribution相关指标）
                const availableMetrics = Object.keys(data.histograms).filter(m => !m.includes('tps') && !m.includes('token_distribution'));
                const metricOptions = availableMetrics.map(m => {
                    const displayName = m.replace('gpt_api_', '').replace(/_seconds$/, '');
                    return `<option value="${m}">${displayName}</option>`;
                }).join('');

                // 获取所有可用的模型（从任意 histogram 指标中提取）
                const availableModels = new Set();
                availableMetrics.forEach(metric => {
                    Object.keys(data.histograms[metric]).forEach(model => availableModels.add(model));
                });
                const modelOptions = [...availableModels].map(m => `<option value="${m}">${m}</option>`).join('');

                // 只有在选项为空时才更新（避免切换时重置选择）
                if (!metricSelect.dataset.initialized) {
                    metricSelect.innerHTML = '<option value="">选择指标</option>' + metricOptions;
                    modelSelect.innerHTML = '<option value="">选择模型</option>' + modelOptions;
                    metricSelect.dataset.initialized = 'true';

                    // 默认选择第一个
                    if (availableMetrics.length > 0) {
                        metricSelect.value = availableMetrics[0];
                    }
                    if (availableModels.size > 0) {
                        modelSelect.value = [...availableModels][0];
                    }

                    // 触发图表更新
                    updateHistogramChart();
                }

                const tokenDistSelect = document.getElementById('tokenDistModel');
                if (data.histograms['gpt_api_token_distribution']) {
                    const tokenDistModels = Object.keys(data.histograms['gpt_api_token_distribution'])
                        .map(m => m.split('::')[0])
                        .filter((v, i, a) => a.indexOf(v) === i);

                    const tokenDistOptions = tokenDistModels.map(m => `<option value="${m}">${m}</option>`).join('');

                    if (!tokenDistSelect.dataset.initialized) {
                        tokenDistSelect.innerHTML = '<option value="">选择模型</option>' + tokenDistOptions;
                        tokenDistSelect.dataset.initialized = 'true';

                        if (tokenDistModels.length > 0) {
                            tokenDistSelect.value = tokenDistModels[0];
                            updateTokenDistChart();
                        }
                    } else {
                        const currentValue = tokenDistSelect.value;
                        tokenDistSelect.innerHTML = '<option value="">选择模型</option>' + tokenDistOptions;
                        if (tokenDistModels.includes(currentValue)) {
                            tokenDistSelect.value = currentValue;
                        } else if (tokenDistModels.length > 0) {
                            tokenDistSelect.value = tokenDistModels[0];
                        }
                        updateTokenDistChart();
                    }
                }
            }

            // Model distribution pie
            const modelDistData = Object.entries(data.totalRequests).map(([model, req]) => ({
                name: model,
                value: req.success + req.failed,
                itemStyle: { color: getModelColor(model) }
            })).filter(d => d.value > 0);

            charts.modelDistribution.setOption({
                series: [{ data: modelDistData }]
            });

            // Active requests trend
            historicalData.timestamps.push(timeStr);
            historicalData.activeRequests.push({ ...data.activeRequests });
            if (historicalData.timestamps.length > MAX_HISTORY) {
                historicalData.timestamps.shift();
                historicalData.activeRequests.shift();
            }

            const allModels = [...new Set(historicalData.activeRequests.flatMap(d => Object.keys(d)))];
            const seriesData = allModels.map(model => ({
                name: model,
                type: 'line',
                smooth: true,
                symbol: 'circle',
                symbolSize: 6,
                showSymbol: false,
                data: historicalData.activeRequests.map(d => d[model] || 0),
                itemStyle: { color: getModelColor(model) }
            }));

            charts.activeRequestsTrend.setOption({
                xAxis: { data: historicalData.timestamps },
                series: seriesData
            });

            // Latency chart
            const latencyData = Object.entries(data.latency).map(([model, val]) => ({
                name: model,
                value: [model, val],
                itemStyle: { color: getModelColor(model) }
            }));

            charts.latencyChart.setOption({
                xAxis: { data: latencyData.map(d => d.name) },
                series: [{ data: latencyData.map(d => d.value[1]) }]
            });

            // TTFT chart
            const ttftData = Object.entries(data.ttft).map(([model, val]) => ({
                name: model,
                value: val,
                itemStyle: { color: getModelColor(model) }
            }));

            charts.ttftChart.setOption({
                xAxis: { data: ttftData.map(d => d.name) },
                series: [{ data: ttftData.map(d => d.value) }]
            });

            // Success rate chart - use model color with opacity indicating performance
            const successData = Object.entries(data.successRate).map(([model, val]) => ({
                name: model,
                value: val,
                itemStyle: { 
                    color: getModelColor(model),
                    opacity: Math.max(0.4, val / 100)
                }
            }));

            charts.successRateChart.setOption({
                xAxis: { data: successData.map(d => d.name) },
                series: [{ data: successData.map(d => d.value) }]
            });

            // Error distribution - 按模型着色
            const errorData = [];
            Object.entries(data.errors).forEach(([model, errors]) => {
                Object.entries(errors).forEach(([type, count]) => {
                    errorData.push({ 
                        name: `${model}: ${type}`, 
                        value: count,
                        itemStyle: { color: getModelColor(model) }
                    });
                });
            });

            charts.errorDistribution.setOption({
                series: [{ data: errorData }]
            });

            // Token chart - 堆叠柱状图：每个模型一个柱，按模型着色
            const tokenModelData = Object.entries(data.tokens).map(([model, t]) => ({
                name: model,
                prompt: Math.max(t.prompt, 1),  // 最小值1，避免log(0)
                completion: Math.max(t.completion, 1),
                color: getModelColor(model)
            }));

            charts.tokenChart.setOption({
                xAxis: { data: tokenModelData.map(d => d.name) },
                series: [
                    {
                        name: '输入 (Prompt)',
                        type: 'bar',
                        barWidth: '40%',
                        barGap: '0%',
                        data: tokenModelData.map(d => ({ value: d.prompt, itemStyle: { color: d.color } })),
                        emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' } }
                    },
                    {
                        name: '输出 (Completion)',
                        type: 'bar',
                        barWidth: '40%',
                        barGap: '0%',
                        data: tokenModelData.map(d => ({ value: d.completion, itemStyle: { color: lightenColor(d.color, 0.35) } })),
                        emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' } }
                    }
                ]
            });

            // TPS trend - 按模型显示多条折线
            const models = Object.keys(data.tps);

            // Keep every model series aligned with the shared timestamp array.
            // A model that appears mid-history gets zero placeholders for earlier ticks.
            const allTpsModels = new Set([
                ...Object.keys(historicalData.tpsByModel),
                ...models
            ]);
            allTpsModels.forEach(model => {
                let values = historicalData.tpsByModel[model] || [];
                const previousTimestampCount = historicalData.timestamps.length - 1;
                if (values.length < previousTimestampCount) {
                    values = Array(previousTimestampCount - values.length).fill(0).concat(values);
                } else if (values.length > previousTimestampCount) {
                    values = values.slice(-previousTimestampCount);
                }
                values.push(models.includes(model) ? (data.tps[model] || 0) : 0);
                if (values.length > MAX_HISTORY) values.shift();
                historicalData.tpsByModel[model] = values;
            });

            // Build series after every model has one value for the current tick.
            const tpsSeries = [...allTpsModels].map(model => ({
                name: model,
                type: 'line',
                smooth: true,
                symbol: 'circle',
                symbolSize: 6,
                showSymbol: false,
                data: historicalData.tpsByModel[model],
                itemStyle: { 
                    color: getModelColor(model)
                },
                lineStyle: { width: 2 },
                emphasis: {
                    showSymbol: true,
                    itemStyle: {
                        borderWidth: 2,
                        borderColor: '#fff'
                    }
                }
            }));

            charts.tpsTrend.setOption({
                xAxis: { data: historicalData.timestamps },
                series: tpsSeries
            });

            // Model table: one summary row per model, followed by backend detail rows.
            // Backend rows are collapsed by default; expanded state persists across refreshes.
            currentModelList = modelList;
            renderModelTable(modelList);
        }

        // Update histogram chart based on selected metric and model
        function updateHistogramChart() {
            if (!currentHistogramData || !currentHistogramData.histograms) return;

            const metricSelect = document.getElementById('histogramMetric');
            const modelSelect = document.getElementById('histogramModel');
            const selectedMetric = metricSelect.value;
            const selectedModel = modelSelect.value;

            if (!selectedMetric || !selectedModel) return;

            const histData = currentHistogramData.histograms[selectedMetric];
            if (!histData || !histData[selectedModel]) {
                charts.histogramChart.setOption({
                    xAxis: { data: [] },
                    series: [{ data: [] }]
                });
                return;
            }

            const modelData = histData[selectedModel];
            const buckets = modelData.buckets;

            // 获取 +Inf bucket 的值作为总数
            const totalCount = buckets['+Inf'] || modelData.count || 1;

            // 提取 bucket 的 le 值，按阈值排序
            const bucketEntries = Object.entries(buckets)
                .filter(([le]) => le !== '+Inf')
                .sort((a, b) => parseFloat(a[0]) - parseFloat(b[0]));

            if (bucketEntries.length === 0) {
                charts.histogramChart.setOption({
                    xAxis: { data: [] },
                    series: [{ data: [] }]
                });
                return;
            }

            // 计算差值（每个区间的实际请求数）
            let previousCount = 0;
            const categories = [];
            const diffCounts = [];

            bucketEntries.forEach(([le, count]) => {
                const diff = count - previousCount;
                const val = parseFloat(le);
                categories.push(val < 1 ? val.toFixed(3) + 's' : val.toFixed(1) + 's');
                diffCounts.push(diff);
                previousCount = count;
            });

            // 添加最后一个区间（超过最大阈值的请求）
            if (previousCount < totalCount) {
                categories.push('>' + bucketEntries[bucketEntries.length - 1][0] + 's');
                diffCounts.push(totalCount - previousCount);
            }

            const histModelColor = getModelColor(selectedModel);
            charts.histogramChart.setOption({
                xAxis: { data: categories },
                series: [{ 
                    type: 'bar',
                    barWidth: '80%',
                    data: diffCounts,
                    itemStyle: { 
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: histModelColor },
                            { offset: 1, color: lightenColor(histModelColor, 0.4) }
                        ]),
                        borderRadius: [4, 4, 0, 0]
                    },
                    emphasis: {
                        itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' }
                    },
                    name: '请求数'
                }]
            });
        }

        function updateTokenDistChart() {
            if (!currentHistogramData || !currentHistogramData.histograms) return;

            const modelSelect = document.getElementById('tokenDistModel');
            const selectedModel = modelSelect.value;

            if (!selectedModel) return;

            const histData = currentHistogramData.histograms['gpt_api_token_distribution'];
            if (!histData) {
                charts.tokenDistChart.setOption({
                    xAxis: { data: [] },
                    series: [{ data: [] }, { data: [] }]
                });
                return;
            }

            const promptKey = `${selectedModel}::prompt`;
            const completionKey = `${selectedModel}::completion`;

            const promptData = histData[promptKey];
            const completionData = histData[completionKey];

            if (!promptData || !completionData) {
                charts.tokenDistChart.setOption({
                    xAxis: { data: [] },
                    series: [{ data: [] }, { data: [] }]
                });
                return;
            }

            const promptBuckets = promptData.buckets;
            const completionBuckets = completionData.buckets;

            const promptTotal = promptBuckets['type="prompt",le="+Inf"'] || 0;
            const completionTotal = completionBuckets['type="completion",le="+Inf"'] || 0;

            const bucketEntries = Object.entries(promptBuckets)
                .filter(([key]) => key.includes('le=') && !key.includes('+Inf'))
                .sort((a, b) => {
                    const leA = a[0].match(/le="([^"]+)"/)[1];
                    const leB = b[0].match(/le="([^"]+)"/)[1];
                    return parseFloat(leA) - parseFloat(leB);
                });

            let previousPrompt = 0;
            let previousCompletion = 0;
            const categories = [];
            const promptCounts = [];
            const completionCounts = [];

            bucketEntries.forEach(([key]) => {
                const leMatch = key.match(/le="([^"]+)"/);
                const le = leMatch[1];

                const promptCount = (promptBuckets[key] || 0) - previousPrompt;
                const completionCount = (completionBuckets[key.replace('prompt', 'completion')] || 0) - previousCompletion;

                const val = parseFloat(le);
                categories.push(val >= 1024 ? (val/1024).toFixed(0) + 'K' : val.toString());

                promptCounts.push(promptCount);
                completionCounts.push(completionCount);

                previousPrompt = promptBuckets[key] || 0;
                previousCompletion = completionBuckets[key.replace('prompt', 'completion')] || 0;
            });

            const lastKey = bucketEntries[bucketEntries.length - 1]?.[0] || '';
            if (previousPrompt < promptTotal) {
                categories.push('>Max');
                promptCounts.push(promptTotal - previousPrompt);
            }
            if (previousCompletion < completionTotal) {
                if (completionCounts.length < categories.length) {
                    completionCounts.push(completionTotal - previousCompletion);
                } else {
                    completionCounts[completionCounts.length - 1] += (completionTotal - previousCompletion);
                }
            }

            const tdColor = getModelColor(selectedModel);
            charts.tokenDistChart.setOption({
                xAxis: { data: categories },
                series: [
                    { 
                        data: promptCounts,
                        itemStyle: { color: tdColor, borderRadius: [4, 4, 0, 0] },
                        emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' } }
                    },
                    { 
                        data: completionCounts,
                        itemStyle: { color: lightenColor(tdColor, 0.35), borderRadius: [4, 4, 0, 0] },
                        emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' } }
                    }
                ]
            });
        }

        // Fetch and update
        async function fetchMetrics() {
            try {
                const response = await fetch(METRICS_API);
                if (!response.ok) throw new Error('HTTP ' + response.status);
                const text = await response.text();
                const metrics = parseMetrics(text);
                const data = processMetrics(metrics);
                updateCharts(data);
                if (window.QQShell) QQShell.setStatus('ok');
            } catch (err) {
                console.error('Failed to fetch metrics:', err);
                if (window.QQShell) QQShell.setStatus('down', '指标不可用');
            }
        }

        // Initialize
        document.addEventListener('DOMContentLoaded', () => {
            initCharts();
            if (window.QQShell) QQShell.onThemeChange(updateChartsTheme);
            fetchMetrics();
            setInterval(fetchMetrics, REFRESH_INTERVAL);

            // Delegate expand/collapse clicks on the model table so expanded state
            // survives the table re-renders triggered by every metrics refresh.
            const modelTable = document.querySelector('#modelTable');
            modelTable.addEventListener('click', event => {
                const expandButton = event.target.closest('.model-expand-button');
                if (!expandButton) return;
                toggleModelExpansion(Number(expandButton.dataset.modelIndex));
            });
        });
