/* 把完整 3D 场景与系统总览作为两张 chart-card 注入原 dashboard（同页直挂，无 iframe） */
import { createNebula } from './nebula-scene.js';
(() => {
  // ---- 过滤 /metrics：只保留有真实后端的模型，去掉 '-'/'unknown'/'pending' ----
  function filterMetrics(txt){
    const MODEL=/model="([^"]*)"/, BACKEND=/backend="([^"]*)"/;
    const lines=txt.split('\n');
    const valid=new Set();
    lines.forEach(l=>{
      if(!l.startsWith('gpt_api_requests_total')) return;
      const m=l.match(MODEL), b=l.match(BACKEND);
      if(!m) return;
      const model=m[1], be=b?b[1]:'';
      if(!model||model==='-'||model==='unknown') return;
      if(!be||be==='-'||be==='unknown'||be==='pending') return;
      valid.add(model);
    });
    return lines.filter(l=>{
      if(!l||l[0]==='#') return true;
      // 客户端 4xx（client_error）不算后端故障，不进入错误分布
      if(l.startsWith('gpt_api_errors_total') && /error_type="client_error"/.test(l)) return false;
      const m=l.match(MODEL);
      return m ? valid.has(m[1]) : true;
    }).join('\n');
  }
  const origFetch=window.fetch;
  window.fetch=async function(input,init){
    const url=typeof input==='string'?input:(input&&input.url)||'';
    let path=''; try{ path=new URL(url,location.href).pathname; }catch(e){}
    if(path==='/metrics'){
      const r=await origFetch(input,init);
      const txt=await r.text();
      return new Response(filterMetrics(txt),{status:r.status,headers:r.headers});
    }
    return origFetch(input,init);
  };

  const grid = document.querySelector('.charts-grid');
  if (!grid) return;

  const style = document.createElement('style');
  style.textContent = `
  #nebulaCard{display:flex;flex-direction:column}
  .nebula-pair{grid-column:1/-1;display:grid;grid-template-columns:minmax(0,2fr) minmax(0,1fr);gap:20px;align-items:stretch}
  @media (max-width:1100px){.nebula-pair{grid-template-columns:1fr}}
  #nebulaCard .chart-header{flex:0 0 auto}
  #nebulaCard .nebula-stage{position:relative;flex:1 1 auto;min-height:420px;border-radius:12px;overflow:hidden;
    background:radial-gradient(120% 80% at 50% 0%, #0b1a2b 0%, #04070d 62%);
    box-shadow:inset 0 0 0 1px rgba(148,163,184,.14), inset 0 -60px 90px -40px rgba(0,0,0,.6)}
  #nebulaCard .nebula-stage::before{content:'';position:absolute;left:0;right:0;top:0;height:52px;z-index:2;pointer-events:none;
    background:linear-gradient(180deg, var(--bg-secondary,#fff) 0%, rgba(255,255,255,0) 100%)}
  #nebulaCard .nebula-stage::after{content:'';position:absolute;left:0;right:0;bottom:0;height:40px;z-index:2;pointer-events:none;
    background:linear-gradient(0deg, rgba(4,7,13,.9) 0%, rgba(4,7,13,0) 100%)}
  #overviewCard{display:flex;flex-direction:column}
  #nebulaCard .nebula-stage canvas{position:absolute;inset:0;width:100%;height:100%;display:block}
  #nebulaCard .nebula-actions{display:flex;gap:8px;margin-left:auto}
  #nebulaCard .nebula-btn{font-size:12px;border:1px solid var(--border-color,#e2e8f0);border-radius:7px;
    padding:5px 10px;background:var(--bg-secondary,#fff);color:var(--text-secondary,#64748b);cursor:pointer;font-family:inherit}
  #nebulaCard .nebula-btn:hover{color:var(--primary-strong,#2563eb);border-color:var(--primary-strong,#2563eb)}
  #nebulaCard .nebula-close{position:absolute;right:14px;top:14px;z-index:10001;display:none;
    width:32px;height:32px;border-radius:50%;border:1px solid rgba(103,232,249,.35);
    background:rgba(6,14,26,.85);color:#a5f3fc;font-size:16px;cursor:pointer}
  #nebulaCard.fs .nebula-stage{position:fixed;inset:20px;height:auto;z-index:9999;
    border-radius:14px;box-shadow:0 30px 80px rgba(0,0,0,.55)}
  #nebulaCard.fs .nebula-close{display:block;position:fixed;right:30px;top:30px}
  body.nebula-fs-lock{overflow:hidden}

  #nebulaCard .lab{position:absolute;left:0;top:0;white-space:nowrap;pointer-events:none;text-shadow:0 0 10px rgba(0,0,0,.9)}
  #nebulaCard .lab.mdl{color:#dbeafe;font-weight:600;font-size:11px}
  #nebulaCard .lab.idle{color:#94a3b8;font-size:9.5px;opacity:.55}
  #nebulaCard .lab.be{color:#a5f3fc;font-size:9px;background:rgba(2,8,16,.55);padding:1px 5px;border-radius:4px;border:1px solid rgba(103,232,249,.18)}
  #nebulaCard .lab.num{color:rgba(165,243,252,.9);font-size:10.5px;font-weight:600;background:rgba(2,8,16,.42);
    border:1px solid rgba(103,232,249,.22);border-radius:999px;padding:1px 6px}
  /* 响应式栅格：2 列 + dense 流式填充，半幅卡自动补位 */
  @media (min-width: 1150px){
    .charts-grid{grid-template-columns:repeat(2,minmax(0,1fr))!important;grid-auto-flow:row dense!important}
  }
  @media (max-width: 1149px){
    .charts-grid{grid-template-columns:minmax(0,1fr)!important;grid-auto-flow:row!important}
  }
  .charts-grid .chart-card.wide{grid-column:span 2}
  .nebula-pair{grid-column:1 / -1}
  #overviewCard .ov-row{padding:9px 4px;border-bottom:1px solid var(--border-light,#f1f5f9)}
  #overviewCard .ov-row:last-child{border-bottom:0}
  #overviewCard .ov-l1{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:600;color:var(--text-primary,#1e293b)}
  #overviewCard .ov-l1 em{margin-left:auto;font-style:normal;font-size:11px;font-weight:600;padding:1px 7px;border-radius:999px}
  #overviewCard .ov-l2{display:flex;gap:12px;margin-top:4px;font-size:11.5px;color:var(--text-muted,#94a3b8);font-variant-numeric:tabular-nums}
  #overviewCard .ov-l2 b{color:var(--text-secondary,#64748b);font-weight:600}
  #overviewCard .ov-busy{height:4px;border-radius:2px;background:var(--bg-tertiary,#f1f5f9);margin:6px 0 4px;overflow:hidden}
  #overviewCard .ov-busy i{display:block;height:100%;border-radius:2px}
  #overviewCard .ov-bk{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--text-muted,#94a3b8);padding:1px 0}
  #overviewCard .ov-bk span:first-child{flex:0 0 120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  #overviewCard .ov-bk .bar{flex:1;height:3px;border-radius:2px;background:var(--bg-tertiary,#f1f5f9);overflow:hidden}
  #overviewCard .ov-bk .bar i{display:block;height:100%;background:linear-gradient(90deg,#38bdf8,#67e8f9)}
  #overviewCard .ov-more{padding:10px 4px;font-size:11.5px;color:var(--text-muted,#94a3b8)}
  #overviewCard .ov-bk u{text-decoration:none;font-variant-numeric:tabular-nums}
  `;
  document.head.appendChild(style);

  // ---- card 1: full 3D ----
  const card = document.createElement('div');
  card.className = 'chart-card';
  card.id = 'nebulaCard';
  card.innerHTML = `
    <div class="chart-header">
      <div>
        <div class="chart-title">模型全景（3D）</div>
        <div class="chart-subtitle">星云 = 模型，星球 = 后端实例，飞船 = 请求；拖拽旋转 / 滚轮缩放 / 点击星云聚焦</div>
      </div>
      <div class="nebula-actions"><button class="nebula-btn" id="nebulaFs">全屏</button></div>
    </div>
    <div class="nebula-stage" id="nebulaStage">
      <button class="nebula-close" id="nebulaClose" aria-label="关闭全屏">✕</button>
    </div>`;
  const pair = document.createElement('div'); pair.className = 'nebula-pair';
  grid.insertBefore(pair, grid.firstChild);
  pair.appendChild(card);
  const stageEl = card.querySelector('#nebulaStage');
  const nebula = createNebula(stageEl);
  let lastStartTs = 0;

  const fsBtn = card.querySelector('#nebulaFs');
  const closeBtn = card.querySelector('#nebulaClose');
  const toggleFs = on => {
    card.classList.toggle('fs', on);
    document.body.classList.toggle('nebula-fs-lock', on);
    fsBtn.textContent = on ? '退出全屏' : '全屏';
  };
  fsBtn.onclick = () => toggleFs(!card.classList.contains('fs'));
  closeBtn.onclick = () => toggleFs(false);
  addEventListener('keydown', e => { if (e.key === 'Escape') toggleFs(false); });

  // ---- card 2: system overview ----
  const ov = document.createElement('div');
  ov.className = 'chart-card';
  ov.id = 'overviewCard';
  ov.innerHTML = `<div class="chart-header">
      <div>
        <div class="chart-title">系统总览</div>
        <div class="chart-subtitle">繁忙度、成功率与后端流量分布</div>
      </div>
    </div><div id="ovList"></div>`;
  pair.appendChild(ov);

  const LEVEL = {
    smooth: ['#67e8f9', '流畅'], normal: ['#34d399', '正常'],
    busy: ['#fbbf24', '繁忙'], over: ['#fb7185', '过载'],
    fault: ['#ef4444', '淤积'], idle: ['#94a3b8', '空闲'],
  };

  async function poll() {
    try {
      const r = await fetch('/dashboard/api/live', { cache: 'no-store' });
      const d = await r.json();
      if (!d || !d.ready) return;
      // 真实调度事件 → 飞船（首帧只校准时间基线，不补放历史）
      if (lastStartTs === 0) {
        lastStartTs = d.ts || Date.now();
      } else {
        const fresh = (d.starts || []).filter(x => x.ts_ms > lastStartTs);
        if (fresh.length) {
          lastStartTs = Math.max(lastStartTs, ...fresh.map(x => x.ts_ms));
          if (nebula) fresh.slice(-8).forEach(x => nebula.spawnFor(x.model, x.backend));
        }
      }

      const list = ov.querySelector('#ovList');
      const all = (d.models || []).slice().sort((a, b) => b.busy - a.busy);
      const top = all.slice(0, 4);
      const rest = all.length - top.length;
      list.innerHTML = top.map(m => {
        const [c, cn] = LEVEL[m.level] || LEVEL.normal;
        const succ = ((m.successRate ?? 1) * 100).toFixed(1);
        return `<div class="ov-row">
          <div class="ov-l1"><i style="width:8px;height:8px;border-radius:50%;background:${c};display:inline-block"></i>
            ${m.name}<em style="background:${c}22;color:${c}">${cn} ${m.busy.toFixed(0)}</em></div>
          <div class="ov-l2"><span><b>${m.rpm.toFixed(1)}</b>/min</span><span>成功率 <b>${succ}%</b></span>
            <span>TTFT <b>${m.ttftP95 ? m.ttftP95.toFixed(1) + 's' : '—'}</b></span>
            <span>解码 <b>${m.decode ? m.decode.toFixed(0) : '—'}</b></span>
            <span>并发 <b>${m.active || 0}</b></span></div>
          <div class="ov-busy"><i style="width:${Math.min(100, m.busy)}%;background:linear-gradient(90deg,${c}55,${c})"></i></div>
          ${(m.backends || []).slice(0, 4).map(b => `<div class="ov-bk"><span>${b.name}</span>
            <span class="bar"><i style="width:${Math.round((b.share || 0) * 100)}%"></i></span>
            <u>${b.req ?? 0} 次 · ${Math.round((b.share || 0) * 100)}% · 并发${b.active ?? 0}</u></div>`).join('')}
        </div>`;
      }).join('') + (rest > 0 ? `<div class="ov-more">另有 ${rest} 个模型空闲或低负载</div>` : '');
    } catch (e) { /* ignore */ }
  }
  // 显式控制栅格（inline style 优先，避免不同页面 CSS 的覆盖差异）
  function applyGrid(){
    const g = document.querySelector('.charts-grid'); if (!g) return;
    const vw = window.innerWidth;
    // 宽度自适应：表格始终整行；指标分布直方图在宽屏整行、窄屏半幅；其余半幅
    document.querySelectorAll('.charts-grid > .chart-card').forEach(c => {
      const t = (c.querySelector('.chart-title') || {}).textContent || '';
      let isWide = false;
      if (t.includes('模型性能详情')) isWide = true;
      else if (t.includes('指标分布直方图')) isWide = false;
      c.classList.toggle('wide', isWide);
    });
    const wide = vw >= 1150;
    g.style.setProperty('grid-template-columns', wide ? 'repeat(2,minmax(0,1fr))' : 'minmax(0,1fr)', 'important');
    g.style.setProperty('grid-auto-flow', wide ? 'row dense' : 'row', 'important');
    // 关键：窄屏禁用 span 2，否则会生成隐式第二列（650px + 290px 的怪布局）
    document.querySelectorAll('.charts-grid .chart-card.wide').forEach(c => {
      c.style.setProperty('grid-column', wide ? 'span 2' : 'auto', 'important');
    });
    const pr = document.querySelector('.nebula-pair');
    if (pr) {
      pr.style.setProperty('grid-column', '1 / -1', 'important');
      pr.style.setProperty('grid-template-columns', window.innerWidth >= 1100 ? 'minmax(0,2fr) minmax(0,1fr)' : 'minmax(0,1fr)', 'important');
    }
  }
  // Token 消耗统计：原图 Y 轴 formatter 会把 1e9 写成 1000M/100000M 并溢出，这里改成紧凑单位
  function fixTokenAxis(){
    if(!window.echarts) return;
    const el=document.getElementById('tokenChart'); if(!el) return;
    const inst=window.echarts.getInstanceByDom(el); if(!inst) return;
    const opt=inst.getOption();
    let dataMax=0, dataMin=Infinity;
    (opt.series||[]).forEach(se=>{(se.data||[]).forEach(d=>{
      const v=typeof d==='number'?d:(d&&typeof d==='object'?d.value:NaN);
      if(typeof v==='number'&&isFinite(v)&&v>0){ dataMax=Math.max(dataMax,v); dataMin=Math.min(dataMin,v); }
    });});
    if(!dataMax) return;
    const lo=Math.max(1, Math.pow(10, Math.floor(Math.log10(dataMax)) - 5));
    const hi=Math.pow(10, Math.ceil(Math.log10(Math.max(10, dataMax*1.15))));
    const decades=Math.max(1, Math.round(Math.log10(hi/lo)));
    const cur=(opt.yAxis&&opt.yAxis[0]);
    if(cur && cur.min===lo && cur.max===hi && cur.splitNumber===decades) return;
    const fmt=v=>{
      const n=Math.abs(Number(v)||0);
      if(n<1000) return String(Math.round(v));
      const units=[[1e15,'P'],[1e12,'T'],[1e9,'B'],[1e6,'M'],[1e3,'K']];
      for(const [u,suf] of units){ if(n>=u){ const k=n/u; const q=(k>=10)?Math.round(k):(Math.round(k*10)/10); return String(q)+suf; } }
      return String(Math.round(v));
    };
    inst.setOption({grid:{left:70,top:54},yAxis:{min:lo,max:hi,splitNumber:decades,name:'Tokens',nameGap:30,nameTextStyle:{align:'left',padding:[0,0,0,-8]},axisLabel:{formatter:fmt,fontSize:10,hideOverlap:false,interval:0},axisTick:{interval:0,show:true}}},{lazyUpdate:true});
  }
  setInterval(fixTokenAxis, 1500); setTimeout(fixTokenAxis, 2500);
  applyGrid(); addEventListener('resize', applyGrid);
  poll(); setInterval(poll, 2000);
})();
