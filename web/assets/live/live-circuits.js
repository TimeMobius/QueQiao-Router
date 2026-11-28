/** 熔断状态：从 /metrics 读取并补全到模型/后端列表（前端自算，不改后端） */

/** 从 /metrics 解析熔断状态：返回 {"model\u0001backend": {state, cooldown}} 与 backend-only 兜底表 */
const CHAT_ENDPOINTS = [/\/v1\/chat\/completions$/, /\/v1\/completions$/, /\/v1\/messages$/, /\/v1\/responses$/];
const isChatEndpoint = (ep) => CHAT_ENDPOINTS.some(re => re.test(ep));

export const CIRCUIT={
  paired:new Map(), byBackend:new Map(), pairs:new Set(),
  /** 模型 → {chat, other}：只按接口类型区分对话/非对话流量 */
  traffic:new Map(),
};

export function parseCircuit(text){
  const paired=new Map(), byBackend=new Map(), pairs=new Set();
  const num=(re,line)=>{
    const m=line.match(re);
    return m ? parseFloat(m[1]) : null;
  };
  for(const raw of text.split('\n')){
    const line=raw.trim();
    if(!line || line[0]==='#') continue;

    // 对话类 vs 非对话类流量（排序用）
    if(line.startsWith('gpt_api_requests_total{')){
      const mm=line.match(/model="([^"]*)"/);
      const em=line.match(/endpoint="([^"]*)"/);
      const v=num(/}\s+([-0-9.eE+]+)\s*$/, line);
      if(mm && em && v!==null && mm[1] && mm[1]!=='-' && mm[1]!=='unknown'){
        const cur=CIRCUIT.traffic.get(mm[1]) || {chat:0, other:0};
        if(isChatEndpoint(em[1])) cur.chat+=v; else cur.other+=v;
        CIRCUIT.traffic.set(mm[1], cur);
      }
      continue;
    }
    // 模型 × 后端
    if(line.startsWith('gpt_api_model_backend_circuit_state{')){
      const bm=line.match(/backend="([^"]*)"/), mm=line.match(/model="([^"]*)"/);
      const v=num(/}\s+([-0-9.eE+]+)\s*$/, line);
      if(bm&&mm&&v!==null) paired.set(mm[1]+'\u0001'+bm[1], {state:v, cooldown:0});
      continue;
    }
    // backend 维度兜底（模型维度缺失时用）
    if(line.startsWith('gpt_api_backend_circuit_state{')){
      const bm=line.match(/backend="([^"]*)"/);
      const v=num(/}\s+([-0-9.eE+]+)\s*$/, line);
      if(bm&&v!==null) byBackend.set(bm[1], {state:v, cooldown:0});
      continue;
    }
    // 健康指标同时给出“配置承诺”的 (model, backend) 组合，用于补全从未路由过的后端
    if(line.startsWith('gpt_api_model_backend_health{')){
      const bm=line.match(/backend="([^"]*)"/), mm=line.match(/model="([^"]*)"/);
      if(bm&&mm) pairs.add(mm[1]+'\u0001'+bm[1]);
      continue;
    }
    // 冷却剩余秒数
    if(line.startsWith('gpt_api_model_backend_cooldown_remaining_seconds{')){
      const bm=line.match(/backend="([^"]*)"/), mm=line.match(/model="([^"]*)"/);
      const v=num(/}\s+([-0-9.eE+]+)\s*$/, line);
      if(bm&&mm&&v!==null){
        const k=mm[1]+'\u0001'+bm[1];
        const cur=paired.get(k)||{state:-1,cooldown:0};
        cur.cooldown=v; paired.set(k,cur);
      }
      continue;
    }
    if(line.startsWith('gpt_api_backend_cooldown_remaining_seconds{')){
      const bm=line.match(/backend="([^"]*)"/);
      const v=num(/}\s+([-0-9.eE+]+)\s*$/, line);
      if(bm&&v!==null){
        const cur=byBackend.get(bm[1])||{state:-1,cooldown:0};
        cur.cooldown=v; byBackend.set(bm[1],cur);
      }
    }
  }
  CIRCUIT.paired=paired; CIRCUIT.byBackend=byBackend; CIRCUIT.pairs=pairs;
  return CIRCUIT;
}

export function circuitOf(model, backend){
  const hit=CIRCUIT.paired.get(model+'\u0001'+backend);
  if(hit) return hit;
  const fb=CIRCUIT.byBackend.get(backend);
  return fb || {state:-1, cooldown:0};
}

export async function loadCircuit(){
  try{
    const r=await fetch('/metrics',{cache:'no-store'});
    if(!r.ok) return;
    parseCircuit(await r.text());
  }catch(e){ /* 面板不该因指标抓取失败而中断 */ }
}


/** 把熔断事实补全到 models（原地修改），返回 models */
export function enrichModels(models){
  (models||[]).forEach(m=>{
    m.backends = m.backends || [];
    const known = new Set(m.backends.map(b=>b.name));
    // 配置里承诺、但窗口内没流量的后端也要出现（例如已熔断、从未成功路由）
    CIRCUIT.pairs.forEach(key=>{
      const [mm,be]=key.split('\u0001');
      if(mm!==m.id || known.has(be)) return;
      m.backends.push({ name:be, req:0, share:0, active:0, decode:0,
                        ttftP95:null, errRate:0, synthetic:true });
      known.add(be);
    });
    let broken=0, disabled=0;
    m.backends.forEach(b=>{
      const c = circuitOf(m.id, b.name);
      b.circuit = c.state;
      b.circuitLabel = c.state===0?'closed':c.state===1?'open':c.state===2?'half_open':c.state===3?'disabled':'unregistered';
      b.cooldownSec = Math.round((c.cooldown||0)*10)/10;
      b.registered = c.state>=0;
      if(c.state===1||c.state===2) broken++;
      else if(c.state===3) disabled++;
      if(!b.synthetic && (b.req||0)<=0 && (b.active||0)<=0 && !(b.circuit>=1)){
        b.busy = 0; b.level='idle';
      }
      if(b.synthetic){
        b.busy  = c.state===1?100:(c.state===2?80:0);
        b.level = c.state===1?'fault':(c.state===2?'over':'idle');
        b.status= (c.state===1||c.state===2)?'jam':'idle';
      }
    });
    const t = CIRCUIT.traffic.get(m.id) || {chat:0, other:0};
    m.chatRequests = Math.round(t.chat);
    m.otherRequests = Math.round(t.other);
    m.dialogue = t.chat > 0;          // 至少处理过一次对话请求
    // 60s 无请求、也无并发 → 空闲，不应因为“TPS 效率”拿分而排在前面
    if((m.rpm||0) <= 0 && ((m.active||0) <= 0)){
      m.busy = 0; m.level='idle'; m.levelCn='空闲';
    }
    m.backendsTotal = m.backends.length;
    m.backendsBroken = broken;
    m.backendsDisabled = disabled;
  });
  return models;
}
