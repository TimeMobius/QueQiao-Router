import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { loadCircuit, enrichModels } from './live-circuits.js';

export function createNebula(container){
const $=id=>container.querySelector('#'+id);

const labels=document.createElement('div'); labels.id='labels';
labels.style.cssText='position:absolute;inset:0;pointer-events:none;overflow:hidden;z-index:2';
container.appendChild(labels);
const tip=document.createElement('div');
tip.style.cssText='position:absolute;left:0;top:0;pointer-events:none;padding:6px 9px;border-radius:7px;background:rgba(6,14,26,.92);border:1px solid rgba(103,232,249,.25);color:#dbeafe;font-size:11.5px;line-height:1.5;opacity:0;transition:opacity .12s;white-space:nowrap;transform:translate(-50%,-100%)';
tip.style.zIndex='3';
container.appendChild(tip);
const SAT={ok:0x67e8f9,warn:0xfbbf24,jam:0xfb7185,idle:0x5b6b82};
const LEVEL={smooth:0x67e8f9,normal:0x34d399,busy:0xfbbf24,over:0xfb7185,fault:0xef4444,idle:0x5b6b82};
const LEVELCSS={smooth:'#67e8f9',normal:'#34d399',busy:'#fbbf24',over:'#fb7185',fault:'#ef4444',idle:'#64748b'};
const UP=new THREE.Vector3(0,1,0);

let scene,camera,renderer,controls,raycaster,clock;
let GLOW=null, wormGroup=null, wormMat=null, wormSpin=null;
const clusters=new Map(), ships=[], labelEls=new Map(), flashes=[];
let DATA=null, idsKey='', rpm=0, acc=0, POSITIONS=[];
let hover=null, focus=null, camAnim=null, downPt=null;


function glowTexture(){
  const c=document.createElement('canvas'); c.width=c.height=128;
  const g=c.getContext('2d'); const gr=g.createRadialGradient(64,64,0,64,64,64);
  gr.addColorStop(0,'rgba(255,255,255,1)'); gr.addColorStop(.25,'rgba(255,255,255,.55)');
  gr.addColorStop(.6,'rgba(255,255,255,.12)'); gr.addColorStop(1,'rgba(255,255,255,0)');
  g.fillStyle=gr; g.fillRect(0,0,128,128); return new THREE.CanvasTexture(c);
}
function textTexture(text,opt){
  opt=opt||{}; const c=document.createElement('canvas'); const g=c.getContext('2d');
  const font=opt.font||'700 44px Inter,PingFang SC,sans-serif';
  g.font=font; const w=Math.ceil(g.measureText(text).width)+40, h=Math.ceil((opt.h||64));
  c.width=w; c.height=h; const gg=c.getContext('2d'); gg.font=font;
  gg.textAlign='center'; gg.textBaseline='middle';
  gg.shadowColor=opt.shadow||'rgba(0,0,0,.95)'; gg.shadowBlur=opt.blur||14;
  gg.fillStyle=opt.color||'rgba(205,225,255,.92)';
  gg.fillText(text,w/2,h/2+2);
  const t=new THREE.CanvasTexture(c); t.colorSpace=THREE.SRGBColorSpace; return {tex:t,w:w,h:h};
}
function ringTexture(text,color){
  const S=256, c=document.createElement('canvas'); c.width=c.height=S;
  const g=c.getContext('2d'); g.translate(S/2,S/2);
  g.font='600 15px Inter,PingFang SC,sans-serif'; g.textAlign='center'; g.textBaseline='middle';
  g.fillStyle=color||'rgba(165,243,252,.85)';
  const n=text.length, R=S*0.38;
  for(let i=0;i<n;i++){ const a=i/n*Math.PI*2-Math.PI/2;
    g.fillText(text[i], Math.cos(a)*R, Math.sin(a)*R); }
  const t=new THREE.CanvasTexture(c); t.colorSpace=THREE.SRGBColorSpace; return t;
}
function beltTexture(text,color){
  const W=2048,H=256; const c=document.createElement('canvas'); c.width=W; c.height=H;
  const g=c.getContext('2d');
  g.font='700 156px Inter,PingFang SC,sans-serif'; g.textBaseline='middle'; g.textAlign='left';
  g.fillStyle=color;
  const unit=text+'     ·     ';
  const uw=g.measureText(unit).width||1;
  let x=0;
  while(x<W){ g.fillText(unit,x,H/2+3); x+=uw; }
  const t=new THREE.CanvasTexture(c); t.colorSpace=THREE.SRGBColorSpace;
  t.wrapS=THREE.RepeatWrapping; t.anisotropy=4;
  return t;
}
function rng(seed){let x=seed>>>0||1;return()=>{x=(x*1664525+1013904223)>>>0;return x/4294967296;};}
function hash(s){let h=2166136261;for(let i=0;i<s.length;i++){h^=s.charCodeAt(i);h=Math.imul(h,16777619);}return h>>>0;}

const EMBED=location.search.indexOf('embed')>=0;
function init(){
  const canvas=document.createElement('canvas');
  canvas.style.cssText='position:absolute;inset:0;width:100%;height:100%;display:block';
  container.appendChild(canvas);
  renderer=new THREE.WebGLRenderer({canvas,antialias:true});
  renderer.setPixelRatio(Math.min(2,devicePixelRatio||1));
  renderer.setSize(container.clientWidth||800, container.clientHeight||420, false);
  renderer.outputColorSpace=THREE.SRGBColorSpace;
  renderer.toneMapping=THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure=1.15;
  scene=new THREE.Scene(); scene.fog=new THREE.FogExp2(0x02040a,0.016);
  camera=new THREE.PerspectiveCamera(52,(container.clientWidth||800)/(container.clientHeight||420),0.1,200);
  camera.position.set(EMBED?0:-0.6, EMBED?2.4:4.4, EMBED?5.4:10.0);
  if(EMBED){ camera.fov=56; camera.updateProjectionMatrix(); }
  controls=new OrbitControls(camera,canvas);
  controls.target.set(EMBED?0:-0.6, EMBED?0:0.2, 0);
  controls.enableDamping=true; controls.dampingFactor=0.07;
  controls.autoRotate=true; controls.autoRotateSpeed=0.22;
  controls.minDistance=EMBED?2.4:4; controls.maxDistance=EMBED?10:22;
  controls.enablePan=true; controls.screenSpacePanning=true; controls.panSpeed=0.8;
  controls.rotateSpeed=0.9; controls.zoomSpeed=0.9;
  canvas.style.touchAction='none';
  // 根据容器比例自适应相机：超宽卡片（如 dashboard 内嵌）自动拉近
  { const asp=(container.clientWidth||800)/(container.clientHeight||420);
    if(asp>2.2){ camera.fov=60; camera.position.set(0,2.0,4.4); controls.target.set(0,0,0);
      controls.minDistance=2.0; controls.maxDistance=9; camera.updateProjectionMatrix(); } }
  controls.addEventListener('start',()=>{controls.autoRotate=false;});
  controls.addEventListener('end',()=>{setTimeout(()=>{controls.autoRotate=true;},4000);});
  canvas.addEventListener('dblclick',()=>{unfocus();});

  scene.add(new THREE.AmbientLight(0x223344,1.2));
  const key=new THREE.DirectionalLight(0x9fdcff,1.3); key.position.set(5,9,7); scene.add(key);
  const rim=new THREE.DirectionalLight(0x3b82f6,0.8); rim.position.set(-7,3,-6); scene.add(rim);

  GLOW=glowTexture();
  buildStars();
  const grid=new THREE.GridHelper(30,30,0x0b2b3a,0x061a26);
  grid.position.y=-2.4; grid.material.transparent=true; grid.material.opacity=0.4; scene.add(grid);
  buildWormhole();

  raycaster=new THREE.Raycaster(); clock=new THREE.Clock();
function fit(){ const w=container.clientWidth||800,h=container.clientHeight||420;
    renderer.setSize(w,h,false); camera.aspect=w/h; camera.updateProjectionMatrix(); }
  new ResizeObserver(fit).observe(container); fit();
  canvas.addEventListener('pointermove',onMove);
  canvas.addEventListener('pointerdown',e=>{downPt={x:e.clientX,y:e.clientY};});
  canvas.addEventListener('pointerup',onPick);
  window.__stardbg=()=>[...clusters.values()].map(c=>({id:c.m.id,n:(c.stars||[]).length,
    pos:(c.stars||[]).map(st=>{const v=new THREE.Vector3();st.getWorldPosition(v);const d=v.clone().project(camera);return {id:st.uuid.slice(0,4),x:+d.x.toFixed(2),y:+d.y.toFixed(2),z:+d.z.toFixed(2),vis:st.visible,sc:+st.scale.x.toFixed(2)};}),
    center:(()=>{const v=new THREE.Vector3();c.grp.getWorldPosition(v);const d=v.clone().project(camera);return {x:+d.x.toFixed(2),y:+d.y.toFixed(2)};})()}));
  window.__focusByName=(n)=>{for(const c of clusters.values()){if(c.m&&c.m.name===n){focusCluster(c);return true;}}return false;};
  load(); setInterval(load,2000);
  loop();
}

function buildStars(){
  const n=1800,pos=new Float32Array(n*3);
  for(let i=0;i<n;i++){const r=20+Math.random()*45,a=Math.random()*Math.PI*2,b=Math.acos(2*Math.random()-1);
    pos[i*3]=r*Math.sin(b)*Math.cos(a);pos[i*3+1]=r*Math.cos(b)*.6;pos[i*3+2]=r*Math.sin(b)*Math.sin(a);}
  const g=new THREE.BufferGeometry(); g.setAttribute('position',new THREE.BufferAttribute(pos,3));
  scene.add(new THREE.Points(g,new THREE.PointsMaterial({color:0x9ecbf4,size:.09,sizeAttenuation:true,transparent:true,opacity:.7,depthWrite:false})));
}

function buildWormhole(){
  wormGroup=new THREE.Group(); wormGroup.scale.setScalar(0.25);
  wormGroup.rotation.set(-Math.PI/2+0.10,0,0.12); scene.add(wormGroup);
  const prof=[[0.98,0.02],[0.94,-0.10],[0.84,-0.34],[0.68,-0.66],[0.52,-1.02],[0.40,-1.40],[0.34,-1.75],[0.32,-2.05]]
    .map(p=>new THREE.Vector2(p[0],p[1]));
  const geo=new THREE.LatheGeometry(prof,128); geo.rotateX(Math.PI/2);
  wormMat=new THREE.ShaderMaterial({uniforms:{uTime:{value:0}},
    vertexShader:'varying vec2 vUv;void main(){vUv=uv;gl_Position=projectionMatrix*modelViewMatrix*vec4(position,1.0);}',
    fragmentShader:['varying vec2 vUv;uniform float uTime;','void main(){','float v=vUv.y;float u=vUv.x;',
      'float s1=sin(u*90.0+v*18.0-uTime*2.0)*0.5+0.5;','float s2=sin(u*47.0-v*9.0+uTime*1.3)*0.5+0.5;',
      'vec3 rim=vec3(0.72,0.97,1.0),mid=vec3(0.10,0.42,0.64),deep=vec3(0.01,0.06,0.14);',
      'vec3 col=mix(rim,mid,smoothstep(0.0,0.35,v));col=mix(col,deep,smoothstep(0.35,0.95,v));',
      'col+=vec3(0.15,0.55,0.75)*(s1*0.6+s2*0.4)*(1.0-smoothstep(0.0,0.8,v));','gl_FragColor=vec4(col,1.0);}'].join('\n'),
    side:THREE.DoubleSide,transparent:true,opacity:0.92,depthWrite:false});
  wormGroup.add(new THREE.Mesh(geo,wormMat));
  const rimT=new THREE.Mesh(new THREE.TorusGeometry(0.985,0.018,10,160),new THREE.MeshBasicMaterial({color:0xeafcff}));
  wormGroup.add(rimT);
  const halo=new THREE.Sprite(new THREE.SpriteMaterial({map:GLOW,color:0x8fe9ff,transparent:true,opacity:.28,blending:THREE.AdditiveBlending,depthWrite:false}));
  halo.scale.setScalar(2.6); wormGroup.add(halo);
  const core=new THREE.Sprite(new THREE.SpriteMaterial({map:GLOW,color:0xdffaff,transparent:true,opacity:.8,blending:THREE.AdditiveBlending,depthWrite:false}));
  core.position.set(0,0,-2.0); core.scale.setScalar(.9); wormGroup.add(core);
  const N=220,pos=new Float32Array(N*3);
  for(let i=0;i<N;i++){const a=Math.random()*Math.PI*2,r=1.02+Math.random()*.22;
    pos[i*3]=Math.cos(a)*r;pos[i*3+1]=Math.sin(a)*r;pos[i*3+2]=(Math.random()-.5)*.12;}
  const pg=new THREE.BufferGeometry(); pg.setAttribute('position',new THREE.BufferAttribute(pos,3));
  wormSpin=new THREE.Points(pg,new THREE.PointsMaterial({map:GLOW,color:0x9fe9ff,size:.06,transparent:true,opacity:.75,blending:THREE.AdditiveBlending,depthWrite:false,sizeAttenuation:true}));
  wormGroup.add(wormSpin);
}

function layoutPositions(models){
  const n=Math.max(1,models.length), R=3.15, pts=[];
  for(let i=0;i<n;i++){
    const t=n===1?0:(i/(n-1))*2-1, y=t*0.72, r=Math.sqrt(Math.max(0.05,1-t*t));
    const th=i*2.399963;
    pts.push(new THREE.Vector3(Math.cos(th)*r*R, y*R*0.55, Math.sin(th)*r*R));
  }
  const rad=models.map(m=>{const nb=(m.backends||[]).length||1;return (0.78+0.09*(nb-1)+Math.min(.12,(m.active||0)*.01))*0.95;});
  for(let it=0;it<80;it++){
    for(let i=0;i<n;i++) for(let j=i+1;j<n;j++){
      const d=new THREE.Vector3().subVectors(pts[i],pts[j]); const len=d.length()||0.001;
      const min=(rad[i]+rad[j])*1.35;
      if(len<min){ const push=d.multiplyScalar((min-len)/len*0.5);
        pts[i].add(push); pts[j].sub(push); }
    }
    pts.forEach(p=>p.setLength(R));
  }
  return pts;
}

// 星球的颜色只由**该后端自己**的 level 决定，与所属星云解耦
function backendColor(b){
  return LEVEL[b&&b.level]!==undefined ? LEVEL[b.level] : SAT.ok;
}

function rebuildClusters(models){
  clusters.forEach(c=>scene.remove(c.grp)); clusters.clear();
  labelEls.forEach(e=>e.remove()); labelEls.clear();
  // 按 id 排序后再布局：同一批模型每次重建都落在同一个坑位
  models=models.slice().sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0);
  POSITIONS=layoutPositions(models);
  models.forEach((m,i)=>{
    const grp=new THREE.Group(); grp.position.copy(POSITIONS[i]);
    // 每个星团一个固定的随机朝向（按模型 id 决定，刷新不变），避免所有椭球同向
    const RO=rng(hash(m.id+'|orient'));
    grp.rotation.set((RO()-0.5)*1.1, RO()*Math.PI*2, (RO()-0.5)*1.1);
    scene.add(grp);
    const col=LEVEL[m.level]!==undefined?LEVEL[m.level]:SAT.ok;
    const nb=(m.backends||[]).length||1;
    const cscale=Math.min(1.9, 0.70+0.15*(nb-1)+Math.min(.10,(m.active||0)*.008));
    const A=1.0*cscale,B=0.40*cscale,C=0.40*cscale;
    // 粒子云用**按模型 id 播种的确定性随机**：重建后形状完全一致，
    // 不会因为繁忙度变化重建而看起来像“朝向变了”
    const PR=rng(hash(m.id+'|particles'));
    const N=340, pos=new Float32Array((N+60)*3);
    for(let k=0;k<N;k++){const u=PR()*2-1,th=PR()*Math.PI*2,r=Math.sqrt(Math.max(0,1-u*u)),j=.93+PR()*.14;
      pos[k*3]=Math.cos(th)*r*A*j;pos[k*3+1]=u*B*j;pos[k*3+2]=Math.sin(th)*r*C*j;}
    for(let k=0;k<60;k++){const u=PR()*2-1,th=PR()*Math.PI*2,r=Math.sqrt(Math.max(0,1-u*u)),t=Math.cbrt(PR())*.9;
      pos[(N+k)*3]=Math.cos(th)*r*A*t;pos[(N+k)*3+1]=u*B*t;pos[(N+k)*3+2]=Math.sin(th)*r*C*t;}
    const sg=new THREE.BufferGeometry(); sg.setAttribute('position',new THREE.BufferAttribute(pos,3));
    const shell=new THREE.Points(sg,new THREE.PointsMaterial({map:GLOW,color:col,size:.15,transparent:true,opacity:.65,
      blending:THREE.AdditiveBlending,depthWrite:false,sizeAttenuation:true}));
    grp.add(shell);
    const neb=[];
    for(let k=0;k<3;k++){const sp=new THREE.Sprite(new THREE.SpriteMaterial({map:GLOW,color:col,transparent:true,opacity:.12,blending:THREE.AdditiveBlending,depthWrite:false}));
      sp.position.set((PR()-.5)*.5,(PR()-.5)*.25,(PR()-.5)*.5); sp.scale.setScalar(1.2+PR()*.6); grp.add(sp); neb.push(sp);}
    const RND=rng(hash(m.id)); const stars=[];
    const bn=Math.max(1,(m.backends||[]).length);
    (m.backends||[]).forEach((b,bi)=>{
      // 均匀分布在椭球内：黄金角方向 + 半径分层，保证彼此分开
      const y0=bn===1?0:1-(bi/(bn-1))*2;
      const rr=Math.sqrt(Math.max(0,1-y0*y0));
      const th=bi*2.399963+m.id.length;
      const band=0.52+((bi%3)*0.16);
      const p=new THREE.Vector3(Math.cos(th)*rr*band*A, y0*band*B, Math.sin(th)*rr*band*C);
      const bcol=backendColor(b);
      const s=new THREE.Mesh(new THREE.SphereGeometry(.075,18,18),
        new THREE.MeshStandardMaterial({color:bcol,emissive:bcol,emissiveIntensity:2.2,roughness:.25,metalness:.05}));
      s.position.copy(p);
      s.userData.baseColor=bcol;
      const h=new THREE.Sprite(new THREE.SpriteMaterial({map:GLOW,color:bcol,transparent:true,opacity:.85,blending:THREE.AdditiveBlending,depthWrite:false}));
      h.scale.setScalar(.55); s.add(h);
      // 水平环绕的文字带（绕球一圈，随球自转）
      const beltCol = backendColor(b)===LEVEL.jam||backendColor(b)===LEVEL.over ? 'rgba(252,165,165,.95)'
        : backendColor(b)===LEVEL.busy ? 'rgba(253,224,160,.95)' : 'rgba(165,243,252,.95)';
      const bt=beltTexture(b.name, beltCol);
      const belt=new THREE.Mesh(new THREE.CylinderGeometry(.30,.30,.15,48,1,true),
        new THREE.MeshBasicMaterial({map:bt,transparent:true,opacity:.98,side:THREE.FrontSide,
          depthWrite:false,blending:THREE.AdditiveBlending}));
      belt.position.y=0; s.add(belt); s.userData.belt=belt;
      grp.add(s); stars.push(s);
    });
    const hit=new THREE.Mesh(new THREE.SphereGeometry(1.05*cscale,10,10),new THREE.MeshBasicMaterial({transparent:true,opacity:0,depthWrite:false,depthTest:false,colorWrite:false}));
    grp.add(hit);
    clusters.set(m.id,{grp,shell,neb,stars,hit,m,cscale,baseRot:grp.rotation.clone(),nebRot:0});
    const tt=textTexture(m.name,{font:'700 42px Inter,PingFang SC,sans-serif',color:'rgba(190,215,255,.85)',blur:16});
    const nameSpr=new THREE.Sprite(new THREE.SpriteMaterial({map:tt.tex,transparent:true,opacity:.26,
      depthWrite:false,depthTest:true,blending:THREE.NormalBlending}));
    nameSpr.scale.set(1.9*cscale,1.9*cscale*tt.h/tt.w,1); nameSpr.position.set(0,0,-0.05); nameSpr.renderOrder=-1;
    grp.add(nameSpr);
    (m.backends||[]).forEach((b,bi)=>{const nd=document.createElement('div'); nd.className='lab num'; nd.textContent=(b.active||0);
      labels.appendChild(nd); labelEls.set('n_'+m.id+'_'+bi,nd);});
  });
}

function updateClusters(models){
  models.forEach(m=>{
    const c=clusters.get(m.id); if(!c)return;
    const col=LEVEL[m.level]!==undefined?LEVEL[m.level]:SAT.ok;
    c.shell.material.color.setHex(col);
    c.shell.material.opacity=m.status==='idle'?.22:.5;
    c.shell.material.size=m.status==='idle'?.09:.12;
    c.neb.forEach(sp=>{sp.material.color.setHex(col);sp.material.opacity=m.status==='idle'?.06:.12;});
    c.stars.forEach((s,bi)=>{const b=(m.backends||[])[bi]; if(!b)return;
      const circuit = (b.circuit===undefined||b.circuit===null)?-1:b.circuit;
      const prevCircuit = s.userData.circuit===undefined?-1:s.userData.circuit;
      s.userData.circuit = circuit;
      const failed = circuit===1 || circuit===2;   // open / half_open：熔断或探测中
      const disabled = circuit===3;                // 配置里显式停用

      if(failed || disabled){
        // 熔断/停用：暗红残骸 + 碎屑，不再按 level 上色
        const deadCol = failed ? 0xff6b4a : 0x6b7280;
        s.material.color.setHex(0x2a1512);
        s.material.emissive.setHex(deadCol);
        s.material.emissiveIntensity = failed ? 1.6 : 0.6;
        const glow=s.children && s.children[0];
        if(glow && glow.material) glow.material.color.setHex(deadCol);
        if(!s.userData.debris) makeDebris(s);       // 首次熔断时生成残骸
        s.scale.setScalar(failed?1.05:0.8);
        // 文字带也暗掉
        const belt=s.userData.belt;
        if(belt && belt.material) belt.material.opacity=.25;
      } else {
        // 正常：解耦的等级色
        const bcol=backendColor(b);
        s.material.color.setHex(bcol);
        s.material.emissive.setHex(bcol);
        s.material.emissiveIntensity = b.status==='idle' ? 1.1 : (b.status==='jam' ? 3.4 : 2.8);
        const glow=s.children && s.children[0];
        if(glow && glow.material) glow.material.color.setHex(bcol);
        s.scale.setScalar(.75+Math.min(1.4,(b.active||0)*.14));
        if(prevCircuit>=1 && s.userData.debris){
          // 恢复：残骸淡出移除
          s.remove(s.userData.debris);
          delete s.userData.debris; delete s.userData.debrisN; delete s.userData.debrisRing;
        }
        const belt=s.userData.belt;
        if(belt && belt.material) belt.material.opacity=.98;
      }
    });
    c.m=m;
  });
}

function wallR(z){
  const prof=[[0.98,0.02],[0.94,-0.10],[0.84,-0.34],[0.68,-0.66],[0.52,-1.02],[0.40,-1.40],[0.34,-1.75],[0.32,-2.05]];
  for(let i=0;i<prof.length-1;i++){const a=prof[i],b=prof[i+1];
    if(z<=a[1]&&z>=b[1]){const k=(z-a[1])/(b[1]-a[1]);return a[0]+k*(b[0]-a[0]);}}
  return 0.34;
}
function launchShip(pick,bi){
  if(!pick||!pick.stars||!pick.stars.length) return false;
  if(ships.length>44) return false;
  if(bi==null||bi<0||bi>=pick.stars.length) bi=Math.floor(Math.random()*pick.stars.length);
  const star=pick.stars[bi]||null;
  const end=new THREE.Vector3();
  if(star) star.getWorldPosition(end); else pick.grp.getWorldPosition(end);
  const ang0=Math.random()*Math.PI*2, pts=[], N=44;
  for(let i=0;i<=N;i++){
    const u=i/N; let z,r;
    if(u<=0.78){const k=u/0.78; z=-1.92+1.92*k; r=Math.max(0.06,wallR(z)*0.55);}
    else {const k=(u-0.78)/0.22; z=0.02+0.62*k; r=0.54+1.35*k;}
    const ang=ang0+7.5*u;
    let p=new THREE.Vector3(Math.cos(ang)*r,Math.sin(ang)*r,z);
    if(wormGroup) p=wormGroup.localToWorld(p);
    pts.push(p);
  }
  const escCurve=new THREE.CatmullRomCurve3(pts,false,'catmullrom',0.5);
  const mesh=new THREE.Mesh(new THREE.ConeGeometry(.04,.14,6),new THREE.MeshStandardMaterial({color:0xdffaff,emissive:0x9fdfff,emissiveIntensity:2.4}));
  const tg=new THREE.BufferGeometry(); tg.setAttribute('position',new THREE.BufferAttribute(new Float32Array(20*3),3));
  const trail=new THREE.Line(tg,new THREE.LineBasicMaterial({color:0x67e8f9,transparent:true,opacity:.4,blending:THREE.AdditiveBlending}));
  scene.add(mesh); scene.add(trail);
  ships.push({mesh,trail,t:0,dur:2.1+Math.random()*.7,target:pick,star,bi,history:[],prev:null,
    esc:true,escT:0,escDur:1.35,escCurve,flight:null,exit:null,tangent:null});
  return true;
}
function loop(){
  requestAnimationFrame(loop);
  const dt=Math.min(.05,clock.getDelta());
  if(camAnim){
    camAnim.t=Math.min(1,camAnim.t+dt/camAnim.dur); const k=camAnim.t<.5?4*camAnim.t**3:1-Math.pow(-2*camAnim.t+2,3)/2;
    camera.position.lerpVectors(camAnim.p0,camAnim.p1,k); controls.target.lerpVectors(camAnim.t0,camAnim.t1,k);
    if(camAnim.t>=1){camAnim=null;controls.enabled=true;}
  }
  controls.update();
  if(wormMat) wormMat.uniforms.uTime.value+=dt;
  if(wormSpin) wormSpin.rotation.z+=dt*.9;
  if(wormGroup) wormGroup.visible=!focus;

  // 飞船由真实请求事件驱动（spawnFor），不再按速率估算

  for(let i=ships.length-1;i>=0;i--){
    const s=ships[i]; let p;
    if(s.esc){
      s.escT+=dt/s.escDur; const t=Math.min(1,s.escT);
      p=s.escCurve.getPointAt(t);
      if(t>=1){
        s.esc=false; s.t=0;
        s.exit=p.clone();
        s.tangent=s.escCurve.getTangentAt(1).normalize();
        const target=new THREE.Vector3();
        if(s.star) s.star.getWorldPosition(target); else s.target.grp.getWorldPosition(target);
        const dist=s.exit.distanceTo(target);
        const c1=s.exit.clone().add(s.tangent.clone().multiplyScalar(Math.min(dist*0.38,3.2)));
        const dir=target.clone().sub(s.exit).normalize();
        const c2=target.clone().sub(dir.multiplyScalar(Math.min(dist*0.28,2.2)));
        s.flight=new THREE.CubicBezierCurve3(s.exit,c1,c2,target);
      }
    } else {
      // 每帧刷新终点与末端控制点，保持平滑逼近
      const target=new THREE.Vector3();
      if(s.star) s.star.getWorldPosition(target); else s.target.grp.getWorldPosition(target);
      const dist=s.flight.v0.distanceTo(target);
      const dir=target.clone().sub(s.flight.v0).normalize();
      s.flight.v3.copy(target);
      s.flight.v2.copy(target).sub(dir.multiplyScalar(Math.min(dist*0.28,2.2)));
      s.t+=dt/s.dur;
      p=s.flight.getPoint(Math.min(1,s.t));
      if(s.t>=1){
        const at=new THREE.Vector3();
        if(s.star) s.star.getWorldPosition(at); else p=s.flight.v3;
        if(!s.star) at.copy(s.flight.v3);
        // 命中：光晕 + 扩散环
        const fl=new THREE.Sprite(new THREE.SpriteMaterial({map:GLOW,color:0xdffaff,transparent:true,opacity:.95,
          blending:THREE.AdditiveBlending,depthWrite:false}));
        fl.position.copy(at); scene.add(fl);
        const ring=new THREE.Mesh(new THREE.TorusGeometry(.12,.008,6,32),
          new THREE.MeshBasicMaterial({color:s.target.m.status==='jam'?SAT.jam:SAT.ok,transparent:true,opacity:.9,
            blending:THREE.AdditiveBlending,depthWrite:false}));
        ring.position.copy(at); ring.lookAt(camera.position); scene.add(ring);
        flashes.push({fl,ring,t:0,dur:.55});
        // 后端球短暂提亮
        if(s.star){ s.star.material.emissiveIntensity=6.5; s.star.userData.flash=0.35; }
        scene.remove(s.mesh); scene.remove(s.trail); ships.splice(i,1); continue;
      }
    }
    s.mesh.position.copy(p);
    if(s.prev){ const d=new THREE.Vector3().subVectors(p,s.prev);
      if(d.lengthSq()>1e-9) s.mesh.quaternion.setFromUnitVectors(UP,d.normalize()); }
    s.prev=p.clone();
    s.history.push(p.clone()); if(s.history.length>22)s.history.shift();
    const arr=s.trail.geometry.attributes.position.array;
    for(let k=0;k<20;k++){const q=s.history[Math.min(k,s.history.length-1)]||p;
      arr[k*3]=q.x;arr[k*3+1]=q.y;arr[k*3+2]=q.z;}
    s.trail.geometry.attributes.position.needsUpdate=true;
  }

  for(let i=flashes.length-1;i>=0;i--){
    const f=flashes[i]; f.t+=dt/f.dur;
    if(f.t>=1){ scene.remove(f.fl); scene.remove(f.ring); flashes.splice(i,1); continue; }
    const k=f.t;
    f.fl.scale.setScalar(.35+1.5*k); f.fl.material.opacity=.95*(1-k);
    f.ring.scale.setScalar(1+4.5*k); f.ring.material.opacity=.9*(1-k);
  }
  // 熔断残骸：缓慢自转 + 火花呼吸
  clusters.forEach(c=>{(c.stars||[]).forEach(st=>{
    const d=st.userData.debris;
    if(d){
      d.rotation.y+=dt*.6;
      const t=performance.now()/1000;
      if(st.userData.debrisN) st.userData.debrisN.material.opacity=.55+.35*Math.sin(t*3.1);
      if(st.userData.debrisRing) st.userData.debrisRing.scale.setScalar(1+.12*Math.sin(t*2.2));
    }
  });});
  clusters.forEach(c=>{(c.stars||[]).forEach(st=>{ if(st.userData.flash!==undefined){
      st.userData.flash-=dt; if(st.userData.flash<=0){ st.material.emissiveIntensity=2.8; delete st.userData.flash; } } });});
  const _bq=new THREE.Quaternion();
  clusters.forEach(c=>{(c.stars||[]).forEach(st=>{
    const b=st.userData.belt; if(!b) return;
    st.getWorldQuaternion(_bq); b.quaternion.copy(_bq).invert();
    b.userData.spin=(b.userData.spin||0)+dt*.55;
    b.rotateY(b.userData.spin);
  });
    const on=!focus||focus===c; const f=on?1:.16;
    c.shell.material.opacity=(c.shell.userData.base||(c.m&&c.m.status==='idle'?.22:.5))*f;
    c.neb.forEach(sp=>{sp.material.opacity=(c.m&&c.m.status==='idle'?.06:.12)*f;});
    c.stars.forEach(st=>{st.material.transparent=true;st.material.opacity=f;st.material.depthWrite=true;});
  });
  projectLabels();
  renderer.render(scene,camera);
}

const tmpV=new THREE.Vector3();
function projectLabels(){
  const W=container.clientWidth,H=container.clientHeight;
  clusters.forEach((c,id)=>{
    c.grp.getWorldPosition(tmpV); tmpV.y+=0.34*(c.cscale||1); tmpV.project(camera);
    const ct=new THREE.Vector3(); c.grp.getWorldPosition(ct); ct.project(camera);
    const projectedClusterX=(ct.x*.5+.5)*W, projectedClusterY=(-ct.y*.5+.5)*H;
    (c.stars||[]).forEach((s,bi)=>{
      s.getWorldPosition(tmpV); tmpV.project(camera);
      const nd=labelEls.get('n_'+id+'_'+bi); if(!nd) return;
      const b=(c.m.backends||[])[bi];
      const act=(b&&b.active)||0;
      nd.textContent=act;
      nd.classList.toggle('hot', act>0);
      nd.style.opacity=focus&&focus!==c?.08:1;
      // 先按投影位置摆放，稍后统一做一次碰撞避让（见下方 resolveLabelOverlap）
      nd.dataset.x=String((tmpV.x*.5+.5)*W+16);
      nd.dataset.y=String((-tmpV.y*.5+.5)*H-14);
    });
  });
  resolveLabelOverlap();
}

/** 数字标签防重叠：以投影点为锚，做几轮轻量力导向散开（只移动标签，不动星球） */
function resolveLabelOverlap(){
  const items=[];
  labelEls.forEach(el=>{
    if(!el.dataset.x) return;
    const w=el.offsetWidth||22, h=el.offsetHeight||16;
    items.push({el,x:parseFloat(el.dataset.x),y:parseFloat(el.dataset.y),ox:parseFloat(el.dataset.x),oy:parseFloat(el.dataset.y),w,h});
  });
  for(let iter=0; iter<8; iter++){
    for(let i=0;i<items.length;i++){
      for(let j=i+1;j<items.length;j++){
        const a=items[i], b=items[j];
        const dx=b.x-a.x, dy=b.y-a.y;
        const minX=(a.w+b.w)*0.55, minY=(a.h+b.h)*0.62;
        if(Math.abs(dx)>minX || Math.abs(dy)>minY) continue;
        // 沿重叠较小的轴推开，并优先横向（避免竖着排一长串）
        const pushX=(minX-Math.abs(dx))*(dx>=0?1:-1)*0.5;
        const pushY=(minY-Math.abs(dy))*(dy>=0?1:-1)*0.5;
        if(Math.abs(dx)>=Math.abs(dy)*0.6){ a.x-=pushX; b.x+=pushX; }
        else { a.y-=pushY; b.y+=pushY; }
      }
    }
    // 回弹：始终轻微吸回锚点，防止标签飘太远
    items.forEach(it=>{ it.x+=(it.ox-it.x)*0.12; it.y+=(it.oy-it.y)*0.12; });
  }
  items.forEach(it=>{ it.el.style.transform=`translate(${it.x.toFixed(1)}px,${it.y.toFixed(1)}px)`; });
}

function onMove(e){
  const rect=renderer.domElement.getBoundingClientRect();
  const v=new THREE.Vector2(((e.clientX-rect.left)/rect.width)*2-1,-((e.clientY-rect.top)/rect.height)*2+1);
  raycaster.setFromCamera(v,camera);
  const hits=raycaster.intersectObjects([...clusters.values()].map(c=>c.hit),false);
  let m=null;
  if(hits.length){for(const c of clusters.values()){ if(c.hit===hits[0].object){m=c.m;break;} }}
  if((m&&hover&&hover.m&&m.id===hover.m.id)) return;
  hover=m?{m}:null; paint(m);
}
function onPick(e){
  if(!downPt) return; const moved=Math.hypot(e.clientX-downPt.x,e.clientY-downPt.y); downPt=null;
  if(moved>6) return;
  const rect=renderer.domElement.getBoundingClientRect();
  const v=new THREE.Vector2(((e.clientX-rect.left)/rect.width)*2-1,-((e.clientY-rect.top)/rect.height)*2+1);
  raycaster.setFromCamera(v,camera);
  const hits=raycaster.intersectObjects([...clusters.values()].map(c=>c.hit),false);
  if(hits.length){ for(const c of clusters.values()){ if(c.hit===hits[0].object){ focusCluster(c); return; } } }
  else if(focus) unfocus();
}
function focusCluster(c){
  const pos=c.grp.position.clone(); const dir=new THREE.Vector3().subVectors(camera.position,controls.target).normalize();
  camAnim={t:0,dur:.9,p0:camera.position.clone(),p1:pos.clone().add(dir.multiplyScalar(2.9*(c.cscale||1))),
    t0:controls.target.clone(),t1:pos.clone()};
  controls.enabled=false; controls.autoRotate=false; focus=c; document.body.classList.add('focused');
}
function unfocus(){
  camAnim={t:0,dur:.9,p0:camera.position.clone(),p1:new THREE.Vector3(-.6,4.4,10),
    t0:controls.target.clone(),t1:new THREE.Vector3(-.6,.2,0)};
  controls.enabled=false; focus=null; document.body.classList.remove('focused');
}

async function load(){
  try{
    const [snap]=await Promise.all([
      fetch('/dashboard/api/live',{cache:'no-store'}).then(r=>r.json()),
      loadCircuit()
    ]);
    applyLive(snap);
  }catch(e){ /* ignore */ }
}
function applyLive(d){
  DATA=d; if(!d||!d.ready) return;
  enrichModels(d.models);
  // 后端按流量排序返回，顺序会随繁忙度抖动；用**排序后的 id 集合**做 key，
  // 只有模型集合真的变化时才重建，避免朝向/位置被反复重置。
  const key=d.models.map(m=>m.id).slice().sort().join('|');
  if(key!==idsKey){ idsKey=key; rebuildClusters(d.models); } else updateClusters(d.models);
  rpm=d.totals.rpm||0;
}
function paint(h){
  if(!h||!h.m){ tip.style.opacity=0; return; }
  const m=h.m;
  tip.innerHTML='<b>'+m.name+'</b><br/>'+(m.levelCn||'')+' '+((m.busy||0).toFixed(0))+' · '+(m.rpm||0).toFixed(1)+'/min · 并发'+(m.active||0);
  tip.style.left=((h.p&&h.p.x)||0)+'px'; tip.style.top=(((h.p&&h.p.y)||0)-12)+'px';
  tip.style.opacity=1;
}

init();
return {
  /** 按真实调度事件发射飞船：优先命中指定后端，找不到退回该星云内随机星球 */
  spawnFor(modelId, backendName){
    const c = clusters.get(modelId);
    if(!c) return false;
    const backends = (c.m && c.m.backends) || [];
    let bi = backends.findIndex(b => b && b.name === backendName);
    if(bi < 0) bi = Math.floor(Math.random() * Math.max(1, c.stars.length));
    launchShip(c, bi);
    return true;
  },
  dispose(){ try{ renderer.dispose(); }catch(e){} }
};
}
