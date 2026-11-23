// Truck multiplayer relay (Node + Socket.io): slots, spawn, colour, state relay by distance. No map/model/sound sent.
const http=require('http'),fs=require('fs'),path=require('path'),zlib=require('zlib'),{Server}=require('socket.io');
const RELAY_ONLY=/^(1|true|yes)$/i.test(process.env.RELAY_ONLY||'');
const PORT=process.env.PORT||3000,ROOT=path.resolve(process.env.PUBLIC_DIR||__dirname),GAME=process.env.GAME_FILE||'index.html';
const MAX_PLAYERS=10,MAX_MSG_PER_SEC=40,TICK_HZ=Math.max(5,Math.min(30,+process.env.TICK_HZ||15));
const NEAR_IN=150,NEAR_OUT=165,SNAP_MS=400,SAME_MS=300,SPAWN_GAP=9,SPAWN_BEHIND=20,SPAWN_JITTER=2,WORLD=20000;
const gapFor=d=>d<30?0:d<100?200:1000; // <30m: every tick (15/s) | 30-100m: 5/s | 100-150m: 1/s | >150m: nothing
const GARAGE={x0:-45,x1:45,z0:-125,z1:-75};
const SLOTS=Array.from({length:MAX_PLAYERS},(_,i)=>({x:-32+(i%5)*16,z:i<5?-90:-112,h:0}));
const MIME={'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.json':'application/json; charset=utf-8','.txt':'text/plain; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml','.ico':'image/x-icon','.glb':'model/gltf-binary','.wav':'audio/wav','.mp3':'audio/mpeg','.ogg':'audio/ogg'};
const PRIVATE=new Set(['server.js','server.txt','package.json','package-lock.json']);
const send=(r,c,t,b)=>{r.writeHead(c,{'Content-Type':t});r.end(b)};
const GZ=new Map(),GZT=new Set(['.html','.js','.json','.txt','.css','.svg']);
function gz(file,st,cb){const k=file+'|'+st.size+'|'+Math.round(st.mtimeMs),h=GZ.get(k);if(h&&h.buf)return cb(h.buf);if(h)return h.q.push(cb);
 const e={buf:null,q:[cb]};for(const x of GZ.keys())if(x.startsWith(file+'|'))GZ.delete(x);GZ.set(k,e);
 fs.readFile(file,(er,raw)=>{if(er){GZ.delete(k);return e.q.forEach(f=>f(null))}zlib.gzip(raw,{level:6},(er2,o)=>{if(er2){GZ.delete(k);return e.q.forEach(f=>f(null))}e.buf=o;e.q.forEach(f=>f(o))})})}
function serveFile(q,r,file){fs.stat(file,(err,st)=>{
 if(err||!st.isFile())return send(r,404,'text/plain','Not found');
 const ext=path.extname(file).toLowerCase(),etag='"'+st.size+'-'+Math.round(st.mtimeMs)+'"';
 if(q.headers['if-none-match']===etag){r.writeHead(304,{ETag:etag});return r.end()}
 const hdr={'Content-Type':MIME[ext]||'application/octet-stream',ETag:etag,'Access-Control-Allow-Origin':'*',Vary:'Accept-Encoding','Cache-Control':(ext==='.html'||ext==='.js')?'no-cache':'public, max-age=31536000, immutable'};
 const raw=()=>{hdr['Content-Length']=st.size;r.writeHead(200,hdr);fs.createReadStream(file).on('error',()=>r.destroy()).pipe(r)};
 if(GZT.has(ext)&&st.size>1024&&st.size<40e6&&/\bgzip\b/.test(q.headers['accept-encoding']||''))
  return gz(file,st,b=>{if(!b)return raw();hdr['Content-Encoding']='gzip';hdr['Content-Length']=b.length;r.writeHead(200,hdr);r.end(b)});
 raw()})}
const srv=http.createServer((q,r)=>{let u;try{u=decodeURIComponent((q.url||'/').split('?')[0])}catch(e){return send(r,400,'text/plain','Bad request')}
 if(u==='/health')return send(r,200,'text/plain','ok '+TICK_HZ+'Hz'+(RELAY_ONLY?' relay-only':''));
 if(RELAY_ONLY)return send(r,404,'text/plain','Relay only');
 if(u==='/')u='/'+GAME;
 const rel=path.normalize(u).replace(/^([/\\])+/,''),file=path.join(ROOT,rel);
 if(!file.startsWith(ROOT+path.sep)||/(^|[/\\])(\.|node_modules)/.test(rel)||PRIVATE.has(path.basename(file).toLowerCase()))return send(r,404,'text/plain','Not found');
 if(u==='/'+GAME&&!fs.existsSync(file))return send(r,200,'text/plain','Truck server OK');
 serveFile(q,r,file)});
const io=new Server(srv,{cors:{origin:'*'},pingInterval:10000,pingTimeout:15000,perMessageDeflate:false,httpCompression:false});
const players=new Map(),cl=(v,a,b)=>Math.min(b,Math.max(a,v)),r2=v=>Math.round(v*100)/100,rnd=a=>(Math.random()*2-1)*a;
let SEQ=0;
const where=o=>o.st?{x:o.st[1],z:o.st[3]}:{x:o.spawn.x,z:o.spawn.z};
const isClear=pt=>{for(const o of players.values()){const q=where(o);if(Math.hypot(q.x-pt.x,q.z-pt.z)<SPAWN_GAP)return false}return true};
function pickSpawn(free){
 for(const s of free)for(let k=0;k<4;k++){const pt={x:SLOTS[s].x+rnd(SPAWN_JITTER),z:SLOTS[s].z,h:0};if(isClear(pt))return pt}
 let prev=null;for(const o of players.values())prev=o;
 if(prev){const q=where(prev);for(let i=1;i<=10;i++){const pt={x:q.x+rnd(4),z:q.z-SPAWN_BEHIND*i,h:0};if(isClear(pt))return pt}}
 const b=SLOTS[free[0]];return{x:b.x,z:b.z,h:0}}
io.on('connection',sk=>{
 const used=new Set([...players.values()].map(p=>p.slot)),free=[];
 for(let i=0;i<MAX_PLAYERS;i++)if(!used.has(i))free.push(i);
 if(!free.length){sk.emit('full');sk.disconnect(true);return}
 const slot=free[0],p={col:undefined,sk,slot,spawn:pickSpawn(free),st:null,ver:0,verAt:0,sig:'',pairs:new Map(),win:0,cnt:0,chatT:0};
 players.set(sk.id,p);
 const peers=[...players.values()].filter(o=>o!==p).map(o=>o.col===undefined?{s:o.slot}:{s:o.slot,c:o.col});
 sk.emit('welcome',{id:sk.id,slot,spawn:p.spawn,garage:GARAGE,slots:SLOTS,hz:TICK_HZ,near:NEAR_IN,peers});
 sk.broadcast.emit('join',{s:slot});
 {const snap=[...players.values()].filter(o=>o!==p&&o.st&&Math.hypot(o.st[1]-p.spawn.x,o.st[3]-p.spawn.z)<=NEAR_IN).map(o=>o.st);if(snap.length)sk.emit('w',snap)}
 sk.on('resync',()=>{p.snapAt=0});
 // state: [x,y,z,heading,speed,wheelAngle,flags,t]  flags: 1 horn,2 lights,4/8 indicators,16 handbrake,32/64 doors,128 reverse,256 brake,512 engine,1024 dead
 sk.on('s',m=>{
  if(!Array.isArray(m)||m.length<8)return;
  const now=Date.now();if(now-p.win>1000){p.win=now;p.cnt=0}if(++p.cnt>MAX_MSG_PER_SEC)return;
  for(let i=0;i<8;i++)if(typeof m[i]!=='number'||!Number.isFinite(m[i]))return;
  const x=r2(cl(m[0],-WORLD,WORLD)),y=r2(cl(m[1],-100,1000)),z=r2(cl(m[2],-WORLD,WORLD));
  let h=m[3]%(2*Math.PI);if(h>Math.PI)h-=2*Math.PI;if(h<-Math.PI)h+=2*Math.PI;h=Math.round(h*1000)/1000;
  const v=Math.round(cl(m[4],-10,110)*10)/10,w=Math.round(cl(m[5],-45,45)),f=cl(m[6]|0,0,2047),t=Math.round(m[7]);
  if(p.st&&t<=p.st[8])return;
  const first=!p.st;p.st=[p.slot,x,y,z,h,v,w,f,t];
  if(first)for(const q of players.values())if(q!==p&&Math.hypot(q.st?q.st[1]-x:1e9,q.st?q.st[3]-z:0)<=NEAR_IN)q.sk.emit('w',[p.st]);
  const sig=x+','+y+','+z+','+h+','+v+','+w+','+f;
  if(sig!==p.sig||now-p.verAt>=SAME_MS){p.sig=sig;p.verAt=now;p.ver=++SEQ}});
 sk.on('col',c=>{if(!Number.isInteger(c)||c<-1||c>0xFFFFFF)return;p.col=c;sk.broadcast.emit('col',{s:slot,c})}); // real paint colour: everyone draws the same
 sk.on('chat',t=>{if(typeof t!=='string')return;const n=Date.now();if(n-p.chatT<500)return;p.chatT=n;io.emit('chat',{from:p.slot+1,t:t.slice(0,120)})});
 sk.on('pingc',t=>sk.emit('pongc',t));
 sk.on('disconnect',()=>{players.delete(sk.id);for(const q of players.values())q.pairs.delete(sk.id);io.emit('leave',p.slot)})});
setInterval(()=>{
 if(players.size<2)return;
 const now=Date.now();
 for(const v of players.values()){
  const vx=v.st?v.st[1]:v.spawn.x,vz=v.st?v.st[3]:v.spawn.z,out=[],due=now-(v.snapAt||0)>=SNAP_MS;if(due)v.snapAt=now;
  for(const o of players.values()){
   if(o===v||!o.st)continue;
   let pr=v.pairs.get(o.sk.id);if(!pr){pr={ver:0,at:0,near:false};v.pairs.set(o.sk.id,pr)}
   const d=Math.hypot(o.st[1]-vx,o.st[3]-vz);
   if(d<=(pr.near?NEAR_OUT:NEAR_IN)){
    if(!pr.near||((pr.ver!==o.ver||due)&&now-pr.at>=gapFor(d))){out.push(o.st);pr.near=true;pr.ver=o.ver;pr.at=now}
   }else if(pr.near){out.push(o.st.concat(1));pr.near=false} // left range once: phone hides the truck; after that nothing is sent
  }
  if(out.length){if(due)v.sk.emit('w',out);else v.sk.volatile.emit('w',out)}}
},1000/TICK_HZ);
srv.listen(PORT,()=>console.log('Truck sync server :'+PORT+' | '+TICK_HZ+' Hz | interest '+NEAR_IN+' m'));
