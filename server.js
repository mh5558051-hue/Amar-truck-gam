// Truck multiplayer server (Node.js + Socket.io) - PLAYER-SYNC RELAY ONLY.   netcode v4
//
// The phone runs ALL driving (physics, steering, brake, horn, map, sound) locally. This server never simulates
// anything. It only:
//   1. gives each player a slot (0-9) = player ID, and a collision-free spawn point,
//   2. receives each player's tiny state packet:  [x, y, z, heading, speed, wheelAngle, flags, t]
//      (the phone sends it at up to 15 Hz, but ONLY when something changed - see the delta rules in index.html),
//   3. relays it to the OTHER players at 15 Hz as ONE small volatile packet: [[slot, x, y, z, h, v, w, f, t(, far)], ...]
//      - only players within NEAR_IN metres are streamed at full rate (interest management),
//      - players farther away get one tiny "beacon" per second (minimap dot) - FAR_BEACON_MS=0 turns even that off,
//      - a state is sent to a viewer only if that viewer has not already received that exact version (delta).
// Packets are 'volatile': when somebody's internet is slow they are dropped instead of piling up, so nobody lags.
//
// It also serves the game files (index.html, mdl.txt, vwmdl.txt, audio.json / audio.txt, config.js) from the same
// folder, so the 3D model and the engine sounds load when you open  http://<server>:3000
//
// Run:  npm install && npm start
const http=require('http'),fs=require('fs'),path=require('path'),{Server}=require('socket.io');

const PORT=process.env.PORT||3000;                       // Render gives the port in process.env.PORT
const ROOT=path.resolve(process.env.PUBLIC_DIR||__dirname);
const GAME=process.env.GAME_FILE||'index.html';
const MAX_PLAYERS=10,MAX_MSG_PER_SEC=40;
const TICK_HZ=15;                                        // server broadcast rate (clients send at the same rate when something changes)
const NEAR_IN=600,NEAR_OUT=650;                          // interest radius (m). A player leaves your radius at NEAR_OUT so border cases don't flicker.
const FAR_BEACON_MS=1000;                                // far players: 1 packet/sec for the minimap. 0 = send NOTHING about far players
const SNAP_MS=400;                                       // every viewer gets a RELIABLE (non-volatile) full snapshot this often, so a dropped packet can never leave a truck invisible
const SAME_MS=300;                                       // an unchanged state is only re-forwarded after this long (guards against clients that send every tick)
const SPAWN_GAP=9,SPAWN_BEHIND=20,SPAWN_JITTER=2;       // spawn: min distance to anybody (m) / fallback spacing behind the previous player / random sideways offset
const GARAGE={x0:-45,x1:45,z0:-125,z1:-75};
const SLOTS=Array.from({length:MAX_PLAYERS},(_,i)=>({x:-32+(i%5)*16,z:i<5?-90:-112,h:0}));
const WORLD=20000;

// ---------------------------------------------------------------- static files
const MIME={'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.json':'application/json; charset=utf-8',
 '.txt':'text/plain; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg',
 '.svg':'image/svg+xml','.ico':'image/x-icon','.glb':'model/gltf-binary','.wav':'audio/wav','.mp3':'audio/mpeg','.ogg':'audio/ogg'};
const PRIVATE=new Set(['server.js','server.txt','package.json','package-lock.json']);
const send=(r,code,type,body)=>{r.writeHead(code,{'Content-Type':type});r.end(body)};
function serveFile(q,r,file){
 fs.stat(file,(err,st)=>{
  if(err||!st.isFile())return send(r,404,'text/plain; charset=utf-8','Not found');
  const etag='"'+st.size+'-'+Math.round(st.mtimeMs)+'"';
  if(q.headers['if-none-match']===etag){r.writeHead(304,{ETag:etag});return r.end()}
  r.writeHead(200,{'Content-Type':MIME[path.extname(file).toLowerCase()]||'application/octet-stream','Content-Length':st.size,
   ETag:etag,'Cache-Control':'no-cache','Access-Control-Allow-Origin':'*'});
  fs.createReadStream(file).on('error',()=>r.destroy()).pipe(r)})}
const srv=http.createServer((q,r)=>{
 let u;try{u=decodeURIComponent((q.url||'/').split('?')[0])}catch(e){return send(r,400,'text/plain','Bad request')}
 if(u==='/health')return send(r,200,'text/plain','ok netcode-v3 '+TICK_HZ+'Hz');
 if(u==='/')u='/'+GAME;
 const rel=path.normalize(u).replace(/^([/\\])+/,'');
 const file=path.join(ROOT,rel);
 if(!file.startsWith(ROOT+path.sep)||/(^|[/\\])(\.|node_modules)/.test(rel)||PRIVATE.has(path.basename(file).toLowerCase()))return send(r,404,'text/plain','Not found');
 if(u==='/'+GAME&&!fs.existsSync(file))return send(r,200,'text/plain; charset=utf-8','Truck server OK (put index.html next to server.js to serve the game)');
 serveFile(q,r,file)});

// ---------------------------------------------------------------- players
const io=new Server(srv,{cors:{origin:'*'},pingInterval:10000,pingTimeout:15000});
const players=new Map(); // socket.id -> {sk, slot, spawn, st, ver, verAt, sig, pairs, win, cnt, chatT}
const cl=(v,a,b)=>Math.min(b,Math.max(a,v));
const r2=v=>Math.round(v*100)/100;
const rnd=a=>(Math.random()*2-1)*a;
let SEQ=0; // global version counter: every forwarded state gets a new number, so "did this viewer already get it?" is one integer compare

// ---- spawn: own garage slot + random sideways offset; never closer than SPAWN_GAP to anybody's CURRENT position.
//      If every free slot is blocked: SPAWN_BEHIND metres behind the previous (most recently joined) player, random sideways offset.
const where=o=>o.st?{x:o.st[1],z:o.st[3]}:{x:o.spawn.x,z:o.spawn.z};
const isClear=pt=>{for(const o of players.values()){const q=where(o);if(Math.hypot(q.x-pt.x,q.z-pt.z)<SPAWN_GAP)return false}return true};
function pickSpawn(freeSlots){
 for(const s of freeSlots)for(let k=0;k<4;k++){const pt={x:SLOTS[s].x+rnd(SPAWN_JITTER),z:SLOTS[s].z,h:0};if(isClear(pt))return pt}
 let prev=null;for(const o of players.values())prev=o;
 if(prev){const q=where(prev);for(let i=1;i<=10;i++){const pt={x:q.x+rnd(4),z:q.z-SPAWN_BEHIND*i,h:0};if(isClear(pt))return pt}}
 const b=SLOTS[freeSlots[0]];return{x:b.x,z:b.z,h:0}}

io.on('connection',sk=>{
 const used=new Set([...players.values()].map(p=>p.slot)),free=[];
 for(let i=0;i<MAX_PLAYERS;i++)if(!used.has(i))free.push(i);
 if(!free.length){sk.emit('full');sk.disconnect(true);return}
 const slot=free[0];
 const p={sk,slot,spawn:pickSpawn(free),st:null,ver:0,verAt:0,sig:'',pairs:new Map(),win:0,cnt:0,chatT:0};
 players.set(sk.id,p);
 const peers=[...players.values()].filter(o=>o!==p).map(o=>({s:o.slot}));
 sk.emit('welcome',{id:sk.id,slot,spawn:p.spawn,garage:GARAGE,slots:SLOTS,hz:TICK_HZ,near:NEAR_IN,peers});
 sk.broadcast.emit('join',{s:slot});
 {const snap=[...players.values()].filter(o=>o!==p&&o.st).map(o=>Math.hypot(o.st[1]-p.spawn.x,o.st[3]-p.spawn.z)<=NEAR_IN?o.st:o.st.concat(1));if(snap.length)sk.emit('w',snap)} // reliable: existing players are visible at once
 sk.on('resync',()=>{p.snapAt=0}); // client asks for a fresh snapshot when it knows a player but has no position for it

 // player state: [x, y, z, heading, speed, wheelAngle, flags, t]   (flags: 1 horn, 2 lights, 4/8 indicators, 16 handbrake,
 //                                                                  32/64 doors, 128 reverse, 256 braking, 512 engine on, 1024 wrecked/dead)
 sk.on('s',m=>{
  if(!Array.isArray(m)||m.length<8)return;
  const now=Date.now();if(now-p.win>1000){p.win=now;p.cnt=0}if(++p.cnt>MAX_MSG_PER_SEC)return; // flood protection
  for(let i=0;i<8;i++)if(typeof m[i]!=='number'||!Number.isFinite(m[i]))return;
  const x=r2(cl(m[0],-WORLD,WORLD)),y=r2(cl(m[1],-100,1000)),z=r2(cl(m[2],-WORLD,WORLD));
  let h=m[3]%(2*Math.PI);if(h>Math.PI)h-=2*Math.PI;if(h<-Math.PI)h+=2*Math.PI;h=Math.round(h*1000)/1000;
  const v=Math.round(cl(m[4],-10,110)*10)/10,w=Math.round(cl(m[5],-45,45)),f=cl(m[6]|0,0,2047),t=Math.round(m[7]);
  if(p.st&&t<=p.st[8])return; // out-of-order packet
  const first=!p.st;
  p.st=[p.slot,x,y,z,h,v,w,f,t];
  if(first)for(const q of players.values())if(q!==p)q.sk.emit('w',[p.st]); // first position: reliable, not volatile
  const sig=x+','+y+','+z+','+h+','+v+','+w+','+f;
  if(sig!==p.sig||now-p.verAt>=SAME_MS){p.sig=sig;p.verAt=now;p.ver=++SEQ} // new version -> goes out on the next tick
 });
 sk.on('chat',t=>{if(typeof t!=='string')return;const now=Date.now();if(now-p.chatT<500)return;p.chatT=now;io.emit('chat',{from:p.slot+1,t:t.slice(0,120)})});
 sk.on('pingc',t=>sk.emit('pongc',t));
 sk.on('disconnect',()=>{players.delete(sk.id);for(const q of players.values())q.pairs.delete(sk.id);io.emit('leave',p.slot)}); // slot is free again
});

// 15x/sec. For every viewer: collect the players whose state this viewer has not received yet AND who are within its interest radius.
// One small volatile packet per viewer per tick; nothing at all when nothing changed.
setInterval(()=>{
 if(players.size<2)return;
 const now=Date.now();
 for(const v of players.values()){
  const vx=v.st?v.st[1]:v.spawn.x,vz=v.st?v.st[3]:v.spawn.z,out=[],due=now-(v.snapAt||0)>=SNAP_MS;if(due)v.snapAt=now;
  for(const o of players.values()){
   if(o===v||!o.st)continue;
   let pr=v.pairs.get(o.sk.id);if(!pr){pr={ver:0,at:0,near:false};v.pairs.set(o.sk.id,pr)}
   const dx=o.st[1]-vx,dz=o.st[3]-vz,lim=pr.near?NEAR_OUT:NEAR_IN,near=dx*dx+dz*dz<=lim*lim;
   if(near){
    if(due||!pr.near||pr.ver!==o.ver){out.push(o.st);pr.near=true;pr.ver=o.ver;pr.at=now}          // in range: full-rate stream (10th element absent)
   }else if(pr.near||(FAR_BEACON_MS>0&&(pr.ver!==o.ver||due)&&now-pr.at>=FAR_BEACON_MS)){
    out.push(o.st.concat(1));pr.near=false;pr.ver=o.ver;pr.at=now}                            // out of range: 'far' flag, tells the phone to hide the 3D truck
  }
  if(out.length){if(due)v.sk.emit('w',out);else v.sk.volatile.emit('w',out)}}
},1000/TICK_HZ);

srv.listen(PORT,()=>console.log('Truck sync server on :'+PORT+' | '+TICK_HZ+' Hz | interest '+NEAR_IN+' m | serving '+ROOT));
