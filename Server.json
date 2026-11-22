// Truck multiplayer server (Node.js + Socket.io) — PLAYER-SYNC RELAY ONLY.
//
// The phone runs ALL driving (physics, steering, brake, horn, map, sound) locally. This server never simulates
// anything. It only:
//   1. gives each player a slot (0-9) = player ID, and a spawn point in the garage,
//   2. receives each player's tiny state packet 10x/sec:  [x, y, z, heading, speed, wheelAngle, flags, t]
//   3. relays it to the OTHER players 10x/sec:            [[slot, x, y, z, h, v, w, f, t], ...]
// Packets are 'volatile': when somebody's internet is slow they are dropped instead of piling up, so nobody lags.
//
// It also serves the game files (index.html, mdl.txt, vwmdl.txt, audio.json / audio.txt, config.js) from the same
// folder, so the 3D model and the engine sounds load when you open  http://<server>:3000
//
// Run:  npm install && npm start
const http=require('http'),fs=require('fs'),path=require('path'),{Server}=require('socket.io');

const PORT=process.env.PORT||3000;
const ROOT=path.resolve(process.env.PUBLIC_DIR||__dirname);
const GAME=process.env.GAME_FILE||'index.html';
const MAX_PLAYERS=10,TICK_HZ=10,HEARTBEAT_MS=500,MAX_MSG_PER_SEC=30;
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
 if(u==='/health')return send(r,200,'text/plain','ok');
 if(u==='/')u='/'+GAME;
 const rel=path.normalize(u).replace(/^([/\\])+/,'');
 const file=path.join(ROOT,rel);
 if(!file.startsWith(ROOT+path.sep)||/(^|[/\\])(\.|node_modules)/.test(rel)||PRIVATE.has(path.basename(file).toLowerCase()))return send(r,404,'text/plain','Not found');
 if(u==='/'+GAME&&!fs.existsSync(file))return send(r,200,'text/plain; charset=utf-8','Truck server OK (put index.html next to server.js to serve the game)');
 serveFile(q,r,file)});

// ---------------------------------------------------------------- players
const io=new Server(srv,{cors:{origin:'*'},pingInterval:10000,pingTimeout:15000});
const players=new Map(); // socket.id -> {sk, slot, st, sig, dirty, lastSent, win, cnt, chatT}
const cl=(v,a,b)=>Math.min(b,Math.max(a,v));
const r2=v=>Math.round(v*100)/100;

io.on('connection',sk=>{
 const used=new Set([...players.values()].map(p=>p.slot));
 let slot=-1;for(let i=0;i<MAX_PLAYERS;i++)if(!used.has(i)){slot=i;break}
 if(slot<0){sk.emit('full');sk.disconnect(true);return}
 const p={sk,slot,st:null,sig:'',dirty:false,lastSent:0,win:0,cnt:0,chatT:0};
 players.set(sk.id,p);
 const peers=[...players.values()].filter(o=>o!==p).map(o=>({s:o.slot}));
 sk.emit('welcome',{id:sk.id,slot,spawn:SLOTS[slot],garage:GARAGE,slots:SLOTS,hz:TICK_HZ,peers});
 sk.broadcast.emit('join',{s:slot});
 for(const o of players.values())if(o!==p&&o.st)o.dirty=true; // the newcomer sees everybody on the very next tick

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
  const sig=x+','+y+','+z+','+h+','+v+','+w+','+f;
  p.st=[p.slot,x,y,z,h,v,w,f,t];
  if(sig!==p.sig){p.sig=sig;p.dirty=true}});
 sk.on('chat',t=>{if(typeof t!=='string')return;const now=Date.now();if(now-p.chatT<500)return;p.chatT=now;io.emit('chat',{from:p.slot+1,t:t.slice(0,120)})});
 sk.on('pingc',t=>sk.emit('pongc',t));
 sk.on('disconnect',()=>{players.delete(sk.id);io.emit('leave',p.slot)}); // slot is free again
});

// 10x/sec: send every moved player (and a 2 Hz heartbeat for parked ones) to everybody else, as ONE small volatile packet
setInterval(()=>{
 const now=Date.now(),moved=[];
 for(const p of players.values())if(p.st&&(p.dirty||now-p.lastSent>=HEARTBEAT_MS))moved.push(p);
 if(!moved.length)return;
 for(const p of players.values()){
  const out=[];for(const o of moved)if(o!==p)out.push(o.st);
  if(out.length)p.sk.volatile.emit('w',out)}
 for(const o of moved){o.dirty=false;o.lastSent=now}
},1000/TICK_HZ);

srv.listen(PORT,()=>console.log('Truck sync server on :'+PORT+' | serving '+ROOT));
