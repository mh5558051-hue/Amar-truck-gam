const C='truck-v4';
self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!==C).map(x=>caches.delete(x)))).then(()=>clients.claim())));
self.addEventListener('fetch',e=>{const r=e.request,u=new URL(r.url);
 if(r.method!=='GET'||u.origin!==location.origin||/socket\.io|\/health/.test(u.pathname))return;
 const fresh=r.mode==='navigate'||u.pathname==='/'||/\.(html|js)$/.test(u.pathname);
 e.respondWith(fresh?fetch(new Request(r,{cache:'no-cache'})).then(x=>{if(x.ok){const y=x.clone();caches.open(C).then(c=>c.put(r,y))}return x}).catch(()=>caches.match(r))
  :caches.open(C).then(c=>c.match(r).then(h=>h||fetch(r).then(x=>{if(x.ok)c.put(r,x.clone());return x}))))});
