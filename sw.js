/**
 * Service Worker for MII Supervisor Hub
 *
 * Strategy:
 *  - App shell (HTML, JS): Cache-first with network fallback
 *  - API calls (/api/*):   Network-first with cache fallback
 *  - Install:              Pre-cache all known app shell files
 *  - Activate:             Clean up old cache versions
 */

const CACHE_VERSION = 'mii-hub-v5';

const APP_SHELL = [
  './',
  './hub.html',
  './action_tracker.html',
  './daily_report_template.html',
  './inspection.html',
  './inspections.html',
  './defects.html',
  './stores.html',
  './stores_requisition.html',
  './near_miss.html',
  './hot_work.html',
  './workplace_inspection.html',
  './supervisor_checklist.html',
  './toolbox_sign.html',
  './ssw.html',
  './training_matrix.html',
  './training_admin.html',
  './havs.html',
  './dashboard.html',
  './sheq_observations.html',
  './powra.html',
  './powra_register.html',
  './mii-db.js',
  './employees.json',
  './employees.js',
];

// ─── Install: pre-cache app shell ────────────────────────────────

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches
      .open(CACHE_VERSION)
      .then((cache) => {
        console.log('[SW] Pre-caching app shell');
        return cache.addAll(APP_SHELL);
      })
      .then(() => self.skipWaiting())
  );
});

// ─── Activate: purge old caches ──────────────────────────────────

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter((k) => k !== CACHE_VERSION)
            .map((k) => caches.delete(k))
        )
      )
      .then(() => self.clients.claim())
  );
});

// ─── Fetch: route by strategy ────────────────────────────────────

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // API calls → network-first
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(networkFirst(event.request));
    return;
  }

  // HTML pages → network-first (always get latest, fall back to cache offline)
  if (event.request.mode === 'navigate' || url.pathname.endsWith('.html')) {
    event.respondWith(networkFirst(event.request));
    return;
  }

  // Static assets (JS, CSS, images) → cache-first
  event.respondWith(cacheFirst(event.request));
});

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;

  try {
    const response = await fetch(request);
    // Cache successful GET responses
    if (response.ok && request.method === 'GET') {
      const cache = await caches.open(CACHE_VERSION);
      cache.put(request, response.clone());
    }
    return response;
  } catch (_) {
    // Offline and not cached — return a basic offline page
    return new Response('Offline — page not cached', {
      status: 503,
      headers: { 'Content-Type': 'text/plain' },
    });
  }
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    // Cache successful API GET responses for offline fallback
    if (response.ok && request.method === 'GET') {
      const cache = await caches.open(CACHE_VERSION);
      cache.put(request, response.clone());
    }
    return response;
  } catch (_) {
    const cached = await caches.match(request);
    if (cached) return cached;
    return new Response(JSON.stringify({ error: 'offline' }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

// ─── Background sync ─────────────────────────────────────────────

self.addEventListener('sync', (event) => {
  if (event.tag === 'mii-hub-sync') {
    event.waitUntil(doBackgroundSync());
  }
});

async function doBackgroundSync() {
  // Open IndexedDB directly from SW context
  const db = await new Promise((resolve, reject) => {
    const req = indexedDB.open('mii-hub', 1);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });

  const items = await new Promise((resolve, reject) => {
    const tx = db.transaction('sync_queue', 'readonly');
    const store = tx.objectStore('sync_queue');
    const req = store.getAll();
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });

  const pending = items.filter(
    (i) => i.status === 'pending' || i.status === 'failed'
  );
  if (pending.length === 0) return;

  // Get JWT from client (sent via postMessage)
  // For now, attempt batch sync if API URL is configured
  const API_BASE = self.__MII_API_BASE || '';
  const token = self.__MII_JWT || '';

  if (!API_BASE) {
    console.log('[SW] No API base configured — skipping sync');
    return;
  }

  try {
    const response = await fetch(`${API_BASE}/api/sync/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: token ? `Bearer ${token}` : '',
      },
      body: JSON.stringify({ items: pending }),
    });

    if (response.ok) {
      const result = await response.json();
      // Remove synced items from queue
      const tx = db.transaction('sync_queue', 'readwrite');
      const store = tx.objectStore('sync_queue');
      for (const item of pending) {
        store.delete(item._qid);
      }
      console.log(`[SW] Synced ${pending.length} items`);

      // Notify clients
      const clients = await self.clients.matchAll();
      clients.forEach((client) =>
        client.postMessage({ type: 'sync-complete', count: pending.length })
      );
    }
  } catch (err) {
    console.warn('[SW] Background sync failed, will retry', err);
  }
}

// ─── Message handling (receive config from main page) ────────────

self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'config') {
    self.__MII_API_BASE = event.data.apiBase || '';
    self.__MII_JWT = event.data.jwt || '';
  }
  if (event.data && event.data.type === 'trigger-sync') {
    doBackgroundSync();
  }
});
