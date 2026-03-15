/**
 * Service Worker for MII Supervisor Hub
 *
 * Strategy: Network-first for ALL requests.
 * Cache is only used as offline fallback.
 * d92242f1 is replaced by deploy.sh on every push.
 */

const CACHE_VERSION = 'mii-hub-d92242f1';

// ─── Install: skip waiting to activate immediately ──────────────
self.addEventListener('install', (event) => {
  console.log('[SW] Installing', CACHE_VERSION);
  self.skipWaiting();
});

// ─── Activate: purge ALL old caches ─────────────────────────────
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter((k) => k !== CACHE_VERSION)
            .map((k) => {
              console.log('[SW] Purging old cache:', k);
              return caches.delete(k);
            })
        )
      )
      .then(() => self.clients.claim())
  );
});

// ─── Fetch: network-first, cache as fallback ────────────────────
self.addEventListener('fetch', (event) => {
  // Skip non-GET and cross-origin
  if (event.request.method !== 'GET') return;
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;

  event.respondWith(networkFirst(event.request));
});

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_VERSION);
      cache.put(request, response.clone());
    }
    return response;
  } catch (_) {
    const cached = await caches.match(request);
    if (cached) return cached;
    return new Response('Offline', {
      status: 503,
      headers: { 'Content-Type': 'text/plain' },
    });
  }
}
