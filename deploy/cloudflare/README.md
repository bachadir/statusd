# Cloudflare setup

The public page is static and holds no state; the data comes from the backends through tunnels.

```
browser ─▶ status.example.com        (Cloudflare Pages: web/public)
        └▶ api.status.example.com    (Load Balancer, cached) ─▶ pool: api-eu1 (tunnel, EU DC)
                                                              └▶ pool: api-us1 (tunnel, US DC)
```

1. **Pages.** Deploy `web/public/` (no build step): `wrangler pages deploy web/public --project-name statusd`.
   Set `apiBase` in `web/public/config.js` to `https://api.status.example.com` first.
2. **Tunnels.** Run one `cloudflared` per DC (`cloudflared-eu1.yml`, and the same for us1). Each exposes only `/api/v1/*` and `/healthz` of its local node.
3. **Load Balancer** on `api.status.example.com`:
   - Two origin pools, `api-eu1` and `api-us1` (one origin each), steering **off** or **dynamic** (both serve identical replicated data).
   - Health monitor: `GET /healthz`, expect `200`, every 15–30s. A node returns 503 when it has no raft leader or its data is older than 3 evaluation intervals, so a partitioned or stuck node is taken out automatically.
   - Fallback pool: either one (if both are unhealthy, Cloudflare still tries it).
4. **Cache rule** for `api.status.example.com/api/v1/*`: eligible for cache, respect origin `Cache-Control`. The API sends `max-age=30, stale-while-revalidate=30, stale-if-error=86400`, so if **both** DCs are down visitors keep getting the last answer, and the page shows its stale-data banner (it compares `data_as_of` with the clock).
5. **CORS**: set `public.cors_origins` in the statusd config to the Pages origin(s).
6. Optional: a Cloudflare Access policy is **not** needed for the public API; never route the admin port through the tunnel.
