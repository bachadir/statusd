// Deploy-time configuration. On Cloudflare Pages set apiBase to the tunnel hostname,
// e.g. "https://api.status.example.com". Empty = same origin (local development).
window.STATUS_CONFIG = {
  apiBase: "",
  refreshSeconds: 60,
  staleAfterSeconds: 300
};
