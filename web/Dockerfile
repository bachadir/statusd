# Public status page for a showcase inside the clusters (the real page runs on Cloudflare Pages).
# nginx serves web/public and proxies the API to a statusd node, so the page works same-origin
# (config.js apiBase "") without CORS. STATUSD_UPSTREAM is set by the Helm chart (publicUI).
FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY public/ /usr/share/nginx/html/
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
ENV STATUSD_UPSTREAM=statusd:8080
EXPOSE 8080
