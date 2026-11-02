# ERTECHLab Command Center Dashboard

> High-Availability 9-Node K3s Cluster & Enterprise Homelab Operations Center.

Synthesizing the best features of **Homer, Homepage, Dashy, Heimdall, and Glance** into a unified, high-performance telemetry and application launcher.

---

## Features

* **From Homer**:
  * Lightning-fast fuzzy search with `/` keyboard shortcut and `Esc` to clear.
  * Tag filter chips (`#all`, `#networking`, `#ai-automation`, `#web-tools`, `#cluster-mgmt`, `#infrastructure`).
* **From Homepage**:
  * Real-time K8s cluster telemetry widgets: Node health, active pod ratios, average response latency, and live system clock.
  * Category service badges and cluster health metrics.
* **From Dashy**:
  * 5 instant hot-swappable themes with `localStorage` persistence:
    * `Midnight Glass` (Default frosted dark glassmorphism)
    * `Cyberpunk Neon` (Electric cyan and magenta glow)
    * `Nordic Frost` (Arctic slate and ice blue Nord palette)
    * `OLED Pure Black` (True black for OLED displays)
    * `Dracula Dark` (Vampire purple and emerald green)
  * Multi-layout switcher:
    * `Cards` (Heimdall rich tiles with descriptions, tags, and dual actions)
    * `Compact` (Homepage dense metric cards with latency badges)
    * `Rows` (Glance minimalist telemetry rows)
  * In-app Customizer Modal to add/edit/delete services directly in the UI with instant save to `/api/config`.
* **From Heimdall**:
  * Crisp SVG logos for every homelab service.
  * Dual-routing action buttons: One-click "Open" (via HTTPS domain) and "LAN" (direct local NodePort/IP).
  * Real-time pulse ping indicator with exact latency (`ms`) and HTTP response code tooltips.
* **From Glance**:
  * Interactive live K3s cluster nodes glance bar showing all 9 nodes (`k3s-cp-01..04`, `k3s-worker-01..05`) with ping latency in milliseconds.
  * Proxmox VE hypervisor status monitor.

---

## Architecture

* **Backend**: Lightweight Python 3 async-threaded telemetry engine (`app.py`), querying the local Kubernetes API (`https://kubernetes.default.svc`) and probing service endpoints in parallel.
* **Frontend**: Pure zero-dependency HTML5 / CSS3 / ES6 JavaScript (`static/index.html`). No heavy external frameworks or CDN dependencies.
* **Configuration**: Dynamic JSON configuration (`config.json`), editable via REST API (`GET/POST /api/config`).

---

## Deployment

### Kubernetes
```bash
kubectl apply -f k8s-manifest.yaml
```

### Local / Docker
```bash
PORT=8080 python3 app.py
```
