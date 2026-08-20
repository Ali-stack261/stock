# Self-Hosted Deployment: k3s + Self-Hosted Runner + Cloudflare Tunnel

Repo: `Ali-stack261/stock` (public)

No cloud account needed — this uses your own Windows machine as the deployment
target, reachable from the internet via Cloudflare Tunnel. Since the repo is public,
security lockdown comes first, not last.

---

## Step 1 — Lock down the security risk (do this before anything else)

Self-hosted runners on public repos carry a real risk: anyone who opens a pull
request could potentially get code execution on your machine through the runner,
unless this is restricted.

1. Go to your repo on GitHub → **Settings → Actions → General**
2. Scroll to **"Fork pull request workflows from outside collaborators"**
3. Select **"Require approval for all outside collaborators"**

This means any PR from someone who isn't you must be manually approved before its
workflow runs at all.

## Step 2 — Restrict deploy jobs to your own pushes only (defense in depth)

In `.github/workflows/ci.yml`, update `deploy-staging` and `deploy-production`:

```diff
   deploy-staging:
     needs: build-and-scan
-    runs-on: ubuntu-latest
+    if: github.event_name == 'push' && github.ref == 'refs/heads/master'
+    runs-on: self-hosted
     steps:
       ...
-      - uses: azure/k8s-set-context@v4
-        with:
-          kubeconfig: ${{ secrets.KUBE_CONFIG }}
+      # No kubeconfig secret needed — the self-hosted runner IS the k3s host,
+      # kubectl uses the local default config directly.

   deploy-production:
     needs: deploy-staging
-    runs-on: ubuntu-latest
+    if: github.event_name == 'push' && github.ref == 'refs/heads/master'
+    runs-on: self-hosted
     steps:
       ...
```

Only these two jobs move to `self-hosted` — `fast-tests`, `spark-mlflow-tests`,
`airflow-tests`, `lint`, and `build-and-scan` all stay on GitHub's own hosted
runners, unaffected. Your machine's exposure is limited to just these two jobs.

---

## Step 3 — Install k3s via WSL2

`k3s` needs a Linux kernel — on Windows, that means WSL2.

```powershell
# If WSL2 isn't already installed:
wsl --install -d Ubuntu-22.04
```
Restart if prompted, then open the new Ubuntu terminal (Start menu → Ubuntu) and set
a username/password when asked. From inside that Ubuntu/WSL2 terminal:

```bash
curl -sfL https://get.k3s.io | sh -
sudo systemctl status k3s   # confirm it's running
```

Get the kubeconfig k3s generates and confirm `kubectl` works:
```bash
sudo cat /etc/rancher/k3s/k3s.yaml
kubectl get nodes   # should show one node, status "Ready"
```

## Step 4 — Install and register a self-hosted GitHub Actions runner

Also inside the WSL2 Ubuntu terminal, so it shares the same environment as k3s:

1. Go to your repo → **Settings → Actions → Runners → New self-hosted runner**
2. Select **Linux**, follow the exact download/config commands GitHub shows you
   there (they include a unique registration token, so copy them directly from that
   page rather than a generic example here)
3. After the `./config.sh` step completes, run:
   ```bash
   ./run.sh
   ```
   Keep this terminal window open — the runner only listens for jobs while this is
   running. For it to survive terminal closes/reboots, install it as a service:
   ```bash
   sudo ./svc.sh install
   sudo ./svc.sh start
   ```

Confirm it shows as **"Idle"** (green) on the Settings → Actions → Runners page
before moving on.

## Step 5 — Cloudflare Tunnel for public reachability

Free tier, no card required for basic tunnel usage. Inside WSL2:

```bash
curl -L --output cloudflared.deb https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
sudo dpkg -i cloudflared.deb
cloudflared tunnel login   # opens a browser to authenticate with your Cloudflare account
cloudflared tunnel create stock-prediction
```
Note the tunnel ID it prints. Create a config file:
```bash
mkdir -p ~/.cloudflared
cat > ~/.cloudflared/config.yml << 'EOF'
tunnel: <paste-your-tunnel-id-here>
credentials-file: /home/<your-wsl-username>/.cloudflared/<tunnel-id>.json
ingress:
  - hostname: stock-prediction.<your-cloudflare-domain>
    service: http://localhost:80
  - service: http_status:404
EOF
cloudflared tunnel route dns stock-prediction stock-prediction.<your-cloudflare-domain>
```
(Requires a domain added to your Cloudflare account — if you don't have one yet,
Cloudflare also offers free `*.trycloudflare.com` quick tunnels for testing, no
domain needed, via `cloudflared tunnel --url http://localhost:80`, though those URLs
change on restart and aren't meant for anything long-term.)

Run it as a persistent service:
```bash
sudo cloudflared service install
sudo systemctl start cloudflared
```

## Step 6 — Create the Kubernetes Secrets the manifests actually need

`kubectl apply` creates Deployments/Services from the YAML files — it does **not**
create the Secret objects those manifests reference via `secretKeyRef`. Do this once:

```bash
kubectl create namespace staging
kubectl create namespace production

kubectl create secret generic serving-api-secrets \
  --from-literal=api-keys="your-real-staging-api-key-here" \
  -n staging

kubectl create secret generic serving-api-secrets \
  --from-literal=api-keys="your-real-production-api-key-here" \
  -n production
```

## Step 7 — Update `smoke-test.sh` to point at your real tunnel domain

Replace the placeholder `https://staging.stock-prediction.example.com` with your
actual `stock-prediction.<your-cloudflare-domain>` (or the `trycloudflare.com` URL if
using a quick tunnel) in wherever `smoke-test.sh` gets called from the workflow.

---

## Verification checklist before pushing

- [ ] `kubectl get nodes` shows a Ready node (from Step 3)
- [ ] Runner shows "Idle" on the GitHub Runners settings page (from Step 4)
- [ ] `cloudflared tunnel list` shows your tunnel, and hitting the public URL in a
      browser reaches *something* (even a 404 is fine at this stage — it confirms
      routing works before anything is actually deployed)
- [ ] Both namespaces and secrets exist: `kubectl get secrets -n staging` and
      `-n production`
- [ ] "Require approval for all outside collaborators" is set (Step 1) — check this
      one especially, since it's the actual security control everything else depends on

## Net effect

Once all of this is in place, pushing to `master` should let `deploy-staging` and
`deploy-production` actually run against your local `k3s` cluster via the self-hosted
runner, with the result reachable at your real Cloudflare Tunnel URL — a genuine,
live deployment with zero cloud cost and no card requirement anywhere in the chain.
