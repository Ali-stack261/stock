# Dashboard: Real Frontend Wired to the Live API — Setup Instructions

Repo: `Ali-stack261/stock`

## What changed

The old bare `PredictionDashboard.jsx` at the repo root (no build tooling, simulated
client-side data) is replaced by a real Vite + React project at `dashboard/`, wired
to the actual FastAPI backend endpoints instead of fabricated data.

**Verified locally in a sandbox before handing this over:**
- `npm install` — installs cleanly, 103 packages
- `npm run build` — succeeds, produces a working `dist/` (570KB JS, gzipped ~161KB)
- `npm run dev` — dev server starts and serves the page correctly

## Files included

```
dashboard/
├── .env.example
├── .gitignore
├── index.html
├── package.json
├── vite.config.js
└── src/
    ├── main.jsx
    └── PredictionDashboard.jsx
docker/
└── dashboard.Dockerfile   (updated — see below)
```

## How to apply

1. **Copy the `dashboard/` folder into your repo root**, replacing nothing (it's new).
2. **Delete the old stray file** at the repo root:
   ```powershell
   cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
   Remove-Item PredictionDashboard.jsx
   ```
3. **Replace `docker/dashboard.Dockerfile`** with the updated version included here
   — it now correctly builds from `dashboard/` instead of assuming a `package.json`
   at the repo root (which never existed, so the old Dockerfile could never have
   actually built).

## What's real vs. what was simulated before

| Panel | Data source |
|---|---|
| Price/prediction chart | **Real** — polls `GET /predictions/{symbol}` every 5s |
| Prediction feed | **Real** — same endpoint, actual stored rows including `realized_error` |
| Model health (RMSE/MAE) | **Real** — polls `GET /metrics/accuracy` |
| "Get a live prediction" form | **Real** — calls `POST /predict` on demand with a price you enter |
| Drift check | **Real** — "run check now" button calls `POST /drift/check` |
| Connection status | **Real** — actual `GET /health` result, not assumed |

**One honest design note:** `/predict` is request/response, not a live stream — there's
no endpoint that pushes real-time ticks to a browser. Rather than fabricate a fake
live feed the backend doesn't actually provide, the dashboard polls real stored
history and gives you a manual form to request a fresh prediction against a price you
type in. If you build a WebSocket streaming endpoint on the backend later, the chart
can switch from polling to push — worth revisiting then.

## Running it locally

```powershell
cd dashboard
copy .env.example .env
```
Edit `.env` if your API isn't running at `http://localhost:8000`, then:
```powershell
npm install
npm run dev
```
Open `http://localhost:5173`. Make sure `serving/app.py` is actually running
(`uvicorn serving.app:app --reload`) or you'll see the red "API unreachable" banner —
that's the dashboard correctly telling you the truth, not a bug.

## Security note — read before deploying publicly

`VITE_API_KEY` gets bundled directly into the built JavaScript. Anyone who opens
browser dev tools on a deployed site can read it. This is fine for local development
against your own machine, but **not safe for a real public deployment** — before
putting this dashboard on the internet for real, put a thin server-side proxy in
front that holds the real API key, rather than shipping it to the browser.

## Not yet done — worth deciding next

- **Not added back to the CI build matrix** (`build-and-scan` in `.github/workflows/ci.yml`)
  — it was removed earlier when no source existed for it to build. Now that real
  source exists, this is a quick addition once you're ready.
- **No production API URL configured** — `.env.example` defaults to `localhost:8000`;
  once you have a real deployed backend URL (from the Kubernetes work), update
  `VITE_API_BASE_URL` accordingly, likely as a build-time environment variable in CI
  rather than a committed `.env`.

## Verification once you've applied this

```powershell
cd dashboard
npm install
npm run build
```
Should complete with `✓ built in ~8s` and no errors — matches what was already
confirmed in the sandbox before this was handed off.
