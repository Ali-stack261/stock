import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  ComposedChart, Line, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from "recharts";
import {
  Activity, TrendingUp, TrendingDown, Wifi, WifiOff, Database, Cpu, Zap,
  GitBranch, AlertTriangle, CheckCircle2, ChevronDown, Send,
} from "lucide-react";

// ---------------------------------------------------------------------------
// Config — talks to the real FastAPI serving layer (serving/app.py).
// Set these at build/dev time via a .env file (see .env.example).
// NOTE: shipping a real API key in frontend JS is not safe for a public
// deployment — anyone can read it from the browser bundle. Fine for local
// dev against your own backend; for a real deploy, put a thin proxy server
// in front that holds the key server-side instead.
// ---------------------------------------------------------------------------
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const API_KEY = import.meta.env.VITE_API_KEY || "dev-key-12345";

// ---------------------------------------------------------------------------
// Token system (unchanged from the original design)
// ---------------------------------------------------------------------------
const C = {
  bg: "#080B10",
  panel: "#0F131A",
  panelAlt: "#12161E",
  border: "#1C222C",
  borderLight: "#262E3B",
  textPrimary: "#E7EAF0",
  textSecondary: "#7C879A",
  textMuted: "#454F5F",
  up: "#33D69F",
  upDim: "#1A5C46",
  down: "#FF5C6C",
  downDim: "#5C2129",
  signal: "#5B8DEF",
  signalDim: "#1E2D52",
  warn: "#F5A623",
  warnDim: "#4A3A15",
};

const FONTS = `
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600&display=swap');
`;

const SYMBOLS = [
  { id: "BTCUSDT", label: "BTC/USDT" },
  { id: "ETHUSDT", label: "ETH/USDT" },
  { id: "AAPL", label: "AAPL" },
];

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------
async function apiFetch(path, options = {}) {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": API_KEY,
      ...(options.headers || {}),
    },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  return res.json();
}

async function fetchHealth() {
  const res = await fetch(`${API_BASE_URL}/health`);
  return res.ok;
}

async function fetchPredictionHistory(symbol, limit = 48) {
  return apiFetch(`/predictions/${symbol}?limit=${limit}`);
}

async function fetchAccuracy(symbol) {
  return apiFetch(`/metrics/accuracy?symbol=${symbol}`);
}

async function postPrediction(payload) {
  return apiFetch(`/predict`, { method: "POST", body: JSON.stringify(payload) });
}

async function postDriftCheck(symbol) {
  return apiFetch(`/drift/check?symbol=${symbol}`, { method: "POST" });
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------
function fmtPrice(p) {
  if (p == null || Number.isNaN(p)) return "—";
  return p.toFixed(2);
}
function fmtTime(iso) {
  try {
    return new Date(iso).toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return "—";
  }
}

// ---------------------------------------------------------------------------
// Small building blocks
// ---------------------------------------------------------------------------
function Eyebrow({ children }) {
  return (
    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, letterSpacing: "0.08em", color: C.textMuted, marginBottom: 8 }}>
      {children}
    </div>
  );
}

function Panel({ children, style }) {
  return (
    <div style={{ background: C.panel, border: `1px solid ${C.border}`, borderRadius: 6, padding: 16, ...style }}>
      {children}
    </div>
  );
}

function LivePulse({ connected }) {
  const color = connected ? C.up : C.down;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: color, animation: connected ? "pulseDot 1.6s ease-out infinite" : "none" }} />
      <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, letterSpacing: "0.08em", color }}>
        {connected ? "API CONNECTED" : "API UNREACHABLE"}
      </span>
    </div>
  );
}

function StatusRow({ icon: Icon, label, value, status = "ok" }) {
  const color = status === "ok" ? C.up : status === "warn" ? C.warn : status === "err" ? C.down : C.textMuted;
  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "7px 0" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <Icon size={13} color={C.textSecondary} />
        <span style={{ fontFamily: "Inter, sans-serif", fontSize: 12.5, color: C.textSecondary }}>{label}</span>
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12.5, color: C.textPrimary }}>{value}</span>
        <span style={{ width: 6, height: 6, borderRadius: "50%", background: color }} />
      </div>
    </div>
  );
}

function DriftGauge({ featureDrift, conceptDrift, checked }) {
  const state = !checked ? "unknown" : featureDrift || conceptDrift ? "drift" : "clean";
  const color = state === "clean" ? C.up : state === "drift" ? C.down : C.textMuted;
  const label = state === "clean" ? "NO DRIFT" : state === "drift" ? "DRIFT DETECTED" : "NOT CHECKED YET";
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 8 }}>
      <div style={{ width: 64, height: 64, borderRadius: "50%", border: `3px solid ${color}`, display: "flex", alignItems: "center", justifyContent: "center" }}>
        {state === "drift" ? <AlertTriangle size={22} color={color} /> : <CheckCircle2 size={22} color={color} />}
      </div>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, letterSpacing: "0.04em", color }}>{label}</div>
      {checked && (
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: C.textMuted, textAlign: "center" }}>
          feature: {featureDrift ? "yes" : "no"} · concept: {conceptDrift ? "yes" : "no"}
        </div>
      )}
    </div>
  );
}

function CustomTooltip({ active, payload }) {
  if (!active || !payload || !payload.length) return null;
  const row = payload[0].payload;
  return (
    <div style={{ background: C.panelAlt, border: `1px solid ${C.borderLight}`, borderRadius: 4, padding: "8px 10px", fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>
      <div style={{ color: C.textMuted, marginBottom: 4 }}>{row.time}</div>
      <div style={{ color: C.textPrimary }}>actual&nbsp;&nbsp;{fmtPrice(row.price)}</div>
      {row.predicted != null && <div style={{ color: C.signal }}>pred&nbsp;&nbsp;&nbsp;&nbsp;{fmtPrice(row.predicted)}</div>}
    </div>
  );
}

function LegendDot({ color, label, dashed }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <span style={{ width: 14, height: dashed ? 0 : 2, borderTop: dashed ? `1.75px dashed ${color}` : `2px solid ${color}` }} />
      <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10.5, color: C.textMuted }}>{label}</span>
    </div>
  );
}

function MetricBlock({ label, value, good }) {
  return (
    <div style={{ background: C.panelAlt, border: `1px solid ${C.border}`, borderRadius: 5, padding: "9px 10px" }}>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: C.textMuted, marginBottom: 3 }}>{label}</div>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 17, fontWeight: 600, color: good === undefined ? C.textPrimary : good ? C.up : C.down }}>
        {value}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export default function PredictionDashboard() {
  const [symIdx, setSymIdx] = useState(0);
  const [symbolOpen, setSymbolOpen] = useState(false);
  const sym = SYMBOLS[symIdx];

  const [connected, setConnected] = useState(false);
  const [history, setHistory] = useState([]); // real prediction rows from /predictions/{symbol}
  const [accuracy, setAccuracy] = useState({ rolling_rmse: null, rolling_mae: null });
  const [drift, setDrift] = useState({ checked: false, featureDrift: false, conceptDrift: false });
  const [loadError, setLoadError] = useState(null);

  // Manual "get a live prediction" form — since /predict is request/response,
  // not a stream, the dashboard can't invent a live tick feed on its own.
  const [formPrice, setFormPrice] = useState("");
  const [formSubmitting, setFormSubmitting] = useState(false);
  const [lastPrediction, setLastPrediction] = useState(null);

  const pollRef = useRef(null);

  const refreshData = useCallback(async () => {
    try {
      const ok = await fetchHealth();
      setConnected(ok);
      const [hist, acc] = await Promise.all([
        fetchPredictionHistory(sym.id, 48),
        fetchAccuracy(sym.id),
      ]);
      setHistory(hist);
      setAccuracy(acc);
      setLoadError(null);
    } catch (err) {
      setConnected(false);
      setLoadError(err.message);
    }
  }, [sym.id]);

  useEffect(() => {
    refreshData();
    pollRef.current = setInterval(refreshData, 5000);
    return () => clearInterval(pollRef.current);
  }, [refreshData]);

  const handleGetPrediction = async (e) => {
    e.preventDefault();
    const price = parseFloat(formPrice);
    if (!price || price <= 0) return;
    setFormSubmitting(true);
    try {
      const result = await postPrediction({ symbol: sym.id, current_price: price });
      setLastPrediction(result);
      await refreshData();
    } catch (err) {
      setLoadError(err.message);
    } finally {
      setFormSubmitting(false);
    }
  };

  const handleCheckDrift = async () => {
    try {
      const result = await postDriftCheck(sym.id);
      setDrift({ checked: true, featureDrift: result.feature_drift_detected, conceptDrift: result.concept_drift_detected });
    } catch (err) {
      setLoadError(err.message);
    }
  };

  // Chart data derived from real history, oldest-first.
  const chartData = [...history].reverse().map((r) => ({
    time: fmtTime(r.timestamp),
    price: r.current_price,
    predicted: r.predicted_price,
  }));
  const latest = history[0] || null;
  const prevLatest = history[1] || null;
  const changePct = latest && prevLatest ? ((latest.current_price - prevLatest.current_price) / prevLatest.current_price) * 100 : 0;
  const isUp = changePct >= 0;

  return (
    <div style={{ minHeight: "100vh", background: C.bg, color: C.textPrimary, fontFamily: "Inter, sans-serif", display: "flex", flexDirection: "column" }}>
      <style>{`
        ${FONTS}
        @keyframes pulseDot { 0% { box-shadow: 0 0 0 0 rgba(51,214,159,0.55);} 70% { box-shadow: 0 0 0 7px rgba(51,214,159,0);} 100% { box-shadow: 0 0 0 0 rgba(51,214,159,0);} }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-thumb { background: ${C.borderLight}; border-radius: 3px; }
        input[type=number]::-webkit-inner-spin-button { opacity: 1; }
      `}</style>

      {/* Header */}
      <div style={{ borderBottom: `1px solid ${C.border}`, padding: "14px 20px", display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 12 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 18 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <Activity size={18} color={C.signal} />
            <span style={{ fontFamily: "'Space Grotesk', sans-serif", fontWeight: 600, fontSize: 16, letterSpacing: "-0.01em" }}>
              stream<span style={{ color: C.signal }}>predict</span>
            </span>
          </div>

          <div style={{ position: "relative" }}>
            <button
              onClick={() => setSymbolOpen((o) => !o)}
              style={{ display: "flex", alignItems: "center", gap: 6, background: C.panelAlt, border: `1px solid ${C.borderLight}`, borderRadius: 5, padding: "6px 10px", cursor: "pointer", color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace", fontSize: 12.5 }}
            >
              {sym.label} <ChevronDown size={13} color={C.textSecondary} />
            </button>
            {symbolOpen && (
              <div style={{ position: "absolute", top: "110%", left: 0, zIndex: 10, background: C.panelAlt, border: `1px solid ${C.borderLight}`, borderRadius: 5, minWidth: 130, overflow: "hidden" }}>
                {SYMBOLS.map((s, i) => (
                  <div
                    key={s.id}
                    onClick={() => { setSymIdx(i); setSymbolOpen(false); setLastPrediction(null); }}
                    style={{ padding: "8px 10px", cursor: "pointer", fontFamily: "'JetBrains Mono', monospace", fontSize: 12.5, color: i === symIdx ? C.signal : C.textSecondary, background: i === symIdx ? C.signalDim : "transparent" }}
                  >
                    {s.label}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
          <LivePulse connected={connected} />
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <GitBranch size={13} color={C.textSecondary} />
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: C.textSecondary }}>
              model {history[0]?.model_version || "—"}
            </span>
          </div>
        </div>
      </div>

      {loadError && (
        <div style={{ background: C.downDim, color: C.down, padding: "8px 20px", fontFamily: "'JetBrains Mono', monospace", fontSize: 12 }}>
          {loadError} — is the API running at {API_BASE_URL}?
        </div>
      )}

      {/* Body */}
      <div style={{ flex: 1, padding: 20, display: "grid", gridTemplateColumns: "1fr 300px", gap: 16 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 16, minWidth: 0 }}>
          <Panel>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 4 }}>
              <div>
                <Eyebrow>// LATEST STORED PREDICTION</Eyebrow>
                <div style={{ display: "flex", alignItems: "baseline", gap: 10 }}>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 30, fontWeight: 600, color: C.textPrimary }}>
                    {latest ? fmtPrice(latest.current_price) : "—"}
                  </span>
                  {latest && prevLatest && (
                    <span style={{ display: "flex", alignItems: "center", gap: 3, fontFamily: "'JetBrains Mono', monospace", fontSize: 13, color: isUp ? C.up : C.down }}>
                      {isUp ? <TrendingUp size={13} /> : <TrendingDown size={13} />}
                      {changePct.toFixed(2)}%
                    </span>
                  )}
                </div>
              </div>
              <div style={{ textAlign: "right" }}>
                <Eyebrow>PREDICTED</Eyebrow>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 20, color: C.signal }}>
                  {latest ? fmtPrice(latest.predicted_price) : "—"}
                </span>
              </div>
            </div>

            <div style={{ height: 280, marginTop: 8 }}>
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={chartData} margin={{ top: 10, right: 8, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id="bandFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={C.signal} stopOpacity={0.18} />
                      <stop offset="100%" stopColor={C.signal} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid stroke={C.border} vertical={false} />
                  <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 10, fontFamily: "JetBrains Mono, monospace" }} axisLine={{ stroke: C.border }} tickLine={false} />
                  <YAxis domain={["auto", "auto"]} tick={{ fill: C.textMuted, fontSize: 10, fontFamily: "JetBrains Mono, monospace" }} axisLine={false} tickLine={false} width={64} tickFormatter={fmtPrice} />
                  <Tooltip content={<CustomTooltip />} />
                  <Area type="monotone" dataKey="predicted" stroke="none" fill="url(#bandFill)" isAnimationActive={false} />
                  <Line type="monotone" dataKey="price" stroke={C.textPrimary} strokeWidth={1.75} dot={false} isAnimationActive={false} connectNulls />
                  <Line type="monotone" dataKey="predicted" stroke={C.signal} strokeWidth={1.75} strokeDasharray="4 3" dot={false} isAnimationActive={false} connectNulls />
                  {latest && <ReferenceLine y={latest.current_price} stroke={C.borderLight} strokeDasharray="2 2" />}
                </ComposedChart>
              </ResponsiveContainer>
            </div>
            <div style={{ display: "flex", gap: 16, marginTop: 6 }}>
              <LegendDot color={C.textPrimary} label="actual" />
              <LegendDot color={C.signal} dashed label="predicted" />
            </div>
          </Panel>

          <Panel>
            <Eyebrow>// GET A LIVE PREDICTION</Eyebrow>
            <form onSubmit={handleGetPrediction} style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: lastPrediction ? 12 : 0 }}>
              <input
                type="number" step="any" placeholder={`current ${sym.label} price`}
                value={formPrice} onChange={(e) => setFormPrice(e.target.value)}
                style={{ flex: 1, background: C.panelAlt, border: `1px solid ${C.borderLight}`, borderRadius: 5, padding: "8px 10px", color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace", fontSize: 13 }}
              />
              <button
                type="submit" disabled={formSubmitting}
                style={{ display: "flex", alignItems: "center", gap: 6, background: C.signalDim, border: `1px solid ${C.signal}`, borderRadius: 5, padding: "8px 14px", color: C.signal, fontFamily: "'JetBrains Mono', monospace", fontSize: 12.5, cursor: formSubmitting ? "wait" : "pointer" }}
              >
                <Send size={13} /> {formSubmitting ? "..." : "predict"}
              </button>
            </form>
            {lastPrediction && (
              <div style={{ display: "flex", gap: 20, fontFamily: "'JetBrains Mono', monospace", fontSize: 12.5 }}>
                <span style={{ color: C.textMuted }}>predicted: <span style={{ color: C.signal }}>{fmtPrice(lastPrediction.predicted_price)}</span></span>
                <span style={{ color: C.textMuted }}>return: <span style={{ color: C.textPrimary }}>{(lastPrediction.predicted_return * 100).toFixed(3)}%</span></span>
                <span style={{ color: C.textMuted }}>ci: <span style={{ color: C.textPrimary }}>[{fmtPrice(lastPrediction.confidence_interval?.[0])}, {fmtPrice(lastPrediction.confidence_interval?.[1])}]</span></span>
              </div>
            )}
          </Panel>

          <Panel>
            <Eyebrow>// PREDICTION FEED (real, from /predictions/{`{symbol}`})</Eyebrow>
            <div style={{ display: "grid", gridTemplateColumns: "70px 1fr 1fr 70px", gap: 8, fontSize: 10.5, color: C.textMuted, fontFamily: "'JetBrains Mono', monospace", paddingBottom: 6, borderBottom: `1px solid ${C.border}` }}>
              <span>TIME</span><span>ACTUAL</span><span>PREDICTED</span><span style={{ textAlign: "right" }}>ERR</span>
            </div>
            <div style={{ maxHeight: 190, overflowY: "auto" }}>
              {history.length === 0 && <div style={{ padding: "14px 0", color: C.textMuted, fontSize: 12 }}>no predictions stored yet for {sym.label}</div>}
              {history.map((r) => (
                <div key={r.id} style={{ display: "grid", gridTemplateColumns: "70px 1fr 1fr 70px", gap: 8, padding: "7px 0", fontFamily: "'JetBrains Mono', monospace", fontSize: 12, borderBottom: `1px solid ${C.border}` }}>
                  <span style={{ color: C.textMuted }}>{fmtTime(r.timestamp)}</span>
                  <span style={{ color: C.textPrimary }}>{fmtPrice(r.current_price)}</span>
                  <span style={{ color: C.signal }}>{fmtPrice(r.predicted_price)}</span>
                  <span style={{ textAlign: "right", color: r.realized_error == null ? C.textMuted : Math.abs(r.realized_error) < r.current_price * 0.002 ? C.up : C.warn }}>
                    {r.realized_error == null ? "…" : r.realized_error.toFixed(2)}
                  </span>
                </div>
              ))}
            </div>
          </Panel>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          <Panel>
            <Eyebrow>// MODEL HEALTH (real, /metrics/accuracy)</Eyebrow>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <MetricBlock label="ROLLING RMSE" value={accuracy.rolling_rmse != null ? accuracy.rolling_rmse.toFixed(3) : "—"} />
              <MetricBlock label="ROLLING MAE" value={accuracy.rolling_mae != null ? accuracy.rolling_mae.toFixed(3) : "—"} />
            </div>
          </Panel>

          <Panel style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
            <Eyebrow>// DRIFT CHECK</Eyebrow>
            <DriftGauge featureDrift={drift.featureDrift} conceptDrift={drift.conceptDrift} checked={drift.checked} />
            <button
              onClick={handleCheckDrift}
              style={{ marginTop: 10, background: "transparent", border: `1px solid ${C.borderLight}`, borderRadius: 5, padding: "6px 12px", color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: 11.5, cursor: "pointer" }}
            >
              run check now
            </button>
          </Panel>

          <Panel>
            <Eyebrow>// API STATUS</Eyebrow>
            <StatusRow icon={connected ? Wifi : WifiOff} label="Prediction API" value={connected ? "reachable" : "unreachable"} status={connected ? "ok" : "err"} />
            <StatusRow icon={Database} label="Stored predictions" value={history.length} status="ok" />
            <StatusRow icon={Cpu} label="Model version" value={history[0]?.model_version || "—"} status="ok" />
            <StatusRow icon={Zap} label="Poll interval" value="5s" status="ok" />
          </Panel>
        </div>
      </div>
    </div>
  );
}
