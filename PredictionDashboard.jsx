import React, { useState, useEffect, useRef, useCallback } from "react";
import {
  ComposedChart, Line, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from "recharts";
import {
  Activity, TrendingUp, TrendingDown, Wifi, Database, Cpu, Zap,
  GitBranch, AlertTriangle, CheckCircle2, ChevronDown,
} from "lucide-react";

// ---------------------------------------------------------------------------
// Token system
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
  { id: "BTCUSDT", label: "BTC/USDT", base: 118420, vol: 0.0009 },
  { id: "ETHUSDT", label: "ETH/USDT", base: 4260, vol: 0.0014 },
  { id: "AAPL", label: "AAPL", base: 231.5, vol: 0.0004 },
];

const HISTORY_LEN = 48;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmtPrice(p, symbol) {
  if (symbol === "AAPL") return p.toFixed(2);
  if (p > 1000) return p.toFixed(2);
  return p.toFixed(2);
}
function fmtTime(d) {
  return d.toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}
function seedSeries(base) {
  const now = Date.now();
  const arr = [];
  let price = base;
  for (let i = HISTORY_LEN; i >= 0; i--) {
    price = price * (1 + (Math.random() - 0.5) * 0.0015);
    const predicted = price * (1 + (Math.random() - 0.5) * 0.0009);
    arr.push({
      t: now - i * 1800,
      time: fmtTime(new Date(now - i * 1800)),
      price,
      predicted: i === 0 ? null : predicted,
      band: predicted * 0.0015,
    });
  }
  return arr;
}

// ---------------------------------------------------------------------------
// Small building blocks
// ---------------------------------------------------------------------------
function Eyebrow({ children }) {
  return (
    <div
      style={{
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: 11,
        letterSpacing: "0.08em",
        color: C.textMuted,
        marginBottom: 8,
      }}
    >
      {children}
    </div>
  );
}

function Panel({ children, style }) {
  return (
    <div
      style={{
        background: C.panel,
        border: `1px solid ${C.border}`,
        borderRadius: 6,
        padding: 16,
        ...style,
      }}
    >
      {children}
    </div>
  );
}

function LivePulse() {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <span
        style={{
          width: 6,
          height: 6,
          borderRadius: "50%",
          background: C.up,
          boxShadow: `0 0 0 0 rgba(51,214,159,0.6)`,
          animation: "pulseDot 1.6s ease-out infinite",
        }}
      />
      <span
        style={{
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 11,
          letterSpacing: "0.08em",
          color: C.up,
        }}
      >
        LIVE
      </span>
    </div>
  );
}

function StatusRow({ icon: Icon, label, value, status = "ok" }) {
  const color = status === "ok" ? C.up : status === "warn" ? C.warn : C.down;
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

function DriftGauge({ score }) {
  // score 0-1, arc gauge
  const angle = -90 + score * 180;
  const color = score < 0.4 ? C.up : score < 0.7 ? C.warn : C.down;
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
      <svg width="120" height="70" viewBox="0 0 120 70">
        <path d="M 10 65 A 50 50 0 0 1 110 65" fill="none" stroke={C.borderLight} strokeWidth="8" strokeLinecap="round" />
        <path
          d="M 10 65 A 50 50 0 0 1 110 65"
          fill="none"
          stroke={color}
          strokeWidth="8"
          strokeLinecap="round"
          strokeDasharray={`${score * 157} 157`}
        />
        <line
          x1="60" y1="65"
          x2={60 + 38 * Math.cos((angle * Math.PI) / 180)}
          y2={65 + 38 * Math.sin((angle * Math.PI) / 180)}
          stroke={C.textPrimary}
          strokeWidth="2"
        />
        <circle cx="60" cy="65" r="3.5" fill={C.textPrimary} />
      </svg>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 20, fontWeight: 600, color, marginTop: -8 }}>
        {(score * 100).toFixed(0)}
      </div>
      <div style={{ fontFamily: "Inter, sans-serif", fontSize: 10.5, color: C.textMuted, letterSpacing: "0.04em" }}>
        DRIFT SCORE
      </div>
    </div>
  );
}

function CustomTooltip({ active, payload, symbol }) {
  if (!active || !payload || !payload.length) return null;
  const row = payload[0].payload;
  return (
    <div
      style={{
        background: C.panelAlt,
        border: `1px solid ${C.borderLight}`,
        borderRadius: 4,
        padding: "8px 10px",
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: 11,
      }}
    >
      <div style={{ color: C.textMuted, marginBottom: 4 }}>{row.time}</div>
      <div style={{ color: C.textPrimary }}>actual&nbsp;&nbsp;{fmtPrice(row.price, symbol)}</div>
      {row.predicted != null && (
        <div style={{ color: C.signal }}>pred&nbsp;&nbsp;&nbsp;&nbsp;{fmtPrice(row.predicted, symbol)}</div>
      )}
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

  const [series, setSeries] = useState(() => seedSeries(sym.base));
  const [lastDir, setLastDir] = useState(0); // -1,0,1 for flash
  const [feed, setFeed] = useState([]);
  const [metrics, setMetrics] = useState({
    version: "v9",
    rmse: 42.3,
    mae: 31.1,
    dirAcc: 61.4,
    drift: 0.18,
  });
  const [infra, setInfra] = useState({
    kafkaLag: 120,
    sparkLatency: 340,
    apiLatency: 58,
    throughput: 1240,
  });

  const priceRef = useRef(sym.base);

  // reset simulation when symbol changes
  useEffect(() => {
    priceRef.current = sym.base;
    setSeries(seedSeries(sym.base));
    setFeed([]);
  }, [symIdx]); // eslint-disable-line

  const tick = useCallback(() => {
    setSeries((prev) => {
      const last = prev[prev.length - 1];
      const drift = (Math.random() - 0.5) * sym.vol * 2;
      const newPrice = priceRef.current * (1 + drift);
      priceRef.current = newPrice;
      const momentum = newPrice - last.price;
      const predicted = newPrice + momentum * (0.6 + Math.random() * 0.5);
      const now = Date.now();
      const point = {
        t: now,
        time: fmtTime(new Date(now)),
        price: newPrice,
        predicted,
        band: predicted * (0.0008 + Math.random() * 0.0006),
      };
      setLastDir(newPrice >= last.price ? 1 : -1);

      setFeed((f) => {
        const error = Math.abs(predicted - newPrice);
        const errPct = (error / newPrice) * 100;
        const row = {
          id: now,
          time: point.time,
          actual: newPrice,
          predicted,
          errPct,
        };
        return [row, ...f].slice(0, 8);
      });

      const next = [...prev.slice(1), point];
      return next;
    });

    setMetrics((m) => ({
      ...m,
      rmse: Math.max(8, m.rmse + (Math.random() - 0.5) * 3),
      mae: Math.max(6, m.mae + (Math.random() - 0.5) * 2.2),
      dirAcc: Math.min(78, Math.max(48, m.dirAcc + (Math.random() - 0.5) * 1.5)),
      drift: Math.min(0.95, Math.max(0.05, m.drift + (Math.random() - 0.5) * 0.04)),
    }));
    setInfra((i) => ({
      kafkaLag: Math.max(20, Math.round(i.kafkaLag + (Math.random() - 0.5) * 40)),
      sparkLatency: Math.max(80, Math.round(i.sparkLatency + (Math.random() - 0.5) * 60)),
      apiLatency: Math.max(20, Math.round(i.apiLatency + (Math.random() - 0.5) * 15)),
      throughput: Math.max(300, Math.round(i.throughput + (Math.random() - 0.5) * 150)),
    }));
  }, [sym.vol]);

  useEffect(() => {
    const id = setInterval(tick, 1500);
    return () => clearInterval(id);
  }, [tick]);

  const current = series[series.length - 1];
  const prevClose = series[0];
  const changePct = ((current.price - prevClose.price) / prevClose.price) * 100;
  const isUp = changePct >= 0;

  const flashBg =
    lastDir === 1 ? "rgba(51,214,159,0.06)" : lastDir === -1 ? "rgba(255,92,108,0.06)" : "transparent";

  const tickerItems = [
    { label: "KAFKA LAG", value: `${infra.kafkaLag}ms`, ok: infra.kafkaLag < 300 },
    { label: "SPARK LATENCY", value: `${infra.sparkLatency}ms`, ok: infra.sparkLatency < 500 },
    { label: "API P99", value: `${infra.apiLatency}ms`, ok: infra.apiLatency < 150 },
    { label: "THROUGHPUT", value: `${infra.throughput}/s`, ok: true },
    { label: "MODEL", value: metrics.version, ok: true },
    { label: "DRIFT", value: `${(metrics.drift * 100).toFixed(0)}%`, ok: metrics.drift < 0.5 },
  ];

  return (
    <div
      style={{
        minHeight: "100vh",
        background: C.bg,
        color: C.textPrimary,
        fontFamily: "Inter, sans-serif",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <style>{`
        ${FONTS}
        @keyframes pulseDot {
          0% { box-shadow: 0 0 0 0 rgba(51,214,159,0.55); }
          70% { box-shadow: 0 0 0 7px rgba(51,214,159,0); }
          100% { box-shadow: 0 0 0 0 rgba(51,214,159,0); }
        }
        @keyframes marquee {
          0% { transform: translateX(0); }
          100% { transform: translateX(-50%); }
        }
        .marquee-track { animation: marquee 22s linear infinite; }
        .feed-row { animation: fadeIn 0.4s ease; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(-4px);} to { opacity:1; transform: translateY(0);} }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-thumb { background: ${C.borderLight}; border-radius: 3px; }
      `}</style>

      {/* Header */}
      <div
        style={{
          borderBottom: `1px solid ${C.border}`,
          padding: "14px 20px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          flexWrap: "wrap",
          gap: 12,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 18 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <Activity size={18} color={C.signal} />
            <span style={{ fontFamily: "'Space Grotesk', sans-serif", fontWeight: 600, fontSize: 16, letterSpacing: "-0.01em" }}>
              stream<span style={{ color: C.signal }}>predict</span>
            </span>
          </div>

          {/* symbol selector */}
          <div style={{ position: "relative" }}>
            <button
              onClick={() => setSymbolOpen((o) => !o)}
              style={{
                display: "flex", alignItems: "center", gap: 6,
                background: C.panelAlt, border: `1px solid ${C.borderLight}`,
                borderRadius: 5, padding: "6px 10px", cursor: "pointer", color: C.textPrimary,
                fontFamily: "'JetBrains Mono', monospace", fontSize: 12.5,
              }}
            >
              {sym.label} <ChevronDown size={13} color={C.textSecondary} />
            </button>
            {symbolOpen && (
              <div
                style={{
                  position: "absolute", top: "110%", left: 0, zIndex: 10,
                  background: C.panelAlt, border: `1px solid ${C.borderLight}`, borderRadius: 5,
                  minWidth: 130, overflow: "hidden",
                }}
              >
                {SYMBOLS.map((s, i) => (
                  <div
                    key={s.id}
                    onClick={() => { setSymIdx(i); setSymbolOpen(false); }}
                    style={{
                      padding: "8px 10px", cursor: "pointer",
                      fontFamily: "'JetBrains Mono', monospace", fontSize: 12.5,
                      color: i === symIdx ? C.signal : C.textSecondary,
                      background: i === symIdx ? C.signalDim : "transparent",
                    }}
                    onMouseEnter={(e) => (e.currentTarget.style.background = C.border)}
                    onMouseLeave={(e) => (e.currentTarget.style.background = i === symIdx ? C.signalDim : "transparent")}
                  >
                    {s.label}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
          <LivePulse />
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <GitBranch size={13} color={C.textSecondary} />
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: C.textSecondary }}>
              model {metrics.version} · production
            </span>
          </div>
        </div>
      </div>

      {/* Body */}
      <div style={{ flex: 1, padding: 20, display: "grid", gridTemplateColumns: "1fr 300px", gap: 16 }}>
        {/* Left: chart + feed */}
        <div style={{ display: "flex", flexDirection: "column", gap: 16, minWidth: 0 }}>
          <Panel style={{ transition: "background 0.4s ease", background: `${C.panel}` }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 4 }}>
              <div>
                <Eyebrow>// LIVE PRICE VS PREDICTION</Eyebrow>
                <div style={{ display: "flex", alignItems: "baseline", gap: 10 }}>
                  <span
                    style={{
                      fontFamily: "'JetBrains Mono', monospace",
                      fontSize: 30, fontWeight: 600, transition: "color 0.3s",
                      color: lastDir === 1 ? C.up : lastDir === -1 ? C.down : C.textPrimary,
                    }}
                  >
                    {fmtPrice(current.price, sym.id)}
                  </span>
                  <span
                    style={{
                      display: "flex", alignItems: "center", gap: 3,
                      fontFamily: "'JetBrains Mono', monospace", fontSize: 13,
                      color: isUp ? C.up : C.down,
                    }}
                  >
                    {isUp ? <TrendingUp size={13} /> : <TrendingDown size={13} />}
                    {changePct.toFixed(2)}%
                  </span>
                </div>
              </div>
              <div style={{ textAlign: "right" }}>
                <Eyebrow>NEXT-TICK PREDICTION</Eyebrow>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 20, color: C.signal }}>
                  {current.predicted != null ? fmtPrice(current.predicted, sym.id) : "—"}
                </span>
              </div>
            </div>

            <div style={{ height: 300, marginTop: 8 }}>
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={series} margin={{ top: 10, right: 8, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id="bandFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={C.signal} stopOpacity={0.18} />
                      <stop offset="100%" stopColor={C.signal} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid stroke={C.border} vertical={false} />
                  <XAxis
                    dataKey="time"
                    tick={{ fill: C.textMuted, fontSize: 10, fontFamily: "JetBrains Mono, monospace" }}
                    interval={Math.floor(HISTORY_LEN / 6)}
                    axisLine={{ stroke: C.border }}
                    tickLine={false}
                  />
                  <YAxis
                    domain={["auto", "auto"]}
                    tick={{ fill: C.textMuted, fontSize: 10, fontFamily: "JetBrains Mono, monospace" }}
                    axisLine={false}
                    tickLine={false}
                    width={64}
                    tickFormatter={(v) => fmtPrice(v, sym.id)}
                  />
                  <Tooltip content={<CustomTooltip symbol={sym.id} />} />
                  <Area type="monotone" dataKey="predicted" stroke="none" fill="url(#bandFill)" isAnimationActive={false} />
                  <Line type="monotone" dataKey="price" stroke={C.textPrimary} strokeWidth={1.75} dot={false} isAnimationActive={false} />
                  <Line
                    type="monotone" dataKey="predicted" stroke={C.signal} strokeWidth={1.75}
                    strokeDasharray="4 3" dot={false} isAnimationActive={false}
                  />
                  <ReferenceLine y={current.price} stroke={C.borderLight} strokeDasharray="2 2" />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
            <div style={{ display: "flex", gap: 16, marginTop: 6 }}>
              <LegendDot color={C.textPrimary} label="actual" />
              <LegendDot color={C.signal} dashed label="predicted" />
            </div>
          </Panel>

          <Panel>
            <Eyebrow>// PREDICTION FEED</Eyebrow>
            <div style={{ display: "grid", gridTemplateColumns: "70px 1fr 1fr 70px", gap: 8, fontSize: 10.5, color: C.textMuted, fontFamily: "'JetBrains Mono', monospace", paddingBottom: 6, borderBottom: `1px solid ${C.border}` }}>
              <span>TIME</span><span>ACTUAL</span><span>PREDICTED</span><span style={{ textAlign: "right" }}>ERR%</span>
            </div>
            <div style={{ maxHeight: 190, overflowY: "auto" }}>
              {feed.length === 0 && (
                <div style={{ padding: "14px 0", color: C.textMuted, fontSize: 12 }}>waiting for first tick…</div>
              )}
              {feed.map((r) => (
                <div
                  key={r.id}
                  className="feed-row"
                  style={{
                    display: "grid", gridTemplateColumns: "70px 1fr 1fr 70px", gap: 8,
                    padding: "7px 0", fontFamily: "'JetBrains Mono', monospace", fontSize: 12,
                    borderBottom: `1px solid ${C.border}`,
                  }}
                >
                  <span style={{ color: C.textMuted }}>{r.time}</span>
                  <span style={{ color: C.textPrimary }}>{fmtPrice(r.actual, sym.id)}</span>
                  <span style={{ color: C.signal }}>{fmtPrice(r.predicted, sym.id)}</span>
                  <span style={{ textAlign: "right", color: r.errPct < 0.15 ? C.up : r.errPct < 0.4 ? C.warn : C.down }}>
                    {r.errPct.toFixed(2)}
                  </span>
                </div>
              ))}
            </div>
          </Panel>
        </div>

        {/* Right: model health */}
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          <Panel>
            <Eyebrow>// MODEL HEALTH</Eyebrow>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 6 }}>
              <MetricBlock label="RMSE" value={metrics.rmse.toFixed(1)} />
              <MetricBlock label="MAE" value={metrics.mae.toFixed(1)} />
              <MetricBlock label="DIR. ACCURACY" value={`${metrics.dirAcc.toFixed(1)}%`} good={metrics.dirAcc > 55} />
              <MetricBlock label="BASELINE EDGE" value={`+${(metrics.dirAcc - 50).toFixed(1)}pp`} good={metrics.dirAcc > 50} />
            </div>
          </Panel>

          <Panel style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
            <Eyebrow>// DATA DRIFT</Eyebrow>
            <DriftGauge score={metrics.drift} />
            {metrics.drift > 0.7 && (
              <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 6, color: C.warn, fontSize: 11.5, fontFamily: "'JetBrains Mono', monospace" }}>
                <AlertTriangle size={12} /> retrain likely to trigger
              </div>
            )}
          </Panel>

          <Panel>
            <Eyebrow>// SYSTEM STATUS</Eyebrow>
            <StatusRow icon={Wifi} label="Market feed" value="connected" status="ok" />
            <StatusRow icon={Database} label="Kafka" value={`${infra.kafkaLag}ms lag`} status={infra.kafkaLag < 300 ? "ok" : "warn"} />
            <StatusRow icon={Cpu} label="Spark stream" value={`${infra.sparkLatency}ms`} status={infra.sparkLatency < 500 ? "ok" : "warn"} />
            <StatusRow icon={Zap} label="Prediction API" value={`${infra.apiLatency}ms`} status={infra.apiLatency < 150 ? "ok" : "warn"} />
            <StatusRow icon={CheckCircle2} label="Registry" value="in sync" status="ok" />
          </Panel>
        </div>
      </div>

      {/* Ticker tape footer */}
      <div
        style={{
          borderTop: `1px solid ${C.border}`,
          background: C.panel,
          overflow: "hidden",
          padding: "9px 0",
          whiteSpace: "nowrap",
        }}
      >
        <div className="marquee-track" style={{ display: "inline-block" }}>
          {[...tickerItems, ...tickerItems, ...tickerItems].map((item, i) => (
            <span key={i} style={{ display: "inline-flex", alignItems: "center", marginRight: 32 }}>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10.5, color: C.textMuted, marginRight: 6 }}>
                {item.label}
              </span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11.5, color: item.ok ? C.textPrimary : C.warn, fontWeight: 500 }}>
                {item.value}
              </span>
              <span style={{ margin: "0 16px", color: C.borderLight }}>·</span>
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}

function LegendDot({ color, label, dashed }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <span
        style={{
          width: 14, height: dashed ? 0 : 2, borderTop: dashed ? `1.75px dashed ${color}` : `2px solid ${color}`,
        }}
      />
      <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10.5, color: C.textMuted }}>{label}</span>
    </div>
  );
}

function MetricBlock({ label, value, good }) {
  return (
    <div style={{ background: C.panelAlt, border: `1px solid ${C.border}`, borderRadius: 5, padding: "9px 10px" }}>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: C.textMuted, marginBottom: 3 }}>{label}</div>
      <div
        style={{
          fontFamily: "'JetBrains Mono', monospace", fontSize: 17, fontWeight: 600,
          color: good === undefined ? C.textPrimary : good ? C.up : C.down,
        }}
      >
        {value}
      </div>
    </div>
  );
}
