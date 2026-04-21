// Store page — reuses A_TOKENS from direction-a.jsx (loaded first)
// Dense, functional, with sidebar ad + 30d chart.

const S = A_TOKENS;

// build 30d history for adidas
const HISTORY = [
  { d: "Mar 20", rate: 3.00, portal: "Rakuten.ca", label: "4%", best: "4%" },
  { d: "Mar 21", rate: 3.00, portal: "Rakuten.ca", label: "4%" },
  { d: "Mar 22", rate: 4.50, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Mar 23", rate: 4.50, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Mar 24", rate: 6.00, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Mar 25", rate: 6.00, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Mar 26", rate: 6.00, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Mar 27", rate: 4.50, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Mar 28", rate: 4.50, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Mar 29", rate: 4.50, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Mar 30", rate: 5.25, portal: "Swagbucks", label: "Up to 7%" },
  { d: "Mar 31", rate: 5.25, portal: "Swagbucks", label: "Up to 7%" },
  { d: "Apr 1",  rate: 6.00, portal: "Swagbucks", label: "Up to 8%" },
  { d: "Apr 2",  rate: 6.00, portal: "Swagbucks", label: "Up to 8%" },
  { d: "Apr 3",  rate: 7.50, portal: "Aeroplan eStore", label: "Earn 5 pts/$" },
  { d: "Apr 4",  rate: 7.50, portal: "Aeroplan eStore", label: "Earn 5 pts/$" },
  { d: "Apr 5",  rate: 6.00, portal: "Swagbucks", label: "Up to 8%" },
  { d: "Apr 6",  rate: 6.00, portal: "Swagbucks", label: "Up to 8%" },
  { d: "Apr 7",  rate: 6.00, portal: "Swagbucks", label: "Up to 8%" },
  { d: "Apr 8",  rate: 6.75, portal: "Swagbucks", label: "Up to 9%" },
  { d: "Apr 9",  rate: 6.75, portal: "Swagbucks", label: "Up to 9%" },
  { d: "Apr 10", rate: 6.75, portal: "Swagbucks", label: "Up to 9%" },
  { d: "Apr 11", rate: 6.75, portal: "Swagbucks", label: "Up to 9%" },
  { d: "Apr 12", rate: 4.50, portal: "Swagbucks", label: "Up to 6%" },
  { d: "Apr 13", rate: 6.75, portal: "Swagbucks", label: "Up to 9%" },
  { d: "Apr 14", rate: 7.50, portal: "Aeroplan eStore", label: "Earn 5 pts/$" },
  { d: "Apr 15", rate: 7.50, portal: "Aeroplan eStore", label: "Earn 5 pts/$" },
  { d: "Apr 16", rate: 7.50, portal: "Aeroplan eStore", label: "Earn 5 pts/$" },
  { d: "Apr 17", rate: 7.50, portal: "Aeroplan eStore", label: "Earn 5 pts/$" },
  { d: "Apr 18", rate: 7.50, portal: "Aeroplan eStore", label: "Earn 5 pts/$" },
];

const PORTAL_COLORS = { "Rakuten.ca": "#BF0000", "Great Canadian Rebates": "#006B3C", "Swagbucks": "#0A7AA6", "Aeroplan eStore": "#D3273E", "TopCashback": "#E8344C", "Air Miles Shops": "#0066A4", "Drop": "#6B4FBB" };

const PORTALS = [
  { name: "Aeroplan eStore", short: "AE", rate: "5 pts/$", effective: 7.50, type: "Points", updated: "3h ago", link: "aeroplan.rewardops.com", best: true },
  { name: "Swagbucks", short: "SB", rate: "Up to 6%", effective: 6.00, type: "Cashback", updated: "3h ago", link: "swagbucks.com" },
  { name: "TopCashback", short: "TCB", rate: "4%", effective: 4.00, type: "Cashback", updated: "3h ago", link: "topcashback.com" },
  { name: "Rakuten.ca", short: "RK", rate: "4%", effective: 4.00, type: "Cashback", updated: "3h ago", link: "rakuten.ca" },
  { name: "Great Canadian Rebates", short: "GCR", rate: "2%", effective: 2.00, type: "Cashback", updated: "3h ago", link: "greatcanadianrebates.ca" },
  { name: "Air Miles Shops", short: "AM", rate: "0.05 pts/$", effective: 0.53, type: "Points", updated: "3h ago", link: "airmilesshops.ca" },
];

function SPortalBadge({ name, size = 32 }) {
  const color = PORTAL_COLORS[name] || "#888";
  const short = { "Rakuten.ca": "RK", "Great Canadian Rebates": "GCR", "Swagbucks": "SB", "Aeroplan eStore": "AE", "TopCashback": "TCB", "Air Miles Shops": "AM", "Drop": "DR" }[name] || name.slice(0, 2);
  return (
    <div style={{
      width: size, height: size, borderRadius: 7, background: color,
      display: "flex", alignItems: "center", justifyContent: "center",
      color: "white", fontFamily: S.sans, fontWeight: 700, fontSize: size * 0.35, letterSpacing: 0.3,
      flexShrink: 0,
    }}>{short}</div>
  );
}

// store logo (big, for hero)
function SStoreLogo({ name, size = 64 }) {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) % 360;
  const bg = `oklch(0.94 0.04 ${h})`;
  const fg = `oklch(0.30 0.08 ${h})`;
  return (
    <div style={{
      width: size, height: size, borderRadius: 14,
      background: bg, color: fg,
      display: "flex", alignItems: "center", justifyContent: "center",
      fontFamily: S.sans, fontWeight: 700, fontSize: size * 0.4,
      letterSpacing: -0.5, flexShrink: 0,
    }}>{name[0]}</div>
  );
}

function SAdSlot({ w, h, size }) {
  return (
    <div style={{
      width: w, height: h,
      border: `1px dashed ${S.line}`, background: "oklch(0.97 0.006 85)",
      borderRadius: 10, display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center", gap: 6,
      fontFamily: S.sans,
    }}>
      <div style={{ fontSize: 10, color: S.inkMute, letterSpacing: 1.2, textTransform: "uppercase" }}>Advertisement</div>
      <div style={{ fontSize: 13, color: S.inkSoft, fontWeight: 500 }}>{size}</div>
    </div>
  );
}

// ─── nav (same as home) ─────
function SNav() {
  return (
    <div style={{
      height: 64, borderBottom: `1px solid ${S.line}`,
      display: "flex", alignItems: "center", padding: "0 40px",
      background: S.surface, position: "sticky", top: 0, zIndex: 10,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ width: 28, height: 28, borderRadius: 7, background: S.ink, display: "flex", alignItems: "center", justifyContent: "center", color: S.bg, fontFamily: S.serif, fontSize: 18, fontStyle: "italic" }}>m</div>
        <div style={{ fontFamily: S.sans, fontWeight: 600, fontSize: 15, letterSpacing: -0.3, color: S.ink }}>MaxCashBack</div>
        <div style={{ fontSize: 10, fontFamily: S.sans, color: S.inkMute, border: `1px solid ${S.line}`, borderRadius: 4, padding: "2px 6px", marginLeft: 6 }}>CA</div>
      </div>
      <div style={{ flex: 1, marginLeft: 32, maxWidth: 520 }}>
        <div style={{
          display: "flex", alignItems: "center", gap: 10,
          background: S.bg, border: `1px solid ${S.line}`, borderRadius: 10,
          padding: "0 14px", height: 38,
        }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={S.inkMute} strokeWidth="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
          <div style={{ fontFamily: S.sans, fontSize: 13, color: S.inkMute, flex: 1 }}>Search 2,081 stores…</div>
          <div style={{ fontFamily: S.sans, fontSize: 10, color: S.inkMute, border: `1px solid ${S.line}`, borderRadius: 3, padding: "1px 5px", background: S.surface }}>⌘K</div>
        </div>
      </div>
      <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 14 }}>
        <div style={{ fontFamily: S.sans, fontSize: 13, color: S.inkSoft }}>Sign in</div>
        <div style={{ fontFamily: S.sans, fontSize: 13, fontWeight: 500, color: S.bg, background: S.ink, padding: "8px 14px", borderRadius: 8 }}>Get alerts</div>
      </div>
    </div>
  );
}

// ─── breadcrumb + store header ─────
function SHeader() {
  return (
    <div style={{ background: S.surface, padding: "20px 40px 28px", borderBottom: `1px solid ${S.line}` }}>
      <div style={{ fontFamily: S.sans, fontSize: 12, color: S.inkMute, display: "flex", alignItems: "center", gap: 8, marginBottom: 20 }}>
        <span style={{ color: S.inkSoft }}>Home</span>
        <span>/</span>
        <span style={{ color: S.inkSoft }}>Apparel</span>
        <span>/</span>
        <span style={{ color: S.ink }}>adidas Canada</span>
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
        <SStoreLogo name="adidas Canada" size={72} />
        <div style={{ flex: 1 }}>
          <div style={{ fontFamily: S.serif, fontSize: 44, color: S.ink, letterSpacing: -1.2, lineHeight: 1 }}>adidas Canada</div>
          <div style={{ display: "flex", alignItems: "center", gap: 14, marginTop: 10, fontFamily: S.sans, fontSize: 13, color: S.inkSoft }}>
            <a style={{ color: S.accentInk, borderBottom: `1px solid ${S.accent}`, paddingBottom: 1 }}>adidas.ca ↗</a>
            <span style={{ color: S.inkMute }}>·</span>
            <span>Apparel · Footwear</span>
            <span style={{ color: S.inkMute }}>·</span>
            <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
              <span style={{ width: 6, height: 6, borderRadius: 99, background: S.pos }} />
              6 portals tracked
            </span>
            <span style={{ color: S.inkMute }}>·</span>
            <span>Updated 3h ago</span>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <div style={{ fontFamily: S.sans, fontSize: 13, color: S.ink, padding: "9px 14px", border: `1px solid ${S.line}`, borderRadius: 8, background: S.surface, display: "flex", alignItems: "center", gap: 7 }}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 2v13m-5-5 5 5 5-5M5 22h14"/></svg>
            Save
          </div>
          <div style={{ fontFamily: S.sans, fontSize: 13, color: S.ink, padding: "9px 14px", border: `1px solid ${S.line}`, borderRadius: 8, background: S.surface, display: "flex", alignItems: "center", gap: 7 }}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M15 17h5l-1.4-1.4A7 7 0 0 0 19 11V8a7 7 0 0 0-14 0v3a7 7 0 0 0 .4 4.6L4 17h5m6 0a3 3 0 1 1-6 0"/></svg>
            Alert me
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── best rate hero card ─────
function SBestRate() {
  const best = PORTALS[0];
  return (
    <div style={{
      margin: "28px 40px 0", padding: 28,
      background: `linear-gradient(135deg, ${S.accentSoft} 0%, oklch(0.96 0.02 185) 100%)`,
      border: `1px solid ${S.accent}`, borderRadius: 16,
      display: "flex", alignItems: "center", gap: 28,
    }}>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontFamily: S.sans, fontSize: 11, color: S.accentInk, letterSpacing: 1.2, textTransform: "uppercase", marginBottom: 10 }}>
          ★ Best rate today
        </div>
        <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 8, whiteSpace: "nowrap" }}>
          <div style={{ fontFamily: S.serif, fontSize: 72, color: S.ink, letterSpacing: -2, lineHeight: 1, fontVariantNumeric: "tabular-nums" }}>5</div>
          <div style={{ fontFamily: S.sans, fontSize: 22, color: S.inkSoft, fontWeight: 500 }}>pts/$</div>
        </div>
        <div style={{ fontFamily: S.sans, fontSize: 13, color: S.inkSoft, marginBottom: 14 }}>
          ≈ <span style={{ color: S.ink, fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>7.50%</span> effective cash
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, whiteSpace: "nowrap" }}>
            <SPortalBadge name={best.name} size={26} />
            <span style={{ fontFamily: S.sans, fontSize: 14, color: S.ink, fontWeight: 500 }}>via {best.name}</span>
          </div>
          <span style={{ fontFamily: S.sans, fontSize: 12, color: S.inkMute, whiteSpace: "nowrap" }}>· updated {best.updated}</span>
        </div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 8, alignItems: "stretch", flexShrink: 0 }}>
        <div style={{
          fontFamily: S.sans, fontSize: 14, fontWeight: 600, color: S.bg,
          background: S.ink, padding: "14px 28px", borderRadius: 10,
          display: "flex", alignItems: "center", gap: 10, whiteSpace: "nowrap",
        }}>
          Shop via Aeroplan eStore
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M7 17 17 7M7 7h10v10"/></svg>
        </div>
        <div style={{ fontFamily: S.sans, fontSize: 12, color: S.inkMute, textAlign: "center" }}>Opens aeroplan.rewardops.com</div>
      </div>
    </div>
  );
}

// ─── portals comparison table (main content, with sidebar) ─────
function SCompareTable() {
  return (
    <div style={{ padding: "40px 40px 0", display: "grid", gridTemplateColumns: "1fr 300px", gap: 32 }}>
      {/* Main — table */}
      <div>
        <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 18 }}>
          <div>
            <div style={{ fontFamily: S.serif, fontSize: 28, color: S.ink, letterSpacing: -0.6 }}>All portals</div>
            <div style={{ fontFamily: S.sans, fontSize: 13, color: S.inkSoft, marginTop: 4 }}>Sorted by effective cash rate. Verify before purchase.</div>
          </div>
          <div style={{ display: "flex", gap: 6 }}>
            <div style={{ fontFamily: S.sans, fontSize: 12, color: S.ink, padding: "6px 12px", border: `1px solid ${S.ink}`, borderRadius: 7, fontWeight: 500 }}>Effective</div>
            <div style={{ fontFamily: S.sans, fontSize: 12, color: S.inkSoft, padding: "6px 12px", border: `1px solid ${S.line}`, borderRadius: 7 }}>Raw rate</div>
            <div style={{ fontFamily: S.sans, fontSize: 12, color: S.inkSoft, padding: "6px 12px", border: `1px solid ${S.line}`, borderRadius: 7 }}>Updated</div>
          </div>
        </div>

        <div style={{ border: `1px solid ${S.line}`, borderRadius: 12, overflow: "hidden", background: S.surface }}>
          <div style={{
            display: "grid",
            gridTemplateColumns: "1.4fr 1fr 1fr 120px 120px 110px",
            fontFamily: S.sans, fontSize: 10, color: S.inkMute,
            letterSpacing: 0.8, textTransform: "uppercase",
            padding: "14px 18px", background: S.bg, borderBottom: `1px solid ${S.line}`,
          }}>
            <div>Portal</div>
            <div>Offer</div>
            <div>Type</div>
            <div style={{ textAlign: "right" }}>Effective</div>
            <div style={{ textAlign: "right" }}>Updated</div>
            <div style={{ textAlign: "right" }}></div>
          </div>
          {PORTALS.map((p, i) => (
            <div key={p.name} style={{
              display: "grid",
              gridTemplateColumns: "1.4fr 1fr 1fr 120px 120px 110px",
              alignItems: "center",
              padding: "16px 18px",
              borderBottom: i === PORTALS.length - 1 ? "none" : `1px solid ${S.lineSoft}`,
              background: p.best ? S.accentSoft : S.surface,
              fontFamily: S.sans,
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                <SPortalBadge name={p.name} />
                <div>
                  <div style={{ fontSize: 14, color: S.ink, fontWeight: 500, display: "flex", alignItems: "center", gap: 8 }}>
                    {p.name}
                    {p.best && <span style={{ fontSize: 10, color: S.accentInk, background: S.surface, border: `1px solid ${S.accent}`, borderRadius: 4, padding: "1px 5px", letterSpacing: 0.5, textTransform: "uppercase", fontWeight: 600 }}>Best</span>}
                  </div>
                  <div style={{ fontSize: 11, color: S.inkMute, marginTop: 2 }}>{p.link}</div>
                </div>
              </div>
              <div style={{ fontFamily: S.serif, fontSize: 18, color: S.ink, letterSpacing: -0.3, fontVariantNumeric: "tabular-nums" }}>
                {p.rate}
              </div>
              <div style={{ fontSize: 12, color: S.inkSoft, display: "flex", alignItems: "center", gap: 5 }}>
                <span style={{ width: 5, height: 5, borderRadius: 99, background: p.type === "Points" ? "oklch(0.6 0.12 260)" : S.pos }} />
                {p.type}
              </div>
              <div style={{ textAlign: "right", fontFamily: S.serif, fontSize: 22, color: S.ink, letterSpacing: -0.3, fontVariantNumeric: "tabular-nums" }}>
                {p.effective.toFixed(2)}<span style={{ fontSize: 12, color: S.inkMute }}>%</span>
              </div>
              <div style={{ textAlign: "right", fontSize: 12, color: S.inkMute, fontVariantNumeric: "tabular-nums" }}>
                {p.updated}
              </div>
              <div style={{ textAlign: "right" }}>
                <div style={{
                  display: "inline-flex", alignItems: "center", gap: 6,
                  fontFamily: S.sans, fontSize: 12, fontWeight: 500,
                  color: p.best ? S.bg : S.ink,
                  background: p.best ? S.ink : S.surface,
                  border: p.best ? "none" : `1px solid ${S.line}`,
                  padding: "7px 12px", borderRadius: 7,
                }}>
                  Shop
                  <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M7 17 17 7M7 7h10v10"/></svg>
                </div>
              </div>
            </div>
          ))}
        </div>

        <div style={{ marginTop: 10, display: "flex", gap: 14, fontFamily: S.sans, fontSize: 11, color: S.inkMute }}>
          <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
            <span style={{ width: 5, height: 5, borderRadius: 99, background: S.pos }} />
            Cashback
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
            <span style={{ width: 5, height: 5, borderRadius: 99, background: "oklch(0.6 0.12 260)" }} />
            Points — converted to CAD at published rate
          </div>
        </div>
      </div>

      {/* Sidebar — ad + stack hint */}
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <SAdSlot w="100%" h={600} size="300 × 600" />
        {/* subtle future-feature hint */}
        <div style={{
          padding: 18, border: `1px solid ${S.line}`, borderRadius: 12, background: S.surface,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
            <span style={{ fontFamily: S.sans, fontSize: 10, color: S.accentInk, letterSpacing: 1, textTransform: "uppercase", fontWeight: 600 }}>Coming soon</span>
          </div>
          <div style={{ fontFamily: S.serif, fontSize: 20, color: S.ink, letterSpacing: -0.4, lineHeight: 1.2, marginBottom: 8 }}>
            Stack with your card.
          </div>
          <div style={{ fontFamily: S.sans, fontSize: 13, color: S.inkSoft, lineHeight: 1.5, marginBottom: 12 }}>
            Add 2–4% more on top of portal cashback. We'll suggest the best credit card for each store.
          </div>
          <div style={{
            fontFamily: S.sans, fontSize: 12, color: S.ink, fontWeight: 500,
            padding: "8px 12px", border: `1px solid ${S.line}`, borderRadius: 7,
            textAlign: "center", background: S.bg,
          }}>Join the waitlist →</div>
        </div>
      </div>
    </div>
  );
}

// ─── 30-day history with chart ─────
function SHistory() {
  const max = Math.max(...HISTORY.map(d => d.rate));
  const w = 820, h = 180, pad = { t: 20, r: 20, b: 24, l: 32 };
  const plotW = w - pad.l - pad.r;
  const plotH = h - pad.t - pad.b;
  const step = plotW / (HISTORY.length - 1);

  const points = HISTORY.map((d, i) => ({
    x: pad.l + i * step,
    y: pad.t + (1 - d.rate / 9) * plotH,
    ...d,
  }));
  const pathD = points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x},${p.y}`).join(" ");
  const areaD = pathD + ` L${points[points.length-1].x},${pad.t + plotH} L${pad.l},${pad.t + plotH} Z`;

  return (
    <div style={{ padding: "56px 40px 40px" }}>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 20 }}>
        <div>
          <div style={{ fontFamily: S.serif, fontSize: 28, color: S.ink, letterSpacing: -0.6 }}>30-day best-rate history</div>
          <div style={{ fontFamily: S.sans, fontSize: 13, color: S.inkSoft, marginTop: 4 }}>Daily snapshot of the highest effective rate across all portals.</div>
        </div>
        <div style={{ fontFamily: S.sans, fontSize: 11, color: S.inkMute, letterSpacing: 0.8, textTransform: "uppercase" }}>Last 30 days</div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 360px", gap: 32 }}>
        {/* Chart */}
        <div style={{ border: `1px solid ${S.line}`, borderRadius: 12, padding: 20, background: S.surface }}>
          <svg viewBox={`0 0 ${w} ${h}`} style={{ width: "100%", height: "auto" }}>
            <defs>
              <linearGradient id="areaFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={S.accent} stopOpacity="0.2" />
                <stop offset="100%" stopColor={S.accent} stopOpacity="0" />
              </linearGradient>
            </defs>
            {/* y grid */}
            {[0, 3, 6, 9].map(v => {
              const y = pad.t + (1 - v / 9) * plotH;
              return (
                <g key={v}>
                  <line x1={pad.l} y1={y} x2={w - pad.r} y2={y} stroke={S.lineSoft} strokeWidth="1" />
                  <text x={pad.l - 8} y={y + 3} fontFamily={S.sans} fontSize="9" fill={S.inkMute} textAnchor="end">{v}%</text>
                </g>
              );
            })}
            {/* area + line */}
            <path d={areaD} fill="url(#areaFill)" />
            <path d={pathD} fill="none" stroke={S.accent} strokeWidth="1.8" strokeLinejoin="round" />
            {/* current point */}
            {points.slice(-1).map((p, i) => (
              <g key={i}>
                <circle cx={p.x} cy={p.y} r="5" fill={S.surface} stroke={S.accent} strokeWidth="2" />
                <circle cx={p.x} cy={p.y} r="2" fill={S.accent} />
              </g>
            ))}
            {/* x ticks */}
            {[0, 7, 14, 21, 29].map(i => (
              <text key={i} x={pad.l + i * step} y={h - 6} fontFamily={S.sans} fontSize="9" fill={S.inkMute} textAnchor="middle">{HISTORY[i].d}</text>
            ))}
          </svg>
        </div>

        {/* Stats block */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
          {[
            { l: "Current", v: "7.50%", sub: "via Aeroplan", c: S.accentInk },
            { l: "30-day high", v: "9.00%", sub: "Apr 8 · Swagbucks", c: S.pos },
            { l: "30-day low", v: "3.00%", sub: "Mar 20 · Rakuten", c: S.neg },
            { l: "30-day delta", v: "+4.50%", sub: "trending up", c: S.pos },
          ].map(s => (
            <div key={s.l} style={{ padding: "16px 16px", border: `1px solid ${S.line}`, borderRadius: 10, background: S.surface }}>
              <div style={{ fontFamily: S.sans, fontSize: 10, color: S.inkMute, letterSpacing: 0.8, textTransform: "uppercase" }}>{s.l}</div>
              <div style={{ fontFamily: S.serif, fontSize: 30, color: S.ink, letterSpacing: -0.7, marginTop: 6, fontVariantNumeric: "tabular-nums" }}>{s.v}</div>
              <div style={{ fontFamily: S.sans, fontSize: 11, color: s.c, marginTop: 2 }}>{s.sub}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Daily log table */}
      <div style={{ marginTop: 24 }}>
        <div style={{ fontFamily: S.sans, fontSize: 11, color: S.inkMute, letterSpacing: 0.8, textTransform: "uppercase", marginBottom: 12 }}>Daily log</div>
        <div style={{ border: `1px solid ${S.line}`, borderRadius: 10, overflow: "hidden", background: S.surface }}>
          <div style={{
            display: "grid", gridTemplateColumns: "120px 1fr 1fr 120px",
            fontFamily: S.sans, fontSize: 10, color: S.inkMute,
            letterSpacing: 0.8, textTransform: "uppercase",
            padding: "10px 16px", background: S.bg, borderBottom: `1px solid ${S.line}`,
          }}>
            <div>Day</div><div>Best offer</div><div>Portal</div><div style={{ textAlign: "right" }}>Effective</div>
          </div>
          {HISTORY.slice().reverse().slice(0, 10).map((d, i) => (
            <div key={d.d} style={{
              display: "grid", gridTemplateColumns: "120px 1fr 1fr 120px",
              alignItems: "center",
              padding: "10px 16px", borderBottom: i === 9 ? "none" : `1px solid ${S.lineSoft}`,
              fontFamily: S.sans, fontSize: 13,
            }}>
              <div style={{ color: S.inkSoft, fontVariantNumeric: "tabular-nums" }}>{d.d}</div>
              <div style={{ color: S.ink }}>{d.label}</div>
              <div style={{ color: S.inkSoft, display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ width: 5, height: 5, borderRadius: 99, background: PORTAL_COLORS[d.portal] }} />
                {d.portal}
              </div>
              <div style={{ textAlign: "right", fontFamily: S.serif, fontSize: 16, color: S.ink, fontVariantNumeric: "tabular-nums", letterSpacing: -0.2 }}>
                {d.rate.toFixed(2)}<span style={{ fontSize: 11, color: S.inkMute }}>%</span>
              </div>
            </div>
          ))}
          <div style={{ padding: "12px 16px", fontFamily: S.sans, fontSize: 12, color: S.accentInk, textAlign: "center", background: S.bg, borderTop: `1px solid ${S.lineSoft}` }}>
            Show full 30 days →
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── related stores ─────
function SRelated() {
  const items = [
    { name: "Nike", cat: "Apparel", rate: "3%" },
    { name: "Lululemon", cat: "Apparel", rate: "2 pts/$" },
    { name: "Gymshark", cat: "Apparel", rate: "2%" },
    { name: "Under Armour", cat: "Apparel", rate: "4%" },
    { name: "SportChek", cat: "Apparel", rate: "1.5%" },
    { name: "Roots", cat: "Apparel", rate: "3%" },
  ];
  return (
    <div style={{ padding: "32px 40px 56px", borderTop: `1px solid ${S.line}`, marginTop: 32 }}>
      <div style={{ fontFamily: S.serif, fontSize: 24, color: S.ink, letterSpacing: -0.5, marginBottom: 16 }}>Similar stores</div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 10 }}>
        {items.map(it => (
          <div key={it.name} style={{
            border: `1px solid ${S.line}`, borderRadius: 10, padding: "14px 14px",
            background: S.surface,
          }}>
            <SStoreLogo name={it.name} size={32} />
            <div style={{ fontFamily: S.sans, fontSize: 13, color: S.ink, fontWeight: 500, marginTop: 10 }}>{it.name}</div>
            <div style={{ fontFamily: S.sans, fontSize: 11, color: S.inkMute, marginTop: 2 }}>{it.cat}</div>
            <div style={{ fontFamily: S.serif, fontSize: 16, color: S.ink, letterSpacing: -0.3, marginTop: 10, fontVariantNumeric: "tabular-nums" }}>{it.rate}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── report + footer ─────
function SFootnote() {
  return (
    <div style={{ padding: "24px 40px", background: S.bg, borderTop: `1px solid ${S.line}`, fontFamily: S.sans, fontSize: 12, color: S.inkMute, display: "flex", justifyContent: "space-between" }}>
      <div>Spot a stale rate, duplicate store, or broken portal link? <span style={{ color: S.accentInk }}>Report an issue →</span></div>
      <div>Rates scraped 3h ago · always verify before purchasing.</div>
    </div>
  );
}

function SFooter() {
  return (
    <div style={{ background: S.ink, color: S.bg, padding: "48px 40px 32px" }}>
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr 1fr", gap: 48 }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 14 }}>
            <div style={{ width: 28, height: 28, borderRadius: 7, background: S.bg, display: "flex", alignItems: "center", justifyContent: "center", color: S.ink, fontFamily: S.serif, fontSize: 18, fontStyle: "italic" }}>m</div>
            <div style={{ fontFamily: S.sans, fontWeight: 600, fontSize: 15 }}>MaxCashBack</div>
          </div>
          <div style={{ fontFamily: S.sans, fontSize: 13, opacity: 0.6, lineHeight: 1.6, maxWidth: 320 }}>
            Independent cashback comparison. Not affiliated with any portal. Rates scraped daily.
          </div>
        </div>
        {[
          { t: "Product", i: ["Browse stores", "Top rates", "Popular", "Categories", "Extension"] },
          { t: "Portals", i: ["Rakuten.ca", "GCR", "Swagbucks", "Aeroplan"] },
          { t: "Company", i: ["About", "Contact", "Privacy", "Disclaimer"] },
        ].map(col => (
          <div key={col.t}>
            <div style={{ fontFamily: S.sans, fontSize: 11, opacity: 0.5, letterSpacing: 0.8, textTransform: "uppercase", marginBottom: 12 }}>{col.t}</div>
            {col.i.map(x => <div key={x} style={{ fontFamily: S.sans, fontSize: 13, marginBottom: 8, opacity: 0.85 }}>{x}</div>)}
          </div>
        ))}
      </div>
      <div style={{ marginTop: 40, paddingTop: 20, borderTop: `1px solid rgba(255,255,255,0.1)`, fontFamily: S.sans, fontSize: 12, opacity: 0.5 }}>© 2026 MaxCashBack</div>
    </div>
  );
}

function StorePage() {
  return (
    <div style={{ width: 1440, background: S.bg, fontFamily: S.sans, color: S.ink }}>
      <SNav />
      <SHeader />
      <SBestRate />
      <SCompareTable />
      <SHistory />
      <SRelated />
      <SFootnote />
      <SFooter />
    </div>
  );
}

Object.assign(window, { StorePage });
