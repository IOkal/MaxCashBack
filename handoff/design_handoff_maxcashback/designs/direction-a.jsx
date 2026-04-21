// Direction A — "Modern Fintech" (v2)
// Refinements: dedicated ad slots, expanded top-rates + popular side-by-side,
// no movers section. Prominent search.

const A_TOKENS = {
  bg: "oklch(0.985 0.004 90)",
  surface: "#ffffff",
  ink: "oklch(0.18 0.01 80)",
  inkSoft: "oklch(0.45 0.01 80)",
  inkMute: "oklch(0.62 0.01 80)",
  line: "oklch(0.92 0.005 80)",
  lineSoft: "oklch(0.95 0.004 80)",
  accent: "oklch(0.58 0.11 185)",
  accentSoft: "oklch(0.94 0.03 185)",
  accentInk: "oklch(0.35 0.08 185)",
  pos: "oklch(0.55 0.13 155)",
  neg: "oklch(0.58 0.18 25)",
  adBg: "oklch(0.97 0.006 85)",
  sans: '"Inter", ui-sans-serif, system-ui, sans-serif',
  serif: '"Instrument Serif", ui-serif, Georgia, serif',
};
const A = A_TOKENS;

function AStoreLogo({ name, size = 34 }) {
  const letter = name[0];
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) % 360;
  const bg = `oklch(0.94 0.04 ${h})`;
  const fg = `oklch(0.38 0.08 ${h})`;
  return (
    <div style={{
      width: size, height: size, borderRadius: 8,
      background: bg, color: fg,
      display: "flex", alignItems: "center", justifyContent: "center",
      fontFamily: A.sans, fontWeight: 600, fontSize: size * 0.44,
      letterSpacing: -0.2, flexShrink: 0,
    }}>{letter}</div>
  );
}

function APortalDot({ portal }) {
  const colors = { "Rakuten.ca": "#BF0000", "Great Canadian Rebates": "#006B3C", "Swagbucks": "#0A7AA6", "Aeroplan eStore": "#D3273E", "TopCashback": "#E8344C", "Drop": "#6B4FBB" };
  return <span style={{ display: "inline-block", width: 6, height: 6, borderRadius: 99, background: colors[portal] || "#888", marginRight: 6, verticalAlign: "middle" }} />;
}

// ─── AD slot components ─────────────────────────────
function AdSlot({ w, h, label = "Advertisement", size = "728 × 90" }) {
  return (
    <div style={{
      width: w, height: h,
      border: `1px dashed ${A.line}`, background: A.adBg,
      borderRadius: 10, display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center", gap: 6,
      fontFamily: A.sans,
    }}>
      <div style={{ fontSize: 10, color: A.inkMute, letterSpacing: 1.2, textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 13, color: A.inkSoft, fontWeight: 500 }}>{size}</div>
    </div>
  );
}

function ANav() {
  return (
    <div style={{
      height: 64, borderBottom: `1px solid ${A.line}`,
      display: "flex", alignItems: "center", padding: "0 40px",
      background: A.surface, position: "sticky", top: 0, zIndex: 10,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ width: 28, height: 28, borderRadius: 7, background: A.ink, display: "flex", alignItems: "center", justifyContent: "center", color: A.bg, fontFamily: A.serif, fontSize: 18, fontStyle: "italic" }}>m</div>
        <div style={{ fontFamily: A.sans, fontWeight: 600, fontSize: 15, letterSpacing: -0.3, color: A.ink }}>MaxCashBack</div>
        <div style={{ fontSize: 10, fontFamily: A.sans, color: A.inkMute, border: `1px solid ${A.line}`, borderRadius: 4, padding: "2px 6px", marginLeft: 6 }}>CA</div>
      </div>
      <div style={{ display: "flex", gap: 28, marginLeft: 48, fontFamily: A.sans, fontSize: 14, color: A.inkSoft }}>
        <div style={{ color: A.ink, fontWeight: 500 }}>Browse</div>
        <div>Top rates</div>
        <div>Popular</div>
        <div>Categories</div>
        <div>Portals</div>
        <div>Extension</div>
      </div>
      <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 14 }}>
        <div style={{ fontFamily: A.sans, fontSize: 13, color: A.inkSoft }}>Sign in</div>
        <div style={{ fontFamily: A.sans, fontSize: 13, fontWeight: 500, color: A.bg, background: A.ink, padding: "8px 14px", borderRadius: 8 }}>Get alerts</div>
      </div>
    </div>
  );
}

function AHero() {
  return (
    <div style={{ padding: "64px 40px 44px", borderBottom: `1px solid ${A.line}`, background: A.surface }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 22 }}>
        <span style={{ width: 6, height: 6, borderRadius: 99, background: A.pos, boxShadow: `0 0 0 3px oklch(0.55 0.13 155 / 0.15)` }} />
        <span style={{ fontFamily: A.sans, fontSize: 12, color: A.inkSoft, letterSpacing: 0.3, textTransform: "uppercase" }}>
          Live · {window.MCB_DATA.stats.lastUpdate}
        </span>
      </div>
      <div style={{ fontFamily: A.serif, fontSize: 62, lineHeight: 1.02, letterSpacing: -1.6, color: A.ink, maxWidth: 900, fontWeight: 400 }}>
        Every cashback rate in Canada, <em style={{ color: A.accentInk }}>in one place.</em>
      </div>
      <div style={{ fontFamily: A.sans, fontSize: 16, color: A.inkSoft, marginTop: 16, maxWidth: 560, lineHeight: 1.5 }}>
        Compare {window.MCB_DATA.stats.stores.toLocaleString()} stores across {window.MCB_DATA.stats.portals} portals.
      </div>

      {/* prominent search */}
      <div style={{ marginTop: 30, display: "flex", gap: 12, alignItems: "center", maxWidth: 760 }}>
        <div style={{
          flex: 1, display: "flex", alignItems: "center", gap: 12,
          background: A.bg, border: `1.5px solid ${A.ink}`, borderRadius: 12,
          padding: "0 20px", height: 64,
        }}>
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke={A.ink} strokeWidth="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
          <div style={{ fontFamily: A.sans, fontSize: 17, color: A.inkMute, flex: 1 }}>Search 2,081 stores — adidas, HelloFresh, 1Password…</div>
          <div style={{ fontFamily: A.sans, fontSize: 11, color: A.inkMute, border: `1px solid ${A.line}`, borderRadius: 4, padding: "3px 7px", background: A.surface }}>⌘ K</div>
        </div>
        <div style={{ fontFamily: A.sans, fontSize: 15, fontWeight: 500, color: A.bg, background: A.accent, padding: "0 28px", height: 64, display: "flex", alignItems: "center", borderRadius: 12 }}>
          Search
        </div>
      </div>
      <div style={{ marginTop: 16, display: "flex", gap: 8, flexWrap: "wrap" }}>
        <div style={{ fontFamily: A.sans, fontSize: 11, color: A.inkMute, letterSpacing: 0.8, textTransform: "uppercase", marginRight: 6, alignSelf: "center" }}>Trending</div>
        {["Amazon", "Best Buy", "Lululemon", "Marriott", "HelloFresh", "adidas", "Canva"].map(t => (
          <div key={t} style={{ fontFamily: A.sans, fontSize: 12, color: A.inkSoft, border: `1px solid ${A.line}`, borderRadius: 99, padding: "5px 12px", background: A.surface }}>{t}</div>
        ))}
      </div>

      <div style={{ marginTop: 44, display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 0, borderTop: `1px solid ${A.line}`, paddingTop: 24 }}>
        {[
          { v: "2,081", l: "Stores tracked" },
          { v: "6", l: "Portals compared" },
          { v: "65%", l: "Highest live rate" },
          { v: "24h", l: "Update cadence" },
        ].map((s, i) => (
          <div key={i} style={{ paddingLeft: i ? 24 : 0, borderLeft: i ? `1px solid ${A.line}` : "none" }}>
            <div style={{ fontFamily: A.serif, fontSize: 36, color: A.ink, lineHeight: 1, letterSpacing: -0.8 }}>{s.v}</div>
            <div style={{ fontFamily: A.sans, fontSize: 11, color: A.inkMute, marginTop: 6, letterSpacing: 0.3, textTransform: "uppercase" }}>{s.l}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Leaderboard ad — right after hero, above the fold on most screens
function AHeroAd() {
  return (
    <div style={{ background: A.surface, padding: "28px 40px 32px", borderBottom: `1px solid ${A.line}`, display: "flex", justifyContent: "center" }}>
      <AdSlot w={970} h={120} label="Advertisement · leaderboard" size="970 × 120" />
    </div>
  );
}

// Combined: top rates (left) + most popular (right). 30 rows each.
function ADualBoards() {
  // build 30 rows for each side from available data + procedurally extended
  const baseTop = window.MCB_DATA.trending;
  const basePop = window.MCB_DATA.popular;
  const catRot = ["Software", "Travel", "Apparel", "Food", "Electronics", "Beauty", "Home", "Learning"];

  const topRows = Array.from({ length: 30 }, (_, i) => {
    if (i < baseTop.length) return baseTop[i];
    const src = baseTop[i % baseTop.length];
    return {
      ...src,
      name: src.name + " " + String.fromCharCode(65 + (i % 26)),
      slug: src.slug + "-" + i,
      rate: +(src.rate * (0.95 - (i - baseTop.length) * 0.018)).toFixed(2),
      trend: ((i * 7) % 9) - 4,
    };
  });

  const popRows = Array.from({ length: 30 }, (_, i) => {
    if (i < basePop.length) return basePop[i];
    const src = basePop[i % basePop.length];
    const names = ["Apple.ca", "Walmart", "Costco", "Indigo", "Sephora", "Nike", "Staples", "Roots", "The Bay", "Aritzia", "Old Navy", "Saje", "MEC", "Hudson's Bay", "Simons", "Altitude Sports", "SSENSE", "Endy", "Casper", "Frank And Oak", "SportChek", "Amazon.ca", "Dell Canada", "Expedia"];
    return {
      name: names[i - basePop.length] || src.name + " " + i,
      slug: "store-" + i,
      rate: ["1%", "2%", "3 pts/$", "1.5%", "2 pts/$", "4%", "0.5%"][i % 7],
      rank: i + 1,
      cat: catRot[i % catRot.length],
    };
  });

  const Col = ({ title, kicker, action, children }) => (
    <div style={{ flex: 1, minWidth: 0 }}>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 18 }}>
        <div>
          <div style={{ fontFamily: A.sans, fontSize: 11, color: A.accentInk, letterSpacing: 0.8, textTransform: "uppercase", marginBottom: 6 }}>{kicker}</div>
          <div style={{ fontFamily: A.serif, fontSize: 34, color: A.ink, letterSpacing: -0.8, fontWeight: 400 }}>{title}</div>
        </div>
        <div style={{ fontFamily: A.sans, fontSize: 12, color: A.accentInk }}>{action}</div>
      </div>
      {children}
    </div>
  );

  return (
    <div style={{ background: A.surface, padding: "56px 40px", borderBottom: `1px solid ${A.line}` }}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 40 }}>
        {/* Top rates */}
        <Col kicker="Live · ranked by effective cash" title="Top rates right now" action="View all 2,081 →">
          <div style={{
            display: "grid",
            gridTemplateColumns: "28px 1fr 90px",
            fontFamily: A.sans, fontSize: 10, color: A.inkMute,
            letterSpacing: 0.6, textTransform: "uppercase",
            padding: "0 0 10px", borderBottom: `1px solid ${A.line}`,
          }}>
            <div>#</div><div>Store</div><div style={{ textAlign: "right" }}>Rate</div>
          </div>
          {topRows.map((r, i) => (
            <div key={r.slug} style={{
              display: "grid",
              gridTemplateColumns: "28px 1fr 90px",
              alignItems: "center",
              padding: "9px 0", borderBottom: `1px solid ${A.lineSoft}`,
              fontFamily: A.sans,
            }}>
              <div style={{ fontSize: 11, color: A.inkMute, fontVariantNumeric: "tabular-nums" }}>{String(i + 1).padStart(2, "0")}</div>
              <div style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
                <AStoreLogo name={r.name} size={26} />
                <div style={{ minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  <div style={{ fontSize: 13, color: A.ink, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.name}</div>
                </div>
              </div>
              <div style={{ textAlign: "right", fontFamily: A.serif, fontSize: 20, color: A.ink, letterSpacing: -0.4, fontVariantNumeric: "tabular-nums" }}>
                {typeof r.rate === "number" ? r.rate : r.rate}<span style={{ fontSize: 12, color: A.inkMute, marginLeft: 1 }}>{typeof r.rate === "number" ? "%" : ""}</span>
              </div>
            </div>
          ))}
        </Col>

        {/* Most popular */}
        <Col kicker="Most visited · last 7 days" title="Popular right now" action="">
          <div style={{
            display: "grid",
            gridTemplateColumns: "28px 1fr 90px",
            fontFamily: A.sans, fontSize: 10, color: A.inkMute,
            letterSpacing: 0.6, textTransform: "uppercase",
            padding: "0 0 10px", borderBottom: `1px solid ${A.line}`,
          }}>
            <div>#</div><div>Store</div><div style={{ textAlign: "right" }}>Best rate</div>
          </div>
          {popRows.map((r, i) => (
            <div key={r.slug} style={{
              display: "grid",
              gridTemplateColumns: "28px 1fr 90px",
              alignItems: "center",
              padding: "9px 0", borderBottom: `1px solid ${A.lineSoft}`,
              fontFamily: A.sans,
            }}>
              <div style={{ fontSize: 11, color: A.inkMute, fontVariantNumeric: "tabular-nums" }}>{String(i + 1).padStart(2, "0")}</div>
              <div style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
                <AStoreLogo name={r.name} size={26} />
                <div style={{ minWidth: 0, overflow: "hidden" }}>
                  <div style={{ fontSize: 13, color: A.ink, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.name}</div>
                </div>
              </div>
              <div style={{ textAlign: "right", fontFamily: A.serif, fontSize: 18, color: A.ink, letterSpacing: -0.3, fontVariantNumeric: "tabular-nums" }}>
                {r.rate}
              </div>
            </div>
          ))}
        </Col>
      </div>
    </div>
  );
}

// Mid-page ad — billboard between boards and portals
function AMidAd() {
  return (
    <div style={{ background: A.bg, padding: "36px 40px", borderBottom: `1px solid ${A.line}`, display: "flex", justifyContent: "center" }}>
      <AdSlot w={970} h={250} label="Advertisement · billboard" size="970 × 250" />
    </div>
  );
}

function APortals() {
  const portals = window.MCB_DATA.portals;
  return (
    <div style={{ background: A.surface, padding: "64px 40px", borderBottom: `1px solid ${A.line}` }}>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 32 }}>
        <div>
          <div style={{ fontFamily: A.serif, fontSize: 34, color: A.ink, letterSpacing: -0.8 }}>Six portals, one scan.</div>
          <div style={{ fontFamily: A.sans, fontSize: 14, color: A.inkSoft, marginTop: 6, maxWidth: 540 }}>We pull from every major Canadian cashback and rewards program so you never sign in twice.</div>
        </div>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 14 }}>
        {portals.map(p => (
          <div key={p.id} style={{
            background: A.bg, border: `1px solid ${A.line}`, borderRadius: 12,
            padding: "18px 20px", display: "flex", alignItems: "center", gap: 14,
          }}>
            <div style={{
              width: 40, height: 40, borderRadius: 10, background: p.color,
              display: "flex", alignItems: "center", justifyContent: "center",
              color: "white", fontFamily: A.sans, fontWeight: 700, fontSize: 13, letterSpacing: 0.3,
            }}>{p.short}</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontFamily: A.sans, fontSize: 14, fontWeight: 500, color: A.ink }}>{p.name}</div>
              <div style={{ fontFamily: A.sans, fontSize: 12, color: A.inkMute, marginTop: 2, fontVariantNumeric: "tabular-nums" }}>
                {400 + p.id.length * 80} stores · updated 2h ago
              </div>
            </div>
            <div style={{ fontFamily: A.sans, fontSize: 12, color: A.inkSoft }}>→</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function ACategories() {
  return (
    <div style={{ background: A.bg, padding: "56px 40px", borderBottom: `1px solid ${A.line}` }}>
      <div style={{ fontFamily: A.serif, fontSize: 30, color: A.ink, letterSpacing: -0.6, marginBottom: 24 }}>Browse by category</div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10 }}>
        {window.MCB_DATA.categories.map(c => (
          <div key={c.name} style={{
            background: A.surface,
            border: `1px solid ${A.line}`, borderRadius: 12, padding: "16px 18px",
            display: "flex", flexDirection: "column", gap: 4,
          }}>
            <div style={{ fontFamily: A.sans, fontSize: 14, color: A.ink, fontWeight: 500 }}>{c.name}</div>
            <div style={{ fontFamily: A.sans, fontSize: 12, color: A.inkMute, fontVariantNumeric: "tabular-nums" }}>{c.count} stores</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function AExtensionCTA() {
  return (
    <div style={{ padding: "72px 40px", background: A.surface, borderBottom: `1px solid ${A.line}` }}>
      <div style={{ display: "grid", gridTemplateColumns: "1.1fr 1fr", gap: 48, alignItems: "center" }}>
        <div>
          <div style={{ fontFamily: A.sans, fontSize: 11, color: A.accentInk, letterSpacing: 0.8, textTransform: "uppercase", marginBottom: 12 }}>Browser extension</div>
          <div style={{ fontFamily: A.serif, fontSize: 42, color: A.ink, letterSpacing: -1, lineHeight: 1.05, marginBottom: 16 }}>
            Never miss a rate. <em style={{ color: A.accentInk }}>Ever.</em>
          </div>
          <div style={{ fontFamily: A.sans, fontSize: 15, color: A.inkSoft, lineHeight: 1.55, maxWidth: 480, marginBottom: 22 }}>
            A quiet ribbon appears at checkout showing the best portal for the store you're on. One click, activated.
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ fontFamily: A.sans, fontSize: 14, fontWeight: 500, color: A.bg, background: A.ink, padding: "12px 20px", borderRadius: 10 }}>Add to Chrome</div>
            <div style={{ fontFamily: A.sans, fontSize: 14, fontWeight: 500, color: A.ink, padding: "12px 20px", borderRadius: 10, border: `1px solid ${A.line}`, background: A.bg }}>Firefox · Safari</div>
          </div>
        </div>
        <div style={{
          background: A.bg, borderRadius: 16, border: `1px solid ${A.line}`,
          padding: 20,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 14 }}>
            <div style={{ width: 10, height: 10, borderRadius: 99, background: "#ED6A5E" }}/>
            <div style={{ width: 10, height: 10, borderRadius: 99, background: "#F5BE4F" }}/>
            <div style={{ width: 10, height: 10, borderRadius: 99, background: "#62C555" }}/>
            <div style={{ flex: 1, textAlign: "center", fontFamily: A.sans, fontSize: 11, color: A.inkMute }}>lululemon.com</div>
          </div>
          <div style={{
            border: `1px solid ${A.accent}`, background: A.accentSoft,
            borderRadius: 10, padding: 14, display: "flex", alignItems: "center", gap: 12,
          }}>
            <div style={{ width: 32, height: 32, borderRadius: 8, background: A.accent, color: A.surface, display: "flex", alignItems: "center", justifyContent: "center", fontFamily: A.serif, fontSize: 18, fontStyle: "italic" }}>m</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontFamily: A.sans, fontSize: 13, color: A.ink, fontWeight: 500 }}>Best rate: <b>2 pts/$</b> via Aeroplan eStore</div>
              <div style={{ fontFamily: A.sans, fontSize: 11, color: A.inkSoft, marginTop: 1 }}>+ 0.5% Rakuten · stackable with TD Cash Back Visa</div>
            </div>
            <div style={{ fontFamily: A.sans, fontSize: 12, fontWeight: 500, color: A.surface, background: A.accent, padding: "7px 12px", borderRadius: 7 }}>Activate</div>
          </div>
        </div>
      </div>
    </div>
  );
}

function AFooter() {
  return (
    <div style={{ background: A.ink, color: A.bg, padding: "56px 40px 36px" }}>
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr 1fr", gap: 48 }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 14 }}>
            <div style={{ width: 28, height: 28, borderRadius: 7, background: A.bg, display: "flex", alignItems: "center", justifyContent: "center", color: A.ink, fontFamily: A.serif, fontSize: 18, fontStyle: "italic" }}>m</div>
            <div style={{ fontFamily: A.sans, fontWeight: 600, fontSize: 15 }}>MaxCashBack</div>
          </div>
          <div style={{ fontFamily: A.sans, fontSize: 13, opacity: 0.6, lineHeight: 1.6, maxWidth: 320 }}>
            Independent cashback comparison. Not affiliated with any portal. Rates scraped daily — always verify before purchasing.
          </div>
        </div>
        {[
          { t: "Product", i: ["Browse stores", "Top rates", "Popular", "Categories", "Extension"] },
          { t: "Portals", i: ["Rakuten.ca", "GCR", "Swagbucks", "Aeroplan"] },
          { t: "Company", i: ["About", "Contact", "Privacy", "Disclaimer"] },
        ].map(col => (
          <div key={col.t}>
            <div style={{ fontFamily: A.sans, fontSize: 11, opacity: 0.5, letterSpacing: 0.8, textTransform: "uppercase", marginBottom: 12 }}>{col.t}</div>
            {col.i.map(x => <div key={x} style={{ fontFamily: A.sans, fontSize: 13, marginBottom: 8, opacity: 0.85 }}>{x}</div>)}
          </div>
        ))}
      </div>
      <div style={{ marginTop: 48, paddingTop: 24, borderTop: `1px solid rgba(255,255,255,0.1)`, fontFamily: A.sans, fontSize: 12, opacity: 0.5, display: "flex", justifyContent: "space-between" }}>
        <div>© 2026 MaxCashBack</div>
        <div>Last scrape {window.MCB_DATA.stats.lastUpdate}</div>
      </div>
    </div>
  );
}

function DirectionA() {
  return (
    <div style={{ width: 1440, background: A.surface, fontFamily: A.sans, color: A.ink }}>
      <ANav />
      <AHero />
      <AHeroAd />
      <ADualBoards />
      <AMidAd />
      <APortals />
      <ACategories />
      <AExtensionCTA />
      <AFooter />
    </div>
  );
}

Object.assign(window, { DirectionA });
