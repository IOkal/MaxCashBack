// Search interaction — inline autocomplete in nav bar.
// Constraint: name-only, fast. Must handle 2,081+ stores cheaply.
// Strategy: flat list, substring + prefix-weighted scoring, cap 8 results.

const SEARCH_TOKENS = A_TOKENS;
const Q = SEARCH_TOKENS;

// Large store list — name only, mimics the full index
const ALL_STORES = (() => {
  // seed from popular/trending + padded procedural names for realism
  const seeds = [
    "adidas Canada", "Amazon.ca", "Apple Canada", "Aeroplan", "Aritzia",
    "Best Buy Canada", "Bluehost", "Booking.com", "Browns Shoes",
    "Canva", "Chefs Plate", "Costco Canada", "CyberGhost VPN", "Cineplex",
    "DAZN", "Dell Canada", "Disney+", "DoorDash", "Drop",
    "eharmony.ca", "Endy", "Expedia.ca",
    "Factor Canada", "FreshBooks", "Frank And Oak",
    "Gymshark", "Grammarly",
    "H&M", "HelloFresh", "Hostinger", "Hotspot Shield", "Hushed App",
    "Indigo", "InVideo", "iolo technologies",
    "Jobber",
    "Keeper Security", "Kobo",
    "Lululemon", "Lowe's Canada",
    "Marriott Bonvoy", "MacPaw", "MEC", "Microsoft Store", "Moo.com",
    "NordPass", "NordVPN", "Nike", "Nespresso",
    "Old Navy", "Oreck", "Overstock",
    "Patagonia", "Puma Canada", "1Password", "Private Internet Access VPN",
    "Rakuten Kobo", "Roots",
    "Sephora", "Skillshare", "Squarespace", "Staples Canada", "SSENSE",
    "TD Insurance", "The Bay", "TopCashback", "TunnelBear",
    "Udemy", "Under Armour", "Uber Eats",
    "Vistaprint", "Vrbo",
    "Walmart.ca", "Wayfair.ca", "Wealthsimple",
    "Zenfolio", "Zappos",
  ];
  // pad to feel like thousands — procedural names tagged by letter
  const extras = [];
  for (let i = 0; i < 20; i++) extras.push("Store " + (100 + i));
  return seeds.concat(extras);
})();

// Fast scorer — lowercase once, prefer prefix matches, then word-prefix, then substring
function scoreStore(name, qLower) {
  const lower = name.toLowerCase();
  if (lower === qLower) return 1000;
  if (lower.startsWith(qLower)) return 500 - lower.length;
  // word-prefix
  const words = lower.split(/[\s.'-]+/);
  for (const w of words) if (w.startsWith(qLower)) return 300 - lower.length;
  const idx = lower.indexOf(qLower);
  if (idx >= 0) return 100 - idx - lower.length * 0.1;
  return -1;
}

function searchStores(query, limit = 8) {
  if (!query) return [];
  const qLower = query.toLowerCase().trim();
  if (!qLower) return [];
  // single pass, keep top-N by score — O(n)
  const results = [];
  for (let i = 0; i < ALL_STORES.length; i++) {
    const s = scoreStore(ALL_STORES[i], qLower);
    if (s > 0) results.push({ name: ALL_STORES[i], score: s });
  }
  results.sort((a, b) => b.score - a.score);
  return results.slice(0, limit);
}

// highlight matched substring (cheap, one indexOf)
function Highlight({ text, q }) {
  if (!q) return <span>{text}</span>;
  const i = text.toLowerCase().indexOf(q.toLowerCase());
  if (i < 0) return <span>{text}</span>;
  return (
    <span>
      {text.slice(0, i)}
      <b style={{ color: Q.ink, fontWeight: 600 }}>{text.slice(i, i + q.length)}</b>
      {text.slice(i + q.length)}
    </span>
  );
}

function QStoreLogo({ name, size = 24 }) {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) % 360;
  return (
    <div style={{
      width: size, height: size, borderRadius: 6,
      background: `oklch(0.94 0.04 ${h})`, color: `oklch(0.38 0.08 ${h})`,
      display: "flex", alignItems: "center", justifyContent: "center",
      fontFamily: Q.sans, fontWeight: 600, fontSize: size * 0.5, flexShrink: 0,
    }}>{name[0]}</div>
  );
}

// Search input + dropdown combined
function SearchBar({ state = "empty", query = "" }) {
  const results = searchStores(query);
  const hasQuery = query.length > 0;
  const showNoResults = hasQuery && results.length === 0;

  const dropdownOpen = state === "focus-empty" || hasQuery;

  return (
    <div style={{ position: "relative", width: 520 }}>
      {/* input */}
      <div style={{
        display: "flex", alignItems: "center", gap: 10,
        background: Q.surface,
        border: `1.5px solid ${dropdownOpen ? Q.ink : Q.line}`,
        borderRadius: dropdownOpen ? "10px 10px 0 0" : 10,
        padding: "0 14px", height: 42,
        borderBottomColor: dropdownOpen ? "transparent" : Q.line,
      }}>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={Q.ink} strokeWidth="2.2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
        {query ? (
          <div style={{ fontFamily: Q.sans, fontSize: 14, color: Q.ink, flex: 1 }}>{query}<span style={{ display: "inline-block", width: 1, height: 14, background: Q.ink, marginLeft: 1, verticalAlign: "middle", animation: "blink 1s step-end infinite" }}/></div>
        ) : (
          <div style={{ fontFamily: Q.sans, fontSize: 14, color: Q.inkMute, flex: 1 }}>Search 2,081 stores…</div>
        )}
        {query && (
          <div style={{ fontFamily: Q.sans, fontSize: 11, color: Q.inkMute, padding: "3px 6px" }}>✕</div>
        )}
        <div style={{ fontFamily: Q.sans, fontSize: 10, color: Q.inkMute, border: `1px solid ${Q.line}`, borderRadius: 3, padding: "1px 5px", background: Q.bg }}>⌘K</div>
      </div>

      {/* dropdown */}
      {dropdownOpen && (
        <div style={{
          position: "absolute", top: 42, left: 0, right: 0,
          background: Q.surface,
          border: `1.5px solid ${Q.ink}`, borderTop: "none",
          borderRadius: "0 0 10px 10px",
          boxShadow: "0 16px 36px rgba(0,0,0,0.08)",
          overflow: "hidden", zIndex: 20,
        }}>
          {state === "focus-empty" && (
            <div style={{ padding: "18px 16px", fontFamily: Q.sans, fontSize: 12, color: Q.inkMute, textAlign: "center", letterSpacing: 0.2 }}>
              Start typing to search stores.
            </div>
          )}
          {showNoResults && (
            <div style={{ padding: "18px 16px" }}>
              <div style={{ fontFamily: Q.sans, fontSize: 13, color: Q.ink, fontWeight: 500, marginBottom: 4 }}>No stores found for "{query}"</div>
              <div style={{ fontFamily: Q.sans, fontSize: 12, color: Q.inkSoft, lineHeight: 1.5 }}>
                Check your spelling, or <span style={{ color: Q.accentInk, borderBottom: `1px solid ${Q.accent}`, paddingBottom: 1 }}>request this store →</span>
              </div>
            </div>
          )}
          {results.map((r, i) => (
            <div key={r.name} style={{
              display: "flex", alignItems: "center", gap: 10,
              padding: "9px 14px",
              background: i === 0 ? Q.bg : Q.surface,
              borderBottom: i === results.length - 1 ? "none" : `1px solid ${Q.lineSoft}`,
              fontFamily: Q.sans,
            }}>
              <QStoreLogo name={r.name} />
              <div style={{ flex: 1, fontSize: 14, color: Q.inkSoft, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                <Highlight text={r.name} q={query} />
              </div>
              {i === 0 && (
                <div style={{ fontFamily: Q.sans, fontSize: 10, color: Q.inkMute, border: `1px solid ${Q.line}`, borderRadius: 3, padding: "1px 5px" }}>↵</div>
              )}
            </div>
          ))}
          {results.length > 0 && (
            <div style={{ padding: "8px 14px", background: Q.bg, borderTop: `1px solid ${Q.lineSoft}`, fontFamily: Q.sans, fontSize: 11, color: Q.inkMute, display: "flex", justifyContent: "space-between" }}>
              <span>{results.length} {results.length === 1 ? "match" : "matches"}</span>
              <span>↑↓ navigate · ↵ open · esc close</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// Nav with inline search (full chrome around it so it feels real)
function SearchNav({ state, query }) {
  return (
    <div style={{
      height: 64, borderBottom: `1px solid ${Q.line}`,
      display: "flex", alignItems: "center", padding: "0 40px",
      background: Q.surface,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ width: 28, height: 28, borderRadius: 7, background: Q.ink, display: "flex", alignItems: "center", justifyContent: "center", color: Q.bg, fontFamily: Q.serif, fontSize: 18, fontStyle: "italic" }}>m</div>
        <div style={{ fontFamily: Q.sans, fontWeight: 600, fontSize: 15, letterSpacing: -0.3, color: Q.ink }}>MaxCashBack</div>
        <div style={{ fontSize: 10, fontFamily: Q.sans, color: Q.inkMute, border: `1px solid ${Q.line}`, borderRadius: 4, padding: "2px 6px", marginLeft: 6 }}>CA</div>
      </div>
      <div style={{ marginLeft: 32 }}>
        <SearchBar state={state} query={query} />
      </div>
      <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 14 }}>
        <div style={{ fontFamily: Q.sans, fontSize: 13, color: Q.inkSoft }}>Sign in</div>
        <div style={{ fontFamily: Q.sans, fontSize: 13, fontWeight: 500, color: Q.bg, background: Q.ink, padding: "8px 14px", borderRadius: 8 }}>Get alerts</div>
      </div>
    </div>
  );
}

// Framed mock of a nav area so dropdown has context
function SearchFrame({ state, query }) {
  return (
    <div style={{ width: 1200, background: Q.bg, height: 420, position: "relative" }}>
      <SearchNav state={state} query={query} />
      {/* page blur behind */}
      <div style={{ padding: "40px 40px", opacity: 0.5 }}>
        <div style={{ fontFamily: Q.serif, fontSize: 36, color: Q.ink, letterSpacing: -1 }}>Every cashback rate in Canada, in one place.</div>
        <div style={{ fontFamily: Q.sans, fontSize: 15, color: Q.inkSoft, marginTop: 10 }}>Compare 2,081 stores across 6 portals.</div>
      </div>
    </div>
  );
}

Object.assign(window, { SearchFrame, SearchBar });
