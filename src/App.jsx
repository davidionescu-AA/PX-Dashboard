import { useState, useEffect, useMemo } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, PieChart, Pie, Cell, AreaChart, Area,
  ComposedChart, ReferenceLine, Line,
} from "recharts";
import { supabase } from "./supabase";

/* ═══════════════════════════════════════════════════
   ALPHA ANYWHERE PX OPERATIONS DASHBOARD  v7
   Full-screen · Sidebar nav · Polished insights
   ═══════════════════════════════════════════════════ */

// ── Brand palette ──
const C = {
  navy: "#0A1628", navyMid: "#111B2E", navyLight: "#162037",
  blue: "#3B82F6", blueLight: "#60A5FA", blueDark: "#2563EB",
  green: "#22C55E", greenDark: "#16A34A", greenLight: "#4ADE80",
  pink: "#EC4899", yellow: "#F59E0B", yellowDark: "#D97706",
  white: "#FFFFFF", bg: "#F1F5F9", surface: "#FFFFFF",
  border: "#E2E8F0", borderLight: "#F1F5F9", borderDark: "#CBD5E1",
  text: "#0F172A", textMid: "#475569", textLight: "#94A3B8",
  red: "#EF4444", redDark: "#DC2626", orange: "#F97316",
  teal: "#14B8A6", purple: "#8B5CF6",
};

const MC = {
  "Curriculum & Learning": "#22C55E", "Platform & Technical": "#F97316",
  "Testing & Assessment": "#3B82F6", "Coaching & Scheduling": "#14B8A6",
  "Progress & Reporting": "#8B5CF6", "Enrollment & Onboarding": "#F59E0B",
  "Account & Billing": "#EC4899", "Funding & Transfers": "#06B6D4",
  "Follow-ups": "#94A3B8", "Rewards": "#D97706", "General & Other": "#CBD5E1",
};

const AGENT_COLORS = ["#EC4899", "#F59E0B", "#3B82F6", "#14B8A6", "#22C55E", "#F97316"];

const AGENT_DISPLAY = {
  "Mary Betz": { short: "Mary", tz: "US (ET)", color: "#8B5CF6", order: 0 },
  "Nene Addico": { short: "Nene", tz: "Accra (GMT)", color: "#14B8A6", order: 1 },
  "David Ionescu": { short: "David", tz: "Bucharest (EET)", color: "#F97316", order: 2 },
};

const SEV = {
  critical: { bg: "#FEF2F2", border: "#FCA5A5", accent: "#DC2626", badge: "#DC2626", badgeText: "#FFF", label: "Critical" },
  warning:  { bg: "#FFFBEB", border: "#FDE68A", accent: "#D97706", badge: "#D97706", badgeText: "#FFF", label: "Warning" },
  info:     { bg: "#EFF6FF", border: "#BFDBFE", accent: "#2563EB", badge: "#2563EB", badgeText: "#FFF", label: "Info" },
  positive: { bg: "#F0FDF4", border: "#BBF7D0", accent: "#16A34A", badge: "#16A34A", badgeText: "#FFF", label: "Positive" },
};

const PRI = {
  P1: { color: "#DC2626", bg: "#FEE2E2" },
  P2: { color: "#D97706", bg: "#FEF3C7" },
  P3: { color: "#2563EB", bg: "#DBEAFE" },
};

const TABS = [
  { key: "overview", label: "Overview", icon: "◎" },
  { key: "insights", label: "Insights", icon: "◆" },
  { key: "categories", label: "Categories", icon: "▦" },
  { key: "volume", label: "Volume & RT", icon: "▲" },
  { key: "team", label: "Team", icon: "●" },
  { key: "attrition", label: "Attrition", icon: "⟁" },
];

// ═══ DATA HOOKS ═══

function useSupabase() {
  const [weeklyStats, setWeeklyStats] = useState([]);
  const [categorizations, setCategorizations] = useState([]);
  const [insights, setInsights] = useState([]);
  const [hourlyPatterns, setHourlyPatterns] = useState([]);
  const [conversations, setConversations] = useState([]);
  const [weeklyReports, setWeeklyReports] = useState([]);
  const [monthlyReports, setMonthlyReports] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      const [ws, cat, ins, hp] = await Promise.all([
        supabase.from("weekly_stats").select("*").order("week_label"),
        supabase.from("categorizations").select("*"),
        supabase.from("weekly_insights").select("*").eq("status", "published"),
        supabase.from("hourly_patterns").select("*").order("hour_num"),
      ]);
      // Fetch weekly reports separately so errors don't block the main dashboard
      let wr = { data: [] };
      let mr = { data: [] };
      try { wr = await supabase.from("weekly_reports").select("*").eq("status", "published"); } catch (e) { console.warn("weekly_reports fetch failed:", e); }
      try { mr = await supabase.from("monthly_reports").select("*").eq("status", "published"); } catch (e) { console.warn("monthly_reports fetch failed:", e); }
      // Fetch conversations — try with first_response_min, fall back without it
      let conv = await supabase.from("conversations").select("id,created_at,user_name,assigned_agent,state,subject,week_label,message_count,source_channel,first_response_min");
      if (conv.error) {
        conv = await supabase.from("conversations").select("id,created_at,user_name,assigned_agent,state,subject,week_label,message_count,source_channel");
      }
      setWeeklyStats(ws.data || []);
      setCategorizations(cat.data || []);
      setInsights(ins.data || []);
      setHourlyPatterns(hp.data || []);
      setConversations(conv.data || []);
      setWeeklyReports(wr.data || []);
      setMonthlyReports(mr.data || []);
      setLoading(false);
    }
    load();
  }, []);

  return { weeklyStats, categorizations, insights, hourlyPatterns, conversations, weeklyReports, monthlyReports, loading };
}

// ═══ DERIVED DATA ═══

function deriveData(weeklyStats, categorizations, hourlyPatterns, insights, conversations, dateRange, inboxFilter) {
  let filteredConvos = conversations.filter(c => c.assigned_agent && c.assigned_agent !== "Unassigned");
  if (dateRange.from || dateRange.to) {
    filteredConvos = filteredConvos.filter(c => {
      const d = c.created_at?.slice(0, 10);
      if (!d) return false;
      if (dateRange.from && d < dateRange.from) return false;
      if (dateRange.to && d > dateRange.to) return false;
      return true;
    });
  }
  if (inboxFilter && inboxFilter !== "all") {
    filteredConvos = filteredConvos.filter(c => c.source_channel === inboxFilter);
  }
  const filteredIds = new Set(filteredConvos.map(c => String(c.id)));
  const filteredCats = categorizations.filter(c => filteredIds.has(String(c.conversation_id)));

  const totalConversations = filteredConvos.length;
  const totalMessages = filteredConvos.reduce((s, c) => s + (c.message_count || 1), 0);
  const msgsPerConvo = totalConversations > 0 ? Math.round(totalMessages / totalConversations * 10) / 10 : 0;

  const catById = {};
  filteredCats.forEach(c => { catById[String(c.conversation_id)] = c; });

  const TEAM_NAMES = new Set(["Mary Betz", "Nene Addico", "David Ionescu"]);
  const weekMap = {};
  filteredConvos.forEach(c => {
    const wl = c.week_label;
    if (!wl) return;
    if (!weekMap[wl]) weekMap[wl] = { convos: [], cats: {} };
    weekMap[wl].convos.push(c);
    const cat = catById[String(c.id)];
    if (cat && cat.primary_macro && cat.primary_macro !== "UNCATEGORIZED") {
      weekMap[wl].cats[cat.primary_macro] = (weekMap[wl].cats[cat.primary_macro] || 0) + 1;
    }
  });

  const weekMeta = {};
  weeklyStats.forEach(w => { weekMeta[w.week_label] = w; });

  const filteredWeekLabels = Object.keys(weekMap).sort((a, b) => {
    // Parse "Mon DD" labels like "Feb 16", "Mar 2" into comparable dates
    const parse = s => { const d = new Date(s + ", 2026"); return isNaN(d) ? 0 : d.getTime(); };
    return parse(a) - parse(b);
  });
  const weekCount = filteredWeekLabels.length;

  // Helper: percentile from sorted array
  const percentile = (sorted, p) => {
    if (!sorted.length) return null;
    if (sorted.length === 1) return sorted[0];
    const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
    return sorted[idx];
  };
  const avg = arr => arr && arr.length > 0 ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length * 10) / 10 : null;
  const med = arr => {
    if (!arr || !arr.length) return null;
    const s = [...arr].sort((a, b) => a - b);
    const mid = Math.floor(s.length / 2);
    return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
  };

  // Build agent stats from filtered conversations (respects date + inbox filters)
  const agentMap = {};
  filteredConvos.forEach(c => {
    const agent = c.assigned_agent;
    if (!agent || !TEAM_NAMES.has(agent)) return;
    if (!agentMap[agent]) agentMap[agent] = { conversations: 0, messages: 0, rts: [] };
    agentMap[agent].conversations += 1;
    agentMap[agent].messages += (c.message_count || 1);
    if (c.first_response_min != null) {
      agentMap[agent].rts.push(c.first_response_min);
    }
  });

  // Aggregate exchange-level stats from weekly_stats agent_stats JSON
  const exchangeByAgent = {};
  filteredWeekLabels.forEach(wl => {
    const meta = weekMeta[wl];
    if (!meta?.agent_stats) return;
    try {
      const agents = typeof meta.agent_stats === "string" ? JSON.parse(meta.agent_stats) : meta.agent_stats;
      agents.forEach(a => {
        if (!TEAM_NAMES.has(a.name)) return;
        if (!exchangeByAgent[a.name]) exchangeByAgent[a.name] = { replyCount: 0, exchangeMedianRT: null, exchangeMeanRT: null, exchangeP90RT: null };
        if (a.reply_count) exchangeByAgent[a.name].replyCount += a.reply_count;
        // For RT, use latest week with data (since we can't merge medians)
        if (a.exchange_median_rt != null) {
          exchangeByAgent[a.name].exchangeMedianRT = a.exchange_median_rt;
          exchangeByAgent[a.name].exchangeMeanRT = a.exchange_mean_rt;
          exchangeByAgent[a.name].exchangeP90RT = a.exchange_p90_rt;
        }
      });
    } catch(e) {}
  });

  const agentList = Object.entries(agentMap)
    .map(([name, d]) => {
      const sorted = [...d.rts].sort((a, b) => a - b);
      const rawMedian = med(sorted);
      const ex = exchangeByAgent[name] || {};
      return {
        name,
        conversations: d.conversations,
        messages: d.messages,
        medianRT: rawMedian != null ? Math.round(rawMedian) : null,
        meanRT: avg(sorted) != null ? Math.round(avg(sorted)) : null,
        p25: sorted.length > 0 ? Math.round(percentile(sorted, 0.25)) : null,
        p75: sorted.length > 0 ? Math.round(percentile(sorted, 0.75)) : null,
        p90: sorted.length > 0 ? Math.round(percentile(sorted, 0.90)) : null,
        responseCount: sorted.length,
        replyCount: ex.replyCount || null,
        exchangeMedianRT: ex.exchangeMedianRT != null ? Math.round(ex.exchangeMedianRT) : null,
        exchangeMeanRT: ex.exchangeMeanRT != null ? Math.round(ex.exchangeMeanRT) : null,
        exchangeP90RT: ex.exchangeP90RT != null ? Math.round(ex.exchangeP90RT) : null,
        ...AGENT_DISPLAY[name],
      };
    })
    .sort((a, b) => (a.order ?? 99) - (b.order ?? 99));

  const realAgentConvos = agentList.reduce((s, a) => s + a.conversations, 0);
  // Team-wide stats from all filtered RTs (not averaging per-agent, but from raw values)
  const allFilteredRTs = filteredConvos
    .filter(c => c.first_response_min != null && TEAM_NAMES.has(c.assigned_agent))
    .map(c => c.first_response_min)
    .sort((a, b) => a - b);
  const teamMedianRT = med(allFilteredRTs) != null ? Math.round(med(allFilteredRTs)) : null;
  const teamMeanRT = avg(allFilteredRTs) != null ? Math.round(avg(allFilteredRTs)) : null;
  const teamP90 = allFilteredRTs.length > 0 ? Math.round(percentile(allFilteredRTs, 0.90)) : null;
  const teamP25 = allFilteredRTs.length > 0 ? Math.round(percentile(allFilteredRTs, 0.25)) : null;

  const macroMap = {};
  const subMap = {};
  filteredCats.forEach(cat => {
    if (cat.primary_macro && cat.primary_macro !== "UNCATEGORIZED") {
      macroMap[cat.primary_macro] = (macroMap[cat.primary_macro] || 0) + 1;
      const subKey = cat.primary_sub || "Other";
      const fullKey = `${subKey}|||${cat.primary_macro}`;
      subMap[fullKey] = (subMap[fullKey] || 0) + 1;
    }
    if (cat.secondary_macro && cat.secondary_macro !== "UNCATEGORIZED") {
      macroMap[cat.secondary_macro] = (macroMap[cat.secondary_macro] || 0) + 1;
    }
  });

  const macroEntries = Object.entries(macroMap).sort((a, b) => b[1] - a[1]);
  const detailRows = Object.entries(subMap)
    .map(([key, count]) => { const [sub, macro] = key.split("|||"); return { name: sub, count, macro }; })
    .sort((a, b) => b.count - a.count);

  const weekly = filteredWeekLabels.map(wl => {
    const wd = weekMap[wl];
    const meta = weekMeta[wl] || {};
    // Compute RT from filtered conversations (respects inbox + date filters)
    const weekRTs = wd.convos
      .filter(c => c.first_response_min != null && TEAM_NAMES.has(c.assigned_agent))
      .map(c => c.first_response_min);
    const weekRT = weekRTs.length > 0 ? Math.round(weekRTs.reduce((a, b) => a + b, 0) / weekRTs.length) : null;
    return { label: wl, vol: wd.convos.length, rt: weekRT, m: wd.cats, partial: meta.is_partial_week, volumeDriver: meta.volume_driver };
  });

  // Compute hourly patterns from filtered conversations (respects inbox + date filters)
  // Convert UTC timestamps to US Eastern Time
  const toET = (utcDate) => {
    const et = new Date(utcDate.toLocaleString("en-US", { timeZone: "America/New_York" }));
    return et.getHours();
  };
  const hourlyCounts = {};
  filteredConvos.forEach(c => {
    if (!c.created_at) return;
    const hour = toET(new Date(c.created_at));
    if (hour >= 5 && hour <= 21) {
      hourlyCounts[hour] = (hourlyCounts[hour] || 0) + 1;
    }
  });
  const hourLabel = h => h === 0 ? '12am' : h < 12 ? `${h}am` : h === 12 ? '12pm' : `${h - 12}pm`;
  const hourly = [];
  for (let h = 5; h <= 21; h++) {
    hourly.push({ h: hourLabel(h), inbound: hourlyCounts[h] || 0 });
  }

  const filteredWeekLabelSet = new Set(filteredWeekLabels);
  const filteredInsights = (dateRange.from || dateRange.to)
    ? insights.filter(i => filteredWeekLabelSet.has(i.week_label))
    : insights;

  // Split insights by severity groups
  const allSignals = filteredInsights.filter(i => i.insight_type === "signal");
  const criticalWarnings = allSignals.filter(i => i.severity === "critical" || i.severity === "warning");
  const positiveSignals = allSignals.filter(i => i.severity === "positive");
  const infoSignals = allSignals.filter(i => i.severity === "info");
  const noise = filteredInsights.filter(i => i.insight_type === "noise");
  const actions = filteredInsights.filter(i => i.insight_type === "action");

  const confMap = {};
  filteredCats.forEach(c => { confMap[c.confidence || "unknown"] = (confMap[c.confidence || "unknown"] || 0) + 1; });

  const dates = filteredConvos.map(c => c.created_at?.slice(0, 10)).filter(Boolean).sort();
  const dataDateRange = dates.length > 0 ? { from: dates[0], to: dates[dates.length - 1] } : {};

  const allNonUnassigned = conversations.filter(c => c.assigned_agent && c.assigned_agent !== "Unassigned");
  const inboxCounts = {};
  allNonUnassigned.forEach(c => {
    const ch = c.source_channel || "Unknown";
    inboxCounts[ch] = (inboxCounts[ch] || 0) + 1;
  });
  const inboxOptions = Object.entries(inboxCounts).sort((a, b) => b[1] - a[1]).map(([name, count]) => ({ name, count }));

  const totalCategorized = filteredCats.length;

  return {
    totalConversations, totalMessages, msgsPerConvo, weekCount, teamMedianRT, teamMeanRT, teamP90, teamP25,
    agentList, realAgentConvos, macroEntries, detailRows, weekly, hourly,
    allSignals, criticalWarnings, positiveSignals, infoSignals, noise, actions,
    confMap, dataDateRange, filteredConvos, inboxOptions, totalCategorized,
  };
}

// ═══ UI PRIMITIVES ═══

function Card({ children, style = {}, noPad = false }) {
  return (
    <div style={{
      background: C.white, borderRadius: 14, padding: noPad ? 0 : 24,
      border: `1px solid ${C.border}`,
      boxShadow: "0 1px 3px rgba(15,23,42,0.06), 0 1px 2px rgba(15,23,42,0.03)",
      ...style,
    }}>{children}</div>
  );
}

function StatCard({ label, value, sub, accent, icon }) {
  return (
    <Card style={{ padding: "20px 22px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <div style={{ fontSize: 11, fontWeight: 600, letterSpacing: 0.5, textTransform: "uppercase", color: C.textLight, marginBottom: 8 }}>{label}</div>
          <div style={{ fontSize: 28, fontWeight: 800, color: accent || C.text, lineHeight: 1, letterSpacing: -1 }}>{value}</div>
          {sub && <div style={{ fontSize: 12, color: C.textLight, marginTop: 8 }}>{sub}</div>}
        </div>
        {icon && <div style={{ fontSize: 20, opacity: 0.2 }}>{icon}</div>}
      </div>
    </Card>
  );
}

function Sec({ children, sub, right }) {
  return (
    <div style={{ marginBottom: 16, marginTop: 36, display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
      <div>
        <h2 style={{ fontSize: 17, fontWeight: 800, color: C.text, margin: 0, letterSpacing: -0.3 }}>{children}</h2>
        {sub && <p style={{ fontSize: 13, color: C.textLight, margin: "4px 0 0", lineHeight: 1.5 }}>{sub}</p>}
      </div>
      {right}
    </div>
  );
}

function Badge({ label, color, bg }) {
  return (
    <span style={{
      fontSize: 10, fontWeight: 800, letterSpacing: 0.5, textTransform: "uppercase",
      color: color, background: bg, borderRadius: 6, padding: "3px 8px",
      lineHeight: 1, whiteSpace: "nowrap",
    }}>{label}</span>
  );
}

function Pill({ label, active, onClick, count, color }) {
  const activeBg = color || C.navy;
  return (
    <button onClick={onClick} style={{
      padding: "6px 14px", borderRadius: 8, border: active ? "none" : `1px solid ${C.border}`,
      cursor: "pointer", fontSize: 12, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
      background: active ? activeBg : C.white,
      color: active ? "#FFF" : C.textMid,
      transition: "all 0.15s ease",
    }}>
      {label}
      {count != null && <span style={{ fontSize: 11, opacity: 0.7 }}>({count})</span>}
    </button>
  );
}

function Tip({ active, payload, label, sfx = "" }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: C.white, borderRadius: 10, padding: "12px 16px", boxShadow: "0 8px 24px rgba(15,23,42,0.12)", border: `1px solid ${C.border}`, maxWidth: 280 }}>
      <div style={{ fontSize: 12, fontWeight: 700, color: C.text, marginBottom: 6 }}>{label}</div>
      {payload.filter(p => p.value != null && p.value > 0).map((p, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12, marginBottom: 2 }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: p.color || p.fill }} />
          <span style={{ color: C.textMid }}>{p.name}:</span>
          <strong style={{ color: C.text }}>{typeof p.value === "number" ? (p.value % 1 !== 0 ? p.value.toFixed(1) : Math.round(p.value)) : p.value}{sfx}</strong>
        </div>
      ))}
    </div>
  );
}

// ═══ INSIGHT CARD ═══

function InsightCard({ insight, defaultExpanded = false }) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const sev = SEV[insight.severity] || SEV.info;
  const pri = PRI[insight.priority] || null;
  const acts = typeof insight.actions === "string" ? JSON.parse(insight.actions) : (insight.actions || []);

  return (
    <div style={{
      background: C.white,
      border: `1px solid ${sev.border}`,
      borderLeft: `4px solid ${sev.accent}`,
      borderRadius: 12,
      overflow: "hidden",
      transition: "box-shadow 0.2s ease",
    }}>
      {/* Header */}
      <div
        onClick={() => acts.length > 0 && setExpanded(!expanded)}
        style={{
          padding: "18px 22px",
          cursor: acts.length > 0 ? "pointer" : "default",
          background: expanded ? sev.bg : "transparent",
          transition: "background 0.15s ease",
        }}
      >
        <div style={{ display: "flex", gap: 14, alignItems: "flex-start" }}>
          <div style={{
            width: 36, height: 36, borderRadius: 10,
            background: sev.bg, border: `1px solid ${sev.border}`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 18, flexShrink: 0,
          }}>{insight.icon || "◆"}</div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap", marginBottom: 6 }}>
              <Badge label={sev.label} color={sev.badgeText} bg={sev.badge} />
              {pri && <Badge label={insight.priority} color={pri.color} bg={pri.bg} />}
              {acts.length > 0 && (
                <span style={{ fontSize: 11, color: C.textLight, marginLeft: "auto" }}>
                  {expanded ? "▾" : "▸"} {acts.length} action{acts.length !== 1 ? "s" : ""}
                </span>
              )}
            </div>
            <div style={{ fontSize: 14, fontWeight: 700, color: C.text, lineHeight: 1.4, marginBottom: 8 }}>
              {insight.title}
            </div>
            <div style={{ fontSize: 13, color: C.textMid, lineHeight: 1.7 }}>
              {insight.body}
            </div>
          </div>
        </div>
      </div>

      {/* Expandable actions */}
      {expanded && acts.length > 0 && (
        <div style={{
          padding: "0 22px 20px 72px",
          background: sev.bg,
          borderTop: `1px solid ${sev.border}`,
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.5, color: sev.accent, marginTop: 16, marginBottom: 12 }}>
            Recommended Actions
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {acts.map((a, i) => (
              <div key={i} style={{
                display: "flex", gap: 12, alignItems: "flex-start",
                padding: "12px 16px", background: C.white, borderRadius: 10,
                border: `1px solid ${C.border}`,
              }}>
                <div style={{
                  width: 22, height: 22, borderRadius: 6,
                  background: `${sev.accent}12`, color: sev.accent,
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 11, fontWeight: 800, flexShrink: 0,
                }}>{i + 1}</div>
                <div style={{ fontSize: 13, color: C.text, lineHeight: 1.6 }}>{a}</div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ═══ INSIGHTS SUB-COMPONENTS ═══

function CollapsibleSection({ title, count, defaultOpen = false, icon, children }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{
      overflow: "hidden", borderRadius: 16,
      background: C.white,
      border: `1px solid ${open ? C.border : C.borderLight}`,
      boxShadow: open ? "0 4px 16px rgba(15,23,42,0.06)" : "0 1px 3px rgba(15,23,42,0.04)",
      transition: "all 0.2s ease",
    }}>
      <div onClick={() => setOpen(!open)} style={{
        padding: "18px 24px", cursor: "pointer",
        display: "grid", gridTemplateColumns: "28px 1fr auto auto", alignItems: "center", gap: 0,
        background: C.white, transition: "all 0.15s ease",
        userSelect: "none", borderBottom: open ? `1px solid ${C.borderLight}` : "1px solid transparent",
      }}>
        <span style={{ fontSize: 15, width: 28, textAlign: "left" }}>{icon || ""}</span>
        <span style={{ fontSize: 14, fontWeight: 700, color: C.text, letterSpacing: -0.2, textAlign: "left" }}>{title}</span>
        {count != null ? (
          <span style={{
            fontSize: 11, fontWeight: 700, color: C.blue, background: "#EFF6FF",
            borderRadius: 20, padding: "3px 10px", lineHeight: 1, marginRight: 10,
          }}>{count}</span>
        ) : <span style={{ marginRight: 10 }} />}
        <span style={{
          fontSize: 11, color: C.textLight, transition: "transform 0.2s ease",
          transform: open ? "rotate(90deg)" : "none", opacity: 0.5,
        }}>▸</span>
      </div>
      {open && <div style={{ padding: "16px 24px 24px" }}>{children}</div>}
    </div>
  );
}

function SignalCard({ signal }) {
  const [showDetail, setShowDetail] = useState(false);
  const sev = SEV[signal.severity] || SEV.info;
  return (
    <div style={{
      background: `linear-gradient(135deg, ${C.white} 0%, ${sev.bg} 100%)`,
      border: `1px solid ${sev.border}`, borderLeft: `3px solid ${sev.accent}`,
      borderRadius: 14, overflow: "hidden",
      transition: "box-shadow 0.2s ease",
      boxShadow: "0 1px 4px rgba(15,23,42,0.04)",
    }}>
      <div style={{ padding: "18px 22px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
          <Badge label={signal.severity} color={sev.badgeText} bg={sev.badge} />
        </div>
        <div style={{ fontSize: 14, fontWeight: 700, color: C.text, marginBottom: 8, lineHeight: 1.4, letterSpacing: -0.2 }}>{signal.title}</div>
        <div style={{ fontSize: 13, color: C.textMid, lineHeight: 1.75 }}>{signal.body}</div>
        {signal.so_what && (
          <div
            onClick={() => setShowDetail(!showDetail)}
            style={{
              marginTop: 14, cursor: "pointer", fontSize: 12, fontWeight: 700,
              color: sev.accent, display: "inline-flex", alignItems: "center", gap: 5,
              padding: "6px 12px", borderRadius: 8,
              background: showDetail ? sev.bg : "transparent",
              border: `1px solid ${showDetail ? sev.border : "transparent"}`,
              transition: "all 0.15s ease",
            }}
          >
            <span style={{ transform: showDetail ? "rotate(90deg)" : "none", transition: "transform 0.15s ease", display: "inline-block", fontSize: 10 }}>▸</span>
            So what?
          </div>
        )}
        {showDetail && signal.so_what && (
          <div style={{
            marginTop: 10, padding: "14px 18px", background: C.white, borderRadius: 10,
            fontSize: 13, color: C.text, lineHeight: 1.75, border: `1px solid ${sev.border}`,
          }}>{signal.so_what}</div>
        )}
      </div>
    </div>
  );
}

function FrictionRow({ family, contacts, summary }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{
      border: `1px solid ${open ? "#FECACA" : C.borderLight}`, borderRadius: 12, overflow: "hidden",
      transition: "all 0.15s ease", background: C.white,
    }}>
      <div onClick={() => setOpen(!open)} style={{
        padding: "13px 18px", cursor: "pointer", display: "flex", alignItems: "center", gap: 12,
        transition: "background 0.15s ease",
      }}>
        <span style={{
          fontSize: 11, fontWeight: 800, color: "#DC2626", background: "#FEF2F2",
          borderRadius: 20, padding: "3px 10px", minWidth: 28, textAlign: "center", lineHeight: 1,
        }}>{contacts}x</span>
        <span style={{ fontSize: 13, fontWeight: 600, color: C.text, flex: 1, letterSpacing: -0.1 }}>{family}</span>
        <span style={{ fontSize: 10, color: C.textLight, transform: open ? "rotate(90deg)" : "none", transition: "transform 0.15s ease", opacity: 0.4 }}>▸</span>
      </div>
      {open && (
        <div style={{
          padding: "0 18px 16px", fontSize: 13, color: C.textMid, lineHeight: 1.75,
          borderTop: `1px solid ${C.borderLight}`, marginTop: 0, paddingTop: 14,
        }}>{summary}</div>
      )}
    </div>
  );
}

// ═══ INSIGHTS TAB ═══

function InsightsTab({ data, weeklyReports = [], monthlyReports = [] }) {
  const parseWeekDate = s => { const d = new Date(s + ", 2026"); return isNaN(d) ? 0 : d.getTime(); };

  // Build sorted week list from conversations + reports
  const weekLabels = [...new Set([
    ...data.filteredConvos.map(c => c.week_label).filter(Boolean),
    ...weeklyReports.map(r => r.week_label).filter(Boolean),
  ])].sort((a, b) => parseWeekDate(a) - parseWeekDate(b));

  const latestWeek = weekLabels.length > 0 ? weekLabels[weekLabels.length - 1] : null;

  const [selectedWeek, setSelectedWeek] = useState(latestWeek);
  const [view, setView] = useState("weekly");

  // Build month groups from weeks
  const monthGroups = {};
  weekLabels.forEach(wl => {
    const d = new Date(wl + ", 2026");
    if (!isNaN(d)) {
      const monthKey = d.toLocaleString("default", { month: "long", year: "numeric" });
      if (!monthGroups[monthKey]) monthGroups[monthKey] = [];
      monthGroups[monthKey].push(wl);
    }
  });
  const monthKeys = Object.keys(monthGroups);
  const latestMonth = monthKeys.length > 0 ? monthKeys[monthKeys.length - 1] : null;
  const [selectedMonth, setSelectedMonth] = useState(latestMonth);

  // Find report for selected week
  const report = weeklyReports.find(r => r.week_label === selectedWeek);
  const parseJSON = v => { try { return typeof v === "string" ? JSON.parse(v) : (v || []); } catch { return []; } };

  return (
    <>
      {/* View toggle */}
      <div style={{
        display: "flex", gap: 4, marginBottom: 28, marginTop: 8,
        background: C.borderLight, borderRadius: 12, padding: 4, width: "fit-content",
      }}>
        {[{ key: "weekly", label: "Weekly" }, { key: "monthly", label: "Monthly" }].map(v => (
          <button key={v.key} onClick={() => setView(v.key)} style={{
            border: "none", cursor: "pointer",
            padding: "8px 22px", borderRadius: 9, fontSize: 13, fontWeight: 600,
            background: view === v.key ? C.white : "transparent",
            color: view === v.key ? C.navy : C.textLight,
            boxShadow: view === v.key ? "0 2px 8px rgba(0,0,0,0.08)" : "none",
            transition: "all 0.15s ease", letterSpacing: -0.1,
          }}>{v.label}</button>
        ))}
      </div>

      {view === "weekly" && (
        <>
          {/* Week selector pills */}
          {weekLabels.length > 0 && (
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 24 }}>
              {weekLabels.map(wl => {
                const hasReport = weeklyReports.some(r => r.week_label === wl);
                const isSelected = selectedWeek === wl;
                return (
                  <button key={wl} onClick={() => setSelectedWeek(wl)} style={{
                    border: isSelected ? "none" : `1px solid ${C.borderLight}`,
                    background: isSelected ? C.navy : C.white,
                    color: isSelected ? "white" : C.textMid,
                    borderRadius: 10, padding: "9px 18px", fontSize: 13, fontWeight: 600,
                    cursor: "pointer", transition: "all 0.15s ease",
                    position: "relative", letterSpacing: -0.1,
                    boxShadow: isSelected ? "0 2px 10px rgba(10,22,40,0.2)" : "0 1px 3px rgba(0,0,0,0.04)",
                  }}>
                    Week of <span style={{ fontWeight: 700 }}>{wl}</span>
                    {hasReport && <span style={{
                      position: "absolute", top: -3, right: -3, width: 8, height: 8,
                      borderRadius: "50%", background: C.green, border: `2px solid ${isSelected ? C.navy : C.white}`,
                      boxShadow: "0 1px 3px rgba(34,197,94,0.4)",
                    }} />}
                  </button>
                );
              })}
            </div>
          )}

          {/* Report content or empty state */}
          {report ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
              {/* ── Executive Summary ── */}
              <div style={{
                borderRadius: 18, overflow: "hidden",
                background: `linear-gradient(145deg, #0A1628 0%, #162037 50%, #1E293B 100%)`,
                padding: "28px 28px 24px", color: "white",
                boxShadow: "0 8px 32px rgba(10,22,40,0.25)",
              }}>
                {report.week_type && (
                  <div style={{ marginBottom: 14 }}>
                    <span style={{
                      fontSize: 10, fontWeight: 800, letterSpacing: 1, textTransform: "uppercase",
                      color: "#93C5FD", background: "rgba(59,130,246,0.15)",
                      borderRadius: 6, padding: "4px 10px", border: "1px solid rgba(59,130,246,0.2)",
                    }}>{report.week_type}</span>
                  </div>
                )}
                <div style={{ fontSize: 14, color: "rgba(255,255,255,0.8)", lineHeight: 1.8, letterSpacing: -0.1 }}>
                  {report.exec_summary}
                </div>
              </div>
              {/* Stat grid */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 12 }}>
                {parseJSON(report.exec_stats).map((s, i) => (
                  <div key={i} style={{
                    background: C.white, border: `1px solid ${C.borderLight}`, borderRadius: 14, padding: "16px 18px",
                    boxShadow: "0 1px 4px rgba(15,23,42,0.04)",
                    transition: "box-shadow 0.15s ease",
                  }}>
                    <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.8, color: C.textLight, marginBottom: 6 }}>{s.label}</div>
                    <div style={{ fontSize: 24, fontWeight: 800, color: C.navy, letterSpacing: -0.5, lineHeight: 1 }}>{s.value}</div>
                    {s.detail && <div style={{ fontSize: 11, color: C.textLight, marginTop: 6, lineHeight: 1.4 }}>{s.detail}</div>}
                  </div>
                ))}
              </div>

              {/* ── Signals ── */}
              {parseJSON(report.signals).length > 0 && (
                <CollapsibleSection title="Signals" count={parseJSON(report.signals).length} icon="⚡" defaultOpen={true}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 8 }}>
                    {parseJSON(report.signals).map((s, i) => <SignalCard key={i} signal={s} />)}
                  </div>
                </CollapsibleSection>
              )}

              {/* ── Noise ── */}
              {parseJSON(report.noise).length > 0 && (
                <CollapsibleSection title="Noise" count={parseJSON(report.noise).length} icon="〰">
                  <div style={{ fontSize: 11, color: C.textLight, marginBottom: 12, marginTop: 2, fontStyle: "italic" }}>Volume that looks concerning but isn't.</div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                    {parseJSON(report.noise).map((n, i) => (
                      <div key={i} style={{
                        padding: "13px 18px", background: "#FAFBFC", borderRadius: 12,
                        border: `1px solid ${C.borderLight}`,
                      }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: C.text }}>{n.title}</span>
                        <div style={{ fontSize: 12.5, color: C.textMid, marginTop: 4, lineHeight: 1.6 }}>{n.body}</div>
                      </div>
                    ))}
                  </div>
                </CollapsibleSection>
              )}

              {/* ── Friction Map ── */}
              {(parseJSON(report.friction_high).length > 0 || parseJSON(report.friction_repeats).length > 0) && (
                <CollapsibleSection
                  title="Friction Map"
                  count={parseJSON(report.friction_high).length + parseJSON(report.friction_repeats).length}
                  icon="🔥"
                >
                  {parseJSON(report.friction_high).length > 0 && (
                    <>
                      <div style={{
                        fontSize: 11, fontWeight: 700, color: C.red, textTransform: "uppercase",
                        letterSpacing: 0.8, marginTop: 4, marginBottom: 10,
                        display: "flex", alignItems: "center", gap: 8,
                      }}>
                        <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.red, display: "inline-block" }} />
                        High-Friction Families (3+ contacts)
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
                        {parseJSON(report.friction_high).map((f, i) => (
                          <FrictionRow key={i} family={f.family} contacts={f.contacts} summary={f.summary} />
                        ))}
                      </div>
                    </>
                  )}
                  {parseJSON(report.friction_repeats).length > 0 && (
                    <>
                      <div style={{
                        fontSize: 11, fontWeight: 700, color: C.orange, textTransform: "uppercase",
                        letterSpacing: 0.8, marginBottom: 10,
                        display: "flex", alignItems: "center", gap: 8,
                      }}>
                        <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.orange, display: "inline-block" }} />
                        Repeat Contacts (2x)
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
                        {parseJSON(report.friction_repeats).map((f, i) => (
                          <FrictionRow key={i} family={f.family} contacts={f.contacts} summary={f.issue} />
                        ))}
                      </div>
                    </>
                  )}
                  {parseJSON(report.friction_prospects).length > 0 && (
                    <>
                      <div style={{
                        fontSize: 11, fontWeight: 700, color: C.yellowDark, textTransform: "uppercase",
                        letterSpacing: 0.8, marginBottom: 10,
                        display: "flex", alignItems: "center", gap: 8,
                      }}>
                        <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.yellow, display: "inline-block" }} />
                        Prospect Friction
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                        {parseJSON(report.friction_prospects).map((f, i) => (
                          <div key={i} style={{
                            display: "flex", alignItems: "center", gap: 12, padding: "10px 16px",
                            background: "#FFFBEB", borderRadius: 10, border: `1px solid #FDE68A`,
                          }}>
                            <span style={{
                              fontSize: 11, fontWeight: 800, color: C.yellowDark,
                              background: "#FEF3C7", borderRadius: 20, padding: "2px 10px", minWidth: 30, textAlign: "center",
                            }}>{f.count}x</span>
                            <span style={{ fontSize: 13, color: C.text, lineHeight: 1.5 }}>{f.issue}</span>
                          </div>
                        ))}
                      </div>
                    </>
                  )}
                </CollapsibleSection>
              )}

              {/* ── Front Door Health ── */}
              {report.front_door_assessment && (
                <CollapsibleSection title="Front Door Health" icon="🚪">
                  {/* Stats row */}
                  {parseJSON(report.front_door_stats).length > 0 && (
                    <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 4, marginBottom: 16 }}>
                      {parseJSON(report.front_door_stats).map((s, i) => (
                        <div key={i} style={{
                          background: "linear-gradient(135deg, #EFF6FF 0%, #DBEAFE 100%)",
                          border: `1px solid #BFDBFE`, borderRadius: 12, padding: "10px 16px",
                        }}>
                          <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.8, color: C.textLight }}>{s.label}</div>
                          <div style={{ fontSize: 20, fontWeight: 800, color: C.navy, letterSpacing: -0.3 }}>{s.value}</div>
                        </div>
                      ))}
                    </div>
                  )}
                  {/* Assessment */}
                  <div style={{
                    padding: "16px 20px", background: "#FAFBFC", borderRadius: 12,
                    border: `1px solid ${C.borderLight}`, fontSize: 13, color: C.text, lineHeight: 1.75, marginBottom: 16,
                  }}>{report.front_door_assessment}</div>
                  {/* Friction points */}
                  {parseJSON(report.front_door_friction).length > 0 && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                      {parseJSON(report.front_door_friction).map((f, i) => (
                        <div key={i} style={{
                          padding: "14px 18px", border: `1px solid ${C.borderLight}`, borderRadius: 12,
                        }}>
                          <div style={{ fontSize: 12, fontWeight: 700, color: C.navy, marginBottom: 5, letterSpacing: -0.1 }}>{f.category}</div>
                          <div style={{ fontSize: 13, color: C.textMid, lineHeight: 1.7 }}>{f.details}</div>
                        </div>
                      ))}
                    </div>
                  )}
                </CollapsibleSection>
              )}

              {/* ── Volume Note ── */}
              {report.volume_note && (
                <CollapsibleSection title="Volume Note" icon="📊">
                  <div style={{ fontSize: 13, color: C.textMid, lineHeight: 1.75, marginTop: 4 }}>{report.volume_note}</div>
                  {parseJSON(report.channel_split).length > 0 && (
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 14 }}>
                      {parseJSON(report.channel_split).map((ch, i) => (
                        <div key={i} style={{
                          padding: "7px 14px", background: "#F8FAFC", borderRadius: 10,
                          fontSize: 12, fontWeight: 600, color: C.textMid,
                          border: `1px solid ${C.borderLight}`,
                        }}>
                          <span style={{ color: C.text, fontWeight: 700 }}>{ch.channel}</span>
                          <span style={{ color: C.textLight, margin: "0 6px" }}>·</span>
                          {ch.count} <span style={{ color: C.textLight }}>({ch.pct}%)</span>
                        </div>
                      ))}
                    </div>
                  )}
                </CollapsibleSection>
              )}
            </div>
          ) : (
            <Card style={{ padding: 48, textAlign: "center" }}>
              <div style={{ fontSize: 32, marginBottom: 12, opacity: 0.3 }}>◆</div>
              <div style={{ fontSize: 15, fontWeight: 700, color: C.textMid, marginBottom: 6 }}>No report for this week yet</div>
              <div style={{ fontSize: 13, color: C.textLight, maxWidth: 400, margin: "0 auto", lineHeight: 1.6 }}>
                Weekly insights for the week of {selectedWeek || "—"} will be added when available.
              </div>
            </Card>
          )}
        </>
      )}

      {view === "monthly" && (() => {
        const mLabels = [...new Set([
          ...monthKeys,
          ...monthlyReports.map(r => r.month_label).filter(Boolean),
        ])].sort((a, b) => new Date(a) - new Date(b));
        const mReport = monthlyReports.find(r => r.month_label === selectedMonth);
        return (
        <>
          {mLabels.length > 0 && (
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 20 }}>
              {mLabels.map(mk => {
                const hasReport = monthlyReports.some(r => r.month_label === mk);
                return (
                <button key={mk} onClick={() => setSelectedMonth(mk)} style={{
                  border: `1px solid ${selectedMonth === mk ? C.navy : C.border}`,
                  background: selectedMonth === mk ? C.navy : C.white,
                  color: selectedMonth === mk ? "white" : C.textMid,
                  borderRadius: 8, padding: "8px 16px", fontSize: 13, fontWeight: 600,
                  cursor: "pointer", transition: "all 0.15s ease", position: "relative",
                }}>
                  {mk}
                  {hasReport && <span style={{
                    position: "absolute", top: -3, right: -3, width: 8, height: 8,
                    borderRadius: "50%", background: C.green, border: `2px solid ${selectedMonth === mk ? C.navy : C.white}`,
                    boxShadow: "0 1px 3px rgba(34,197,94,0.4)",
                  }} />}
                </button>
                );
              })}
            </div>
          )}

          {mReport ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
              {/* ── Executive Summary ── */}
              <div style={{
                borderRadius: 18, overflow: "hidden",
                background: `linear-gradient(145deg, #0A1628 0%, #162037 50%, #1E293B 100%)`,
                padding: "28px 28px 24px", color: "white",
                boxShadow: "0 8px 32px rgba(10,22,40,0.25)",
              }}>
                {mReport.month_type && (
                  <div style={{ marginBottom: 14 }}>
                    <span style={{
                      fontSize: 10, fontWeight: 800, letterSpacing: 1, textTransform: "uppercase",
                      color: "#93C5FD", background: "rgba(59,130,246,0.15)",
                      borderRadius: 6, padding: "4px 10px", border: "1px solid rgba(59,130,246,0.2)",
                    }}>{mReport.month_type}</span>
                  </div>
                )}
                <div style={{ fontSize: 14, color: "rgba(255,255,255,0.8)", lineHeight: 1.8, letterSpacing: -0.1 }}>
                  {mReport.exec_summary}
                </div>
              </div>

              {/* ── Stat grid ── */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
                {parseJSON(mReport.exec_stats).map((s, i) => (
                  <div key={i} style={{
                    background: C.white, border: `1px solid ${C.borderLight}`, borderRadius: 14, padding: "16px 18px",
                    boxShadow: "0 1px 4px rgba(15,23,42,0.04)",
                  }}>
                    <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.8, color: C.textLight, marginBottom: 6 }}>{s.label}</div>
                    <div style={{ fontSize: 24, fontWeight: 800, color: C.navy, letterSpacing: -0.5, lineHeight: 1 }}>{s.value}</div>
                    {s.detail && <div style={{ fontSize: 11, color: C.textLight, marginTop: 6, lineHeight: 1.4 }}>{s.detail}</div>}
                  </div>
                ))}
              </div>

              {/* ── Key Trends ── */}
              {parseJSON(mReport.key_trends).length > 0 && (
                <CollapsibleSection title="Key Trends" count={parseJSON(mReport.key_trends).length} icon="📈" defaultOpen={true}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 8 }}>
                    {parseJSON(mReport.key_trends).map((t, i) => (
                      <div key={i} style={{
                        padding: "16px 20px", background: "#FAFBFC", borderRadius: 14,
                        border: `1px solid ${C.borderLight}`,
                      }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
                          <span style={{ fontSize: 16 }}>{t.icon || "→"}</span>
                          <span style={{ fontSize: 14, fontWeight: 700, color: C.text }}>{t.title}</span>
                        </div>
                        <div style={{ fontSize: 13, color: C.textMid, lineHeight: 1.7 }}>{t.body}</div>
                      </div>
                    ))}
                  </div>
                </CollapsibleSection>
              )}

              {/* ── Signals ── */}
              {parseJSON(mReport.signals).length > 0 && (
                <CollapsibleSection title="Signals" count={parseJSON(mReport.signals).length} icon="⚡" defaultOpen={true}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 8 }}>
                    {parseJSON(mReport.signals).map((s, i) => <SignalCard key={i} signal={s} />)}
                  </div>
                </CollapsibleSection>
              )}

              {/* ── Outcomes ── */}
              {parseJSON(mReport.outcomes).length > 0 && (
                <CollapsibleSection title="Outcomes" count={parseJSON(mReport.outcomes).length} icon="📋">
                  <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 8 }}>
                    {parseJSON(mReport.outcomes).map((o, i) => (
                      <div key={i} style={{
                        padding: "14px 18px", background: "#FAFBFC", borderRadius: 12,
                        border: `1px solid ${C.borderLight}`,
                      }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: C.text }}>{o.title}</span>
                        <div style={{ fontSize: 12.5, color: C.textMid, marginTop: 4, lineHeight: 1.6 }}>{o.body}</div>
                      </div>
                    ))}
                  </div>
                </CollapsibleSection>
              )}

              {/* ── Friction Patterns ── */}
              {parseJSON(mReport.friction_patterns).length > 0 && (
                <CollapsibleSection title="Friction Patterns" count={parseJSON(mReport.friction_patterns).length} icon="🔥">
                  <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 8 }}>
                    {parseJSON(mReport.friction_patterns).map((f, i) => (
                      <div key={i} style={{
                        padding: "14px 18px", borderRadius: 12,
                        border: `1px solid ${C.borderLight}`,
                      }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: C.text }}>{f.title}</span>
                        <div style={{ fontSize: 12.5, color: C.textMid, marginTop: 4, lineHeight: 1.6 }}>{f.body}</div>
                      </div>
                    ))}
                  </div>
                </CollapsibleSection>
              )}

              {/* ── Front Door Health ── */}
              {mReport.front_door && (() => {
                const fd = parseJSON(mReport.front_door);
                return (
                <CollapsibleSection title="Front Door Health" icon="🚪">
                  {(fd.stats || []).length > 0 && (
                    <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 4, marginBottom: 16 }}>
                      {fd.stats.map((s, i) => (
                        <div key={i} style={{
                          background: "linear-gradient(135deg, #EFF6FF 0%, #DBEAFE 100%)",
                          border: `1px solid #BFDBFE`, borderRadius: 12, padding: "10px 16px",
                        }}>
                          <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.8, color: C.textLight }}>{s.label}</div>
                          <div style={{ fontSize: 20, fontWeight: 800, color: C.navy, letterSpacing: -0.3 }}>{s.value}</div>
                        </div>
                      ))}
                    </div>
                  )}
                  {fd.assessment && (
                    <div style={{
                      padding: "16px 20px", background: "#FAFBFC", borderRadius: 12,
                      border: `1px solid ${C.borderLight}`, fontSize: 13, color: C.text, lineHeight: 1.75, marginBottom: 16,
                    }}>{fd.assessment}</div>
                  )}
                  {(fd.friction || []).length > 0 && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                      {fd.friction.map((f, i) => (
                        <div key={i} style={{
                          padding: "14px 18px", border: `1px solid ${C.borderLight}`, borderRadius: 12,
                        }}>
                          <div style={{ fontSize: 12, fontWeight: 700, color: C.navy, marginBottom: 5 }}>{f.category}</div>
                          <div style={{ fontSize: 13, color: C.textMid, lineHeight: 1.7 }}>{f.details}</div>
                        </div>
                      ))}
                    </div>
                  )}
                </CollapsibleSection>
                );
              })()}

              {/* ── Looking Ahead ── */}
              {mReport.looking_ahead && (
                <CollapsibleSection title="Looking Ahead" icon="🔭">
                  <div style={{ fontSize: 13, color: C.textMid, lineHeight: 1.75, marginTop: 4 }}>{mReport.looking_ahead}</div>
                </CollapsibleSection>
              )}
            </div>
          ) : (
            <Card style={{ padding: 48, textAlign: "center" }}>
              <div style={{ fontSize: 32, marginBottom: 12, opacity: 0.3 }}>◈</div>
              <div style={{ fontSize: 15, fontWeight: 700, color: C.textMid, marginBottom: 6 }}>No report for this month yet</div>
              <div style={{ fontSize: 13, color: C.textLight, maxWidth: 400, margin: "0 auto", lineHeight: 1.6 }}>
                A consolidated monthly view for {selectedMonth || "—"} covering trends, patterns, and recommendations across all weeks.
              </div>
            </Card>
          )}
        </>
        );
      })()}
    </>
  );
}

// ═══ OVERVIEW TAB ═══

function OverviewTab({ data, avgWeeklyVol, volRtData }) {
  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 14, marginBottom: 28, marginTop: 8 }}>
        <StatCard label="Conversations" value={data.totalConversations} sub={`${avgWeeklyVol}/week avg`} icon="💬" />
        <StatCard label="Team Median RT" value={data.teamMedianRT != null ? `${data.teamMedianRT}m` : "—"} sub="business hours (6a-6p ET)" accent={data.teamMedianRT && data.teamMedianRT < 120 ? C.greenDark : C.orange} icon="⏱" />
        <StatCard label="Messages" value={data.totalMessages.toLocaleString()} sub={`${data.msgsPerConvo} per convo`} icon="✉" />
        <StatCard label="Categorized" value={data.totalCategorized} sub={`${data.confMap.high || 0} high confidence`} icon="🏷" />
        <StatCard label="Team" value={data.agentList.length} sub={`${data.realAgentConvos} convos handled`} icon="👥" />
      </div>


      {/* Volume trend */}
      {volRtData.length > 1 && (
        <>
          <Sec sub="Weekly volume and response time trend">Volume Trend</Sec>
          <Card>
            <ResponsiveContainer width="100%" height={260}>
              <ComposedChart data={volRtData}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="name" tick={{ fontSize: 11, fill: C.textLight }} />
                <YAxis yAxisId="vol" tick={{ fontSize: 11, fill: C.textLight }} />
                <YAxis yAxisId="rt" orientation="right" tick={{ fontSize: 11, fill: C.textLight }} unit="m" />
                <Tooltip content={<Tip />} />
                <Bar yAxisId="vol" dataKey="volume" fill={C.blue} radius={[6, 6, 0, 0]} name="Conversations" fillOpacity={0.75} />
                <Line yAxisId="rt" type="monotone" dataKey="medianRT" stroke={C.red} strokeWidth={2.5} dot={{ r: 4, fill: C.red, strokeWidth: 2, stroke: C.white }} name="Median RT (min)" connectNulls />
                <ReferenceLine yAxisId="vol" y={avgWeeklyVol} stroke={C.yellow} strokeDasharray="6 4" strokeWidth={1.5} />
              </ComposedChart>
            </ResponsiveContainer>
          </Card>
        </>
      )}

      {/* Category donut */}
      <Sec sub="Distribution across operational domains">Support by Category</Sec>
      <Card>
        <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
          <div style={{ flex: "1 1 280px" }}>
            <ResponsiveContainer width="100%" height={260}>
              <PieChart>
                <Pie data={data.macroEntries.map(([n, v]) => ({ name: n, value: v }))} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={105} innerRadius={55} paddingAngle={2} strokeWidth={0}>
                  {data.macroEntries.map(([n], i) => <Cell key={i} fill={MC[n] || C.textLight} />)}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div style={{ flex: "1 1 260px", display: "flex", flexDirection: "column", justifyContent: "center", gap: 4 }}>
            {data.macroEntries.map(([n, v]) => (
              <div key={n} style={{ display: "flex", alignItems: "center", gap: 10, padding: "4px 0" }}>
                <div style={{ width: 10, height: 10, borderRadius: 3, background: MC[n] || C.textLight, flexShrink: 0 }} />
                <div style={{ flex: 1, fontSize: 13, fontWeight: 500, color: C.text }}>{n}</div>
                <div style={{ fontSize: 12, color: C.textLight, width: 36, textAlign: "right" }}>{data.totalConversations > 0 ? Math.round(v / data.totalConversations * 100) : 0}%</div>
                <div style={{ fontSize: 14, fontWeight: 700, color: C.text, width: 32, textAlign: "right" }}>{v}</div>
              </div>
            ))}
          </div>
        </div>
      </Card>
    </>
  );
}

// ═══ CATEGORIES TAB ═══

function CategoriesTab({ data, weekly, TOP4 }) {
  const trendData = useMemo(() => weekly.filter(w => w.vol > 3).map(w => {
    const weekTotal = Object.values(w.m || {}).reduce((a, b) => a + b, 0);
    const row = { week: w.label };
    TOP4.forEach(cat => { row[cat] = weekTotal > 0 ? Math.round((w.m[cat] || 0) / weekTotal * 1000) / 10 : 0; });
    return row;
  }), [weekly, TOP4]);

  return (
    <>
      <Sec sub="All issue categories ranked by volume">All Categories</Sec>
      <Card>
        {data.detailRows.length > 0 ? data.detailRows.map((row) => (
          <div key={row.name} style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 6, padding: "3px 0" }}>
            <div style={{ width: 10, height: 10, borderRadius: 3, background: MC[row.macro] || C.textLight, flexShrink: 0 }} />
            <div style={{ width: 200, fontSize: 13, fontWeight: 500, flexShrink: 0, color: C.text }}>{row.name}</div>
            <div style={{ flex: 1, height: 8, background: C.borderLight, borderRadius: 4, overflow: "hidden" }}>
              <div style={{ height: "100%", borderRadius: 4, background: MC[row.macro] || C.textLight, width: `${(row.count / data.detailRows[0].count) * 100}%`, transition: "width 0.3s" }} />
            </div>
            <div style={{ width: 36, textAlign: "right", fontSize: 13, fontWeight: 700, flexShrink: 0, color: C.text }}>{row.count}</div>
          </div>
        )) : <div style={{ fontSize: 14, color: C.textLight, padding: 24, textAlign: "center" }}>No categorizations yet</div>}
      </Card>

      {trendData.length > 1 && TOP4.length > 0 && (
        <>
          <Sec sub="Top 4 categories as share of weekly total">Category Mix Over Time</Sec>
          <Card>
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={trendData}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="week" tick={{ fontSize: 11, fill: C.textLight }} />
                <YAxis tick={{ fontSize: 11, fill: C.textLight }} unit="%" domain={[0, "auto"]} />
                <Tooltip content={<Tip sfx="%" />} />
                {TOP4.map(k => (
                  <Area key={k} type="monotone" dataKey={k} stroke={MC[k] || C.textLight} fill={MC[k] || C.textLight} fillOpacity={0.12} strokeWidth={2.5} />
                ))}
              </AreaChart>
            </ResponsiveContainer>
            <div style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}>
              {TOP4.map(k => (
                <div key={k} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <div style={{ width: 12, height: 3, borderRadius: 2, background: MC[k] || C.textLight }} />
                  <span style={{ fontSize: 12, color: C.textMid }}>{k}</span>
                </div>
              ))}
            </div>
          </Card>
        </>
      )}
    </>
  );
}

// ═══ VOLUME TAB ═══

function VolumeTab({ data, avgWeeklyVol, volRtData }) {
  return (
    <>
      {volRtData.length > 0 && (
        <>
          <Sec sub="Weekly conversation volume (bars) vs median response time (line)">Volume vs. Response Time</Sec>
          <Card>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={volRtData}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="name" tick={{ fontSize: 11, fill: C.textLight }} />
                <YAxis yAxisId="vol" tick={{ fontSize: 11, fill: C.textLight }} />
                <YAxis yAxisId="rt" orientation="right" tick={{ fontSize: 11, fill: C.textLight }} unit="m" />
                <Tooltip content={<Tip />} />
                <Bar yAxisId="vol" dataKey="volume" fill={C.blue} radius={[6, 6, 0, 0]} name="Conversations" fillOpacity={0.75} />
                <Line yAxisId="rt" type="monotone" dataKey="medianRT" stroke={C.red} strokeWidth={2.5} dot={{ r: 4, fill: C.red, strokeWidth: 2, stroke: C.white }} name="Median RT (min)" connectNulls />
                <ReferenceLine yAxisId="vol" y={avgWeeklyVol} stroke={C.yellow} strokeDasharray="6 4" strokeWidth={1.5} />
              </ComposedChart>
            </ResponsiveContainer>
          </Card>
        </>
      )}

      {WEEKLY_DRIVERS.length > 0 && (
        <>
          <Sec sub="What drove conversation volume each week">Weekly Volume Drivers</Sec>
          <Card>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {WEEKLY_DRIVERS.map((d, i) => (
                <WeeklyDriverRow key={i} driver={d} />
              ))}
            </div>
          </Card>
        </>
      )}

      {data.hourly.length > 0 && (
        <>
          <Sec sub="When parents reach out (by hour)">Inbound by Hour</Sec>
          <Card>
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={data.hourly}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="h" tick={{ fontSize: 11, fill: C.textLight }} />
                <YAxis tick={{ fontSize: 11, fill: C.textLight }} />
                <Tooltip content={<Tip />} />
                <Area dataKey="inbound" stroke={C.blue} fill={C.blue} fillOpacity={0.06} strokeWidth={2.5} type="monotone" name="Inbound" />
              </AreaChart>
            </ResponsiveContainer>
          </Card>
        </>
      )}
    </>
  );
}

// ═══ TEAM TAB ═══

const WEEKLY_DRIVERS = [
  {
    week: "Feb 16", convos: 157, cancellations: 2,
    oneLiner: "New cohort launch + MAP Growth results arriving",
    highlights: [
      { label: "Login issues", value: "55 convos", note: "highest of any week" },
      { label: "Onboarding", value: "13 convos" },
      { label: "Bracketing", value: "28 mentions", note: "new families testing" },
      { label: "MAP Growth", value: "17 convos", note: "existing customers checking scores" },
      { label: "MAP Screener", value: "17 convos", note: "13 from enrolling families — not yet crisis" },
      { label: "Apps & Software", value: "22 convos", note: "IXL (18), StudyReel (21), Lalilo" },
    ],
    summary: "A typical \"getting started\" week with predictable new-cohort volume.",
  },
  {
    week: "Feb 23", convos: 151, cancellations: 2,
    oneLiner: "Quiet week, but platform friction building underneath",
    highlights: [
      { label: "MAP Screener", value: "19 convos", note: "up from 17 — 15 from enrolling families" },
      { label: "Platform & Technical", value: "20 convos", note: "up from 12 prior week" },
      { label: "Dash & Portal", value: "10 convos" },
      { label: "Outages & Downtime", value: "5 convos", note: "emerging" },
      { label: "StudyReel", value: "24 mentions", note: "still high" },
    ],
    summary: "The screener and platform problems that would explode in Mar 2 were already accumulating here.",
  },
  {
    week: "Mar 2", convos: 194, cancellations: 7,
    oneLiner: "Funnel opened + MAP Screener technical crisis — peak volume",
    highlights: [
      { label: "Prospect volume", value: "+41%", note: "61 → 86 prospects" },
      { label: "MAP Screener", value: "33 convos", note: "26 from enrolling families — 30% of prospect convos" },
      { label: "Login issues", value: "44 convos" },
      { label: "MAP test mentions", value: "41 convos" },
      { label: "Cancellations", value: "7", note: "highest week — refund mentioned in 8 convos" },
      { label: "Enrollment demand", value: "ongoing", note: "16 onboarding, 9 inquiries, pricing in 9 convos" },
    ],
    summary: "Team handled 194 conversations across 3 people while the core technical problem was outside their control.",
  },
  {
    week: "Mar 9", convos: 157, cancellations: 4,
    oneLiner: "Screener crisis resolved, enrollment converting, student channel inflecting",
    highlights: [
      { label: "MAP Screener", value: "16 convos", note: "down from 33 — fix confirmed" },
      { label: "Prospect volume", value: "40", note: "down from 86 — funnel wave passed" },
      { label: "Enrollment & Onboarding", value: "28 convos", note: "#1 category for the first time" },
      { label: "New inquiries", value: "11", note: "4-week high — Mar 2 prospects converting" },
      { label: "Student channel", value: "18+ convos", note: "23+ including misrouted emails" },
      { label: "Positive Feedback", value: "9", note: "highest of any week" },
    ],
    summary: "A stabilization week with healthy leading indicators.",
  },
  {
    week: "Mar 16", convos: 153, cancellations: 6,
    oneLiner: "Lauren Cole churned + ClassBank upgrade confusion + 6 cancellations",
    highlights: [
      { label: "Cancellations", value: "6", note: "3 from churn analysis (Cole, Godfrey, Christiansen)" },
      { label: "Account & Billing", value: "14 convos", note: "highest ever — cancellations + refunds" },
      { label: "Rewards", value: "10 convos", note: "ClassBank upgrade confusion spike" },
      { label: "Student contacts", value: "16", note: "all-time high — students navigating independently" },
      { label: "Refund requests", value: "2", note: "under 45-day guarantee (Hamaoui, Talbot)" },
      { label: "YouTube concern", value: "2nd family", note: "Pellegrino proactively blocks YouTube" },
    ],
    summary: "Cancellation week — 3 churn analysis predictions materialized. ClassBank upgrade email created cross-cutting confusion.",
  },
  {
    week: "Mar 23", convos: 163, cancellations: 6,
    oneLiner: "Highest cancellation week + YouTube escalation + pre-launch pipeline",
    highlights: [
      { label: "Cancellations", value: "6", note: "Thannisch (3 boys), Benitt (bracketing), Lovell, Goodman, Cohen, Fett" },
      { label: "Enrollment", value: "25 convos", note: "up from 17 — Mar 30 pre-launch" },
      { label: "Open rate", value: "32%", note: "43 of 135 still open — highest of any week" },
      { label: "YouTube concern", value: "3rd family", note: "Meyer — children wasting time, family in crisis" },
      { label: "International", value: "4 countries", note: "Australia, Mexico, Hong Kong, UK" },
      { label: "Pricing gap", value: "5 convos", note: "families replying to marketing can't find cost" },
    ],
    summary: "The YouTube-in-StudyReel concern peaked with a third family. Cancellations confirmed bracketing and Dash trust patterns from earlier weeks.",
  },
];

function WeeklyDriverRow({ driver }) {
  const [open, setOpen] = useState(false);
  const peakWeek = driver.convos >= 190;
  return (
    <div style={{
      background: C.borderLight, borderRadius: 12, overflow: "hidden",
      border: peakWeek ? `1px solid ${C.orange}40` : "1px solid transparent",
    }}>
      <div
        onClick={() => setOpen(!open)}
        style={{
          padding: "14px 18px", cursor: "pointer", userSelect: "none",
          display: "flex", gap: 14, alignItems: "center",
          transition: "background 0.15s ease",
          ...(open ? { background: `${C.blue}08` } : {}),
        }}
      >
        <div style={{ fontSize: 13, fontWeight: 800, color: C.blue, minWidth: 56, flexShrink: 0 }}>{driver.week}</div>
        <div style={{
          fontSize: 12, fontWeight: 800, color: C.text, minWidth: 56, flexShrink: 0,
          background: peakWeek ? `${C.orange}18` : `${C.blue}10`,
          borderRadius: 6, padding: "3px 8px", textAlign: "center",
        }}>{driver.convos}</div>
        <div style={{ flex: 1, fontSize: 13, color: C.textMid, lineHeight: 1.4 }}>
          {driver.oneLiner}
        </div>
        <span style={{
          fontSize: 10, color: C.blue, transition: "transform 0.2s ease",
          display: "inline-block", transform: open ? "rotate(90deg)" : "rotate(0deg)",
          flexShrink: 0, opacity: 0.6,
        }}>&#9654;</span>
      </div>
      {open && (
        <div style={{ padding: "0 18px 16px 88px", animation: "fadeIn 0.15s ease" }}>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 10 }}>
            {driver.highlights.map((h, i) => (
              <div key={i} style={{
                fontSize: 12, lineHeight: 1.4, padding: "6px 10px",
                background: C.white, borderRadius: 8, border: `1px solid ${C.border}`,
                maxWidth: 260,
              }}>
                <span style={{ fontWeight: 700, color: C.text }}>{h.label}:</span>{" "}
                <span style={{ fontWeight: 600, color: C.blue }}>{h.value}</span>
                {h.note && <span style={{ color: C.textLight }}> — {h.note}</span>}
              </div>
            ))}
          </div>
          {driver.cancellations != null && (
            <div style={{ fontSize: 12, color: driver.cancellations >= 5 ? C.redDark : C.textLight, marginBottom: 6 }}>
              Cancellations: <strong>{driver.cancellations}</strong>{driver.cancellations >= 5 ? " ⚠" : ""}
            </div>
          )}
          <div style={{ fontSize: 12, color: C.textMid, fontStyle: "italic", lineHeight: 1.5 }}>
            {driver.summary}
          </div>
        </div>
      )}
    </div>
  );
}

function TeamTab({ data }) {
  return (
    <>
      {data.teamMedianRT != null && (
        <div style={{ background: `linear-gradient(135deg, #F0FDF4, #ECFDF5)`, borderRadius: 14, padding: "24px 28px", marginTop: 8, marginBottom: 28, border: `1px solid #BBF7D0` }}>
          <div style={{ fontSize: 16, fontWeight: 800, color: C.text, marginBottom: 10 }}>Team Overview</div>
          <div style={{ display: "flex", gap: 32, marginBottom: 16, flexWrap: "wrap" }}>
            {[
              ["Median RT", data.teamMedianRT, "min", C.greenDark],
              ["Mean RT", data.teamMeanRT, "min", C.blue],
              ["P90 RT", data.teamP90, "min", C.orange],
              ["P25 RT", data.teamP25, "min", C.teal],
            ].filter(([, v]) => v != null).map(([label, val, unit, color]) => (
              <div key={label}>
                <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, color: C.textLight, marginBottom: 4 }}>{label}</div>
                <div style={{ fontSize: 26, fontWeight: 800, color }}>{val}<span style={{ fontSize: 14, fontWeight: 600, color: C.textLight, marginLeft: 3 }}>{unit}</span></div>
              </div>
            ))}
          </div>
          <div style={{ fontSize: 14, color: C.textMid, lineHeight: 1.7 }}>
            {data.realAgentConvos} conversations handled by {data.agentList.length} agents.
            The team operates across US, European, and West African time zones providing extended coverage.
            SLA target: 24 business hours.
          </div>
        </div>
      )}

      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        {data.agentList.map((agent, idx) => {
          const color = agent.color || AGENT_COLORS[idx % AGENT_COLORS.length];
          const pct = data.realAgentConvos > 0 ? Math.round(agent.conversations / data.realAgentConvos * 100) : 0;
          return (
            <Card key={agent.name}>
              <div style={{ display: "flex", gap: 20, alignItems: "flex-start", flexWrap: "wrap" }}>
                <div style={{ width: 52, height: 52, borderRadius: 14, background: `${color}15`, display: "flex", alignItems: "center", justifyContent: "center", color: color, fontSize: 22, fontWeight: 800, flexShrink: 0, border: `2px solid ${color}30` }}>
                  {agent.name.charAt(0)}
                </div>
                <div style={{ flex: 1, minWidth: 240 }}>
                  <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 4 }}>
                    <span style={{ fontSize: 18, fontWeight: 800, color: C.text }}>{agent.short || agent.name}</span>
                    {agent.tz && <span style={{ fontSize: 12, color: C.textLight, fontWeight: 500 }}>{agent.tz}</span>}
                  </div>
                  <div style={{ fontSize: 12, color: C.textLight, marginBottom: 14 }}>{agent.name}</div>

                  <div style={{ display: "flex", gap: 28, marginBottom: 16, flexWrap: "wrap" }}>
                    {[
                      ["Conversations", agent.conversations, `${pct}%`],
                      ...(agent.replyCount != null ? [["Replies", agent.replyCount, null]] : [["Messages", agent.messages, null]]),
                      ...(agent.exchangeMedianRT != null ? [["Median RT", `${agent.exchangeMedianRT}m`, "all replies"]] : agent.medianRT != null ? [["Median RT", `${agent.medianRT}m`, "1st reply"]] : []),
                      ...(agent.exchangeMeanRT != null ? [["Mean RT", `${agent.exchangeMeanRT}m`, null]] : agent.meanRT != null ? [["Mean RT", `${agent.meanRT}m`, null]] : []),
                      ...(agent.exchangeP90RT != null ? [["P90 RT", `${agent.exchangeP90RT}m`, null]] : agent.p90 != null ? [["P90 RT", `${agent.p90}m`, null]] : []),
                      ...(agent.p25 != null && agent.exchangeMedianRT == null ? [["P25 RT", `${agent.p25}m`, null]] : []),
                    ].map(([l, v, badge]) => (
                      <div key={l}>
                        <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, color: C.textLight, marginBottom: 4 }}>{l}</div>
                        <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
                          <span style={{ fontSize: 22, fontWeight: 800, color: C.text }}>{v}</span>
                          {badge && <span style={{ fontSize: 11, fontWeight: 700, color: color, background: `${color}15`, padding: "2px 8px", borderRadius: 6 }}>{badge}</span>}
                        </div>
                      </div>
                    ))}
                  </div>

                  <div style={{ height: 6, background: C.borderLight, borderRadius: 3, overflow: "hidden" }}>
                    <div style={{ height: "100%", borderRadius: 3, background: color, width: `${pct}%`, transition: "width 0.4s ease" }} />
                  </div>
                </div>
              </div>
            </Card>
          );
        })}
      </div>

      {data.agentList.length > 1 && (
        <>
          <Sec sub="Relative conversation load across the team">Workload Distribution</Sec>
          <Card>
            <div style={{ display: "flex", gap: 6, height: 36, borderRadius: 10, overflow: "hidden", marginBottom: 16 }}>
              {data.agentList.map((agent, idx) => {
                const color = agent.color || AGENT_COLORS[idx % AGENT_COLORS.length];
                const pct = data.realAgentConvos > 0 ? (agent.conversations / data.realAgentConvos * 100) : 0;
                return (
                  <div key={agent.name} style={{ width: `${pct}%`, background: color, display: "flex", alignItems: "center", justifyContent: "center", color: "white", fontSize: 12, fontWeight: 700, borderRadius: 8, minWidth: pct > 5 ? 60 : 0, transition: "width 0.4s ease" }}>
                    {pct > 10 ? `${agent.short || agent.name} ${Math.round(pct)}%` : ""}
                  </div>
                );
              })}
            </div>
            <div style={{ display: "flex", gap: 20, flexWrap: "wrap" }}>
              {data.agentList.map((agent, idx) => {
                const color = agent.color || AGENT_COLORS[idx % AGENT_COLORS.length];
                return (
                  <div key={agent.name} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <div style={{ width: 10, height: 10, borderRadius: "50%", background: color }} />
                    <span style={{ fontSize: 13, color: C.textMid }}>{agent.short || agent.name}: <strong>{agent.conversations}</strong></span>
                  </div>
                );
              })}
            </div>
          </Card>
        </>
      )}

      <div style={{ fontSize: 12, color: C.textLight, lineHeight: 1.6, marginTop: 18, fontStyle: "italic", background: C.borderLight, padding: "14px 18px", borderRadius: 12 }}>
        Note: David operates from Bucharest (EET) and Nene from Accra (GMT). Response time includes business hours (6am-6pm ET, Mon-Fri) only. P90 reflects the experience of the families who wait the longest.
      </div>
    </>
  );
}

// ═══ ATTRITION TAB ═══

const ATTRITION_PROFILES = [{"parent":"Cheryl Turner","students":"Thomas Turner","start":"2025-09-15","cancel":"2025-12-01","convos":8,"classification":"preventable","category":"Technical Issues","summary":"Dash not registering completed work repeatedly. Science stuck. Cumulative platform reliability failures over 2.5 months."},{"parent":"Kirk Konert","students":"Kiara, Kora Konert","start":"2025-10-13","cancel":"2025-12-23","convos":5,"classification":"preventable","category":"Technical Issues","summary":"Bracketing confusion. IXL and Zearn login issues. Two kids, both hit the same Day 1 technical walls."},{"parent":"Erin Dvorak","students":"James, Jonah Dvorak","start":"2025-10-13","cancel":"2025-12-01","convos":4,"classification":"preventable","category":"Technical Issues","summary":"Parent Dash login failures. Science not updating. AlphaWrite access issues."},{"parent":"Amy Wheeler","students":"Noah, Skylar Wheeler","start":"2025-09-15","cancel":"2025-11-20","convos":12,"classification":"preventable","category":"Technical Issues","summary":"Highly engaged. Reading disappeared, science stuck, Dash not registering work. StudyReel issues. Cumulative reliability breakdown."},{"parent":"Kara Porterfield","students":"Nathan Porterfield","start":"2025-09-15","cancel":"2025-12-15","convos":6,"classification":"preventable","category":"Engagement","summary":"Low engagement from child. Parent struggled to get Nathan to complete 2 hours daily. Content not compelling enough."},{"parent":"Amanda Nigg","students":"Jackson, Stella Nigg","start":"2025-09-15","cancel":"2025-12-01","convos":26,"classification":"preventable","category":"Process Failure","summary":"Cancellation email lost for 4 days. Charged over weekend before response. Most conversations in original dataset. Trust destroyed by billing mishap."},{"parent":"Rebecca Hamaoui","students":"Romy Hamaoui","start":"2025-10-13","cancel":"2025-12-11","convos":3,"classification":"involuntary","category":"Life Event","summary":"Family circumstances changed. Minimal technical issues. Clean exit."},{"parent":"Veronica Max","students":"Jackson Max","start":"2025-09-15","cancel":"2025-12-23","convos":9,"classification":"preventable","category":"Process Failure","summary":"Alphie dead-end experiences. Tried to get human help, couldn\u2019t break through bot layer. Cancel button UX issues."},{"parent":"Maryam Ehtsham","students":"Hassan Abdullah","start":"2025-08-18","cancel":"2026-04-01","convos":14,"classification":"preventable","category":"Process Failure","summary":"Charged after cancellation request. Weekend timing meant no response before billing cycle. Payment processing failure."},{"parent":"Elliot Cunningham","students":"Dylan Cunningham","start":"2025-09-15","cancel":"2025-12-01","convos":7,"classification":"preventable","category":"Technical Issues","summary":"Science lessons stuck. Dash not counting completed work. Reading disappeared."},{"parent":"Joanna Nebab","students":"Sophia Nebab","start":"2025-10-13","cancel":"2026-01-05","convos":4,"classification":"preventable","category":"Engagement","summary":"Child disengaged. Parent couldn\u2019t sustain motivation. Content fit issues."},{"parent":"Carolina Paulon","students":"Samuel Paulon","start":"2025-09-15","cancel":"2025-12-01","convos":3,"classification":"involuntary","category":"Life Event","summary":"Returning to Brazil. Geographic/life change, not platform driven."},{"parent":"Brian Bates","students":"Maya Bates","start":"2025-09-15","cancel":"2025-12-15","convos":5,"classification":"preventable","category":"Technical Issues","summary":"Dash not registering work. Bracketing confusion. Technical friction cumulative."},{"parent":"John Quintero","students":"Julian Quintero","start":"2025-10-01","cancel":"2025-12-01","convos":3,"classification":"preventable","category":"Value/Fit","summary":"Program not matching expectations. Short tenure with minimal engagement."},{"parent":"Dana Pereira","students":"Madeline, Olivia Pereira","start":"2025-10-13","cancel":"2025-12-23","convos":6,"classification":"preventable","category":"Technical Issues","summary":"IXL issues. Science stuck. Two kids both experiencing same platform problems."},{"parent":"Jay Shah","students":"Krish Shah","start":"2025-09-15","cancel":"2025-12-01","convos":8,"classification":"preventable","category":"Process Failure","summary":"Cancel portal UX failure. Couldn\u2019t find how to cancel. Searched help center for \u2018cancel\u2019."},{"parent":"Pavel Gavrichev","students":"Nika Gavrichev","start":"2025-10-13","cancel":"2025-12-15","convos":2,"classification":"involuntary","category":"Life Event","summary":"Family relocation. Clean exit unrelated to platform."},{"parent":"Josh Timonen","students":"Ripley Timonen","start":"2025-10-13","cancel":"2025-12-15","convos":4,"classification":"preventable","category":"Technical Issues","summary":"Bracketing issues. Platform reliability."},{"parent":"Katie Anderson","students":"Ainsley, Emory Anderson","start":"2025-10-27","cancel":"2025-12-23","convos":6,"classification":"preventable","category":"Technical Issues","summary":"Science video didn\u2019t match questions. Dash not counting work. Two kids."},{"parent":"Enoch Owen","students":"Amara, Christian Owen","start":"2025-09-15","cancel":"2025-12-01","convos":7,"classification":"preventable","category":"Engagement","summary":"Children not engaging with content. Parent struggled to sustain daily routine."},{"parent":"Eric Breon","students":"Luke, Owen Breon","start":"2025-10-13","cancel":"2025-12-15","convos":5,"classification":"preventable","category":"Technical Issues","summary":"IXL login. Dash issues. Two kids."},{"parent":"Yinlai Meng","students":"Derek Meng","start":"2025-10-27","cancel":"2025-12-23","convos":3,"classification":"involuntary","category":"Life Event","summary":"Family circumstances changed. Minimal engagement."},{"parent":"Alexander McLawhorn","students":"Dominic McLawhorn","start":"2025-10-13","cancel":"2026-01-05","convos":4,"classification":"preventable","category":"Technical Issues","summary":"StudyReel crashes. Platform reliability issues."},{"parent":"Xavior Bringas","students":"Elijah Bringas","start":"2025-09-15","cancel":"2025-12-01","convos":3,"classification":"preventable","category":"Engagement","summary":"Low engagement. Child not connecting with content format."},{"parent":"Thomas Upchurch","students":"Garrett (cancelled), Mason (active)","start":"2025-09-15","cancel":"2025-12-01","convos":5,"classification":"preventable","category":"Engagement","summary":"Partial churn: one child cancelled, sibling stayed. Garrett disengaged while Mason thrived."},{"parent":"Stephen O\u2019Neal","students":"Cannon O\u2019Neal","start":"2025-09-15","cancel":"2025-12-15","convos":10,"classification":"transfer","category":"Advanced Student","summary":"Transferred to GT Anywhere. Cannon too advanced for AA Rocket Math. Doing 5+1 sums as a 7th grader."},{"parent":"Storie Esquell","students":"Eden Esquell","start":"2025-10-13","cancel":"2026-01-05","convos":3,"classification":"preventable","category":"Technical Issues","summary":"Bracketing issues. StudyReel problems."},{"parent":"Faith Bowen","students":"Dakota Bowen","start":"2025-09-15","cancel":"2025-11-20","convos":4,"classification":"preventable","category":"Special Needs","summary":"Dakota has dyslexia. No accommodations pathway available. Family needed text-to-speech and modified content."},{"parent":"Nicholas Burdick","students":"Nora Burdick","start":"2025-10-13","cancel":"2025-12-15","convos":2,"classification":"preventable","category":"Engagement","summary":"Low engagement. Child didn\u2019t connect with self-directed format."},{"parent":"Catherine Besk","students":"Estelle Besk","start":"2025-10-27","cancel":"2025-12-23","convos":3,"classification":"preventable","category":"Technical Issues","summary":"Platform reliability issues. Science not updating."},{"parent":"John Creedon","students":"Noel Creedon","start":"2025-10-13","cancel":"2025-12-15","convos":2,"classification":"preventable","category":"Value/Fit","summary":"Program not matching family expectations for the price."},{"parent":"Lauren McCullough","students":"Leona (cancelled), Leo (active)","start":"2025-09-15","cancel":"2025-12-01","convos":5,"classification":"preventable","category":"Engagement","summary":"Partial churn. Leona disengaged, Leo continued. Gender/engagement pattern."},{"parent":"Aimee Perry","students":"Tennyson Perry","start":"2025-10-13","cancel":"2025-12-23","convos":3,"classification":"preventable","category":"Technical Issues","summary":"Dash not registering work. Platform reliability."},{"parent":"ekah markey","students":"Caleb Markey","start":"2025-09-15","cancel":"2025-12-01","convos":2,"classification":"preventable","category":"Engagement","summary":"Minimal contact. Silent churn with low engagement."},{"parent":"Jan Schaefer","students":"Lena Schaefer","start":"2025-10-27","cancel":"2025-12-23","convos":3,"classification":"preventable","category":"Technical Issues","summary":"StudyReel issues. Platform reliability."},{"parent":"Channing Ross","students":"Zion Ross","start":"2025-10-01","cancel":"2025-11-20","convos":2,"classification":"preventable","category":"Engagement","summary":"Short tenure. Minimal engagement before exit."},{"parent":"Jenny Kheng","students":"Noah Kheng","start":"2025-10-13","cancel":"2025-12-15","convos":3,"classification":"preventable","category":"Technical Issues","summary":"Bracketing confusion. IXL issues."},{"parent":"Ron Beck","students":"Mariana Beck","start":"2025-09-15","cancel":"2025-12-01","convos":5,"classification":"preventable","category":"Engagement","summary":"Content not compelling for child. Parent struggled with daily routine."},{"parent":"Unnur Gretarsdottir","students":"Sara Gretarsdottir","start":"2025-10-13","cancel":"2025-12-23","convos":2,"classification":"involuntary","category":"Life Event","summary":"International family. Circumstances changed."},{"parent":"Jeff Bezner","students":"Ben Bezner","start":"2026-03-09","cancel":"2026-04-01","convos":2,"classification":"preventable","category":"Engagement","summary":"Missed kickoff. Referrer was help center policies page. Minimal engagement before exit."},{"parent":"Lauren Cole","students":"Stella Cole","start":"2025-10-13","cancel":"2026-04-01","convos":46,"classification":"preventable","category":"Technical Issues","summary":"HIGHEST engagement in dataset. Grandmother fought for 6 months. StudyReel quarantined by antivirus repeatedly, reading/writing/science disappearing, kicked from 7th to 3rd grade writing. Every app had login issues."},{"parent":"Sean & Tania Donovan","students":"Paula Donovan","start":"2026-01-26","cancel":"2026-04-01","convos":3,"classification":"preventable","category":"Engagement","summary":"Light engagement. Meeting scheduling with Brittany. Joe handled a date error."},{"parent":"Lina Godfrey","students":"Alia Godfrey","start":"2025-11-10","cancel":"2026-04-01","convos":8,"classification":"preventable","category":"Process Failure","summary":"Angry: \u2018Your system does not allow me to cancel and your bots won\u2019t let me speak to a human.\u2019 StudyReel glitches, MAP proctor failures."},{"parent":"Dylan Cohen","students":"Lexssa Link, Jack (never started)","start":"2026-01-05","cancel":"2026-04-01","convos":21,"classification":"preventable","category":"Coach Departure","summary":"Coach Harley left. Family didn\u2019t want replacement. Also hit bracketing, Rocket Math too low, Dash not counting, billing discrepancy, IXL login."},{"parent":"Jaclyn Porto","students":"Jack Porto, Ryan Porto","start":"2025-03-17","cancel":"2026-04-01","convos":4,"classification":"preventable","category":"Value/Fit","summary":"Changing homeschool program after long tenure. MAP report request. Ryan cancelled earlier."},{"parent":"William Thannisch","students":"Conrad, Luke, Rhett","start":"2025-10-27","cancel":"2026-04-01","convos":16,"classification":"transfer","category":"Advanced Student","summary":"GT Anywhere transfer. Luke \u2018bored and doesn\u2019t feel challenged.\u2019 Subjects disappearing, Fast Math not recording, bracketing confusion."},{"parent":"Emily Christiansen","students":"Jonathan & William Tingey, Grace (never started)","start":"2026-01-26","cancel":"2026-04-01","convos":8,"classification":"involuntary","category":"Seasonal Pause","summary":"\u2018May return Aug/Sep.\u2019 Missed kickoff, Dash not loading, typed \u2018agent\u2019 and \u2018human\u2019 to bypass Alphie. Pause, not permanent."},{"parent":"Alex Talbot","students":"Jack Talbot","start":"2026-02-16","cancel":"2026-03-17","convos":6,"classification":"preventable","category":"Technical Issues","summary":"29 days. AlphaWrite CAPTCHA broken. Bracketing status confusion. Thanked Krys for professionalism. Refund request."},{"parent":"Amanda Shipka","students":"Dotty Buehler","start":"","cancel":"2026-02-13","convos":0,"classification":"involuntary","category":"Internal/Test","summary":"Internal/test account with 10 fake learners. localhost referrer. SKIP."},{"parent":"Sandeep Kella","students":"Aryn, Asreena, Sahana (never started)","start":"2025-09-15","cancel":"2026-02-02","convos":7,"classification":"preventable","category":"Value/Fit","summary":"\u2018It hasn\u2019t been what we expected.\u2019 IXL issues. ClassBank. 3rd child never started."},{"parent":"Ryan Sullivan","students":"Ryan Sullivan","start":"2025-12-08","cancel":"2026-02-02","convos":3,"classification":"preventable","category":"Technical Issues","summary":"Account issues resolved by Nene. Phishing email incident. Minimal signal."},{"parent":"Amie Wilson","students":"Kieren Wilson","start":"2025-09-15","cancel":"2026-02-02","convos":1,"classification":"preventable","category":"Engagement","summary":"Dad asked for own Parent Dash access. Referrer searched \u2018alpha bucks.\u2019 Mostly silent."},{"parent":"Sterling Snead","students":"Ace & Ian Snead","start":"2025-09-03","cancel":"2026-01-29","convos":4,"classification":"preventable","category":"Technical Issues","summary":"Microschool operator (Silver Creek Academy). StudyReel/Dash issues. Payment issue post-cancellation. Graceful exit acknowledging positive impact."},{"parent":"Desmond Brand","students":"Andromeda Brand","start":"2025-10-01","cancel":"2026-01-23","convos":1,"classification":"preventable","category":"Engagement","summary":"\u2018We\u2019ve stopped using 2HL/Alpha and need to unsubscribe.\u2019 One message, no explanation."},{"parent":"Keely Denenberg","students":"Leo Denenberg","start":"2025-10-13","cancel":"2026-01-23","convos":5,"classification":"preventable","category":"Technical Issues","summary":"Parent Dash login failure Day 1. Language lessons question. Alpha coins question."},{"parent":"Jason Hujet","students":"Jack Hujet","start":"2025-10-13","cancel":"2026-01-23","convos":5,"classification":"preventable","category":"Technical Issues","summary":"Login issue Day 1 (wrong email). Halloween check-in on progress. Meeting with Joe."},{"parent":"Martin Key","students":"Arabella & Archie Key","start":"2025-09-15","cancel":"2026-01-23","convos":2,"classification":"involuntary","category":"Life Event","summary":"ESA family moving houses. Searched \u2018cancel\u2019 in help center. UK family in Florida."},{"parent":"Renee Warren","students":"Max & Noah Martell","start":"2025-11-01","cancel":"2026-01-23","convos":3,"classification":"preventable","category":"Value/Fit","summary":"Nanny/assistant handled comms. Re-enrolling in public school. Bracketing questions. IXL expired."},{"parent":"Linda Sapolsky","students":"Hannah & Harrison","start":"2025-11-10","cancel":"2026-01-23","convos":5,"classification":"preventable","category":"Process Failure","summary":"\u2018I keep emailing and not hearing back. I want to cancel for both children.\u2019 Lost cancellation request. MAP test not showing."},{"parent":"Jerin Schreiber","students":"Annabella Schreiber","start":"2025-10-13","cancel":"2026-01-23","convos":7,"classification":"preventable","category":"Technical Issues","summary":"Missing grade field in enrollment. MAP session at capacity error. ESA (FL Step Up)."},{"parent":"Nandini Patel","students":"Zev Thakkar","start":"2025-12-08","cancel":"2026-01-23","convos":5,"classification":"involuntary","category":"Seasonal Pause","summary":"\u2018Strongly believe in Alpha but over committed this winter/spring.\u2019 Zearn login issue. Mary shared microschool info. Pause."},{"parent":"elizabeth Katzman","students":"Ben & Harlow Katzman","start":"2025-10-13","cancel":"2026-01-05","convos":11,"classification":"preventable","category":"Technical Issues","summary":"Harlow science stuck repeatedly. Rocket Math confusion. Ben couldn\u2019t access classes. VOE request. Transcript needed for PCDS school application."},{"parent":"Saverio La Francesca","students":"Alessandra","start":"2025-09-15","cancel":"2026-01-05","convos":3,"classification":"preventable","category":"Engagement","summary":"IXL account setup. ClassBank. Minimal engagement."},{"parent":"Brian Marcinek","students":"Milan Marcinek","start":"2025-10-27","cancel":"2026-01-05","convos":1,"classification":"preventable","category":"Engagement","summary":"ClassBank only conversation. Silent exit."},{"parent":"Sierra Pack","students":"Daxton, India, Jett, Kiya","start":"2025-09-15","cancel":"2026-01-05","convos":1,"classification":"preventable","category":"Process Failure","summary":"4 kids, 1 conversation: \u2018I can\u2019t figure out how to cancel.\u2019 Searched \u2018cancel\u2019 in help center. Purest cancel-button UX failure."},{"parent":"Ari Rastegar","students":"Victoria Rastegar","start":"2025-10-13","cancel":"2026-01-05","convos":4,"classification":"preventable","category":"Technical Issues","summary":"Joe personally reached out AND processed cancellation with \u2018apology and regret.\u2019 Something warranted leadership involvement."},{"parent":"DeVante Warren","students":"Westen Warren","start":"2025-09-15","cancel":"","convos":0,"classification":"preventable","category":"Silent","summary":"Zero conversations. Complete ghost. No cancel date in system."},{"parent":"Emma Bowes","students":"Jack, Mia, Oliver Bowes","start":"2025-09-15","cancel":"2025-12-30","convos":13,"classification":"involuntary","category":"Life Event","summary":"Australian family returning home. Warm departure. But Jack (14) was doing 5+1 in Rocket Math, confirming advanced student pattern."},{"parent":"Duran Torrez","students":"Calder Torrez","start":"2025-11-10","cancel":"2025-12-29","convos":4,"classification":"preventable","category":"Technical Issues","summary":"Confused about kickoff time (PST vs CDT). Unenrollment via Krys."},{"parent":"Ashlea Stares","students":"Beckett & Sully Stares","start":"2025-10-01","cancel":"2025-12-27","convos":2,"classification":"involuntary","category":"Life Event","summary":"Health issues with Beckett. Asked to pause. Science not showing for a week."},{"parent":"Christina Fenters","students":"Jack Fenters","start":"2025-10-13","cancel":"2025-12-23","convos":3,"classification":"preventable","category":"Value/Fit","summary":"\u2018Not a good fit after a quarter.\u2019 AlphaWrite access issues. Rocket Math login."},{"parent":"Crystal Delatore","students":"June (cancelled), Everett (active)","start":"2025-08-25","cancel":"2025-12-19","convos":0,"classification":"preventable","category":"Silent","summary":"Partial churn. Zero conversations. Silent exit for June while Everett stayed."},{"parent":"Charles Lubbat","students":"Elizabeth & Madeline Lubbat","start":"2025-10-27","cancel":"2025-12-11","convos":1,"classification":"preventable","category":"Silent","summary":"2 kids, only 1 Alphie bot conversation. Silent exit."},{"parent":"Jessica Goldman","students":"Gabriella & Juliette Goldman","start":"2025-10-13","cancel":"2025-12-02","convos":1,"classification":"preventable","category":"Technical Issues","summary":"Birth year wrong in MAP testing system. Minimal other contact."},{"parent":"Cheryl Kitchener","students":"Indiana & Zalia Kitchener","start":"2025-09-15","cancel":"2025-12-08","convos":2,"classification":"preventable","category":"Engagement","summary":"Australian family. \u2018My children are just not logging in.\u2019 Sent same cancellation message twice."},{"parent":"Riz Jamal","students":"Owen (cancelled), Ella (active)","start":"2025-12-08","cancel":"2025-12-15","convos":3,"classification":"preventable","category":"Engagement","summary":"Partial churn. Owen cancelled after 7 days. Ella stayed. Straightforward cancellation request."},{"parent":"Alydia Grimm","students":"Quigley Grimm","start":"2025-11-10","cancel":"2025-12-11","convos":7,"classification":"preventable","category":"Technical Issues","summary":"4 Alphie bot convos. David clarified bracketing Day 1. Duplicate ClassBank accounts."},{"parent":"Na Wang","students":"Natalie Liu","start":"2025-10-13","cancel":"2025-12-01","convos":8,"classification":"preventable","category":"Process Failure","summary":"International (China). Payment processed AFTER cancellation twice. Asked to delete credit card. Login failures. Time zone issues."},{"parent":"Luke Gittemeier","students":"Quinn & Paul Gittemeier","start":"2025-10-27","cancel":"2025-11-28","convos":13,"classification":"preventable","category":"Technical Issues","summary":"\u2018We are out.\u2019 31 days. IXL lessons REPEATEDLY not updating. StudyReel broken. Login errors Day 1. Joe forwarded escalations."},{"parent":"George Berar","students":"Eric (cancelled), Sophia (enrolling)","start":"2025-09-15","cancel":"2025-12-15","convos":2,"classification":"preventable","category":"Value/Fit","summary":"\u2018A little more work than I thought, they require our help with laptops.\u2019 2nd child never started."},{"parent":"Peter Estrada","students":"Peter Estrada Jr","start":"2025-09-22","cancel":"2025-12-18","convos":17,"classification":"preventable","category":"Technical Issues","summary":"Highly engaged. Parent Dash login failures twice, science/math not updating, IXL issues, AlphaWrite login, ClassBank gift card delayed."},{"parent":"Danny Aqua","students":"Ethan Aqua","start":"2025-06-30","cancel":"2025-11-13","convos":0,"classification":"preventable","category":"Silent","summary":"136 days, zero conversations. Complete ghost."},{"parent":"Dominique Boles","students":"Jace & Jalen Boles","start":"2025-09-15","cancel":"2025-11-20","convos":2,"classification":"preventable","category":"Special Needs","summary":"Jalen has a diagnosis needing text-to-speech. Mary escalated to academics but conversation closed with no follow-up. Searched \u2018cancel\u2019 in help center 2 months later."},{"parent":"Sarah Miller","students":"Elani & Imara Maheswaran","start":"2025-09-15","cancel":"2025-11-20","convos":3,"classification":"preventable","category":"Value/Fit","summary":"Most articulate feedback: triple testing burden, AA can\u2019t replicate in-person \u2018magic\u2019, unschooled kids experienced AA as LESS freedom. Toured Alpha School Austin, loved it."},{"parent":"Loren Jacobs","students":"Colt Jacobs","start":"2025-10-13","cancel":"2025-11-25","convos":7,"classification":"preventable","category":"Value/Fit","summary":"\u2018I can\u2019t justify paying 10x for a more cumbersome system.\u2019 Son went from excitement to resentment in 2 weeks. Coaching best part but not enough. Mary refunded $510."},{"parent":"Melissa Lamebull Ingram","students":"Jacoby Lamebull-Ingram","start":"2025-10-13","cancel":"2025-11-25","convos":10,"classification":"involuntary","category":"Life Event","summary":"\u2018Unforeseen job change.\u2019 But still had: no parent Dash pre-launch, desktop incompatible with StudyReel, bracketing routed to wrong subject."},{"parent":"Siobhan Lee","students":"Illia Atterbury","start":"2025-09-15","cancel":"2025-11-20","convos":9,"classification":"preventable","category":"Technical Issues","summary":"Bought Chromebook specifically for Alpha. StudyReel doesn\u2019t work on Chromebook. Reading test glitched. Zearn wrong passwords. 9 convos of friction."},{"parent":"Michael Bonen","students":"Bryce Bonen","start":"2025-10-13","cancel":"2025-11-20","convos":2,"classification":"preventable","category":"Engagement","summary":"Minimal. Cancel/hold via Krys. Clean exit."},{"parent":"Mailyn Chico","students":"Arielle Chico, Asher (never started)","start":"2025-09-15","cancel":"2025-11-20","convos":1,"classification":"preventable","category":"Value/Fit","summary":"Found alternative. 2nd child never started."},{"parent":"Brittany Kline","students":"Jagger Kline","start":"2025-09-15","cancel":"2025-11-20","convos":0,"classification":"preventable","category":"Silent","summary":"Zero conversations. Complete ghost."},{"parent":"Patricia Morales","students":"Aiden Nethercott","start":"2025-10-01","cancel":"2025-11-20","convos":3,"classification":"preventable","category":"Process Failure","summary":"Internal flag from Turker: \u2018Did this cancellation fall through the cracks?\u2019 Another lost cancellation."},{"parent":"Kalyn Rodriguez","students":"Woodson Heidenfelder","start":"2025-09-15","cancel":"2025-11-17","convos":6,"classification":"preventable","category":"Value/Fit","summary":"\u2018Dash skill plans almost entirely reliant on IXL.\u2019 Daily glitches. AlphaLearn never launched as promised. \u2018Expensive IXL wrapper\u2019 value perception."},{"parent":"Summer Freedman","students":"Piper Freedman, Nova (never started)","start":"2025-10-27","cancel":"2025-11-13","convos":7,"classification":"preventable","category":"Technical Issues","summary":"17 days. Portal broken, told at live Q&A it\u2019d be fixed, still wasn\u2019t. MAP not showing. 2nd child never started."},{"parent":"Keryn Gold","students":"Warren Meyer","start":"2025-10-01","cancel":"2025-11-13","convos":4,"classification":"preventable","category":"Technical Issues","summary":"Alpha Read 404 errors. Alphas not posting to ClassBank. StudyReel not syncing scores."},{"parent":"Nicole Ramsay","students":"Julian Ramsay","start":"2025-10-13","cancel":"2025-11-13","convos":4,"classification":"preventable","category":"Engagement","summary":"Process of cancellation inquiry. Missing meeting link."},{"parent":"Colleen Tsikira","students":"Zoe & Ethan Tsikira","start":"2025-09-15","cancel":"2025-11-13","convos":7,"classification":"preventable","category":"Technical Issues","summary":"Withdrew Zoe first, kept Ethan, but apps kept failing. Chromebook incompatible. Head of Academics intervened. Requested reimbursement. Ethan payment_defaulted."},{"parent":"Jennifer DeFlorio","students":"Leonardo DeFlorio","start":"2025-10-13","cancel":"2025-11-12","convos":7,"classification":"preventable","category":"Technical Issues","summary":"\u2018Nobody on onboarding call that was supposed to begin at 10:30.\u2019 Edulastic broken. Bracketing not working. Devastating first impression."},{"parent":"Eric Bjurstrom","students":"Asher Bjurstrom","start":"2025-09-15","cancel":"2025-11-06","convos":2,"classification":"preventable","category":"Process Failure","summary":"\u2018Requested by email and chat support the termination of account and refund but have had no response.\u2019 Lost cancellation."},{"parent":"Leah Brunton","students":"Vivienne & Emmersen Brunton","start":"2025-10-13","cancel":"2025-11-06","convos":7,"classification":"preventable","category":"Technical Issues","summary":"24 days. Couldn\u2019t access daughters\u2019 Dash (kept logging into parent). Preferred name glitch. Never really got started."},{"parent":"Kamisha Corbitt","students":"Jayce & Kylee Corbitt","start":"2025-10-13","cancel":"2025-11-06","convos":6,"classification":"preventable","category":"Technical Issues","summary":"ESA family. \u2018Total disaster trying to get them started. My children and I were very excited.\u2019 First-week failure. ESA refund requested."},{"parent":"Monica Cox","students":"Eero Cox","start":"2025-10-01","cancel":"2025-11-06","convos":1,"classification":"preventable","category":"Value/Fit","summary":"\u2018We believe in the ethos but the way Eero learns isn\u2019t the best fit.\u2019 Clean exit."},{"parent":"Paula Tellez","students":"Tomas Dasilva","start":"2025-10-01","cancel":"2025-11-06","convos":8,"classification":"preventable","category":"Technical Issues","summary":"\u2018I feel more like a QA tester than a parent using a tool I\u2019m paying for.\u2019 StudyReel not opening. Lessons not recorded."},{"parent":"Susan Krick","students":"Lilly Krick","start":"2025-09-15","cancel":"2025-11-06","convos":6,"classification":"preventable","category":"Process Failure","summary":"\u2018Full of tears and struggles.\u2019 Tried multiple times to cancel, no response. Kept getting billed: \u2018Can you please stop trying to bill me?\u2019"},{"parent":"Carey Martin","students":"Willa Martin","start":"2025-09-15","cancel":"2025-11-06","convos":20,"classification":"preventable","category":"Technical Issues","summary":"Extremely engaged. Reading stuck for a week. Started positive, ground down by cumulative issues. Charged after cancellation."},{"parent":"Hanna Richards","students":"Elijah Richards","start":"2025-09-15","cancel":"2025-11-06","convos":6,"classification":"preventable","category":"Technical Issues","summary":"Beta tester. No lessons after placement tests. Refund vs $1000 credit issue."},{"parent":"Lauren Bean","students":"Mikko Bean, Tyson (never started)","start":"2025-09-15","cancel":"2025-10-05","convos":7,"classification":"preventable","category":"Technical Issues","summary":"20 days. Son scored 4 grades behind in math. Reading stuck. Replied \u2018stop\u2019 to weekly pulse. IXL data transfer needed."},{"parent":"Channi Fett","students":"Warren (cancelled), Gavin & Oaklen (active)","start":"2025-09-15","cancel":"2025-10-05","convos":1,"classification":"involuntary","category":"Life Event","summary":"Partial churn: Warren returned to traditional school. Siblings continued. Not a platform issue."},{"parent":"Toni Weinbrandt","students":"Lux Weinbrandt","start":"2025-09-15","cancel":"","convos":17,"classification":"involuntary","category":"Special Needs","summary":"PDA profile needing flexibility. Beta family. Child model (work permit needed). Now inquiring about RE-ENROLLMENT March 2026. Boomerang."},{"parent":"Brittany Canfield","students":"David Canfield","start":"2025-08-11","cancel":"2025-09-22","convos":0,"classification":"preventable","category":"Silent","summary":"Pre-Intercom. Zero conversations. 42 days."},{"parent":"Tiffany McWaters","students":"Mayli McWaters","start":"2025-07-21","cancel":"2025-09-15","convos":0,"classification":"preventable","category":"Silent","summary":"Pre-Intercom. Zero conversations."},{"parent":"Sarah Mabe","students":"Bennett Mabe","start":"2025-08-04","cancel":"2025-09-05","convos":0,"classification":"preventable","category":"Silent","summary":"Pre-Intercom. Zero conversations. 32 days."},{"parent":"Jonathan Doyle","students":"Sonia Doyle","start":"2025-10-01","cancel":"2025-09-01","convos":0,"classification":"preventable","category":"Silent","summary":"Cancel date before start date. Likely data issue or pre-start cancellation."},{"parent":"Chanel Smith","students":"Kameron & Torrey Smith","start":"2025-06-30","cancel":"2025-09-01","convos":0,"classification":"preventable","category":"Silent","summary":"Pre-Intercom. Zero conversations."},{"parent":"Rhonda Wallace","students":"Jayden Wallace","start":"2025-06-16","cancel":"2025-09-01","convos":0,"classification":"preventable","category":"Silent","summary":"Pre-Intercom. Zero conversations."},{"parent":"Jessica Ries","students":"Julia & Nicholas Ries","start":"2025-06-16","cancel":"2025-08-25","convos":0,"classification":"preventable","category":"Silent","summary":"Pre-Intercom. Zero conversations. 70 days."},{"parent":"Laura Strangio","students":"Joseph & Juliana Strangio","start":"2025-06-09","cancel":"2025-08-04","convos":0,"classification":"preventable","category":"Silent","summary":"Pre-Intercom. Zero conversations. 56 days."},{"parent":"Hope Smith","students":"Legend Smith","start":"2025-10-01","cancel":"2025-08-01","convos":0,"classification":"involuntary","category":"Life Event","summary":"Cancel date before start date. Data anomaly. Pre-start withdrawal."},{"parent":"Praveen Patel","students":"Nehvin Patel","start":"2025-06-09","cancel":"2025-06-28","convos":0,"classification":"preventable","category":"Silent","summary":"Pre-Intercom. 19 days. Fastest exit in dataset."}];

const ATTRITION_COLORS = {
  preventable: { color: C.red, bg: "#FEE2E2", label: "Preventable" },
  involuntary: { color: C.green, bg: "#DCFCE7", label: "Involuntary" },
  transfer: { color: C.purple, bg: "#EDE9FE", label: "Transfer" },
};

const CATEGORY_CHART_DATA = [
  { name: "Technical Issues", value: 41 },
  { name: "Engagement", value: 20 },
  { name: "Silent", value: 14 },
  { name: "Value/Fit", value: 12 },
  { name: "Process Failure", value: 11 },
  { name: "Life Event", value: 11 },
  { name: "Special Needs", value: 3 },
  { name: "Advanced Student", value: 2 },
  { name: "Seasonal Pause", value: 2 },
  { name: "Coach Departure", value: 1 },
  { name: "Internal/Test", value: 1 },
];

const NET_CHURN_DATA = [
  { month: "Aug '25", enrolled: 35, cancelled: 6 },
  { month: "Sep '25", enrolled: 114, cancelled: 8 },
  { month: "Oct '25", enrolled: 115, cancelled: 2 },
  { month: "Nov '25", enrolled: 47, cancelled: 30 },
  { month: "Dec '25", enrolled: 81, cancelled: 19 },
  { month: "Jan '26", enrolled: 121, cancelled: 23 },
  { month: "Feb '26", enrolled: 57, cancelled: 11 },
  { month: "Mar '26", enrolled: 154, cancelled: 28 },
  { month: "Apr '26", enrolled: 20, cancelled: 18 },
];

const COHORT_CHURN_DATA = [
  { cohort: "Sep '25", rate: 42.1, students: 114, color: C.red },
  { cohort: "Oct '25", rate: 39.1, students: 115, color: C.red },
  { cohort: "Nov '25", rate: 23.4, students: 47, color: C.yellow },
  { cohort: "Dec '25", rate: 22.2, students: 81, color: C.yellow },
  { cohort: "Jan '26", rate: 9.9, students: 121, color: C.green },
  { cohort: "Feb '26", rate: 5.3, students: 57, color: C.green },
  { cohort: "Mar '26", rate: 1.3, students: 154, color: C.green },
];

const RETENTION_DATA = [
  { cohort: "2025-09", vals: [100.0, 98.2, 93.9, 84.2, 76.3, 68.4, 64.0] },
  { cohort: "2025-10", vals: [98.3, 93.9, 80.0, 74.8, 69.6, 67.0, 62.6] },
  { cohort: "2025-11", vals: [100.0, 100.0, 93.6, 87.2, 78.7, 76.6, 76.6] },
  { cohort: "2025-12", vals: [100.0, 98.8, 92.6, 81.5, 80.2, 80.2, 80.2] },
  { cohort: "2026-01", vals: [100.0, 100.0, 95.0, 90.9, 90.9, 90.9, 90.9] },
  { cohort: "2026-02", vals: [100.0, 94.7, 94.7, 94.7, 94.7, 94.7, 94.7] },
  { cohort: "2026-03", vals: [100.0, 98.7, 98.7, 98.7, 98.7, 98.7, 98.7] },
];

const TENURE_DATA = [
  { month: "Month 0", cancellations: 14, danger: true },
  { month: "Month 1", cancellations: 39, danger: true },
  { month: "Month 2", cancellations: 39, danger: true },
  { month: "Month 3", cancellations: 20, danger: false },
  { month: "Month 4", cancellations: 16, danger: false },
  { month: "Month 5", cancellations: 10, danger: false },
  { month: "Month 6", cancellations: 1, danger: false },
];

function AttritionTab() {
  const [filter, setFilter] = useState("all");
  const [searchTerm, setSearchTerm] = useState("");

  const filtered = ATTRITION_PROFILES.filter(p => {
    if (filter !== "all" && p.classification !== filter) return false;
    if (searchTerm) {
      const q = searchTerm.toLowerCase();
      return p.parent.toLowerCase().includes(q) || p.students.toLowerCase().includes(q) || p.summary.toLowerCase().includes(q) || p.category.toLowerCase().includes(q);
    }
    return true;
  });

  const counts = { all: ATTRITION_PROFILES.length, preventable: 101, involuntary: 15, transfer: 2 };

  const retentionColor = v => {
    if (v >= 90) return { bg: "rgba(34,197,94,0.12)", color: C.green };
    if (v >= 75) return { bg: "rgba(34,197,94,0.07)", color: C.greenDark };
    if (v >= 65) return { bg: "rgba(245,158,11,0.10)", color: C.yellow };
    return { bg: "rgba(239,68,68,0.10)", color: C.red };
  };

  return (
    <>
      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 16, marginBottom: 32 }}>
        {[
          { num: "118", label: "Cancelled Families", color: C.red, bg: "#FEF2F2" },
          { num: "495", label: "Active Students", color: C.green, bg: "#F0FDF4" },
          { num: "101", label: "Preventable", color: C.yellow, bg: "#FFFBEB" },
          { num: "15", label: "Involuntary", color: C.blue, bg: "#EFF6FF" },
          { num: "2", label: "Transfers", color: C.purple, bg: "#F5F3FF" },
        ].map(k => (
          <div key={k.label} style={{ background: k.bg, borderRadius: 14, padding: "20px 16px", textAlign: "center", border: `1px solid ${C.border}` }}>
            <div style={{ fontSize: 28, fontWeight: 800, color: k.color, letterSpacing: -1 }}>{k.num}</div>
            <div style={{ fontSize: 11, fontWeight: 600, color: C.textMid, textTransform: "uppercase", letterSpacing: 0.5, marginTop: 4 }}>{k.label}</div>
          </div>
        ))}
      </div>

      {/* Section 1: Classification */}
      <div style={{ fontSize: 16, fontWeight: 800, color: C.text, marginBottom: 16 }}>Classification: Preventable vs Involuntary vs Transfer</div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 36 }}>
        <Card>
          <ResponsiveContainer width="100%" height={260}>
            <PieChart>
              <Pie data={[{ name: "Preventable", value: 101 }, { name: "Involuntary", value: 15 }, { name: "Transfer", value: 2 }]} cx="50%" cy="50%" innerRadius={55} outerRadius={100} dataKey="value" stroke="none">
                <Cell fill={C.red} />
                <Cell fill={C.green} />
                <Cell fill={C.purple} />
              </Pie>
              <Tooltip contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }} />
            </PieChart>
          </ResponsiveContainer>
          <div style={{ display: "flex", justifyContent: "center", gap: 20, marginTop: 8 }}>
            {[["Preventable", C.red], ["Involuntary", C.green], ["Transfer", C.purple]].map(([l, c]) => (
              <div key={l} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: C.textMid }}>
                <div style={{ width: 10, height: 10, borderRadius: 3, background: c }} />
                {l}
              </div>
            ))}
          </div>
        </Card>
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {[
            { title: "Preventable (101 families)", desc: "Could have been retained with better platform reliability, onboarding, or process. This is the addressable pool.", color: C.red, bg: "#FEF2F2" },
            { title: "Involuntary (15 families)", desc: "Life events, relocations, job changes, health, seasonal pauses. Not preventable by product or CX changes.", color: C.green, bg: "#F0FDF4" },
            { title: "Transfers (2 families)", desc: "Moved to GT Anywhere or Alpha School. Positive for the ecosystem but lost from AA revenue.", color: C.purple, bg: "#F5F3FF" },
          ].map(c => (
            <div key={c.title} style={{ background: c.bg, borderLeft: `3px solid ${c.color}`, borderRadius: "0 10px 10px 0", padding: "14px 18px" }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: C.text, marginBottom: 4 }}>{c.title}</div>
              <div style={{ fontSize: 12, color: C.textMid, lineHeight: 1.5 }}>{c.desc}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Section 2: Net Churn */}
      <div style={{ fontSize: 16, fontWeight: 800, color: C.text, marginBottom: 16 }}>Net Churn: Enrollments vs Cancellations by Month</div>
      <Card>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={NET_CHURN_DATA} barGap={2}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
            <XAxis dataKey="month" tick={{ fill: C.textMid, fontSize: 11 }} axisLine={{ stroke: C.border }} />
            <YAxis tick={{ fill: C.textMid, fontSize: 11 }} axisLine={{ stroke: C.border }} />
            <Tooltip contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }} />
            <Bar dataKey="enrolled" name="Enrolled" fill={C.green} radius={[4, 4, 0, 0]} />
            <Bar dataKey="cancelled" name="Cancelled" fill={C.red} radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
        <div style={{ fontSize: 12, color: C.textMid, marginTop: 12, lineHeight: 1.5 }}>
          Enrollment is scaling strongly. November 2025 was the first month where cancellations became material (30 students). The net has been positive every month.
        </div>
      </Card>

      {/* Section 3: Cohort Churn Rate */}
      <div style={{ fontSize: 16, fontWeight: 800, color: C.text, marginBottom: 16, marginTop: 32 }}>Cohort Churn Rate by Enrollment Month</div>
      <Card>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={COHORT_CHURN_DATA}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
            <XAxis dataKey="cohort" tick={{ fill: C.textMid, fontSize: 11 }} axisLine={{ stroke: C.border }} />
            <YAxis tick={{ fill: C.textMid, fontSize: 11 }} tickFormatter={v => v + "%"} axisLine={{ stroke: C.border }} />
            <Tooltip contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }} formatter={(v, _, p) => [`${v}% (${p.payload.students} students)`, "Churn Rate"]} />
            <Bar dataKey="rate" name="Churn Rate" radius={[4, 4, 0, 0]}>
              {COHORT_CHURN_DATA.map((d, i) => <Cell key={i} fill={d.color} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div style={{ fontSize: 12, color: C.textMid, marginTop: 12, lineHeight: 1.5 }}>
          Sep/Oct 2025 cohorts have the highest churn (~37-42%). These were the first scaling cohorts hitting beta-era platform issues. Jan 2026+ cohorts show dramatically lower churn (5-10%), indicating product stability improvements.
        </div>
      </Card>

      {/* Section 4: Top Categories */}
      <div style={{ fontSize: 16, fontWeight: 800, color: C.text, marginBottom: 16, marginTop: 32 }}>Top Churn Categories</div>
      <Card>
        <ResponsiveContainer width="100%" height={340}>
          <BarChart data={CATEGORY_CHART_DATA} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
            <XAxis type="number" tick={{ fill: C.textMid, fontSize: 11 }} axisLine={{ stroke: C.border }} />
            <YAxis type="category" dataKey="name" tick={{ fill: C.textMid, fontSize: 11 }} width={120} axisLine={{ stroke: C.border }} />
            <Tooltip contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }} />
            <Bar dataKey="value" name="Families" fill={C.purple} radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </Card>

      {/* Section 5: Cohort Retention Heatmap */}
      <div style={{ fontSize: 16, fontWeight: 800, color: C.text, marginBottom: 16, marginTop: 32 }}>Cohort Retention</div>
      <Card>
        <div style={{ fontSize: 12, color: C.textMid, marginBottom: 16 }}>What percentage of each enrollment cohort survives to Month N?</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr>
                {["Cohort", "Start", "M1", "M2", "M3", "M4", "M5", "M6"].map(h => (
                  <th key={h} style={{ padding: "10px 14px", textAlign: "center", borderBottom: `2px solid ${C.border}`, color: C.textMid, fontWeight: 600, fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {RETENTION_DATA.map(row => (
                <tr key={row.cohort}>
                  <td style={{ padding: "10px 14px", fontWeight: 700, color: C.text, borderBottom: `1px solid ${C.borderLight}` }}>{row.cohort}</td>
                  {row.vals.map((v, i) => {
                    const rc = retentionColor(v);
                    return (
                      <td key={i} style={{ padding: "10px 14px", textAlign: "center", fontWeight: 600, color: rc.color, background: rc.bg, borderBottom: `1px solid ${C.borderLight}` }}>{v.toFixed(1)}%</td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div style={{ fontSize: 11, color: C.textLight, marginTop: 12 }}>
          Cohorts with fewer than 5 students excluded. M0 = survived to day 0 (enrolled), M1 = survived 30+ days, etc.
        </div>
      </Card>

      {/* Section 6: When Attrition Peaks */}
      <div style={{ fontSize: 16, fontWeight: 800, color: C.text, marginBottom: 16, marginTop: 32 }}>When Attrition Peaks</div>
      <Card>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={TENURE_DATA}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
            <XAxis dataKey="month" tick={{ fill: C.textMid, fontSize: 11 }} axisLine={{ stroke: C.border }} />
            <YAxis tick={{ fill: C.textMid, fontSize: 11 }} axisLine={{ stroke: C.border }} />
            <Tooltip contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }} />
            <Bar dataKey="cancellations" name="Cancellations" radius={[4, 4, 0, 0]}>
              {TENURE_DATA.map((d, i) => <Cell key={i} fill={d.danger ? C.red : C.purple} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div style={{ fontSize: 12, color: C.textMid, marginTop: 12, lineHeight: 1.5 }}>
          The danger zone is Months 1-2 (days 30-89). Together they account for 78 of 139 cancellations (56%). Month 0 (first 30 days) accounts for 14 more. After Month 3, attrition drops sharply. Families who survive 90 days are likely to stay.
        </div>
      </Card>

      {/* Section 7: Family Records */}
      <div style={{ fontSize: 16, fontWeight: 800, color: C.text, marginBottom: 12, marginTop: 32 }}>Full Family Records</div>
      <div style={{ fontSize: 12, color: C.textMid, marginBottom: 16 }}>Every cancelled family with a summary of why they left.</div>

      {/* Filters + Search */}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center", marginBottom: 16 }}>
        {["all", "preventable", "involuntary", "transfer"].map(f => {
          const active = filter === f;
          const label = f === "all" ? `All (${counts.all})` : `${ATTRITION_COLORS[f]?.label || f} (${counts[f]})`;
          return (
            <button key={f} onClick={() => setFilter(f)} style={{
              padding: "6px 16px", borderRadius: 99, fontSize: 12, fontWeight: 600,
              cursor: "pointer", border: active ? "none" : `1px solid ${C.border}`,
              background: active ? C.blue : C.white,
              color: active ? "#FFF" : C.textMid,
              transition: "all 0.15s ease",
            }}>{label}</button>
          );
        })}
        <div style={{ flex: 1 }} />
        <input
          type="text" placeholder="Search families..."
          value={searchTerm} onChange={e => setSearchTerm(e.target.value)}
          style={{ padding: "7px 14px", borderRadius: 8, border: `1px solid ${C.border}`, fontSize: 12, color: C.text, background: C.white, width: 200, outline: "none" }}
        />
      </div>

      {/* Family cards */}
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {filtered.map((p, i) => {
          const tenure = p.start && p.cancel ? Math.round((new Date(p.cancel) - new Date(p.start)) / 86400000) : null;
          const tenureStr = tenure !== null && tenure >= 0 ? tenure + "d" : "N/A";
          const cls = ATTRITION_COLORS[p.classification] || ATTRITION_COLORS.preventable;
          return (
            <div key={i} style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 10, padding: "14px 18px", transition: "border-color 0.15s" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                  <span style={{ fontWeight: 700, fontSize: 13, color: C.text }}>{p.parent}</span>
                  <span style={{ display: "inline-block", padding: "2px 10px", borderRadius: 99, fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.3, background: cls.bg, color: cls.color }}>{p.classification}</span>
                  <span style={{ display: "inline-block", padding: "2px 10px", borderRadius: 99, fontSize: 10, fontWeight: 600, background: "#EFF6FF", color: C.blue }}>{p.category}</span>
                </div>
                <div style={{ display: "flex", gap: 16, fontSize: 11, color: C.textLight }}>
                  <span>{p.students}</span>
                  <span>{tenureStr}</span>
                  <span>{p.convos} convos</span>
                </div>
              </div>
              <div style={{ fontSize: 12, color: C.textMid, marginTop: 8, lineHeight: 1.5 }}>{p.summary}</div>
            </div>
          );
        })}
      </div>
      {filtered.length === 0 && (
        <div style={{ textAlign: "center", padding: 40, color: C.textLight, fontSize: 13 }}>No families match the current filters.</div>
      )}
    </>
  );
}

// ═══ MAIN DASHBOARD ═══

export default function Dashboard() {
  const [tab, setTab] = useState("insights");
  const [dateRange, setDateRange] = useState({ from: "", to: "" });
  const [inboxFilter, setInboxFilter] = useState("all");
  const { weeklyStats, categorizations, insights, hourlyPatterns, conversations, weeklyReports, monthlyReports, loading } = useSupabase();

  const data = useMemo(
    () => deriveData(weeklyStats, categorizations, hourlyPatterns, insights, conversations, dateRange, inboxFilter),
    [weeklyStats, categorizations, hourlyPatterns, insights, conversations, dateRange, inboxFilter],
  );

  const avgWeeklyVol = data.weekCount > 0 ? Math.round(data.totalConversations / data.weekCount) : 0;
  const TOP4 = data.macroEntries.slice(0, 4).map(([name]) => name);

  const volRtData = useMemo(() => data.weekly.map(w => ({
    name: w.label, volume: w.vol, medianRT: w.rt,
  })), [data.weekly]);

  const isFiltered = dateRange.from || dateRange.to || inboxFilter !== "all";

  if (loading) {
    return (
      <div style={{ minHeight: "100vh", background: C.bg, display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'Inter', system-ui, sans-serif" }}>
        <div style={{ textAlign: "center" }}>
          <div style={{ width: 40, height: 40, border: `3px solid ${C.border}`, borderTop: `3px solid ${C.blue}`, borderRadius: "50%", animation: "spin 0.8s linear infinite", margin: "0 auto 16px" }} />
          <div style={{ fontSize: 16, fontWeight: 700, color: C.text }}>Loading Dashboard</div>
          <div style={{ fontSize: 13, color: C.textLight, marginTop: 4 }}>Fetching data from Supabase...</div>
        </div>
        <style>{`
          @keyframes spin { to { transform: rotate(360deg); } }
          * { margin: 0; padding: 0; box-sizing: border-box; }
          html, body, #root { height: 100%; width: 100%; }
        `}</style>
      </div>
    );
  }

  if (data.totalConversations === 0 && !dateRange.from && !dateRange.to) {
    return (
      <div style={{ minHeight: "100vh", background: C.bg, display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'Inter', system-ui, sans-serif" }}>
        <Card style={{ maxWidth: 400, textAlign: "center", padding: 40 }}>
          <div style={{ fontSize: 40, marginBottom: 12 }}>◎</div>
          <div style={{ fontSize: 20, fontWeight: 800, color: C.text, marginBottom: 8 }}>No Data Yet</div>
          <div style={{ fontSize: 14, color: C.textMid, lineHeight: 1.6 }}>Run the pipeline to populate:<br/><code style={{ background: C.borderLight, padding: "4px 8px", borderRadius: 6, fontSize: 13 }}>python pipeline.py your_export.csv</code></div>
        </Card>
      </div>
    );
  }

  const tabLabel = TABS.find(t => t.key === tab)?.label || "Overview";

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "'Inter', 'DM Sans', system-ui, sans-serif", color: C.text, overflow: "hidden" }}>
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
      <style>{`
        * { margin: 0; padding: 0; box-sizing: border-box; }
        html, body, #root { height: 100%; width: 100%; overflow: hidden; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: ${C.borderDark}; }
      `}</style>

      {/* ── SIDEBAR ── */}
      <aside style={{
        width: 230, flexShrink: 0, height: "100vh",
        background: `linear-gradient(180deg, ${C.navy} 0%, ${C.navyMid} 100%)`,
        display: "flex", flexDirection: "column",
        borderRight: `1px solid rgba(255,255,255,0.06)`,
      }}>
        {/* Brand */}
        <div style={{ padding: "28px 24px 20px" }}>
          <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: 2, textTransform: "uppercase", color: "rgba(255,255,255,0.3)", marginBottom: 4 }}>Alpha Anywhere</div>
          <div style={{ fontSize: 16, fontWeight: 800, color: "white", letterSpacing: -0.3 }}>PX Dashboard</div>
        </div>

        {/* Nav items */}
        <nav style={{ flex: 1, padding: "8px 12px", display: "flex", flexDirection: "column", gap: 2 }}>
          {TABS.map(t => {
            const active = tab === t.key;
            return (
              <button key={t.key} onClick={() => setTab(t.key)} style={{
                display: "flex", alignItems: "center", gap: 12,
                padding: "10px 14px", borderRadius: 10, border: "none",
                cursor: "pointer", width: "100%", textAlign: "left",
                background: active ? "rgba(255,255,255,0.1)" : "transparent",
                color: active ? "white" : "rgba(255,255,255,0.45)",
                fontSize: 13, fontWeight: active ? 700 : 500,
                transition: "all 0.15s ease",
              }}>
                <span style={{ fontSize: 14, width: 20, textAlign: "center", opacity: active ? 1 : 0.5 }}>{t.icon}</span>
                {t.label}
              </button>
            );
          })}
        </nav>

        {/* Stats footer */}
        <div style={{ padding: "16px 24px 24px", borderTop: "1px solid rgba(255,255,255,0.06)" }}>
          <div style={{ fontSize: 11, color: "rgba(255,255,255,0.3)", marginBottom: 8 }}>
            {isFiltered ? "Filtered view" : "Full period"}
          </div>
          <div style={{ fontSize: 20, fontWeight: 800, color: "white", letterSpacing: -0.5 }}>
            {data.totalConversations}
          </div>
          <div style={{ fontSize: 11, color: "rgba(255,255,255,0.4)" }}>
            conversations · {data.weekCount}w
          </div>
        </div>
      </aside>

      {/* ── MAIN AREA ── */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", background: C.bg }}>

        {/* Top bar */}
        <header style={{
          flexShrink: 0, padding: "14px 32px",
          background: C.white,
          borderBottom: `1px solid ${C.border}`,
        }}>
          {/* Row 1: Title + Inbox */}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: (tab === "insights" || tab === "attrition") ? 0 : 12 }}>
            <div>
              <h1 style={{ fontSize: 20, fontWeight: 800, color: C.text, margin: 0, letterSpacing: -0.5 }}>{tabLabel}</h1>
              <div style={{ fontSize: 12, color: C.textLight, marginTop: 2 }}>
                {tab === "attrition"
                  ? "118 cancelled families · Intercom conversation analysis & enrollment CSV"
                  : isFiltered
                    ? `Showing ${data.filteredConvos.length} filtered conversations`
                    : `${data.totalConversations} conversations across ${data.weekCount} week${data.weekCount !== 1 ? "s" : ""}`}
              </div>
            </div>
            {tab !== "insights" && tab !== "attrition" && data.inboxOptions.length > 1 && (
              <select
                value={inboxFilter}
                onChange={e => setInboxFilter(e.target.value)}
                style={{
                  background: C.white, color: C.text, border: `1px solid ${C.border}`,
                  borderRadius: 8, padding: "7px 12px", fontSize: 12, fontWeight: 500,
                  cursor: "pointer", appearance: "auto",
                }}
              >
                <option value="all">All Inboxes</option>
                {data.inboxOptions.map(o => (
                  <option key={o.name} value={o.name}>{o.name} ({o.count})</option>
                ))}
              </select>
            )}
          </div>
          {/* Row 2: Week presets + date range (hidden on Insights tab) */}
          {tab !== "insights" && tab !== "attrition" && <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
            <span style={{ fontSize: 11, fontWeight: 600, color: C.textLight, textTransform: "uppercase", letterSpacing: 0.5, marginRight: 2 }}>Week:</span>
            {data.weekly.length > 0 && data.weekly.map(w => {
              const weekDates = (() => {
                const parts = w.label.match(/([A-Za-z]+)\s+(\d+)/);
                if (!parts) return null;
                const monthNames = { Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5, Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11 };
                const m = monthNames[parts[1]];
                if (m == null) return null;
                const year = data.dataDateRange.from ? parseInt(data.dataDateRange.from.slice(0, 4)) : new Date().getFullYear();
                const from = new Date(year, m, parseInt(parts[2]));
                const to = new Date(from);
                to.setDate(to.getDate() + 6);
                const fmt = d => d.toISOString().slice(0, 10);
                return { from: fmt(from), to: fmt(to) };
              })();
              if (!weekDates) return null;
              const isActive = dateRange.from === weekDates.from && dateRange.to === weekDates.to;
              return (
                <button key={w.label} onClick={() => {
                  if (isActive) setDateRange({ from: "", to: "" });
                  else setDateRange(weekDates);
                }} style={{
                  padding: "5px 10px", borderRadius: 7, fontSize: 11, fontWeight: 600,
                  cursor: "pointer", border: isActive ? "none" : `1px solid ${C.border}`,
                  background: isActive ? C.blue : C.white,
                  color: isActive ? "#FFF" : C.textMid,
                  transition: "all 0.15s ease", whiteSpace: "nowrap",
                }}>
                  {w.label}
                </button>
              );
            })}
            <div style={{ width: 1, height: 20, background: C.border, margin: "0 4px" }} />
            <span style={{ fontSize: 11, fontWeight: 600, color: C.textLight, textTransform: "uppercase", letterSpacing: 0.5, marginRight: 2 }}>Custom:</span>
            <input type="date" value={dateRange.from || ""}
              min={data.dataDateRange.from || undefined}
              max={dateRange.to || data.dataDateRange.to || undefined}
              onChange={e => setDateRange(r => ({ ...r, from: e.target.value }))}
              style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, padding: "5px 10px", fontSize: 12, color: C.text, outline: "none" }} />
            <span style={{ color: C.textLight, fontSize: 12 }}>to</span>
            <input type="date" value={dateRange.to || ""}
              min={dateRange.from || data.dataDateRange.from || undefined}
              max={data.dataDateRange.to || undefined}
              onChange={e => setDateRange(r => ({ ...r, to: e.target.value }))}
              style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, padding: "5px 10px", fontSize: 12, color: C.text, outline: "none" }} />
            {(dateRange.from || dateRange.to) && (
              <button onClick={() => setDateRange({ from: "", to: "" })} style={{
                background: C.borderLight, border: `1px solid ${C.border}`, borderRadius: 8,
                padding: "5px 12px", fontSize: 11, color: C.textMid,
                cursor: "pointer", fontWeight: 600,
              }}>Clear</button>
            )}
          </div>}
        </header>

        {/* Scrollable content */}
        <main style={{ flex: 1, overflow: "auto", padding: "24px 32px 64px" }}>
          <div style={{ maxWidth: 1100, margin: "0 auto" }}>
            {tab === "overview" && <OverviewTab data={data} avgWeeklyVol={avgWeeklyVol} volRtData={volRtData} />}
            {tab === "insights" && <InsightsTab data={data} weeklyReports={weeklyReports} monthlyReports={monthlyReports} />}
            {tab === "categories" && <CategoriesTab data={data} weekly={data.weekly} TOP4={TOP4} />}
            {tab === "volume" && <VolumeTab data={data} avgWeeklyVol={avgWeeklyVol} volRtData={volRtData} />}
            {tab === "team" && <TeamTab data={data} />}
            {tab === "attrition" && <AttritionTab />}
          </div>
        </main>
      </div>
    </div>
  );
}
