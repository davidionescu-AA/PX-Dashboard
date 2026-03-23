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
];

// ═══ DATA HOOKS ═══

function useSupabase() {
  const [weeklyStats, setWeeklyStats] = useState([]);
  const [categorizations, setCategorizations] = useState([]);
  const [insights, setInsights] = useState([]);
  const [hourlyPatterns, setHourlyPatterns] = useState([]);
  const [conversations, setConversations] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      const [ws, cat, ins, hp] = await Promise.all([
        supabase.from("weekly_stats").select("*").order("week_label"),
        supabase.from("categorizations").select("*"),
        supabase.from("weekly_insights").select("*").eq("status", "published"),
        supabase.from("hourly_patterns").select("*").order("hour_num"),
      ]);
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
      setLoading(false);
    }
    load();
  }, []);

  return { weeklyStats, categorizations, insights, hourlyPatterns, conversations, loading };
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

  const agentList = Object.entries(agentMap)
    .map(([name, d]) => {
      const sorted = [...d.rts].sort((a, b) => a - b);
      const rawMedian = med(sorted);
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

// ═══ INSIGHTS TAB ═══

function InsightsTab({ data }) {
  const parseWeekDate = s => { const d = new Date(s + ", 2026"); return isNaN(d) ? 0 : d.getTime(); };

  // Build sorted week list from available data
  const weekLabels = [...new Set([
    ...data.filteredConvos.map(c => c.week_label).filter(Boolean),
  ])].sort((a, b) => parseWeekDate(a) - parseWeekDate(b));

  const latestWeek = weekLabels.length > 0 ? weekLabels[weekLabels.length - 1] : null;

  const [selectedWeek, setSelectedWeek] = useState(latestWeek);
  const [showPrevious, setShowPrevious] = useState(false);
  const [view, setView] = useState("weekly"); // "weekly" | "monthly"

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

  // Previous weeks = all except the most recent
  const previousWeeks = weekLabels.slice(0, -1).reverse();

  return (
    <>
      {/* View toggle: Weekly / Monthly */}
      <div style={{
        display: "flex", gap: 4, marginBottom: 24, marginTop: 8,
        background: C.borderLight, borderRadius: 10, padding: 4, width: "fit-content",
      }}>
        {[{ key: "weekly", label: "Weekly" }, { key: "monthly", label: "Monthly" }].map(v => (
          <button key={v.key} onClick={() => setView(v.key)} style={{
            border: "none", cursor: "pointer",
            padding: "8px 20px", borderRadius: 8, fontSize: 13, fontWeight: 600,
            background: view === v.key ? C.white : "transparent",
            color: view === v.key ? C.navy : C.textLight,
            boxShadow: view === v.key ? "0 1px 3px rgba(0,0,0,0.1)" : "none",
            transition: "all 0.15s ease",
          }}>{v.label}</button>
        ))}
      </div>

      {view === "weekly" && (
        <>
          {/* Current week header */}
          {latestWeek && (
            <Card style={{ marginBottom: 20 }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <div>
                  <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, color: C.textLight, marginBottom: 4 }}>Current Week</div>
                  <div style={{ fontSize: 18, fontWeight: 800, color: C.navy }}>Week of {latestWeek}</div>
                </div>
                <div style={{
                  padding: "6px 14px", borderRadius: 20, fontSize: 11, fontWeight: 700,
                  background: C.borderLight, color: C.textLight,
                }}>No insights yet</div>
              </div>
            </Card>
          )}

          {/* Empty state for current week */}
          <Card style={{ padding: 48, textAlign: "center", marginBottom: 32 }}>
            <div style={{ fontSize: 32, marginBottom: 12, opacity: 0.3 }}>◆</div>
            <div style={{ fontSize: 15, fontWeight: 700, color: C.textMid, marginBottom: 6 }}>Weekly insights will appear here</div>
            <div style={{ fontSize: 13, color: C.textLight, maxWidth: 400, margin: "0 auto", lineHeight: 1.6 }}>
              Signals, trends, and action items for the week of {latestWeek || "—"} will be added soon.
            </div>
          </Card>

          {/* Previous weeks toggle */}
          {previousWeeks.length > 0 && (
            <div style={{ marginBottom: 24 }}>
              <button onClick={() => setShowPrevious(!showPrevious)} style={{
                border: "none", cursor: "pointer", background: "transparent",
                display: "flex", alignItems: "center", gap: 8,
                padding: "10px 0", fontSize: 13, fontWeight: 600, color: C.textLight,
              }}>
                <span style={{
                  display: "inline-flex", alignItems: "center", justifyContent: "center",
                  width: 20, height: 20, borderRadius: 6, background: C.borderLight,
                  fontSize: 10, transition: "transform 0.2s ease",
                  transform: showPrevious ? "rotate(90deg)" : "rotate(0deg)",
                }}>▶</span>
                Previous Weeks ({previousWeeks.length})
              </button>

              {showPrevious && (
                <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 8 }}>
                  {previousWeeks.map(wl => (
                    <Card key={wl} style={{ padding: "16px 22px" }}>
                      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                        <div style={{ fontSize: 14, fontWeight: 700, color: C.navy }}>Week of {wl}</div>
                        <div style={{
                          padding: "4px 12px", borderRadius: 16, fontSize: 11, fontWeight: 600,
                          background: C.borderLight, color: C.textLight,
                        }}>No insights</div>
                      </div>
                    </Card>
                  ))}
                </div>
              )}
            </div>
          )}
        </>
      )}

      {view === "monthly" && (
        <>
          {/* Month selector pills */}
          {monthKeys.length > 0 && (
            <div style={{
              display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 20,
            }}>
              {monthKeys.map(mk => (
                <button key={mk} onClick={() => setSelectedMonth(mk)} style={{
                  border: `1px solid ${selectedMonth === mk ? C.navy : C.border}`,
                  background: selectedMonth === mk ? C.navy : C.white,
                  color: selectedMonth === mk ? "white" : C.textMid,
                  borderRadius: 8, padding: "8px 16px", fontSize: 13, fontWeight: 600,
                  cursor: "pointer", transition: "all 0.15s ease",
                }}>{mk}</button>
              ))}
            </div>
          )}

          {/* Monthly empty state */}
          <Card style={{ padding: 48, textAlign: "center" }}>
            <div style={{ fontSize: 32, marginBottom: 12, opacity: 0.3 }}>◈</div>
            <div style={{ fontSize: 15, fontWeight: 700, color: C.textMid, marginBottom: 6 }}>Monthly insights will appear here</div>
            <div style={{ fontSize: 13, color: C.textLight, maxWidth: 400, margin: "0 auto", lineHeight: 1.6 }}>
              A consolidated monthly view for {selectedMonth || "—"} covering trends, patterns, and recommendations across all weeks.
            </div>
          </Card>
        </>
      )}
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
                      ["Messages", agent.messages, null],
                      ...(agent.medianRT != null ? [["Median RT", `${agent.medianRT}m`, null]] : []),
                      ...(agent.meanRT != null ? [["Mean RT", `${agent.meanRT}m`, null]] : []),
                      ...(agent.p90 != null ? [["P90 RT", `${agent.p90}m`, null]] : []),
                      ...(agent.p25 != null ? [["P25 RT", `${agent.p25}m`, null]] : []),
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

// ═══ MAIN DASHBOARD ═══

export default function Dashboard() {
  const [tab, setTab] = useState("insights");
  const [dateRange, setDateRange] = useState({ from: "", to: "" });
  const [inboxFilter, setInboxFilter] = useState("all");
  const { weeklyStats, categorizations, insights, hourlyPatterns, conversations, loading } = useSupabase();

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
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <div>
              <h1 style={{ fontSize: 20, fontWeight: 800, color: C.text, margin: 0, letterSpacing: -0.5 }}>{tabLabel}</h1>
              <div style={{ fontSize: 12, color: C.textLight, marginTop: 2 }}>
                {isFiltered
                  ? `Showing ${data.filteredConvos.length} filtered conversations`
                  : `${data.totalConversations} conversations across ${data.weekCount} week${data.weekCount !== 1 ? "s" : ""}`}
              </div>
            </div>
            {data.inboxOptions.length > 1 && (
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
          {/* Row 2: Week presets + date range */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
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
          </div>
        </header>

        {/* Scrollable content */}
        <main style={{ flex: 1, overflow: "auto", padding: "24px 32px 64px" }}>
          <div style={{ maxWidth: 1100, margin: "0 auto" }}>
            {tab === "overview" && <OverviewTab data={data} avgWeeklyVol={avgWeeklyVol} volRtData={volRtData} />}
            {tab === "insights" && <InsightsTab data={data} />}
            {tab === "categories" && <CategoriesTab data={data} weekly={data.weekly} TOP4={TOP4} />}
            {tab === "volume" && <VolumeTab data={data} avgWeeklyVol={avgWeeklyVol} volRtData={volRtData} />}
            {tab === "team" && <TeamTab data={data} />}
          </div>
        </main>
      </div>
    </div>
  );
}
