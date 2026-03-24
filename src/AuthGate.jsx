import { useState, useEffect } from "react";

const C = {
  navy: "#0A1628", navyMid: "#111B2E",
  blue: "#3B82F6", blueDark: "#2563EB",
  white: "#FFFFFF", bg: "#F1F5F9",
  border: "#E2E8F0", text: "#0F172A",
  textMid: "#475569", textLight: "#94A3B8",
  red: "#EF4444",
};

const SESSION_KEY = "px_dash_auth";

export default function AuthGate({ children }) {
  const [authed, setAuthed] = useState(() => sessionStorage.getItem(SESSION_KEY) === "1");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  if (authed) return children;

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    setLoading(true);

    try {
      const res = await fetch("/api/verify-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password }),
      });

      if (res.ok) {
        sessionStorage.setItem(SESSION_KEY, "1");
        setAuthed(true);
      } else {
        setError("Wrong password");
        setPassword("");
      }
    } catch {
      setError("Could not verify — try again");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center",
      background: C.bg, fontFamily: "'Inter', -apple-system, sans-serif",
    }}>
      <form onSubmit={handleSubmit} style={{
        background: C.white, borderRadius: 16, padding: "48px 40px", width: 380,
        boxShadow: "0 4px 24px rgba(0,0,0,0.08)", border: `1px solid ${C.border}`,
        display: "flex", flexDirection: "column", alignItems: "center", gap: 20,
      }}>
        {/* Logo / title */}
        <div style={{ textAlign: "center", marginBottom: 4 }}>
          <div style={{
            fontSize: 28, fontWeight: 800, color: C.navy, letterSpacing: -0.5,
          }}>
            PX Dashboard
          </div>
          <div style={{ fontSize: 13, color: C.textLight, marginTop: 4 }}>
            Enter password to continue
          </div>
        </div>

        {/* Password input */}
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          autoFocus
          style={{
            width: "100%", padding: "12px 16px", fontSize: 15, borderRadius: 10,
            border: `1.5px solid ${error ? C.red : C.border}`, outline: "none",
            background: C.bg, color: C.text, transition: "border 0.15s",
            boxSizing: "border-box",
          }}
          onFocus={(e) => (e.target.style.borderColor = C.blue)}
          onBlur={(e) => (e.target.style.borderColor = error ? C.red : C.border)}
        />

        {/* Error message */}
        {error && (
          <div style={{ fontSize: 13, color: C.red, fontWeight: 500, marginTop: -8 }}>
            {error}
          </div>
        )}

        {/* Submit */}
        <button type="submit" disabled={loading || !password} style={{
          width: "100%", padding: "12px 0", fontSize: 15, fontWeight: 700,
          borderRadius: 10, border: "none", cursor: loading ? "wait" : "pointer",
          background: password ? C.blue : C.border,
          color: password ? C.white : C.textLight,
          transition: "all 0.15s",
          opacity: loading ? 0.7 : 1,
        }}>
          {loading ? "Checking…" : "Enter"}
        </button>
      </form>
    </div>
  );
}
