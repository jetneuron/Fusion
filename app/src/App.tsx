import { useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";

type Phase = "idle" | "running" | "done" | "failed";

function App() {
  const [phase, setPhase] = useState<Phase>("idle");
  const [detail, setDetail] = useState("");
  const [fileName, setFileName] = useState("");
  const [graphId, setGraphId] = useState("");
  const fileRef = useRef<HTMLInputElement>(null);

  async function handleFile(file: File | undefined) {
    if (!file) return;
    setFileName(file.name);
    setDetail("");
    setGraphId("");
    setPhase("running");

    try {
      const yaml = await file.text();
      const id = await invoke<string>("run_graph", { yaml });
      setGraphId(id);
      pollStatus(id);
    } catch (e) {
      setPhase("failed");
      setDetail(String(e));
    }
  }

  // Poll until the graph leaves "running" (or times out).
  async function pollStatus(id: string) {
    for (let i = 0; i < 200; i++) {
      await new Promise((r) => setTimeout(r, 300));
      const s = await invoke<string | null>("graph_status", { id });
      if (s === null || s === "running") continue;
      if (s === "done") {
        setPhase("done");
        setDetail("executed successfully");
      } else {
        setPhase("failed");
        setDetail(s.replace(/^failed:\s*/, ""));
      }
      return;
    }
    setPhase("failed");
    setDetail("timeout waiting for graph");
  }

  const statusStyle: Record<Phase, React.CSSProperties> = {
    idle: { color: "#888" },
    running: { color: "#b8860b" },
    done: { color: "#2e7d32" },
    failed: { color: "#c62828" },
  };

  return (
    <main style={{ fontFamily: "system-ui, sans-serif", padding: 24, maxWidth: 640 }}>
      <h1>Fusion</h1>
      <p style={{ color: "#666" }}>Upload a YAML graph to execute it.</p>

      <button
        onClick={() => fileRef.current?.click()}
        style={{
          padding: "8px 16px",
          border: "1px solid #ccc",
          borderRadius: 6,
          background: "#f5f5f5",
          cursor: "pointer",
        }}
      >
        Choose YAML file…
      </button>
      <input
        ref={fileRef}
        type="file"
        accept=".yaml,.yml"
        hidden
        onChange={(e) => handleFile(e.target.files?.[0])}
      />
      {fileName && <span style={{ marginLeft: 12 }}>{fileName}</span>}

      <div style={{ marginTop: 16 }}>
        <span style={{ fontWeight: 600 }}>Status: </span>
        <span style={statusStyle[phase]}>
          {phase === "idle" && "idle"}
          {phase === "running" && "running…"}
          {phase === "done" && "done"}
          {phase === "failed" && "failed"}
        </span>
        {graphId && (
          <span style={{ marginLeft: 12, color: "#666" }}>graph: {graphId}</span>
        )}
        {detail && <p style={{ color: "#666", fontSize: 13 }}>{detail}</p>}
      </div>
    </main>
  );
}

export default App;
