import { useState } from "react";
import { invoke } from "@tauri-apps/api/core";

function App() {
  const [greeting, setGreeting] = useState("");

  async function handleGreet() {
    const msg = await invoke("greet", { name: "Fusion" });
    setGreeting(msg as string);
  }

  return (
    <main className="container">
      <h1>Welcome to Fusion</h1>
      <p>Click the button to call the Rust backend:</p>
      <button onClick={handleGreet}>Greet</button>
      {greeting && <p className="result">{greeting}</p>}
    </main>
  );
}

export default App;
