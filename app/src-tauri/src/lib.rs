use serde::Deserialize;

#[derive(Deserialize)]
struct GreetArgs {
    name: String,
}

#[tauri::command]
fn greet(args: GreetArgs) -> String {
    format!("Hello, {}! You've been greeted from Rust.", args.name)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![greet])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
