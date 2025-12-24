use crate::utils::script::ScriptType::Lua;
use std::str::FromStr;

#[derive(Default)]
pub(crate) enum ScriptType {
    #[default]
    Lua
}
impl FromStr for ScriptType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lua" => Ok(Lua),
            &_ => unreachable!()
        }
    }
}

#[derive(Default)]
pub(crate) struct Script {
    pub script_type: ScriptType,
    pub code: String,
}