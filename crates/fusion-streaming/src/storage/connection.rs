use std::fmt::{Display, Formatter};

pub trait ConnectionTrait {
    fn info(&self) -> String;
}

impl Display for dyn ConnectionTrait {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let pretty = f.alternate();
        f.write_str(&"---------")
    }
}