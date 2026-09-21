use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rustc-env=REQWEST_VERSION={}", reqwest_version());
}

fn reqwest_version() -> String {
    let Ok(lock) = fs::read_to_string("Cargo.lock") else {
        return "unknown".to_string();
    };
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line == "name = \"reqwest\"" {
            if let Some(version) = lines
                .next()
                .and_then(|l| l.strip_prefix("version = \""))
                .and_then(|v| v.strip_suffix('"'))
            {
                return version.to_string();
            }
        }
    }
    "unknown".to_string()
}
