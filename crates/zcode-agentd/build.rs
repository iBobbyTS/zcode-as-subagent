use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");
    let revision = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"])
        .map(|output| if output.is_empty() { "false" } else { "true" })
        .unwrap_or("unknown");
    println!("cargo:rustc-env=ZAS_SOURCE_REVISION={revision}");
    println!("cargo:rustc-env=ZAS_SOURCE_DIRTY={dirty}");
}

fn git(arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir("../..")
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
