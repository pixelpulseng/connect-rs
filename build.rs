// Embed the git version at build time (the C++ generated version.cpp from
// `git describe` in the Makefile). An externally set GITVERSION wins, so
// tarball/package builds without .git can still stamp the binary.

use std::process::Command;

fn main() {
    let ver = std::env::var("GITVERSION")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["describe", "--always", "--dirty", "--tags"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .filter(|s| !s.is_empty())
        });
    if let Some(v) = ver {
        println!("cargo:rustc-env=GITVERSION={v}");
    }
    println!("cargo:rerun-if-env-changed=GITVERSION");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
}
