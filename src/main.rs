// Nonolith Connect — Rust port entry point (server.cpp main()).

use connect::device::ServerState;

fn main() {
    let mut debug = false;
    let mut allow_remote = false;
    let mut allow_any_origin = false;
    let mut port: u16 = 9003;

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "debug" => debug = true,
            "allow-remote" => allow_remote = true,
            "allow-any-origin" => allow_any_origin = true,
            a => {
                if let Some(p) = a.strip_prefix("port=") {
                    port = p.parse().unwrap_or(9003);
                }
            }
        }
    }

    let rt = tokio::runtime::Runtime::new().expect("failed to start tokio runtime");
    rt.block_on(async {
        let state = ServerState::new(debug, allow_any_origin);

        connect::usb::start(state.clone());

        if let Err(e) = connect::net::run(state, port, allow_remote).await {
            eprintln!("Exception: {e}");
            std::process::exit(1);
        }
    });
}
