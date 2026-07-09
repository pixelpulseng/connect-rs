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

        tokio::select! {
            r = connect::net::run(state.clone(), port, allow_remote) => {
                if let Err(e) = r {
                    eprintln!("Exception: {e}");
                    std::process::exit(1);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("Interrupted; shutting down");
                shutdown(&state);
            }
        }
    });
}

/// Pause any running captures so devices stop streaming and USB interfaces
/// are released before the process exits.
fn shutdown(state: &ServerState) {
    let devices: Vec<_> = state.devices.lock().unwrap().clone();
    for dev in devices {
        if let connect::device::AnyDevice::Streaming(d) = &mut *dev.lock().unwrap() {
            d.pause_capture();
        }
    }
}
