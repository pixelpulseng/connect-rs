// WebSocket protocol connection logic, ported from websocket_service.cpp.
// Transport-independent: WsConn talks to the client through a ClientHandle,
// so the full dispatch stack is testable without sockets.

use crate::device::{ClientHandle, DevicePtr, ServerState};
use crate::jsonutil::*;
use serde_json::{json, Value};
use std::sync::Arc;

pub struct WsConn {
    pub client: ClientHandle,
    pub device: Option<DevicePtr>,
}

impl WsConn {
    /// On open: serverHello, then the device list.
    pub fn new(state: &Arc<ServerState>, client: ClientHandle) -> WsConn {
        client.send_json(json!({
            "_action": "serverHello",
            "server": "Nonolith Connect",
            "version": crate::SERVER_VERSION,
            "gitVersion": crate::server_git_version(),
        }));
        let conn = WsConn {
            client,
            device: None,
        };
        conn.send_device_list(state);
        conn
    }

    pub fn send_device_list(&self, state: &Arc<ServerState>) {
        self.client.send_json(json!({
            "_action": "devices",
            "devices": state.devices_json(),
        }));
    }

    fn select_device(&mut self, dev: DevicePtr) {
        if let Some(old) = self.device.take() {
            old.lock().unwrap().on_client_detach(self.client.id);
        }
        dev.lock().unwrap().on_client_attach(&self.client);
        self.device = Some(dev);
    }

    pub fn on_message(&mut self, state: &Arc<ServerState>, msg: &str) {
        if state.debug {
            println!("RXD: {msg}");
        }

        let mut id: i64 = 0;

        let result = (|| -> Result<()> {
            let n: Value =
                serde_json::from_str(msg).map_err(|e| Error(format!("JSON parse error: {e}")))?;
            let cmd = json_string_prop(&n, "_cmd")?;
            id = json_int_prop_def(&n, "id", 0);

            if cmd == "selectDevice" {
                let dev_id = json_string_prop(&n, "id")?;
                if let Some(dev) = state.device_by_id(&dev_id) {
                    self.select_device(dev);
                } else {
                    eprintln!("Error selecting device {dev_id}");
                }
                return Ok(());
            }

            let Some(dev) = &self.device else {
                eprintln!("selectDevice before using other WS calls");
                return Ok(());
            };

            if dev
                .lock()
                .unwrap()
                .process_message(&self.client, &cmd, &n)?
            {
                return Ok(());
            }

            eprintln!("Unknown command {cmd}");
            Ok(())
        })();

        if let Err(e) = result {
            eprintln!("WS JSON error: {e}");
            let mut err = serde_json::Map::new();
            err.insert("_action".into(), json!("error"));
            err.insert("error".into(), json!(e.0));
            if id != 0 {
                err.insert("id".into(), json!(id));
            }
            self.client.send_json(Value::Object(err));
        }
    }

    pub fn on_close(&mut self) {
        if let Some(dev) = self.device.take() {
            dev.lock().unwrap().on_client_detach(self.client.id);
        }
    }
}

impl Drop for WsConn {
    fn drop(&mut self) {
        self.on_close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{AnyDevice, OutMsg};
    use crate::streaming::make_test_device;
    use std::sync::Mutex;

    fn setup() -> (Arc<ServerState>, WsConn, crate::device::ClientReceiver) {
        let state = ServerState::new(false, false);
        state.add_device(Arc::new(Mutex::new(AnyDevice::Streaming(
            make_test_device("WS1"),
        ))));
        let (client, rx) = ClientHandle::pair();
        let conn = WsConn::new(&state, client);
        (state, conn, rx)
    }

    fn drain(rx: &mut crate::device::ClientReceiver) -> Vec<Value> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let OutMsg::Json(v) = m {
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn hello_then_devices_on_connect() {
        let (_state, _conn, mut rx) = setup();
        let msgs = drain(&mut rx);
        assert_eq!(msgs[0]["_action"], "serverHello");
        assert_eq!(msgs[0]["server"], "Nonolith Connect");
        assert_eq!(msgs[0]["version"], "1.3");
        assert_eq!(msgs[1]["_action"], "devices");
        assert!(msgs[1]["devices"]["com.nonolithlabs.test~WS1"].is_object());
    }

    #[test]
    fn parse_error_yields_error_action() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(&state, "this is not json");
        let msgs = drain(&mut rx);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["_action"], "error");
        assert!(msgs[0].get("id").is_none());
    }

    #[test]
    fn missing_cmd_yields_error() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(&state, r#"{"foo": 1}"#);
        let msgs = drain(&mut rx);
        assert_eq!(msgs[0]["_action"], "error");
    }

    #[test]
    fn commands_before_select_are_dropped_silently() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(&state, r#"{"_cmd": "startCapture"}"#);
        assert!(drain(&mut rx).is_empty());
    }

    #[test]
    fn select_unknown_device_is_silent_and_connection_survives() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(&state, r#"{"_cmd": "selectDevice", "id": "nope~X"}"#);
        assert!(drain(&mut rx).is_empty());
        conn.on_message(&state, "still not json");
        assert_eq!(drain(&mut rx)[0]["_action"], "error");
    }

    #[test]
    fn select_device_sends_device_config() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(
            &state,
            r#"{"_cmd": "selectDevice", "id": "com.nonolithlabs.test~WS1"}"#,
        );
        let msgs = drain(&mut rx);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["_action"], "deviceConfig");
        assert_eq!(msgs[0]["device"]["id"], "com.nonolithlabs.test~WS1");
    }

    #[test]
    fn command_errors_carry_id() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(
            &state,
            r#"{"_cmd": "selectDevice", "id": "com.nonolithlabs.test~WS1"}"#,
        );
        drain(&mut rx);
        conn.on_message(&state, r#"{"_cmd": "set", "id": 42, "channel": "zz"}"#);
        let msgs = drain(&mut rx);
        assert_eq!(msgs[0]["_action"], "error");
        assert_eq!(msgs[0]["error"], "Channel not found");
        assert_eq!(msgs[0]["id"], 42);
    }

    #[test]
    fn unknown_command_gets_no_reply() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(
            &state,
            r#"{"_cmd": "selectDevice", "id": "com.nonolithlabs.test~WS1"}"#,
        );
        drain(&mut rx);
        // Q6 kept: unknown commands are logged, no reply
        conn.on_message(&state, r#"{"_cmd": "frobnicate", "id": 7}"#);
        assert!(drain(&mut rx).is_empty());
    }

    #[test]
    fn full_set_flow_broadcasts_output_changed() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(
            &state,
            r#"{"_cmd": "selectDevice", "id": "com.nonolithlabs.test~WS1"}"#,
        );
        drain(&mut rx);
        conn.on_message(
            &state,
            r#"{"_cmd": "set", "channel": "a", "mode": 1, "source": "constant", "value": 2.5}"#,
        );
        let msgs = drain(&mut rx);
        assert_eq!(msgs[0]["_action"], "outputChanged");
        assert_eq!(msgs[0]["channel"], "a");
        assert_eq!(msgs[0]["value"], 2.5);
        assert_eq!(msgs[0]["effective"], false);
        assert_eq!(msgs[0]["startSample"], 0);
    }

    #[test]
    fn detach_on_close() {
        let (state, mut conn, mut rx) = setup();
        drain(&mut rx);
        conn.on_message(
            &state,
            r#"{"_cmd": "selectDevice", "id": "com.nonolithlabs.test~WS1"}"#,
        );
        let dev = state.device_by_id("com.nonolithlabs.test~WS1").unwrap();
        {
            let d = dev.lock().unwrap();
            if let AnyDevice::Streaming(sd) = &*d {
                assert_eq!(sd.connections.len(), 1);
            }
        }
        conn.on_close();
        let d = dev.lock().unwrap();
        if let AnyDevice::Streaming(sd) = &*d {
            assert!(sd.connections.is_empty());
        }
    }
}
