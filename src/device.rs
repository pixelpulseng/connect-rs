// Device registry and client-connection handles.
//
// A ClientHandle is the Rust equivalent of the C++ ClientConn*: a cheap
// cloneable handle that device code uses to push JSON/binary frames to one
// WebSocket connection. Frames are queued on an unbounded channel and
// written in order by the connection's writer task, mirroring the C++
// per-connection write queue.

use crate::streaming::StreamingDevice;
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};

#[derive(Debug)]
pub enum OutMsg {
    Json(Value),
    Binary(Vec<u8>),
}

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Once this many bytes of binary data frames are queued for a connection,
/// further data frames are dropped until the client drains the backlog.
/// Frames are self-describing (idx/sampleIndex), so clients tolerate gaps.
/// JSON messages are never dropped — they carry protocol state.
pub const BINARY_BACKLOG_LIMIT: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct ClientHandle {
    pub id: u64,
    tx: mpsc::UnboundedSender<OutMsg>,
    queued_binary: Arc<AtomicUsize>,
    dropping: Arc<AtomicBool>,
}

/// Receiving end of a connection's outgoing queue; keeps the backlog
/// accounting in sync as the writer task drains messages.
pub struct ClientReceiver {
    rx: mpsc::UnboundedReceiver<OutMsg>,
    queued_binary: Arc<AtomicUsize>,
}

impl ClientHandle {
    /// Create a handle plus the receiving end (tests and connection setup).
    pub fn pair() -> (ClientHandle, ClientReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let queued_binary = Arc::new(AtomicUsize::new(0));
        (
            ClientHandle {
                id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
                tx,
                queued_binary: queued_binary.clone(),
                dropping: Arc::new(AtomicBool::new(false)),
            },
            ClientReceiver { rx, queued_binary },
        )
    }

    pub fn send_json(&self, v: Value) {
        let _ = self.tx.send(OutMsg::Json(v));
    }

    pub fn send_binary(&self, data: Vec<u8>) {
        let queued = self.queued_binary.load(Ordering::Relaxed);
        if queued + data.len() > BINARY_BACKLOG_LIMIT {
            if !self.dropping.swap(true, Ordering::Relaxed) {
                eprintln!(
                    "Client {} is not keeping up ({queued} bytes queued); dropping data frames",
                    self.id
                );
            }
            return;
        }
        if self.dropping.swap(false, Ordering::Relaxed) {
            eprintln!("Client {} caught up; resuming data frames", self.id);
        }
        self.queued_binary.fetch_add(data.len(), Ordering::Relaxed);
        let _ = self.tx.send(OutMsg::Binary(data));
    }
}

impl ClientReceiver {
    pub async fn recv(&mut self) -> Option<OutMsg> {
        let m = self.rx.recv().await;
        if let Some(OutMsg::Binary(b)) = &m {
            self.queued_binary.fetch_sub(b.len(), Ordering::Relaxed);
        }
        m
    }

    pub fn try_recv(&mut self) -> Result<OutMsg, mpsc::error::TryRecvError> {
        let m = self.rx.try_recv();
        if let Ok(OutMsg::Binary(b)) = &m {
            self.queued_binary.fetch_sub(b.len(), Ordering::Relaxed);
        }
        m
    }
}

/// The device types the server can own. Streaming covers CEE/M1K/test
/// devices; the Xmega bootloader is a non-streaming USB device.
pub enum AnyDevice {
    Streaming(StreamingDevice),
    Bootloader(crate::usb::bootloader::BootloaderDevice),
}

pub type DevicePtr = Arc<Mutex<AnyDevice>>;

impl AnyDevice {
    pub fn model(&self) -> String {
        match self {
            AnyDevice::Streaming(d) => d.model.clone(),
            AnyDevice::Bootloader(d) => d.model(),
        }
    }

    pub fn serial(&self) -> String {
        match self {
            AnyDevice::Streaming(d) => d.serial.clone(),
            AnyDevice::Bootloader(d) => d.serial.clone(),
        }
    }

    pub fn get_id(&self) -> String {
        format!("{}~{}", self.model(), self.serial())
    }

    /// The devices-list entry (device.cpp Device::toJSON)
    pub fn to_json(&self) -> Value {
        let (hw, fw) = match self {
            AnyDevice::Streaming(d) => (d.hw_version.clone(), d.fw_version.clone()),
            AnyDevice::Bootloader(d) => (d.hw_version(), "unknown".to_string()),
        };
        serde_json::json!({
            "id": self.get_id(),
            "model": self.model(),
            "hwVersion": hw,
            "fwVersion": fw,
            "serial": self.serial(),
        })
    }

    pub fn on_client_attach(&mut self, client: &ClientHandle) {
        match self {
            AnyDevice::Streaming(d) => d.on_client_attach(client),
            AnyDevice::Bootloader(d) => d.on_client_attach(client),
        }
    }

    pub fn on_client_detach(&mut self, client_id: u64) {
        match self {
            AnyDevice::Streaming(d) => d.on_client_detach(client_id),
            AnyDevice::Bootloader(d) => d.on_client_detach(client_id),
        }
    }

    /// USB device removed: notify attached clients and drop listeners.
    pub fn on_disconnect(&mut self) {
        match self {
            AnyDevice::Streaming(d) => d.on_disconnect(),
            AnyDevice::Bootloader(d) => {
                d.broadcast_json(serde_json::json!({"_action": "deviceDisconnected"}))
            }
        }
    }

    /// WS command dispatch. Returns Ok(true) if handled, Ok(false) for an
    /// unknown command (logged, no reply — Q6), Err -> `_action:"error"`.
    pub fn process_message(
        &mut self,
        client: &ClientHandle,
        cmd: &str,
        n: &Value,
    ) -> crate::jsonutil::Result<bool> {
        match self {
            AnyDevice::Streaming(d) => d.process_message(client, cmd, n),
            AnyDevice::Bootloader(d) => d.process_message(client, cmd, n),
        }
    }
}

pub struct ServerState {
    pub devices: Mutex<Vec<DevicePtr>>,
    pub device_list_changed: broadcast::Sender<()>,
    pub debug: bool,
    pub allow_any_origin: bool,
}

impl ServerState {
    pub fn new(debug: bool, allow_any_origin: bool) -> Arc<ServerState> {
        let (tx, _) = broadcast::channel(16);
        Arc::new(ServerState {
            devices: Mutex::new(Vec::new()),
            device_list_changed: tx,
            debug,
            allow_any_origin,
        })
    }

    /// getDeviceById: exact `<model>~<serial>` match, or `<model>*` prefix
    /// matching the first device of that model. Empty ids return None
    /// (Q2 fixed: the C++ crashed on "").
    pub fn device_by_id(&self, id: &str) -> Option<DevicePtr> {
        if id.is_empty() {
            return None;
        }
        let devices = self.devices.lock().unwrap();
        if let Some(model) = id.strip_suffix('*') {
            devices
                .iter()
                .find(|d| d.lock().unwrap().model() == model)
                .cloned()
        } else {
            devices
                .iter()
                .find(|d| d.lock().unwrap().get_id() == id)
                .cloned()
        }
    }

    /// The `devices` object keyed by device id (jsonDevicesArray).
    pub fn devices_json(&self) -> Value {
        let devices = self.devices.lock().unwrap();
        let mut m = Map::new();
        for d in devices.iter() {
            let d = d.lock().unwrap();
            m.insert(d.get_id(), d.to_json());
        }
        Value::Object(m)
    }

    pub fn add_device(&self, dev: DevicePtr) {
        self.devices.lock().unwrap().push(dev);
        let _ = self.device_list_changed.send(());
    }

    pub fn remove_device(&self, dev: &DevicePtr) {
        {
            let mut devices = self.devices.lock().unwrap();
            devices.retain(|d| !Arc::ptr_eq(d, dev));
        }
        let _ = self.device_list_changed.send(());
        dev.lock().unwrap().on_disconnect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_backpressure_drops_when_backlogged() {
        let (client, mut rx) = ClientHandle::pair();
        let frame = vec![0u8; 1024 * 1024];
        for _ in 0..8 {
            client.send_binary(frame.clone());
        }
        // Backlog at the limit: the next data frame is dropped, but JSON
        // messages still go through.
        client.send_binary(frame.clone());
        client.send_json(serde_json::json!({"x": 1}));
        let (mut binary, mut json) = (0, 0);
        while let Ok(m) = rx.try_recv() {
            match m {
                OutMsg::Binary(_) => binary += 1,
                OutMsg::Json(_) => json += 1,
            }
        }
        assert_eq!(binary, 8);
        assert_eq!(json, 1);
        // Draining the queue resets the accounting; sends resume.
        client.send_binary(frame.clone());
        assert!(matches!(rx.try_recv(), Ok(OutMsg::Binary(_))));
    }
}
