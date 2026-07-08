// USB device discovery and shared USB plumbing (usb.cpp + usb_device.hpp),
// built on nusb (no libusb, no dedicated event thread).

pub mod bootloader;
pub mod cee;
pub mod m1k;

use crate::device::{AnyDevice, ClientHandle, DevicePtr, ServerState};
use crate::jsonutil::*;
use crate::streaming::{Backend, StreamingDevice};
use futures_util::StreamExt;
use nusb::transfer::{ControlIn, ControlOut, ControlType, Recipient};
use nusb::MaybeFuture;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

const NONOLITH_VID: u16 = 0x59e3;
const CEE_PID: u16 = 0xCEE1;
const BOOTLOADER_PID: u16 = 0xBBBB;
const BOOTLOADER_PID2: u16 = 0xb003;
const ADI_VID: u16 = 0x0456;
const M1K_PID: u16 = 0xCEE2;
const ADI_VID2: u16 = 0x064B;
const M1K_PID2: u16 = 0x784C;

/// Control-transfer timeout, matching the C++ (usb_device.hpp).
const CONTROL_TIMEOUT: Duration = Duration::from_millis(25);

/// Opened USB device handle with libusb-style synchronous control
/// transfers. Return values mimic libusb: >= 0 is the transferred byte
/// count, negative is an error.
#[derive(Clone)]
pub struct UsbHandle {
    pub device: nusb::Device,
    pub debug_label: &'static str,
}

fn decompose(bm_request_type: u8) -> (ControlType, Recipient) {
    let ct = match (bm_request_type >> 5) & 0x3 {
        0 => ControlType::Standard,
        1 => ControlType::Class,
        _ => ControlType::Vendor,
    };
    let rc = match bm_request_type & 0x1f {
        0 => Recipient::Device,
        1 => Recipient::Interface,
        2 => Recipient::Endpoint,
        _ => Recipient::Other,
    };
    (ct, rc)
}

impl UsbHandle {
    /// IN control transfer; returns (status, data).
    pub fn control_in(&self, bm_request_type: u8, b_request: u8, w_value: u16, w_index: u16, w_length: u16) -> (i32, Vec<u8>) {
        let (control_type, recipient) = decompose(bm_request_type);
        let r = self
            .device
            .control_in(
                ControlIn {
                    control_type,
                    recipient,
                    request: b_request,
                    value: w_value,
                    index: w_index,
                    length: w_length,
                },
                CONTROL_TIMEOUT,
            )
            .wait();
        match r {
            Ok(data) => (data.len() as i32, data),
            Err(e) => {
                eprintln!("{}: control IN 0x{b_request:02x} failed: {e}", self.debug_label);
                (-1, Vec::new())
            }
        }
    }

    /// OUT control transfer; returns status.
    pub fn control_out(&self, bm_request_type: u8, b_request: u8, w_value: u16, w_index: u16, data: &[u8]) -> i32 {
        let (control_type, recipient) = decompose(bm_request_type);
        let r = self
            .device
            .control_out(
                ControlOut {
                    control_type,
                    recipient,
                    request: b_request,
                    value: w_value,
                    index: w_index,
                    data,
                },
                CONTROL_TIMEOUT,
            )
            .wait();
        match r {
            Ok(()) => data.len() as i32,
            Err(e) => {
                eprintln!("{}: control OUT 0x{b_request:02x} failed: {e}", self.debug_label);
                -1
            }
        }
    }

    /// Read an ASCII string from a vendor IN request (version strings).
    pub fn read_string(&self, b_request: u8, w_value: u16, w_index: u16) -> String {
        let (r, data) = self.control_in(0xC0, b_request, w_value, w_index, 64);
        if r < 0 {
            return String::new();
        }
        let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
        String::from_utf8_lossy(&data[..end]).into_owned()
    }
}

/// controlTransfer / enterBootloader WS passthrough
/// (usb.cpp USB_device::processMessage). Shared by CEE, M1K, bootloader.
pub fn handle_usb_message(handle: &UsbHandle, client: &ClientHandle, cmd: &str, n: &Value) -> Result<bool> {
    match cmd {
        "controlTransfer" => {
            let id = json_int_prop_def(n, "id", 0);
            let bm_request_type = json_int_prop_def(n, "bmRequestType", 0xC0) as u8;
            let b_request = json_int_prop(n, "bRequest")? as u8;
            let w_value = json_int_prop_def(n, "wValue", 0) as u16;
            let w_index = json_int_prop_def(n, "wIndex", 0) as u16;

            let is_in = bm_request_type & 0x80 != 0;

            let mut reply = serde_json::Map::new();
            reply.insert("_action".into(), json!("return"));
            reply.insert("id".into(), json!(id));

            let ret;
            if is_in {
                let mut w_length = json_int_prop_def(n, "wLength", 64);
                w_length = w_length.clamp(0, 64);
                let (r, data) = handle.control_in(bm_request_type, b_request, w_value, w_index, w_length as u16);
                ret = r;
                if r >= 0 {
                    reply.insert("data".into(), json!(data));
                }
            } else {
                let data = n.get("data").ok_or_else(|| Error::new("JSON missing property: data"))?;
                let bytes: Vec<u8> = match data {
                    Value::Array(a) => a
                        .iter()
                        .map(|v| v.as_f64().unwrap_or(0.0) as i64 as u8)
                        .collect(),
                    Value::String(s) => s.as_bytes().to_vec(),
                    _ => Vec::new(),
                };
                ret = handle.control_out(bm_request_type, b_request, w_value, w_index, &bytes);
            }

            reply.insert("status".into(), json!(ret));
            client.send_json(Value::Object(reply));
            Ok(true)
        }
        "enterBootloader" => {
            print!("enterBootloader: ");
            // The C++ issues an IN request 0xBB with wLength 100; the device
            // reboots regardless of transfer status.
            let (r, _) = handle.control_in(0xC0, 0xBB, 0, 0, 100);
            println!("return {r}");
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// USB passthrough for streaming devices (extracts the handle from the
/// backend).
pub fn process_usb_message(dev: &mut StreamingDevice, client: &ClientHandle, cmd: &str, n: &Value) -> Result<bool> {
    let handle = match &dev.backend {
        Backend::M1k(b) => b.handle.clone(),
        Backend::Cee(b) => b.handle.clone(),
        Backend::Test(_) => return Ok(false),
    };
    handle_usb_message(&handle, client, cmd, n)
}

/// Start USB discovery: initial scan plus hotplug watching.
pub fn start(state: Arc<ServerState>) {
    tokio::spawn(async move {
        let mut active: HashMap<nusb::DeviceId, DevicePtr> = HashMap::new();

        match nusb::list_devices().await {
            Ok(devices) => {
                for info in devices {
                    device_added(&state, &mut active, info).await;
                }
            }
            Err(e) => eprintln!("Could not enumerate USB devices: {e}"),
        }

        let watch = match nusb::watch_devices() {
            Ok(w) => w,
            Err(e) => {
                eprintln!("Could not watch USB hotplug events: {e}");
                return;
            }
        };
        let mut watch = watch;
        while let Some(event) = watch.next().await {
            match event {
                nusb::hotplug::HotplugEvent::Connected(info) => {
                    device_added(&state, &mut active, info).await;
                }
                nusb::hotplug::HotplugEvent::Disconnected(id) => {
                    if let Some(dev) = active.remove(&id) {
                        eprintln!("Device removed");
                        state.remove_device(&dev);
                    }
                }
            }
        }
    });
}

async fn device_added(state: &Arc<ServerState>, active: &mut HashMap<nusb::DeviceId, DevicePtr>, info: nusb::DeviceInfo) {
    let vid = info.vendor_id();
    let pid = info.product_id();

    let kind = if (vid == NONOLITH_VID || vid == 0x9999) && pid == CEE_PID {
        "cee"
    } else if vid == NONOLITH_VID && (pid == BOOTLOADER_PID || pid == BOOTLOADER_PID2) {
        "bootloader"
    } else if (vid == ADI_VID && pid == M1K_PID) || (vid == ADI_VID2 && pid == M1K_PID2) {
        "m1k"
    } else {
        return;
    };

    let id = info.id();
    // Serial from the descriptor cache; trimmed of NUL padding (Q10 fixed).
    let serial = info
        .serial_number()
        .unwrap_or("0")
        .trim_end_matches('\0')
        .to_string();

    let device = match info.open().await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error initializing new device (open: {e}). Ignoring.");
            return;
        }
    };

    let dev_ptr: DevicePtr = match kind {
        "m1k" => {
            let handle = UsbHandle { device, debug_label: "M1K" };
            match m1k::create(handle, serial) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Error initializing M1K: {e}. Ignoring.");
                    return;
                }
            }
        }
        "cee" => {
            let handle = UsbHandle { device, debug_label: "CEE" };
            match cee::create(handle, serial) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Error initializing CEE: {e}. Ignoring.");
                    return;
                }
            }
        }
        _ => {
            let handle = UsbHandle { device, debug_label: "bootloader" };
            let d = bootloader::BootloaderDevice::new(handle, serial);
            Arc::new(Mutex::new(AnyDevice::Bootloader(d)))
        }
    };

    // Give streaming backends a weak self-reference for their transfer tasks
    if let AnyDevice::Streaming(sd) = &mut *dev_ptr.lock().unwrap() {
        let weak = Arc::downgrade(&dev_ptr);
        match &mut sd.backend {
            Backend::M1k(b) => b.self_ref = weak,
            Backend::Cee(b) => b.self_ref = weak,
            Backend::Test(_) => {}
        }
    }

    active.insert(id, dev_ptr.clone());
    state.add_device(dev_ptr);
}

/// Shared helper: weak self-reference type used by streaming backends.
pub type SelfRef = Weak<Mutex<AnyDevice>>;
