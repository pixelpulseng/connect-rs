// Xmega bootloader driver, ported from bootloader/bootloader.cpp.

use super::UsbHandle;
use crate::device::ClientHandle;
use crate::jsonutil::*;
use nusb::transfer::{Buffer, Bulk, Out};
use nusb::MaybeFuture;
use serde_json::{json, Value};
use std::time::Duration;

const REQ_INFO: u8 = 0xB0;
const REQ_ERASE: u8 = 0xB1;
const REQ_START_WRITE: u8 = 0xB2;
const REQ_CRC_APP: u8 = 0xB3;
const REQ_CRC_BOOT: u8 = 0xB4;
const REQ_RESET: u8 = 0xBF;

#[derive(Default, Clone)]
pub struct BootloaderInfo {
    pub magic: u32, // stored big-endian-rendered like the C++ ntohl
    pub version: u8,
    pub devid: u32,
    pub page_size: u16,
    pub app_section_end: u32,
    pub hw_product: String,
    pub hw_version: String,
}

impl BootloaderInfo {
    fn from_bytes(d: &[u8]) -> BootloaderInfo {
        if d.len() < 51 {
            return BootloaderInfo::default();
        }
        let cstr = |b: &[u8]| {
            let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            String::from_utf8_lossy(&b[..end]).into_owned()
        };
        BootloaderInfo {
            // ntohl(le read) == interpret the raw bytes as big-endian
            magic: u32::from_be_bytes(d[0..4].try_into().unwrap()),
            version: d[4],
            devid: u32::from_be_bytes(d[5..9].try_into().unwrap()),
            page_size: u16::from_le_bytes(d[9..11].try_into().unwrap()),
            app_section_end: u32::from_le_bytes(d[11..15].try_into().unwrap()),
            // d[15..19] entry_jmp_pointer (unused)
            hw_product: cstr(&d[19..35]),
            hw_version: cstr(&d[35..51]),
        }
    }
}

pub struct BootloaderDevice {
    pub handle: UsbHandle,
    pub serial: String,
    pub info: BootloaderInfo,
    pub connections: Vec<ClientHandle>,
}

impl BootloaderDevice {
    pub fn new(handle: UsbHandle, serial: String) -> BootloaderDevice {
        eprintln!("Found a bootloader: {serial}");
        let (r, data) = handle.control_in(0xC0, REQ_INFO, 0, 0, 51);
        eprintln!("bootloader: getInfo {r}");
        let info = BootloaderInfo::from_bytes(&data);
        BootloaderDevice {
            handle,
            serial,
            info,
            connections: Vec::new(),
        }
    }

    pub fn model(&self) -> String {
        "com.nonolithlabs.bootloader".into()
    }

    pub fn hw_version(&self) -> String {
        format!("{} {}", self.info.hw_product, self.info.hw_version)
    }

    pub fn broadcast_json(&self, v: Value) {
        for c in &self.connections {
            c.send_json(v.clone());
        }
    }

    pub fn on_client_attach(&mut self, client: &ClientHandle) {
        self.connections.push(client.clone());
        client.send_json(json!({
            "_action": "info",
            "serial": self.serial,
            "magic": format!("{:08X}", self.info.magic),
            "version": self.info.version,
            "devid": format!("{:08X}", self.info.devid),
            "page_size": self.info.page_size,
            "app_section_end": self.info.app_section_end,
            "hw_product": self.info.hw_product,
            "hw_version": self.info.hw_version,
        }));
    }

    pub fn on_client_detach(&mut self, client_id: u64) {
        self.connections.retain(|c| c.id != client_id);
    }

    fn write(&self, data: &[u8]) -> i32 {
        let interface = match self.handle.device.claim_interface(0).wait() {
            Ok(i) => i,
            Err(e) => {
                eprintln!("bootloader: could not claim interface: {e}");
                return -1;
            }
        };
        self.handle.control_in(0xC0, REQ_START_WRITE, 0, 0, 0);
        println!(
            "Starting bootloader write {} {}",
            self.info.page_size,
            data.len()
        );

        let mut ep = match interface.endpoint::<Bulk, Out>(0x01) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("bootloader: could not open EP1: {e}");
                return -1;
            }
        };
        let completion = ep.transfer_blocking(Buffer::from(data.to_vec()), Duration::from_secs(1));
        let transferred = completion.actual_len;
        let r = if completion.status.is_ok() { 0 } else { -1 };
        println!("Wrote: {transferred}, {r}");
        r
    }

    fn crc(&self, req: u8) -> u32 {
        let (r, data) = self.handle.control_in(0xC0, req, 0, 0, 64);
        if r >= 4 {
            u32::from_le_bytes(data[0..4].try_into().unwrap())
        } else {
            0
        }
    }

    pub fn process_message(&mut self, client: &ClientHandle, cmd: &str, n: &Value) -> Result<bool> {
        let id = json_int_prop_def(n, "id", 0);
        match cmd {
            "erase" => {
                self.handle.control_in(0xC0, REQ_ERASE, 0, 0, 0);
                client.send_json(json!({"_action": "return", "id": id}));
            }
            "write" => {
                // `data` is base64 (libjson's as_binary decoded base64)
                let data_str = json_string_prop(n, "data")?;
                let data =
                    base64_decode(&data_str).ok_or_else(|| Error::new("Invalid base64 data"))?;
                let r = self.write(&data);
                client.send_json(json!({"_action": "return", "id": id, "result": r}));
            }
            "crc_app" => {
                let crc = self.crc(REQ_CRC_APP);
                client.send_json(json!({"_action": "return", "id": id, "crc": crc}));
            }
            "crc_boot" => {
                let crc = self.crc(REQ_CRC_BOOT);
                client.send_json(json!({"_action": "return", "id": id, "crc": crc}));
            }
            "reset" => {
                self.handle.control_in(0xC0, REQ_RESET, 0, 0, 0);
            }
            _ => return super::handle_usb_message(&self.handle, client, cmd, n),
        }
        Ok(true)
    }
}

/// Minimal base64 decoder (standard alphabet, optional padding).
pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes: Vec<u8> = s
        .bytes()
        .filter(|&b| !b.is_ascii_whitespace() && b != b'=')
        .collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut acc: u32 = 0;
        for &b in chunk {
            acc = (acc << 6) | val(b)?;
        }
        match chunk.len() {
            4 => {
                out.push((acc >> 16) as u8);
                out.push((acc >> 8) as u8);
                out.push(acc as u8);
            }
            3 => {
                acc <<= 6;
                out.push((acc >> 16) as u8);
                out.push((acc >> 8) as u8);
            }
            2 => {
                acc <<= 12;
                out.push((acc >> 16) as u8);
            }
            _ => return None,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64() {
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("aGVsbG8").unwrap(), b"hello");
        assert_eq!(base64_decode("").unwrap(), b"");
        assert_eq!(base64_decode("AA==").unwrap(), vec![0]);
        assert!(base64_decode("!!!").is_none());
    }

    #[test]
    fn info_parsing() {
        let mut d = vec![0u8; 51];
        d[0..4].copy_from_slice(&[0xCA, 0xFE, 0xBA, 0xBE]); // magic raw bytes
        d[4] = 2; // version
        d[9..11].copy_from_slice(&512u16.to_le_bytes()); // page_size
        d[11..15].copy_from_slice(&0x1FFFFu32.to_le_bytes()); // app_section_end
        d[19..24].copy_from_slice(b"CEE\0\0");
        d[35..38].copy_from_slice(b"B\0\0");
        let info = BootloaderInfo::from_bytes(&d);
        assert_eq!(info.magic, 0xCAFEBABE);
        assert_eq!(info.page_size, 512);
        assert_eq!(info.app_section_end, 0x1FFFF);
        assert_eq!(info.hw_product, "CEE");
        assert_eq!(info.hw_version, "B");
    }
}
