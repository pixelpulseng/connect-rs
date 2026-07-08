// M1K (ADALM1000) device support, ported from m1k/m1k.cpp + m1k.hpp.

use super::{SelfRef, UsbHandle};
use crate::device::{AnyDevice, ClientHandle, DevicePtr};
use crate::jsonutil::*;
use crate::rest::{parse_query, RestRequest, RestResponse};
use crate::source::OutputSource;
use crate::streaming::{Backend, Channel, Stream, StreamingDevice};
use nusb::transfer::{Buffer, Bulk, In, Out};
use nusb::MaybeFuture;
use serde_json::{json, Map, Value};
use std::sync::{Arc, Mutex};

const EP_BULK_IN: u8 = 0x81;
const EP_BULK_OUT: u8 = 0x02;

const CHUNK_SIZE: usize = 256;
const IN_PACKET_SIZE: usize = CHUNK_SIZE * 4 * 2; // 2048 bytes
const OUT_PACKET_SIZE: usize = CHUNK_SIZE * 2 * 2; // 1024 bytes

const TIMER_CLOCK: f64 = 48e6;
const MIN_PER: f64 = 240.0; // 100 ksps
const MAX_PER: f64 = 24000.0; // ~1 ksps
const N_TRANSFERS: usize = 2;

const V_RESOLUTION: f64 = 5.0 / 65536.0;
const I_RESOLUTION: f64 = 0.4 / 65536.0;
const V_MIN: f32 = 0.0;
const V_MAX: f32 = 5.0;
const I_MIN: f32 = -200.0; // mA
const I_MAX: f32 = 200.0;

#[cfg(windows)]
const BUFFER_TIME: f64 = 0.050;
#[cfg(not(windows))]
const BUFFER_TIME: f64 = 0.020;

const DEFAULT_SAMPLE_TIME: f64 = 1.0 / 100000.0; // 100 ksps

pub const EEPROM_VALID: u32 = 0x01ee02dd;

// Modes
pub const HI_Z: u32 = 0;
pub const SVMI: u32 = 1;
pub const SIMV: u32 = 2;
pub const HI_Z_SPLIT: u32 = 3;
pub const SVMI_SPLIT: u32 = 4;
pub const SIMV_SPLIT: u32 = 5;

// GPIO pins (PIOB offset 0x20)
const CHA_50R_2V5: u16 = 0x20;
const CHA_50R_GND: u16 = 0x21;
const CHA_FEEDBACK: u16 = 0x22;
const CHA_OUTPUT_EN: u16 = 0x23;
const CHB_50R_2V5: u16 = 0x25;
const CHB_50R_GND: u16 = 0x26;
const CHB_FEEDBACK: u16 = 0x27;
const CHB_OUTPUT_EN: u16 = 0x28;
const CHA_SPLIT: u16 = 34; // PA2
const CHB_SPLIT: u16 = 39; // PA7

#[derive(Clone, Copy)]
pub struct M1kCal {
    pub valid: bool,
    pub offset: [f32; 8],
    pub gain_p: [f32; 8],
    pub gain_n: [f32; 8],
}

impl Default for M1kCal {
    fn default() -> Self {
        M1kCal {
            valid: false,
            offset: [0.0; 8],
            gain_p: [1.0; 8],
            gain_n: [1.0; 8],
        }
    }
}

impl M1kCal {
    fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(100);
        b.extend_from_slice(&EEPROM_VALID.to_le_bytes());
        for arr in [&self.offset, &self.gain_p, &self.gain_n] {
            for v in arr.iter() {
                b.extend_from_slice(&v.to_le_bytes());
            }
        }
        b
    }

    fn from_bytes(data: &[u8]) -> Option<M1kCal> {
        if data.len() < 100 {
            return None;
        }
        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let mut cal = M1kCal {
            valid: magic == EEPROM_VALID,
            ..Default::default()
        };
        if cal.valid {
            let f = |i: usize| f32::from_le_bytes(data[4 + i * 4..8 + i * 4].try_into().unwrap());
            for i in 0..8 {
                cal.offset[i] = f(i);
                cal.gain_p[i] = f(8 + i);
                cal.gain_n[i] = f(16 + i);
            }
        }
        Some(cal)
    }
}

#[derive(Clone, Copy, Default)]
pub struct Frontend {
    pub r50_2v5: bool,
    pub r50_gnd: bool,
    pub feedback: bool,
    pub output_en: bool,
    pub split: bool,
    pub pot_r1: u8,
    pub pot_r2: u8,
}

pub struct M1kBackend {
    pub handle: UsbHandle,
    pub self_ref: SelfRef,
    interface: Option<nusb::Interface>,
    tasks: Vec<tokio::task::JoinHandle<()>>,

    pub cal: M1kCal,
    pub m_mode: [u32; 2],
    pub m1k_per: u32,
    pub led_state: u8,
    pub fw_interleaved: bool,
    pub frontend: [Frontend; 2],
    pub packets_per_transfer: usize,

    // Output/input lead tracking for stream resync (see handle_in_transfer)
    lead_baseline: Option<i64>,
    lead_min: i64,
    lead_count: u32,
    lead_settle: u32,
}

fn be(dev: &StreamingDevice) -> &M1kBackend {
    match &dev.backend {
        Backend::M1k(b) => b,
        _ => unreachable!("not an M1K"),
    }
}

fn bm(dev: &mut StreamingDevice) -> &mut M1kBackend {
    match &mut dev.backend {
        Backend::M1k(b) => b,
        _ => unreachable!("not an M1K"),
    }
}

/// Parse the leading float of a version string, like C atof().
fn leading_float(s: &str) -> f64 {
    let end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-' && c != '+')
        .unwrap_or(s.len());
    s[..end].parse().unwrap_or(0.0)
}

pub fn create(handle: UsbHandle, serial: String) -> std::result::Result<DevicePtr, Error> {
    eprintln!("Found an M1K: \n    Serial: {serial}");

    let hw_version = handle.read_string(0x00, 0, 0);
    let fw_version = handle.read_string(0x00, 0, 1);
    let fw_interleaved = leading_float(&fw_version) >= 2.0;

    eprintln!("    Hardware: {hw_version}");
    eprintln!("    Firmware: {fw_version}");

    // Stop any ongoing sampling
    handle.control_out(0x40, 0xC5, 0, 0, &[]);

    // Read calibration EEPROM
    let (r, data) = handle.control_in(0xC0, 0x01, 0, 0, 100);
    let cal = if r >= 100 {
        let c = M1kCal::from_bytes(&data).unwrap_or_default();
        if c.valid {
            eprintln!("    Calibration loaded");
        } else {
            eprintln!("    Calibration data invalid, using defaults");
        }
        c
    } else {
        eprintln!("    Calibration data invalid, using defaults");
        M1kCal::default()
    };

    let backend = M1kBackend {
        handle,
        self_ref: SelfRef::new(),
        interface: None,
        tasks: Vec::new(),
        cal,
        m_mode: [HI_Z, HI_Z],
        m1k_per: 0,
        led_state: 0,
        fw_interleaved,
        frontend: [
            Frontend { feedback: true, ..Default::default() },
            Frontend { feedback: true, ..Default::default() },
        ],
        packets_per_transfer: 1,
        lead_baseline: None,
        lead_min: i64::MAX,
        lead_count: 0,
        lead_settle: 16,
    };

    let mut dev = StreamingDevice {
        model: "com.analogdevices.m1k".into(),
        hw_version,
        fw_version,
        serial,
        connections: Vec::new(),
        channels: Vec::new(),
        listeners: Vec::new(),
        dev_mode: 0,
        raw_mode: false,
        capture_state: false,
        capture_done: false,
        capture_length: 0.0,
        capture_samples: 0,
        capture_continuous: false,
        sample_time: DEFAULT_SAMPLE_TIME,
        min_sample_time: 2.0 * MIN_PER / TIMER_CLOCK,
        capture_i: 0,
        capture_o: 0,
        current_limit: 200,
        backend: Backend::M1k(Box::new(backend)),
    };

    let samples = (12.0 / DEFAULT_SAMPLE_TIME).ceil() as u32;
    dev.configure(0, DEFAULT_SAMPLE_TIME, samples, true, false);

    Ok(Arc::new(Mutex::new(AnyDevice::Streaming(dev))))
}

pub fn configure(dev: &mut StreamingDevice, mode: i32, sample_time: f64, samples: u32, continuous: bool, raw: bool) {
    // Timer period: a sample takes 2 timer ticks (A/B ADC phases)
    let mut per = (sample_time * TIMER_CLOCK).round() / 2.0;
    per = per.clamp(MIN_PER, MAX_PER);
    dev.sample_time = 2.0 * per / TIMER_CLOCK;

    let mut mode = mode;
    if mode != 0 {
        eprintln!("M1K: unsupported mode {mode} requested, using 0");
        mode = 0;
    }

    dev.capture_samples = samples;
    dev.capture_continuous = continuous;
    dev.dev_mode = mode as u32;
    dev.raw_mode = raw;
    dev.capture_length = samples as f32 * dev.sample_time as f32;
    dev.capture_i = 0;
    dev.capture_o = 0;

    let ppt = (BUFFER_TIME / (dev.sample_time * CHUNK_SIZE as f64) / N_TRANSFERS as f64).ceil() as usize;
    let sample_time = dev.sample_time;
    {
        let b = bm(dev);
        b.m1k_per = per as u32;
        b.packets_per_transfer = ppt.max(1);
    }

    eprintln!(
        "M1K configure: per={} transfers={} pkt/xfer={} samples={}",
        per as u32,
        N_TRANSFERS,
        ppt.max(1),
        samples
    );
    let _ = sample_time;

    dev.channels.clear();
    for (cid, cname) in [("a", "A"), ("b", "B")] {
        let mut c = Channel::new(cid, cname);
        c.source = Some(OutputSource::constant(0, 0.0));
        let (mut v, mut i) = if raw {
            (
                Stream::new("v", &format!("Voltage {cname}"), "LSB", 0.0, 65535.0, 1, V_RESOLUTION as f32, 1),
                Stream::new("i", &format!("Current {cname}"), "LSB", 0.0, 65535.0, 2, (I_RESOLUTION * 1000.0) as f32, 1),
            )
        } else {
            (
                Stream::new("v", &format!("Voltage {cname}"), "V", V_MIN, V_MAX, 1, V_RESOLUTION as f32, 1),
                Stream::new("i", &format!("Current {cname}"), "mA", I_MIN, I_MAX, 2, (I_RESOLUTION * 1000.0) as f32, 1),
            )
        };
        v.allocate(samples);
        i.allocate(samples);
        c.streams.push(v);
        c.streams.push(i);
        dev.channels.push(c);
    }
}

/// Append M1K-specific state (frontend, mode names) to deviceConfig JSON.
pub fn state_extras(dev: &StreamingDevice, n: &mut Map<String, Value>) {
    let b = be(dev);
    n.insert("frontend".into(), frontend_to_json(b));
    const MODE_NAMES: [&str; 6] = ["hi_z", "svmi", "simv", "hi_z_split", "svmi_split", "simv_split"];
    n.insert(
        "m1k_modes".into(),
        json!({
            "a": MODE_NAMES[b.m_mode[0] as usize % 6],
            "b": MODE_NAMES[b.m_mode[1] as usize % 6],
        }),
    );
}

impl M1kBackend {
    fn set_mode(&mut self, channel: u32, mode: u32) {
        let (pset, split): (u16, bool) = match mode {
            SIMV_SPLIT => (0x7f7f, true),
            SIMV => (0x7f7f, false),
            SVMI_SPLIT => (0x0000, true),
            SVMI => (0x0000, false),
            HI_Z_SPLIT => (0x3000, true),
            _ => (0x3000, false),
        };

        // Set feedback potentiometers
        self.handle.control_out(0x40, 0x59, channel as u16, pset, &[]);

        // Set mode (firmware only understands 0/1/2)
        self.handle.control_out(0x40, 0x53, channel as u16, (mode % 3) as u16, &[]);

        // Set SPLIT pin for split modes
        if split {
            let pin = if channel == 0 { CHA_SPLIT } else { CHB_SPLIT };
            self.handle.control_out(0x40, 0x51, pin, 0, &[]);
        }

        self.m_mode[channel as usize] = mode;

        // Mirror what firmware set_mode() does to the GPIO pins
        let base_mode = mode % 3;
        let fe = &mut self.frontend[channel as usize];
        fe.feedback = true; // firmware always clears feedback pin (active LOW = on)
        fe.output_en = base_mode != 0;
        fe.split = split;
        fe.pot_r1 = (pset >> 8) as u8 & 0xFF;
        fe.pot_r2 = (pset & 0xFF) as u8;
    }

    fn set_gpio(&self, pin: u16, high: bool) {
        self.handle.control_out(0x40, if high { 0x51 } else { 0x50 }, pin, 0, &[]);
    }

    fn set_digipot(&mut self, channel: usize, r1: u8, r2: u8) {
        self.handle
            .control_out(0x40, 0x59, channel as u16, ((r1 as u16) << 8) | r2 as u16, &[]);
        self.frontend[channel].pot_r1 = r1;
        self.frontend[channel].pot_r2 = r2;
    }

    fn set_leds(&mut self, state: u8) {
        self.led_state = state & 0x7;
        self.handle.control_out(0x40, 0x03, self.led_state as u16, 0, &[]);
    }

    fn set_frontend_switch(&mut self, ch: usize, name: &str, val: bool) {
        let pin = match name {
            "r50_2v5" => {
                self.frontend[ch].r50_2v5 = val;
                if ch == 0 { CHA_50R_2V5 } else { CHB_50R_2V5 }
            }
            "r50_gnd" => {
                self.frontend[ch].r50_gnd = val;
                if ch == 0 { CHA_50R_GND } else { CHB_50R_GND }
            }
            "feedback" => {
                self.frontend[ch].feedback = val;
                if ch == 0 { CHA_FEEDBACK } else { CHB_FEEDBACK }
            }
            "output_en" => {
                self.frontend[ch].output_en = val;
                if ch == 0 { CHA_OUTPUT_EN } else { CHB_OUTPUT_EN }
            }
            "split" => {
                self.frontend[ch].split = val;
                let pin = if ch == 0 { CHA_SPLIT } else { CHB_SPLIT };
                // SPLIT pin is active HIGH (opposite of others)
                self.set_gpio(pin, val);
                return;
            }
            _ => return,
        };
        // Active LOW switches: true = ON = pin LOW
        self.set_gpio(pin, !val);
    }
}

pub fn on_reset_capture(dev: &mut StreamingDevice) {
    for c in &mut dev.channels {
        if let Some(src) = &mut c.source {
            src.start_sample = 0;
        }
    }
    let b = bm(dev);
    b.lead_baseline = None;
    b.lead_min = i64::MAX;
    b.lead_count = 0;
    b.lead_settle = 16;
}

pub fn on_start_capture(dev: &mut StreamingDevice) {
    let interface = match be(dev).handle.device.claim_interface(0).wait() {
        Ok(i) => i,
        Err(e) => {
            eprintln!("M1K: could not claim interface: {e}");
            return;
        }
    };
    if let Err(e) = interface.set_alt_setting(1).wait() {
        eprintln!("M1K: could not set alt setting 1: {e}");
    }

    {
        let modes = be(dev).m_mode;
        let b = bm(dev);
        b.set_mode(0, modes[0]);
        b.set_mode(1, modes[1]);

        // Stop any ongoing sampling, then init hardware
        b.handle.control_out(0x40, 0xC5, 0, 0, &[]);
        b.handle.control_out(0x40, 0xCC, 0, 0, &[]);

        // Sync start to a future USB frame boundary
        let (r, data) = b.handle.control_in(0xC0, 0x6F, 0, 0, 2);
        let mut sof: u16 = if r >= 2 {
            u16::from_le_bytes([data[0], data[1]])
        } else {
            0
        };
        sof = (((sof >> 3) + 0x1f) & 0x7FF) << 3;

        // Start sampling
        let per = b.m1k_per as u16;
        b.handle.control_out(0x40, 0xC5, per, sof, &[]);
    }

    // Ignore output samples sent before pausing
    dev.capture_o = dev.capture_i;

    let ppt = be(dev).packets_per_transfer;
    let self_ref = be(dev).self_ref.clone();
    let continuous = dev.capture_continuous;
    let capture_samples = dev.capture_samples as u64;

    let ep_in = interface.endpoint::<Bulk, In>(EP_BULK_IN);
    let ep_out = interface.endpoint::<Bulk, Out>(EP_BULK_OUT);
    let (ep_in, ep_out) = match (ep_in, ep_out) {
        (Ok(i), Ok(o)) => (i, o),
        _ => {
            eprintln!("M1K: could not open bulk endpoints");
            return;
        }
    };

    let mut tasks = Vec::new();

    // IN pipeline
    {
        let self_ref = self_ref.clone();
        let isize = IN_PACKET_SIZE * ppt;
        tasks.push(tokio::spawn(async move {
            let mut ep = ep_in;
            for _ in 0..N_TRANSFERS {
                let mut b = ep.allocate(isize);
                b.set_requested_len(isize);
                ep.submit(b);
            }
            let mut incount = N_TRANSFERS as u64;
            loop {
                let c = ep.next_complete().await;
                if c.status.is_err() {
                    eprintln!("M1K IN transfer error: {:?}", c.status);
                    break;
                }
                let buffer = c.buffer;
                let Some(dev) = self_ref.upgrade() else { break };
                {
                    let mut dev = dev.lock().unwrap();
                    if let AnyDevice::Streaming(d) = &mut *dev {
                        handle_in_transfer(d, &buffer[..]);
                    }
                }
                if continuous || incount * CHUNK_SIZE as u64 * (ppt as u64) < capture_samples {
                    let mut b = buffer;
                    b.clear();
                    b.set_requested_len(isize);
                    ep.submit(b);
                    incount += 1;
                } else {
                    break;
                }
            }
        }));
    }

    // OUT pipeline
    {
        let self_ref = self_ref.clone();
        tasks.push(tokio::spawn(async move {
            let mut ep = ep_out;
            for _ in 0..N_TRANSFERS {
                let Some(dev) = self_ref.upgrade() else { return };
                let data = {
                    let mut dev = dev.lock().unwrap();
                    match &mut *dev {
                        AnyDevice::Streaming(d) => fill_out_transfer(d),
                        _ => return,
                    }
                };
                ep.submit(Buffer::from(data));
            }
            let mut outcount = N_TRANSFERS as u64;
            loop {
                let c = ep.next_complete().await;
                if c.status.is_err() {
                    eprintln!("M1K OUT transfer error: {:?}", c.status);
                    break;
                }
                if continuous || outcount * CHUNK_SIZE as u64 * (ppt as u64) < capture_samples {
                    let Some(dev) = self_ref.upgrade() else { break };
                    let data = {
                        let mut dev = dev.lock().unwrap();
                        match &mut *dev {
                            AnyDevice::Streaming(d) => fill_out_transfer(d),
                            _ => break,
                        }
                    };
                    let mut b = c.buffer;
                    b.clear();
                    b.extend_from_slice(&data);
                    ep.submit(b);
                    outcount += 1;
                } else {
                    break;
                }
            }
        }));
    }

    let b = bm(dev);
    b.interface = Some(interface);
    b.tasks = tasks;
}

pub fn on_pause_capture(dev: &mut StreamingDevice) {
    {
        let b = bm(dev);

        // Stop sampling
        b.handle.control_out(0x40, 0xC5, 0, 0, &[]);

        // Reset modes to HI_Z
        b.set_mode(0, HI_Z);
        b.set_mode(1, HI_Z);

        for t in b.tasks.drain(..) {
            t.abort();
        }
        b.interface = None;
    }

    dev.capture_o = dev.capture_i;
}

pub fn on_set_output(dev: &mut StreamingDevice, chan: usize) {
    let mode = dev.channels[chan].source.as_ref().map(|s| s.mode).unwrap_or(0);
    let capture_state = dev.capture_state;
    let b = bm(dev);
    if b.m_mode[chan] != mode {
        if capture_state {
            b.set_mode(chan as u32, mode);
        } else {
            b.m_mode[chan] = mode;
        }
    }
}

// ---- data pipeline ----

fn handle_in_transfer(dev: &mut StreamingDevice, buffer: &[u8]) {
    let (ppt, interleaved, cal) = {
        let b = be(dev);
        (b.packets_per_transfer, b.fw_interleaved, b.cal)
    };
    let raw_mode = dev.raw_mode;

    for p in 0..ppt {
        let base = p * IN_PACKET_SIZE;
        if base + IN_PACKET_SIZE > buffer.len() {
            break;
        }
        let buf = &buffer[base..base + IN_PACKET_SIZE];

        for i in 0..CHUNK_SIZE {
            let (raw_av, raw_ai, raw_bv, raw_bi) = if interleaved {
                // fw >= 2.0: interleaved [AV, AI, BV, BI] per sample
                (
                    u16::from_be_bytes([buf[i * 8], buf[i * 8 + 1]]),
                    u16::from_be_bytes([buf[i * 8 + 2], buf[i * 8 + 3]]),
                    u16::from_be_bytes([buf[i * 8 + 4], buf[i * 8 + 5]]),
                    u16::from_be_bytes([buf[i * 8 + 6], buf[i * 8 + 7]]),
                )
            } else {
                // fw < 2.0: block format [all AV][all AI][all BV][all BI]
                let g = |block: usize| {
                    let o = (i + CHUNK_SIZE * block) * 2;
                    u16::from_be_bytes([buf[o], buf[o + 1]])
                };
                (g(0), g(1), g(2), g(3))
            };

            if raw_mode {
                dev.put(0, 0, raw_av as f32);
                dev.put(0, 1, raw_ai as f32);
                dev.put(1, 0, raw_bv as f32);
                dev.put(1, 1, raw_bi as f32);
            } else {
                let v = raw_av as f64 * V_RESOLUTION;
                dev.put(0, 0, ((v - cal.offset[0] as f64) * cal.gain_p[0] as f64) as f32);

                let v = (raw_ai as f64 * I_RESOLUTION - 0.195) * 1.25;
                let g = if v > 0.0 { cal.gain_p[1] } else { cal.gain_n[1] };
                dev.put(0, 1, ((v - cal.offset[1] as f64) * g as f64 * 1000.0) as f32);

                let v = raw_bv as f64 * V_RESOLUTION;
                dev.put(1, 0, ((v - cal.offset[4] as f64) * cal.gain_p[4] as f64) as f32);

                let v = (raw_bi as f64 * I_RESOLUTION - 0.195) * 1.25;
                let g = if v > 0.0 { cal.gain_p[5] } else { cal.gain_n[5] };
                dev.put(1, 1, ((v - cal.offset[5] as f64) * g as f64 * 1000.0) as f32);
            }

            dev.sample_done();
        }
    }

    dev.packet_done();
    dev.check_output_effective(0);
    dev.check_output_effective(1);

    // --- Output/input stream resync ---
    // capture_o (encode position) and capture_i (capture position) advance
    // in lockstep with a fixed queue lead. Host scheduling stalls can shift
    // that lead permanently, dragging out-source trigger alignment with it.
    // Track the per-window minimum lead and re-anchor capture_o when it
    // drifts more than half a chunk from the baseline.
    let lead = dev.capture_o as i64 - dev.capture_i as i64;
    let mut adjust: i64 = 0;
    {
        let b = bm(dev);
        if b.lead_settle > 0 {
            b.lead_settle -= 1;
        } else {
            if lead < b.lead_min {
                b.lead_min = lead;
            }
            b.lead_count += 1;
            if b.lead_count >= 64 {
                match b.lead_baseline {
                    None => b.lead_baseline = Some(b.lead_min),
                    Some(baseline) => {
                        let drift = b.lead_min - baseline;
                        if drift.abs() > (CHUNK_SIZE / 2) as i64 {
                            adjust = drift;
                            eprintln!("M1K: output stream resynced by {} samples", -drift);
                        }
                    }
                }
                b.lead_min = i64::MAX;
                b.lead_count = 0;
            }
        }
    }
    if adjust != 0 {
        dev.capture_o = (dev.capture_o as i64 - adjust) as u64;
    }
}

fn encode_out(mode: u32, cal: &M1kCal, raw_mode: bool, channel: usize, val: f32) -> u16 {
    if raw_mode {
        return val.clamp(0.0, 65535.0) as u16;
    }

    let mut v: i32 = 32768 * 4 / 5; // HI_Z midscale default

    if mode == SVMI || mode == SVMI_SPLIT {
        // val is in V, apply source calibration
        let mut val = (val - cal.offset[channel * 4 + 2]) * cal.gain_p[channel * 4 + 2];
        val = val.clamp(V_MIN, V_MAX);
        v = (val as f64 * (1.0 / V_RESOLUTION)) as i32;
    } else if mode == SIMV || mode == SIMV_SPLIT {
        // val is in mA, convert to A for encoding
        let mut val_a = val / 1000.0;
        let g = if val_a > 0.0 { cal.gain_p[channel * 4 + 3] } else { cal.gain_n[channel * 4 + 3] };
        val_a = (val_a - cal.offset[channel * 4 + 3]) * g;
        val_a = val_a.clamp(-0.2, 0.2);
        v = (65536.0 * (2.0 / 5.0 + 0.8 * 0.2 * 20.0 * 0.5 * val_a as f64)) as i32;
    }

    v.clamp(0, 65535) as u16
}

/// Encode one OUT transfer's worth of samples, advancing capture_o.
/// Runs with the device lock held (the moral equivalent of outputMutex).
fn fill_out_transfer(dev: &mut StreamingDevice) -> Vec<u8> {
    let (ppt, interleaved, cal, m_mode) = {
        let b = be(dev);
        (b.packets_per_transfer, b.fw_interleaved, b.cal, b.m_mode)
    };
    let raw_mode = dev.raw_mode;
    let sample_time = dev.sample_time;
    let osize = OUT_PACKET_SIZE * ppt;
    let mut buf = vec![0u8; osize];

    if dev.channels.len() == 2 && dev.channels[0].source.is_some() && dev.channels[1].source.is_some() {
        let mut o = dev.capture_o;
        for p in 0..ppt {
            let pkt = p * OUT_PACKET_SIZE;
            for i in 0..CHUNK_SIZE {
                let av = dev.channels[0].source.as_mut().unwrap().get_value(o, sample_time);
                let bv = dev.channels[1].source.as_mut().unwrap().get_value(o, sample_time);
                let a = encode_out(m_mode[0], &cal, raw_mode, 0, av);
                let b = encode_out(m_mode[1], &cal, raw_mode, 1, bv);

                if interleaved {
                    buf[pkt + i * 4] = (a >> 8) as u8;
                    buf[pkt + i * 4 + 1] = (a & 0xff) as u8;
                    buf[pkt + i * 4 + 2] = (b >> 8) as u8;
                    buf[pkt + i * 4 + 3] = (b & 0xff) as u8;
                } else {
                    buf[pkt + i * 2] = (a >> 8) as u8;
                    buf[pkt + i * 2 + 1] = (a & 0xff) as u8;
                    buf[pkt + (i + CHUNK_SIZE) * 2] = (b >> 8) as u8;
                    buf[pkt + (i + CHUNK_SIZE) * 2 + 1] = (b & 0xff) as u8;
                }

                o += 1;
            }
        }
        dev.capture_o = o;
    }

    buf
}

// ---- JSON helpers ----

fn frontend_channel_json(fe: &Frontend) -> Value {
    json!({
        "r50_2v5": fe.r50_2v5,
        "r50_gnd": fe.r50_gnd,
        "feedback": fe.feedback,
        "output_en": fe.output_en,
        "split": fe.split,
        "pot": [fe.pot_r1, fe.pot_r2],
    })
}

fn frontend_to_json(b: &M1kBackend) -> Value {
    json!({
        "a": frontend_channel_json(&b.frontend[0]),
        "b": frontend_channel_json(&b.frontend[1]),
    })
}

fn calibration_json(cal: &M1kCal) -> (Value, Value, Value) {
    let f = |a: &[f32; 8]| Value::Array(a.iter().map(|v| crate::jsonutil::f32_json(*v)).collect());
    (f(&cal.offset), f(&cal.gain_p), f(&cal.gain_n))
}

fn read_power(dev: &StreamingDevice) -> Value {
    let b = be(dev);
    let (r, buf) = b.handle.control_in(0xC0, 0x17, 0, 3, 3);
    if r >= 1 {
        // Alert bit position depends on firmware version (string compare)
        let alert_bit: u8 = if dev.fw_version.as_str() >= "2.11" { 0x8 } else { 0x4 };
        json!({
            "status_raw": buf[0],
            "alert_bit": alert_bit,
            "overcurrent": (buf[0] & alert_bit) != 0,
        })
    } else {
        json!({"status_raw": 0, "alert_bit": 0, "overcurrent": false, "error": r})
    }
}

fn read_temperature(b: &M1kBackend) -> Value {
    let read = |ch: u16| {
        let (r, buf) = b.handle.control_in(0xC0, 0x19, ch, 0, 2);
        if r >= 2 {
            ((buf[0] as i32) << 8) | buf[1] as i32
        } else {
            0
        }
    };
    json!({"a": read(0), "b": read(1)})
}

fn led_json(b: &M1kBackend) -> Value {
    json!({
        "leds": b.led_state,
        "red": (b.led_state & 0x4) != 0,
        "green": (b.led_state & 0x2) != 0,
        "blue": (b.led_state & 0x1) != 0,
    })
}

fn write_calibration(dev: &mut StreamingDevice) -> i32 {
    let b = bm(dev);
    b.cal.valid = true;
    let bytes = b.cal.to_bytes();
    b.handle.control_out(0x40, 0x02, 0, 0, &bytes)
}

fn write_serial(dev: &mut StreamingDevice, new_serial: &str) -> i32 {
    if new_serial.is_empty() || new_serial.len() > 32 {
        return -1;
    }
    be(dev).handle.control_out(0x40, 0x05, 0, 0, new_serial.as_bytes())
}

fn apply_frontend_json(dev: &mut StreamingDevice, ch: usize, n: &Value) {
    let b = bm(dev);
    for name in ["r50_2v5", "r50_gnd", "feedback", "output_en", "split"] {
        if let Some(Value::Bool(v)) = n.get(name) {
            b.set_frontend_switch(ch, name, *v);
        }
    }
    let pot_r1 = json_int_prop_def(n, "pot_r1", -1);
    let pot_r2 = json_int_prop_def(n, "pot_r2", -1);
    if pot_r1 >= 0 || pot_r2 >= 0 {
        let r1 = if pot_r1 >= 0 { (pot_r1 & 0x7f) as u8 } else { b.frontend[ch].pot_r1 };
        let r2 = if pot_r2 >= 0 { (pot_r2 & 0x7f) as u8 } else { b.frontend[ch].pot_r2 };
        b.set_digipot(ch, r1, r2);
    }
}

fn ret(id: i64, extra: &[(&str, Value)]) -> Value {
    let mut m = Map::new();
    m.insert("_action".into(), json!("return"));
    m.insert("id".into(), json!(id));
    for (k, v) in extra {
        m.insert((*k).into(), v.clone());
    }
    Value::Object(m)
}

// ---- WS commands (m1k.cpp processMessage) ----

pub fn process_message(dev: &mut StreamingDevice, client: &ClientHandle, cmd: &str, n: &Value) -> Result<bool> {
    let id = json_int_prop_def(n, "id", 0);
    match cmd {
        "readCalibration" => {
            let cal = be(dev).cal;
            let (off, gp, gn) = calibration_json(&cal);
            client.send_json(ret(
                id,
                &[
                    ("offset", off),
                    ("gain_p", gp),
                    ("gain_n", gn),
                    ("valid", json!(cal.valid)),
                ],
            ));
        }
        "writeCalibration" => {
            let get_arr = |name: &str| -> Result<[f32; 8]> {
                let a = n
                    .get(name)
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| Error(format!("JSON missing property: {name}")))?;
                let mut out = [0.0f32; 8];
                for i in 0..8 {
                    out[i] = a.get(i).and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
                }
                Ok(out)
            };
            let offset = get_arr("offset")?;
            let gain_p = get_arr("gain_p")?;
            let gain_n = get_arr("gain_n")?;
            {
                let b = bm(dev);
                b.cal.offset = offset;
                b.cal.gain_p = gain_p;
                b.cal.gain_n = gain_n;
            }
            let r = write_calibration(dev);
            client.send_json(ret(id, &[("status", json!(r))]));
        }
        "getFrontend" => {
            client.send_json(ret(id, &[("frontend", frontend_to_json(be(dev)))]));
        }
        "setFrontend" => {
            let ch = json_string_prop(n, "channel")?;
            let ch_idx = if ch == "b" || ch == "B" { 1 } else { 0 };
            apply_frontend_json(dev, ch_idx, n);
            client.send_json(ret(id, &[("frontend", frontend_to_json(be(dev)))]));
        }
        "getPower" => {
            let p = read_power(dev);
            client.send_json(ret(id, &[("power", p)]));
        }
        "getTemperature" => {
            client.send_json(ret(id, &[("temperature", read_temperature(be(dev)))]));
        }
        "getLED" => {
            client.send_json(ret(id, &[("leds", led_json(be(dev)))]));
        }
        "setLED" => {
            set_leds_json(dev, n);
            client.send_json(ret(id, &[("leds", led_json(be(dev)))]));
        }
        "setSerial" => {
            let new_serial = json_string_prop_def(n, "serial", "");
            let r = write_serial(dev, &new_serial);
            client.send_json(ret(id, &[("status", json!(r))]));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn set_leds_json(dev: &mut StreamingDevice, n: &Value) {
    let b = bm(dev);
    let val = json_int_prop_def(n, "leds", -1);
    if val >= 0 {
        b.set_leds((val & 0x7) as u8);
    } else {
        let mut state = b.led_state;
        for (name, bit) in [("red", 0x4u8), ("green", 0x2), ("blue", 0x1)] {
            if let Some(Value::Bool(v)) = n.get(name) {
                if *v {
                    state |= bit;
                } else {
                    state &= !bit;
                }
            }
        }
        b.set_leds(state);
    }
}

// ---- REST endpoints (m1k.cpp handleREST) ----

pub fn handle_rest(dev: &mut StreamingDevice, req: &RestRequest, level: usize) -> Option<RestResponse> {
    let seg = req.parts.get(level).map(|s| s.as_str()).unwrap_or("");
    match seg {
        "frontend" => Some(if req.method == "POST" {
            let map = parse_query(&req.body);
            let ch = map.get("channel").cloned().unwrap_or_else(|| "a".into());
            let ch_idx = if ch == "b" || ch == "B" { 1 } else { 0 };
            {
                let b = bm(dev);
                for name in ["r50_2v5", "r50_gnd", "feedback", "output_en", "split"] {
                    if let Some(v) = map.get(name) {
                        if !v.is_empty() {
                            b.set_frontend_switch(ch_idx, name, v == "true" || v == "1");
                        }
                    }
                }
                let p1 = map.get("pot_r1").filter(|s| !s.is_empty());
                let p2 = map.get("pot_r2").filter(|s| !s.is_empty());
                if p1.is_some() || p2.is_some() {
                    let r1 = p1
                        .and_then(|s| s.parse::<i64>().ok())
                        .map(|v| (v & 0x7f) as u8)
                        .unwrap_or(b.frontend[ch_idx].pot_r1);
                    let r2 = p2
                        .and_then(|s| s.parse::<i64>().ok())
                        .map(|v| (v & 0x7f) as u8)
                        .unwrap_or(b.frontend[ch_idx].pot_r2);
                    b.set_digipot(ch_idx, r1, r2);
                }
            }
            RestResponse::json(&frontend_to_json(be(dev)))
        } else {
            RestResponse::json(&frontend_to_json(be(dev)))
        }),
        "power" => Some(RestResponse::json(&read_power(dev))),
        "temperature" => Some(RestResponse::json(&read_temperature(be(dev)))),
        "leds" => Some(if req.method == "POST" {
            let map = parse_query(&req.body);
            let b = bm(dev);
            if let Some(raw) = map.get("leds").filter(|s| !s.is_empty()) {
                b.set_leds((raw.parse::<i64>().unwrap_or(0) & 0x7) as u8);
            } else {
                let mut state = b.led_state;
                for (name, bit) in [("red", 0x4u8), ("green", 0x2), ("blue", 0x1)] {
                    if let Some(v) = map.get(name).filter(|s| !s.is_empty()) {
                        if v == "true" || v == "1" {
                            state |= bit;
                        } else {
                            state &= !bit;
                        }
                    }
                }
                b.set_leds(state);
            }
            RestResponse::json(&led_json(be(dev)))
        } else {
            RestResponse::json(&led_json(be(dev)))
        }),
        "calibration" => Some(if req.method == "POST" {
            match rest_calibration_post(dev, &req.body) {
                Ok(r) => r,
                Err(e) => RestResponse::error(&e),
            }
        } else {
            let cal = be(dev).cal;
            let (off, gp, gn) = calibration_json(&cal);
            RestResponse::json(&json!({
                "valid": cal.valid,
                "offset": off,
                "gain_p": gp,
                "gain_n": gn,
            }))
        }),
        "serial" => Some(if req.method == "POST" {
            let map = parse_query(&req.body);
            let new_serial = map.get("serial").cloned().unwrap_or_default();
            let r = write_serial(dev, &new_serial);
            RestResponse::json(&json!({"status": r, "serial": new_serial}))
        } else {
            RestResponse::json(&json!({"serial": dev.serial}))
        }),
        _ => None,
    }
}

fn rest_calibration_post(dev: &mut StreamingDevice, body: &str) -> Result<RestResponse> {
    let n: Value = serde_json::from_str(body).map_err(|e| Error(format!("JSON parse error: {e}")))?;

    if n.get("reset").and_then(|v| v.as_bool()).unwrap_or(false) {
        let b = bm(dev);
        b.cal.offset = [0.0; 8];
        b.cal.gain_p = [1.0; 8];
        b.cal.gain_n = [1.0; 8];
    } else {
        let get_arr = |name: &str| -> Result<[f32; 8]> {
            let a = n
                .get(name)
                .and_then(|v| v.as_array())
                .ok_or_else(|| Error(format!("JSON missing property: {name}")))?;
            let mut out = [0.0f32; 8];
            for i in 0..8 {
                out[i] = a.get(i).and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
            }
            Ok(out)
        };
        let offset = get_arr("offset")?;
        let gain_p = get_arr("gain_p")?;
        let gain_n = get_arr("gain_n")?;
        let b = bm(dev);
        b.cal.offset = offset;
        b.cal.gain_p = gain_p;
        b.cal.gain_n = gain_n;
    }

    let r = write_calibration(dev);
    Ok(RestResponse::json(&json!({"status": r, "valid": true})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_period_math() {
        // per = round(st*48e6)/2 clamped [240, 24000]; st = 2*per/48e6
        let cases = [
            (1e-5, 240.0, 1e-5),          // 100 ksps exactly
            (1e-6, 240.0, 1e-5),          // clamped to min
            (1.0, 24000.0, 1e-3),         // clamped to max
            (2.5e-5, 600.0, 2.5e-5),      // exact
        ];
        for (st, want_per, want_st) in cases {
            let mut per = (st * TIMER_CLOCK).round() / 2.0;
            per = per.clamp(MIN_PER, MAX_PER);
            let actual = 2.0 * per / TIMER_CLOCK;
            assert_eq!(per, want_per, "st={st}");
            assert!((actual - want_st).abs() < 1e-12, "st={st}");
        }
    }

    #[test]
    fn encode_out_values() {
        let cal = M1kCal::default();
        // Hi-Z: constant midscale 26214
        assert_eq!(encode_out(HI_Z, &cal, false, 0, 3.3), 26214);
        // SVMI: v = val * 65536/5
        assert_eq!(encode_out(SVMI, &cal, false, 0, 0.0), 0);
        assert_eq!(encode_out(SVMI, &cal, false, 0, 5.0), 65535);
        assert_eq!(encode_out(SVMI, &cal, false, 0, 2.5), (2.5 * 65536.0 / 5.0) as u16);
        // SIMV: v = 65536 * (0.4 + 1.6 * amps); val in mA
        assert_eq!(encode_out(SIMV, &cal, false, 0, 0.0), (65536.0 * 0.4) as u16);
        assert_eq!(encode_out(SIMV, &cal, false, 0, 100.0), (65536.0 * (0.4 + 1.6 * 0.1)) as u16);
        assert_eq!(encode_out(SIMV, &cal, false, 0, -300.0), (65536.0 * (0.4 - 1.6 * 0.2)) as u16); // clamped ±0.2 A
        // raw passthrough
        assert_eq!(encode_out(SVMI, &cal, true, 0, 1234.0), 1234);
        assert_eq!(encode_out(SVMI, &cal, true, 0, 99999.0), 65535);
    }

    #[test]
    fn scaling_formulas() {
        // volts = raw * 5/65536
        let raw: u16 = 32768;
        assert!((raw as f64 * V_RESOLUTION - 2.5).abs() < 1e-9);
        // current = ((raw * 0.4/65536) - 0.195) * 1.25 (amps)
        let raw: u16 = 0;
        let i = (raw as f64 * I_RESOLUTION - 0.195) * 1.25;
        assert!((i - -0.24375).abs() < 1e-9);
    }

    #[test]
    fn sof_alignment() {
        // sof_start = (((sof >> 3) + 0x1f) & 0x7FF) << 3
        let sof: u16 = 0x1234;
        let out = (((sof >> 3) + 0x1f) & 0x7FF) << 3;
        assert_eq!(out, (((0x1234 >> 3) + 0x1f) & 0x7FF) << 3);
        // always a multiple of 8 (frame-aligned)
        assert_eq!(out % 8, 0);
    }

    #[test]
    fn eeprom_roundtrip() {
        let mut cal = M1kCal::default();
        cal.offset[3] = 0.25;
        cal.gain_p[7] = 1.5;
        cal.gain_n[0] = 0.5;
        let bytes = cal.to_bytes();
        assert_eq!(bytes.len(), 100);
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), EEPROM_VALID);
        let back = M1kCal::from_bytes(&bytes).unwrap();
        assert!(back.valid);
        assert_eq!(back.offset[3], 0.25);
        assert_eq!(back.gain_p[7], 1.5);
        assert_eq!(back.gain_n[0], 0.5);
        // invalid magic -> defaults
        let mut bad = bytes.clone();
        bad[0] = 0;
        let back = M1kCal::from_bytes(&bad).unwrap();
        assert!(!back.valid);
        assert_eq!(back.gain_p[7], 1.0);
    }
}
