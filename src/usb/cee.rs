// CEE device support, ported from cee/cee.cpp + cee.hpp.

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

const CMD_CONFIG_CAPTURE: u8 = 0x80;
const CMD_CONFIG_GAIN: u8 = 0x65;
const CMD_ISET_DAC: u8 = 0x15;
const DEVMODE_OFF: u16 = 0;
const DEVMODE_2SMU: u16 = 1;

const TIMER_CLOCK: f64 = 4e6; // 4 MHz
const DEFAULT_SAMPLE_TIME: f64 = 1.0 / 10000.0;
const CURRENT_GAIN_SCALE: f64 = 100000.0;
const DEFAULT_CURRENT_GAIN: u32 = (45.0 * 0.07 * 100000.0) as u32;

const V_MIN: f32 = 0.0;
const V_MAX: f32 = 5.0;
const DEFAULT_CURRENT_LIMIT: u32 = 200;

const IN_SAMPLES_PER_PACKET: usize = 10;
const OUT_SAMPLES_PER_PACKET: usize = 10;
const IN_PACKET_SIZE: usize = 4 + 10 * 6; // 64 bytes
const OUT_PACKET_SIZE: usize = 2 + 10 * 3; // 32 bytes
const FLAG_PACKET_DROPPED: u8 = 1 << 0;

const NTRANSFERS: usize = 4;

#[cfg(windows)]
const BUFFER_TIME: f64 = 0.050;
#[cfg(not(windows))]
const BUFFER_TIME: f64 = 0.020;

pub const EEPROM_VALID_MAGIC: u32 = 0x90e26cee;
const EEPROM_FLAG_USB_POWER: u8 = 1 << 0;

// CEE channel modes
const DISABLED: u8 = 0;
const SVMI: u8 = 1;
const SIMV: u8 = 2;

#[derive(Clone, Copy)]
pub struct CeeCal {
    pub magic: u32,
    pub offset_a_v: i8,
    pub offset_a_i: i8,
    pub offset_b_v: i8,
    pub offset_b_i: i8,
    pub dac200_a: i16,
    pub dac200_b: i16,
    pub dac400_a: i16,
    pub dac400_b: i16,
    pub current_gain_a: u32,
    pub current_gain_b: u32,
    pub flags: u8,
}

impl CeeCal {
    fn defaults() -> CeeCal {
        CeeCal {
            magic: 0xffffffff,
            offset_a_v: 0,
            offset_a_i: 0,
            offset_b_v: 0,
            offset_b_i: 0,
            dac200_a: 0x6B7,
            dac200_b: 0x6B7,
            dac400_a: 0x6B7,
            dac400_b: 0x6B7,
            current_gain_a: 0xffffffff,
            current_gain_b: 0xffffffff,
            flags: 0xff,
        }
    }

    fn from_bytes(d: &[u8]) -> Option<CeeCal> {
        if d.len() < 25 {
            return None;
        }
        Some(CeeCal {
            magic: u32::from_le_bytes(d[0..4].try_into().unwrap()),
            offset_a_v: d[4] as i8,
            offset_a_i: d[5] as i8,
            offset_b_v: d[6] as i8,
            offset_b_i: d[7] as i8,
            dac200_a: i16::from_le_bytes(d[8..10].try_into().unwrap()),
            dac200_b: i16::from_le_bytes(d[10..12].try_into().unwrap()),
            dac400_a: i16::from_le_bytes(d[12..14].try_into().unwrap()),
            dac400_b: i16::from_le_bytes(d[14..16].try_into().unwrap()),
            current_gain_a: u32::from_le_bytes(d[16..20].try_into().unwrap()),
            current_gain_b: u32::from_le_bytes(d[20..24].try_into().unwrap()),
            flags: d[24],
        })
    }

    #[allow(clippy::wrong_self_convention)] // serializer, not a conversion
    fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(25);
        b.extend_from_slice(&self.magic.to_le_bytes());
        b.push(self.offset_a_v as u8);
        b.push(self.offset_a_i as u8);
        b.push(self.offset_b_v as u8);
        b.push(self.offset_b_i as u8);
        b.extend_from_slice(&self.dac200_a.to_le_bytes());
        b.extend_from_slice(&self.dac200_b.to_le_bytes());
        b.extend_from_slice(&self.dac400_a.to_le_bytes());
        b.extend_from_slice(&self.dac400_b.to_le_bytes());
        b.extend_from_slice(&self.current_gain_a.to_le_bytes());
        b.extend_from_slice(&self.current_gain_b.to_le_bytes());
        b.push(self.flags);
        b
    }
}

pub struct CeeBackend {
    pub handle: UsbHandle,
    pub self_ref: SelfRef,
    interface: Option<nusb::Interface>,
    tasks: Vec<tokio::task::JoinHandle<()>>,

    pub cal: CeeCal,
    pub min_per: u32,
    pub xmega_per: u32,
    pub packets_per_transfer: usize,
    pub first_packet: bool,
}

fn be(dev: &StreamingDevice) -> &CeeBackend {
    match &dev.backend {
        Backend::Cee(b) => b,
        _ => unreachable!("not a CEE"),
    }
}

fn bm(dev: &mut StreamingDevice) -> &mut CeeBackend {
    match &mut dev.backend {
        Backend::Cee(b) => b,
        _ => unreachable!("not a CEE"),
    }
}

fn signextend12(v: u16) -> i16 {
    if v > 0x7ff {
        v as i16 - 4096
    } else {
        v as i16
    }
}

/// Unpack one 6-byte IN sample into (av, ai, bv, bi).
fn unpack_in_sample(d: &[u8]) -> (i16, i16, i16, i16) {
    let (avl, ail, aih_avh, bvl, bil, bih_bvh) = (
        d[0] as u16,
        d[1] as u16,
        d[2] as u16,
        d[3] as u16,
        d[4] as u16,
        d[5] as u16,
    );
    (
        signextend12(((aih_avh & 0x0f) << 8) | avl),
        signextend12(((aih_avh & 0xf0) << 4) | ail),
        signextend12(((bih_bvh & 0x0f) << 8) | bvl),
        signextend12(((bih_bvh & 0xf0) << 4) | bil),
    )
}

/// Pack two 12-bit values into a 3-byte OUT sample.
fn pack_out_sample(a: u16, b: u16) -> [u8; 3] {
    [
        (a & 0xff) as u8,
        (b & 0xff) as u8,
        (((b >> 4) & 0xf0) | (a >> 8)) as u8,
    ]
}

pub fn create(handle: UsbHandle, serial: String) -> std::result::Result<DevicePtr, Error> {
    eprintln!("Found a CEE: \n    Serial: {serial}");

    let hw_version = handle.read_string(0x00, 0, 0);
    let fw = handle.read_string(0x00, 0, 1);

    let mut git_version = String::new();
    let mut min_per: u32 = 100;
    if fw.as_str() >= "1.2" {
        let (r, data) = handle.control_in(0xC0, 0x00, 0, 0xff, 5);
        if r >= 5 {
            let per_ns = data[3];
            min_per = data[4] as u32;
            if per_ns != 250 {
                eprintln!(
                    "    Error: alternate timer clock {per_ns} is not supported in this release."
                );
            }
        }
        git_version = handle.read_string(0x00, 0, 2);
    }

    let fw_version = if git_version.is_empty() {
        fw.clone()
    } else {
        format!("{fw}/{git_version}")
    };

    eprintln!("    Hardware: {hw_version}");
    eprintln!("    Firmware version: {fw} ({git_version})");

    // Reset the state
    handle.control_out(0x40, CMD_CONFIG_CAPTURE, 0, DEVMODE_OFF, &[]);

    // Reset the gains (a_i and b_i have normalGain 2)
    handle.control_out(0x40, CMD_CONFIG_GAIN, 0x01 << 2, 0, &[]);
    handle.control_out(0x40, CMD_CONFIG_GAIN, 0x00 << 2, 1, &[]);
    handle.control_out(0x40, CMD_CONFIG_GAIN, 0x00 << 2, 2, &[]);
    handle.control_out(0x40, CMD_CONFIG_GAIN, 0x01 << 2, 3, &[]);

    // Read calibration
    let (r, data) = handle.control_in(0xC0, 0xE0, 0, 0, 64);
    let mut cal = if r > 0 {
        CeeCal::from_bytes(&data).filter(|c| c.magic == EEPROM_VALID_MAGIC)
    } else {
        None
    }
    .unwrap_or_else(|| {
        eprintln!("    Reading calibration data failed {r}");
        CeeCal::defaults()
    });

    let current_limit = if cal.flags & EEPROM_FLAG_USB_POWER != 0 {
        DEFAULT_CURRENT_LIMIT
    } else {
        2000
    };

    if cal.current_gain_a == u32::MAX {
        cal.current_gain_a = DEFAULT_CURRENT_GAIN;
    }
    if cal.current_gain_b == u32::MAX {
        cal.current_gain_b = DEFAULT_CURRENT_GAIN;
    }
    eprintln!(
        "    Current gain {} {}",
        cal.current_gain_a, cal.current_gain_b
    );

    let backend = CeeBackend {
        handle,
        self_ref: SelfRef::new(),
        interface: None,
        tasks: Vec::new(),
        cal,
        min_per,
        xmega_per: 0,
        packets_per_transfer: 1,
        first_packet: true,
    };

    let mut dev = StreamingDevice {
        model: "com.nonolithlabs.cee".into(),
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
        min_sample_time: min_per as f64 / TIMER_CLOCK,
        capture_i: 0,
        capture_o: 0,
        current_limit: 0,
        backend: Backend::Cee(Box::new(backend)),
    };

    set_current_limit(&mut dev, current_limit);

    let samples = (12.0 / DEFAULT_SAMPLE_TIME).ceil() as u32;
    dev.configure(0, DEFAULT_SAMPLE_TIME, samples, true, false);

    Ok(Arc::new(Mutex::new(AnyDevice::Streaming(dev))))
}

pub fn configure(
    dev: &mut StreamingDevice,
    mode: i32,
    sample_time: f64,
    samples: u32,
    continuous: bool,
    raw: bool,
) {
    let min_per = be(dev).min_per;
    let mut per = (sample_time * TIMER_CLOCK).round() as u32;
    if per < min_per {
        per = min_per;
    }
    dev.sample_time = per as f64 / TIMER_CLOCK;

    dev.capture_samples = samples;
    dev.capture_continuous = continuous;
    dev.dev_mode = mode.max(0) as u32;
    dev.raw_mode = raw;
    dev.capture_length = samples as f32 * dev.sample_time as f32;
    dev.capture_i = 0;
    dev.capture_o = 0;

    let ppt = (BUFFER_TIME / (dev.sample_time * 10.0) / NTRANSFERS as f64).ceil() as usize;
    let (gain_a, gain_b) = {
        let b = bm(dev);
        b.xmega_per = per;
        b.packets_per_transfer = ppt.max(1);
        (b.cal.current_gain_a, b.cal.current_gain_b)
    };

    eprintln!(
        "CEE prepare {} {} {} {} {}",
        per,
        NTRANSFERS,
        ppt.max(1),
        samples,
        dev.current_limit
    );

    // Preserve gains across reconfigure (the C++ streams are persistent
    // members; ours are rebuilt)
    let prev_gains: Vec<Vec<u32>> = dev
        .channels
        .iter()
        .map(|c| c.streams.iter().map(|s| s.gain).collect())
        .collect();

    dev.channels.clear();
    if dev.dev_mode == 0 {
        let current_limit = dev.current_limit;
        for (ci, (cid, cname, igain)) in [("a", "A", gain_a), ("b", "B", gain_b)].iter().enumerate()
        {
            let mut c = Channel::new(cid, cname);
            c.source = Some(OutputSource::constant(0, 0.0));
            let (mut v, mut i) = if raw {
                (
                    Stream::new(
                        "v",
                        &format!("Voltage {cname}"),
                        "LSB",
                        -100.0,
                        2047.0,
                        1,
                        V_MAX / 2048.0,
                        1,
                    ),
                    Stream::new(
                        "i",
                        &format!("Current {cname}"),
                        "LSB",
                        -2048.0,
                        2047.0,
                        2,
                        1.0,
                        2,
                    ),
                )
            } else {
                let mut limit = 2.5 / (*igain as f64 / CURRENT_GAIN_SCALE) / 2.0 * 1000.0;
                if limit > current_limit as f64 {
                    limit = current_limit as f64;
                }
                (
                    Stream::new(
                        "v",
                        &format!("Voltage {cname}"),
                        "V",
                        V_MIN,
                        V_MAX,
                        1,
                        V_MAX / 2048.0,
                        1,
                    ),
                    Stream::new(
                        "i",
                        &format!("Current {cname}"),
                        "mA",
                        -limit as f32,
                        limit as f32,
                        2,
                        1.0,
                        2,
                    ),
                )
            };
            if let Some(g) = prev_gains.get(ci) {
                if let Some(&gv) = g.first() {
                    v.gain = gv;
                }
                if let Some(&gi) = g.get(1) {
                    i.gain = gi;
                }
            }
            v.allocate(samples);
            i.allocate(samples);
            c.streams.push(v);
            c.streams.push(i);
            dev.channels.push(c);
        }
    }
}

pub fn set_current_limit(dev: &mut StreamingDevice, mode: u32) {
    let b = bm(dev);
    let (a, bb) = match mode {
        200 => (b.cal.dac200_a, b.cal.dac200_b),
        400 => (b.cal.dac400_a, b.cal.dac400_b),
        2000 => (0, 0),
        _ => {
            eprintln!("Invalid current limit {mode}");
            return;
        }
    };
    b.handle
        .control_in(0xC0, CMD_ISET_DAC, a as u16, bb as u16, 0);
    dev.current_limit = mode;
}

pub fn set_internal_gain(dev: &mut StreamingDevice, chan: usize, stream: usize, gain: i32) {
    // stream index for the gain command: 0=a_i, 1=a_v, 2=b_v, 3=b_i
    let streamval: u16 = match (chan, stream) {
        (0, 1) => 0, // a_i
        (0, 0) => 1, // a_v
        (1, 0) => 2, // b_v
        (1, 1) => 3, // b_i
        _ => return,
    };
    let gainval: u16 = match gain {
        1 => 0x00 << 2,
        2 => 0x01 << 2,
        4 => 0x02 << 2,
        8 => 0x03 << 2,
        16 => 0x04 << 2,
        32 => 0x05 << 2,
        64 => 0x06 << 2,
        _ => return,
    };

    dev.channels[chan].streams[stream].gain = gain as u32;

    let was_capturing = dev.capture_state;
    if was_capturing {
        on_pause_capture(dev);
    }

    be(dev)
        .handle
        .control_out(0x40, CMD_CONFIG_GAIN, gainval, streamval, &[]);

    if was_capturing {
        on_start_capture(dev);
    }

    eprintln!(
        "Set gain {} {} {gain} {streamval} {gainval}",
        dev.channels[chan].id, dev.channels[chan].streams[stream].id
    );
    dev.notify_gain_changed(chan, stream);
}

pub fn on_reset_capture(dev: &mut StreamingDevice) {
    for c in &mut dev.channels {
        if let Some(src) = &mut c.source {
            src.start_sample = 0;
        }
    }
}

pub fn on_start_capture(dev: &mut StreamingDevice) {
    let interface = match be(dev).handle.device.claim_interface(0).wait() {
        Ok(i) => i,
        Err(e) => {
            eprintln!("CEE: could not claim interface: {e}");
            return;
        }
    };

    let per = be(dev).xmega_per as u16;
    be(dev)
        .handle
        .control_out(0x40, CMD_CONFIG_CAPTURE, per, DEVMODE_2SMU, &[]);

    // Ignore the effect of output samples we sent before pausing
    dev.capture_o = dev.capture_i;
    bm(dev).first_packet = true;

    let ppt = be(dev).packets_per_transfer;
    let self_ref = be(dev).self_ref.clone();

    let ep_in = interface.endpoint::<Bulk, In>(EP_BULK_IN);
    let ep_out = interface.endpoint::<Bulk, Out>(EP_BULK_OUT);
    let (ep_in, ep_out) = match (ep_in, ep_out) {
        (Ok(i), Ok(o)) => (i, o),
        _ => {
            eprintln!("CEE: could not open bulk endpoints");
            return;
        }
    };

    let mut tasks = Vec::new();

    {
        let self_ref = self_ref.clone();
        let isize = IN_PACKET_SIZE * ppt;
        tasks.push(tokio::spawn(async move {
            let mut ep = ep_in;
            for _ in 0..NTRANSFERS {
                let mut b = ep.allocate(isize);
                b.set_requested_len(isize);
                ep.submit(b);
            }
            loop {
                let c = ep.next_complete().await;
                if c.status.is_err() {
                    eprintln!("CEE IN transfer error: {:?}", c.status);
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
                // DISABLE_SELF_STOP in the C++: always resubmit; completion
                // pauses capture and aborts this task.
                let mut b = buffer;
                b.clear();
                b.set_requested_len(isize);
                ep.submit(b);
            }
        }));
    }

    {
        let self_ref = self_ref.clone();
        tasks.push(tokio::spawn(async move {
            let mut ep = ep_out;
            for _ in 0..NTRANSFERS {
                let Some(dev) = self_ref.upgrade() else {
                    return;
                };
                let data = {
                    let mut dev = dev.lock().unwrap();
                    match &mut *dev {
                        AnyDevice::Streaming(d) => fill_out_transfer(d),
                        _ => return,
                    }
                };
                ep.submit(Buffer::from(data));
            }
            loop {
                let c = ep.next_complete().await;
                if c.status.is_err() {
                    eprintln!("CEE OUT transfer error: {:?}", c.status);
                    break;
                }
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
        b.handle
            .control_out(0x40, CMD_CONFIG_CAPTURE, 0, DEVMODE_OFF, &[]);
        for t in b.tasks.drain(..) {
            t.abort();
        }
        b.interface = None;
    }
    dev.capture_o = dev.capture_i;
}

fn handle_in_transfer(dev: &mut StreamingDevice, buffer: &[u8]) {
    let (ppt, cal) = {
        let b = be(dev);
        (b.packets_per_transfer, b.cal)
    };

    let (v_factor, i_factor_a, i_factor_b) = if dev.raw_mode {
        (1.0f64, 1.0f64, 1.0f64)
    } else {
        (
            5.0 / 2048.0,
            2.5 / 2048.0 / (cal.current_gain_a as f64 / CURRENT_GAIN_SCALE) * 1000.0,
            2.5 / 2048.0 / (cal.current_gain_b as f64 / CURRENT_GAIN_SCALE) * 1000.0,
        )
    };

    let gains: Vec<f64> = vec![
        dev.channels[0].streams[0].gain as f64,
        dev.channels[0].streams[1].gain as f64,
        dev.channels[1].streams[0].gain as f64,
        dev.channels[1].streams[1].gain as f64,
    ];

    let mut drop_broadcast = false;

    for p in 0..ppt {
        let base = p * IN_PACKET_SIZE;
        if base + IN_PACKET_SIZE > buffer.len() {
            break;
        }
        let pkt = &buffer[base..base + IN_PACKET_SIZE];
        let (mode_a, mode_b, flags) = (pkt[0], pkt[1], pkt[2]);

        if flags & FLAG_PACKET_DROPPED != 0 && !be(dev).first_packet {
            eprintln!("Warning: dropped packet");
            drop_broadcast = true;
        }
        bm(dev).first_packet = false;

        for i in 0..IN_SAMPLES_PER_PACKET {
            let s = &pkt[4 + i * 6..4 + i * 6 + 6];
            let (av, ai, bv, bi) = unpack_in_sample(s);

            dev.put(
                0,
                0,
                ((cal.offset_a_v as f64 + av as f64) * v_factor / gains[0]) as f32,
            );
            if mode_a & 0x3 != DISABLED {
                dev.put(
                    0,
                    1,
                    ((cal.offset_a_i as f64 + ai as f64) * i_factor_a / gains[1]) as f32,
                );
            } else {
                dev.put(0, 1, 0.0);
            }
            dev.put(
                1,
                0,
                ((cal.offset_b_v as f64 + bv as f64) * v_factor / gains[2]) as f32,
            );
            if mode_b & 0x3 != DISABLED {
                dev.put(
                    1,
                    1,
                    ((cal.offset_b_i as f64 + bi as f64) * i_factor_b / gains[3]) as f32,
                );
            } else {
                dev.put(1, 1, 0.0);
            }
            dev.sample_done();
        }
    }

    if drop_broadcast {
        dev.broadcast_json(json!({"_action": "packetDrop"}));
    }

    dev.packet_done();
    dev.check_output_effective(0);
    dev.check_output_effective(1);
}

fn encode_out(raw_mode: bool, current_limit: u32, mode: u8, val: f32, igain: u32) -> u16 {
    if raw_mode {
        return val.clamp(0.0, 4095.0) as u16;
    }
    let mut v: i32 = 0;
    if mode == SVMI {
        let val = val.clamp(V_MIN, V_MAX);
        v = (4095.0 * val as f64 / 5.0) as i32;
    } else if mode == SIMV {
        let val = val.clamp(-(current_limit as f32), current_limit as f32);
        v = (4095.0 * (1.25 + (igain as f64 / CURRENT_GAIN_SCALE) * val as f64 / 1000.0) / 2.5)
            as i32;
    }
    v.clamp(0, 4095) as u16
}

fn fill_out_transfer(dev: &mut StreamingDevice) -> Vec<u8> {
    let (ppt, cal) = {
        let b = be(dev);
        (b.packets_per_transfer, b.cal)
    };
    let raw_mode = dev.raw_mode;
    let current_limit = dev.current_limit;
    let sample_time = dev.sample_time;
    let osize = OUT_PACKET_SIZE * ppt;
    let mut buf = vec![0u8; osize];

    if dev.channels.len() == 2
        && dev.channels[0].source.is_some()
        && dev.channels[1].source.is_some()
    {
        let mode_a = dev.channels[0].source.as_ref().unwrap().mode as u8;
        let mode_b = dev.channels[1].source.as_ref().unwrap().mode as u8;
        let mut o = dev.capture_o;
        for p in 0..ppt {
            let pkt = p * OUT_PACKET_SIZE;
            buf[pkt] = mode_a;
            buf[pkt + 1] = mode_b;
            for i in 0..OUT_SAMPLES_PER_PACKET {
                let av = dev.channels[0]
                    .source
                    .as_mut()
                    .unwrap()
                    .get_value(o, sample_time);
                let bv = dev.channels[1]
                    .source
                    .as_mut()
                    .unwrap()
                    .get_value(o, sample_time);
                let a = encode_out(raw_mode, current_limit, mode_a, av, cal.current_gain_a);
                let b = encode_out(raw_mode, current_limit, mode_b, bv, cal.current_gain_b);
                let s = pack_out_sample(a, b);
                buf[pkt + 2 + i * 3..pkt + 2 + i * 3 + 3].copy_from_slice(&s);
                o += 1;
            }
        }
        dev.capture_o = o;
    }

    buf
}

fn gpio(dev: &StreamingDevice, set: bool, dir: u8, out: u8) -> Value {
    let (r, buf) = be(dev).handle.control_in(
        0xC0,
        if set { 0x21 } else { 0x20 },
        out as u16,
        dir as u16,
        4,
    );
    let get = |i: usize| buf.get(i).copied().unwrap_or(0);
    json!({
        "status": r,
        "in": get(0),
        "dir": get(1),
        "out": get(2),
    })
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

// ---- WS commands (cee.cpp processMessage) ----

pub fn process_message(
    dev: &mut StreamingDevice,
    client: &ClientHandle,
    cmd: &str,
    n: &Value,
) -> Result<bool> {
    let id = json_int_prop_def(n, "id", 0);
    match cmd {
        "writeCalibration" => {
            {
                let cal = CeeCal {
                    magic: EEPROM_VALID_MAGIC,
                    offset_a_v: json_int_prop(n, "offset_a_v")? as i8,
                    offset_a_i: json_int_prop(n, "offset_a_i")? as i8,
                    offset_b_v: json_int_prop(n, "offset_b_v")? as i8,
                    offset_b_i: json_int_prop(n, "offset_b_i")? as i8,
                    dac200_a: json_int_prop(n, "dac200_a")? as i16,
                    dac200_b: json_int_prop(n, "dac200_b")? as i16,
                    dac400_a: json_int_prop(n, "dac400_a")? as i16,
                    dac400_b: json_int_prop(n, "dac400_b")? as i16,
                    current_gain_a: json_int_prop_def(n, "current_gain_a", -1) as u32,
                    current_gain_b: json_int_prop_def(n, "current_gain_b", -1) as u32,
                    flags: json_int_prop_def(n, "flags", 0xff) as u8,
                };
                bm(dev).cal = cal;
            }
            let bytes = be(dev).cal.to_bytes();
            let r = be(dev).handle.control_out(0x40, 0xE1, 0, 0, &bytes);
            eprintln!("Wrote calibration, {r}");
            client.send_json(ret(id, &[("status", json!(r))]));
        }
        "readCalibration" => {
            let cal = be(dev).cal;
            client.send_json(ret(
                id,
                &[
                    ("offset_a_v", json!(cal.offset_a_v)),
                    ("offset_a_i", json!(cal.offset_a_i)),
                    ("offset_b_v", json!(cal.offset_b_v)),
                    ("offset_b_i", json!(cal.offset_b_i)),
                    ("dac200_a", json!(cal.dac200_a)),
                    ("dac200_b", json!(cal.dac200_b)),
                    ("dac400_a", json!(cal.dac400_a)),
                    ("dac400_b", json!(cal.dac400_b)),
                    ("current_gain_a", json!(cal.current_gain_a)),
                    ("current_gain_b", json!(cal.current_gain_b)),
                    ("flags", json!(cal.flags)),
                ],
            ));
        }
        "tempCalibration" => {
            // RAM-only calibration offsets; no reply (matches the C++)
            let b = bm(dev);
            b.cal.offset_a_v = json_int_prop_def(n, "offset_a_v", 0) as i8;
            b.cal.offset_a_i = json_int_prop_def(n, "offset_a_i", 0) as i8;
            b.cal.offset_b_v = json_int_prop_def(n, "offset_b_v", 0) as i8;
            b.cal.offset_b_i = json_int_prop_def(n, "offset_b_i", 0) as i8;
            eprintln!("Applied temporary calibration");
        }
        _ => return Ok(false),
    }
    Ok(true)
}

// ---- REST (cee.cpp handleREST: /gpio) ----

pub fn handle_rest(
    dev: &mut StreamingDevice,
    req: &RestRequest,
    level: usize,
) -> Option<RestResponse> {
    let seg = req.parts.get(level).map(|s| s.as_str()).unwrap_or("");
    if seg != "gpio" {
        return None;
    }
    Some(if req.method == "POST" {
        let map = parse_query(&req.body);
        let dir = map
            .get("dir")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0) as u8;
        let out = map
            .get("out")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0) as u8;
        RestResponse::json(&gpio(dev, true, dir, out))
    } else {
        RestResponse::json(&gpio(dev, false, 0, 0))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_pack_unpack_roundtrip() {
        // 12-bit signed values pack into 6-byte IN samples
        for (av, ai, bv, bi) in [
            (0i16, 0, 0, 0),
            (2047, -2048, 1, -1),
            (-100, 100, -2000, 2000),
        ] {
            let e = |v: i16| (v as u16) & 0xfff;
            let s = [
                (e(av) & 0xff) as u8,
                (e(ai) & 0xff) as u8,
                (((e(ai) >> 4) & 0xf0) | (e(av) >> 8)) as u8,
                (e(bv) & 0xff) as u8,
                (e(bi) & 0xff) as u8,
                (((e(bi) >> 4) & 0xf0) | (e(bv) >> 8)) as u8,
            ];
            assert_eq!(unpack_in_sample(&s), (av, ai, bv, bi));
        }
    }

    #[test]
    fn out_sample_packing() {
        let s = pack_out_sample(0xABC, 0x123);
        assert_eq!(s[0], 0xBC); // a low byte
        assert_eq!(s[1], 0x23); // b low byte
        assert_eq!(s[2], ((0x123u16 >> 4) & 0xf0 | (0xABCu16 >> 8)) as u8);
    }

    #[test]
    fn encode_out_values() {
        // SVMI: 4095 * val / 5
        assert_eq!(encode_out(false, 200, SVMI, 0.0, DEFAULT_CURRENT_GAIN), 0);
        assert_eq!(
            encode_out(false, 200, SVMI, 5.0, DEFAULT_CURRENT_GAIN),
            4095
        );
        assert_eq!(
            encode_out(false, 200, SVMI, 2.5, DEFAULT_CURRENT_GAIN),
            2047
        );
        // SIMV: 4095 * (1.25 + gain*val/1000) / 2.5 with gain 3.15
        let v = encode_out(false, 200, SIMV, 0.0, DEFAULT_CURRENT_GAIN);
        assert_eq!(v, (4095.0 * 1.25 / 2.5) as u16);
        // disabled: 0
        assert_eq!(
            encode_out(false, 200, DISABLED, 3.0, DEFAULT_CURRENT_GAIN),
            0
        );
        // raw
        assert_eq!(
            encode_out(true, 200, SVMI, 5000.0, DEFAULT_CURRENT_GAIN),
            4095
        );
    }

    #[test]
    fn timer_period() {
        // xmega_per = round(st * 4e6), min 100; st = per/4e6
        let st = 1.0 / 10000.0;
        let per = (st * TIMER_CLOCK).round() as u32;
        assert_eq!(per, 400);
        assert_eq!(per as f64 / TIMER_CLOCK, st);
    }

    #[test]
    fn eeprom_roundtrip() {
        let cal = CeeCal {
            magic: EEPROM_VALID_MAGIC,
            offset_a_v: -5,
            offset_a_i: 3,
            offset_b_v: 0,
            offset_b_i: -1,
            dac200_a: 0x6B7,
            dac200_b: 0x6B8,
            dac400_a: 100,
            dac400_b: -100,
            current_gain_a: 315000,
            current_gain_b: 315001,
            flags: 1,
        };
        let bytes = cal.to_bytes();
        assert_eq!(bytes.len(), 25);
        let back = CeeCal::from_bytes(&bytes).unwrap();
        assert_eq!(back.magic, EEPROM_VALID_MAGIC);
        assert_eq!(back.offset_a_v, -5);
        assert_eq!(back.dac400_b, -100);
        assert_eq!(back.current_gain_b, 315001);
        assert_eq!(back.flags, 1);
    }
}
