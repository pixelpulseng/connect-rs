// StreamingDevice: capture state machine, sample ring buffer, output
// management, and WS command handling. Ported from
// streaming_device/streaming_device.cpp + ws_api.cpp.
//
// Counters are u64 (SPEC.md Q8 fix; the C++ u32 wrapped after ~11 hours).

use crate::device::ClientHandle;
use crate::jsonutil::*;
use crate::listener::{make_stream_listener, Listener};
use crate::source::{make_source, OutputSource};
use serde_json::{json, Map, Value};

pub struct Stream {
    pub id: String,
    pub display_name: String,
    pub units: String,
    pub min: f32,
    pub max: f32,
    /// mode for output that "sources" this stream's variable (0 = not supported)
    pub output_mode: u32,
    /// Internal device gain factor
    pub gain: u32,
    /// Default gain factor
    pub normal_gain: u32,
    pub uncertainty: f32,
    /// Raw sample ring buffer
    pub data: Vec<f32>,
}

impl Stream {
    pub fn new(
        id: &str,
        display_name: &str,
        units: &str,
        min: f32,
        max: f32,
        output_mode: u32,
        uncertainty: f32,
        gain: u32,
    ) -> Stream {
        Stream {
            id: id.to_string(),
            display_name: display_name.to_string(),
            units: units.to_string(),
            min,
            max,
            output_mode,
            gain,
            normal_gain: gain,
            uncertainty,
            data: Vec::new(),
        }
    }

    pub fn get_gain(&self) -> f64 {
        self.gain as f64 / self.normal_gain as f64
    }

    pub fn allocate(&mut self, size: u32) {
        self.data.clear();
        self.data.resize(size as usize, 0.0);
    }

    pub fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "displayName": self.display_name,
            "units": self.units,
            "min": f32_json(self.min),
            "max": f32_json(self.max),
            "outputMode": self.output_mode,
            "gain": self.get_gain(),
            "uncertainty": f32_json(self.uncertainty),
        })
    }
}

pub struct Channel {
    pub id: String,
    pub display_name: String,
    pub streams: Vec<Stream>,
    pub source: Option<OutputSource>,
}

impl Channel {
    pub fn new(id: &str, display_name: &str) -> Channel {
        Channel {
            id: id.to_string(),
            display_name: display_name.to_string(),
            streams: Vec::new(),
            source: None,
        }
    }

    pub fn stream_by_id(&self, id: &str) -> Option<usize> {
        self.streams.iter().position(|s| s.id == id)
    }

    pub fn to_json(&self) -> Value {
        let mut n = Map::new();
        n.insert("id".into(), json!(self.id));
        n.insert("displayName".into(), json!(self.display_name));
        if let Some(src) = &self.source {
            n.insert("output".into(), src.describe_json());
        }
        let mut streams = Map::new();
        for s in &self.streams {
            streams.insert(s.id.clone(), s.to_json());
        }
        n.insert("streams".into(), Value::Object(streams));
        Value::Object(n)
    }
}

/// Hardware-specific state and hooks. Test is the synthetic device used by
/// the unit tests (mirrors tests/support.hpp TestDevice).
pub enum Backend {
    Test(TestBackend),
    M1k(Box<crate::usb::m1k::M1kBackend>),
    Cee(Box<crate::usb::cee::CeeBackend>),
}

#[derive(Default)]
pub struct TestBackend {
    pub resets: u32,
    pub starts: u32,
    pub pauses: u32,
    pub gain_sets: Vec<(String, String, i32)>,
    pub current_limits: Vec<u32>,
}

pub struct StreamingDevice {
    pub model: String,
    pub hw_version: String,
    pub fw_version: String,
    pub serial: String,

    pub connections: Vec<ClientHandle>,
    pub channels: Vec<Channel>,
    pub listeners: Vec<Listener>,

    pub dev_mode: u32,
    pub raw_mode: bool,
    pub capture_state: bool,
    pub capture_done: bool,
    pub capture_length: f32,
    pub capture_samples: u32,
    pub capture_continuous: bool,
    pub sample_time: f64,
    pub min_sample_time: f64,
    /// IN sample counter; next write position is capture_i % capture_samples
    pub capture_i: u64,
    /// OUT sample counter
    pub capture_o: u64,
    pub current_limit: u32,

    pub backend: Backend,
}

impl StreamingDevice {
    pub fn get_id(&self) -> String {
        format!("{}~{}", self.model, self.serial)
    }

    // ---- ring buffer ----

    /// Store a sample. Call sample_done() after putting one sample to each
    /// stream.
    pub fn put(&mut self, chan: usize, stream: usize, v: f32) {
        let caps = self.capture_samples as u64;
        let i = self.capture_i;
        let continuous = self.capture_continuous;
        let s = &mut self.channels[chan].streams[stream];
        if s.data.is_empty() || caps == 0 || (i >= caps && !continuous) {
            return;
        }
        s.data[(i % caps) as usize] = v;
    }

    /// Get the sample with index i, or NaN if overwritten / not yet collected.
    pub fn get(&self, chan: usize, stream: usize, i: u64) -> f32 {
        let caps = self.capture_samples as u64;
        let s = &self.channels[chan].streams[stream];
        if s.data.is_empty()
            || caps == 0
            || i >= self.capture_i // not yet collected
            || (self.capture_i > caps && i <= self.capture_i - caps)
        // overwritten
        {
            f32::NAN
        } else {
            s.data[(i % caps) as usize]
        }
    }

    /// Mean of `count` samples starting at `start` (f32 accumulation, like
    /// the C++). NaN if any part of the window is unavailable.
    pub fn resample(&self, chan: usize, stream: usize, start: u64, count: u64) -> f32 {
        let caps = self.capture_samples as u64;
        let s = &self.channels[chan].streams[stream];
        if s.data.is_empty()
            || caps == 0
            || start + count > self.capture_i
            || (self.capture_i > caps && start <= self.capture_i - caps)
        {
            return f32::NAN;
        }
        let mut total: f32 = 0.0;
        for i in 0..count {
            total += s.data[((start + i) % caps) as usize];
        }
        total / count as f32
    }

    pub fn buffer_min(&self) -> u64 {
        if self.capture_i < self.capture_samples as u64 {
            0
        } else {
            self.capture_i - self.capture_samples as u64
        }
    }

    pub fn buffer_max(&self) -> u64 {
        self.capture_i
    }

    pub fn sample_done(&mut self) {
        self.capture_i += 1;
    }

    /// Called after each input packet: feed listeners, check completion.
    pub fn packet_done(&mut self) {
        self.handle_new_data();
        if !self.capture_continuous && self.capture_i >= self.capture_samples as u64 {
            self.done_capture();
        }
    }

    // ---- listeners ----

    pub fn add_listener(&mut self, mut l: Listener) {
        if l.handle_new_data(self) {
            self.listeners.push(l);
        }
    }

    pub fn find_listener(&self, client_id: u64, id: i64) -> Option<usize> {
        self.listeners
            .iter()
            .position(|l| l.is_from_client(client_id) && l.id == id)
    }

    pub fn cancel_listen(&mut self, idx: Option<usize>) {
        if let Some(i) = idx {
            self.listeners.remove(i);
        }
    }

    pub fn clear_all_listeners(&mut self) {
        self.listeners.clear();
    }

    pub fn reset_all_listeners(&mut self) {
        for l in &mut self.listeners {
            l.reset();
        }
    }

    fn handle_new_data(&mut self) {
        let mut listeners = std::mem::take(&mut self.listeners);
        listeners.retain_mut(|l| l.handle_new_data(self));
        debug_assert!(self.listeners.is_empty());
        self.listeners = listeners;
    }

    // ---- capture state machine ----

    pub fn reset_capture(&mut self) {
        self.capture_done = false;
        self.capture_i = 0;
        self.capture_o = 0;
        self.on_reset_capture();
        self.notify_capture_reset();
    }

    pub fn start_capture(&mut self) {
        if !self.capture_state {
            if self.capture_done {
                self.reset_capture();
            }
            eprintln!("Start capture");
            self.on_start_capture();
            self.capture_state = true;
            self.notify_capture_state();
        }
    }

    pub fn pause_capture(&mut self) {
        if self.capture_state {
            self.capture_state = false;
            eprintln!("Pause capture");
            self.on_pause_capture();
            self.notify_capture_state();
        }
    }

    fn done_capture(&mut self) {
        self.capture_done = true;
        eprintln!("Done capture");
        if self.capture_state {
            self.capture_state = false;
            self.on_pause_capture();
        }
        self.notify_capture_state();
    }

    // ---- backend hooks ----

    fn on_reset_capture(&mut self) {
        match &mut self.backend {
            Backend::Test(t) => t.resets += 1,
            Backend::M1k(_) => crate::usb::m1k::on_reset_capture(self),
            Backend::Cee(_) => crate::usb::cee::on_reset_capture(self),
        }
    }

    fn on_start_capture(&mut self) {
        match &mut self.backend {
            Backend::Test(t) => t.starts += 1,
            Backend::M1k(_) => crate::usb::m1k::on_start_capture(self),
            Backend::Cee(_) => crate::usb::cee::on_start_capture(self),
        }
    }

    fn on_pause_capture(&mut self) {
        match &mut self.backend {
            Backend::Test(t) => t.pauses += 1,
            Backend::M1k(_) => crate::usb::m1k::on_pause_capture(self),
            Backend::Cee(_) => crate::usb::cee::on_pause_capture(self),
        }
    }

    /// Rebuild channels/streams for new capture parameters. Pauses capture,
    /// zeroes counters, clears listeners, broadcasts deviceConfig.
    pub fn configure(&mut self, mode: i32, sample_time: f64, samples: u32, continuous: bool, raw: bool) {
        self.pause_capture();
        match &self.backend {
            Backend::Test(_) => configure_test(self, mode, sample_time, samples, continuous, raw),
            Backend::M1k(_) => crate::usb::m1k::configure(self, mode, sample_time, samples, continuous, raw),
            Backend::Cee(_) => crate::usb::cee::configure(self, mode, sample_time, samples, continuous, raw),
        }
        self.notify_config();
    }

    /// setGain: gain is the user-visible multiplier; internal gain is
    /// round(gain * normalGain).
    pub fn set_gain(&mut self, chan: usize, stream: usize, gain: f64) {
        let internal = (gain * self.channels[chan].streams[stream].normal_gain as f64).round() as i32;
        match &mut self.backend {
            Backend::Test(_) => {
                let (cid, sid) = {
                    let c = &self.channels[chan];
                    (c.id.clone(), c.streams[stream].id.clone())
                };
                if let Backend::Test(t) = &mut self.backend {
                    t.gain_sets.push((cid, sid, internal));
                }
                if internal >= 1 {
                    self.channels[chan].streams[stream].gain = internal as u32;
                }
                self.notify_gain_changed(chan, stream);
            }
            Backend::M1k(_) => {} // M1K has no internal gain
            Backend::Cee(_) => crate::usb::cee::set_internal_gain(self, chan, stream, internal),
        }
    }

    pub fn set_current_limit(&mut self, limit: u32) {
        match &mut self.backend {
            Backend::Test(t) => t.current_limits.push(limit),
            Backend::M1k(_) => {}
            Backend::Cee(_) => crate::usb::cee::set_current_limit(self, limit),
        }
    }

    /// Replace a channel's output source (`set` command / POST /output).
    pub fn set_output(&mut self, chan: usize, mut source: OutputSource) {
        // The sample currently being encoded can't be affected on real
        // hardware, so USB backends mark the source effective one sample
        // later than the base class did.
        let start_offset = match &self.backend {
            Backend::Test(_) => 0,
            Backend::M1k(_) | Backend::Cee(_) => 1,
        };
        source.initialize(self.capture_o, self.channels[chan].source.as_ref());
        source.start_sample = self.capture_o + start_offset;
        self.channels[chan].source = Some(source);

        if let Backend::M1k(_) = &self.backend {
            crate::usb::m1k::on_set_output(self, chan);
        }

        self.notify_output_changed(chan);
    }

    /// Mark a source effective (and re-broadcast) once its effect is visible
    /// in the input stream.
    pub fn check_output_effective(&mut self, chan: usize) {
        let Some(src) = &self.channels[chan].source else {
            return;
        };
        if !src.effective && self.capture_i > src.start_sample {
            self.channels[chan].source.as_mut().unwrap().effective = true;
            self.notify_output_changed(chan);
        }
    }

    // ---- lookups ----

    pub fn channel_by_id(&self, id: &str) -> Option<usize> {
        self.channels.iter().position(|c| c.id == id)
    }

    pub fn find_stream(&self, channel_id: &str, stream_id: &str) -> Result<(usize, usize)> {
        let c = self
            .channel_by_id(channel_id)
            .ok_or_else(|| Error::new("Channel not found"))?;
        let s = self.channels[c]
            .stream_by_id(stream_id)
            .ok_or_else(|| Error::new("Stream not found"))?;
        Ok((c, s))
    }

    // ---- serialization ----

    pub fn state_to_json(&self, config_only: bool) -> Value {
        let mut n = Map::new();
        if !config_only {
            n.insert("id".into(), json!(self.get_id()));
            n.insert("model".into(), json!(self.model));
            n.insert("hwVersion".into(), json!(self.hw_version));
            n.insert("fwVersion".into(), json!(self.fw_version));
            n.insert("serial".into(), json!(self.serial));
        }

        n.insert("sampleTime".into(), json!(self.sample_time));
        n.insert("minSampleTime".into(), json!(self.min_sample_time));
        n.insert("mode".into(), json!(self.dev_mode));
        n.insert("samples".into(), json!(self.capture_samples));
        n.insert("length".into(), f32_json(self.capture_length));
        n.insert("continuous".into(), json!(self.capture_continuous));
        n.insert("raw".into(), json!(self.raw_mode));
        n.insert("currentLimit".into(), json!(self.current_limit));

        if config_only {
            return Value::Object(n);
        }

        n.insert("captureState".into(), json!(self.capture_state));
        n.insert("captureDone".into(), json!(self.capture_done));

        let mut channels = Map::new();
        for c in &self.channels {
            channels.insert(c.id.clone(), c.to_json());
        }
        n.insert("channels".into(), Value::Object(channels));

        // M1K appends frontend switch state and mode names
        if let Backend::M1k(_) = &self.backend {
            crate::usb::m1k::state_extras(self, &mut n);
        }

        Value::Object(n)
    }

    // ---- notifications ----

    pub fn broadcast_json(&self, v: Value) {
        for c in &self.connections {
            c.send_json(v.clone());
        }
    }

    fn notify_capture_state(&self) {
        self.broadcast_json(json!({
            "_action": "captureState",
            "state": self.capture_state,
            "done": self.capture_done,
        }));
    }

    fn notify_capture_reset(&mut self) {
        self.reset_all_listeners();
        self.broadcast_json(json!({"_action": "captureReset"}));
    }

    pub fn notify_config(&mut self) {
        self.clear_all_listeners();
        self.broadcast_json(json!({
            "_action": "deviceConfig",
            "device": self.state_to_json(false),
        }));
    }

    pub fn notify_output_changed(&self, chan: usize) {
        let c = &self.channels[chan];
        let Some(src) = &c.source else { return };
        let mut n = Map::new();
        n.insert("_action".into(), json!("outputChanged"));
        n.insert("channel".into(), json!(c.id));
        src.describe(&mut n);
        self.broadcast_json(Value::Object(n));
    }

    pub fn notify_gain_changed(&self, chan: usize, stream: usize) {
        let c = &self.channels[chan];
        self.broadcast_json(json!({
            "_action": "gainChanged",
            "channel": c.id,
            "stream": c.streams[stream].id,
            "gain": c.streams[stream].get_gain(),
        }));
    }

    // ---- client attachment ----

    pub fn on_client_attach(&mut self, client: &ClientHandle) {
        self.connections.push(client.clone());
        client.send_json(json!({
            "_action": "deviceConfig",
            "device": self.state_to_json(false),
        }));
    }

    pub fn on_client_detach(&mut self, client_id: u64) {
        self.connections.retain(|c| c.id != client_id);
        self.listeners.retain(|l| !l.is_from_client(client_id));
    }

    pub fn on_disconnect(&mut self) {
        self.broadcast_json(json!({"_action": "deviceDisconnected"}));
        self.clear_all_listeners();
    }

    // ---- WS command dispatch (ws_api.cpp + device-specific handlers) ----

    /// Returns Ok(true) if handled, Ok(false) if the command is unknown.
    pub fn process_message(&mut self, client: &ClientHandle, cmd: &str, n: &Value) -> Result<bool> {
        // Device-specific handler runs first (m1k.cpp / cee.cpp), then the
        // USB passthrough, then the StreamingDevice commands.
        match &self.backend {
            Backend::M1k(_) => {
                if crate::usb::m1k::process_message(self, client, cmd, n)? {
                    return Ok(true);
                }
                if crate::usb::process_usb_message(self, client, cmd, n)? {
                    return Ok(true);
                }
            }
            Backend::Cee(_) => {
                if crate::usb::cee::process_message(self, client, cmd, n)? {
                    return Ok(true);
                }
                if crate::usb::process_usb_message(self, client, cmd, n)? {
                    return Ok(true);
                }
            }
            Backend::Test(_) => {}
        }

        match cmd {
            "listen" => {
                let idx = self.find_listener(client.id, json_int_prop(n, "id")?);
                self.cancel_listen(idx);
                let l = make_stream_listener(self, client, n)?;
                self.add_listener(l);
            }
            "cancelListen" => {
                let idx = self.find_listener(client.id, json_int_prop(n, "id")?);
                self.cancel_listen(idx);
            }
            "configure" => {
                let mode = json_int_prop(n, "mode")? as i32;
                let samples = json_int_prop(n, "samples")? as u32;
                let sample_time = json_float_prop(n, "sampleTime")?;
                let continuous = json_bool_prop_def(n, "continuous", false);
                let raw = json_bool_prop_def(n, "raw", false);
                self.configure(mode, sample_time, samples, continuous, raw);
            }
            "startCapture" => self.start_capture(),
            "pauseCapture" => self.pause_capture(),
            "set" => {
                let chan = self
                    .channel_by_id(&json_string_prop(n, "channel")?)
                    .ok_or_else(|| Error::new("Channel not found"))?;
                let source = make_source(n)?;
                self.set_output(chan, source);
            }
            "setGain" => {
                let channel_id = json_string_prop(n, "channel")?;
                self.channel_by_id(&channel_id)
                    .ok_or_else(|| Error::new("Channel not found"))?;
                let (c, s) = self.find_stream(&channel_id, &json_string_prop(n, "stream")?)?;
                let gain = json_float_prop_def(n, "gain", 1.0);
                self.set_gain(c, s, gain);
            }
            "setCurrentLimit" => {
                let limit = json_float_prop(n, "currentLimit")? as u32;
                self.set_current_limit(limit);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}

// ---- Test backend construction/configuration ----

const TEST_MIN_SAMPLE_TIME: f64 = 1e-5;

/// Synthetic M1K-shaped streaming device for tests (tests/support.hpp).
pub fn make_test_device(serial: &str) -> StreamingDevice {
    let mut dev = StreamingDevice {
        model: "com.nonolithlabs.test".into(),
        hw_version: "T1".into(),
        fw_version: "1.0".into(),
        serial: serial.into(),
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
        sample_time: 1e-4,
        min_sample_time: TEST_MIN_SAMPLE_TIME,
        capture_i: 0,
        capture_o: 0,
        current_limit: 200,
        backend: Backend::Test(TestBackend::default()),
    };
    configure_test(&mut dev, 0, 1e-4, 1000, false, false);
    dev
}

fn configure_test(dev: &mut StreamingDevice, mode: i32, sample_time: f64, samples: u32, continuous: bool, raw: bool) {
    let mut st = sample_time;
    if st < TEST_MIN_SAMPLE_TIME {
        st = TEST_MIN_SAMPLE_TIME;
    }
    dev.sample_time = st;
    dev.capture_samples = samples;
    dev.capture_continuous = continuous;
    dev.dev_mode = mode.max(0) as u32;
    dev.raw_mode = raw;
    dev.capture_length = samples as f32 * st as f32;
    dev.capture_i = 0;
    dev.capture_o = 0;

    dev.channels.clear();
    for (cid, cname) in [("a", "A"), ("b", "B")] {
        let mut c = Channel::new(cid, cname);
        c.source = Some(OutputSource::constant(0, 0.0));
        let suffix = cname;
        let mut v = Stream::new("v", &format!("Voltage {suffix}"), "V", 0.0, 5.0, 1, 5.0 / 65536.0, 1);
        let mut i = Stream::new("i", &format!("Current {suffix}"), "mA", -200.0, 200.0, 2, 0.4 / 65536.0 * 1000.0, 1);
        v.allocate(samples);
        i.allocate(samples);
        c.streams.push(v);
        c.streams.push(i);
        dev.channels.push(c);
    }
}

#[cfg(test)]
pub mod test_util {
    use super::*;

    /// Feed one sample per stream (v = value, i = value/10 by convention) and
    /// complete the packet.
    pub fn feed(dev: &mut StreamingDevice, values: &[f32]) {
        for &v in values {
            for c in 0..dev.channels.len() {
                dev.put(c, 0, v);
                dev.put(c, 1, v / 10.0);
            }
            dev.sample_done();
        }
        dev.packet_done();
        for c in 0..dev.channels.len() {
            dev.check_output_effective(c);
        }
    }

    /// Feed n samples of a ramp: sample k has v = k*10.
    pub fn feed_ramp(dev: &mut StreamingDevice, n: u64) {
        let start = dev.capture_i;
        let vals: Vec<f32> = (0..n).map(|k| ((start + k) * 10) as f32).collect();
        feed(dev, &vals);
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::*;
    use super::*;
    use crate::device::OutMsg;

    fn drain(rx: &mut tokio::sync::mpsc::UnboundedReceiver<OutMsg>) -> Vec<Value> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let OutMsg::Json(v) = m {
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn buffer_get_put_semantics() {
        let mut dev = make_test_device("BUF1");
        dev.configure(0, 1e-4, 5, false, false);

        // nothing collected yet
        assert!(dev.get(0, 0, 0).is_nan());

        feed(&mut dev, &[1.0, 2.0, 3.0]);
        assert_eq!(dev.get(0, 0, 0), 1.0);
        assert_eq!(dev.get(0, 0, 2), 3.0);
        assert!(dev.get(0, 0, 3).is_nan()); // not yet collected
        assert_eq!(dev.buffer_min(), 0);
        assert_eq!(dev.buffer_max(), 3);
    }

    #[test]
    fn buffer_wrap_continuous() {
        let mut dev = make_test_device("BUF2");
        dev.configure(0, 1e-4, 5, true, false);
        feed(&mut dev, &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
        // capture_i = 8, N = 5: samples 0..=3 overwritten (boundary NaN too)
        assert!(dev.get(0, 0, 0).is_nan());
        assert!(dev.get(0, 0, 3).is_nan());
        assert_eq!(dev.get(0, 0, 4), 4.0);
        assert_eq!(dev.get(0, 0, 7), 7.0);
        assert!(dev.get(0, 0, 8).is_nan());
        assert_eq!(dev.buffer_min(), 3);
        assert_eq!(dev.buffer_max(), 8);
    }

    #[test]
    fn non_continuous_overflow_dropped_but_counted() {
        let mut dev = make_test_device("BUF3");
        dev.configure(0, 1e-4, 5, false, false);
        feed(&mut dev, &[0.0, 1.0, 2.0, 3.0, 4.0, 99.0, 98.0]);
        // capture_i counts all 7, but samples 5,6 were dropped
        assert_eq!(dev.capture_i, 7);
        assert_eq!(dev.get(0, 0, 4), 4.0);
        // Q7 fixed in listener paths by C++ semantics: get() wraps and reads
        // stale data in the overshoot window (kept bug-for-bug: index 5 wraps
        // to slot 0)
        assert_eq!(dev.get(0, 0, 5), 0.0);
        assert!(dev.get(0, 0, 7).is_nan());
        // capture completed
        assert!(dev.capture_done);
        assert!(!dev.capture_state);
    }

    #[test]
    fn resample_mean_and_nan() {
        let mut dev = make_test_device("RS1");
        dev.configure(0, 1e-4, 100, false, false);
        feed(&mut dev, &[1.0, 2.0, 3.0, 4.0]);
        assert_eq!(dev.resample(0, 0, 0, 2), 1.5);
        assert_eq!(dev.resample(0, 0, 2, 2), 3.5);
        assert!(dev.resample(0, 0, 3, 2).is_nan()); // extends past capture_i
    }

    #[test]
    fn capture_state_machine_and_broadcasts() {
        let mut dev = make_test_device("SM1");
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        let msgs = drain(&mut rx);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["_action"], "deviceConfig");
        assert_eq!(msgs[0]["device"]["id"], "com.nonolithlabs.test~SM1");
        assert_eq!(msgs[0]["device"]["channels"]["a"]["streams"]["v"]["units"], "V");

        dev.configure(0, 1e-4, 3, false, false);
        dev.start_capture();
        let msgs = drain(&mut rx);
        // deviceConfig + captureState{true,false}
        assert_eq!(msgs[0]["_action"], "deviceConfig");
        assert_eq!(msgs[1]["_action"], "captureState");
        assert_eq!(msgs[1]["state"], true);
        assert_eq!(msgs[1]["done"], false);

        // start while running is a no-op
        dev.start_capture();
        assert!(drain(&mut rx).is_empty());

        feed(&mut dev, &[1.0, 2.0, 3.0]);
        let msgs = drain(&mut rx);
        // (feed also fires effective-handshake outputChanged broadcasts after
        // packetDone, matching the C++ handleInTransfer ordering)
        let cs: Vec<_> = msgs.iter().filter(|m| m["_action"] == "captureState").collect();
        assert_eq!(cs.len(), 1);
        assert_eq!(cs[0]["state"], false);
        assert_eq!(cs[0]["done"], true);

        // restarting after done resets counters and broadcasts captureReset
        dev.start_capture();
        let msgs = drain(&mut rx);
        assert_eq!(msgs[0]["_action"], "captureReset");
        assert_eq!(msgs[1]["_action"], "captureState");
        assert_eq!(dev.capture_i, 0);
        assert!(!dev.capture_done);
    }

    #[test]
    fn output_effective_handshake() {
        let mut dev = make_test_device("FX1");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        let src = crate::source::make_source(
            &json!({"source":"constant","mode":1,"value":2.5,"hint":"h"}),
        )
        .unwrap();
        dev.set_output(0, src);
        let msgs = drain(&mut rx);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["_action"], "outputChanged");
        assert_eq!(msgs[0]["channel"], "a");
        assert_eq!(msgs[0]["effective"], false);
        assert_eq!(msgs[0]["hint"], "h");

        feed(&mut dev, &[1.0, 1.0]);
        let msgs = drain(&mut rx);
        // channel b's initial constant source also becomes effective; look at a
        let oc: Vec<_> = msgs
            .iter()
            .filter(|m| m["_action"] == "outputChanged" && m["channel"] == "a")
            .collect();
        assert_eq!(oc.len(), 1);
        assert_eq!(oc[0]["effective"], true);
    }

    #[test]
    fn set_gain_notifies() {
        let mut dev = make_test_device("G1");
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "setGain",
            &json!({"_cmd":"setGain","channel":"a","stream":"v","gain":2.0}),
        )
        .unwrap();
        let msgs = drain(&mut rx);
        assert_eq!(msgs[0]["_action"], "gainChanged");
        assert_eq!(msgs[0]["channel"], "a");
        assert_eq!(msgs[0]["stream"], "v");
        assert_eq!(msgs[0]["gain"], 2.0);

        // unknown channel errors
        let e = dev
            .process_message(&client, "setGain", &json!({"channel":"x","stream":"v"}))
            .unwrap_err();
        assert_eq!(e.0, "Channel not found");
        let e = dev
            .process_message(&client, "setGain", &json!({"channel":"a","stream":"x"}))
            .unwrap_err();
        assert_eq!(e.0, "Stream not found");
    }

    #[test]
    fn unknown_command_unhandled() {
        let mut dev = make_test_device("U1");
        let (client, _rx) = ClientHandle::pair();
        assert!(!dev.process_message(&client, "bogus", &json!({})).unwrap());
    }

    #[test]
    fn detach_removes_listeners() {
        let mut dev = make_test_device("D1");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, _rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "count": 0, "streams": [{"channel":"a","stream":"v"}]}),
        )
        .unwrap();
        assert_eq!(dev.listeners.len(), 1);
        dev.on_client_detach(client.id);
        assert!(dev.listeners.is_empty());
        assert!(dev.connections.is_empty());
    }

    #[test]
    fn configure_clears_listeners_and_data() {
        let mut dev = make_test_device("C1");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        feed(&mut dev, &[1.0, 2.0]);
        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "count": 0, "streams": [{"channel":"a","stream":"v"}]}),
        )
        .unwrap();
        assert_eq!(dev.listeners.len(), 1);
        drain(&mut rx);

        dev.configure(0, 1e-4, 50, true, false);
        assert!(dev.listeners.is_empty());
        assert_eq!(dev.capture_i, 0);
        let msgs = drain(&mut rx);
        assert_eq!(msgs.last().unwrap()["_action"], "deviceConfig");
        assert_eq!(msgs.last().unwrap()["device"]["samples"], 50);
    }
}
