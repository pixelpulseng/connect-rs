// Stream listeners: decimated/triggered sample delivery to one client.
// Ported from streaming_device/stream_listener.cpp and the RESTListener in
// streaming_device/rest_api.cpp.

use crate::device::ClientHandle;
use crate::jsonutil::*;
use crate::streaming::StreamingDevice;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerType {
    None,
    InStream,
    OutSource,
}

pub enum Sink {
    Ws { client: ClientHandle, binary: bool },
    Rest { tx: mpsc::UnboundedSender<String> },
}

pub struct Listener {
    pub id: i64,
    pub streams: Vec<(usize, usize)>,

    pub decimate_factor: u64,
    pub index: u64,
    pub out_index: u64,
    pub count: i64,

    pub sink: Sink,

    pub trigger_type: TriggerType,
    pub triggered: bool,
    pub trigger_repeat: bool,
    pub trigger_channel: usize,
    pub trigger_level: f32,
    pub trigger_stream: (usize, usize),
    pub trigger_holdoff: i64,
    pub trigger_offset: i64,
    pub trigger_force: u64,
    pub trigger_force_index: u64,
    pub trigger_subsample_error: f64,
}

impl Listener {
    pub fn is_from_client(&self, client_id: u64) -> bool {
        match &self.sink {
            Sink::Ws { client, .. } => client.id == client_id,
            Sink::Rest { .. } => false,
        }
    }

    pub fn reset(&mut self) {
        self.index = 0;
        self.out_index = 0;
    }

    fn how_many_samples(&mut self, dev: &StreamingDevice) -> u64 {
        if self.trigger_type != TriggerType::None && !self.triggered && !self.find_trigger(dev) {
            // Waiting for a trigger and haven't found it yet
            return 0;
        }

        if self.index + self.decimate_factor >= dev.capture_i {
            // The data for our next output sample hasn't been collected yet
            // (note >=: the boundary chunk is withheld, matching the C++)
            return 0;
        }

        // Number of decimateFactor-sized chunks available
        let mut nchunks = (dev.capture_i - self.index) / self.decimate_factor;

        // Clamp to the remaining number of output samples
        if self.count > 0 {
            let remaining = self.count - self.out_index as i64;
            if remaining >= 0 && (remaining as u64) < nchunks {
                nchunks = remaining as u64;
            }
        }

        nchunks
    }

    /// Returns true if the trigger was found (sets `triggered` and adjusts
    /// `index`).
    fn find_trigger(&mut self, dev: &StreamingDevice) -> bool {
        self.trigger_subsample_error = 0.0;
        match self.trigger_type {
            TriggerType::InStream => {
                let (c, s) = self.trigger_stream;
                // NaN > level is false, matching the C++ float comparison
                let mut state = dev.get(c, s, self.index) > self.trigger_level;
                loop {
                    self.index += 1;
                    if self.index >= dev.capture_i {
                        break;
                    }
                    let new_state = dev.get(c, s, self.index) > self.trigger_level;
                    if new_state && !state {
                        self.index = self.index.wrapping_add_signed(self.trigger_offset);
                        self.triggered = true;
                        return true;
                    }
                    state = new_state;
                }

                if self.trigger_force != 0 && self.index > self.trigger_force_index {
                    self.triggered = true;
                    return true;
                }
            }
            TriggerType::OutSource => {
                let Some(source) = &dev.channels[self.trigger_channel].source else {
                    return false;
                };
                let zero = source.phase_zero_after(self.index);

                if zero.is_finite() && dev.capture_o >= zero.round() as u64 {
                    self.index = (zero.round() as u64).wrapping_add_signed(self.trigger_offset);
                    self.trigger_subsample_error = zero - zero.round();
                    self.triggered = true;
                    return true;
                } else if self.trigger_force != 0 && dev.capture_i >= self.trigger_force_index {
                    self.index = self.trigger_force_index;
                    self.triggered = true;
                    return true;
                }
            }
            TriggerType::None => {}
        }
        false
    }

    /// Deliver any available data. Returns false when the listener should be
    /// removed.
    pub fn handle_new_data(&mut self, dev: &StreamingDevice) -> bool {
        loop {
            if let Sink::Rest { tx } = &self.sink {
                if tx.is_closed() {
                    return false;
                }
            }

            let nchunks = self.how_many_samples(dev);
            if nchunks == 0 {
                return true;
            }

            let will_be_done = self.count > 0 && (self.out_index + nchunks) as i64 >= self.count;

            match &self.sink {
                Sink::Ws { client, binary: true } => {
                    let buf = self.binary_frame(dev, nchunks, will_be_done);
                    client.send_binary(buf);
                }
                Sink::Ws { client, binary: false } => {
                    let msg = self.json_update(dev, nchunks, will_be_done);
                    client.send_json(msg);
                }
                Sink::Rest { tx } => {
                    let mut o = String::new();
                    for chunk in 0..nchunks {
                        let mut first = true;
                        for &(c, s) in &self.streams {
                            if !first {
                                o.push_str(", ");
                            } else {
                                first = false;
                            }
                            o.push_str(&fmt_g(dev.resample(
                                c,
                                s,
                                self.index + chunk * self.decimate_factor,
                                self.decimate_factor,
                            )));
                        }
                        o.push('\n');
                    }
                    if tx.send(o).is_err() {
                        return false;
                    }
                }
            }

            self.index += nchunks * self.decimate_factor;
            self.out_index += nchunks;

            if let Sink::Rest { .. } = self.sink {
                return !(self.count > 0 && self.out_index as i64 >= self.count);
            }

            if will_be_done && self.trigger_repeat {
                // Trigger sweep end: re-arm and immediately retry in case
                // another packet's worth of data is already buffered.
                self.out_index = 0;
                self.triggered = false;
                self.index = self.index.wrapping_add_signed(self.trigger_holdoff);
                self.trigger_force_index = self.index + self.trigger_force;
                continue;
            }

            return !will_be_done;
        }
    }

    // Binary update frame (all values little-endian):
    //   u8   type = 1 (stream update)
    //   u8   flags: bit0 done, bit1 triggerForced, bit2 subsample valid
    //   u16  stream count
    //   u32  listener id
    //   u32  idx (output sample index of first sample in this frame)
    //   u32  sampleIndex (device sample index, valid when idx == 0)
    //   f32  subsample (trigger subsample error, valid when flags bit2 set)
    //   u32  nchunks (samples per stream)
    //   f32  data[stream count][nchunks]
    fn binary_frame(&self, dev: &StreamingDevice, nchunks: u64, will_be_done: bool) -> Vec<u8> {
        const HEADER: usize = 24;
        let nstreams = self.streams.len();
        let mut buf = vec![0u8; HEADER + nstreams * nchunks as usize * 4];

        let mut flags = 0u8;
        if will_be_done && !self.trigger_repeat {
            flags |= 1;
        }
        if self.out_index == 0 && self.trigger_force != 0 && self.index > self.trigger_force_index {
            flags |= 2;
        }
        if self.out_index == 0 && self.triggered {
            flags |= 4;
        }

        buf[0] = 1;
        buf[1] = flags;
        buf[2..4].copy_from_slice(&(nstreams as u16).to_le_bytes());
        buf[4..8].copy_from_slice(&(self.id as u32).to_le_bytes());
        buf[8..12].copy_from_slice(&(self.out_index as u32).to_le_bytes());
        buf[12..16].copy_from_slice(&(self.index as u32).to_le_bytes());
        buf[16..20].copy_from_slice(&(self.trigger_subsample_error as f32).to_le_bytes());
        buf[20..24].copy_from_slice(&(nchunks as u32).to_le_bytes());

        let mut off = HEADER;
        for &(c, s) in &self.streams {
            for chunk in 0..nchunks {
                let v = dev.resample(c, s, self.index + chunk * self.decimate_factor, self.decimate_factor);
                buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
                off += 4;
            }
        }
        buf
    }

    fn json_update(&self, dev: &StreamingDevice, nchunks: u64, will_be_done: bool) -> Value {
        let mut n = Map::new();
        n.insert("id".into(), json!(self.id));
        n.insert("idx".into(), json!(self.out_index));

        if self.out_index == 0 {
            if self.trigger_force != 0 && self.index > self.trigger_force_index {
                n.insert("triggerForced".into(), json!(true));
            }
            if self.triggered {
                n.insert("subsample".into(), json!(self.trigger_subsample_error));
            }
            n.insert("sampleIndex".into(), json!(self.index));
        }

        let mut data = Vec::new();
        for &(c, s) in &self.streams {
            let a: Vec<Value> = (0..nchunks)
                .map(|chunk| {
                    f32_json(dev.resample(
                        c,
                        s,
                        self.index + chunk * self.decimate_factor,
                        self.decimate_factor,
                    ))
                })
                .collect();
            data.push(Value::Array(a));
        }
        n.insert("data".into(), Value::Array(data));

        if will_be_done && !self.trigger_repeat {
            n.insert("done".into(), json!(true));
        }

        n.insert("_action".into(), json!("update"));
        Value::Object(n)
    }
}

/// Build a listener from a WS `listen` command (makeStreamListener).
pub fn make_stream_listener(dev: &StreamingDevice, client: &ClientHandle, n: &Value) -> Result<Listener> {
    let mut l = new_listener(Sink::Ws {
        client: client.clone(),
        binary: json_bool_prop_def(n, "binary", false),
    });

    l.id = json_int_prop(n, "id")?;

    let df = json_int_prop_def(n, "decimateFactor", 1);
    l.decimate_factor = if df < 1 { 1 } else { df as u64 };

    let start = json_int_prop_def(n, "start", -1);
    l.index = resolve_start(dev, start);

    l.count = json_int_prop(n, "count")?;

    let streams = n
        .get("streams")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::new("JSON missing property: streams"))?;
    for i in streams {
        l.streams
            .push(dev.find_stream(&json_string_prop(i, "channel")?, &json_string_prop(i, "stream")?)?);
    }

    if let Some(trigger) = n.get("trigger").filter(|t| t.is_object()) {
        let ttype = json_string_prop_def(trigger, "type", "in");
        match ttype.as_str() {
            "in" => {
                l.trigger_type = TriggerType::InStream;
                l.trigger_level = json_float_prop(trigger, "level")? as f32;
                l.trigger_stream = dev.find_stream(
                    &json_string_prop(trigger, "channel")?,
                    &json_string_prop(trigger, "stream")?,
                )?;
            }
            "out" => {
                l.trigger_type = TriggerType::OutSource;
                l.trigger_channel = dev
                    .channel_by_id(&json_string_prop(trigger, "channel")?)
                    .ok_or_else(|| Error::new("Trigger channel not found"))?;
            }
            _ => return Err(Error::new("Invalid trigger type")),
        }

        l.trigger_repeat = json_bool_prop_def(trigger, "repeat", true);
        l.trigger_holdoff = json_int_prop_def(trigger, "holdoff", 0);
        l.trigger_offset = json_int_prop_def(trigger, "offset", 0);

        if l.trigger_offset < 0 && -l.trigger_offset >= l.trigger_holdoff {
            // Prevent big negative offsets that could cause infinite loops
            l.trigger_holdoff = -l.trigger_offset;
        }
        let force = json_int_prop_def(trigger, "force", 0);
        l.trigger_force = if force < 0 { 0 } else { force as u64 };
        l.trigger_force_index = l.index + l.trigger_force;
    }

    Ok(l)
}

/// Build a REST CSV listener (RESTListener in rest_api.cpp).
pub fn make_rest_listener(
    dev: &StreamingDevice,
    channel: usize,
    tx: mpsc::UnboundedSender<String>,
    decimate_factor: u64,
    start: i64,
    count: i64,
) -> Listener {
    let mut l = new_listener(Sink::Rest { tx });
    l.streams = (0..dev.channels[channel].streams.len()).map(|s| (channel, s)).collect();
    l.decimate_factor = decimate_factor.max(1);
    l.index = resolve_start(dev, start);
    l.count = count;
    l
}

fn resolve_start(dev: &StreamingDevice, start: i64) -> u64 {
    let start = if start < 0 {
        // Negative indexes are relative to the latest sample
        dev.buffer_max() as i64 + start + 1
    } else {
        start
    };
    if start < 0 {
        0
    } else {
        start as u64
    }
}

fn new_listener(sink: Sink) -> Listener {
    Listener {
        id: 0,
        streams: Vec::new(),
        decimate_factor: 1,
        index: 0,
        out_index: 0,
        count: 0,
        sink,
        trigger_type: TriggerType::None,
        triggered: false,
        trigger_repeat: false,
        trigger_channel: 0,
        trigger_level: 0.0,
        trigger_stream: (0, 0),
        trigger_holdoff: 0,
        trigger_offset: 0,
        trigger_force: 0,
        trigger_force_index: 0,
        trigger_subsample_error: 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::OutMsg;
    use crate::streaming::test_util::*;
    use crate::streaming::make_test_device;

    fn drain(rx: &mut mpsc::UnboundedReceiver<OutMsg>) -> (Vec<Value>, Vec<Vec<u8>>) {
        let mut json = Vec::new();
        let mut bin = Vec::new();
        while let Ok(m) = rx.try_recv() {
            match m {
                OutMsg::Json(v) => json.push(v),
                OutMsg::Binary(b) => bin.push(b),
            }
        }
        (json, bin)
    }

    fn updates(msgs: &[Value]) -> Vec<&Value> {
        msgs.iter().filter(|m| m["_action"] == "update").collect()
    }

    #[test]
    fn listen_streams_decimated_means() {
        let mut dev = make_test_device("L1");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "decimateFactor": 2, "start": 0, "count": 0,
                    "streams": [{"channel":"a","stream":"v"}]}),
        )
        .unwrap();

        feed(&mut dev, &[1.0, 3.0, 5.0, 7.0, 9.0]);
        let (msgs, _) = drain(&mut rx);
        let ups = updates(&msgs);
        assert_eq!(ups.len(), 1);
        assert_eq!(ups[0]["id"], 1);
        assert_eq!(ups[0]["idx"], 0);
        assert_eq!(ups[0]["sampleIndex"], 0);
        // capture_i=5, decimate=2: gate withholds the boundary chunk ->
        // (5-0)/2 = 2 chunks: mean(1,3)=2, mean(5,7)=6
        assert_eq!(ups[0]["data"], json!([[2.0, 6.0]]));
    }

    #[test]
    fn count_limited_listener_sends_done_and_is_removed() {
        let mut dev = make_test_device("L2");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 7, "decimateFactor": 1, "start": 0, "count": 3,
                    "streams": [{"channel":"a","stream":"v"}]}),
        )
        .unwrap();
        assert_eq!(dev.listeners.len(), 1);

        feed(&mut dev, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let (msgs, _) = drain(&mut rx);
        let ups = updates(&msgs);
        assert_eq!(ups.len(), 1);
        assert_eq!(ups[0]["data"], json!([[1.0, 2.0, 3.0]]));
        assert_eq!(ups[0]["done"], true);
        assert!(dev.listeners.is_empty());
    }

    #[test]
    fn negative_start_is_relative_to_latest() {
        let mut dev = make_test_device("L3");
        dev.configure(0, 1e-4, 100, true, false);
        feed(&mut dev, &[10.0, 20.0, 30.0, 40.0]);

        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        // start: -1 -> index = buffer_max() -1 + 1 = 4 (live from latest)
        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "start": -1, "count": 0,
                    "streams": [{"channel":"a","stream":"v"}]}),
        )
        .unwrap();
        assert_eq!(dev.listeners[0].index, 4);

        feed(&mut dev, &[50.0, 60.0]);
        let (msgs, _) = drain(&mut rx);
        let ups = updates(&msgs);
        // capture_i=6, index=4 -> gate: 4+1 >= 6 false; (6-4)/1 = 2... but
        // gate is index+decimate >= capture_i: 5 >= 6 false -> 2 chunks?
        // No: nchunks = (6-4)/1 = 2, but the last chunk [5] would end at
        // capture_i; C++ delivers it (gate only checks the first chunk).
        assert_eq!(ups.len(), 1);
        assert_eq!(ups[0]["sampleIndex"], 4);
        assert_eq!(ups[0]["data"], json!([[50.0, 60.0]]));
    }

    #[test]
    fn replacing_listener_with_same_id() {
        let mut dev = make_test_device("L4");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        for _ in 0..2 {
            dev.process_message(
                &client,
                "listen",
                &json!({"id": 1, "start": 0, "count": 0,
                        "streams": [{"channel":"a","stream":"v"}]}),
            )
            .unwrap();
        }
        assert_eq!(dev.listeners.len(), 1);

        dev.process_message(&client, "cancelListen", &json!({"id": 1})).unwrap();
        assert!(dev.listeners.is_empty());
    }

    #[test]
    fn binary_frame_layout_byte_exact() {
        let mut dev = make_test_device("B1");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 5, "decimateFactor": 2, "start": 0, "count": 0, "binary": true,
                    "streams": [{"channel":"a","stream":"v"}, {"channel":"a","stream":"i"}]}),
        )
        .unwrap();

        feed(&mut dev, &[1.0, 3.0, 5.0, 7.0, 9.0]);
        let (_, bins) = drain(&mut rx);
        assert_eq!(bins.len(), 1);
        let b = &bins[0];
        // 2 streams x 2 chunks
        assert_eq!(b.len(), 24 + 2 * 2 * 4);
        assert_eq!(b[0], 1); // frame type
        assert_eq!(b[1], 0); // flags
        assert_eq!(u16::from_le_bytes([b[2], b[3]]), 2); // nstreams
        assert_eq!(u32::from_le_bytes([b[4], b[5], b[6], b[7]]), 5); // id
        assert_eq!(u32::from_le_bytes([b[8], b[9], b[10], b[11]]), 0); // idx
        assert_eq!(u32::from_le_bytes([b[12], b[13], b[14], b[15]]), 0); // sampleIndex
        assert_eq!(f32::from_le_bytes([b[16], b[17], b[18], b[19]]), 0.0); // subsample
        assert_eq!(u32::from_le_bytes([b[20], b[21], b[22], b[23]]), 2); // nchunks
        let f = |o: usize| f32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
        // stream-major: v chunks then i chunks (i = v/10 in the test feeder)
        assert_eq!(f(24), 2.0);
        assert_eq!(f(28), 6.0);
        assert_eq!(f(32), 0.2);
        assert_eq!(f(36), 0.6);
    }

    #[test]
    fn binary_done_flag() {
        let mut dev = make_test_device("B2");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "start": 0, "count": 2, "binary": true,
                    "streams": [{"channel":"a","stream":"v"}]}),
        )
        .unwrap();
        feed(&mut dev, &[1.0, 2.0, 3.0]);
        let (_, bins) = drain(&mut rx);
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0][1] & 1, 1); // done flag
        assert!(dev.listeners.is_empty());
    }

    #[test]
    fn in_trigger_rising_edge() {
        let mut dev = make_test_device("T1");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "start": 0, "count": 3,
                    "streams": [{"channel":"a","stream":"v"}],
                    "trigger": {"type": "in", "channel": "a", "stream": "v",
                                "level": 2.5, "repeat": false}}),
        )
        .unwrap();

        // rises through 2.5 between sample 2 (2.0) and sample 3 (3.0)
        feed(&mut dev, &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
        let (msgs, _) = drain(&mut rx);
        let ups = updates(&msgs);
        assert_eq!(ups.len(), 1);
        assert_eq!(ups[0]["sampleIndex"], 3);
        assert_eq!(ups[0]["subsample"], 0.0);
        assert_eq!(ups[0]["data"], json!([[3.0, 4.0, 5.0]]));
        assert_eq!(ups[0]["done"], true);
    }

    #[test]
    fn out_trigger_phase_zero_with_subsample() {
        let mut dev = make_test_device("T2");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        // 30.5-sample period sine with phase 10: first phase zero after
        // sample 0 is at 30.5 - 10 = 20.5
        let src = crate::source::make_source(&json!({
            "source": "sine", "mode": 1, "offset": 2.5, "amplitude": 1.0,
            "period": 30.5, "phase": 10.0, "relPhase": false,
        }))
        .unwrap();
        dev.set_output(0, src);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "start": 0, "count": 2,
                    "streams": [{"channel":"a","stream":"v"}],
                    "trigger": {"type": "out", "channel": "a", "repeat": false}}),
        )
        .unwrap();

        // No output yet: trigger waits for capture_o
        feed(&mut dev, &[0.0; 10]);
        let (msgs, _) = drain(&mut rx);
        assert!(updates(&msgs).is_empty());

        // Simulate output progress past the phase zero. round(20.5) = 21
        // (half away from zero, like C round()); subsample = 20.5 - 21 = -0.5
        dev.capture_o = 40;
        feed(&mut dev, &[1.0; 40]);
        let (msgs, _) = drain(&mut rx);
        let ups = updates(&msgs);
        assert_eq!(ups.len(), 1);
        assert_eq!(ups[0]["sampleIndex"], 21);
        assert_eq!(ups[0]["subsample"], -0.5);
    }

    #[test]
    fn forced_trigger_reports_trigger_forced() {
        let mut dev = make_test_device("T3");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "start": 0, "count": 2,
                    "streams": [{"channel":"a","stream":"v"}],
                    "trigger": {"type": "in", "channel": "a", "stream": "v",
                                "level": 100.0, "repeat": false, "force": 4}}),
        )
        .unwrap();

        // never crosses level 100 -> forced once the scan passes force index.
        // The forced trigger leaves index at the scan position (end of data),
        // so the update arrives with the next packet.
        feed(&mut dev, &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]);
        let (msgs, _) = drain(&mut rx);
        assert!(updates(&msgs).is_empty());
        feed(&mut dev, &[1.0, 1.0, 1.0, 1.0]);
        let (msgs, _) = drain(&mut rx);
        let ups = updates(&msgs);
        assert_eq!(ups.len(), 1);
        assert_eq!(ups[0]["triggerForced"], true);
        assert_eq!(ups[0]["sampleIndex"], 8);
    }

    #[test]
    fn repeating_trigger_rearms_with_holdoff() {
        let mut dev = make_test_device("T4");
        dev.configure(0, 1e-4, 1000, true, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "start": 0, "count": 2,
                    "streams": [{"channel":"a","stream":"v"}],
                    "trigger": {"type": "in", "channel": "a", "stream": "v",
                                "level": 2.5, "repeat": true, "holdoff": 0}}),
        )
        .unwrap();

        // Two rising edges through 2.5 with enough data after each.
        // (The scan starts at index 1 — the C++ pre-increments before any
        // data exists — so the edges must land at sample >= 2.)
        feed(&mut dev, &[0.0, 0.0, 5.0, 5.0, 0.0, 0.0, 5.0, 5.0, 0.0, 0.0]);
        let (msgs, _) = drain(&mut rx);
        let ups = updates(&msgs);
        assert_eq!(ups.len(), 2, "{ups:?}");
        assert_eq!(ups[0]["idx"], 0);
        assert_eq!(ups[0]["sampleIndex"], 2);
        assert_eq!(ups[1]["idx"], 0);
        assert_eq!(ups[1]["sampleIndex"], 6);
        // listener still armed (repeat)
        assert_eq!(dev.listeners.len(), 1);
    }

    #[test]
    fn trigger_errors() {
        let mut dev = make_test_device("T5");
        dev.configure(0, 1e-4, 100, true, false);
        let (client, _rx) = ClientHandle::pair();

        let e = dev
            .process_message(
                &client,
                "listen",
                &json!({"id":1,"count":0,"streams":[{"channel":"a","stream":"v"}],
                        "trigger":{"type":"bogus"}}),
            )
            .unwrap_err();
        assert_eq!(e.0, "Invalid trigger type");

        let e = dev
            .process_message(
                &client,
                "listen",
                &json!({"id":1,"count":0,"streams":[{"channel":"a","stream":"v"}],
                        "trigger":{"type":"in","channel":"a","stream":"v"}}),
            )
            .unwrap_err();
        assert_eq!(e.0, "JSON missing float property: level");

        let e = dev
            .process_message(
                &client,
                "listen",
                &json!({"id":1,"count":0,"streams":[{"channel":"a","stream":"v"}],
                        "trigger":{"type":"out","channel":"x"}}),
            )
            .unwrap_err();
        assert_eq!(e.0, "Trigger channel not found");
    }

    #[test]
    fn capture_reset_rewinds_listeners() {
        let mut dev = make_test_device("R1");
        dev.configure(0, 1e-4, 10, false, false);
        let (client, mut rx) = ClientHandle::pair();
        dev.on_client_attach(&client);
        drain(&mut rx);

        dev.process_message(
            &client,
            "listen",
            &json!({"id": 1, "start": 0, "count": 0,
                    "streams": [{"channel":"a","stream":"v"}]}),
        )
        .unwrap();

        dev.start_capture();
        feed(&mut dev, &[1.0; 10]); // completes non-continuous capture
        assert!(dev.capture_done);
        assert!(dev.listeners[0].index > 0);

        dev.start_capture(); // triggers reset
        assert_eq!(dev.listeners[0].index, 0);
        assert_eq!(dev.listeners[0].out_index, 0);
    }
}
