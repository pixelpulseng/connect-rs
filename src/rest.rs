// REST API, ported from rest_service.cpp + streaming_device/rest_api.cpp.
// Handlers are plain functions over RestRequest/RestResponse so the whole
// routing stack is testable without sockets (like the C++ FakeSession tests).

use crate::device::{AnyDevice, ServerState};
use crate::jsonutil::*;
use crate::listener::make_rest_listener;
use crate::source::{make_source, OutputSource};
use crate::streaming::StreamingDevice;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct RestRequest {
    pub method: String,
    /// Path parts from splitting the target on '/'; parts[0] is "" for
    /// absolute paths, mirroring the C++ Url. One trailing slash is removed.
    pub parts: Vec<String>,
    pub params: HashMap<String, String>,
    pub body: String,
}

impl RestRequest {
    /// Parse a request target the way url.cpp does: split path on '/',
    /// drop one trailing empty segment, parse the query as key=value pairs
    /// split on '&' with NO url-decoding.
    pub fn new(method: &str, target: &str, body: &str) -> RestRequest {
        let (path, query) = match target.split_once('?') {
            Some((p, q)) => (p, q),
            None => (target, ""),
        };
        let mut parts: Vec<String> = path.split('/').map(|s| s.to_string()).collect();
        if parts.last().map(|s| s.is_empty()).unwrap_or(false) {
            parts.pop();
        }
        RestRequest {
            method: method.to_string(),
            parts,
            params: parse_query(query),
            body: body.to_string(),
        }
    }

    pub fn param(&self, key: &str, def: &str) -> String {
        self.params.get(key).cloned().unwrap_or_else(|| def.to_string())
    }

    fn part(&self, level: usize) -> &str {
        self.parts.get(level).map(|s| s.as_str()).unwrap_or("")
    }

    fn leaf(&self, level: usize) -> bool {
        self.parts.len() <= level
    }
}

pub fn parse_query(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}

pub enum RestBody {
    Full(String),
    /// Chunked text/plain stream (CSV); ends when the sender side is dropped.
    Stream(mpsc::UnboundedReceiver<String>),
}

pub struct RestResponse {
    pub status: u16,
    pub body: RestBody,
}

impl RestResponse {
    pub fn full(status: u16, body: impl Into<String>) -> RestResponse {
        RestResponse {
            status,
            body: RestBody::Full(body.into()),
        }
    }

    pub fn json(n: &Value) -> RestResponse {
        RestResponse::full(200, serde_json::to_string_pretty(n).unwrap())
    }

    pub fn error(e: &Error) -> RestResponse {
        eprintln!("Exception while processing request: {e}");
        // Yes, 402 — matches the C++ respondError (SPEC.md Q4).
        RestResponse::full(402, serde_json::to_string_pretty(&json!({"error": e.0})).unwrap())
    }

    #[cfg(test)]
    pub fn body_string(&self) -> &str {
        match &self.body {
            RestBody::Full(s) => s,
            _ => panic!("streaming body"),
        }
    }

    #[cfg(test)]
    pub fn json_body(&self) -> Value {
        serde_json::from_str(self.body_string()).unwrap()
    }
}

/// handleJSONRequest: entered with parts[1] == "rest".
pub fn handle_json_request(state: &Arc<ServerState>, req: &RestRequest) -> RestResponse {
    if req.leaf(2) {
        return RestResponse::full(404, "No API version selected.");
    }
    if req.part(2) != "v1" {
        return RestResponse::full(404, "API version not supported");
    }

    if req.leaf(3) {
        return RestResponse::json(&json!({
            "server": "Nonolith Connect",
            "version": crate::SERVER_VERSION,
            "gitVersion": crate::server_git_version(),
        }));
    }

    if req.part(3) == "devices" {
        if req.leaf(4) {
            return RestResponse::json(&state.devices_json());
        }
        if let Some(dev) = state.device_by_id(req.part(4)) {
            let mut dev = dev.lock().unwrap();
            if let AnyDevice::Streaming(d) = &mut *dev {
                if let Some(r) = device_rest(d, req, 5) {
                    return r;
                }
            }
        }
    }

    RestResponse::full(404, "REST object not found")
}

/// StreamingDevice::handleREST + device-specific endpoints (level = first
/// path index below the device id).
fn device_rest(dev: &mut StreamingDevice, req: &RestRequest, level: usize) -> Option<RestResponse> {
    // Device-specific endpoints take precedence (m1k.cpp / cee.cpp)
    if !req.leaf(level) {
        match &dev.backend {
            crate::streaming::Backend::M1k(_) => {
                if let Some(r) = crate::usb::m1k::handle_rest(dev, req, level) {
                    return Some(r);
                }
            }
            crate::streaming::Backend::Cee(_) => {
                if let Some(r) = crate::usb::cee::handle_rest(dev, req, level) {
                    return Some(r);
                }
            }
            crate::streaming::Backend::Test(_) => {}
        }
    }

    if req.leaf(level) {
        if req.method == "POST" {
            return Some(rest_device_post(dev, &req.body));
        }
        return Some(RestResponse::json(&dev.state_to_json(false)));
    }

    match req.part(level) {
        "configuration" => {
            if req.method == "POST" {
                Some(rest_configuration_post(dev, &req.body))
            } else {
                Some(RestResponse::json(&dev.state_to_json(true)))
            }
        }
        other => {
            let chan = dev.channel_by_id(other)?;
            if req.leaf(level + 1) {
                return Some(RestResponse::json(&dev.channels[chan].to_json()));
            }
            match req.part(level + 1) {
                "output" => rest_output(dev, chan, req),
                "input" => rest_input(dev, chan, req),
                _ => None,
            }
        }
    }
}

fn rest_device_post(dev: &mut StreamingDevice, postdata: &str) -> RestResponse {
    if postdata.starts_with('{') {
        // JSON body: not implemented in the C++ either (Q11); still
        // responds with device state.
    } else {
        let map = parse_query(postdata);
        match map.get("capture").map(|s| s.as_str()) {
            Some("true") | Some("on") | Some("1") => dev.start_capture(),
            Some("false") | Some("off") | Some("0") => dev.pause_capture(),
            _ => {}
        }
    }
    RestResponse::json(&dev.state_to_json(false))
}

fn rest_configuration_post(dev: &mut StreamingDevice, postdata: &str) -> RestResponse {
    if !postdata.starts_with('{') {
        let map = parse_query(postdata);
        match rest_configure(dev, &map) {
            Ok(()) => {}
            Err(e) => return RestResponse::error(&e),
        }
    }
    RestResponse::json(&dev.state_to_json(true))
}

fn rest_configure(dev: &mut StreamingDevice, map: &HashMap<String, String>) -> Result<()> {
    let samples = map_get_num(map, "samples", dev.capture_samples as f64)? as u32;

    let mut sample_time = map_get_num(map, "sampleTime", dev.sample_time)?;
    if sample_time <= 0.0 {
        sample_time = dev.sample_time;
    }
    if sample_time > 0.001 {
        sample_time = 0.001;
    }

    let current = map_get_num(map, "currentLimit", 0.0)? as u32;
    dev.set_current_limit(current);

    dev.configure(
        dev.dev_mode as i32,
        sample_time,
        samples,
        dev.capture_continuous,
        dev.raw_mode,
    );
    Ok(())
}

pub fn map_get_num(map: &HashMap<String, String>, key: &str, def: f64) -> Result<f64> {
    match map.get(key) {
        Some(v) => v
            .parse::<f64>()
            .map_err(|_| Error(format!("Invalid number for parameter {key}"))),
        None => Ok(def),
    }
}

// ---- /output ----

fn rest_output(dev: &mut StreamingDevice, chan: usize, req: &RestRequest) -> Option<RestResponse> {
    if req.method == "POST" {
        Some(match rest_output_post(dev, chan, &req.body) {
            Ok(()) => RestResponse::json(&dev.channels[chan].source.as_ref().unwrap().describe_json()),
            Err(e) => RestResponse::error(&e),
        })
    } else {
        let src = dev.channels[chan].source.as_ref()?;
        Some(RestResponse::json(&src.describe_json()))
    }
}

fn rest_output_post(dev: &mut StreamingDevice, chan: usize, postdata: &str) -> Result<()> {
    let source = if postdata.starts_with('{') {
        let n: Value = serde_json::from_str(postdata).map_err(|e| Error(format!("JSON parse error: {e}")))?;
        make_source(&n)?
    } else {
        form_source(dev.sample_time, postdata)?
    };
    dev.set_output(chan, source);
    Ok(())
}

/// Form-encoded output source (rest_api.cpp handleRESTOutputCallback):
/// time-domain parameters are converted to samples here.
fn form_source(sample_time: f64, postdata: &str) -> Result<OutputSource> {
    let map = parse_query(postdata);
    let value = map_get_num(&map, "value", 0.0)? as f32;
    let mode = map.get("mode").cloned().unwrap_or_else(|| "0".into()).to_lowercase();
    let modeval: u32 = match mode.as_str() {
        "0" | "disabled" | "d" => 0,
        "1" | "svmi" | "v" => 1,
        "2" | "simv" | "i" => 2,
        _ => 0,
    };

    let source = map.get("wave").cloned().unwrap_or_else(|| "constant".into());
    let hint = map.get("hint").cloned().unwrap_or_default();

    let mut src = match source.as_str() {
        "constant" => OutputSource::constant(modeval, value),
        "adv_square" => {
            let value1 = map_get_num(&map, "value1", 0.0)? as f32;
            let value2 = map_get_num(&map, "value2", 0.0)? as f32;
            let mut time1 = (map_get_num(&map, "time1", 0.5)? / sample_time) as i64;
            if time1 <= 0 {
                time1 = 1;
            }
            let mut time2 = (map_get_num(&map, "time2", 0.5)? / sample_time) as i64;
            if time2 <= 0 {
                time2 = 1;
            }
            let phase = (map_get_num(&map, "phase", 0.0)? / sample_time) as i64;
            let rel_phase = map.get("relPhase").map(|s| s == "1").unwrap_or(true);
            OutputSource::adv_square(modeval, value1, value2, time1 as u32, time2 as u32, phase, rel_phase)?
        }
        "arb" => {
            let mut phase = (map_get_num(&map, "phase", -1.0)? / sample_time) as i64;
            if phase < 0 {
                phase = -1;
            }
            let repeat = map_get_num(&map, "repeat", 0.0)? as i64;
            let mut pointspec = map.get("points").cloned().unwrap_or_default();
            // The only URL-decoding the server does, anywhere:
            pointspec = pointspec.replace("%3A", ":").replace("%2C", ",");

            let mut values = Vec::new();
            for pair in pointspec.split(',') {
                let (ts, vs) = pair
                    .split_once(':')
                    .ok_or_else(|| Error::new("Invalid arbitrary wave point spec"))?;
                let t: f64 = ts
                    .parse()
                    .map_err(|_| Error::new("Invalid arbitrary wave point spec"))?;
                let v: f32 = vs
                    .parse()
                    .map_err(|_| Error::new("Invalid arbitrary wave point spec"))?;
                values.push(((t / sample_time).round() as i64, v));
            }
            OutputSource::arb(modeval, phase, values, repeat)?
        }
        wave @ ("sine" | "triangle" | "square") => {
            let amplitude = map_get_num(&map, "amplitude", 0.0)?;
            let mut freq = map_get_num(&map, "frequency", 1.0)?;
            if freq <= 0.0 {
                freq = 0.001;
            }
            let period = 1.0 / sample_time / freq;
            let phase = map_get_num(&map, "phase", 1.0)? / sample_time;
            let rel_phase = map.get("relPhase").map(|s| s == "1").unwrap_or(true);
            let w = match wave {
                "sine" => crate::source::Wave::Sine,
                "triangle" => crate::source::Wave::Triangle,
                _ => crate::source::Wave::Square,
            };
            OutputSource::periodic(modeval, w, value as f64, amplitude, period, phase, rel_phase)
        }
        _ => return Err(Error::new("Invalid source")),
    };
    src.hint = hint;
    Ok(src)
}

// ---- /input ----

fn rest_input(dev: &mut StreamingDevice, chan: usize, req: &RestRequest) -> Option<RestResponse> {
    if req.method == "POST" {
        return Some(rest_input_post(dev, chan, &req.body));
    }

    // Q5 fixed: bad query parameters return a JSON error instead of an
    // uncaught-exception 500.
    let parse = || -> Result<(u64, i64, i64, bool)> {
        let resample_s: f64 = req
            .param("resample", "0.01")
            .parse()
            .map_err(|_| Error::new("Invalid resample parameter"))?;
        let decimate = (resample_s / dev.sample_time).round() as i64;
        let start: i64 = req
            .param("start", "-1")
            .parse()
            .map_err(|_| Error::new("Invalid start parameter"))?;
        let count: i64 = req
            .param("count", "1")
            .parse()
            .map_err(|_| Error::new("Invalid count parameter"))?;
        if count < 0 {
            return Err(Error::new("Invalid count parameter"));
        }
        let header = req.param("header", "1") == "1";
        Ok((decimate.max(1) as u64, start, count, header))
    };
    let (decimate, start, count, header) = match parse() {
        Ok(v) => v,
        Err(e) => return Some(RestResponse::error(&e)),
    };

    let (tx, rx) = mpsc::unbounded_channel();

    if header {
        let mut o = String::new();
        let mut first = true;
        for s in &dev.channels[chan].streams {
            if !first {
                o.push(',');
            } else {
                first = false;
            }
            o.push_str(&format!("{} ({})", s.display_name, s.units));
        }
        o.push('\n');
        let _ = tx.send(o);
    }

    let l = make_rest_listener(dev, chan, tx, decimate, start, count);
    dev.add_listener(l);

    Some(RestResponse {
        status: 200,
        body: RestBody::Stream(rx),
    })
}

fn rest_input_post(dev: &mut StreamingDevice, chan: usize, postdata: &str) -> RestResponse {
    let map = parse_query(postdata);
    let nstreams = dev.channels[chan].streams.len();
    let mut g = serde_json::Map::new();
    for s in 0..nstreams {
        let sid = dev.channels[chan].streams[s].id.clone();
        let gain = match map_get_num(&map, &format!("gain_{sid}"), 0.0) {
            Ok(v) => v,
            Err(e) => return RestResponse::error(&e),
        };
        if gain != 0.0 {
            dev.set_gain(chan, s, gain);
        }
        g.insert(sid, json!(dev.channels[chan].streams[s].get_gain()));
    }
    RestResponse::json(&json!({ "gain": g }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{AnyDevice, ClientHandle};
    use crate::streaming::make_test_device;
    use crate::streaming::test_util::*;
    use std::sync::Mutex;

    fn state_with_device(serial: &str) -> Arc<ServerState> {
        let state = ServerState::new(false, false);
        let dev = Arc::new(Mutex::new(AnyDevice::Streaming(make_test_device(serial))));
        state.add_device(dev);
        state
    }

    fn get(state: &Arc<ServerState>, target: &str) -> RestResponse {
        handle_json_request(state, &RestRequest::new("GET", target, ""))
    }

    fn post(state: &Arc<ServerState>, target: &str, body: &str) -> RestResponse {
        handle_json_request(state, &RestRequest::new("POST", target, body))
    }

    #[test]
    fn routing_basics() {
        let state = state_with_device("R1");

        let r = get(&state, "/rest");
        assert_eq!(r.status, 404);
        assert_eq!(r.body_string(), "No API version selected.");

        let r = get(&state, "/rest/v9/devices");
        assert_eq!(r.status, 404);
        assert_eq!(r.body_string(), "API version not supported");

        let r = get(&state, "/rest/v1/");
        assert_eq!(r.status, 200);
        let j = r.json_body();
        assert_eq!(j["server"], "Nonolith Connect");
        assert_eq!(j["version"], "1.3");

        let r = get(&state, "/rest/v1/devices");
        let j = r.json_body();
        assert!(j.is_object());
        assert!(j["com.nonolithlabs.test~R1"]["serial"] == "R1");

        let r = get(&state, "/rest/v1/devices/unknown~X");
        assert_eq!(r.status, 404);
        assert_eq!(r.body_string(), "REST object not found");
    }

    #[test]
    fn device_id_wildcard_matches_model() {
        let state = state_with_device("W1");
        let r = get(&state, "/rest/v1/devices/com.nonolithlabs.test*");
        assert_eq!(r.status, 200);
        assert_eq!(r.json_body()["serial"], "W1");
    }

    #[test]
    fn device_state_and_channel() {
        let state = state_with_device("S1");
        let r = get(&state, "/rest/v1/devices/com.nonolithlabs.test~S1");
        let j = r.json_body();
        assert_eq!(j["id"], "com.nonolithlabs.test~S1");
        assert_eq!(j["captureState"], false);
        assert_eq!(j["channels"]["a"]["streams"]["i"]["units"], "mA");

        let r = get(&state, "/rest/v1/devices/com.nonolithlabs.test~S1/a");
        let j = r.json_body();
        assert_eq!(j["id"], "a");
        assert_eq!(j["output"]["source"], "constant");

        let r = get(&state, "/rest/v1/devices/com.nonolithlabs.test~S1/x");
        assert_eq!(r.status, 404);
    }

    #[test]
    fn capture_post() {
        let state = state_with_device("C1");
        let r = post(&state, "/rest/v1/devices/com.nonolithlabs.test~C1", "capture=on");
        assert_eq!(r.json_body()["captureState"], true);
        let r = post(&state, "/rest/v1/devices/com.nonolithlabs.test~C1", "capture=off");
        assert_eq!(r.json_body()["captureState"], false);
    }

    #[test]
    fn configuration_clamps_sample_time() {
        let state = state_with_device("CF1");
        let r = get(&state, "/rest/v1/devices/com.nonolithlabs.test~CF1/configuration");
        assert_eq!(r.status, 200);
        assert!(r.json_body().get("captureState").is_none());

        let r = post(
            &state,
            "/rest/v1/devices/com.nonolithlabs.test~CF1/configuration",
            "sampleTime=0.5&samples=100",
        );
        let j = r.json_body();
        assert_eq!(j["sampleTime"], 0.001); // clamped to 1 ms max
        assert_eq!(j["samples"], 100);

        // non-positive sampleTime ignored
        let r = post(
            &state,
            "/rest/v1/devices/com.nonolithlabs.test~CF1/configuration",
            "sampleTime=-1",
        );
        assert_eq!(r.json_body()["sampleTime"], 0.001);
    }

    #[test]
    fn output_form_post() {
        let state = state_with_device("O1");
        let base = "/rest/v1/devices/com.nonolithlabs.test~O1/a/output";

        let r = post(&state, base, "mode=svmi&value=2.5");
        let j = r.json_body();
        assert_eq!(j["mode"], 1);
        assert_eq!(j["source"], "constant");
        assert_eq!(j["value"], 2.5);
        assert_eq!(j["effective"], false);

        // sine at 10 Hz with 1e-4 sample time -> period 1000 samples
        let r = post(&state, base, "mode=v&wave=sine&value=2.5&amplitude=1&frequency=10");
        let j = r.json_body();
        assert_eq!(j["source"], "sine");
        assert_eq!(j["period"], 1000.0);

        // mode aliases are case-insensitive; unknown modes are 0
        let r = post(&state, base, "mode=SIMV&value=1");
        assert_eq!(r.json_body()["mode"], 2);
        let r = post(&state, base, "mode=zzz&value=1");
        assert_eq!(r.json_body()["mode"], 0);

        // JSON body works too
        let r = post(&state, base, r#"{"source":"constant","mode":1,"value":1.25}"#);
        assert_eq!(r.json_body()["value"], 1.25);

        // errors are 402 with an error body
        let r = post(&state, base, "wave=bogus");
        assert_eq!(r.status, 402);
        assert_eq!(r.json_body()["error"], "Invalid source");

        let r = get(&state, base);
        assert_eq!(r.json_body()["value"], 1.25);
    }

    #[test]
    fn output_arb_points_decoding() {
        let state = state_with_device("O2");
        let base = "/rest/v1/devices/com.nonolithlabs.test~O2/a/output";
        // %3A / %2C are the only decoded escapes; sampleTime 1e-4 so 0.001s = 10 samples
        let r = post(&state, base, "mode=1&wave=arb&points=0%3A0%2C0.001%3A5&repeat=0");
        let j = r.json_body();
        assert_eq!(j["source"], "arb");
        assert_eq!(j["values"][0]["t"], 0);
        assert_eq!(j["values"][1]["t"], 10);
        assert_eq!(j["values"][1]["v"], 5.0);

        let r = post(&state, base, "wave=arb&points=zzz");
        assert_eq!(r.status, 402);
        assert_eq!(r.json_body()["error"], "Invalid arbitrary wave point spec");
    }

    #[test]
    fn input_gain_post() {
        let state = state_with_device("G1");
        let r = post(
            &state,
            "/rest/v1/devices/com.nonolithlabs.test~G1/a/input",
            "gain_v=2",
        );
        let j = r.json_body();
        assert_eq!(j["gain"]["v"], 2.0);
        assert_eq!(j["gain"]["i"], 1.0);
    }

    #[test]
    fn input_csv_stream() {
        let state = state_with_device("CSV1");
        {
            let dev = state.device_by_id("com.nonolithlabs.test~CSV1").unwrap();
            let mut dev = dev.lock().unwrap();
            if let AnyDevice::Streaming(d) = &mut *dev {
                d.configure(0, 1e-4, 100, true, false);
            }
        }

        let r = get(
            &state,
            "/rest/v1/devices/com.nonolithlabs.test~CSV1/a/input?resample=0.0001&start=0&count=3",
        );
        assert_eq!(r.status, 200);
        let RestBody::Stream(mut rx) = r.body else { panic!() };

        // header first
        assert_eq!(rx.try_recv().unwrap(), "Voltage A (V),Current A (mA)\n");

        // feed data; the listener delivers incrementally
        {
            let dev = state.device_by_id("com.nonolithlabs.test~CSV1").unwrap();
            let mut dev = dev.lock().unwrap();
            if let AnyDevice::Streaming(d) = &mut *dev {
                feed_ramp(d, 4);
            }
        }
        let mut got = String::new();
        while let Ok(chunk) = rx.try_recv() {
            got.push_str(&chunk);
        }
        // feeder puts v = 10k, i = v/10 = k
        assert_eq!(got, "0, 0\n10, 1\n20, 2\n");
        // channel closed after count satisfied (listener dropped)
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn input_csv_bad_params_are_4xx() {
        let state = state_with_device("CSV2");
        let r = get(
            &state,
            "/rest/v1/devices/com.nonolithlabs.test~CSV2/a/input?count=-1",
        );
        assert_eq!(r.status, 402);
        let r = get(
            &state,
            "/rest/v1/devices/com.nonolithlabs.test~CSV2/a/input?resample=zzz",
        );
        assert_eq!(r.status, 402);
    }

    #[test]
    fn broadcasts_go_to_attached_ws_clients() {
        // REST-triggered state changes broadcast to WS clients on the device
        let state = state_with_device("BR1");
        let dev = state.device_by_id("com.nonolithlabs.test~BR1").unwrap();
        let (client, mut rx) = ClientHandle::pair();
        dev.lock().unwrap().on_client_attach(&client);
        while rx.try_recv().is_ok() {}

        post(&state, "/rest/v1/devices/com.nonolithlabs.test~BR1", "capture=on");
        let mut actions = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let crate::device::OutMsg::Json(v) = m {
                actions.push(v["_action"].as_str().unwrap().to_string());
            }
        }
        assert!(actions.contains(&"captureState".to_string()));
    }
}
