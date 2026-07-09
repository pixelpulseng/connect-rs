// Signal generation sources, ported from streaming_device/output_source.cpp.
//
// Semantics match the C++ exactly, with one deliberate fix (SPEC.md Q1):
// TriangleWaveSource used sign-preserving fmod, yielding out-of-range values
// for the first quarter-period; here we use rem_euclid, so the waveform is
// in range everywhere and identical to the C++ wherever the C++ was correct.

use crate::jsonutil::*;
use serde_json::{json, Map, Value};

#[derive(Debug, Clone)]
pub struct OutputSource {
    /// SMU mode this source drives: 0 disabled/Hi-Z, 1 SVMI, 2 SIMV
    pub mode: u32,
    /// Output sample number at which this source was added
    pub start_sample: u64,
    /// true once this source's effect has come back as input
    pub effective: bool,
    /// Client hint, echoed back verbatim
    pub hint: String,
    pub kind: SourceKind,
}

#[derive(Debug, Clone)]
pub enum SourceKind {
    Constant {
        value: f32,
    },
    /// sine | triangle | square
    Periodic {
        wave: Wave,
        offset: f64,
        amplitude: f64,
        period: f64,
        phase: f64,
        rel_phase: bool,
    },
    AdvSquare {
        high: f32,
        low: f32,
        high_samples: u32,
        low_samples: u32,
        phase: i64,
        rel_phase: bool,
    },
    Arb {
        phase: i64,
        start_time: i64,
        values: Vec<(i64, f32)>, // (t, v), t in samples
        index: usize,
        repeat_count: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wave {
    Sine,
    Triangle,
    Square,
}

impl OutputSource {
    pub fn constant(mode: u32, value: f32) -> OutputSource {
        OutputSource {
            mode,
            start_sample: 0,
            effective: false,
            hint: String::new(),
            kind: SourceKind::Constant { value },
        }
    }

    pub fn periodic(
        mode: u32,
        wave: Wave,
        offset: f64,
        amplitude: f64,
        period: f64,
        phase: f64,
        rel_phase: bool,
    ) -> OutputSource {
        OutputSource {
            mode,
            start_sample: 0,
            effective: false,
            hint: String::new(),
            kind: SourceKind::Periodic {
                wave,
                offset,
                amplitude,
                period,
                phase,
                rel_phase,
            },
        }
    }

    pub fn adv_square(
        mode: u32,
        high: f32,
        low: f32,
        high_samples: u32,
        low_samples: u32,
        phase: i64,
        rel_phase: bool,
    ) -> Result<OutputSource> {
        if high_samples + low_samples == 0 {
            return Err(Error::new("Square wave must have nonzero period."));
        }
        Ok(OutputSource {
            mode,
            start_sample: 0,
            effective: false,
            hint: String::new(),
            kind: SourceKind::AdvSquare {
                high,
                low,
                high_samples,
                low_samples,
                phase,
                rel_phase,
            },
        })
    }

    pub fn arb(
        mode: u32,
        phase: i64,
        values: Vec<(i64, f32)>,
        repeat_count: i64,
    ) -> Result<OutputSource> {
        let mut repeat_count = repeat_count;
        if repeat_count == 0 {
            repeat_count = 1;
        }
        if values.is_empty() {
            return Err(Error::new("Arb wave must have at least one point."));
        }
        if values[0].0 != 0 {
            return Err(Error::new("Arb wave first point must have t=0."));
        }
        let mut last_t = 0;
        for &(t, _) in &values {
            if t < last_t {
                return Err(Error::new("Arb wave points must be in time order."));
            }
            last_t = t;
        }
        let period = values[values.len() - 1].0;
        if period == 0 && repeat_count != 1 {
            return Err(Error::new("Arb wave with repeat must have nonzero period."));
        }
        Ok(OutputSource {
            mode,
            start_sample: 0,
            effective: false,
            hint: String::new(),
            kind: SourceKind::Arb {
                phase,
                start_time: 0,
                values,
                index: 0,
                repeat_count,
            },
        })
    }

    pub fn display_name(&self) -> &'static str {
        match &self.kind {
            SourceKind::Constant { .. } => "constant",
            SourceKind::Periodic {
                wave: Wave::Sine, ..
            } => "sine",
            SourceKind::Periodic {
                wave: Wave::Triangle,
                ..
            } => "triangle",
            SourceKind::Periodic {
                wave: Wave::Square, ..
            } => "square",
            SourceKind::AdvSquare { .. } => "adv_square",
            SourceKind::Arb { .. } => "arb",
        }
    }

    /// Evaluate the source at `sample`. `&mut` because the arb evaluator is a
    /// stateful forward walker (must be queried with non-decreasing samples).
    pub fn get_value(&mut self, sample: u64, _sample_time: f64) -> f32 {
        match &mut self.kind {
            SourceKind::Constant { value } => *value,
            SourceKind::Periodic {
                wave,
                offset,
                amplitude,
                period,
                phase,
                ..
            } => {
                let s = sample as f64;
                match wave {
                    Wave::Sine => {
                        (((s + *phase) * 2.0 * std::f64::consts::PI / *period).sin() * *amplitude
                            + *offset) as f32
                    }
                    Wave::Triangle => {
                        // Q1 fixed: rem_euclid instead of sign-preserving fmod
                        ((((s + *phase - *period / 4.0).rem_euclid(*period) / *period * 2.0 - 1.0)
                            .abs()
                            * 2.0
                            - 1.0)
                            * *amplitude
                            + *offset) as f32
                    }
                    Wave::Square => {
                        let m = (s + *phase) % *period;
                        if m < *period / 2.0 {
                            (*offset + *amplitude) as f32
                        } else {
                            (*offset - *amplitude) as f32
                        }
                    }
                }
            }
            SourceKind::AdvSquare {
                high,
                low,
                high_samples,
                low_samples,
                phase,
                ..
            } => {
                let per = (*high_samples + *low_samples) as i64;
                let s = (sample as i64 + *phase).rem_euclid(per);
                if s < *low_samples as i64 {
                    *low
                } else {
                    *high
                }
            }
            SourceKind::Arb {
                start_time,
                values,
                index,
                repeat_count,
                ..
            } => {
                let mut sample = sample as i64;
                if sample < *start_time {
                    sample = 0;
                } else {
                    // All times are relative to startTime
                    sample -= *start_time;
                }

                let length = values.len();
                let (time1, time2, value1, value2);
                loop {
                    let t1 = values[*index].0;
                    let v1 = values[*index].1;

                    let next_index = *index + 1;
                    if next_index >= length {
                        // repeat == -1 means infinite
                        if *repeat_count > 1 || *repeat_count == -1 {
                            if *repeat_count > 0 {
                                *repeat_count -= 1;
                            }
                            *index = 0;
                            *start_time += t1;
                            sample -= t1;
                            continue;
                        } else {
                            // If repeat is disabled, the last value remains forever
                            return v1;
                        }
                    }

                    let t2 = values[next_index].0;
                    let v2 = values[next_index].1;

                    if sample >= t2 {
                        // When we pass the next point, move forward in the list
                        *index = next_index;
                        continue;
                    } else {
                        time1 = t1;
                        time2 = t2;
                        value1 = v1;
                        value2 = v2;
                        break;
                    }
                }

                // For the first point
                if sample < time1 {
                    return value1;
                }

                // Proportion of the time between the last point and the next point
                let p = (sample - time1) as f64 / (time2 - time1) as f64;

                // Trapezoidal interpolation
                ((1.0 - p) * value1 as f64 + p * value2 as f64) as f32
            }
        }
    }

    /// First (possibly fractional) sample index >= `sample` at which the
    /// waveform is at phase zero. Used by out-source triggers.
    pub fn phase_zero_after(&self, sample: u64) -> f64 {
        let s = sample as f64;
        match &self.kind {
            SourceKind::Constant { .. } => s,
            SourceKind::Periodic {
                wave: Wave::Square,
                period,
                phase,
                ..
            } => {
                // its own definition because it jumps instead of slides
                let m = (s + phase) % period;
                s + (period - m).ceil()
            }
            SourceKind::Periodic { period, phase, .. } => {
                s + (period - (s + phase) % period) % period
            }
            SourceKind::AdvSquare {
                high_samples,
                low_samples,
                phase,
                ..
            } => {
                let per = (*high_samples + *low_samples) as i64;
                let low = *low_samples as i64;
                (sample as i64
                    + (per + low - (sample as i64 + *phase).rem_euclid(per)).rem_euclid(per))
                    as f64
            }
            SourceKind::Arb { phase, values, .. } => {
                let per = values[values.len() - 1].0;
                if per == 0 {
                    return s;
                }
                (sample as i64 + (per - (sample as i64 - *phase).rem_euclid(per)).rem_euclid(per))
                    as f64
            }
        }
    }

    /// Phase adjustment hook called when this source replaces `prev` at
    /// output sample `sample`.
    pub fn initialize(&mut self, sample: u64, prev: Option<&OutputSource>) {
        match &mut self.kind {
            SourceKind::Constant { .. } => {}
            SourceKind::Periodic {
                period,
                phase,
                rel_phase,
                ..
            } => {
                if *rel_phase {
                    if let Some(OutputSource {
                        kind:
                            SourceKind::Periodic {
                                period: prev_period,
                                phase: prev_phase,
                                ..
                            },
                        ..
                    }) = prev
                    {
                        *phase += (sample as f64 + prev_phase) % prev_period / prev_period
                            * *period
                            - sample as f64;
                    }
                }
                *phase %= *period;
            }
            SourceKind::AdvSquare {
                high_samples,
                low_samples,
                phase,
                rel_phase,
                ..
            } => {
                let period = (*high_samples + *low_samples) as i64;
                if *rel_phase {
                    if let Some(OutputSource {
                        kind:
                            SourceKind::AdvSquare {
                                high_samples: ph,
                                low_samples: pl,
                                phase: pp,
                                ..
                            },
                        ..
                    }) = prev
                    {
                        let old_period = (*ph + *pl) as i64;
                        let frac =
                            (sample as i64 + *pp).rem_euclid(old_period) as f64 / old_period as f64;
                        *phase += (frac * period as f64).round() as i64 - (sample as i64 % period);
                    }
                }
                *phase = phase.rem_euclid(period);
            }
            SourceKind::Arb {
                phase,
                start_time,
                values,
                repeat_count,
                ..
            } => {
                let sample = sample as i64;
                if *phase < 0 {
                    *start_time = sample;
                    *phase = sample;
                } else if *repeat_count != 1 {
                    let per = values[values.len() - 1].0;
                    *start_time = sample - sample % per + *phase % per;
                } else {
                    *start_time = *phase;
                }
            }
        }
    }

    /// The `describeJSON` serialization: common fields then variant fields,
    /// in the same order as the C++.
    pub fn describe(&self, n: &mut Map<String, Value>) {
        n.insert("mode".into(), json!(self.mode));
        n.insert("startSample".into(), json!(self.start_sample));
        n.insert("effective".into(), json!(self.effective));
        n.insert("source".into(), json!(self.display_name()));
        n.insert("hint".into(), json!(self.hint));
        match &self.kind {
            SourceKind::Constant { value } => {
                n.insert("value".into(), f32_json(*value));
            }
            SourceKind::Periodic {
                offset,
                amplitude,
                period,
                phase,
                ..
            } => {
                // C++ PeriodicSource stores offset/amplitude as float
                n.insert("offset".into(), f32_json(*offset as f32));
                n.insert("amplitude".into(), f32_json(*amplitude as f32));
                n.insert("period".into(), json!(period));
                n.insert("phase".into(), json!(phase));
            }
            SourceKind::AdvSquare {
                high,
                low,
                high_samples,
                low_samples,
                ..
            } => {
                n.insert("high".into(), f32_json(*high));
                n.insert("low".into(), f32_json(*low));
                n.insert("highSamples".into(), json!(high_samples));
                n.insert("lowSamples".into(), json!(low_samples));
            }
            SourceKind::Arb {
                phase,
                values,
                repeat_count,
                ..
            } => {
                n.insert("phase".into(), json!(phase));
                n.insert("repeat".into(), json!(repeat_count));
                let points: Vec<Value> = values
                    .iter()
                    .map(|&(t, v)| json!({"t": t, "v": f32_json(v)}))
                    .collect();
                n.insert("values".into(), Value::Array(points));
                n.insert("period".into(), json!(values[values.len() - 1].0));
            }
        }
    }

    pub fn describe_json(&self) -> Value {
        let mut m = Map::new();
        self.describe(&mut m);
        Value::Object(m)
    }
}

/// Build a source from a WS `set` command / JSON POST body
/// (output_source.cpp makeSource(JSONNode&)).
pub fn make_source(n: &Value) -> Result<OutputSource> {
    let source = json_string_prop_def(n, "source", "constant");
    let mode = json_float_prop_def(n, "mode", 0.0) as u32;
    let hint = json_string_prop_def(n, "hint", "");

    let mut r = match source.as_str() {
        "constant" => {
            let val = json_float_prop_def(n, "value", 0.0) as f32;
            OutputSource::constant(mode, val)
        }
        "adv_square" => {
            let high = json_float_prop(n, "high")? as f32;
            let low = json_float_prop(n, "low")? as f32;
            let high_samples = json_int_prop(n, "highSamples")?;
            let low_samples = json_int_prop(n, "lowSamples")?;
            let phase = json_int_prop_def(n, "phase", 0);
            let rel_phase = json_bool_prop_def(n, "relPhase", true);
            OutputSource::adv_square(
                mode,
                high,
                low,
                high_samples as u32,
                low_samples as u32,
                phase,
                rel_phase,
            )?
        }
        "sine" | "triangle" | "square" => {
            let wave = match source.as_str() {
                "sine" => Wave::Sine,
                "triangle" => Wave::Triangle,
                _ => Wave::Square,
            };
            let offset = json_float_prop(n, "offset")?;
            let amplitude = json_float_prop(n, "amplitude")?;
            let period = json_float_prop(n, "period")?;
            let phase = json_float_prop_def(n, "phase", 0.0);
            let rel_phase = json_bool_prop_def(n, "relPhase", true);
            OutputSource::periodic(mode, wave, offset, amplitude, period, phase, rel_phase)
        }
        "arb" => {
            let phase = json_int_prop_def(n, "phase", -1);
            let repeat = json_int_prop_def(n, "repeat", 0);
            let j_values = n
                .get("values")
                .and_then(|v| v.as_array())
                .ok_or_else(|| Error::new("JSON missing property: values"))?;
            let mut values = Vec::new();
            for i in j_values {
                values.push((json_int_prop(i, "t")?, json_float_prop(i, "v")? as f32));
            }
            OutputSource::arb(mode, phase, values, repeat)?
        }
        _ => return Err(Error::new("Invalid source")),
    };

    r.hint = hint;
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(s: &mut OutputSource, i: u64) -> f32 {
        s.get_value(i, 1e-4)
    }

    #[test]
    fn constant_source() {
        let mut s = OutputSource::constant(1, 2.5);
        assert_eq!(val(&mut s, 0), 2.5);
        assert_eq!(val(&mut s, 1000), 2.5);
        assert_eq!(s.phase_zero_after(42), 42.0);
        assert_eq!(s.display_name(), "constant");
    }

    #[test]
    fn sine_source() {
        let mut s = OutputSource::periodic(1, Wave::Sine, 2.5, 1.0, 100.0, 0.0, false);
        assert!((val(&mut s, 0) - 2.5).abs() < 1e-4);
        assert!((val(&mut s, 25) - 3.5).abs() < 1e-4);
        assert!((val(&mut s, 50) - 2.5).abs() < 1e-4);
        assert!((val(&mut s, 75) - 1.5).abs() < 1e-4);
        assert!((val(&mut s, 100) - 2.5).abs() < 1e-4);
        // phase shifts left
        let mut s = OutputSource::periodic(1, Wave::Sine, 0.0, 1.0, 100.0, 25.0, false);
        assert!((val(&mut s, 0) - 1.0).abs() < 1e-4);
    }

    #[test]
    fn triangle_source_in_range_everywhere() {
        // Q1: the C++ returned offset + amplitude*[1..2] for samples 0..25.
        // The port fixes this: the triangle is sine-aligned — offset at 0,
        // peak at period/4, minimum at 3*period/4.
        let mut s = OutputSource::periodic(1, Wave::Triangle, 2.5, 1.0, 100.0, 0.0, false);
        for i in 0..300 {
            let v = val(&mut s, i);
            assert!((1.5..=3.5).contains(&v), "sample {i} out of range: {v}");
        }
        assert!((val(&mut s, 0) - 2.5).abs() < 1e-4);
        assert!((val(&mut s, 25) - 3.5).abs() < 1e-4);
        assert!((val(&mut s, 50) - 2.5).abs() < 1e-4);
        assert!((val(&mut s, 75) - 1.5).abs() < 1e-4);
        assert!((val(&mut s, 100) - 2.5).abs() < 1e-4);
    }

    #[test]
    fn square_source() {
        let mut s = OutputSource::periodic(1, Wave::Square, 2.0, 1.0, 100.0, 0.0, false);
        assert_eq!(val(&mut s, 0), 3.0);
        assert_eq!(val(&mut s, 49), 3.0);
        assert_eq!(val(&mut s, 50), 1.0);
        assert_eq!(val(&mut s, 99), 1.0);
        assert_eq!(val(&mut s, 100), 3.0);
        // phase zero jumps to the next period start
        assert_eq!(s.phase_zero_after(0), 100.0);
        assert_eq!(s.phase_zero_after(1), 100.0);
        assert_eq!(s.phase_zero_after(100), 200.0);
    }

    #[test]
    fn periodic_phase_zero() {
        let s = OutputSource::periodic(1, Wave::Sine, 0.0, 1.0, 100.0, 0.0, false);
        assert_eq!(s.phase_zero_after(0), 0.0);
        assert_eq!(s.phase_zero_after(1), 100.0);
        assert_eq!(s.phase_zero_after(100), 100.0);
        assert_eq!(s.phase_zero_after(101), 200.0);
        let s = OutputSource::periodic(1, Wave::Sine, 0.0, 1.0, 100.0, 30.0, false);
        assert_eq!(s.phase_zero_after(0), 70.0);
    }

    #[test]
    fn adv_square_low_first() {
        // low segment comes first (phase 0 starts in the low samples)
        let mut s = OutputSource::adv_square(1, 3.0, 1.0, 10, 5, 0, false).unwrap();
        assert_eq!(val(&mut s, 0), 1.0);
        assert_eq!(val(&mut s, 4), 1.0);
        assert_eq!(val(&mut s, 5), 3.0);
        assert_eq!(val(&mut s, 14), 3.0);
        assert_eq!(val(&mut s, 15), 1.0);
        // phase zero = next low->high transition
        assert_eq!(s.phase_zero_after(0), 5.0);
        assert_eq!(s.phase_zero_after(5), 5.0);
        assert_eq!(s.phase_zero_after(6), 20.0);
        // zero period rejected
        assert!(OutputSource::adv_square(1, 3.0, 1.0, 0, 0, 0, false).is_err());
    }

    #[test]
    fn arb_interpolation_and_repeat() {
        // ramp 0..10 over 10 samples, hold
        let mut s = OutputSource::arb(1, -1, vec![(0, 0.0), (10, 10.0)], 1).unwrap();
        s.initialize(0, None);
        assert_eq!(val(&mut s, 0), 0.0);
        assert_eq!(val(&mut s, 5), 5.0);
        assert_eq!(val(&mut s, 10), 10.0);
        // last value holds forever when not repeating
        assert_eq!(val(&mut s, 11), 10.0);
        assert_eq!(val(&mut s, 100), 10.0);

        // infinite repeat wraps
        let mut s = OutputSource::arb(1, -1, vec![(0, 0.0), (10, 10.0)], -1).unwrap();
        s.initialize(0, None);
        assert_eq!(val(&mut s, 0), 0.0);
        assert_eq!(val(&mut s, 5), 5.0);
        assert_eq!(val(&mut s, 12), 2.0);
        assert_eq!(val(&mut s, 25), 5.0);

        // N passes then hold
        let mut s = OutputSource::arb(1, -1, vec![(0, 0.0), (10, 10.0)], 2).unwrap();
        s.initialize(0, None);
        assert_eq!(val(&mut s, 5), 5.0);
        assert_eq!(val(&mut s, 15), 5.0);
        assert_eq!(val(&mut s, 25), 10.0); // second pass done, hold

        // validation
        assert!(OutputSource::arb(1, -1, vec![], 1).is_err());
        assert!(OutputSource::arb(1, -1, vec![(1, 0.0)], 1).is_err());
        assert!(OutputSource::arb(1, -1, vec![(0, 0.0), (5, 1.0), (3, 2.0)], 1).is_err());
        assert!(OutputSource::arb(1, -1, vec![(0, 0.0)], 2).is_err());
    }

    #[test]
    fn rel_phase_continuity() {
        // switching between same-family periodic sources preserves phase fraction
        let mut a = OutputSource::periodic(1, Wave::Sine, 0.0, 1.0, 100.0, 0.0, false);
        let mut b = OutputSource::periodic(1, Wave::Sine, 0.0, 1.0, 200.0, 0.0, true);
        b.initialize(150, Some(&a));
        // at sample 150 source a is at phase fraction 0.5; b must be too
        let va = a.get_value(150, 1e-4);
        let vb = b.get_value(150, 1e-4);
        assert!((va - vb).abs() < 1e-4);
        if let SourceKind::Periodic { phase, .. } = b.kind {
            assert!(((150.0 + phase) % 200.0 / 200.0 - 0.5).abs() < 1e-9);
        } else {
            panic!()
        }
    }

    #[test]
    fn make_source_from_json() {
        let s = make_source(
            &serde_json::json!({"source":"constant","mode":1,"value":2.5,"hint":"dutycycle:4"}),
        )
        .unwrap();
        assert_eq!(s.mode, 1);
        assert_eq!(s.hint, "dutycycle:4");
        let j = s.describe_json();
        assert_eq!(j["source"], "constant");
        assert_eq!(j["value"], 2.5);
        assert_eq!(j["effective"], false);
        assert_eq!(j["hint"], "dutycycle:4");

        // default source is constant, default value 0
        let s = make_source(&serde_json::json!({})).unwrap();
        assert_eq!(s.display_name(), "constant");

        // missing required props error with the C++ message
        let e = make_source(&serde_json::json!({"source":"sine"})).unwrap_err();
        assert_eq!(e.0, "JSON missing float property: offset");

        let e = make_source(&serde_json::json!({"source":"nope"})).unwrap_err();
        assert_eq!(e.0, "Invalid source");
    }
}
