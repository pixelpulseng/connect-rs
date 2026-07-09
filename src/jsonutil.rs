// JSON property helpers matching the C++ json_helpers.hpp semantics
// (including error message text, which clients/tests may observe).

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Error(pub String);

impl Error {
    pub fn new(s: impl Into<String>) -> Error {
        Error(s.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub fn json_string_prop(n: &Value, prop: &str) -> Result<String> {
    match n.get(prop) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(Error(format!("JSON missing string property: {prop}"))),
    }
}

pub fn json_string_prop_def(n: &Value, prop: &str, def: &str) -> String {
    match n.get(prop) {
        Some(Value::String(s)) => s.clone(),
        _ => def.to_string(),
    }
}

pub fn json_bool_prop(n: &Value, prop: &str) -> Result<bool> {
    match n.get(prop) {
        Some(Value::Bool(b)) => Ok(*b),
        _ => Err(Error(format!("JSON missing bool property: {prop}"))),
    }
}

pub fn json_bool_prop_def(n: &Value, prop: &str, def: bool) -> bool {
    match n.get(prop) {
        Some(Value::Bool(b)) => *b,
        _ => def,
    }
}

pub fn json_int_prop(n: &Value, prop: &str) -> Result<i64> {
    match n.get(prop) {
        Some(v) if v.is_number() => Ok(v.as_f64().unwrap_or(0.0) as i64),
        _ => Err(Error(format!("JSON missing int property: {prop}"))),
    }
}

pub fn json_int_prop_def(n: &Value, prop: &str, def: i64) -> i64 {
    match n.get(prop) {
        Some(v) if v.is_number() => v.as_f64().unwrap_or(0.0) as i64,
        _ => def,
    }
}

pub fn json_float_prop(n: &Value, prop: &str) -> Result<f64> {
    match n.get(prop) {
        Some(v) if v.is_number() => Ok(v.as_f64().unwrap_or(0.0)),
        _ => Err(Error(format!("JSON missing float property: {prop}"))),
    }
}

pub fn json_float_prop_def(n: &Value, prop: &str, def: f64) -> f64 {
    match n.get(prop) {
        Some(v) if v.is_number() => v.as_f64().unwrap_or(0.0),
        _ => def,
    }
}

/// Serialize an f32 sample for JSON without inventing garbage digits
/// (0.3f32 must serialize as 0.3, not 0.30000001192092896). NaN/Inf become
/// null, which is what libjson emitted for non-finite floats.
pub fn f32_json(v: f32) -> Value {
    if !v.is_finite() {
        return Value::Null;
    }
    // Shortest representation that round-trips the f32, parsed back as f64.
    // {:?} keeps a trailing ".0" so whole values stay JSON floats.
    let s = format!("{v:?}");
    serde_json::from_str::<Value>(&s).unwrap_or(Value::Null)
}

/// Format a float the way `std::ostream <<` does by default: like printf
/// "%g" with 6 significant digits. Used for REST CSV rows.
pub fn fmt_g(v: f32) -> String {
    let v = v as f64;
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if v == 0.0 {
        return "0".to_string();
    }
    const PREC: i32 = 6;
    // Exponent of the value rounded to PREC significant digits.
    let mut exp = v.abs().log10().floor() as i32;
    // Rounding can bump the exponent (e.g. 999999.5 -> 1e+06).
    let scale = 10f64.powi(PREC - 1 - exp);
    let rounded = (v * scale).round() / scale;
    if rounded.abs().log10().floor() as i32 != exp && rounded != 0.0 {
        exp = rounded.abs().log10().floor() as i32;
    }
    if !(-4..PREC).contains(&exp) {
        // Scientific notation, PREC-1 fractional digits, trailing zeros stripped
        let s = format!("{:.*e}", (PREC - 1) as usize, v);
        // Rust: "2.5e0"; C++: "2.5e+00"
        let (mantissa, e) = s.split_once('e').unwrap();
        let mantissa = strip_trailing_zeros(mantissa);
        let eval: i32 = e.parse().unwrap();
        format!(
            "{}e{}{:02}",
            mantissa,
            if eval < 0 { '-' } else { '+' },
            eval.abs()
        )
    } else {
        let decimals = (PREC - 1 - exp).max(0) as usize;
        let s = format!("{v:.decimals$}");
        strip_trailing_zeros(&s)
    }
}

fn strip_trailing_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }
    let s = s.trim_end_matches('0');
    s.trim_end_matches('.').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_g_matches_cxx_ostream() {
        assert_eq!(fmt_g(0.0), "0");
        assert_eq!(fmt_g(1.0), "1");
        assert_eq!(fmt_g(10.0), "10");
        assert_eq!(fmt_g(20.0), "20");
        assert_eq!(fmt_g(2.5), "2.5");
        assert_eq!(fmt_g(-2.5), "-2.5");
        assert_eq!(fmt_g(0.5), "0.5");
        assert_eq!(fmt_g(2.34567), "2.34567");
        assert_eq!(fmt_g(123456.0), "123456");
        assert_eq!(fmt_g(1234567.0), "1.23457e+06");
        assert_eq!(fmt_g(1e6), "1e+06");
        assert_eq!(fmt_g(0.0001), "0.0001");
        assert_eq!(fmt_g(0.00001), "1e-05");
        assert_eq!(fmt_g(f32::NAN), "nan");
        // float 0.1 promoted to double prints as 0.1 with 6 sigfigs
        assert_eq!(fmt_g(0.1), "0.1");
        assert_eq!(fmt_g(1.5), "1.5");
    }

    #[test]
    fn prop_helpers() {
        let n = serde_json::json!({"s": "x", "i": 3, "f": 2.5, "b": true});
        assert_eq!(json_string_prop(&n, "s").unwrap(), "x");
        assert!(json_string_prop(&n, "i").is_err());
        assert_eq!(json_int_prop(&n, "i").unwrap(), 3);
        assert_eq!(json_int_prop(&n, "f").unwrap(), 2);
        assert_eq!(json_float_prop(&n, "f").unwrap(), 2.5);
        assert!(json_bool_prop(&n, "s").is_err());
        assert!(json_bool_prop(&n, "b").unwrap());
        assert_eq!(
            json_string_prop(&n, "nope").unwrap_err().0,
            "JSON missing string property: nope"
        );
    }
}
