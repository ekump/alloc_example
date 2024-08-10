use std::borrow::Cow;
use crate::{read_map_strs, read_string_ref};
use crate::DecodeError;
use crate::number::read_number;
use crate::SpanLink;
use rmp::Marker;
use std::str::FromStr;

/// Reads a slice of bytes and decodes it into a vector of `SpanLink` objects.
///
/// # Arguments
///
/// * `buf` - A mutable reference to a slice of bytes containing the encoded data.
///
/// # Returns
///
/// * `Ok(Vec<SpanLink>)` - A vector of decoded `SpanLink` objects if successful.
/// * `Err(DecodeError)` - An error if the decoding process fails.
///
/// # Errors
///
/// This function will return an error if:
/// - The marker for the array length cannot be read.
/// - Any `SpanLink` cannot be decoded.
/// ```
pub(crate) fn read_span_links<'a>(buf: &mut &'a [u8]) -> Result<Vec<SpanLink<'a>>, DecodeError> {
    match rmp::decode::read_marker(buf).map_err(|_| {
        DecodeError::Generic("Unable to read marker for span links".to_owned())
    })? {
        Marker::FixArray(len) => {
            let mut vec: Vec<SpanLink> = Vec::new();
            for _ in 0..len {
                vec.push(decode_span_link(buf)?);
            }
            Ok(vec)
        }
        _ => Err(DecodeError::Generic(
            "Unable to read span link from buffer".to_owned(),
        )),
    }
}
#[derive(Debug, PartialEq)]
enum SpanLinkKey {
    TraceId,
    TraceIdHigh,
    SpanId,
    Attributes,
    Tracestate,
    Flags,
}
impl FromStr for SpanLinkKey {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trace_id" => Ok(SpanLinkKey::TraceId),
            "trace_id_high" => Ok(SpanLinkKey::TraceIdHigh),
            "span_id" => Ok(SpanLinkKey::SpanId),
            "attributes" => Ok(SpanLinkKey::Attributes),
            "tracestate" => Ok(SpanLinkKey::Tracestate),
            "flags" => Ok(SpanLinkKey::Flags),
            _ => Err(DecodeError::Generic(
                format!("Invalid span link key: {}", s).to_owned(),
            )),
        }
    }
}

fn decode_span_link<'a>(buf: &mut &'a [u8]) -> Result<SpanLink<'a>, DecodeError> {
    let mut span = SpanLink::default();
    let span_size = rmp::decode::read_map_len(buf)
        .map_err(|_| DecodeError::Generic("Unable to get map len for span size".to_owned()))?;

    for _ in 0..span_size {
        let (key, value) = read_string_ref(buf)?;
        *buf = value;
        let key = key.parse::<SpanLinkKey>()?;

        match key {
            SpanLinkKey::TraceId => span.trace_id = read_number(buf)?.try_into()?,
            SpanLinkKey::TraceIdHigh => span.trace_id_high = read_number(buf)?.try_into()?,
            SpanLinkKey::SpanId => span.span_id = read_number(buf)?.try_into()?,
            SpanLinkKey::Attributes => span.attributes = read_map_strs(buf)?,
            SpanLinkKey::Tracestate => {
                let (value, next) = read_string_ref(buf)?;
                *buf = next;
                span.trace_state = Cow::Borrowed(value);
            }
            SpanLinkKey::Flags => span.flags = read_number(buf)?.try_into()?,
        }
    }

    Ok(span)
}
