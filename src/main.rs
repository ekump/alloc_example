use std::collections::HashMap;
use std::borrow::Cow;
use std::str::FromStr;
use rmp::{decode, Marker};
use rmp::decode::{DecodeStringError, RmpRead};
use rmp_serde::encode::to_vec_named;
use serde::Serialize;

mod number;
mod span_link;
#[derive(Clone, Debug)]
pub enum TracerPayloadCollection<'a> {
    V04(Vec<Vec<Span<'a>>>),
}

fn main() {
    println!("Starting app");
    let span = create_source_span();
    let span_2 = create_source_span();

    let spans = vec![vec![span, span_2]];
    let payload = source_span_to_msgpack(&spans).unwrap();
    sidecar_send_trace_v04(payload.as_slice());
}

// This function is intended to simulate the sidecar_server impl method send_trace_v04, which receives a &[u8] of msgpack data.
fn sidecar_send_trace_v04(data: &[u8]) {
    let trace_payload = msgpack_to_tracer_payload_collection(data).unwrap();
    println!("Payload decoded: {:?}", trace_payload);
}
// This function is inteded to simulate the public interface with tracer_payload. It is intended to replace the try_into with the TracerPayloadParams as that isn't neecessary anymore.
fn msgpack_to_tracer_payload_collection<'a>(mut data: &'a [u8]) -> Result<TracerPayloadCollection<'a>, DecodeError> {
    let traces = from_slice(&mut data)?;

    Ok(TracerPayloadCollection::V04(traces))
}

fn from_slice<'a>(mut data: &'a [u8]) -> Result<Vec<Vec<Span<'a>>>, DecodeError> {
    let trace_count = rmp::decode::read_array_len(&mut data).map_err(|_| {
        DecodeError::Generic("Unable to get array len for trace count".to_owned())
    })?;

    println!("Trace count: {}", trace_count);

    let mut traces: Vec<Vec<Span<'a>>> = Default::default();

    for _ in 0..trace_count {
        let span_count = rmp::decode::read_array_len(&mut data).map_err(|_| {
            DecodeError::Generic("Unable to get map len for span size".to_owned())
        })?;

        println!("Span count: {}", span_count);
        let mut trace: Vec<Span<'a>> = Default::default();

        for _ in 0..span_count {
            let span = decode_span(data)?;
            println!("Span: {:?}", span);
            trace.push(span);
        }
        traces.push(trace);
    }

    Ok(traces)
}

// Note: buf has to be a mutable reference to a reference to a slice of bytes for rmp.
fn decode_span<'a>(mut buf: &'a [u8]) -> Result<Span<'a>, DecodeError> {
    let mut span = Span::default();

    let span_size = rmp::decode::read_map_len(&mut buf).map_err(|_| {
        DecodeError::Generic("Unable to get map len for span size".to_owned())
    })?;

    for _ in 0..span_size {
        fill_span(&mut span, &mut buf)?;
    }

    Ok(span)
}

fn fill_span<'a>(span: &mut Span<'a>, buf: &mut &'a [u8]) -> Result<(), DecodeError> {
    let (key, value) = read_string_ref(buf)?;
    let key = key.parse::<SpanKey>()?;

    *buf = value;

    match key {
        SpanKey::Service => {
            let (value , next) = read_string_ref(buf)?;
            span.service = Cow::Borrowed(value);
            *buf = next;
        }
        SpanKey::Name => {
            let (value , next) = read_string_ref(buf)?;
            span.name = Cow::Borrowed(value);
            *buf = next;
        }
        SpanKey::Resource => {
            let (value , next) = read_string_ref(buf)?;
            span.resource = Cow::Borrowed(value);
            *buf = next;
        }
        SpanKey::TraceId => span.trace_id = number::read_number(buf)?.try_into()?,
        SpanKey::SpanId => span.span_id = number::read_number(buf)?.try_into()?,
        SpanKey::ParentId => span.parent_id = number::read_number(buf)?.try_into()?,
        SpanKey::Start => span.start = number::read_number(buf)?.try_into()?,
        SpanKey::Duration => span.duration = number::read_number(buf)?.try_into()?,
        SpanKey::Error => span.error = number::read_number(buf)?.try_into()?,
        SpanKey::Meta => span.meta = read_map_strs(buf)?,
        SpanKey::Metrics => span.metrics = read_metrics(buf)?,
        SpanKey::Type => {
            let (value , next) = read_string_ref(buf)?;
            span.r#type = Cow::Borrowed(value);
            *buf = next;
        }
        SpanKey::MetaStruct => span.meta_struct = read_meta_struct(buf)?,
        SpanKey::SpanLinks => span.span_links = span_link::read_span_links(buf)?,
    }
    Ok(())
}


fn read_metrics<'a>(buf: &mut &'a [u8]) -> Result<HashMap<Cow<'a, str>, f64>, DecodeError> {
    let len = read_map_len(buf)?;
    // read_map(len, buf, read_metric_pair)
    read_metric_pair(len, buf)
}

fn read_meta_struct<'a>(buf: &mut &'a [u8]) -> Result<HashMap<Cow<'a, str>, Vec<u8>>, DecodeError> {
    let len = read_map_len(buf)?;

    let mut map = HashMap::new();
    // read_map(len, buf, read_metric_pair)

    for _ in 0..len {
        let (k, v) = read_meta_struct_pair(len, buf)?;
        map.insert(k, v);
    }

    Ok(map)
}

fn read_meta_struct_pair<'a>(len: usize, buf: &mut &'a [u8]) -> Result<(Cow<'a, str>, Vec<u8>), DecodeError> {
    let (k, next) = read_string_ref(buf)?;
    *buf = next;

    let mut v = vec![];
    let array_len = decode::read_array_len(buf).map_err(|_| {
        DecodeError::Generic("Unable to read array len for meta_struct".to_owned())
    })?;
    for _ in 0..array_len {
        let value = number::read_number(buf)?.try_into()?;
        v.push(value);
    }
    Ok((Cow::Borrowed(k), v))
}

fn read_metric_pair<'a>(len: usize, buf: &mut &'a [u8]) -> Result<HashMap<Cow<'a, str>, f64>, DecodeError> {
    let mut map = HashMap::new();
    for _ in 0..len {
        let (k, next) = read_string_ref(buf)?;
        *buf = next;

        let v = number::read_number(buf)?.try_into()?;
        map.insert(Cow::Borrowed(k), v);
    }

    Ok(map)
}

fn read_map_strs<'a>(buf: &mut &'a [u8]) -> Result<HashMap<Cow<'a, str>, Cow<'a, str>>, DecodeError> {
    let len = read_map_len(buf)?;
    // read_map(len, buf, read_str_pair)
    read_str_pair(len, buf)
}

fn read_str_pair<'a>(len: usize, buf: &mut &'a [u8]) -> Result<HashMap<Cow<'a, str>, Cow<'a, str>>, DecodeError> {
    let mut map = HashMap::new();
    for _ in 0..len {
        let (k, next) = read_string_ref(buf)?;
        *buf = next;

        let (v, next) = read_string_ref(buf)?;
        *buf = next;
        map.insert(Cow::Borrowed(k), Cow::Borrowed(v));
    }
    Ok(map)
}

// #[inline]
// fn read_str_pair(buf: &mut &[u8]) -> Result<(String, String), DecodeError> {
//     let k = read_string(buf)?;
//     let v = read_string(buf)?;
//
//     Ok((k, v))
// }

fn read_map_len(buf: &mut &[u8]) -> Result<usize, DecodeError> {
    match decode::read_marker(buf)
        .map_err(|_| DecodeError::Generic("Unable to read marker for map".to_owned()))?
    {
        Marker::FixMap(len) => Ok(len as usize),
        Marker::Map16 => buf
            .read_data_u16()
            .map_err(|_| DecodeError::Generic("generic".to_owned()))
            .map(|len| len as usize),
        Marker::Map32 => buf
            .read_data_u32()
            .map_err(|_| DecodeError::Generic("generic".to_owned()))
            .map(|len| len as usize),
        _ => Err(DecodeError::Generic(
            "Unable to read map from buffer".to_owned(),
        )),
    }
}


#[inline]
fn read_string_ref(buf: &[u8]) -> Result<(&str, &[u8]), DecodeError> {
    decode::read_str_from_slice(buf).map_err(|e| match e {
        DecodeStringError::InvalidMarkerRead(e) => DecodeError::Generic(e.to_string()),
        DecodeStringError::InvalidDataRead(e) => DecodeError::Generic(e.to_string()),
        DecodeStringError::TypeMismatch(marker) => {
            DecodeError::Generic(format!("Type mismatch at marker {:?}", marker))
        }
        DecodeStringError::InvalidUtf8(_, e) => DecodeError::Generic(e.to_string()),
        _ => DecodeError::Generic("foo".to_owned()),
    })
}

#[inline]
fn read_string(buf: &mut &[u8]) -> Result<String, DecodeError> {
    let (str_ref, remaining_buf) = read_string_ref(buf)?;
    *buf = remaining_buf;
    Ok(str_ref.to_string())
}

#[derive(Debug, PartialEq)]
enum SpanKey {
    Service,
    Name,
    Resource,
    TraceId,
    SpanId,
    ParentId,
    Start,
    Duration,
    Error,
    Meta,
    Metrics,
    Type,
    MetaStruct,
    SpanLinks,
}

impl FromStr for SpanKey {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "service" => Ok(SpanKey::Service),
            "name" => Ok(SpanKey::Name),
            "resource" => Ok(SpanKey::Resource),
            "trace_id" => Ok(SpanKey::TraceId),
            "span_id" => Ok(SpanKey::SpanId),
            "parent_id" => Ok(SpanKey::ParentId),
            "start" => Ok(SpanKey::Start),
            "duration" => Ok(SpanKey::Duration),
            "error" => Ok(SpanKey::Error),
            "meta" => Ok(SpanKey::Meta),
            "metrics" => Ok(SpanKey::Metrics),
            "type" => Ok(SpanKey::Type),
            "meta_struct" => Ok(SpanKey::MetaStruct),
            "span_links" => Ok(SpanKey::SpanLinks),
            _ => Err(DecodeError::Generic(
                format!("Invalid span key: {}", s).to_owned(),
            )),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Span<'a> {
   service:  Cow<'a, str>,
    name: Cow<'a, str>,
    resource: Cow<'a, str>,
    r#type: Cow<'a, str>,
    trace_id: u64,
    span_id: u64,
    parent_id: u64,
    start: i64,
    duration: i64,
    error: i32,
    meta: HashMap<Cow<'a, str>, Cow<'a, str>>,
    metrics: HashMap<Cow<'a, str>, f64>,
    meta_struct: HashMap<Cow<'a, str>, Vec<u8>>,
    span_links: Vec<SpanLink<'a>>,
}

#[derive(Clone, Debug, Default)]
struct SpanLink<'a> {
    trace_id: u64,
    trace_id_high: u64,
    span_id: u64,
    attributes: HashMap<Cow<'a, str>, Cow<'a, str>>,
    trace_state: Cow<'a, str>,
    flags: u64,
}

#[derive(Serialize)]
struct SourceSpanLink {
    trace_id: u64,
    trace_id_high: u64,
    span_id: u64,
    attributes: HashMap<String, String>,
    tracestate: String,
    flags: u64,
}
#[derive(Serialize)]
struct SourceSpan {
    service: String,
    name: String,
    resource: String,
    trace_id: u64,
    span_id: u64,
    parent_id: u64,
    start: i64,
    duration: i64,
    error: i32,
    meta: HashMap<String, String>,
    metrics: HashMap<String, f64>,
    r#type: String,
    meta_struct: HashMap<String, Vec<u8>>,
    span_links: Vec<SourceSpanLink>,
}

#[derive(Debug, PartialEq)]
pub enum DecodeError {
    Generic(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Generic(msg) => write!(f, "Error: {}", msg),
        }
    }
}
fn create_source_span() -> SourceSpan {
    let mut meta = HashMap::new();
    meta.insert("example_meta_key1".to_string(), "example_meta_value1".to_string());
    meta.insert("example_meta_key2".to_string(), "example_meta_value2".to_string());

    let mut metrics = HashMap::new();

    metrics.insert("example_metric_key1".to_string(), 1.0);
    metrics.insert("example_metric_key2".to_string(), 2.0);

   let mut meta_struct = HashMap::new();
    meta_struct.insert("example_meta_struct_key1".to_string(), vec![1, 2, 3]);
    meta_struct.insert("example_meta_struct_key2".to_string(), vec![4, 5, 6]);

    let mut span_links = Vec::new();

    let mut span_link_1_attributes = HashMap::new();
    span_link_1_attributes.insert("example_span_link_1_key1".to_string(), "example_span_link_1_value1".to_string());
    span_link_1_attributes.insert("example_span_link_1_key2".to_string(), "example_span_link_1_value2".to_string());

    let span_link_1 = SourceSpanLink {
        trace_id: 12345,
        trace_id_high: 67890,
        span_id: 11111,
        attributes: span_link_1_attributes,
        tracestate: "example_trace_state".to_string(),
        flags: 0,
    };

   span_links.push(span_link_1);

    let mut span_link_2_attributes = HashMap::new();
    span_link_2_attributes.insert("example_span_link_2_key1".to_string(), "example_span_link_2_value1".to_string());
    span_link_2_attributes.insert("example_span_link_2_key2".to_string(), "example_span_link_2_value2".to_string());

    let span_link_2 = SourceSpanLink {
        trace_id: 54321,
        trace_id_high: 98765,
        span_id: 22222,
        attributes: span_link_2_attributes,
        tracestate: "example_trace_state".to_string(),
        flags: 0,
    };

    span_links.push(span_link_2);

    SourceSpan {
        service: "example_service".to_string(),
        name: "example_name".to_string(),
        resource: "example_resource".to_string(),
        trace_id: 12345,
        span_id: 67890,
        parent_id: 11111,
        start: 1625247600,
        duration: 1500,
        error: 0,
        meta,
        metrics,
        r#type: "example_type".to_string(),
        meta_struct,
        span_links,
    }
}

fn source_span_to_msgpack(span: &Vec<Vec<SourceSpan>>) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    to_vec_named(span)
}
