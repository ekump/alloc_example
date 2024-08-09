use std::collections::HashMap;
use std::borrow::Cow;
use rmp_serde::encode::to_vec_named;
use serde::Serialize;

fn main() {
    println!("Starting app");
    let span = create_source_span();
    let span_2 = create_source_span();

    let spans = vec![vec![span, span_2]];
    let payload = source_span_to_msgpack(&spans).unwrap();
    let mut payload_slice: &[u8] = &payload;
    let decoded_payload = from_slice(&mut payload_slice).unwrap();
    println!("Payload decoded: {:?}", decoded_payload);
    println!("Payload created");
}

fn from_slice<'a>(data: &'a mut &'a [u8]) -> Result<Vec<Span<'a>>, DecodeError> {
    let trace_count = rmp::decode::read_array_len(data).map_err(|_| {
        DecodeError::Generic("Unable to get array len for trace count".to_owned())
    })?;

    println!("Trace count: {}", trace_count);

    let mut traces: Vec<Span> = Default::default();

    for _ in 0..trace_count {
        let span_count = rmp::decode::read_array_len(data).map_err(|_| {
            DecodeError::Generic("Unable to get map len for span size".to_owned())
        })?;

        println!("Span count: {}", span_count);
        let mut trace: Vec<Span> = Default::default();

        for _ in 0..span_count {
            let span = decode_span(data)?;
            trace.push(span);
        }
    }

    Ok(traces)
}

// Note: buf has to be a mutable reference to a reference to a slice of bytes for rmp.
fn decode_span<'a>(buf: &'a mut &'a [u8]) -> Result<Span<'a>, DecodeError> {
    let mut span = Span::default();

    // fill_span(&mut span, &mut buf)?;
    Ok(span)
}

#[derive(Debug, Default)]
struct Span<'a> {
   service:  Cow<'a, str>,
}

#[derive(Serialize)]
struct SourceSpanLink {
    trace_id: u64,
    trace_id_high: u64,
    span_id: u64,
    attributes: HashMap<String, String>,
    trace_state: String,
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
        meta: HashMap::new(),
        metrics: HashMap::new(),
        r#type: "example_type".to_string(),
        meta_struct: HashMap::new(),
        span_links: vec![],
    }
}

fn source_span_to_msgpack(span: &Vec<Vec<SourceSpan>>) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    to_vec_named(span)
}
