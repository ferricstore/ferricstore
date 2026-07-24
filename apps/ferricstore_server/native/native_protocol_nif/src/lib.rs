use rustler::{Binary, Encoder, Env, ListIterator, NifResult, OwnedBinary, Term};

mod fql;

const MAGIC: &[u8; 4] = b"FSNP";
const VERSION: u8 = 1;
const RESPONSE_DIRECTION: u8 = 0x80;
const HEADER_SIZE: usize = 24;
const FLAG_CUSTOM_PAYLOAD: u8 = 0x02;
const STATUS_OK: u16 = 0;
const COMPACT_FLOW_CLAIM_JOBS: u8 = 0x80;
const COMPACT_OK_LIST: u8 = 0x81;
const COMPACT_KV_GET: u8 = 0x82;
const COMPACT_KV_MGET: u8 = 0x83;
const COMPACT_KV_MGET_FIXED: u8 = 0x89;
const MAX_FRAMES_PER_DECODE: usize = 128;
const MAX_FRAME_BODY_BYTES: usize = 128 * 1024 * 1024 - HEADER_SIZE;

#[derive(Debug, PartialEq, Eq)]
struct FrameSlice<'a> {
    lane_id: u32,
    opcode: u16,
    request_id: u64,
    flags: u8,
    body: &'a [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactClaimMode {
    Base,
    State,
}

#[rustler::nif]
fn parse_fql<'a>(env: Env<'a>, query: Binary<'a>) -> NifResult<Term<'a>> {
    match fql::parse_with_diagnostic(query.as_slice()) {
        Ok(parsed) => {
            let mode = match parsed.mode {
                fql::Mode::Execute => atoms::execute(),
                fql::Mode::Explain => atoms::explain(),
                fql::Mode::Analyze => atoms::analyze(),
            };
            let source = match parsed.source {
                fql::Source::Runs => atoms::runs(),
                fql::Source::Events => atoms::events(),
            };
            let shape = match parsed.shape {
                fql::Shape::Point => atoms::point(),
                fql::Shape::Collection => atoms::collection(),
                fql::Shape::History => atoms::history(),
                fql::Shape::Count => atoms::count(),
            };
            let predicates = parsed
                .predicates
                .into_iter()
                .map(|predicate| encode_fql_predicate(env, predicate))
                .collect::<NifResult<Vec<_>>>()?;
            let order_by = parsed
                .order_by
                .into_iter()
                .map(|order| encode_fql_order(env, order))
                .collect::<NifResult<Vec<_>>>()?;
            let cursor = match parsed.cursor {
                Some(value) => encode_fql_value(env, value)?,
                None => atoms::nil().encode(env),
            };
            let limit = match parsed.limit {
                Some(limit) => (limit as u64).encode(env),
                None => atoms::nil().encode(env),
            };
            let projection = match parsed.projection {
                Some(fields) => fields
                    .into_iter()
                    .map(|field| encode_fql_binary(env, field.external_name))
                    .collect::<NifResult<Vec<_>>>()?
                    .encode(env),
                None => atoms::nil().encode(env),
            };

            Ok(rustler::types::tuple::make_tuple(
                env,
                &[
                    atoms::ok().encode(env),
                    mode.encode(env),
                    source.encode(env),
                    shape.encode(env),
                    predicates.encode(env),
                    order_by.encode(env),
                    limit,
                    cursor,
                    projection,
                ],
            ))
        }
        Err(failure) => {
            let reason = match failure.reason {
                fql::ParseError::DuplicateProjectionField => atoms::duplicate_projection_field(),
                fql::ParseError::InvalidParameterType => atoms::invalid_parameter_type(),
                fql::ParseError::InvalidSyntax => atoms::invalid_syntax(),
                fql::ParseError::QueryCursorInvalid => atoms::query_cursor_invalid(),
                fql::ParseError::QueryProjectionLimitExceeded => {
                    atoms::query_projection_limit_exceeded()
                }
                fql::ParseError::QueryTooLarge => atoms::query_too_large(),
                fql::ParseError::UnsupportedField => atoms::unsupported_field(),
                fql::ParseError::UnsupportedQueryShape => atoms::unsupported_query_shape(),
                fql::ParseError::UnsupportedSource => atoms::unsupported_source(),
            };

            Ok((atoms::error(), reason, failure.byte as u64).encode(env))
        }
    }
}

fn encode_fql_predicate<'a>(env: Env<'a>, predicate: fql::Predicate<'_>) -> NifResult<Term<'a>> {
    match predicate {
        fql::Predicate::Eq(field, value) => Ok((
            atoms::eq(),
            encode_fql_field(env, field)?,
            encode_fql_value(env, value)?,
        )
            .encode(env)),
        fql::Predicate::In(field, values) => {
            let values = values
                .into_iter()
                .map(|value| encode_fql_value(env, value))
                .collect::<NifResult<Vec<_>>>()?;
            Ok((atoms::in_operator(), encode_fql_field(env, field)?, values).encode(env))
        }
        fql::Predicate::Range(field, lower, upper) => Ok((
            atoms::range(),
            encode_fql_field(env, field)?,
            encode_fql_value(env, lower)?,
            encode_fql_value(env, upper)?,
        )
            .encode(env)),
        fql::Predicate::TimeWindow(field, lower, upper) => Ok((
            atoms::time_window(),
            encode_fql_field(env, field)?,
            encode_fql_value(env, lower)?,
            encode_fql_value(env, upper)?,
        )
            .encode(env)),
        fql::Predicate::IsNull(field) => Ok((
            atoms::is_operator(),
            encode_fql_field(env, field)?,
            atoms::null(),
        )
            .encode(env)),
        fql::Predicate::IsMissing(field) => Ok((
            atoms::is_operator(),
            encode_fql_field(env, field)?,
            atoms::missing(),
        )
            .encode(env)),
    }
}

fn encode_fql_order<'a>(env: Env<'a>, order: fql::Order<'_>) -> NifResult<Term<'a>> {
    let direction = match order.direction {
        fql::Direction::Asc => atoms::asc(),
        fql::Direction::Desc => atoms::desc(),
    };
    Ok((encode_fql_field(env, order.field)?, direction).encode(env))
}

fn encode_fql_field<'a>(env: Env<'a>, field: fql::Field<'_>) -> NifResult<Binary<'a>> {
    encode_fql_binary(env, field.external_name)
}

fn encode_fql_value<'a>(env: Env<'a>, value: fql::QueryValue<'_>) -> NifResult<Term<'a>> {
    match value {
        fql::QueryValue::LiteralKeyword(value) => Ok((
            atoms::literal(),
            atoms::keyword(),
            encode_fql_binary(env, value)?,
        )
            .encode(env)),
        fql::QueryValue::LiteralInteger(value) => {
            Ok((atoms::literal(), atoms::integer(), value).encode(env))
        }
        fql::QueryValue::Parameter(value_type, name) => {
            let value_type = match value_type {
                fql::ValueType::Keyword => atoms::keyword(),
                fql::ValueType::Integer => atoms::integer(),
                fql::ValueType::Dynamic => atoms::dynamic(),
            };
            Ok((
                atoms::parameter(),
                value_type,
                encode_fql_binary(env, name)?,
            )
                .encode(env))
        }
    }
}

fn encode_fql_binary<'a>(env: Env<'a>, value: impl AsRef<[u8]>) -> NifResult<Binary<'a>> {
    let value = value.as_ref();
    let mut binary = OwnedBinary::new(value.len())
        .ok_or_else(|| rustler::Error::Term(Box::new("FQL value allocation failed")))?;
    binary.as_mut_slice().copy_from_slice(value);

    Ok(Binary::from_owned(binary, env))
}

// One pass may still copy one configured max-size frame, which can exceed a normal scheduler slice.
#[rustler::nif(schedule = "DirtyCpu")]
fn decode_frames<'a>(
    env: Env<'a>,
    buffer: Binary<'a>,
    max_frame_bytes: u32,
) -> NifResult<Term<'a>> {
    let bytes = buffer.as_slice();
    let (frame_slices, offset, has_more) = match scan_frames(bytes, max_frame_bytes) {
        Ok(result) => result,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    let mut frames: Vec<Term<'a>> = Vec::with_capacity(frame_slices.len());

    for frame in frame_slices {
        let mut body = OwnedBinary::new(frame.body.len())
            .ok_or_else(|| rustler::Error::Term(Box::new("native body allocation failed")))?;
        body.as_mut_slice().copy_from_slice(frame.body);
        let body_term = Binary::from_owned(body, env);

        frames.push(
            (
                frame.lane_id as u64,
                frame.opcode as u64,
                frame.request_id,
                frame.flags as u64,
                body_term,
            )
                .encode(env),
        );
    }

    let rest_len = bytes.len() - offset;

    let rest_term = if has_more {
        buffer.make_subbinary(offset, rest_len)?
    } else {
        let rest_bytes = &bytes[offset..];
        let mut rest = OwnedBinary::new(rest_len)
            .ok_or_else(|| rustler::Error::Term(Box::new("native rest allocation failed")))?;
        rest.as_mut_slice().copy_from_slice(rest_bytes);
        Binary::from_owned(rest, env)
    };

    let continuation = if has_more {
        atoms::more()
    } else {
        atoms::done()
    };

    Ok((atoms::ok(), frames, rest_term, continuation).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn encode_frame<'a>(
    env: Env<'a>,
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    body: Binary<'a>,
    flags: u8,
    response: bool,
) -> NifResult<Term<'a>> {
    let body = body.as_slice();
    let body_len = validate_frame_body_len(body.len())?;
    let len = HEADER_SIZE + body.len();
    let mut out = OwnedBinary::new(len)
        .ok_or_else(|| rustler::Error::Term(Box::new("native frame allocation failed")))?;
    let bytes = out.as_mut_slice();

    bytes[0..4].copy_from_slice(MAGIC);
    bytes[4] = if response {
        VERSION | RESPONSE_DIRECTION
    } else {
        VERSION
    };
    bytes[5] = flags;
    bytes[6..10].copy_from_slice(&lane_id.to_be_bytes());
    bytes[10..12].copy_from_slice(&opcode.to_be_bytes());
    bytes[12..20].copy_from_slice(&request_id.to_be_bytes());
    bytes[20..24].copy_from_slice(&body_len.to_be_bytes());
    bytes[24..].copy_from_slice(body);

    Ok(Binary::from_owned(out, env).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn encode_compact_claim_jobs_response_frame<'a>(
    env: Env<'a>,
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    jobs: Term<'a>,
) -> NifResult<Term<'a>> {
    let frame = match build_compact_claim_jobs_response_frame(opcode, lane_id, request_id, jobs) {
        Some(frame) => frame,
        None => return Ok(atoms::nil().encode(env)),
    };

    Ok(Binary::from_owned(frame, env).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn encode_compact_ok_list_response_frame<'a>(
    env: Env<'a>,
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    values: Term<'a>,
) -> NifResult<Term<'a>> {
    let frame = match build_compact_ok_list_response_frame(opcode, lane_id, request_id, values) {
        Some(frame) => frame,
        None => return Ok(atoms::nil().encode(env)),
    };

    Ok(Binary::from_owned(frame, env).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn encode_compact_kv_get_response_frame<'a>(
    env: Env<'a>,
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    value: Term<'a>,
) -> NifResult<Term<'a>> {
    let frame = match build_compact_kv_get_response_frame(opcode, lane_id, request_id, value) {
        Some(frame) => frame,
        None => return Ok(atoms::nil().encode(env)),
    };

    Ok(Binary::from_owned(frame, env).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn encode_compact_kv_mget_response_frame<'a>(
    env: Env<'a>,
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    values: Term<'a>,
) -> NifResult<Term<'a>> {
    let layout = match compact_kv_mget_layout(values) {
        Some(layout) => layout,
        None => return Ok(atoms::nil().encode(env)),
    };
    let mut frame =
        match allocate_custom_ok_response_frame(opcode, lane_id, request_id, layout.payload_len) {
            Some(frame) => frame,
            None => return Ok(atoms::nil().encode(env)),
        };
    if write_compact_kv_mget_payload(&mut frame.as_mut_slice()[HEADER_SIZE + 2..], values, layout)
        .is_none()
    {
        return Ok(atoms::nil().encode(env));
    }

    Ok(Binary::from_owned(frame, env).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn encode_compact_kv_mget<'a>(env: Env<'a>, values: Term<'a>) -> NifResult<Term<'a>> {
    let payload = match build_compact_kv_mget_payload(values) {
        Some(payload) => payload,
        None => return Ok(atoms::nil().encode(env)),
    };

    Ok(Binary::from_owned(payload, env).encode(env))
}

fn scan_frames(
    bytes: &[u8],
    max_frame_bytes: u32,
) -> Result<(Vec<FrameSlice<'_>>, usize, bool), String> {
    let mut offset = 0usize;
    let mut frames = Vec::new();
    let mut decoded_bytes = 0usize;
    let max_decode_bytes = max_frame_bytes as usize + HEADER_SIZE;

    while bytes.len().saturating_sub(offset) >= HEADER_SIZE {
        if frames.len() >= MAX_FRAMES_PER_DECODE {
            return Ok((frames, offset, true));
        }

        if &bytes[offset..offset + 4] != MAGIC {
            return Err("ERR native invalid frame magic".to_string());
        }

        let version_byte = bytes[offset + 4];
        let direction = version_byte & RESPONSE_DIRECTION;
        let version = version_byte & 0x7f;

        if version != VERSION {
            return Err(format!("ERR native unsupported protocol version {version}"));
        }

        if direction != 0 {
            return Err("ERR native client frame cannot use response direction".to_string());
        }

        let flags = bytes[offset + 5];
        let lane_id = read_u32(bytes, offset + 6);
        let opcode = read_u16(bytes, offset + 10);
        let request_id = read_u64(bytes, offset + 12);
        let body_len = read_u32(bytes, offset + 20) as usize;

        if body_len > max_frame_bytes as usize {
            return Err("ERR native frame exceeds max_frame_bytes".to_string());
        }

        let frame_size = HEADER_SIZE + body_len;
        let frame_end = offset + frame_size;

        if bytes.len() < frame_end {
            break;
        }

        if decoded_bytes + frame_size > max_decode_bytes {
            return Ok((frames, offset, true));
        }

        frames.push(FrameSlice {
            lane_id,
            opcode,
            request_id,
            flags,
            body: &bytes[offset + HEADER_SIZE..frame_end],
        });

        offset = frame_end;
        decoded_bytes += frame_size;
    }

    Ok((frames, offset, false))
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

struct CompactSliceWriter<'a> {
    bytes: &'a mut [u8],
    offset: usize,
}

impl<'a> CompactSliceWriter<'a> {
    fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn write(&mut self, value: &[u8]) -> Option<()> {
        let end = self.offset.checked_add(value.len())?;
        self.bytes.get_mut(self.offset..end)?.copy_from_slice(value);
        self.offset = end;
        Some(())
    }

    fn byte(&mut self, value: u8) -> Option<()> {
        self.write(&[value])
    }

    fn binary(&mut self, value: &[u8]) -> Option<()> {
        let len = u32::try_from(value.len()).ok()?;
        self.write(&len.to_be_bytes())?;
        self.write(value)
    }

    fn optional_binary(&mut self, value: Option<&[u8]>) -> Option<()> {
        match value {
            Some(value) => self.binary(value),
            None => self.write(&u32::MAX.to_be_bytes()),
        }
    }

    fn finish(self) -> Option<()> {
        (self.offset == self.bytes.len()).then_some(())
    }
}

struct CompactClaimJob<'a> {
    id: Binary<'a>,
    partition: Option<Binary<'a>>,
    lease: Binary<'a>,
    fencing: i64,
    run_state: Option<Option<Binary<'a>>>,
}

impl CompactClaimJob<'_> {
    fn mode(&self) -> CompactClaimMode {
        if self.run_state.is_some() {
            CompactClaimMode::State
        } else {
            CompactClaimMode::Base
        }
    }

    fn encoded_len(&self) -> Option<usize> {
        let mut len = compact_binary_len(self.id.len())?;
        len = len.checked_add(compact_optional_binary_len(
            self.partition.as_ref().map(|value| value.len()),
        )?)?;
        len = len.checked_add(compact_binary_len(self.lease.len())?)?;
        len = len.checked_add(8)?;
        if let Some(run_state) = self.run_state.as_ref() {
            len = len.checked_add(compact_optional_binary_len(
                run_state.as_ref().map(|value| value.len()),
            )?)?;
        }
        Some(len)
    }

    fn write(&self, writer: &mut CompactSliceWriter<'_>) -> Option<()> {
        writer.binary(self.id.as_slice())?;
        writer.optional_binary(self.partition.as_ref().map(Binary::as_slice))?;
        writer.binary(self.lease.as_slice())?;
        writer.write(&self.fencing.to_be_bytes())?;
        if let Some(run_state) = self.run_state.as_ref() {
            writer.optional_binary(run_state.as_ref().map(Binary::as_slice))?;
        }
        Some(())
    }
}

fn compact_binary_len(len: usize) -> Option<usize> {
    u32::try_from(len).ok()?;
    4usize.checked_add(len)
}

fn compact_optional_binary_len(len: Option<usize>) -> Option<usize> {
    match len {
        Some(len) => compact_binary_len(len),
        None => Some(4),
    }
}

fn decode_compact_claim_job<'a>(job: Term<'a>) -> Option<CompactClaimJob<'a>> {
    let mut fields: ListIterator<'a> = job.decode().ok()?;
    let id = fields.next()?.decode::<Binary<'a>>().ok()?;
    let partition = fields.next()?.decode::<Option<Binary<'a>>>().ok()?;
    let lease = fields.next()?.decode::<Binary<'a>>().ok()?;
    let fencing = fields.next()?.decode::<i64>().ok()?;
    let run_state = match fields.next() {
        Some(value) => Some(value.decode::<Option<Binary<'a>>>().ok()?),
        None => None,
    };
    if fields.next().is_some() {
        return None;
    }
    Some(CompactClaimJob {
        id,
        partition,
        lease,
        fencing,
        run_state,
    })
}

fn build_compact_claim_jobs_response_frame<'a>(
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    jobs: Term<'a>,
) -> Option<OwnedBinary> {
    let mut jobs_iter: ListIterator<'a> = jobs.decode().ok()?;
    let mut payload_len = 5usize;
    let mut count = 0u32;
    let mut mode: Option<CompactClaimMode> = None;

    for job in &mut jobs_iter {
        let job = decode_compact_claim_job(job)?;
        let job_mode = job.mode();
        match mode {
            Some(expected) if expected != job_mode => return None,
            None => mode = Some(job_mode),
            _ => {}
        }
        payload_len = payload_len.checked_add(job.encoded_len()?)?;
        count = count.checked_add(1)?;
    }

    let mut frame = allocate_custom_ok_response_frame(opcode, lane_id, request_id, payload_len)?;
    let mut writer = CompactSliceWriter::new(&mut frame.as_mut_slice()[HEADER_SIZE + 2..]);
    writer.byte(COMPACT_FLOW_CLAIM_JOBS)?;
    writer.write(&count.to_be_bytes())?;
    let mut jobs_iter: ListIterator<'a> = jobs.decode().ok()?;
    for job in &mut jobs_iter {
        decode_compact_claim_job(job)?.write(&mut writer)?;
    }
    writer.finish()?;
    Some(frame)
}

fn build_compact_ok_list_response_frame<'a>(
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    values: Term<'a>,
) -> Option<OwnedBinary> {
    let mut values_iter: ListIterator<'a> = values.decode().ok()?;
    let mut count = 0u32;

    for value in &mut values_iter {
        let value = value.decode::<Binary<'a>>().ok()?;

        if !is_ok_binary(value.as_slice()) {
            return None;
        }

        count = count.checked_add(1)?;
    }

    let mut payload = [0u8; 5];
    payload[0] = COMPACT_OK_LIST;
    payload[1..5].copy_from_slice(&count.to_be_bytes());
    build_custom_ok_response_frame(opcode, lane_id, request_id, &payload)
}

fn build_compact_kv_get_response_frame<'a>(
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    value: Term<'a>,
) -> Option<OwnedBinary> {
    let value = value.decode::<Option<Binary<'a>>>().ok()?;
    let payload_len = match value.as_ref() {
        Some(value) => 2usize.checked_add(compact_binary_len(value.len())?)?,
        None => 2,
    };
    let mut frame = allocate_custom_ok_response_frame(opcode, lane_id, request_id, payload_len)?;
    let mut writer = CompactSliceWriter::new(&mut frame.as_mut_slice()[HEADER_SIZE + 2..]);
    writer.byte(COMPACT_KV_GET)?;
    match value.as_ref() {
        Some(value) => {
            writer.byte(1)?;
            writer.binary(value.as_slice())?;
        }
        None => writer.byte(0)?,
    }
    writer.finish()?;
    Some(frame)
}

#[cfg(test)]
fn build_compact_kv_get_payload(value: Option<&[u8]>) -> Option<Vec<u8>> {
    let mut payload = Vec::with_capacity(32);
    payload.push(COMPACT_KV_GET);

    match value {
        Some(value) => {
            payload.push(1);
            append_compact_binary(&mut payload, value)?;
        }
        None => payload.push(0),
    }

    Some(payload)
}

#[derive(Clone, Copy)]
enum CompactMgetEncoding {
    Fixed { value_size: u32 },
    Variable,
}

#[derive(Clone, Copy)]
struct CompactMgetLayout {
    count: u32,
    payload_len: usize,
    encoding: CompactMgetEncoding,
}

fn compact_kv_mget_layout<'a>(values: Term<'a>) -> Option<CompactMgetLayout> {
    let mut values_iter: ListIterator<'a> = values.decode().ok()?;
    let mut count = 0u32;
    let mut all_present = true;
    let mut fixed_size: Option<usize> = None;
    let mut total_value_bytes = 0usize;
    let mut variable_len = 5usize;

    for value in &mut values_iter {
        let value = value.decode::<Option<Binary<'a>>>().ok()?;
        count = count.checked_add(1)?;
        variable_len = variable_len.checked_add(1)?;

        match value {
            Some(value) => {
                let size = value.len();
                u32::try_from(size).ok()?;
                total_value_bytes = total_value_bytes.checked_add(size)?;
                variable_len = variable_len.checked_add(4)?.checked_add(size)?;

                if let Some(existing_size) = fixed_size {
                    if existing_size != size {
                        all_present = false;
                    }
                } else {
                    fixed_size = Some(size);
                }
            }
            None => {
                all_present = false;
            }
        }
    }

    if all_present {
        let size = fixed_size.unwrap_or(0);
        return Some(CompactMgetLayout {
            count,
            payload_len: 9usize.checked_add(total_value_bytes)?,
            encoding: CompactMgetEncoding::Fixed {
                value_size: u32::try_from(size).ok()?,
            },
        });
    }

    Some(CompactMgetLayout {
        count,
        payload_len: variable_len,
        encoding: CompactMgetEncoding::Variable,
    })
}

fn write_compact_kv_mget_payload<'a>(
    output: &mut [u8],
    values: Term<'a>,
    layout: CompactMgetLayout,
) -> Option<()> {
    let mut writer = CompactSliceWriter::new(output);
    let mut values_iter: ListIterator<'a> = values.decode().ok()?;

    match layout.encoding {
        CompactMgetEncoding::Fixed { value_size } => {
            writer.byte(COMPACT_KV_MGET_FIXED)?;
            writer.write(&layout.count.to_be_bytes())?;
            writer.write(&value_size.to_be_bytes())?;
            for value in &mut values_iter {
                let value = value.decode::<Option<Binary<'a>>>().ok()??;
                if value.len() != value_size as usize {
                    return None;
                }
                writer.write(value.as_slice())?;
            }
        }
        CompactMgetEncoding::Variable => {
            writer.byte(COMPACT_KV_MGET)?;
            writer.write(&layout.count.to_be_bytes())?;
            for value in &mut values_iter {
                match value.decode::<Option<Binary<'a>>>().ok()? {
                    Some(value) => {
                        writer.byte(1)?;
                        writer.binary(value.as_slice())?;
                    }
                    None => writer.byte(0)?,
                }
            }
        }
    }

    writer.finish()
}

fn build_compact_kv_mget_payload<'a>(values: Term<'a>) -> Option<OwnedBinary> {
    let layout = compact_kv_mget_layout(values)?;
    let mut out = OwnedBinary::new(layout.payload_len)?;
    write_compact_kv_mget_payload(out.as_mut_slice(), values, layout)?;
    Some(out)
}

fn is_ok_binary(value: &[u8]) -> bool {
    value.len() == 2
        && (value[0] == b'O' || value[0] == b'o')
        && (value[1] == b'K' || value[1] == b'k')
}

#[cfg(test)]
fn append_compact_binary(out: &mut Vec<u8>, value: &[u8]) -> Option<()> {
    let len = u32::try_from(value.len()).ok()?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Some(())
}

fn build_custom_ok_response_frame(
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    payload: &[u8],
) -> Option<OwnedBinary> {
    let mut out = allocate_custom_ok_response_frame(opcode, lane_id, request_id, payload.len())?;
    out.as_mut_slice()[HEADER_SIZE + 2..].copy_from_slice(payload);
    Some(out)
}

fn allocate_custom_ok_response_frame(
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    payload_len: usize,
) -> Option<OwnedBinary> {
    let body_len = 2usize.checked_add(payload_len)?;
    if body_len > MAX_FRAME_BODY_BYTES {
        return None;
    }
    let frame_len = HEADER_SIZE.checked_add(body_len)?;
    let mut out = OwnedBinary::new(frame_len)?;
    write_custom_ok_response_header(out.as_mut_slice(), opcode, lane_id, request_id, body_len);
    Some(out)
}

fn validate_frame_body_len(body_len: usize) -> NifResult<u32> {
    if body_len > MAX_FRAME_BODY_BYTES {
        return Err(rustler::Error::Term(Box::new(
            "native frame body exceeds protocol limit",
        )));
    }

    u32::try_from(body_len)
        .map_err(|_| rustler::Error::Term(Box::new("native frame body exceeds u32 length")))
}

#[cfg(test)]
fn write_custom_ok_response_frame(
    bytes: &mut [u8],
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    body_len: usize,
    payload: &[u8],
) {
    write_custom_ok_response_header(bytes, opcode, lane_id, request_id, body_len);
    bytes[HEADER_SIZE + 2..].copy_from_slice(payload);
}

fn write_custom_ok_response_header(
    bytes: &mut [u8],
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    body_len: usize,
) {
    bytes[0..4].copy_from_slice(MAGIC);
    bytes[4] = VERSION | RESPONSE_DIRECTION;
    bytes[5] = FLAG_CUSTOM_PAYLOAD;
    bytes[6..10].copy_from_slice(&lane_id.to_be_bytes());
    bytes[10..12].copy_from_slice(&opcode.to_be_bytes());
    bytes[12..20].copy_from_slice(&request_id.to_be_bytes());
    bytes[20..24].copy_from_slice(&(body_len as u32).to_be_bytes());
    bytes[24..26].copy_from_slice(&STATUS_OK.to_be_bytes());
}

#[cfg(test)]
fn build_custom_ok_response_bytes(
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    payload: &[u8],
) -> Option<Vec<u8>> {
    let body_len = 2usize.checked_add(payload.len())?;
    let mut bytes = vec![0u8; HEADER_SIZE + body_len];
    write_custom_ok_response_frame(&mut bytes, opcode, lane_id, request_id, body_len, payload);
    Some(bytes)
}

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        more,
        done,
        execute,
        explain,
        analyze,
        point,
        collection,
        history,
        count,
        runs,
        events,
        literal,
        parameter,
        keyword,
        integer,
        dynamic,
        duplicate_projection_field,
        eq,
        in_operator = "in",
        range,
        time_window,
        is_operator = "is",
        null,
        missing,
        asc,
        desc,
        invalid_parameter_type,
        invalid_syntax,
        query_cursor_invalid,
        query_projection_limit_exceeded,
        query_too_large,
        unsupported_field,
        unsupported_query_shape,
        unsupported_source
    }
}

rustler::init!("Elixir.FerricstoreServer.Native.NIF");

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(opcode: u16, lane_id: u32, request_id: u64, flags: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_SIZE + body.len());
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.push(flags);
        bytes.extend_from_slice(&lane_id.to_be_bytes());
        bytes.extend_from_slice(&opcode.to_be_bytes());
        bytes.extend_from_slice(&request_id.to_be_bytes());
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes
    }

    #[test]
    fn scan_frames_returns_frames_and_partial_remainder_offset() {
        let first_body = b"abc";
        let second_body = b"defg";
        let first = frame(0x0102, 7, 42, 0x10, first_body);
        let second = frame(0x0101, 8, 43, 0, second_body);
        let partial_second_len = second.len() - 2;
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second[..partial_second_len]);

        let (frames, offset, has_more) = scan_frames(&bytes, 1024).expect("scan succeeds");

        assert_eq!(offset, first.len());
        assert!(!has_more);
        assert_eq!(
            frames,
            vec![FrameSlice {
                lane_id: 7,
                opcode: 0x0102,
                request_id: 42,
                flags: 0x10,
                body: first_body
            }]
        );
    }

    #[test]
    fn scan_frames_bounds_tiny_frame_batches_and_preserves_the_continuation() {
        let frames: Vec<Vec<u8>> = (1..=129)
            .map(|request_id| frame(0x0003, 0, request_id, 0, b""))
            .collect();
        let bytes: Vec<u8> = frames.iter().flatten().copied().collect();

        let (decoded, offset, has_more) =
            scan_frames(&bytes, 16 * 1024 * 1024).expect("scan succeeds");

        assert_eq!(decoded.len(), 128);
        assert_eq!(offset, frames[..128].iter().map(Vec::len).sum());
        assert_eq!(&bytes[offset..], frames[128].as_slice());
        assert!(has_more);
    }

    #[test]
    fn scan_frames_bounds_total_wire_bytes_per_decode() {
        let first = frame(0x0102, 7, 1, 0, &[1; 700]);
        let second = frame(0x0102, 7, 2, 0, &[2; 700]);
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second);

        let (decoded, offset, has_more) = scan_frames(&bytes, 1024).expect("scan succeeds");

        assert_eq!(decoded.len(), 1);
        assert_eq!(offset, first.len());
        assert!(has_more);

        let (decoded, offset, has_more) =
            scan_frames(&bytes[offset..], 1024).expect("continuation succeeds");

        assert_eq!(decoded.len(), 1);
        assert_eq!(offset, second.len());
        assert!(!has_more);
    }

    #[test]
    fn scan_frames_rejects_response_direction() {
        let mut bytes = frame(0x0101, 1, 1, 0, b"");
        bytes[4] = VERSION | RESPONSE_DIRECTION;

        assert_eq!(
            scan_frames(&bytes, 1024),
            Err("ERR native client frame cannot use response direction".to_string())
        );
    }

    #[test]
    fn scan_frames_rejects_too_large_body_before_waiting_for_rest() {
        let bytes = frame(0x0101, 1, 1, 0, b"abc");

        assert_eq!(
            scan_frames(&bytes, 2),
            Err("ERR native frame exceeds max_frame_bytes".to_string())
        );
    }

    #[test]
    fn encoder_body_length_rejects_values_above_the_protocol_limit() {
        assert_eq!(validate_frame_body_len(0).expect("zero-length body"), 0);
        assert_eq!(
            validate_frame_body_len(MAX_FRAME_BODY_BYTES).expect("maximum body"),
            MAX_FRAME_BODY_BYTES as u32
        );
        assert!(validate_frame_body_len(MAX_FRAME_BODY_BYTES + 1).is_err());
        assert!(validate_frame_body_len(u32::MAX as usize + 1).is_err());
    }

    #[test]
    fn custom_ok_response_frame_wraps_payload_in_one_buffer() {
        let frame =
            build_custom_ok_response_bytes(0x0203, 2, 99, &[COMPACT_FLOW_CLAIM_JOBS, 0, 0, 0, 0])
                .expect("frame allocation succeeds");
        let bytes = frame.as_slice();

        assert_eq!(&bytes[0..4], MAGIC);
        assert_eq!(bytes[4], VERSION | RESPONSE_DIRECTION);
        assert_eq!(bytes[5], FLAG_CUSTOM_PAYLOAD);
        assert_eq!(&bytes[6..10], &2u32.to_be_bytes());
        assert_eq!(&bytes[10..12], &0x0203u16.to_be_bytes());
        assert_eq!(&bytes[12..20], &99u64.to_be_bytes());
        assert_eq!(&bytes[20..24], &7u32.to_be_bytes());
        assert_eq!(&bytes[24..26], &STATUS_OK.to_be_bytes());
        assert_eq!(&bytes[26..31], &[COMPACT_FLOW_CLAIM_JOBS, 0, 0, 0, 0]);
    }

    #[test]
    fn compact_kv_get_response_frame_wraps_payload() {
        let payload = build_compact_kv_get_payload(Some(b"value")).expect("payload builds");
        let frame = build_custom_ok_response_bytes(0x0101, 2, 99, &payload)
            .expect("frame allocation succeeds");
        let bytes = frame.as_slice();

        assert_eq!(&bytes[0..4], MAGIC);
        assert_eq!(bytes[4], VERSION | RESPONSE_DIRECTION);
        assert_eq!(bytes[5], FLAG_CUSTOM_PAYLOAD);
        assert_eq!(&bytes[10..12], &0x0101u16.to_be_bytes());
        assert_eq!(&bytes[12..20], &99u64.to_be_bytes());
        assert_eq!(&bytes[24..26], &STATUS_OK.to_be_bytes());
        assert_eq!(bytes[26], COMPACT_KV_GET);
        assert_eq!(bytes[27], 1);
        assert_eq!(&bytes[28..32], &5u32.to_be_bytes());
        assert_eq!(&bytes[32..37], b"value");
    }

    #[test]
    fn compact_response_builders_do_not_materialize_payload_sized_temporaries() {
        let source = include_str!("lib.rs");
        let claim = source
            .split("fn build_compact_claim_jobs_response_frame")
            .nth(1)
            .unwrap()
            .split("fn build_compact_ok_list_response_frame")
            .next()
            .unwrap();
        let get = source
            .split("fn build_compact_kv_get_response_frame")
            .nth(1)
            .unwrap()
            .split("fn build_compact_kv_get_payload")
            .next()
            .unwrap();
        let framed_mget = source
            .split("fn encode_compact_kv_mget_response_frame")
            .nth(1)
            .unwrap()
            .split("fn encode_compact_kv_mget")
            .next()
            .unwrap();

        assert!(!claim.contains("Vec::with_capacity"));
        assert!(!claim.contains("build_custom_ok_response_frame("));
        assert!(!get.contains("build_compact_kv_get_payload"));
        assert!(!get.contains("build_custom_ok_response_frame("));
        assert!(!framed_mget.contains("build_compact_kv_mget_payload"));
        assert!(!framed_mget.contains("build_custom_ok_response_frame("));
    }
}
