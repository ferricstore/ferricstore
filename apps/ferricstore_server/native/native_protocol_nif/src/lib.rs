use rustler::{Binary, Encoder, Env, ListIterator, NifResult, OwnedBinary, Term};

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

#[rustler::nif(schedule = "Normal")]
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

#[rustler::nif(schedule = "Normal")]
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

#[rustler::nif(schedule = "Normal")]
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
    let payload = match build_compact_kv_mget_payload(values) {
        Some(payload) => payload,
        None => return Ok(atoms::nil().encode(env)),
    };

    let frame =
        match build_custom_ok_response_frame(opcode, lane_id, request_id, payload.as_slice()) {
            Some(frame) => frame,
            None => return Ok(atoms::nil().encode(env)),
        };

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

fn build_compact_claim_jobs_response_frame<'a>(
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    jobs: Term<'a>,
) -> Option<OwnedBinary> {
    let mut jobs_iter: ListIterator<'a> = jobs.decode().ok()?;
    let mut payload = Vec::with_capacity(4096);
    payload.push(COMPACT_FLOW_CLAIM_JOBS);
    payload.extend_from_slice(&0u32.to_be_bytes());

    let mut count = 0u32;

    for job in &mut jobs_iter {
        let mut fields: ListIterator<'a> = job.decode().ok()?;
        let id = fields.next()?.decode::<Binary<'a>>().ok()?;
        let partition = fields.next()?.decode::<Option<Binary<'a>>>().ok()?;
        let lease = fields.next()?.decode::<Binary<'a>>().ok()?;
        let fencing = fields.next()?.decode::<i64>().ok()?;

        if fields.next().is_some() {
            return None;
        }

        append_compact_binary(&mut payload, id.as_slice())?;
        append_compact_optional_binary(
            &mut payload,
            partition.as_ref().map(|value| value.as_slice()),
        )?;
        append_compact_binary(&mut payload, lease.as_slice())?;
        payload.extend_from_slice(&fencing.to_be_bytes());
        count = count.checked_add(1)?;
    }

    payload[1..5].copy_from_slice(&count.to_be_bytes());
    build_custom_ok_response_frame(opcode, lane_id, request_id, &payload)
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
    let payload = build_compact_kv_get_payload(value.as_ref().map(|binary| binary.as_slice()))?;

    build_custom_ok_response_frame(opcode, lane_id, request_id, &payload)
}

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

fn build_compact_kv_mget_payload<'a>(values: Term<'a>) -> Option<OwnedBinary> {
    let mut values_iter: ListIterator<'a> = values.decode().ok()?;
    let mut values = Vec::new();

    let mut count = 0u32;
    let mut all_present = true;
    let mut fixed_size: Option<usize> = None;
    let mut total_value_bytes = 0usize;

    for value in &mut values_iter {
        let value = value.decode::<Option<Binary<'a>>>().ok()?;
        count = count.checked_add(1)?;

        match value {
            Some(value) => {
                let size = value.as_slice().len();
                total_value_bytes = total_value_bytes.checked_add(size)?;

                if let Some(existing_size) = fixed_size {
                    if existing_size != size {
                        all_present = false;
                    }
                } else {
                    fixed_size = Some(size);
                }

                values.push(Some(value));
            }
            None => {
                all_present = false;
                values.push(None);
            }
        }
    }

    if all_present {
        let size = fixed_size.unwrap_or(0);
        let size_u32 = u32::try_from(size).ok()?;
        let payload_len = 9usize.checked_add(total_value_bytes)?;
        let mut out = OwnedBinary::new(payload_len)?;
        let out_bytes = out.as_mut_slice();

        out_bytes[0] = COMPACT_KV_MGET_FIXED;
        out_bytes[1..5].copy_from_slice(&count.to_be_bytes());
        out_bytes[5..9].copy_from_slice(&size_u32.to_be_bytes());

        let mut offset = 9usize;
        for value in &values {
            let value = value.as_ref()?;
            let value_bytes = value.as_slice();
            let end = offset.checked_add(size)?;
            out_bytes[offset..end].copy_from_slice(value_bytes);
            offset = end;
        }

        return Some(out);
    }

    let payload_len = values.iter().try_fold(5usize, |acc, value| {
        let acc = acc.checked_add(1)?;
        match value {
            Some(value) => acc.checked_add(4)?.checked_add(value.as_slice().len()),
            None => Some(acc),
        }
    })?;

    let mut payload = Vec::with_capacity(payload_len);
    payload.push(COMPACT_KV_MGET);
    payload.extend_from_slice(&count.to_be_bytes());

    for value in values {
        match value {
            Some(value) => {
                payload.push(1);
                append_compact_binary(&mut payload, value.as_slice())?;
            }
            None => payload.push(0),
        }
    }

    let mut out = OwnedBinary::new(payload.len())?;
    out.as_mut_slice().copy_from_slice(&payload);
    Some(out)
}

fn is_ok_binary(value: &[u8]) -> bool {
    value.len() == 2
        && (value[0] == b'O' || value[0] == b'o')
        && (value[1] == b'K' || value[1] == b'k')
}

fn append_compact_binary(out: &mut Vec<u8>, value: &[u8]) -> Option<()> {
    let len = u32::try_from(value.len()).ok()?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Some(())
}

fn append_compact_optional_binary(out: &mut Vec<u8>, value: Option<&[u8]>) -> Option<()> {
    match value {
        Some(value) => append_compact_binary(out, value),
        None => {
            out.extend_from_slice(&u32::MAX.to_be_bytes());
            Some(())
        }
    }
}

fn build_custom_ok_response_frame(
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    payload: &[u8],
) -> Option<OwnedBinary> {
    let body_len = 2usize.checked_add(payload.len())?;
    if body_len > MAX_FRAME_BODY_BYTES {
        return None;
    }
    let mut out = OwnedBinary::new(HEADER_SIZE + body_len)?;
    write_custom_ok_response_frame(
        out.as_mut_slice(),
        opcode,
        lane_id,
        request_id,
        body_len,
        payload,
    );

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

fn write_custom_ok_response_frame(
    bytes: &mut [u8],
    opcode: u16,
    lane_id: u32,
    request_id: u64,
    body_len: usize,
    payload: &[u8],
) {
    bytes[0..4].copy_from_slice(MAGIC);
    bytes[4] = VERSION | RESPONSE_DIRECTION;
    bytes[5] = FLAG_CUSTOM_PAYLOAD;
    bytes[6..10].copy_from_slice(&lane_id.to_be_bytes());
    bytes[10..12].copy_from_slice(&opcode.to_be_bytes());
    bytes[12..20].copy_from_slice(&request_id.to_be_bytes());
    bytes[20..24].copy_from_slice(&(body_len as u32).to_be_bytes());
    bytes[24..26].copy_from_slice(&STATUS_OK.to_be_bytes());
    bytes[26..].copy_from_slice(payload);
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
        done
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
}
