use rustler::{Binary, Encoder, Env, ListIterator, NifResult, OwnedBinary, Term};

const MAGIC: &[u8; 4] = b"FSNP";
const VERSION: u8 = 1;
const RESPONSE_DIRECTION: u8 = 0x80;
const HEADER_SIZE: usize = 24;
const FLAG_CUSTOM_PAYLOAD: u8 = 0x02;
const STATUS_OK: u16 = 0;
const COMPACT_FLOW_CLAIM_JOBS: u8 = 0x80;
const COMPACT_OK_LIST: u8 = 0x81;

#[derive(Debug, PartialEq, Eq)]
struct FrameSlice<'a> {
    lane_id: u32,
    opcode: u16,
    request_id: u64,
    flags: u8,
    body: &'a [u8],
}

#[rustler::nif(schedule = "Normal")]
fn decode_frames<'a>(
    env: Env<'a>,
    buffer: Binary<'a>,
    max_frame_bytes: u32,
) -> NifResult<Term<'a>> {
    let bytes = buffer.as_slice();
    let (frame_slices, offset) = match scan_frames(bytes, max_frame_bytes) {
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

    let rest_bytes = &bytes[offset..];
    let mut rest = OwnedBinary::new(rest_bytes.len())
        .ok_or_else(|| rustler::Error::Term(Box::new("native rest allocation failed")))?;
    rest.as_mut_slice().copy_from_slice(rest_bytes);
    let rest_term = Binary::from_owned(rest, env);

    Ok((atoms::ok(), frames, rest_term).encode(env))
}

#[rustler::nif(schedule = "Normal")]
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
    bytes[20..24].copy_from_slice(&(body.len() as u32).to_be_bytes());
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

fn scan_frames(bytes: &[u8], max_frame_bytes: u32) -> Result<(Vec<FrameSlice<'_>>, usize), String> {
    let mut offset = 0usize;
    let mut frames = Vec::new();

    while bytes.len().saturating_sub(offset) >= HEADER_SIZE {
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

        let frame_end = offset + HEADER_SIZE + body_len;

        if bytes.len() < frame_end {
            break;
        }

        frames.push(FrameSlice {
            lane_id,
            opcode,
            request_id,
            flags,
            body: &bytes[offset + HEADER_SIZE..frame_end],
        });

        offset = frame_end;
    }

    Ok((frames, offset))
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
        nil
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

        let (frames, offset) = scan_frames(&bytes, 1024).expect("scan succeeds");

        assert_eq!(offset, first.len());
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
}
