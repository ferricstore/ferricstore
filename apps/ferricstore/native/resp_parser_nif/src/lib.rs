use rustler::{Atom, BigInt, Binary, Encoder, Env, NewBinary, NifResult, Term};
use std::str::FromStr;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        inline,
        simple,
        nil,
        push,
        verbatim,
        attribute,
        command,
        unknown,
        get,
        set,
        nx,
        xx,
        keepttl,
        ex,
        px,
        exat,
        pxat,
        ping,
        del,
        exists,
        mget,
        mset,
        incr,
        decr,
        incrby,
        decrby,
        incrbyfloat,
        append,
        strlen,
        getset,
        getdel,
        getex,
        setnx,
        setex,
        psetex,
        getrange,
        setrange,
        msetnx,
        expire,
        pexpire,
        expireat,
        pexpireat,
        ttl,
        pttl,
        persist,
        gt,
        lt,
        lpush,
        rpush,
        lpop,
        rpop,
        lrange,
        llen,
        lindex,
        lset,
        lrem,
        ltrim,
        linsert,
        lmove,
        lpushx,
        rpushx,
        rpoplpush,
        before,
        after,
        left,
        right,
        blpop,
        brpop,
        blmove,
        blmpop,
        hset,
        hget,
        hdel,
        hmget,
        hgetall,
        hexists,
        hkeys,
        hvals,
        hlen,
        hincrby,
        hincrbyfloat,
        hsetnx,
        hstrlen,
        hrandfield,
        hscan,
        hexpire,
        httl,
        hpersist,
        hpexpire,
        hpttl,
        hexpiretime,
        hgetdel,
        hgetex,
        hsetex,
        withvalues,
        sadd,
        srem,
        smembers,
        sismember,
        smismember,
        scard,
        sinter,
        sunion,
        sdiff,
        sdiffstore,
        sinterstore,
        sunionstore,
        sintercard,
        srandmember,
        spop,
        smove,
        sscan,
        count,
        zadd,
        zrem,
        zscore,
        zrank,
        zrevrank,
        zrange,
        zrevrange,
        zcard,
        zincrby,
        zcount,
        zpopmin,
        zpopmax,
        zrandmember,
        zscan,
        zmscore,
        zrangebyscore,
        zrevrangebyscore,
        ch,
        withscores,
        limit,
        inclusive,
        exclusive,
        inf,
        neg_inf,
        setbit,
        getbit,
        bitcount,
        bitpos,
        bitop,
        byte,
        bit,
        all,
        start,
        band,
        bor,
        bxor,
        bnot,
        map,
        __struct__,
        value_too_large,
        invalid_bulk_length,
        bulk_crlf_missing,
        protocol_error,
        nesting_too_deep,
    }
}

const MAX_ARRAY_COUNT: i64 = 1_048_576;
const MAX_RESP_NESTING_DEPTH: usize = 128;
const FLOW_MAX_REF_SIZE: usize = 4_096;

// =========================================================================
// Pure parsing layer — no Env/Term, fully testable.
// Only compiled in test builds; the NIF layer above is the production path.
// =========================================================================

#[cfg(test)]
mod resp {
    use super::*;

    /// A parsed RESP value with byte-slice references into the input buffer.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum RespValue<'a> {
        /// Bulk string data — a slice of the input buffer.
        BulkString(&'a [u8]),
        /// Simple string (+OK\r\n) — the string content.
        SimpleString(&'a [u8]),
        /// Simple error (-ERR ...\r\n) — the error content.
        SimpleError(&'a [u8]),
        /// RESP integer.
        Integer(i64),
        /// Nil ($-1 or *-1 or _).
        Nil,
        /// Array of values.
        Array(Vec<RespValue<'a>>),
        /// Inline command — tokens split on whitespace.
        Inline(Vec<&'a [u8]>),
    }

    /// Result of parsing one RESP element from the buffer.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum RespParseResult<'a> {
        /// Successfully parsed value + new position.
        Ok(RespValue<'a>, usize),
        /// Not enough data yet.
        Incomplete,
        /// Protocol error.
        Error(RespError),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum RespError {
        InvalidArrayCount(String),
        ArrayTooLarge,
        InvalidBulkLength(String),
        ValueTooLarge { len: usize, max: usize },
        BulkCrlfMissing,
        InvalidInteger(String),
        InvalidNull(String),
        InlineTooLong,
        ProtocolError(String),
        NestingTooDeep,
    }

    /// Parse all complete RESP messages from `buf`.
    /// Returns (parsed_values, consumed_bytes) on success.
    pub(crate) fn parse_resp(
        buf: &[u8],
        max_value_size: usize,
    ) -> Result<(Vec<RespValue<'_>>, usize), RespError> {
        let mut pos = 0;
        let mut commands = Vec::new();

        loop {
            if pos >= buf.len() {
                break;
            }

            match parse_one_resp_depth(buf, pos, max_value_size, 0) {
                RespParseResult::Ok(val, new_pos) => {
                    commands.push(val);
                    pos = new_pos;
                }
                RespParseResult::Incomplete => break,
                RespParseResult::Error(e) => return Err(e),
            }
        }

        Ok((commands, pos))
    }

    pub(crate) fn parse_one_resp(
        buf: &[u8],
        pos: usize,
        max_value_size: usize,
    ) -> RespParseResult<'_> {
        parse_one_resp_depth(buf, pos, max_value_size, 0)
    }

    fn parse_one_resp_depth(
        buf: &[u8],
        pos: usize,
        max_value_size: usize,
        depth: usize,
    ) -> RespParseResult<'_> {
        if pos >= buf.len() {
            return RespParseResult::Incomplete;
        }

        match buf[pos] {
            b'*' if depth >= MAX_RESP_NESTING_DEPTH => {
                RespParseResult::Error(RespError::NestingTooDeep)
            }
            b'*' => parse_array_resp(buf, pos + 1, max_value_size, depth),
            b'$' => parse_bulk_string_resp(buf, pos + 1, max_value_size),
            b'+' => parse_simple_string_resp(buf, pos + 1),
            b'-' => parse_simple_error_resp(buf, pos + 1),
            b':' => parse_integer_resp(buf, pos + 1),
            b'_' => parse_null_resp(buf, pos + 1),
            b'#' | b',' | b'(' | b'!' | b'=' | b'%' | b'~' | b'>' | b'|' => RespParseResult::Error(
                RespError::ProtocolError("resp3 term conversion is tested through the NIF".into()),
            ),
            _ => parse_inline_resp(buf, pos),
        }
    }

    fn parse_array_resp(
        buf: &[u8],
        pos: usize,
        max_value_size: usize,
        depth: usize,
    ) -> RespParseResult<'_> {
        let (line, after_crlf) = match find_crlf(buf, pos) {
            Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
            None => return RespParseResult::Incomplete,
        };

        if line == b"-1" {
            return RespParseResult::Ok(RespValue::Nil, after_crlf);
        }

        let count = match parse_int_bytes(line) {
            Some(n) => n,
            None => {
                return RespParseResult::Error(RespError::InvalidArrayCount(lossy_str(line)));
            }
        };

        if count > MAX_ARRAY_COUNT {
            return RespParseResult::Error(RespError::ArrayTooLarge);
        }

        if count < 0 {
            return RespParseResult::Error(RespError::InvalidArrayCount(lossy_str(line)));
        }

        let mut elements = Vec::with_capacity(count as usize);
        let mut cur = after_crlf;

        for _ in 0..count {
            match parse_one_resp_depth(buf, cur, max_value_size, depth + 1) {
                RespParseResult::Ok(val, new_pos) => {
                    elements.push(val);
                    cur = new_pos;
                }
                RespParseResult::Incomplete => return RespParseResult::Incomplete,
                RespParseResult::Error(e) => return RespParseResult::Error(e),
            }
        }

        RespParseResult::Ok(RespValue::Array(elements), cur)
    }

    fn parse_bulk_string_resp(
        buf: &[u8],
        pos: usize,
        max_value_size: usize,
    ) -> RespParseResult<'_> {
        let (line, after_crlf) = match find_crlf(buf, pos) {
            Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
            None => return RespParseResult::Incomplete,
        };

        if line == b"-1" {
            return RespParseResult::Ok(RespValue::Nil, after_crlf);
        }

        let len = match parse_int_bytes(line) {
            Some(n) if n >= 0 => n as usize,
            Some(_) => {
                return RespParseResult::Error(RespError::InvalidBulkLength(lossy_str(line)));
            }
            None => {
                return RespParseResult::Error(RespError::InvalidBulkLength(lossy_str(line)));
            }
        };

        if len > max_value_size {
            return RespParseResult::Error(RespError::ValueTooLarge {
                len,
                max: max_value_size,
            });
        }

        let needed = after_crlf + len + 2;
        if needed > buf.len() {
            return RespParseResult::Incomplete;
        }

        if buf[after_crlf + len] != b'\r' || buf[after_crlf + len + 1] != b'\n' {
            return RespParseResult::Error(RespError::BulkCrlfMissing);
        }

        RespParseResult::Ok(
            RespValue::BulkString(&buf[after_crlf..after_crlf + len]),
            after_crlf + len + 2,
        )
    }

    fn parse_simple_string_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (line_end, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };
        RespParseResult::Ok(RespValue::SimpleString(&buf[pos..line_end]), after_crlf)
    }

    fn parse_simple_error_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (line_end, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };
        RespParseResult::Ok(RespValue::SimpleError(&buf[pos..line_end]), after_crlf)
    }

    fn parse_integer_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };

        let line = &buf[pos..cr_pos];
        match parse_int_bytes(line) {
            Some(n) => RespParseResult::Ok(RespValue::Integer(n), after_crlf),
            None => RespParseResult::Error(RespError::InvalidInteger(lossy_str(line))),
        }
    }

    fn parse_null_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };

        if cr_pos != pos {
            return RespParseResult::Error(RespError::InvalidNull(lossy_str(&buf[pos..cr_pos])));
        }

        RespParseResult::Ok(RespValue::Nil, after_crlf)
    }

    fn parse_inline_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };

        let line = &buf[pos..cr_pos];
        if line.len() > 1_048_576 {
            return RespParseResult::Error(RespError::InlineTooLong);
        }

        let tokens: Vec<&[u8]> = line
            .split(|&b| b == b' ' || b == b'\t')
            .filter(|s| !s.is_empty())
            .collect();

        RespParseResult::Ok(RespValue::Inline(tokens), after_crlf)
    }
}

// =========================================================================
// NIF layer — thin wrapper converting RespValue -> Term
// =========================================================================

#[rustler::nif]
fn parse<'a>(env: Env<'a>, data: Binary<'a>, max_value_size: usize) -> NifResult<Term<'a>> {
    let buf = data.as_slice();
    let mut pos = 0;
    let mut commands: Vec<Term<'a>> = Vec::new();

    loop {
        if pos >= buf.len() {
            break;
        }

        match parse_one(env, &data, buf, pos, max_value_size, 0) {
            ParseResult::Ok(term, new_pos) => {
                commands.push(term);
                pos = new_pos;
            }
            ParseResult::Skip(new_pos) => {
                pos = new_pos;
            }
            ParseResult::Incomplete => break,
            ParseResult::Error(reason) => {
                return Ok((atoms::error(), reason).encode(env));
            }
        }
    }

    let rest_len = buf.len() - pos;
    let rest = if rest_len == 0 {
        "".encode(env)
    } else {
        unsafe { data.make_subbinary_unchecked(pos, rest_len) }.encode(env)
    };
    let list = commands.encode(env);
    Ok((atoms::ok(), list, rest).encode(env))
}

#[rustler::nif]
fn parse_commands<'a>(
    env: Env<'a>,
    data: Binary<'a>,
    max_value_size: usize,
) -> NifResult<Term<'a>> {
    let buf = data.as_slice();
    let mut pos = 0;
    let mut commands: Vec<Term<'a>> = Vec::new();

    loop {
        if pos >= buf.len() {
            break;
        }

        match parse_command(env, &data, buf, pos, max_value_size) {
            ParseResult::Ok(term, new_pos) => {
                commands.push(term);
                pos = new_pos;
            }
            ParseResult::Skip(new_pos) => {
                pos = new_pos;
            }
            ParseResult::Incomplete => break,
            ParseResult::Error(reason) => {
                return Ok((atoms::error(), reason).encode(env));
            }
        }
    }

    let rest_len = buf.len() - pos;
    let rest = if rest_len == 0 {
        "".encode(env)
    } else {
        unsafe { data.make_subbinary_unchecked(pos, rest_len) }.encode(env)
    };

    Ok((atoms::ok(), commands, rest).encode(env))
}

enum ParseResult<'a> {
    Ok(Term<'a>, usize),
    Skip(usize),
    Incomplete,
    Error(Term<'a>),
}

fn parse_one<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    depth: usize,
) -> ParseResult<'a> {
    if pos >= buf.len() {
        return ParseResult::Incomplete;
    }

    match buf[pos] {
        b'*' if depth >= MAX_RESP_NESTING_DEPTH => {
            ParseResult::Error(atoms::nesting_too_deep().encode(env))
        }
        b'*' => parse_array(env, data, buf, pos + 1, max_value_size, depth),
        b'$' => parse_bulk_string(env, data, buf, pos + 1, max_value_size),
        b'+' => parse_simple_string(env, data, buf, pos + 1),
        b'-' => parse_simple_error(env, data, buf, pos + 1),
        b':' => parse_integer(env, buf, pos + 1),
        b'_' => parse_null(env, buf, pos + 1),
        b'#' => parse_boolean(env, buf, pos + 1),
        b',' => parse_double(env, buf, pos + 1),
        b'(' => parse_big_number(env, buf, pos + 1),
        b'!' => parse_blob_error(env, data, buf, pos + 1, max_value_size),
        b'=' => parse_verbatim_string(env, data, buf, pos + 1, max_value_size),
        b'%' if depth >= MAX_RESP_NESTING_DEPTH => {
            ParseResult::Error(atoms::nesting_too_deep().encode(env))
        }
        b'%' => parse_map(env, data, buf, pos + 1, max_value_size, depth),
        b'~' if depth >= MAX_RESP_NESTING_DEPTH => {
            ParseResult::Error(atoms::nesting_too_deep().encode(env))
        }
        b'~' => parse_set(env, data, buf, pos + 1, max_value_size, depth),
        b'>' if depth >= MAX_RESP_NESTING_DEPTH => {
            ParseResult::Error(atoms::nesting_too_deep().encode(env))
        }
        b'>' => parse_push(env, data, buf, pos + 1, max_value_size, depth),
        b'|' if depth >= MAX_RESP_NESTING_DEPTH => {
            ParseResult::Error(atoms::nesting_too_deep().encode(env))
        }
        b'|' => parse_attribute(env, data, buf, pos + 1, max_value_size, depth),
        _ => parse_inline(env, buf, pos),
    }
}

fn parse_command<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
) -> ParseResult<'a> {
    if pos >= buf.len() {
        return ParseResult::Incomplete;
    }

    match buf[pos] {
        b'*' => parse_command_array(env, data, buf, pos + 1, max_value_size),
        b'$' | b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' | b'!' | b'=' | b'%' | b'~'
        | b'>' | b'|' => ParseResult::Error(
            Atom::from_str(env, "invalid_command_format")
                .unwrap()
                .encode(env),
        ),
        _ => parse_inline_command(env, data, buf, pos),
    }
}

fn parse_command_array<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
) -> ParseResult<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return ParseResult::Incomplete,
    };

    let count = match parse_int_bytes(line) {
        Some(n) if (0..=MAX_ARRAY_COUNT).contains(&n) => n as usize,
        Some(n) if n > MAX_ARRAY_COUNT => {
            return ParseResult::Error(make_binary_term(
                env,
                b"ERR protocol error: array too large",
            ));
        }
        _ => {
            return ParseResult::Error(
                (
                    Atom::from_str(env, "invalid_array_count").unwrap(),
                    lossy_str(line),
                )
                    .encode(env),
            );
        }
    };

    if count == 0 {
        return ParseResult::Skip(after_crlf);
    }

    let (cmd_start, cmd_len, mut cur) =
        match parse_command_bulk_arg(env, buf, after_crlf, max_value_size, true) {
            SizedPayloadRange::Ok(start, len, new_pos) => (start, len, new_pos),
            SizedPayloadRange::Incomplete => return ParseResult::Incomplete,
            SizedPayloadRange::Error(term) => return ParseResult::Error(term),
        };

    let cmd_bytes = uppercase_bytes(&buf[cmd_start..cmd_start + cmd_len]);
    let cmd = make_uppercase_term(env, data, buf, cmd_start, cmd_len);
    let mut args: Vec<Term<'a>> = Vec::with_capacity(count.saturating_sub(1));
    let mut arg_bytes: Vec<&[u8]> = Vec::with_capacity(count.saturating_sub(1));

    for _ in 1..count {
        match parse_command_bulk_arg(env, buf, cur, max_value_size, true) {
            SizedPayloadRange::Ok(start, len, new_pos) => {
                args.push(unsafe { data.make_subbinary_unchecked(start, len) }.encode(env));
                arg_bytes.push(&buf[start..start + len]);
                cur = new_pos;
            }
            SizedPayloadRange::Incomplete => return ParseResult::Incomplete,
            SizedPayloadRange::Error(term) => return ParseResult::Error(term),
        }
    }

    ParseResult::Ok(
        make_command_term(env, &cmd_bytes, cmd, args, &arg_bytes),
        cur,
    )
}

fn parse_inline_command<'a>(
    env: Env<'a>,
    _data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
) -> ParseResult<'a> {
    let (line_end, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let line = &buf[pos..line_end];
    if line.len() > 1_048_576 {
        return ParseResult::Error(make_binary_term(
            env,
            b"ERR protocol error: inline command too long",
        ));
    }

    let tokens = match parse_inline_token_bytes(line) {
        Ok(tokens) => tokens,
        Err(message) => return ParseResult::Error(make_binary_term(env, message)),
    };

    if tokens.is_empty() {
        return ParseResult::Skip(after_crlf);
    }

    let cmd_bytes = uppercase_bytes(&tokens[0]);
    let cmd = make_binary_term(env, &cmd_bytes);
    let mut args = Vec::with_capacity(tokens.len() - 1);

    for token in &tokens[1..] {
        args.push(make_binary_term(env, token));
    }

    let arg_bytes: Vec<&[u8]> = tokens[1..].iter().map(|token| token.as_slice()).collect();

    ParseResult::Ok(
        make_command_term(env, &cmd_bytes, cmd, args, &arg_bytes),
        after_crlf,
    )
}

fn parse_array<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    depth: usize,
) -> ParseResult<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return ParseResult::Incomplete,
    };

    if line == b"-1" {
        return ParseResult::Ok(atoms::nil().encode(env), after_crlf);
    }

    let count = match parse_int_bytes(line) {
        Some(n) => n,
        None => {
            let line_str = lossy_str(line);
            return ParseResult::Error(
                (
                    rustler::types::atom::Atom::from_str(env, "invalid_array_count").unwrap(),
                    line_str,
                )
                    .encode(env),
            );
        }
    };

    if count > MAX_ARRAY_COUNT {
        return ParseResult::Error(make_binary_term(
            env,
            b"ERR protocol error: array too large",
        ));
    }

    if count < 0 {
        let line_str = lossy_str(line);
        return ParseResult::Error(
            (
                rustler::types::atom::Atom::from_str(env, "invalid_array_count").unwrap(),
                line_str,
            )
                .encode(env),
        );
    }

    let mut elements: Vec<Term<'a>> = Vec::with_capacity(count as usize);
    let mut cur = after_crlf;

    for _ in 0..count {
        match parse_one(env, data, buf, cur, max_value_size, depth + 1) {
            ParseResult::Ok(term, new_pos) => {
                elements.push(term);
                cur = new_pos;
            }
            ParseResult::Incomplete => return ParseResult::Incomplete,
            ParseResult::Error(e) => return ParseResult::Error(e),
            ParseResult::Skip(_) => return ParseResult::Error(atoms::protocol_error().encode(env)),
        }
    }

    ParseResult::Ok(elements.encode(env), cur)
}

fn parse_bulk_string<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
) -> ParseResult<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return ParseResult::Incomplete,
    };

    if line == b"-1" {
        return ParseResult::Ok(atoms::nil().encode(env), after_crlf);
    }

    let len = match parse_int_bytes(line) {
        Some(n) if n >= 0 => n as usize,
        Some(_) => {
            let line_str = lossy_str(line);
            return ParseResult::Error((atoms::invalid_bulk_length(), line_str).encode(env));
        }
        None => {
            let line_str = lossy_str(line);
            return ParseResult::Error((atoms::invalid_bulk_length(), line_str).encode(env));
        }
    };

    if len > max_value_size {
        return ParseResult::Error((atoms::value_too_large(), len, max_value_size).encode(env));
    }

    let needed = after_crlf + len + 2;
    if needed > buf.len() {
        return ParseResult::Incomplete;
    }

    if buf[after_crlf + len] != b'\r' || buf[after_crlf + len + 1] != b'\n' {
        return ParseResult::Error(atoms::bulk_crlf_missing().encode(env));
    }

    let term = unsafe { data.make_subbinary_unchecked(after_crlf, len) }.encode(env);
    ParseResult::Ok(term, after_crlf + len + 2)
}

fn parse_simple_string<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
) -> ParseResult<'a> {
    let (line_end, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };
    let line = unsafe { data.make_subbinary_unchecked(pos, line_end - pos) }.encode(env);
    ParseResult::Ok((atoms::simple(), line).encode(env), after_crlf)
}

fn parse_simple_error<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
) -> ParseResult<'a> {
    let (line_end, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };
    let line = unsafe { data.make_subbinary_unchecked(pos, line_end - pos) }.encode(env);
    ParseResult::Ok((atoms::error(), line).encode(env), after_crlf)
}

fn parse_integer<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let line = &buf[pos..cr_pos];
    match std::str::from_utf8(line)
        .ok()
        .and_then(|s| BigInt::from_str(s).ok())
    {
        Some(n) => ParseResult::Ok(n.encode(env), after_crlf),
        None => {
            let line_str = lossy_str(line);
            ParseResult::Error(
                (
                    rustler::types::atom::Atom::from_str(env, "invalid_integer").unwrap(),
                    line_str,
                )
                    .encode(env),
            )
        }
    }
}

fn parse_null<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    if cr_pos != pos {
        let line = lossy_str(&buf[pos..cr_pos]);
        return ParseResult::Error(
            (
                rustler::types::atom::Atom::from_str(env, "invalid_null").unwrap(),
                line,
            )
                .encode(env),
        );
    }

    ParseResult::Ok(atoms::nil().encode(env), after_crlf)
}

fn parse_boolean<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    match &buf[pos..cr_pos] {
        b"t" => ParseResult::Ok(true.encode(env), after_crlf),
        b"f" => ParseResult::Ok(false.encode(env), after_crlf),
        other => ParseResult::Error(
            (
                Atom::from_str(env, "invalid_boolean").unwrap(),
                lossy_str(other),
            )
                .encode(env),
        ),
    }
}

fn parse_double<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let line = &buf[pos..cr_pos];
    match line {
        b"inf" => ParseResult::Ok(
            Atom::from_str(env, "infinity").unwrap().encode(env),
            after_crlf,
        ),
        b"-inf" => ParseResult::Ok(
            Atom::from_str(env, "neg_infinity").unwrap().encode(env),
            after_crlf,
        ),
        b"nan" | b"NaN" | b"NAN" => {
            ParseResult::Ok(Atom::from_str(env, "nan").unwrap().encode(env), after_crlf)
        }
        _ => match std::str::from_utf8(line)
            .ok()
            .and_then(|s| f64::from_str(s).ok())
        {
            Some(value) if value.is_finite() => ParseResult::Ok(value.encode(env), after_crlf),
            _ => ParseResult::Error(
                (
                    Atom::from_str(env, "invalid_double").unwrap(),
                    lossy_str(line),
                )
                    .encode(env),
            ),
        },
    }
}

fn parse_big_number<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let line = &buf[pos..cr_pos];
    match std::str::from_utf8(line)
        .ok()
        .and_then(|s| BigInt::from_str(s).ok())
    {
        Some(value) => ParseResult::Ok(value.encode(env), after_crlf),
        None => ParseResult::Error(
            (
                Atom::from_str(env, "invalid_big_number").unwrap(),
                lossy_str(line),
            )
                .encode(env),
        ),
    }
}

fn parse_blob_error<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
) -> ParseResult<'a> {
    match parse_sized_payload_range(env, buf, pos, max_value_size, "invalid_blob_error_length") {
        SizedPayloadRange::Ok(start, len, new_pos) => {
            let term = unsafe { data.make_subbinary_unchecked(start, len) }.encode(env);
            ParseResult::Ok((atoms::error(), term).encode(env), new_pos)
        }
        SizedPayloadRange::Incomplete => ParseResult::Incomplete,
        SizedPayloadRange::Error(term) => ParseResult::Error(term),
    }
}

fn parse_verbatim_string<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
) -> ParseResult<'a> {
    match parse_sized_payload_range(env, buf, pos, max_value_size, "invalid_verbatim_length") {
        SizedPayloadRange::Ok(start, len, new_pos) => {
            if len < 4 {
                return ParseResult::Error(
                    (Atom::from_str(env, "invalid_verbatim_length").unwrap(), len).encode(env),
                );
            }

            if buf[start + 3] != b':' {
                return ParseResult::Error(
                    Atom::from_str(env, "invalid_verbatim_payload")
                        .unwrap()
                        .encode(env),
                );
            }

            let encoding = unsafe { data.make_subbinary_unchecked(start, 3) }.encode(env);
            let value = unsafe { data.make_subbinary_unchecked(start + 4, len - 4) }.encode(env);
            ParseResult::Ok((atoms::verbatim(), encoding, value).encode(env), new_pos)
        }
        SizedPayloadRange::Incomplete => ParseResult::Incomplete,
        SizedPayloadRange::Error(term) => ParseResult::Error(term),
    }
}

fn parse_map<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    depth: usize,
) -> ParseResult<'a> {
    match parse_map_term(env, data, buf, pos, max_value_size, depth) {
        ParseMapResult::Ok(map, new_pos) => ParseResult::Ok(map, new_pos),
        ParseMapResult::Incomplete => ParseResult::Incomplete,
        ParseMapResult::Error(term) => ParseResult::Error(term),
    }
}

fn parse_set<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    depth: usize,
) -> ParseResult<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return ParseResult::Incomplete,
    };

    let count = match parse_non_negative_count(line) {
        Ok(count) => count,
        Err(line) => {
            return ParseResult::Error(
                (Atom::from_str(env, "invalid_set_count").unwrap(), line).encode(env),
            );
        }
    };

    let mut cur = after_crlf;
    let mut inner = Term::map_new(env);
    let empty: Vec<Term<'a>> = Vec::new();
    let empty_list = empty.encode(env);

    for _ in 0..count {
        match parse_one(env, data, buf, cur, max_value_size, depth + 1) {
            ParseResult::Ok(term, new_pos) => {
                inner = match inner.map_put(term, empty_list) {
                    Ok(map) => map,
                    Err(_) => return ParseResult::Error(atoms::protocol_error().encode(env)),
                };
                cur = new_pos;
            }
            ParseResult::Incomplete => return ParseResult::Incomplete,
            ParseResult::Error(e) => return ParseResult::Error(e),
            ParseResult::Skip(_) => return ParseResult::Error(atoms::protocol_error().encode(env)),
        }
    }

    let mapset_module = Atom::from_str(env, "Elixir.MapSet").unwrap();
    let mapset = match Term::map_new(env)
        .map_put(atoms::__struct__(), mapset_module)
        .and_then(|term| term.map_put(atoms::map(), inner))
    {
        Ok(term) => term,
        Err(_) => return ParseResult::Error(atoms::protocol_error().encode(env)),
    };

    ParseResult::Ok(mapset, cur)
}

fn parse_push<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    depth: usize,
) -> ParseResult<'a> {
    match parse_counted_elements(
        env,
        data,
        buf,
        pos,
        max_value_size,
        "invalid_push_count",
        depth,
    ) {
        CountedElements::Ok(elements, new_pos) => {
            ParseResult::Ok((atoms::push(), elements).encode(env), new_pos)
        }
        CountedElements::Incomplete => ParseResult::Incomplete,
        CountedElements::Error(term) => ParseResult::Error(term),
    }
}

fn parse_attribute<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    depth: usize,
) -> ParseResult<'a> {
    match parse_map_term(env, data, buf, pos, max_value_size, depth) {
        ParseMapResult::Ok(map, new_pos) => {
            ParseResult::Ok((atoms::attribute(), map).encode(env), new_pos)
        }
        ParseMapResult::Incomplete => ParseResult::Incomplete,
        ParseMapResult::Error(term) => ParseResult::Error(term),
    }
}

fn parse_inline<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let line = &buf[pos..cr_pos];
    if line.len() > 1_048_576 {
        return ParseResult::Error(make_binary_term(
            env,
            b"ERR protocol error: inline command too long",
        ));
    }

    let token_bytes = match parse_inline_token_bytes(line) {
        Ok(tokens) => tokens,
        Err(message) => return ParseResult::Error(make_binary_term(env, message)),
    };

    // Inline tokens are copied because quoted strings may need unescaping and
    // may not be contiguous sub-binaries of the input buffer.
    let tokens: Vec<Term<'a>> = token_bytes
        .iter()
        .map(|token| make_binary_term(env, token))
        .collect();

    ParseResult::Ok((atoms::inline(), tokens).encode(env), after_crlf)
}

// -- Helpers --

enum CountedElements<'a> {
    Ok(Vec<Term<'a>>, usize),
    Incomplete,
    Error(Term<'a>),
}

enum ParseMapResult<'a> {
    Ok(Term<'a>, usize),
    Incomplete,
    Error(Term<'a>),
}

enum SizedPayloadRange<'a> {
    Ok(usize, usize, usize),
    Incomplete,
    Error(Term<'a>),
}

fn parse_counted_elements<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    error_atom: &str,
    depth: usize,
) -> CountedElements<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return CountedElements::Incomplete,
    };

    let count = match parse_non_negative_count(line) {
        Ok(count) => count,
        Err(line) => {
            return CountedElements::Error(
                (Atom::from_str(env, error_atom).unwrap(), line).encode(env),
            );
        }
    };

    let mut elements: Vec<Term<'a>> = Vec::with_capacity(count);
    let mut cur = after_crlf;

    for _ in 0..count {
        match parse_one(env, data, buf, cur, max_value_size, depth + 1) {
            ParseResult::Ok(term, new_pos) => {
                elements.push(term);
                cur = new_pos;
            }
            ParseResult::Incomplete => return CountedElements::Incomplete,
            ParseResult::Error(e) => return CountedElements::Error(e),
            ParseResult::Skip(_) => {
                return CountedElements::Error(atoms::protocol_error().encode(env))
            }
        }
    }

    CountedElements::Ok(elements, cur)
}

fn parse_map_term<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    depth: usize,
) -> ParseMapResult<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return ParseMapResult::Incomplete,
    };

    let count = match parse_non_negative_count(line) {
        Ok(count) => count,
        Err(line) => {
            return ParseMapResult::Error(
                (Atom::from_str(env, "invalid_map_count").unwrap(), line).encode(env),
            );
        }
    };

    let mut cur = after_crlf;
    let mut map = Term::map_new(env);

    for _ in 0..count {
        let key = match parse_one(env, data, buf, cur, max_value_size, depth + 1) {
            ParseResult::Ok(term, new_pos) => {
                cur = new_pos;
                term
            }
            ParseResult::Incomplete => return ParseMapResult::Incomplete,
            ParseResult::Error(e) => return ParseMapResult::Error(e),
            ParseResult::Skip(_) => {
                return ParseMapResult::Error(atoms::protocol_error().encode(env))
            }
        };

        let value = match parse_one(env, data, buf, cur, max_value_size, depth + 1) {
            ParseResult::Ok(term, new_pos) => {
                cur = new_pos;
                term
            }
            ParseResult::Incomplete => return ParseMapResult::Incomplete,
            ParseResult::Error(e) => return ParseMapResult::Error(e),
            ParseResult::Skip(_) => {
                return ParseMapResult::Error(atoms::protocol_error().encode(env))
            }
        };

        map = match map.map_put(key, value) {
            Ok(updated) => updated,
            Err(_) => return ParseMapResult::Error(atoms::protocol_error().encode(env)),
        };
    }

    ParseMapResult::Ok(map, cur)
}

fn parse_sized_payload_range<'a>(
    env: Env<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    error_atom: &str,
) -> SizedPayloadRange<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return SizedPayloadRange::Incomplete,
    };

    let len = match parse_int_bytes(line) {
        Some(n) if n >= 0 => n as usize,
        Some(n) => {
            return SizedPayloadRange::Error(
                (Atom::from_str(env, error_atom).unwrap(), n).encode(env),
            );
        }
        None => {
            return SizedPayloadRange::Error(
                (Atom::from_str(env, error_atom).unwrap(), lossy_str(line)).encode(env),
            );
        }
    };

    if len > max_value_size {
        return SizedPayloadRange::Error(
            (atoms::value_too_large(), len, max_value_size).encode(env),
        );
    }

    let needed = after_crlf + len + 2;
    if needed > buf.len() {
        return SizedPayloadRange::Incomplete;
    }

    if buf[after_crlf + len] != b'\r' || buf[after_crlf + len + 1] != b'\n' {
        return SizedPayloadRange::Error(atoms::bulk_crlf_missing().encode(env));
    }

    SizedPayloadRange::Ok(after_crlf, len, after_crlf + len + 2)
}

fn parse_command_bulk_arg<'a>(
    env: Env<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
    allow_empty: bool,
) -> SizedPayloadRange<'a> {
    if pos >= buf.len() {
        return SizedPayloadRange::Incomplete;
    }

    if buf[pos] != b'$' {
        return SizedPayloadRange::Error(
            Atom::from_str(env, "invalid_command_argument")
                .unwrap()
                .encode(env),
        );
    }

    match parse_sized_payload_range(env, buf, pos + 1, max_value_size, "invalid_bulk_length") {
        SizedPayloadRange::Ok(_, 0, _) if !allow_empty => SizedPayloadRange::Error(
            Atom::from_str(env, "invalid_command_argument")
                .unwrap()
                .encode(env),
        ),
        other => other,
    }
}

fn make_command_term<'a>(
    env: Env<'a>,
    cmd_bytes: &[u8],
    cmd: Term<'a>,
    args: Vec<Term<'a>>,
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let ast = make_command_ast(env, cmd_bytes, &args, arg_bytes);
    let keys = make_command_keys(env, cmd_bytes, &args, arg_bytes);
    (atoms::command(), cmd, args, ast, keys).encode(env)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandAstKind {
    Get,
    Set,
    Ping0,
    Ping1,
    Del,
    Exists,
    Mget,
    Mset,
    Incr,
    Decr,
    Incrby,
    Decrby,
    Incrbyfloat,
    Append,
    Strlen,
    Getset,
    Getdel,
    Getex,
    Setnx,
    Setex,
    Psetex,
    Getrange,
    Setrange,
    Msetnx,
    Expire,
    Pexpire,
    Expireat,
    Pexpireat,
    Ttl,
    Pttl,
    Persist,
    Lpush,
    Rpush,
    Lpop,
    Rpop,
    Lrange,
    Llen,
    Lindex,
    Lset,
    Lrem,
    Ltrim,
    Linsert,
    Lmove,
    Lpushx,
    Rpushx,
    Rpoplpush,
    Hset,
    Hget,
    Hdel,
    Hmget,
    Hgetall,
    Hexists,
    Hkeys,
    Hvals,
    Hlen,
    Hincrby,
    Hincrbyfloat,
    Hsetnx,
    Hstrlen,
    Hrandfield,
    Hscan,
    Hexpire,
    Httl,
    Hpersist,
    Hpexpire,
    Hpttl,
    Hexpiretime,
    Hgetdel,
    Hgetex,
    Hsetex,
    Sadd,
    Srem,
    Smembers,
    Sismember,
    Smismember,
    Scard,
    Sinter,
    Sunion,
    Sdiff,
    Sdiffstore,
    Sinterstore,
    Sunionstore,
    Sintercard,
    Srandmember,
    Spop,
    Smove,
    Sscan,
    Zadd,
    Zrem,
    Zscore,
    Zrank,
    Zrevrank,
    Zrange,
    Zrevrange,
    Zcard,
    Zincrby,
    Zcount,
    Zpopmin,
    Zpopmax,
    Zrandmember,
    Zscan,
    Zmscore,
    Zrangebyscore,
    Zrevrangebyscore,
    Setbit,
    Getbit,
    Bitcount,
    Bitpos,
    Bitop,
    Type,
    Unlink,
    Rename,
    Renamenx,
    Copy,
    Randomkey,
    Scan,
    Expiretime,
    Pexpiretime,
    Object,
    Wait,
    Xadd,
    Xlen,
    Xrange,
    Xrevrange,
    Xread,
    Xtrim,
    Xdel,
    Xinfo,
    Xgroup,
    Xreadgroup,
    Xack,
    JsonSet,
    JsonGet,
    JsonDel,
    JsonNumincrby,
    JsonType,
    JsonStrlen,
    JsonObjkeys,
    JsonObjlen,
    JsonArrappend,
    JsonArrlen,
    JsonToggle,
    JsonClear,
    JsonMget,
    Geoadd,
    Geopos,
    Geodist,
    Geohash,
    Geosearch,
    Geosearchstore,
    Pfadd,
    Pfcount,
    Pfmerge,
    BfReserve,
    BfAdd,
    BfMadd,
    BfExists,
    BfMexists,
    BfCard,
    BfInfo,
    CfReserve,
    CfAdd,
    CfAddnx,
    CfDel,
    CfExists,
    CfMexists,
    CfCount,
    CfInfo,
    CmsInitbydim,
    CmsInitbyprob,
    CmsIncrby,
    CmsQuery,
    CmsMerge,
    CmsInfo,
    TopkReserve,
    TopkAdd,
    TopkIncrby,
    TopkQuery,
    TopkList,
    TopkCount,
    TopkInfo,
    TdigestCreate,
    TdigestAdd,
    TdigestReset,
    TdigestQuantile,
    TdigestCdf,
    TdigestRank,
    TdigestRevrank,
    TdigestByrank,
    TdigestByrevrank,
    TdigestTrimmedMean,
    TdigestMin,
    TdigestMax,
    TdigestInfo,
    TdigestMerge,
    FlowCreate,
    FlowCreateMany,
    FlowGet,
    FlowPolicySet,
    FlowPolicyGet,
    FlowClaimDue,
    FlowReclaim,
    FlowComplete,
    FlowCompleteMany,
    FlowTransition,
    FlowTransitionMany,
    FlowRetry,
    FlowRetryMany,
    FlowFail,
    FlowFailMany,
    FlowCancel,
    FlowCancelMany,
    FlowRewind,
    FlowList,
    FlowByParent,
    FlowByRoot,
    FlowByCorrelation,
    FlowInfo,
    FlowStuck,
    FlowHistory,
    Blpop,
    Brpop,
    Blmove,
    Blmpop,
    Hello,
    Auth,
    Acl,
    Client,
    Sandbox,
    Subscribe,
    Unsubscribe,
    Psubscribe,
    Punsubscribe,
    Multi,
    Exec,
    Discard,
    Watch,
    Unwatch,
    Reset,
    Quit,
    Unknown,
}

fn classify_command_ast(cmd: &[u8], arity: usize) -> CommandAstKind {
    match cmd {
        b"GET" if arity == 1 => CommandAstKind::Get,
        b"SET" if arity == 2 => CommandAstKind::Set,
        b"PING" if arity == 0 => CommandAstKind::Ping0,
        b"PING" if arity == 1 => CommandAstKind::Ping1,
        b"DEL" => CommandAstKind::Del,
        b"EXISTS" => CommandAstKind::Exists,
        b"MGET" => CommandAstKind::Mget,
        b"MSET" => CommandAstKind::Mset,
        b"INCR" if arity == 1 => CommandAstKind::Incr,
        b"DECR" if arity == 1 => CommandAstKind::Decr,
        b"INCRBY" if arity == 2 => CommandAstKind::Incrby,
        b"DECRBY" if arity == 2 => CommandAstKind::Decrby,
        b"INCRBYFLOAT" if arity == 2 => CommandAstKind::Incrbyfloat,
        b"APPEND" if arity == 2 => CommandAstKind::Append,
        b"STRLEN" if arity == 1 => CommandAstKind::Strlen,
        b"GETSET" if arity == 2 => CommandAstKind::Getset,
        b"GETDEL" if arity == 1 => CommandAstKind::Getdel,
        b"GETEX" if arity == 1 || arity == 2 || arity == 3 => CommandAstKind::Getex,
        b"SETNX" if arity == 2 => CommandAstKind::Setnx,
        b"SETEX" if arity == 3 => CommandAstKind::Setex,
        b"PSETEX" if arity == 3 => CommandAstKind::Psetex,
        b"GETRANGE" if arity == 3 => CommandAstKind::Getrange,
        b"SETRANGE" if arity == 3 => CommandAstKind::Setrange,
        b"MSETNX" => CommandAstKind::Msetnx,
        b"EXPIRE" if arity == 2 || arity == 3 => CommandAstKind::Expire,
        b"PEXPIRE" if arity == 2 || arity == 3 => CommandAstKind::Pexpire,
        b"EXPIREAT" if arity == 2 || arity == 3 => CommandAstKind::Expireat,
        b"PEXPIREAT" if arity == 2 || arity == 3 => CommandAstKind::Pexpireat,
        b"TTL" if arity == 1 => CommandAstKind::Ttl,
        b"PTTL" if arity == 1 => CommandAstKind::Pttl,
        b"PERSIST" if arity == 1 => CommandAstKind::Persist,
        b"LPUSH" if arity >= 2 => CommandAstKind::Lpush,
        b"RPUSH" if arity >= 2 => CommandAstKind::Rpush,
        b"LPOP" if arity == 1 || arity == 2 => CommandAstKind::Lpop,
        b"RPOP" if arity == 1 || arity == 2 => CommandAstKind::Rpop,
        b"LRANGE" if arity == 3 => CommandAstKind::Lrange,
        b"LLEN" if arity == 1 => CommandAstKind::Llen,
        b"LINDEX" if arity == 2 => CommandAstKind::Lindex,
        b"LSET" if arity == 3 => CommandAstKind::Lset,
        b"LREM" if arity == 3 => CommandAstKind::Lrem,
        b"LTRIM" if arity == 3 => CommandAstKind::Ltrim,
        b"LINSERT" if arity == 4 => CommandAstKind::Linsert,
        b"LMOVE" if arity == 4 => CommandAstKind::Lmove,
        b"LPUSHX" if arity >= 2 => CommandAstKind::Lpushx,
        b"RPUSHX" if arity >= 2 => CommandAstKind::Rpushx,
        b"RPOPLPUSH" if arity == 2 => CommandAstKind::Rpoplpush,
        b"HSET" if arity >= 3 => CommandAstKind::Hset,
        b"HGET" if arity == 2 => CommandAstKind::Hget,
        b"HDEL" if arity >= 2 => CommandAstKind::Hdel,
        b"HMGET" if arity >= 2 => CommandAstKind::Hmget,
        b"HGETALL" if arity == 1 => CommandAstKind::Hgetall,
        b"HEXISTS" if arity == 2 => CommandAstKind::Hexists,
        b"HKEYS" if arity == 1 => CommandAstKind::Hkeys,
        b"HVALS" if arity == 1 => CommandAstKind::Hvals,
        b"HLEN" if arity == 1 => CommandAstKind::Hlen,
        b"HINCRBY" if arity == 3 => CommandAstKind::Hincrby,
        b"HINCRBYFLOAT" if arity == 3 => CommandAstKind::Hincrbyfloat,
        b"HSETNX" if arity == 3 => CommandAstKind::Hsetnx,
        b"HSTRLEN" if arity == 2 => CommandAstKind::Hstrlen,
        b"HRANDFIELD" if arity == 1 || arity == 2 || arity == 3 => CommandAstKind::Hrandfield,
        b"HSCAN" if arity >= 2 => CommandAstKind::Hscan,
        b"HEXPIRE" if arity >= 4 => CommandAstKind::Hexpire,
        b"HTTL" if arity >= 3 => CommandAstKind::Httl,
        b"HPERSIST" if arity >= 3 => CommandAstKind::Hpersist,
        b"HPEXPIRE" if arity >= 4 => CommandAstKind::Hpexpire,
        b"HPTTL" if arity >= 3 => CommandAstKind::Hpttl,
        b"HEXPIRETIME" if arity >= 3 => CommandAstKind::Hexpiretime,
        b"HGETDEL" if arity >= 3 => CommandAstKind::Hgetdel,
        b"HGETEX" if arity >= 4 => CommandAstKind::Hgetex,
        b"HSETEX" if arity >= 4 => CommandAstKind::Hsetex,
        b"SADD" if arity >= 2 => CommandAstKind::Sadd,
        b"SREM" if arity >= 2 => CommandAstKind::Srem,
        b"SMEMBERS" if arity == 1 => CommandAstKind::Smembers,
        b"SISMEMBER" if arity == 2 => CommandAstKind::Sismember,
        b"SMISMEMBER" if arity >= 2 => CommandAstKind::Smismember,
        b"SCARD" if arity == 1 => CommandAstKind::Scard,
        b"SINTER" if arity >= 1 => CommandAstKind::Sinter,
        b"SUNION" if arity >= 1 => CommandAstKind::Sunion,
        b"SDIFF" if arity >= 1 => CommandAstKind::Sdiff,
        b"SDIFFSTORE" if arity >= 2 => CommandAstKind::Sdiffstore,
        b"SINTERSTORE" if arity >= 2 => CommandAstKind::Sinterstore,
        b"SUNIONSTORE" if arity >= 2 => CommandAstKind::Sunionstore,
        b"SINTERCARD" if arity >= 2 => CommandAstKind::Sintercard,
        b"SRANDMEMBER" if arity == 1 || arity == 2 => CommandAstKind::Srandmember,
        b"SPOP" if arity == 1 || arity == 2 => CommandAstKind::Spop,
        b"SMOVE" if arity == 3 => CommandAstKind::Smove,
        b"SSCAN" if arity >= 2 => CommandAstKind::Sscan,
        b"ZADD" if arity >= 3 => CommandAstKind::Zadd,
        b"ZREM" if arity >= 2 => CommandAstKind::Zrem,
        b"ZSCORE" if arity == 2 => CommandAstKind::Zscore,
        b"ZRANK" if arity == 2 => CommandAstKind::Zrank,
        b"ZREVRANK" if arity == 2 => CommandAstKind::Zrevrank,
        b"ZRANGE" if arity == 3 || arity == 4 => CommandAstKind::Zrange,
        b"ZREVRANGE" if arity == 3 || arity == 4 => CommandAstKind::Zrevrange,
        b"ZCARD" if arity == 1 => CommandAstKind::Zcard,
        b"ZINCRBY" if arity == 3 => CommandAstKind::Zincrby,
        b"ZCOUNT" if arity == 3 => CommandAstKind::Zcount,
        b"ZPOPMIN" if arity == 1 || arity == 2 => CommandAstKind::Zpopmin,
        b"ZPOPMAX" if arity == 1 || arity == 2 => CommandAstKind::Zpopmax,
        b"ZRANDMEMBER" if arity == 1 || arity == 2 || arity == 3 => CommandAstKind::Zrandmember,
        b"ZSCAN" if arity >= 2 => CommandAstKind::Zscan,
        b"ZMSCORE" if arity >= 2 => CommandAstKind::Zmscore,
        b"ZRANGEBYSCORE" if arity >= 3 => CommandAstKind::Zrangebyscore,
        b"ZREVRANGEBYSCORE" if arity >= 3 => CommandAstKind::Zrevrangebyscore,
        b"SETBIT" if arity == 3 => CommandAstKind::Setbit,
        b"GETBIT" if arity == 2 => CommandAstKind::Getbit,
        b"BITCOUNT" if arity == 1 || arity == 3 || arity == 4 => CommandAstKind::Bitcount,
        b"BITPOS" if (2..=5).contains(&arity) => CommandAstKind::Bitpos,
        b"BITOP" if arity >= 3 => CommandAstKind::Bitop,
        b"TYPE" if arity == 1 => CommandAstKind::Type,
        b"UNLINK" if arity >= 1 => CommandAstKind::Unlink,
        b"RENAME" if arity == 2 => CommandAstKind::Rename,
        b"RENAMENX" if arity == 2 => CommandAstKind::Renamenx,
        b"COPY" if arity >= 2 => CommandAstKind::Copy,
        b"RANDOMKEY" if arity == 0 => CommandAstKind::Randomkey,
        b"SCAN" if arity >= 1 => CommandAstKind::Scan,
        b"EXPIRETIME" if arity == 1 => CommandAstKind::Expiretime,
        b"PEXPIRETIME" if arity == 1 => CommandAstKind::Pexpiretime,
        b"OBJECT" if arity >= 1 => CommandAstKind::Object,
        b"WAIT" if arity == 2 => CommandAstKind::Wait,
        b"XADD" if arity >= 4 => CommandAstKind::Xadd,
        b"XLEN" if arity == 1 => CommandAstKind::Xlen,
        b"XRANGE" if arity >= 3 => CommandAstKind::Xrange,
        b"XREVRANGE" if arity >= 3 => CommandAstKind::Xrevrange,
        b"XREAD" if arity >= 3 => CommandAstKind::Xread,
        b"XTRIM" if arity >= 2 => CommandAstKind::Xtrim,
        b"XDEL" if arity >= 2 => CommandAstKind::Xdel,
        b"XINFO" if arity >= 2 => CommandAstKind::Xinfo,
        b"XGROUP" if arity >= 4 => CommandAstKind::Xgroup,
        b"XREADGROUP" if arity >= 6 => CommandAstKind::Xreadgroup,
        b"XACK" if arity >= 3 => CommandAstKind::Xack,
        b"JSON.SET" => CommandAstKind::JsonSet,
        b"JSON.GET" => CommandAstKind::JsonGet,
        b"JSON.DEL" => CommandAstKind::JsonDel,
        b"JSON.NUMINCRBY" => CommandAstKind::JsonNumincrby,
        b"JSON.TYPE" => CommandAstKind::JsonType,
        b"JSON.STRLEN" => CommandAstKind::JsonStrlen,
        b"JSON.OBJKEYS" => CommandAstKind::JsonObjkeys,
        b"JSON.OBJLEN" => CommandAstKind::JsonObjlen,
        b"JSON.ARRAPPEND" => CommandAstKind::JsonArrappend,
        b"JSON.ARRLEN" => CommandAstKind::JsonArrlen,
        b"JSON.TOGGLE" => CommandAstKind::JsonToggle,
        b"JSON.CLEAR" => CommandAstKind::JsonClear,
        b"JSON.MGET" => CommandAstKind::JsonMget,
        b"GEOADD" => CommandAstKind::Geoadd,
        b"GEOPOS" => CommandAstKind::Geopos,
        b"GEODIST" => CommandAstKind::Geodist,
        b"GEOHASH" => CommandAstKind::Geohash,
        b"GEOSEARCH" => CommandAstKind::Geosearch,
        b"GEOSEARCHSTORE" => CommandAstKind::Geosearchstore,
        b"PFADD" => CommandAstKind::Pfadd,
        b"PFCOUNT" => CommandAstKind::Pfcount,
        b"PFMERGE" => CommandAstKind::Pfmerge,
        b"BF.RESERVE" => CommandAstKind::BfReserve,
        b"BF.ADD" => CommandAstKind::BfAdd,
        b"BF.MADD" => CommandAstKind::BfMadd,
        b"BF.EXISTS" => CommandAstKind::BfExists,
        b"BF.MEXISTS" => CommandAstKind::BfMexists,
        b"BF.CARD" => CommandAstKind::BfCard,
        b"BF.INFO" => CommandAstKind::BfInfo,
        b"CF.RESERVE" => CommandAstKind::CfReserve,
        b"CF.ADD" => CommandAstKind::CfAdd,
        b"CF.ADDNX" => CommandAstKind::CfAddnx,
        b"CF.DEL" => CommandAstKind::CfDel,
        b"CF.EXISTS" => CommandAstKind::CfExists,
        b"CF.MEXISTS" => CommandAstKind::CfMexists,
        b"CF.COUNT" => CommandAstKind::CfCount,
        b"CF.INFO" => CommandAstKind::CfInfo,
        b"CMS.INITBYDIM" => CommandAstKind::CmsInitbydim,
        b"CMS.INITBYPROB" => CommandAstKind::CmsInitbyprob,
        b"CMS.INCRBY" => CommandAstKind::CmsIncrby,
        b"CMS.QUERY" => CommandAstKind::CmsQuery,
        b"CMS.MERGE" => CommandAstKind::CmsMerge,
        b"CMS.INFO" => CommandAstKind::CmsInfo,
        b"TOPK.RESERVE" => CommandAstKind::TopkReserve,
        b"TOPK.ADD" => CommandAstKind::TopkAdd,
        b"TOPK.INCRBY" => CommandAstKind::TopkIncrby,
        b"TOPK.QUERY" => CommandAstKind::TopkQuery,
        b"TOPK.LIST" => CommandAstKind::TopkList,
        b"TOPK.COUNT" => CommandAstKind::TopkCount,
        b"TOPK.INFO" => CommandAstKind::TopkInfo,
        b"TDIGEST.CREATE" => CommandAstKind::TdigestCreate,
        b"TDIGEST.ADD" => CommandAstKind::TdigestAdd,
        b"TDIGEST.RESET" => CommandAstKind::TdigestReset,
        b"TDIGEST.QUANTILE" => CommandAstKind::TdigestQuantile,
        b"TDIGEST.CDF" => CommandAstKind::TdigestCdf,
        b"TDIGEST.RANK" => CommandAstKind::TdigestRank,
        b"TDIGEST.REVRANK" => CommandAstKind::TdigestRevrank,
        b"TDIGEST.BYRANK" => CommandAstKind::TdigestByrank,
        b"TDIGEST.BYREVRANK" => CommandAstKind::TdigestByrevrank,
        b"TDIGEST.TRIMMED_MEAN" => CommandAstKind::TdigestTrimmedMean,
        b"TDIGEST.MIN" => CommandAstKind::TdigestMin,
        b"TDIGEST.MAX" => CommandAstKind::TdigestMax,
        b"TDIGEST.INFO" => CommandAstKind::TdigestInfo,
        b"TDIGEST.MERGE" => CommandAstKind::TdigestMerge,
        b"FLOW.CREATE" => CommandAstKind::FlowCreate,
        b"FLOW.CREATE_MANY" => CommandAstKind::FlowCreateMany,
        b"FLOW.GET" => CommandAstKind::FlowGet,
        b"FLOW.POLICY.SET" => CommandAstKind::FlowPolicySet,
        b"FLOW.POLICY.GET" => CommandAstKind::FlowPolicyGet,
        b"FLOW.CLAIM_DUE" => CommandAstKind::FlowClaimDue,
        b"FLOW.RECLAIM" => CommandAstKind::FlowReclaim,
        b"FLOW.COMPLETE" => CommandAstKind::FlowComplete,
        b"FLOW.COMPLETE_MANY" => CommandAstKind::FlowCompleteMany,
        b"FLOW.TRANSITION" => CommandAstKind::FlowTransition,
        b"FLOW.TRANSITION_MANY" => CommandAstKind::FlowTransitionMany,
        b"FLOW.RETRY" => CommandAstKind::FlowRetry,
        b"FLOW.RETRY_MANY" => CommandAstKind::FlowRetryMany,
        b"FLOW.FAIL" => CommandAstKind::FlowFail,
        b"FLOW.FAIL_MANY" => CommandAstKind::FlowFailMany,
        b"FLOW.CANCEL" => CommandAstKind::FlowCancel,
        b"FLOW.CANCEL_MANY" => CommandAstKind::FlowCancelMany,
        b"FLOW.REWIND" => CommandAstKind::FlowRewind,
        b"FLOW.LIST" => CommandAstKind::FlowList,
        b"FLOW.BY_PARENT" => CommandAstKind::FlowByParent,
        b"FLOW.BY_ROOT" => CommandAstKind::FlowByRoot,
        b"FLOW.BY_CORRELATION" => CommandAstKind::FlowByCorrelation,
        b"FLOW.INFO" => CommandAstKind::FlowInfo,
        b"FLOW.STUCK" => CommandAstKind::FlowStuck,
        b"FLOW.HISTORY" => CommandAstKind::FlowHistory,
        b"BLPOP" => CommandAstKind::Blpop,
        b"BRPOP" => CommandAstKind::Brpop,
        b"BLMOVE" => CommandAstKind::Blmove,
        b"BLMPOP" => CommandAstKind::Blmpop,
        b"HELLO" => CommandAstKind::Hello,
        b"AUTH" => CommandAstKind::Auth,
        b"ACL" => CommandAstKind::Acl,
        b"CLIENT" => CommandAstKind::Client,
        b"SANDBOX" => CommandAstKind::Sandbox,
        b"SUBSCRIBE" => CommandAstKind::Subscribe,
        b"UNSUBSCRIBE" => CommandAstKind::Unsubscribe,
        b"PSUBSCRIBE" => CommandAstKind::Psubscribe,
        b"PUNSUBSCRIBE" => CommandAstKind::Punsubscribe,
        b"MULTI" => CommandAstKind::Multi,
        b"EXEC" => CommandAstKind::Exec,
        b"DISCARD" => CommandAstKind::Discard,
        b"WATCH" => CommandAstKind::Watch,
        b"UNWATCH" => CommandAstKind::Unwatch,
        b"RESET" => CommandAstKind::Reset,
        b"QUIT" => CommandAstKind::Quit,
        _ => CommandAstKind::Unknown,
    }
}

fn make_command_ast<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    match classify_command_ast(cmd, args.len()) {
        CommandAstKind::Get => (atoms::get(), args[0]).encode(env),
        CommandAstKind::Set => (atoms::set(), args[0], args[1]).encode(env),
        CommandAstKind::Ping0 => atoms::ping().encode(env),
        CommandAstKind::Ping1 => (atoms::ping(), args[0]).encode(env),
        CommandAstKind::Del => (atoms::del(), args).encode(env),
        CommandAstKind::Exists => (atoms::exists(), args).encode(env),
        CommandAstKind::Mget => (atoms::mget(), args).encode(env),
        CommandAstKind::Mset => (atoms::mset(), args).encode(env),
        CommandAstKind::Incr => (atoms::incr(), args[0]).encode(env),
        CommandAstKind::Decr => (atoms::decr(), args[0]).encode(env),
        CommandAstKind::Incrby => {
            make_one_int_command_ast(env, atoms::incrby(), args, arg_bytes, 1)
        }
        CommandAstKind::Decrby => {
            make_one_int_command_ast(env, atoms::decrby(), args, arg_bytes, 1)
        }
        CommandAstKind::Incrbyfloat => make_float_command_ast(env, args, arg_bytes),
        CommandAstKind::Append => (atoms::append(), args[0], args[1]).encode(env),
        CommandAstKind::Strlen => (atoms::strlen(), args[0]).encode(env),
        CommandAstKind::Getset => (atoms::getset(), args[0], args[1]).encode(env),
        CommandAstKind::Getdel => (atoms::getdel(), args[0]).encode(env),
        CommandAstKind::Getex => make_getex_command_ast(env, args, arg_bytes),
        CommandAstKind::Setnx => (atoms::setnx(), args[0], args[1]).encode(env),
        CommandAstKind::Setex => make_ttl_value_command_ast(env, atoms::setex(), args, arg_bytes),
        CommandAstKind::Psetex => make_ttl_value_command_ast(env, atoms::psetex(), args, arg_bytes),
        CommandAstKind::Getrange => make_getrange_command_ast(env, args, arg_bytes),
        CommandAstKind::Setrange => {
            make_one_int_command_ast(env, atoms::setrange(), args, arg_bytes, 1)
        }
        CommandAstKind::Msetnx => (atoms::msetnx(), args).encode(env),
        CommandAstKind::Expire => make_expiry_command_ast(env, atoms::expire(), args, arg_bytes),
        CommandAstKind::Pexpire => make_expiry_command_ast(env, atoms::pexpire(), args, arg_bytes),
        CommandAstKind::Expireat => {
            make_expiry_command_ast(env, atoms::expireat(), args, arg_bytes)
        }
        CommandAstKind::Pexpireat => {
            make_expiry_command_ast(env, atoms::pexpireat(), args, arg_bytes)
        }
        CommandAstKind::Ttl => (atoms::ttl(), args[0]).encode(env),
        CommandAstKind::Pttl => (atoms::pttl(), args[0]).encode(env),
        CommandAstKind::Persist => (atoms::persist(), args[0]).encode(env),
        CommandAstKind::Lpush => (atoms::lpush(), args).encode(env),
        CommandAstKind::Rpush => (atoms::rpush(), args).encode(env),
        CommandAstKind::Lpop => {
            make_optional_count_command_ast(env, atoms::lpop(), args, arg_bytes)
        }
        CommandAstKind::Rpop => {
            make_optional_count_command_ast(env, atoms::rpop(), args, arg_bytes)
        }
        CommandAstKind::Lrange => make_two_int_command_ast(env, atoms::lrange(), args, arg_bytes),
        CommandAstKind::Llen => (atoms::llen(), args[0]).encode(env),
        CommandAstKind::Lindex => {
            make_one_int_command_ast(env, atoms::lindex(), args, arg_bytes, 1)
        }
        CommandAstKind::Lset => make_one_int_command_ast(env, atoms::lset(), args, arg_bytes, 1),
        CommandAstKind::Lrem => make_one_int_command_ast(env, atoms::lrem(), args, arg_bytes, 1),
        CommandAstKind::Ltrim => make_two_int_command_ast(env, atoms::ltrim(), args, arg_bytes),
        CommandAstKind::Linsert => make_linsert_command_ast(env, args, arg_bytes),
        CommandAstKind::Lmove => make_lmove_command_ast(env, args, arg_bytes),
        CommandAstKind::Lpushx => (atoms::lpushx(), args).encode(env),
        CommandAstKind::Rpushx => (atoms::rpushx(), args).encode(env),
        CommandAstKind::Rpoplpush => (atoms::rpoplpush(), args[0], args[1]).encode(env),
        CommandAstKind::Hset => (atoms::hset(), args).encode(env),
        CommandAstKind::Hget => (atoms::hget(), args[0], args[1]).encode(env),
        CommandAstKind::Hdel => (atoms::hdel(), args).encode(env),
        CommandAstKind::Hmget => (atoms::hmget(), args).encode(env),
        CommandAstKind::Hgetall => (atoms::hgetall(), args[0]).encode(env),
        CommandAstKind::Hexists => (atoms::hexists(), args[0], args[1]).encode(env),
        CommandAstKind::Hkeys => (atoms::hkeys(), args[0]).encode(env),
        CommandAstKind::Hvals => (atoms::hvals(), args[0]).encode(env),
        CommandAstKind::Hlen => (atoms::hlen(), args[0]).encode(env),
        CommandAstKind::Hincrby => {
            make_hash_one_int_command_ast(env, atoms::hincrby(), args, arg_bytes)
        }
        CommandAstKind::Hincrbyfloat => make_hash_float_command_ast(env, args, arg_bytes),
        CommandAstKind::Hsetnx => (atoms::hsetnx(), args[0], args[1], args[2]).encode(env),
        CommandAstKind::Hstrlen => (atoms::hstrlen(), args[0], args[1]).encode(env),
        CommandAstKind::Hrandfield => make_hrandfield_command_ast(env, args, arg_bytes),
        CommandAstKind::Hscan => make_scan_command_ast(env, atoms::hscan(), args, arg_bytes),
        CommandAstKind::Hexpire => {
            make_hash_ttl_fields_ast(env, atoms::hexpire(), args, arg_bytes, b"seconds")
        }
        CommandAstKind::Httl => make_hash_fields_ast(env, atoms::httl(), args, arg_bytes),
        CommandAstKind::Hpersist => make_hash_fields_ast(env, atoms::hpersist(), args, arg_bytes),
        CommandAstKind::Hpexpire => {
            make_hash_ttl_fields_ast(env, atoms::hpexpire(), args, arg_bytes, b"milliseconds")
        }
        CommandAstKind::Hpttl => make_hash_fields_ast(env, atoms::hpttl(), args, arg_bytes),
        CommandAstKind::Hexpiretime => {
            make_hash_fields_ast(env, atoms::hexpiretime(), args, arg_bytes)
        }
        CommandAstKind::Hgetdel => make_hash_fields_ast(env, atoms::hgetdel(), args, arg_bytes),
        CommandAstKind::Hgetex => make_hgetex_command_ast(env, args, arg_bytes),
        CommandAstKind::Hsetex => make_hsetex_command_ast(env, args, arg_bytes),
        CommandAstKind::Sadd => (atoms::sadd(), args).encode(env),
        CommandAstKind::Srem => (atoms::srem(), args).encode(env),
        CommandAstKind::Smembers => (atoms::smembers(), args[0]).encode(env),
        CommandAstKind::Sismember => (atoms::sismember(), args[0], args[1]).encode(env),
        CommandAstKind::Smismember => (atoms::smismember(), args).encode(env),
        CommandAstKind::Scard => (atoms::scard(), args[0]).encode(env),
        CommandAstKind::Sinter => (atoms::sinter(), args).encode(env),
        CommandAstKind::Sunion => (atoms::sunion(), args).encode(env),
        CommandAstKind::Sdiff => (atoms::sdiff(), args).encode(env),
        CommandAstKind::Sdiffstore => (atoms::sdiffstore(), args).encode(env),
        CommandAstKind::Sinterstore => (atoms::sinterstore(), args).encode(env),
        CommandAstKind::Sunionstore => (atoms::sunionstore(), args).encode(env),
        CommandAstKind::Sintercard => make_sintercard_command_ast(env, args, arg_bytes),
        CommandAstKind::Srandmember => {
            make_set_optional_count_command_ast(env, atoms::srandmember(), args, arg_bytes, true)
        }
        CommandAstKind::Spop => {
            make_set_optional_count_command_ast(env, atoms::spop(), args, arg_bytes, false)
        }
        CommandAstKind::Smove => (atoms::smove(), args[0], args[1], args[2]).encode(env),
        CommandAstKind::Sscan => make_sscan_command_ast(env, args, arg_bytes),
        CommandAstKind::Zadd => make_zadd_command_ast(env, args, arg_bytes),
        CommandAstKind::Zrem => (atoms::zrem(), args).encode(env),
        CommandAstKind::Zscore => (atoms::zscore(), args[0], args[1]).encode(env),
        CommandAstKind::Zrank => (atoms::zrank(), args[0], args[1]).encode(env),
        CommandAstKind::Zrevrank => (atoms::zrevrank(), args[0], args[1]).encode(env),
        CommandAstKind::Zrange => make_zrange_command_ast(env, atoms::zrange(), args, arg_bytes),
        CommandAstKind::Zrevrange => {
            make_zrange_command_ast(env, atoms::zrevrange(), args, arg_bytes)
        }
        CommandAstKind::Zcard => (atoms::zcard(), args[0]).encode(env),
        CommandAstKind::Zincrby => make_zincrby_command_ast(env, args, arg_bytes),
        CommandAstKind::Zcount => make_zcount_command_ast(env, args, arg_bytes),
        CommandAstKind::Zpopmin => {
            make_set_optional_count_command_ast(env, atoms::zpopmin(), args, arg_bytes, false)
        }
        CommandAstKind::Zpopmax => {
            make_set_optional_count_command_ast(env, atoms::zpopmax(), args, arg_bytes, false)
        }
        CommandAstKind::Zrandmember => make_zrandmember_command_ast(env, args, arg_bytes),
        CommandAstKind::Zscan => make_zscan_command_ast(env, args, arg_bytes),
        CommandAstKind::Zmscore => (atoms::zmscore(), args).encode(env),
        CommandAstKind::Zrangebyscore => {
            make_zrangebyscore_command_ast(env, atoms::zrangebyscore(), args, arg_bytes)
        }
        CommandAstKind::Zrevrangebyscore => {
            make_zrangebyscore_command_ast(env, atoms::zrevrangebyscore(), args, arg_bytes)
        }
        CommandAstKind::Setbit => make_setbit_command_ast(env, args, arg_bytes),
        CommandAstKind::Getbit => make_getbit_command_ast(env, args, arg_bytes),
        CommandAstKind::Bitcount => make_bitcount_command_ast(env, args, arg_bytes),
        CommandAstKind::Bitpos => make_bitpos_command_ast(env, args, arg_bytes),
        CommandAstKind::Bitop => make_bitop_command_ast(env, args, arg_bytes),
        CommandAstKind::Type => (atom(env, "type"), args[0]).encode(env),
        CommandAstKind::Unlink => (atom(env, "unlink"), args).encode(env),
        CommandAstKind::Rename => (atom(env, "rename"), args[0], args[1]).encode(env),
        CommandAstKind::Renamenx => (atom(env, "renamenx"), args[0], args[1]).encode(env),
        CommandAstKind::Copy => make_copy_command_ast(env, args, arg_bytes),
        CommandAstKind::Randomkey => (atom(env, "randomkey"), Vec::<Term<'a>>::new()).encode(env),
        CommandAstKind::Scan => make_generic_scan_command_ast(env, args, arg_bytes),
        CommandAstKind::Expiretime => (atom(env, "expiretime"), args[0]).encode(env),
        CommandAstKind::Pexpiretime => (atom(env, "pexpiretime"), args[0]).encode(env),
        CommandAstKind::Object => make_object_command_ast(env, args, arg_bytes),
        CommandAstKind::Wait => (atom(env, "wait"), args[0], args[1]).encode(env),
        CommandAstKind::Xadd => make_xadd_command_ast(env, args, arg_bytes),
        CommandAstKind::Xlen => (atom(env, "xlen"), args[0]).encode(env),
        CommandAstKind::Xrange => {
            make_xrange_command_ast(env, atom(env, "xrange"), args, arg_bytes)
        }
        CommandAstKind::Xrevrange => make_xrevrange_command_ast(env, args, arg_bytes),
        CommandAstKind::Xread => make_xread_command_ast(env, args, arg_bytes),
        CommandAstKind::Xtrim => make_xtrim_command_ast(env, args, arg_bytes),
        CommandAstKind::Xdel => (atom(env, "xdel"), args[0], args[1..].to_vec()).encode(env),
        CommandAstKind::Xinfo => make_xinfo_command_ast(env, args, arg_bytes),
        CommandAstKind::Xgroup => make_xgroup_command_ast(env, args, arg_bytes),
        CommandAstKind::Xreadgroup => make_xreadgroup_command_ast(env, args, arg_bytes),
        CommandAstKind::Xack => {
            (atom(env, "xack"), args[0], args[1], args[2..].to_vec()).encode(env)
        }
        CommandAstKind::JsonSet => make_json_set_command_ast(env, args, arg_bytes),
        CommandAstKind::JsonGet => make_json_get_command_ast(env, args, arg_bytes),
        CommandAstKind::JsonDel => make_json_path_command_ast(
            env,
            atom(env, "json_del"),
            args,
            arg_bytes,
            b"json.del",
            true,
        ),
        CommandAstKind::JsonNumincrby => make_json_numincrby_command_ast(env, args, arg_bytes),
        CommandAstKind::JsonType => make_json_path_command_ast(
            env,
            atom(env, "json_type"),
            args,
            arg_bytes,
            b"json.type",
            true,
        ),
        CommandAstKind::JsonStrlen => make_json_path_command_ast(
            env,
            atom(env, "json_strlen"),
            args,
            arg_bytes,
            b"json.strlen",
            true,
        ),
        CommandAstKind::JsonObjkeys => make_json_path_command_ast(
            env,
            atom(env, "json_objkeys"),
            args,
            arg_bytes,
            b"json.objkeys",
            true,
        ),
        CommandAstKind::JsonObjlen => make_json_path_command_ast(
            env,
            atom(env, "json_objlen"),
            args,
            arg_bytes,
            b"json.objlen",
            true,
        ),
        CommandAstKind::JsonArrappend => make_json_arrappend_command_ast(env, args, arg_bytes),
        CommandAstKind::JsonArrlen => make_json_path_command_ast(
            env,
            atom(env, "json_arrlen"),
            args,
            arg_bytes,
            b"json.arrlen",
            true,
        ),
        CommandAstKind::JsonToggle => make_json_path_command_ast(
            env,
            atom(env, "json_toggle"),
            args,
            arg_bytes,
            b"json.toggle",
            false,
        ),
        CommandAstKind::JsonClear => make_json_path_command_ast(
            env,
            atom(env, "json_clear"),
            args,
            arg_bytes,
            b"json.clear",
            true,
        ),
        CommandAstKind::JsonMget => make_json_mget_command_ast(env, args, arg_bytes),
        CommandAstKind::Geoadd => make_geoadd_command_ast(env, args, arg_bytes),
        CommandAstKind::Geopos => make_geo_members_command_ast(env, atom(env, "geopos"), args),
        CommandAstKind::Geodist => make_geodist_command_ast(env, args, arg_bytes),
        CommandAstKind::Geohash => make_geo_members_command_ast(env, atom(env, "geohash"), args),
        CommandAstKind::Geosearch => make_geosearch_command_ast(env, args, arg_bytes),
        CommandAstKind::Geosearchstore => make_geosearchstore_command_ast(env, args, arg_bytes),
        CommandAstKind::Pfadd => make_hll_command_ast(env, atom(env, "pfadd"), args, b"pfadd", 1),
        CommandAstKind::Pfcount => {
            make_hll_command_ast(env, atom(env, "pfcount"), args, b"pfcount", 1)
        }
        CommandAstKind::Pfmerge => {
            make_hll_command_ast(env, atom(env, "pfmerge"), args, b"pfmerge", 2)
        }
        CommandAstKind::BfReserve => make_bf_reserve_command_ast(env, args, arg_bytes),
        CommandAstKind::BfAdd => make_min_arity_list_ast(env, "bf_add", args, b"bf.add", 2),
        CommandAstKind::BfMadd => make_min_arity_list_ast(env, "bf_madd", args, b"bf.madd", 2),
        CommandAstKind::BfExists => {
            make_min_arity_list_ast(env, "bf_exists", args, b"bf.exists", 2)
        }
        CommandAstKind::BfMexists => {
            make_min_arity_list_ast(env, "bf_mexists", args, b"bf.mexists", 2)
        }
        CommandAstKind::BfCard => make_exact_arity_list_ast(env, "bf_card", args, b"bf.card", 1),
        CommandAstKind::BfInfo => make_exact_arity_list_ast(env, "bf_info", args, b"bf.info", 1),
        CommandAstKind::CfReserve => make_cf_reserve_command_ast(env, args, arg_bytes),
        CommandAstKind::CfAdd => make_min_arity_list_ast(env, "cf_add", args, b"cf.add", 2),
        CommandAstKind::CfAddnx => make_min_arity_list_ast(env, "cf_addnx", args, b"cf.addnx", 2),
        CommandAstKind::CfDel => make_min_arity_list_ast(env, "cf_del", args, b"cf.del", 2),
        CommandAstKind::CfExists => {
            make_min_arity_list_ast(env, "cf_exists", args, b"cf.exists", 2)
        }
        CommandAstKind::CfMexists => {
            make_min_arity_list_ast(env, "cf_mexists", args, b"cf.mexists", 2)
        }
        CommandAstKind::CfCount => make_exact_arity_list_ast(env, "cf_count", args, b"cf.count", 2),
        CommandAstKind::CfInfo => make_exact_arity_list_ast(env, "cf_info", args, b"cf.info", 1),
        CommandAstKind::CmsInitbydim => make_cms_initbydim_command_ast(env, args, arg_bytes),
        CommandAstKind::CmsInitbyprob => make_cms_initbyprob_command_ast(env, args, arg_bytes),
        CommandAstKind::CmsIncrby => make_count_pairs_command_ast(
            env,
            "cms_incrby",
            args,
            arg_bytes,
            b"cms.incrby",
            b"ERR CMS: invalid count value",
        ),
        CommandAstKind::CmsQuery => {
            make_min_arity_list_ast(env, "cms_query", args, b"cms.query", 2)
        }
        CommandAstKind::CmsMerge => make_cms_merge_command_ast(env, args, arg_bytes),
        CommandAstKind::CmsInfo => make_exact_arity_list_ast(env, "cms_info", args, b"cms.info", 1),
        CommandAstKind::TopkReserve => make_topk_reserve_command_ast(env, args, arg_bytes),
        CommandAstKind::TopkAdd => make_min_arity_list_ast(env, "topk_add", args, b"topk.add", 2),
        CommandAstKind::TopkIncrby => make_count_pairs_command_ast(
            env,
            "topk_incrby",
            args,
            arg_bytes,
            b"topk.incrby",
            b"ERR TOPK: invalid count value",
        ),
        CommandAstKind::TopkQuery => {
            make_min_arity_list_ast(env, "topk_query", args, b"topk.query", 2)
        }
        CommandAstKind::TopkList => make_topk_list_command_ast(env, args, arg_bytes),
        CommandAstKind::TopkCount => {
            make_min_arity_list_ast(env, "topk_count", args, b"topk.count", 2)
        }
        CommandAstKind::TopkInfo => {
            make_exact_arity_list_ast(env, "topk_info", args, b"topk.info", 1)
        }
        CommandAstKind::TdigestCreate => make_tdigest_create_command_ast(env, args, arg_bytes),
        CommandAstKind::TdigestAdd => make_float_list_command_ast(
            env,
            "tdigest_add",
            args,
            arg_bytes,
            b"tdigest.add",
            2,
            false,
        ),
        CommandAstKind::TdigestReset => {
            make_exact_arity_list_ast(env, "tdigest_reset", args, b"tdigest.reset", 1)
        }
        CommandAstKind::TdigestQuantile => make_float_list_command_ast(
            env,
            "tdigest_quantile",
            args,
            arg_bytes,
            b"tdigest.quantile",
            2,
            true,
        ),
        CommandAstKind::TdigestCdf => make_float_list_command_ast(
            env,
            "tdigest_cdf",
            args,
            arg_bytes,
            b"tdigest.cdf",
            2,
            false,
        ),
        CommandAstKind::TdigestRank => make_float_list_command_ast(
            env,
            "tdigest_rank",
            args,
            arg_bytes,
            b"tdigest.rank",
            2,
            false,
        ),
        CommandAstKind::TdigestRevrank => make_float_list_command_ast(
            env,
            "tdigest_revrank",
            args,
            arg_bytes,
            b"tdigest.revrank",
            2,
            false,
        ),
        CommandAstKind::TdigestByrank => {
            make_int_list_command_ast(env, "tdigest_byrank", args, arg_bytes, b"tdigest.byrank", 2)
        }
        CommandAstKind::TdigestByrevrank => make_int_list_command_ast(
            env,
            "tdigest_byrevrank",
            args,
            arg_bytes,
            b"tdigest.byrevrank",
            2,
        ),
        CommandAstKind::TdigestTrimmedMean => {
            make_tdigest_trimmed_mean_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::TdigestMin => {
            make_exact_arity_list_ast(env, "tdigest_min", args, b"tdigest.min", 1)
        }
        CommandAstKind::TdigestMax => {
            make_exact_arity_list_ast(env, "tdigest_max", args, b"tdigest.max", 1)
        }
        CommandAstKind::TdigestInfo => {
            make_exact_arity_list_ast(env, "tdigest_info", args, b"tdigest.info", 1)
        }
        CommandAstKind::TdigestMerge => make_tdigest_merge_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCreate => make_flow_create_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCreateMany => make_flow_create_many_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowGet => make_flow_get_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowPolicySet => make_flow_policy_set_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowPolicyGet => make_flow_policy_get_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowClaimDue => make_flow_claim_due_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowReclaim => make_flow_reclaim_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowComplete => make_flow_complete_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCompleteMany => {
            make_flow_complete_many_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::FlowTransition => make_flow_transition_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowTransitionMany => {
            make_flow_transition_many_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::FlowRetry => make_flow_retry_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowRetryMany => make_flow_retry_many_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowFail => make_flow_fail_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowFailMany => make_flow_fail_many_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCancel => make_flow_cancel_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCancelMany => make_flow_cancel_many_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowRewind => make_flow_rewind_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowList => make_flow_list_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowByParent => make_flow_index_query_command_ast(
            env,
            "flow_by_parent",
            b"flow.by_parent",
            args,
            arg_bytes,
        ),
        CommandAstKind::FlowByRoot => {
            make_flow_index_query_command_ast(env, "flow_by_root", b"flow.by_root", args, arg_bytes)
        }
        CommandAstKind::FlowByCorrelation => make_flow_index_query_command_ast(
            env,
            "flow_by_correlation",
            b"flow.by_correlation",
            args,
            arg_bytes,
        ),
        CommandAstKind::FlowInfo => make_flow_info_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowStuck => make_flow_stuck_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowHistory => make_flow_history_command_ast(env, args, arg_bytes),
        CommandAstKind::Blpop => make_blocking_pop_command_ast(
            env,
            atoms::blpop(),
            b"ERR wrong number of arguments for 'blpop' command",
            args,
            arg_bytes,
        ),
        CommandAstKind::Brpop => make_blocking_pop_command_ast(
            env,
            atoms::brpop(),
            b"ERR wrong number of arguments for 'brpop' command",
            args,
            arg_bytes,
        ),
        CommandAstKind::Blmove => make_blmove_command_ast(env, args, arg_bytes),
        CommandAstKind::Blmpop => make_blmpop_command_ast(env, args, arg_bytes),
        CommandAstKind::Hello => make_hello_command_ast(env, args, arg_bytes),
        CommandAstKind::Auth => make_auth_command_ast(env, args),
        CommandAstKind::Acl => make_upper_subcommand_ast(
            env,
            "acl",
            b"ERR wrong number of arguments for 'acl' command",
            args,
            arg_bytes,
        ),
        CommandAstKind::Client => make_client_command_ast(env, args, arg_bytes),
        CommandAstKind::Sandbox => make_upper_subcommand_ast(
            env,
            "sandbox",
            b"ERR unknown SANDBOX subcommand",
            args,
            arg_bytes,
        ),
        CommandAstKind::Subscribe => make_nonempty_list_ast(
            env,
            "subscribe",
            b"ERR wrong number of arguments for 'subscribe' command",
            args,
        ),
        CommandAstKind::Unsubscribe => (atom(env, "unsubscribe"), args).encode(env),
        CommandAstKind::Psubscribe => make_nonempty_list_ast(
            env,
            "psubscribe",
            b"ERR wrong number of arguments for 'psubscribe' command",
            args,
        ),
        CommandAstKind::Punsubscribe => (atom(env, "punsubscribe"), args).encode(env),
        CommandAstKind::Multi => make_zero_arity_atom_or_error(
            env,
            "multi",
            b"ERR wrong number of arguments for 'multi' command",
            args,
        ),
        CommandAstKind::Exec => make_zero_arity_atom_or_error(
            env,
            "exec",
            b"ERR wrong number of arguments for 'exec' command",
            args,
        ),
        CommandAstKind::Discard => make_zero_arity_atom_or_error(
            env,
            "discard",
            b"ERR wrong number of arguments for 'discard' command",
            args,
        ),
        CommandAstKind::Watch => make_nonempty_list_ast(
            env,
            "watch",
            b"ERR wrong number of arguments for 'watch' command",
            args,
        ),
        CommandAstKind::Unwatch => make_zero_arity_atom_or_error(
            env,
            "unwatch",
            b"ERR wrong number of arguments for 'unwatch' command",
            args,
        ),
        CommandAstKind::Reset => make_zero_arity_atom_or_error(
            env,
            "reset",
            b"ERR wrong number of arguments for 'reset' command",
            args,
        ),
        CommandAstKind::Quit => make_zero_arity_atom_or_error(
            env,
            "quit",
            b"ERR wrong number of arguments for 'quit' command",
            args,
        ),
        CommandAstKind::Unknown if cmd == b"SET" && args.len() >= 2 => {
            make_set_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::Unknown if upper_first_arg_command(cmd) => {
            make_upper_first_arg_ast(env, cmd, args, arg_bytes)
        }
        CommandAstKind::Unknown if upper_all_args_command(cmd) => {
            make_upper_all_args_ast(env, cmd, args, arg_bytes)
        }
        CommandAstKind::Unknown if native_typed_command(cmd) => {
            make_native_typed_ast(env, cmd, args, arg_bytes)
        }
        CommandAstKind::Unknown => match command_tag_name(cmd) {
            Some(tag) => (Atom::from_str(env, tag).unwrap(), args).encode(env),
            None => (atoms::unknown(), make_binary_term(env, cmd), args).encode(env),
        },
    }
}

fn upper_first_arg_command(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"COMMAND"
            | b"DEBUG"
            | b"CONFIG"
            | b"MODULE"
            | b"SLOWLOG"
            | b"MEMORY"
            | b"FERRICSTORE.CONFIG"
            | b"PUBSUB"
            | b"LOLWUT"
    )
}

fn upper_all_args_command(cmd: &[u8]) -> bool {
    matches!(cmd, b"FLUSHDB" | b"FLUSHALL")
}

fn native_typed_command(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"CAS"
            | b"LOCK"
            | b"UNLOCK"
            | b"EXTEND"
            | b"RATELIMIT.ADD"
            | b"FETCH_OR_COMPUTE"
            | b"FETCH_OR_COMPUTE_RESULT"
            | b"FETCH_OR_COMPUTE_ERROR"
            | b"FERRICSTORE.KEY_INFO"
    )
}

fn make_upper_first_arg_ast<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let Some(tag) = command_tag_name(cmd) else {
        return (atoms::unknown(), make_binary_term(env, cmd), args).encode(env);
    };

    if args.is_empty() {
        return (Atom::from_str(env, tag).unwrap(), Vec::<Term<'a>>::new()).encode(env);
    }

    let mut parsed = Vec::with_capacity(args.len());
    parsed.push(uppercase_binary_term(env, arg_bytes[0]));
    parsed.extend_from_slice(&args[1..]);
    (Atom::from_str(env, tag).unwrap(), parsed).encode(env)
}

fn make_upper_all_args_ast<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let Some(tag) = command_tag_name(cmd) else {
        return (atoms::unknown(), make_binary_term(env, cmd), args).encode(env);
    };

    let parsed: Vec<Term<'a>> = arg_bytes
        .iter()
        .map(|arg| uppercase_binary_term(env, arg))
        .collect();
    (Atom::from_str(env, tag).unwrap(), parsed).encode(env)
}

fn make_native_typed_ast<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    match cmd {
        b"CAS" => make_cas_command_ast(env, args, arg_bytes),
        b"LOCK" => make_lock_like_command_ast(env, atom(env, "lock"), b"lock", args, arg_bytes),
        b"UNLOCK" => make_unlock_command_ast(env, args),
        b"EXTEND" => {
            make_lock_like_command_ast(env, atom(env, "extend"), b"extend", args, arg_bytes)
        }
        b"RATELIMIT.ADD" => make_ratelimit_add_command_ast(env, args, arg_bytes),
        b"FETCH_OR_COMPUTE" => make_fetch_or_compute_command_ast(env, args, arg_bytes),
        b"FETCH_OR_COMPUTE_RESULT" => {
            make_fetch_or_compute_result_command_ast(env, args, arg_bytes)
        }
        b"FETCH_OR_COMPUTE_ERROR" => make_fetch_or_compute_error_command_ast(env, args),
        b"FERRICSTORE.KEY_INFO" => make_key_info_command_ast(env, args),
        _ => (atoms::unknown(), make_binary_term(env, cmd), args).encode(env),
    }
}

fn make_cas_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    let tag = atom(env, "cas");
    match args.len() {
        3 => (tag, args[0], args[1], args[2], atoms::nil()).encode(env),
        5 if ascii_eq_ignore_case(arg_bytes[3], b"EX") => {
            match parse_positive_ms_from_seconds_ast(env, arg_bytes[4]) {
                Ok(ttl_ms) => (tag, args[0], args[1], args[2], ttl_ms).encode(env),
                Err(err) => (tag, err).encode(env),
            }
        }
        _ => (tag, wrong_number_error(env, b"cas")).encode(env),
    }
}

fn make_lock_like_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    name: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    if args.len() != 3 {
        return (tag, wrong_number_error(env, name)).encode(env);
    }

    match parse_positive_int_ast_arg(env, arg_bytes[2], b"value") {
        Ok(ttl_ms) => (tag, args[0], args[1], ttl_ms).encode(env),
        Err(_) => (
            tag,
            args[0],
            args[1],
            generic_ast_error(env, b"ERR value is not an integer or out of range"),
        )
            .encode(env),
    }
}

fn make_unlock_command_ast<'a>(env: Env<'a>, args: &[Term<'a>]) -> Term<'a> {
    let tag = atom(env, "unlock");
    if args.len() == 2 {
        (tag, args[0], args[1]).encode(env)
    } else {
        (tag, wrong_number_error(env, b"unlock")).encode(env)
    }
}

fn make_ratelimit_add_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "ratelimit_add");
    if args.len() != 3 && args.len() != 4 {
        return (tag, wrong_number_error(env, b"ratelimit.add")).encode(env);
    }

    let count_arg = if args.len() == 4 { arg_bytes[3] } else { b"1" };
    let parsed = (
        parse_positive_int_ast_arg(env, arg_bytes[1], b"value"),
        parse_positive_int_ast_arg(env, arg_bytes[2], b"value"),
        parse_positive_int_ast_arg(env, count_arg, b"value"),
    );

    match parsed {
        (Ok(window_ms), Ok(max), Ok(count)) => (tag, args[0], window_ms, max, count).encode(env),
        _ => (
            tag,
            generic_ast_error(env, b"ERR value is not an integer or out of range"),
        )
            .encode(env),
    }
}

fn make_fetch_or_compute_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "fetch_or_compute");
    if args.len() != 2 && args.len() != 3 {
        return (tag, wrong_number_error(env, b"fetch_or_compute")).encode(env);
    }

    match parse_positive_int_ast_arg(env, arg_bytes[1], b"value") {
        Ok(ttl_ms) => {
            let hint = if args.len() == 3 {
                args[2]
            } else {
                make_binary_term(env, b"")
            };
            (tag, args[0], ttl_ms, hint).encode(env)
        }
        Err(_) => (
            tag,
            args[0],
            generic_ast_error(env, b"ERR value is not an integer or out of range"),
        )
            .encode(env),
    }
}

fn make_fetch_or_compute_result_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "fetch_or_compute_result");
    if args.len() != 3 {
        return (tag, wrong_number_error(env, b"fetch_or_compute_result")).encode(env);
    }

    match parse_int_ast_arg(env, arg_bytes[2]) {
        Ok(ttl_ms) if ttl_ms >= 0 => (tag, args[0], args[1], ttl_ms).encode(env),
        _ => (
            tag,
            args[0],
            generic_ast_error(env, b"ERR value is not an integer or out of range"),
        )
            .encode(env),
    }
}

fn make_fetch_or_compute_error_command_ast<'a>(env: Env<'a>, args: &[Term<'a>]) -> Term<'a> {
    let tag = atom(env, "fetch_or_compute_error");
    if args.len() == 2 {
        (tag, args[0], args[1]).encode(env)
    } else {
        (tag, wrong_number_error(env, b"fetch_or_compute_error")).encode(env)
    }
}

fn make_key_info_command_ast<'a>(env: Env<'a>, args: &[Term<'a>]) -> Term<'a> {
    let tag = atom(env, "ferricstore_key_info");
    if args.len() == 1 {
        (tag, args[0]).encode(env)
    } else {
        (tag, wrong_number_error(env, b"key_info")).encode(env)
    }
}

fn parse_positive_ms_from_seconds_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<i64, Term<'a>> {
    match parse_int_bytes(data) {
        Some(value) if value > 0 && value <= i64::MAX / 1000 => Ok(value * 1000),
        _ => Err(generic_ast_error(
            env,
            b"ERR value is not an integer or out of range",
        )),
    }
}

fn make_one_int_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    int_idx: usize,
) -> Term<'a> {
    debug_assert!(args.len() > int_idx);

    match parse_int_ast_arg(env, arg_bytes[int_idx]) {
        Ok(value) => match args.len() {
            2 => (tag, args[0], value).encode(env),
            3 => (tag, args[0], value, args[2]).encode(env),
            _ => (tag, args).encode(env),
        },
        Err(err) => match args.len() {
            2 => (tag, args[0], err).encode(env),
            3 => (tag, args[0], err, args[2]).encode(env),
            _ => (tag, args).encode(env),
        },
    }
}

fn make_optional_count_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() == 1 || args.len() == 2);

    if args.len() == 1 {
        return (tag, args[0]).encode(env);
    }

    match parse_int_ast_arg(env, arg_bytes[1]) {
        Ok(value) => (tag, args[0], value).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_two_int_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() == 3);

    let first = parse_int_ast_arg(env, arg_bytes[1]);
    let second = parse_int_ast_arg(env, arg_bytes[2]);

    match (first, second) {
        (Ok(first), Ok(second)) => (tag, args[0], first, second).encode(env),
        (Err(err), _) | (_, Err(err)) => (tag, args[0], err, args[2]).encode(env),
    }
}

fn make_float_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 2);

    match parse_float_ast_arg(env, arg_bytes[1]) {
        Ok(value) => (atoms::incrbyfloat(), args[0], value).encode(env),
        Err(err) => (atoms::incrbyfloat(), args[0], err).encode(env),
    }
}

fn make_ttl_value_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() == 3);

    match parse_int_ast_arg(env, arg_bytes[1]) {
        Ok(value) => (tag, args[0], value, args[2]).encode(env),
        Err(err) => (tag, args[0], err, args[2]).encode(env),
    }
}

fn make_getex_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!((1..=3).contains(&args.len()));

    if args.len() == 1 {
        return (atoms::getex(), args[0]).encode(env);
    }

    if args.len() == 2 {
        return if ascii_eq_ignore_case(arg_bytes[1], b"PERSIST") {
            (atoms::getex(), args[0], atoms::persist()).encode(env)
        } else {
            (
                atoms::getex(),
                args[0],
                generic_ast_error(env, b"ERR syntax error"),
            )
                .encode(env)
        };
    }

    let opt = arg_bytes[1];
    let Some(tag) = getex_expiry_atom(opt) else {
        return (
            atoms::getex(),
            args[0],
            generic_ast_error(env, b"ERR syntax error"),
        )
            .encode(env);
    };

    match parse_int_ast_arg(env, arg_bytes[2]) {
        Ok(value) if value > 0 => (atoms::getex(), args[0], (tag, value)).encode(env),
        Ok(_) => (
            atoms::getex(),
            args[0],
            generic_ast_error(env, b"ERR invalid expire time in 'getex' command"),
        )
            .encode(env),
        Err(err) => (atoms::getex(), args[0], err).encode(env),
    }
}

fn getex_expiry_atom(opt: &[u8]) -> Option<Atom> {
    if ascii_eq_ignore_case(opt, b"EX") {
        Some(atoms::ex())
    } else if ascii_eq_ignore_case(opt, b"PX") {
        Some(atoms::px())
    } else if ascii_eq_ignore_case(opt, b"EXAT") {
        Some(atoms::exat())
    } else if ascii_eq_ignore_case(opt, b"PXAT") {
        Some(atoms::pxat())
    } else {
        None
    }
}

fn make_linsert_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 4);

    match parse_linsert_direction_ast(env, arg_bytes[1]) {
        Ok(direction) => (atoms::linsert(), args[0], direction, args[2], args[3]).encode(env),
        Err(err) => (atoms::linsert(), args[0], err, args[2], args[3]).encode(env),
    }
}

fn make_lmove_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 4);

    let from_dir = parse_lr_direction_ast(env, arg_bytes[2]);
    let to_dir = parse_lr_direction_ast(env, arg_bytes[3]);

    match (from_dir, to_dir) {
        (Ok(from_dir), Ok(to_dir)) => {
            (atoms::lmove(), args[0], args[1], from_dir, to_dir).encode(env)
        }
        (Err(err), Ok(to_dir)) => (atoms::lmove(), args[0], args[1], err, to_dir).encode(env),
        (Ok(from_dir), Err(err)) => (atoms::lmove(), args[0], args[1], from_dir, err).encode(env),
        (Err(err), Err(_)) => (atoms::lmove(), args[0], args[1], err, args[3]).encode(env),
    }
}

fn parse_linsert_direction_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Atom, Term<'a>> {
    if ascii_eq_ignore_case(data, b"BEFORE") {
        Ok(atoms::before())
    } else if ascii_eq_ignore_case(data, b"AFTER") {
        Ok(atoms::after())
    } else {
        Err(generic_ast_error(env, b"ERR syntax error"))
    }
}

fn parse_lr_direction_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Atom, Term<'a>> {
    if ascii_eq_ignore_case(data, b"LEFT") {
        Ok(atoms::left())
    } else if ascii_eq_ignore_case(data, b"RIGHT") {
        Ok(atoms::right())
    } else {
        Err(generic_ast_error(env, b"ERR syntax error"))
    }
}

fn make_blocking_pop_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    arity_error: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    if args.len() < 2 {
        return (tag, generic_ast_error(env, arity_error)).encode(env);
    }

    let timeout_idx = arg_bytes.len() - 1;
    match parse_timeout_ms_ast(env, arg_bytes[timeout_idx]) {
        Ok(timeout_ms) => (tag, &args[..timeout_idx], timeout_ms).encode(env),
        Err(err) => (tag, err).encode(env),
    }
}

fn make_blmove_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    if args.len() != 5 {
        return (
            atoms::blmove(),
            generic_ast_error(env, b"ERR wrong number of arguments for 'blmove' command"),
        )
            .encode(env);
    }

    let from_dir = parse_lr_direction_ast(env, arg_bytes[2]);
    let to_dir = parse_lr_direction_ast(env, arg_bytes[3]);
    let timeout_ms = parse_timeout_ms_ast(env, arg_bytes[4]);

    match (from_dir, to_dir, timeout_ms) {
        (Ok(from_dir), Ok(to_dir), Ok(timeout_ms)) => (
            atoms::blmove(),
            args[0],
            args[1],
            from_dir,
            to_dir,
            timeout_ms,
        )
            .encode(env),
        (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
            (atoms::blmove(), err).encode(env)
        }
    }
}

fn make_blmpop_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    if args.len() < 4 {
        return (
            atoms::blmpop(),
            generic_ast_error(env, b"ERR wrong number of arguments for 'blmpop' command"),
        )
            .encode(env);
    }

    let timeout_ms = match parse_timeout_ms_ast(env, arg_bytes[0]) {
        Ok(timeout_ms) => timeout_ms,
        Err(err) => return (atoms::blmpop(), err).encode(env),
    };

    let numkeys = match parse_int_bytes(arg_bytes[1]) {
        Some(value) if value > 0 => value as usize,
        Some(_) | None => {
            return (
                atoms::blmpop(),
                generic_ast_error(env, b"ERR value is not an integer or out of range"),
            )
                .encode(env)
        }
    };

    if args.len() < 2 + numkeys + 1 {
        return (atoms::blmpop(), generic_ast_error(env, b"ERR syntax error")).encode(env);
    }

    let dir_idx = 2 + numkeys;
    let direction = match parse_lr_direction_ast(env, arg_bytes[dir_idx]) {
        Ok(direction) => direction,
        Err(err) => return (atoms::blmpop(), err).encode(env),
    };

    let count = match parse_blmpop_count_ast(env, &arg_bytes[(dir_idx + 1)..]) {
        Ok(count) => count,
        Err(err) => return (atoms::blmpop(), err).encode(env),
    };

    (
        atoms::blmpop(),
        &args[2..dir_idx],
        direction,
        count,
        timeout_ms,
    )
        .encode(env)
}

fn parse_blmpop_count_ast<'a>(env: Env<'a>, arg_bytes: &[&[u8]]) -> Result<i64, Term<'a>> {
    match arg_bytes {
        [] => Ok(1),
        [keyword, count] if ascii_eq_ignore_case(keyword, b"COUNT") => {
            match parse_int_bytes(count) {
                Some(value) if value > 0 => Ok(value),
                _ => Err(generic_ast_error(
                    env,
                    b"ERR value is not an integer or out of range",
                )),
            }
        }
        _ => Err(generic_ast_error(env, b"ERR syntax error")),
    }
}

fn parse_timeout_ms_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<i64, Term<'a>> {
    let value = std::str::from_utf8(data)
        .ok()
        .and_then(|value| f64::from_str(value).ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| generic_ast_error(env, b"ERR timeout is not a float or out of range"))?;

    if value < 0.0 {
        return Err(generic_ast_error(env, b"ERR timeout is negative"));
    }

    if value > (i64::MAX as f64 / 1000.0) {
        return Err(generic_ast_error(
            env,
            b"ERR timeout is not a float or out of range",
        ));
    }

    Ok((value * 1000.0).trunc() as i64)
}

fn make_hello_command_ast<'a>(env: Env<'a>, _args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    match arg_bytes {
        [] => atom(env, "hello").encode(env),
        [version, ..] if *version == b"3" => (atom(env, "hello"), 3_i64).encode(env),
        [_version, ..] => (
            atom(env, "hello"),
            generic_ast_error(
                env,
                b"NOPROTO this server does not support the requested protocol version",
            ),
        )
            .encode(env),
    }
}

fn make_auth_command_ast<'a>(env: Env<'a>, args: &[Term<'a>]) -> Term<'a> {
    match args {
        [password] => (
            atom(env, "auth"),
            make_binary_term(env, b"default"),
            *password,
        )
            .encode(env),
        [username, password] => (atom(env, "auth"), *username, *password).encode(env),
        _ => (
            atom(env, "auth"),
            generic_ast_error(env, b"ERR wrong number of arguments for 'auth' command"),
        )
            .encode(env),
    }
}

fn make_client_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    match arg_bytes {
        [] => (
            atom(env, "client"),
            generic_ast_error(env, b"ERR wrong number of arguments for 'client' command"),
        )
            .encode(env),
        [subcmd, rest @ ..] if ascii_eq_ignore_case(subcmd, b"HELLO") => {
            make_hello_command_ast(env, &args[1..], rest)
        }
        [subcmd, ..] => (
            atom(env, "client"),
            uppercase_binary_term(env, subcmd),
            &args[1..],
        )
            .encode(env),
    }
}

fn make_upper_subcommand_ast<'a>(
    env: Env<'a>,
    tag: &str,
    empty_error: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    match arg_bytes {
        [] => (atom(env, tag), generic_ast_error(env, empty_error)).encode(env),
        [subcmd, ..] => (
            atom(env, tag),
            uppercase_binary_term(env, subcmd),
            &args[1..],
        )
            .encode(env),
    }
}

fn make_nonempty_list_ast<'a>(
    env: Env<'a>,
    tag: &str,
    empty_error: &[u8],
    args: &[Term<'a>],
) -> Term<'a> {
    if args.is_empty() {
        (atom(env, tag), generic_ast_error(env, empty_error)).encode(env)
    } else {
        (atom(env, tag), args).encode(env)
    }
}

fn make_zero_arity_atom_or_error<'a>(
    env: Env<'a>,
    tag: &str,
    arity_error: &[u8],
    args: &[Term<'a>],
) -> Term<'a> {
    if args.is_empty() {
        atom(env, tag).encode(env)
    } else {
        (atom(env, tag), generic_ast_error(env, arity_error)).encode(env)
    }
}

fn make_hash_one_int_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() == 3);

    match parse_int_ast_arg(env, arg_bytes[2]) {
        Ok(value) => (tag, args[0], args[1], value).encode(env),
        Err(err) => (tag, args[0], args[1], err).encode(env),
    }
}

fn make_hash_float_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() == 3);

    match parse_float_ast_arg(env, arg_bytes[2]) {
        Ok(value) => (atoms::hincrbyfloat(), args[0], args[1], value).encode(env),
        Err(err) => (atoms::hincrbyfloat(), args[0], args[1], err).encode(env),
    }
}

fn make_hrandfield_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!((1..=3).contains(&args.len()));

    if args.len() == 1 {
        return (atoms::hrandfield(), args[0]).encode(env);
    }

    let count = match parse_int_ast_arg(env, arg_bytes[1]) {
        Ok(value) => value,
        Err(err) => return (atoms::hrandfield(), args[0], err).encode(env),
    };

    if args.len() == 2 {
        return (atoms::hrandfield(), args[0], count).encode(env);
    }

    if ascii_eq_ignore_case(arg_bytes[2], b"WITHVALUES") {
        (atoms::hrandfield(), args[0], count, atoms::withvalues()).encode(env)
    } else {
        (
            atoms::hrandfield(),
            args[0],
            count,
            generic_ast_error(env, b"ERR syntax error"),
        )
            .encode(env)
    }
}

fn make_hash_ttl_fields_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    label: &[u8],
) -> Term<'a> {
    debug_assert!(args.len() >= 4);

    let fields = match parse_hash_fields_tail(env, args, arg_bytes, 3, 2) {
        Ok(fields) => fields,
        Err(err) => return (tag, args[0], err).encode(env),
    };

    match parse_positive_int_ast_arg(env, arg_bytes[1], label) {
        Ok(value) => (tag, args[0], value, fields).encode(env),
        Err(err) => (tag, args[0], err, fields).encode(env),
    }
}

fn make_hash_fields_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() >= 3);

    match parse_hash_fields_tail(env, args, arg_bytes, 2, 1) {
        Ok(fields) => (tag, args[0], fields).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_hgetex_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 4);

    if ascii_eq_ignore_case(arg_bytes[1], b"PERSIST") {
        return match parse_hash_fields_tail(env, args, arg_bytes, 3, 2) {
            Ok(fields) => (atoms::hgetex(), args[0], atoms::persist(), fields).encode(env),
            Err(err) => (atoms::hgetex(), args[0], err).encode(env),
        };
    }

    let Some(expiry_tag) = getex_expiry_atom(arg_bytes[1]) else {
        return (
            atoms::hgetex(),
            args[0],
            generic_ast_error(env, b"ERR wrong number of arguments for 'hgetex' command"),
        )
            .encode(env);
    };

    if args.len() < 5 {
        return (
            atoms::hgetex(),
            args[0],
            generic_ast_error(env, b"ERR wrong number of arguments for 'hgetex' command"),
        )
            .encode(env);
    }

    let fields = match parse_hash_fields_tail(env, args, arg_bytes, 4, 3) {
        Ok(fields) => fields,
        Err(err) => return (atoms::hgetex(), args[0], err).encode(env),
    };

    match parse_positive_int_ast_arg(env, arg_bytes[2], b"value") {
        Ok(value) => (atoms::hgetex(), args[0], (expiry_tag, value), fields).encode(env),
        Err(_err) => (
            atoms::hgetex(),
            args[0],
            generic_ast_error(env, b"ERR value is not an integer or out of range"),
            fields,
        )
            .encode(env),
    }
}

fn make_hsetex_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 4);

    let pairs: Vec<Term<'a>> = args[2..].to_vec();

    match parse_positive_int_ast_arg(env, arg_bytes[1], b"seconds") {
        Ok(value) => (atoms::hsetex(), args[0], value, pairs).encode(env),
        Err(err) => (atoms::hsetex(), args[0], err, pairs).encode(env),
    }
}

fn parse_hash_fields_tail<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    count_idx: usize,
    fields_keyword_idx: usize,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    if fields_keyword_idx >= arg_bytes.len()
        || !ascii_eq_ignore_case(arg_bytes[fields_keyword_idx], b"FIELDS")
        || count_idx >= arg_bytes.len()
    {
        return Err(generic_ast_error(
            env,
            b"ERR wrong number of arguments for hash field command",
        ));
    }

    let count = match parse_int_bytes(arg_bytes[count_idx]) {
        Some(value) if value > 0 => value as usize,
        _ => return Err(positive_int_ast_error(env, b"count")),
    };

    let fields_start = count_idx + 1;
    let actual = args.len().saturating_sub(fields_start);
    if actual != count {
        return Err(generic_ast_error(
            env,
            b"ERR number of fields does not match the count argument",
        ));
    }

    Ok(args[fields_start..].to_vec())
}

fn parse_positive_int_ast_arg<'a>(
    env: Env<'a>,
    data: &[u8],
    label: &[u8],
) -> Result<i64, Term<'a>> {
    match parse_int_bytes(data) {
        Some(value) if value > 0 => Ok(value),
        _ => Err(positive_int_ast_error(env, label)),
    }
}

fn positive_int_ast_error<'a>(env: Env<'a>, label: &[u8]) -> Term<'a> {
    let mut msg = b"ERR ".to_vec();
    msg.extend_from_slice(label);
    msg.extend_from_slice(b" is not a positive integer");
    generic_ast_error(env, &msg)
}

fn make_set_optional_count_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    allow_negative: bool,
) -> Term<'a> {
    debug_assert!(args.len() == 1 || args.len() == 2);

    if args.len() == 1 {
        return (tag, args[0]).encode(env);
    }

    match parse_int_ast_arg(env, arg_bytes[1]) {
        Ok(value) if allow_negative || value >= 0 => (tag, args[0], value).encode(env),
        Ok(_) => (
            tag,
            args[0],
            generic_ast_error(env, b"ERR value is not an integer or out of range"),
        )
            .encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_sscan_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 2);

    make_scan_command_ast(env, atoms::sscan(), args, arg_bytes)
}

fn make_zscan_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 2);

    make_scan_command_ast(env, atoms::zscan(), args, arg_bytes)
}

fn make_scan_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let cursor = match parse_int_ast_arg(env, arg_bytes[1]) {
        Ok(value) if value >= 0 => value,
        Ok(_) => return (tag, args[0], generic_ast_error(env, b"ERR invalid cursor")).encode(env),
        Err(_) => return (tag, args[0], generic_ast_error(env, b"ERR invalid cursor")).encode(env),
    };

    match parse_scan_opts_ast(env, args, arg_bytes, 2) {
        Ok(opts) => (tag, args[0], cursor, opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn parse_scan_opts_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::with_capacity(arg_bytes.len().saturating_sub(start) / 2);
    let mut idx = start;

    while idx < arg_bytes.len() {
        if idx + 1 >= arg_bytes.len() {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }

        if ascii_eq_ignore_case(arg_bytes[idx], b"MATCH") {
            opts.push((Atom::from_str(env, "match").unwrap(), args[idx + 1]).encode(env));
            idx += 2;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"COUNT") {
            match parse_int_ast_arg(env, arg_bytes[idx + 1]) {
                Ok(value) if value > 0 => opts.push((atoms::count(), value).encode(env)),
                _ => {
                    return Err(generic_ast_error(
                        env,
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
            idx += 2;
        } else {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }
    }

    Ok(opts)
}

fn make_sintercard_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() >= 2);

    let numkeys = match parse_int_bytes(arg_bytes[0]) {
        Some(value) if value > 0 => value as usize,
        _ => {
            return (
                atoms::sintercard(),
                generic_ast_error(env, b"ERR numkeys can't be non-positive value"),
            )
                .encode(env)
        }
    };

    if args.len() < 1 + numkeys {
        return (
            atoms::sintercard(),
            generic_ast_error(
                env,
                b"ERR Number of keys can't be greater than number of args",
            ),
        )
            .encode(env);
    }

    let keys = args[1..1 + numkeys].to_vec();
    let tail_start = 1 + numkeys;

    match args.len().saturating_sub(tail_start) {
        0 => (atoms::sintercard(), keys, 0_i64).encode(env),
        2 if ascii_eq_ignore_case(arg_bytes[tail_start], b"LIMIT") => {
            match parse_int_ast_arg(env, arg_bytes[tail_start + 1]) {
                Ok(limit) if limit >= 0 => (atoms::sintercard(), keys, limit).encode(env),
                _ => (
                    atoms::sintercard(),
                    generic_ast_error(env, b"ERR value is not an integer or out of range"),
                )
                    .encode(env),
            }
        }
        _ => (
            atoms::sintercard(),
            generic_ast_error(env, b"ERR syntax error"),
        )
            .encode(env),
    }
}

fn make_zadd_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 3);

    let mut opts = Vec::with_capacity(4);
    let mut idx = 1;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;

    while idx < arg_bytes.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], b"NX") {
            nx = true;
            opts.push(atoms::nx().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"XX") {
            xx = true;
            opts.push(atoms::xx().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"GT") {
            gt = true;
            opts.push(atoms::gt().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"LT") {
            lt = true;
            opts.push(atoms::lt().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"CH") {
            opts.push(atoms::ch().encode(env));
            idx += 1;
        } else {
            break;
        }
    }

    let err = if nx && xx {
        Some(b"ERR XX and NX options at the same time are not compatible".as_slice())
    } else if (gt && (lt || nx)) || (lt && nx) {
        Some(b"ERR GT, LT, and NX options at the same time are not compatible".as_slice())
    } else if idx >= arg_bytes.len() || !(arg_bytes.len() - idx).is_multiple_of(2) {
        Some(b"ERR wrong number of arguments for 'zadd' command".as_slice())
    } else {
        None
    };

    if let Some(msg) = err {
        return (atoms::zadd(), args[0], generic_ast_error(env, msg)).encode(env);
    }

    let mut pairs = Vec::with_capacity((arg_bytes.len() - idx) / 2);
    while idx < arg_bytes.len() {
        let score = match parse_float_ast_arg(env, arg_bytes[idx]) {
            Ok(value) => value,
            Err(err) => return (atoms::zadd(), args[0], err).encode(env),
        };
        pairs.push((score, args[idx + 1]).encode(env));
        idx += 2;
    }

    (atoms::zadd(), args[0], opts, pairs).encode(env)
}

fn make_zincrby_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 3);

    match parse_float_ast_arg(env, arg_bytes[1]) {
        Ok(value) => (atoms::zincrby(), args[0], value, args[2]).encode(env),
        Err(err) => (atoms::zincrby(), args[0], err, args[2]).encode(env),
    }
}

fn make_zrange_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() == 3 || args.len() == 4);

    let start = parse_int_ast_arg(env, arg_bytes[1]);
    let stop = parse_int_ast_arg(env, arg_bytes[2]);
    let with_scores = args.len() == 4 && ascii_eq_ignore_case(arg_bytes[3], b"WITHSCORES");

    if args.len() == 4 && !with_scores {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR syntax error"),
            args[3],
        )
            .encode(env);
    }

    match (start, stop) {
        (Ok(start), Ok(stop)) => (tag, args[0], start, stop, with_scores).encode(env),
        (Err(err), _) => (tag, args[0], err, args[2]).encode(env),
        (_, Err(err)) => (tag, args[0], err, args[2]).encode(env),
    }
}

fn make_zcount_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 3);

    let min = parse_score_bound_ast(env, arg_bytes[1]);
    let max = parse_score_bound_ast(env, arg_bytes[2]);

    match (min, max) {
        (Ok(min), Ok(max)) => (atoms::zcount(), args[0], min, max).encode(env),
        (Err(err), _) | (_, Err(err)) => (atoms::zcount(), args[0], err).encode(env),
    }
}

fn make_zrandmember_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!((1..=3).contains(&args.len()));

    if args.len() == 1 {
        return (atoms::zrandmember(), args[0]).encode(env);
    }

    let count = match parse_int_ast_arg(env, arg_bytes[1]) {
        Ok(value) => value,
        Err(err) => return (atoms::zrandmember(), args[0], err).encode(env),
    };

    if args.len() == 2 {
        return (atoms::zrandmember(), args[0], count, false).encode(env);
    }

    if ascii_eq_ignore_case(arg_bytes[2], b"WITHSCORES") {
        (atoms::zrandmember(), args[0], count, true).encode(env)
    } else {
        (
            atoms::zrandmember(),
            args[0],
            count,
            generic_ast_error(env, b"ERR syntax error"),
        )
            .encode(env)
    }
}

fn make_zrangebyscore_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() >= 3);

    let first = parse_score_bound_ast(env, arg_bytes[1]);
    let second = parse_score_bound_ast(env, arg_bytes[2]);

    let (first, second) = match (first, second) {
        (Ok(first), Ok(second)) => (first, second),
        _ => {
            return (
                tag,
                args[0],
                generic_ast_error(env, b"ERR min or max is not a float"),
            )
                .encode(env)
        }
    };

    match parse_zrangebyscore_opts_ast(env, arg_bytes, 3) {
        Ok(opts) => (tag, args[0], first, second, opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn parse_score_bound_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Term<'a>, Term<'a>> {
    if ascii_eq_ignore_case(data, b"+INF") || ascii_eq_ignore_case(data, b"INF") {
        return Ok(atoms::inf().encode(env));
    }
    if ascii_eq_ignore_case(data, b"-INF") {
        return Ok(atoms::neg_inf().encode(env));
    }

    if let Some(rest) = data.strip_prefix(b"(") {
        return parse_float_ast_arg(env, rest)
            .map(|value| (atoms::exclusive(), value).encode(env))
            .map_err(|_| generic_ast_error(env, b"ERR min or max is not a float"));
    }

    parse_float_ast_arg(env, data)
        .map(|value| (atoms::inclusive(), value).encode(env))
        .map_err(|_| generic_ast_error(env, b"ERR min or max is not a float"))
}

fn parse_zrangebyscore_opts_ast<'a>(
    env: Env<'a>,
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::with_capacity(2);
    let mut idx = start;

    while idx < arg_bytes.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], b"WITHSCORES") {
            opts.push((atoms::withscores(), true).encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"LIMIT") {
            if idx + 2 >= arg_bytes.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }

            let offset = match parse_int_ast_arg(env, arg_bytes[idx + 1]) {
                Ok(value) if value >= 0 => value,
                Ok(_) => return Err(generic_ast_error(env, b"ERR syntax error")),
                Err(_) => {
                    return Err(generic_ast_error(
                        env,
                        b"ERR value is not an integer or out of range",
                    ))
                }
            };
            let count = match parse_int_ast_arg(env, arg_bytes[idx + 2]) {
                Ok(value) => value,
                Err(_) => {
                    return Err(generic_ast_error(
                        env,
                        b"ERR value is not an integer or out of range",
                    ))
                }
            };

            opts.push((atoms::limit(), (offset, count)).encode(env));
            idx += 3;
        } else {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }
    }

    Ok(opts)
}

fn make_getrange_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 3);

    let start = parse_int_ast_arg(env, arg_bytes[1]);
    let stop = parse_int_ast_arg(env, arg_bytes[2]);

    match (start, stop) {
        (Ok(start), Ok(stop)) => (atoms::getrange(), args[0], start, stop).encode(env),
        (Err(err), _) | (_, Err(err)) => (atoms::getrange(), args[0], err, args[2]).encode(env),
    }
}

fn make_expiry_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() == 2 || args.len() == 3);

    let ttl = match parse_int_ast_arg(env, arg_bytes[1]) {
        Ok(value) => value,
        Err(err) => return (tag, args[0], err).encode(env),
    };

    if args.len() == 2 {
        return (tag, args[0], ttl).encode(env);
    }

    match parse_expiry_flag_ast(env, arg_bytes[2]) {
        Ok(flag) => (tag, args[0], ttl, flag).encode(env),
        Err(err) => (tag, args[0], ttl, err).encode(env),
    }
}

fn make_setbit_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 3);

    let offset = parse_bitmap_offset_ast_arg(env, arg_bytes[1]);
    let bit = parse_bit_value_ast_arg(env, arg_bytes[2]);

    match (offset, bit) {
        (Ok(offset), Ok(bit)) => (atoms::setbit(), args[0], offset, bit).encode(env),
        (Err(err), _) => (atoms::setbit(), args[0], err, args[2]).encode(env),
        (Ok(offset), Err(err)) => (atoms::setbit(), args[0], offset, err).encode(env),
    }
}

fn make_getbit_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 2);

    match parse_bitmap_offset_ast_arg(env, arg_bytes[1]) {
        Ok(offset) => (atoms::getbit(), args[0], offset).encode(env),
        Err(err) => (atoms::getbit(), args[0], err).encode(env),
    }
}

fn make_bitcount_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() == 1 || args.len() == 3 || args.len() == 4);

    if args.len() == 1 {
        return (atoms::bitcount(), args[0]).encode(env);
    }

    let mode = if args.len() == 4 {
        parse_bitmap_mode_ast_arg(env, arg_bytes[3])
    } else {
        Ok(atoms::byte())
    };

    let start = parse_int_ast_arg(env, arg_bytes[1]);
    let end = parse_int_ast_arg(env, arg_bytes[2]);

    match (mode, start, end) {
        (Ok(mode), Ok(start), Ok(end)) => {
            (atoms::bitcount(), args[0], (start, end, mode)).encode(env)
        }
        (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
            (atoms::bitcount(), args[0], err).encode(env)
        }
    }
}

fn make_bitpos_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!((2..=5).contains(&args.len()));

    let bit = match parse_bit_value_ast_arg(env, arg_bytes[1]) {
        Ok(bit) => bit,
        Err(err) => return (atoms::bitpos(), args[0], err).encode(env),
    };

    if args.len() == 2 {
        return (atoms::bitpos(), args[0], bit, atoms::all()).encode(env);
    }

    let start = match parse_int_ast_arg(env, arg_bytes[2]) {
        Ok(start) => start,
        Err(err) => return (atoms::bitpos(), args[0], bit, err).encode(env),
    };

    if args.len() == 3 {
        return (atoms::bitpos(), args[0], bit, (atoms::start(), start)).encode(env);
    }

    let end = match parse_int_ast_arg(env, arg_bytes[3]) {
        Ok(end) => end,
        Err(err) => return (atoms::bitpos(), args[0], bit, err).encode(env),
    };

    let mode = if args.len() == 5 {
        match parse_bitmap_mode_ast_arg(env, arg_bytes[4]) {
            Ok(mode) => mode,
            Err(err) => return (atoms::bitpos(), args[0], bit, err).encode(env),
        }
    } else {
        atoms::byte()
    };

    (atoms::bitpos(), args[0], bit, (start, end, mode)).encode(env)
}

fn make_bitop_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 3);

    let source_keys = args[2..].to_vec();
    match parse_bitop_atom_ast_arg(env, arg_bytes[0], source_keys.len()) {
        Ok(op) => (atoms::bitop(), op, args[1], source_keys).encode(env),
        Err(err) => (atoms::bitop(), err).encode(env),
    }
}

fn make_copy_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 2);

    match parse_copy_replace_ast(env, arg_bytes) {
        Ok(replace) => (atom(env, "copy"), args[0], args[1], replace).encode(env),
        Err(err) => (atom(env, "copy"), args[0], args[1], err).encode(env),
    }
}

fn make_generic_scan_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(!args.is_empty());

    match parse_generic_scan_opts_ast(env, args, arg_bytes, 1) {
        Ok(opts) => (atom(env, "scan"), args[0], opts).encode(env),
        Err(err) => (atom(env, "scan"), args[0], err).encode(env),
    }
}

fn make_object_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(!args.is_empty());

    let object = atom(env, "object");
    let subcmd = arg_bytes[0];

    if ascii_eq_ignore_case(subcmd, b"HELP") && args.len() == 1 {
        return (object, atom(env, "help")).encode(env);
    }

    let Some(subcmd_atom) = object_subcommand_atom(env, subcmd) else {
        return (object, unknown_object_subcommand_error(env, subcmd)).encode(env);
    };

    if args.len() == 2 {
        (object, subcmd_atom, args[1]).encode(env)
    } else {
        (object, unknown_object_subcommand_error(env, subcmd)).encode(env)
    }
}

fn parse_copy_replace_ast<'a>(env: Env<'a>, arg_bytes: &[&[u8]]) -> Result<bool, Term<'a>> {
    match arg_bytes.len() {
        2 => Ok(false),
        3 if ascii_eq_ignore_case(arg_bytes[2], b"REPLACE") => Ok(true),
        _ => Err(generic_ast_error(env, b"ERR syntax error")),
    }
}

fn parse_generic_scan_opts_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::with_capacity(arg_bytes.len().saturating_sub(start) / 2);
    let mut idx = start;

    while idx < arg_bytes.len() {
        if idx + 1 >= arg_bytes.len() {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }

        if ascii_eq_ignore_case(arg_bytes[idx], b"MATCH") {
            opts.push((atom(env, "match"), args[idx + 1]).encode(env));
            idx += 2;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"COUNT") {
            match parse_int_ast_arg(env, arg_bytes[idx + 1]) {
                Ok(value) if value > 0 => opts.push((atoms::count(), value).encode(env)),
                _ => {
                    return Err(generic_ast_error(
                        env,
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
            idx += 2;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"TYPE") {
            opts.push(
                (
                    atom(env, "type"),
                    lower_ascii_binary_term(env, arg_bytes[idx + 1]),
                )
                    .encode(env),
            );
            idx += 2;
        } else {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }
    }

    Ok(opts)
}

fn object_subcommand_atom(env: Env<'_>, subcmd: &[u8]) -> Option<Atom> {
    if ascii_eq_ignore_case(subcmd, b"ENCODING") {
        Some(atom(env, "encoding"))
    } else if ascii_eq_ignore_case(subcmd, b"FREQ") {
        Some(atom(env, "freq"))
    } else if ascii_eq_ignore_case(subcmd, b"IDLETIME") {
        Some(atom(env, "idletime"))
    } else if ascii_eq_ignore_case(subcmd, b"REFCOUNT") {
        Some(atom(env, "refcount"))
    } else {
        None
    }
}

fn unknown_object_subcommand_error<'a>(env: Env<'a>, subcmd: &[u8]) -> Term<'a> {
    let mut msg = b"ERR unknown subcommand or wrong number of arguments for '".to_vec();
    msg.extend_from_slice(&lower_ascii_bytes(subcmd));
    msg.extend_from_slice(b"' command");
    generic_ast_error(env, &msg)
}

fn make_xadd_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 4);

    match parse_xadd_ast(env, args, arg_bytes) {
        Ok((key, id_spec, fields, trim_opts, nomkstream)) => (
            atom(env, "xadd"),
            key,
            (id_spec, fields, trim_opts, nomkstream),
        )
            .encode(env),
        Err(err) => (atom(env, "xadd"), err).encode(env),
    }
}

fn make_xrange_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() >= 3);

    let start = parse_stream_range_id_ast(env, arg_bytes[1], true);
    let end = parse_stream_range_id_ast(env, arg_bytes[2], false);
    let count = parse_stream_count_opt_ast(env, arg_bytes, 3);

    match (start, end, count) {
        (Ok(start), Ok(end), Ok(count)) => (tag, args[0], start, end, count).encode(env),
        (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => (tag, args[0], err).encode(env),
    }
}

fn make_xrevrange_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    debug_assert!(args.len() >= 3);

    let end = parse_stream_range_id_ast(env, arg_bytes[1], false);
    let start = parse_stream_range_id_ast(env, arg_bytes[2], true);
    let count = parse_stream_count_opt_ast(env, arg_bytes, 3);

    match (start, end, count) {
        (Ok(start), Ok(end), Ok(count)) => {
            (atom(env, "xrevrange"), args[0], start, end, count).encode(env)
        }
        (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
            (atom(env, "xrevrange"), args[0], err).encode(env)
        }
    }
}

fn make_xtrim_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 2);

    match parse_stream_trim_ast(env, args, arg_bytes, 1) {
        Ok((trim, _next)) => (atom(env, "xtrim"), args[0], trim).encode(env),
        Err(err) => (atom(env, "xtrim"), args[0], err).encode(env),
    }
}

fn make_xread_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    match parse_stream_read_ast(env, args, arg_bytes, 0) {
        Ok((count, block, stream_ids)) => {
            (atom(env, "xread"), count, block, stream_ids).encode(env)
        }
        Err(err) => (atom(env, "xread"), err).encode(env),
    }
}

fn make_xinfo_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    if ascii_eq_ignore_case(arg_bytes[0], b"STREAM") && args.len() >= 2 {
        (atom(env, "xinfo_stream"), args[1]).encode(env)
    } else {
        (
            atom(env, "xinfo"),
            generic_ast_error(env, b"ERR wrong number of arguments for 'xinfo' command"),
        )
            .encode(env)
    }
}

fn make_xgroup_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    if !ascii_eq_ignore_case(arg_bytes[0], b"CREATE") || args.len() < 4 {
        return (
            atom(env, "xgroup"),
            generic_ast_error(env, b"ERR syntax error"),
        )
            .encode(env);
    }

    let mkstream = arg_bytes[4..]
        .iter()
        .any(|arg| ascii_eq_ignore_case(arg, b"MKSTREAM"));

    (
        atom(env, "xgroup_create"),
        args[1],
        args[2],
        args[3],
        mkstream,
    )
        .encode(env)
}

fn make_xreadgroup_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    if !ascii_eq_ignore_case(arg_bytes[0], b"GROUP") || args.len() < 6 {
        return (
            atom(env, "xreadgroup"),
            generic_ast_error(env, b"ERR syntax error"),
        )
            .encode(env);
    }

    match parse_stream_read_ast(env, args, arg_bytes, 3) {
        Ok((count, block, stream_ids)) => (
            atom(env, "xreadgroup"),
            args[1],
            args[2],
            (count, block, stream_ids),
        )
            .encode(env),
        Err(err) => (atom(env, "xreadgroup"), err).encode(env),
    }
}

fn parse_xadd_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Result<(Term<'a>, Term<'a>, Vec<Term<'a>>, Term<'a>, bool), Term<'a>> {
    let key = args[0];
    let mut idx = 1;
    let mut nomkstream = false;

    if idx < arg_bytes.len() && ascii_eq_ignore_case(arg_bytes[idx], b"NOMKSTREAM") {
        nomkstream = true;
        idx += 1;
    }

    let trim_opts = if idx < arg_bytes.len()
        && (ascii_eq_ignore_case(arg_bytes[idx], b"MAXLEN")
            || ascii_eq_ignore_case(arg_bytes[idx], b"MINID"))
    {
        let (trim, next_idx) = parse_stream_trim_ast(env, args, arg_bytes, idx)?;
        idx = next_idx;
        trim
    } else {
        atoms::nil().encode(env)
    };

    if idx >= arg_bytes.len() {
        return Err(generic_ast_error(
            env,
            b"ERR wrong number of arguments for 'xadd' command",
        ));
    }

    let id_spec = parse_stream_id_spec_ast(env, arg_bytes[idx])?;
    idx += 1;

    if idx >= args.len() || !(args.len() - idx).is_multiple_of(2) {
        return Err(generic_ast_error(
            env,
            b"ERR wrong number of arguments for 'xadd' command",
        ));
    }

    Ok((key, id_spec, args[idx..].to_vec(), trim_opts, nomkstream))
}

fn parse_stream_trim_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<(Term<'a>, usize), Term<'a>> {
    let tag = arg_bytes[start];
    let mut idx = start + 1;
    let mut approx = false;

    if idx < arg_bytes.len() && ascii_eq_ignore_case(arg_bytes[idx], b"~") {
        approx = true;
        idx += 1;
    } else if idx < arg_bytes.len() && ascii_eq_ignore_case(arg_bytes[idx], b"=") {
        idx += 1;
    }

    if idx >= arg_bytes.len() {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    if ascii_eq_ignore_case(tag, b"MAXLEN") {
        match parse_int_ast_arg(env, arg_bytes[idx]) {
            Ok(n) if n >= 0 => Ok(((atom(env, "maxlen"), approx, n).encode(env), idx + 1)),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        }
    } else if ascii_eq_ignore_case(tag, b"MINID") {
        Ok(((atom(env, "minid"), approx, args[idx]).encode(env), idx + 1))
    } else {
        Err(generic_ast_error(env, b"ERR syntax error"))
    }
}

fn parse_stream_id_spec_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Term<'a>, Term<'a>> {
    if data == b"*" {
        return Ok(atom(env, "auto").encode(env));
    }

    match split_once_byte(data, b'-') {
        Some((ms, seq)) => {
            let ms = parse_stream_id_component(env, ms)?;
            let seq = parse_stream_id_component(env, seq)?;
            Ok((atom(env, "explicit"), ms, seq).encode(env))
        }
        None => {
            let ms = parse_stream_id_component(env, data)?;
            Ok((atom(env, "partial"), ms).encode(env))
        }
    }
}

fn parse_stream_range_id_ast<'a>(
    env: Env<'a>,
    data: &[u8],
    is_start: bool,
) -> Result<Term<'a>, Term<'a>> {
    if is_start && data == b"-" {
        Ok(atom(env, "min").encode(env))
    } else if !is_start && data == b"+" {
        Ok(atom(env, "max").encode(env))
    } else {
        parse_stream_full_id_ast(env, data)
    }
}

fn parse_stream_full_id_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Term<'a>, Term<'a>> {
    match split_once_byte(data, b'-') {
        Some((ms, seq)) => {
            let ms = parse_stream_id_component(env, ms)?;
            let seq = parse_stream_id_component(env, seq)?;
            Ok((ms, seq).encode(env))
        }
        None => {
            let ms = parse_stream_id_component(env, data)?;
            Ok((ms, 0_i64).encode(env))
        }
    }
}

fn parse_stream_id_component<'a>(env: Env<'a>, data: &[u8]) -> Result<i64, Term<'a>> {
    match parse_int_bytes(data) {
        Some(value) if value >= 0 => Ok(value),
        _ => Err(generic_ast_error(
            env,
            b"ERR Invalid stream ID specified as stream command argument",
        )),
    }
}

fn parse_stream_count_opt_ast<'a>(
    env: Env<'a>,
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<Term<'a>, Term<'a>> {
    if start >= arg_bytes.len() {
        return Ok(atom(env, "infinity").encode(env));
    }

    if start + 1 < arg_bytes.len() && ascii_eq_ignore_case(arg_bytes[start], b"COUNT") {
        match parse_int_ast_arg(env, arg_bytes[start + 1]) {
            Ok(n) if n >= 0 => Ok(n.encode(env)),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        }
    } else {
        Err(generic_ast_error(env, b"ERR syntax error"))
    }
}

fn parse_stream_read_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<(Term<'a>, Term<'a>, Vec<Term<'a>>), Term<'a>> {
    let mut idx = start;
    let mut count = atom(env, "infinity").encode(env);
    let mut block = atom(env, "no_block").encode(env);

    for _ in 0..2 {
        if idx + 1 < arg_bytes.len() && ascii_eq_ignore_case(arg_bytes[idx], b"COUNT") {
            match parse_int_ast_arg(env, arg_bytes[idx + 1]) {
                Ok(n) if n >= 0 => count = n.encode(env),
                _ => {
                    return Err(generic_ast_error(
                        env,
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
            idx += 2;
        } else if idx + 1 < arg_bytes.len() && ascii_eq_ignore_case(arg_bytes[idx], b"BLOCK") {
            match parse_int_ast_arg(env, arg_bytes[idx + 1]) {
                Ok(n) if n >= 0 => block = (atom(env, "block"), n).encode(env),
                _ => {
                    return Err(generic_ast_error(
                        env,
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
            idx += 2;
        }
    }

    if idx >= arg_bytes.len() || !ascii_eq_ignore_case(arg_bytes[idx], b"STREAMS") {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    let after = args.len().saturating_sub(idx + 1);
    if after == 0 || !after.is_multiple_of(2) {
        return Err(generic_ast_error(
            env,
            b"ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified",
        ));
    }

    let keys_start = idx + 1;
    let half = after / 2;
    let ids_start = keys_start + half;
    let mut stream_ids = Vec::with_capacity(half);

    for offset in 0..half {
        stream_ids.push((args[keys_start + offset], args[ids_start + offset]).encode(env));
    }

    Ok((count, block, stream_ids))
}

fn make_json_set_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    let tag = atom(env, "json_set");
    if args.len() != 3 && args.len() != 4 {
        return (tag, wrong_number_error(env, b"json.set")).encode(env);
    }

    let path = parse_json_path_ast(env, arg_bytes[1]);
    let flags = parse_json_set_flags_ast(env, &arg_bytes[3..]);

    match (path, flags) {
        (Ok(path), Ok(flags)) => (tag, args[0], path, args[2], flags).encode(env),
        (Err(err), _) | (_, Err(err)) => (tag, err).encode(env),
    }
}

fn make_json_get_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    let tag = atom(env, "json_get");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"json.get")).encode(env);
    }

    let mut paths = Vec::with_capacity(args.len().saturating_sub(1));
    for idx in 1..args.len() {
        match parse_json_path_ast(env, arg_bytes[idx]) {
            Ok(path) => paths.push((args[idx], path).encode(env)),
            Err(err) => return (tag, args[0], err).encode(env),
        }
    }

    (tag, args[0], paths).encode(env)
}

fn make_json_path_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    command_name: &[u8],
    path_optional: bool,
) -> Term<'a> {
    let valid_arity = if path_optional {
        args.len() == 1 || args.len() == 2
    } else {
        args.len() == 2
    };

    if !valid_arity {
        return (tag, wrong_number_error(env, command_name)).encode(env);
    }

    let path = if args.len() == 1 {
        Ok(Vec::new())
    } else {
        parse_json_path_ast(env, arg_bytes[1])
    };

    match path {
        Ok(path) => (tag, args[0], path).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_json_numincrby_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "json_numincrby");
    if args.len() != 3 {
        return (tag, wrong_number_error(env, b"json.numincrby")).encode(env);
    }

    let path = parse_json_path_ast(env, arg_bytes[1]);
    let number = parse_json_number_ast(env, arg_bytes[2]);

    match (path, number) {
        (Ok(path), Ok(number)) => (tag, args[0], path, number).encode(env),
        (Err(err), _) | (_, Err(err)) => (tag, args[0], err).encode(env),
    }
}

fn make_json_arrappend_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "json_arrappend");
    if args.len() < 3 {
        return (tag, wrong_number_error(env, b"json.arrappend")).encode(env);
    }

    match parse_json_path_ast(env, arg_bytes[1]) {
        Ok(path) => (tag, args[0], path, args[2..].to_vec()).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_json_mget_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "json_mget");
    if args.len() < 2 {
        return (tag, wrong_number_error(env, b"json.mget")).encode(env);
    }

    match parse_json_path_ast(env, arg_bytes[arg_bytes.len() - 1]) {
        Ok(path) => (tag, args[..args.len() - 1].to_vec(), path).encode(env),
        Err(err) => (tag, err).encode(env),
    }
}

fn parse_json_set_flags_ast<'a>(
    env: Env<'a>,
    opt_bytes: &[&[u8]],
) -> Result<Vec<Term<'a>>, Term<'a>> {
    match opt_bytes {
        [] => Ok(Vec::new()),
        [flag] if ascii_eq_ignore_case(flag, b"NX") => Ok(vec![atoms::nx().encode(env)]),
        [flag] if ascii_eq_ignore_case(flag, b"XX") => Ok(vec![atoms::xx().encode(env)]),
        [other] => {
            let mut msg = b"ERR syntax error, option '".to_vec();
            msg.extend_from_slice(other);
            msg.extend_from_slice(b"' not recognized");
            Err(generic_ast_error(env, &msg))
        }
        _ => Err(generic_ast_error(env, b"ERR syntax error")),
    }
}

fn parse_json_number_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Term<'a>, Term<'a>> {
    if data.contains(&b'.') {
        std::str::from_utf8(data)
            .ok()
            .and_then(|value| f64::from_str(value).ok())
            .filter(|value| value.is_finite())
            .map(|value| value.encode(env))
            .ok_or_else(|| generic_ast_error(env, b"ERR value is not a number"))
    } else {
        parse_int_bytes(data)
            .map(|value| value.encode(env))
            .ok_or_else(|| generic_ast_error(env, b"ERR value is not a number"))
    }
}

fn parse_json_path_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Vec<Term<'a>>, Term<'a>> {
    if data == b"$" {
        return Ok(Vec::new());
    }

    if !data.starts_with(b"$") {
        return Err(generic_ast_error(env, b"ERR invalid JSONPath syntax"));
    }

    let mut idx = 1;
    let mut segments = Vec::new();

    while idx < data.len() {
        match data[idx] {
            b'.' => {
                idx += 1;
                let start = idx;
                while idx < data.len() && data[idx] != b'.' && data[idx] != b'[' {
                    idx += 1;
                }
                if idx == start {
                    return Err(generic_ast_error(env, b"ERR invalid JSONPath syntax"));
                }
                segments.push(make_binary_term(env, &data[start..idx]));
            }
            b'[' => {
                idx += 1;
                let start = idx;
                while idx < data.len() && data[idx] != b']' {
                    idx += 1;
                }
                if idx >= data.len() {
                    return Err(generic_ast_error(env, b"ERR invalid JSONPath syntax"));
                }

                let inner = &data[start..idx];
                idx += 1;
                segments.push(parse_json_bracket_segment_ast(env, inner)?);
            }
            _ => return Err(generic_ast_error(env, b"ERR invalid JSONPath syntax")),
        }
    }

    Ok(segments)
}

fn parse_json_bracket_segment_ast<'a>(env: Env<'a>, inner: &[u8]) -> Result<Term<'a>, Term<'a>> {
    if inner.len() >= 2
        && ((inner[0] == b'"' && inner[inner.len() - 1] == b'"')
            || (inner[0] == b'\'' && inner[inner.len() - 1] == b'\''))
    {
        return Ok(make_binary_term(env, &inner[1..inner.len() - 1]));
    }

    parse_int_bytes(inner)
        .map(|value| value.encode(env))
        .ok_or_else(|| generic_ast_error(env, b"ERR invalid JSONPath syntax"))
}

fn wrong_number_error<'a>(env: Env<'a>, command_name: &[u8]) -> Term<'a> {
    let mut msg = b"ERR wrong number of arguments for '".to_vec();
    msg.extend_from_slice(command_name);
    msg.extend_from_slice(b"' command");
    generic_ast_error(env, &msg)
}

fn make_geoadd_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    let tag = atom(env, "geoadd");
    if args.len() < 4 {
        return (tag, wrong_number_error(env, b"geoadd")).encode(env);
    }

    match parse_geoadd_ast(env, args, arg_bytes) {
        Ok((flags, pairs)) => (tag, args[0], flags, pairs).encode(env),
        Err(err) => (tag, err).encode(env),
    }
}

fn make_geo_members_command_ast<'a>(env: Env<'a>, tag: Atom, args: &[Term<'a>]) -> Term<'a> {
    if args.len() < 2 {
        let command_name = if tag == atom(env, "geopos") {
            b"geopos".as_slice()
        } else {
            b"geohash".as_slice()
        };
        return (tag, wrong_number_error(env, command_name)).encode(env);
    }

    (tag, args).encode(env)
}

fn make_geodist_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    let tag = atom(env, "geodist");
    if args.len() != 3 && args.len() != 4 {
        return (tag, wrong_number_error(env, b"geodist")).encode(env);
    }

    let unit = if args.len() == 4 {
        parse_geo_unit_ast(env, arg_bytes[3])
    } else {
        Ok(make_binary_term(env, b"M"))
    };

    match unit {
        Ok(unit) => (tag, args[0], args[1], args[2], unit).encode(env),
        Err(err) => (tag, args[0], args[1], args[2], err).encode(env),
    }
}

fn make_geosearch_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "geosearch");
    if args.len() < 2 {
        return (tag, wrong_number_error(env, b"geosearch")).encode(env);
    }

    match parse_geosearch_opts_ast(env, args, arg_bytes, 1) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_geosearchstore_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "geosearchstore");
    if args.len() < 3 {
        return (tag, wrong_number_error(env, b"geosearchstore")).encode(env);
    }

    match parse_geosearch_opts_ast(env, args, arg_bytes, 2) {
        Ok(opts) => (tag, args[0], args[1], opts).encode(env),
        Err(err) => (tag, args[0], args[1], err).encode(env),
    }
}

fn parse_geoadd_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Result<(Vec<Term<'a>>, Vec<Term<'a>>), Term<'a>> {
    let mut idx = 1;
    let mut flags = Vec::new();
    let mut has_nx = false;
    let mut has_xx = false;

    while idx < arg_bytes.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], b"NX") {
            has_nx = true;
            flags.push(atoms::nx().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"XX") {
            has_xx = true;
            flags.push(atoms::xx().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"CH") {
            flags.push(atom(env, "ch").encode(env));
            idx += 1;
        } else {
            break;
        }
    }

    if has_nx && has_xx {
        return Err(generic_ast_error(
            env,
            b"ERR XX and NX options at the same time are not compatible",
        ));
    }

    let remaining = arg_bytes.len().saturating_sub(idx);
    if remaining == 0 || !remaining.is_multiple_of(3) {
        return Err(wrong_number_error(env, b"geoadd"));
    }

    let mut pairs = Vec::with_capacity(remaining / 3);
    while idx < arg_bytes.len() {
        let lng = parse_geo_float_ast(env, arg_bytes[idx])?;
        let lat = parse_geo_float_ast(env, arg_bytes[idx + 1])?;
        if !valid_geo_coordinates(lng, lat) {
            let mut msg = b"ERR invalid longitude,latitude pair ".to_vec();
            msg.extend_from_slice(arg_bytes[idx]);
            msg.extend_from_slice(b",");
            msg.extend_from_slice(arg_bytes[idx + 1]);
            return Err(generic_ast_error(env, &msg));
        }
        pairs.push((lng, lat, args[idx + 2]).encode(env));
        idx += 3;
    }

    Ok((flags, pairs))
}

fn parse_geosearch_opts_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut idx = start;
    let mut opts = Vec::new();
    let mut has_center = false;
    let mut has_shape = false;

    while idx < arg_bytes.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], b"FROMLONLAT") {
            if has_center || idx + 2 >= arg_bytes.len() {
                return Err(generic_ast_error(
                    env,
                    b"ERR exactly one of FROMMEMBER or FROMLONLAT must be provided",
                ));
            }
            let lng = parse_geo_float_ast(env, arg_bytes[idx + 1])?;
            let lat = parse_geo_float_ast(env, arg_bytes[idx + 2])?;
            opts.push((atom(env, "center"), (atom(env, "lonlat"), lng, lat)).encode(env));
            has_center = true;
            idx += 3;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"FROMMEMBER") {
            if has_center || idx + 1 >= arg_bytes.len() {
                return Err(generic_ast_error(
                    env,
                    b"ERR exactly one of FROMMEMBER or FROMLONLAT must be provided",
                ));
            }
            opts.push((atom(env, "center"), (atom(env, "member"), args[idx + 1])).encode(env));
            has_center = true;
            idx += 2;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"BYRADIUS") {
            if has_shape || idx + 2 >= arg_bytes.len() {
                return Err(generic_ast_error(
                    env,
                    b"ERR exactly one of BYRADIUS or BYBOX must be provided",
                ));
            }
            let radius = parse_geo_float_ast(env, arg_bytes[idx + 1])?;
            let (unit, factor) = parse_geo_unit_factor_ast(env, arg_bytes[idx + 2])?;
            opts.push((atom(env, "shape"), (atom(env, "radius"), radius * factor)).encode(env));
            opts.push((atom(env, "unit"), unit).encode(env));
            opts.push((atom(env, "raw_radius"), radius).encode(env));
            has_shape = true;
            idx += 3;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"BYBOX") {
            if has_shape || idx + 3 >= arg_bytes.len() {
                return Err(generic_ast_error(
                    env,
                    b"ERR exactly one of BYRADIUS or BYBOX must be provided",
                ));
            }
            let width = parse_geo_float_ast(env, arg_bytes[idx + 1])?;
            let height = parse_geo_float_ast(env, arg_bytes[idx + 2])?;
            let (unit, factor) = parse_geo_unit_factor_ast(env, arg_bytes[idx + 3])?;
            opts.push(
                (
                    atom(env, "shape"),
                    (atom(env, "box"), width * factor, height * factor),
                )
                    .encode(env),
            );
            opts.push((atom(env, "unit"), unit).encode(env));
            has_shape = true;
            idx += 4;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"ASC") {
            opts.push((atom(env, "sort"), atom(env, "asc")).encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"DESC") {
            opts.push((atom(env, "sort"), atom(env, "desc")).encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"COUNT") {
            if idx + 1 >= arg_bytes.len() {
                return Err(generic_ast_error(
                    env,
                    b"ERR value is not an integer or out of range",
                ));
            }
            match parse_int_ast_arg(env, arg_bytes[idx + 1]) {
                Ok(count) if count > 0 => opts.push((atoms::count(), count).encode(env)),
                _ => {
                    return Err(generic_ast_error(
                        env,
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
            idx += 2;
            if idx < arg_bytes.len() && ascii_eq_ignore_case(arg_bytes[idx], b"ANY") {
                opts.push((atom(env, "any"), true).encode(env));
                idx += 1;
            } else {
                opts.push((atom(env, "any"), false).encode(env));
            }
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"WITHCOORD") {
            opts.push((atom(env, "withcoord"), true).encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"WITHDIST") {
            opts.push((atom(env, "withdist"), true).encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"WITHHASH") {
            opts.push((atom(env, "withhash"), true).encode(env));
            idx += 1;
        } else {
            let mut msg = b"ERR syntax error, unexpected '".to_vec();
            msg.extend_from_slice(arg_bytes[idx]);
            msg.extend_from_slice(b"'");
            return Err(generic_ast_error(env, &msg));
        }
    }

    if !has_center {
        return Err(generic_ast_error(
            env,
            b"ERR exactly one of FROMMEMBER or FROMLONLAT must be provided",
        ));
    }

    if !has_shape {
        return Err(generic_ast_error(
            env,
            b"ERR exactly one of BYRADIUS or BYBOX must be provided",
        ));
    }

    Ok(opts)
}

fn parse_geo_unit_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Term<'a>, Term<'a>> {
    parse_geo_unit_factor_ast(env, data).map(|(unit, _factor)| unit)
}

fn parse_geo_unit_factor_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<(Term<'a>, f64), Term<'a>> {
    if ascii_eq_ignore_case(data, b"M") {
        Ok((make_binary_term(env, b"M"), 1.0))
    } else if ascii_eq_ignore_case(data, b"KM") {
        Ok((make_binary_term(env, b"KM"), 1000.0))
    } else if ascii_eq_ignore_case(data, b"FT") {
        Ok((make_binary_term(env, b"FT"), 0.3048))
    } else if ascii_eq_ignore_case(data, b"MI") {
        Ok((make_binary_term(env, b"MI"), 1609.344))
    } else {
        Err(generic_ast_error(
            env,
            b"ERR unsupported unit provided. please use M, KM, FT, MI",
        ))
    }
}

fn parse_geo_float_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<f64, Term<'a>> {
    std::str::from_utf8(data)
        .ok()
        .and_then(|value| f64::from_str(value).ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| generic_ast_error(env, b"ERR value is not a valid float"))
}

fn valid_geo_coordinates(lng: f64, lat: f64) -> bool {
    (-180.0..=180.0).contains(&lng) && (-85.05112878..=85.05112878).contains(&lat)
}

fn make_hll_command_ast<'a>(
    env: Env<'a>,
    tag: Atom,
    args: &[Term<'a>],
    command_name: &[u8],
    min_arity: usize,
) -> Term<'a> {
    if args.len() < min_arity {
        (tag, wrong_number_error(env, command_name)).encode(env)
    } else {
        (tag, args).encode(env)
    }
}

fn make_min_arity_list_ast<'a>(
    env: Env<'a>,
    tag_name: &str,
    args: &[Term<'a>],
    command_name: &[u8],
    min_arity: usize,
) -> Term<'a> {
    let tag = atom(env, tag_name);
    if args.len() < min_arity {
        (tag, wrong_number_error(env, command_name)).encode(env)
    } else {
        (tag, args).encode(env)
    }
}

fn make_exact_arity_list_ast<'a>(
    env: Env<'a>,
    tag_name: &str,
    args: &[Term<'a>],
    command_name: &[u8],
    arity: usize,
) -> Term<'a> {
    let tag = atom(env, tag_name);
    if args.len() != arity {
        (tag, wrong_number_error(env, command_name)).encode(env)
    } else {
        (tag, args).encode(env)
    }
}

fn make_bf_reserve_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "bf_reserve");
    if args.len() != 3 {
        return (tag, wrong_number_error(env, b"bf.reserve")).encode(env);
    }

    match (
        parse_named_float_ast(env, arg_bytes[1], b"error_rate", false),
        parse_named_pos_int_ast(env, arg_bytes[2], b"capacity"),
    ) {
        (Ok(error_rate), Ok(capacity)) => (tag, args[0], error_rate, capacity).encode(env),
        (Err(err), _) | (_, Err(err)) => (tag, args[0], err).encode(env),
    }
}

fn make_cf_reserve_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "cf_reserve");
    if args.len() != 2 {
        return (tag, wrong_number_error(env, b"cf.reserve")).encode(env);
    }

    match parse_named_pos_int_ast(env, arg_bytes[1], b"capacity") {
        Ok(capacity) => (tag, args[0], capacity).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_cms_initbydim_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "cms_initbydim");
    if args.len() != 3 {
        return (tag, wrong_number_error(env, b"cms.initbydim")).encode(env);
    }

    match (
        parse_named_pos_int_ast(env, arg_bytes[1], b"width"),
        parse_named_pos_int_ast(env, arg_bytes[2], b"depth"),
    ) {
        (Ok(width), Ok(depth)) => (tag, args[0], width, depth).encode(env),
        (Err(err), _) | (_, Err(err)) => (tag, args[0], err).encode(env),
    }
}

fn make_cms_initbyprob_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "cms_initbyprob");
    if args.len() != 3 {
        return (tag, wrong_number_error(env, b"cms.initbyprob")).encode(env);
    }

    match (
        parse_named_prob_float_ast(env, arg_bytes[1], b"error"),
        parse_named_prob_float_ast(env, arg_bytes[2], b"probability"),
    ) {
        (Ok(error), Ok(probability)) => (tag, args[0], error, probability).encode(env),
        (Err(err), _) | (_, Err(err)) => (tag, args[0], err).encode(env),
    }
}

fn make_count_pairs_command_ast<'a>(
    env: Env<'a>,
    tag_name: &str,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    command_name: &[u8],
    count_error: &[u8],
) -> Term<'a> {
    let tag = atom(env, tag_name);
    if args.len() < 3 || !arg_bytes.len().saturating_sub(1).is_multiple_of(2) {
        return (tag, args[0], wrong_number_error(env, command_name)).encode(env);
    }

    match parse_element_count_pairs_ast(env, args, arg_bytes, count_error) {
        Ok(pairs) => (tag, args[0], pairs).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_cms_merge_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "cms_merge");
    if args.len() < 3 {
        return (tag, wrong_number_error(env, b"cms.merge")).encode(env);
    }

    let numkeys = match parse_named_pos_int_ast(env, arg_bytes[1], b"numkeys") {
        Ok(numkeys) => numkeys as usize,
        Err(err) => return (tag, args[0], err).encode(env),
    };

    if args.len() < 2 + numkeys {
        return (tag, args[0], wrong_number_error(env, b"cms.merge")).encode(env);
    }

    let src_keys = args[2..2 + numkeys].to_vec();
    let rest_start = 2 + numkeys;
    if rest_start == args.len() {
        return (tag, args[0], src_keys, vec![1_i64; numkeys]).encode(env);
    }

    if !ascii_eq_ignore_case(arg_bytes[rest_start], b"WEIGHTS")
        || args.len() != rest_start + 1 + numkeys
    {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR syntax error in 'cms.merge' command"),
        )
            .encode(env);
    }

    let mut weights = Vec::with_capacity(numkeys);
    for data in arg_bytes.iter().skip(rest_start + 1) {
        match parse_int_bytes(data) {
            Some(weight) => weights.push(weight),
            None => {
                return (
                    tag,
                    args[0],
                    generic_ast_error(env, b"ERR CMS: invalid weight value"),
                )
                    .encode(env)
            }
        }
    }

    (tag, args[0], src_keys, weights).encode(env)
}

fn make_topk_reserve_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "topk_reserve");
    if args.len() != 2 && args.len() != 5 {
        return (tag, wrong_number_error(env, b"topk.reserve")).encode(env);
    }

    let k = match parse_named_pos_int_ast(env, arg_bytes[1], b"k") {
        Ok(k) => k,
        Err(err) => return (tag, args[0], err).encode(env),
    };

    if args.len() == 2 {
        return (tag, args[0], k, 8_i64, 7_i64, 0.9_f64).encode(env);
    }

    match (
        parse_named_pos_int_ast(env, arg_bytes[2], b"width"),
        parse_named_pos_int_ast(env, arg_bytes[3], b"depth"),
        parse_decay_ast(env, arg_bytes[4]),
    ) {
        (Ok(width), Ok(depth), Ok(decay)) => (tag, args[0], k, width, depth, decay).encode(env),
        (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => (tag, args[0], err).encode(env),
    }
}

fn make_topk_list_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "topk_list");
    match args.len() {
        1 => (tag, args[0], false).encode(env),
        2 if ascii_eq_ignore_case(arg_bytes[1], b"WITHCOUNT") => (tag, args[0], true).encode(env),
        2 => (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env),
        _ => (tag, wrong_number_error(env, b"topk.list")).encode(env),
    }
}

fn make_tdigest_create_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "tdigest_create");
    match args.len() {
        1 => (tag, args[0], 100_i64).encode(env),
        3 if ascii_eq_ignore_case(arg_bytes[1], b"COMPRESSION") => {
            match parse_named_pos_int_ast(env, arg_bytes[2], b"compression") {
                Ok(compression) => (tag, args[0], compression).encode(env),
                Err(err) => (tag, args[0], err).encode(env),
            }
        }
        _ => (tag, wrong_number_error(env, b"tdigest.create")).encode(env),
    }
}

fn make_float_list_command_ast<'a>(
    env: Env<'a>,
    tag_name: &str,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    command_name: &[u8],
    min_arity: usize,
    quantile: bool,
) -> Term<'a> {
    let tag = atom(env, tag_name);
    if args.len() < min_arity {
        return (tag, wrong_number_error(env, command_name)).encode(env);
    }

    let mut values = Vec::with_capacity(args.len() - 1);
    for data in &arg_bytes[1..] {
        let parsed = if quantile {
            parse_quantile_float_ast(env, data)
        } else {
            parse_float_value_ast(env, data)
        };

        match parsed {
            Ok(value) => values.push(value),
            Err(err) => return (tag, args[0], err).encode(env),
        }
    }

    (tag, args[0], values).encode(env)
}

fn make_int_list_command_ast<'a>(
    env: Env<'a>,
    tag_name: &str,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    command_name: &[u8],
    min_arity: usize,
) -> Term<'a> {
    let tag = atom(env, tag_name);
    if args.len() < min_arity {
        return (tag, wrong_number_error(env, command_name)).encode(env);
    }

    let mut values = Vec::with_capacity(args.len() - 1);
    for data in &arg_bytes[1..] {
        match parse_int_bytes(data) {
            Some(value) => values.push(value),
            None => {
                return (
                    tag,
                    args[0],
                    generic_ast_error(env, b"ERR value is not an integer or out of range"),
                )
                    .encode(env)
            }
        }
    }

    (tag, args[0], values).encode(env)
}

fn make_tdigest_trimmed_mean_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "tdigest_trimmed_mean");
    if args.len() != 3 {
        return (tag, wrong_number_error(env, b"tdigest.trimmed_mean")).encode(env);
    }

    match (
        parse_quantile_float_ast(env, arg_bytes[1]),
        parse_quantile_float_ast(env, arg_bytes[2]),
    ) {
        (Ok(lo), Ok(hi)) if lo < hi => (tag, args[0], lo, hi).encode(env),
        (Ok(_), Ok(_)) => (
            tag,
            args[0],
            generic_ast_error(
                env,
                b"ERR TDIGEST: low_quantile must be less than high_quantile",
            ),
        )
            .encode(env),
        (Err(err), _) | (_, Err(err)) => (tag, args[0], err).encode(env),
    }
}

fn make_tdigest_merge_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "tdigest_merge");
    if args.len() < 3 {
        return (tag, wrong_number_error(env, b"tdigest.merge")).encode(env);
    }

    let numkeys = match parse_named_pos_int_ast(env, arg_bytes[1], b"numkeys") {
        Ok(numkeys) => numkeys as usize,
        Err(err) => return (tag, args[0], err).encode(env),
    };

    if args.len() < 2 + numkeys {
        return (tag, args[0], wrong_number_error(env, b"tdigest.merge")).encode(env);
    }

    let src_keys = args[2..2 + numkeys].to_vec();
    let mut opts = Vec::new();
    let mut idx = 2 + numkeys;

    while idx < args.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], b"COMPRESSION") {
            if idx + 1 >= args.len() {
                return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
            }
            match parse_named_pos_int_ast(env, arg_bytes[idx + 1], b"compression") {
                Ok(compression) => opts.push((atom(env, "compression"), compression).encode(env)),
                Err(err) => return (tag, args[0], err).encode(env),
            }
            idx += 2;
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"OVERRIDE") {
            opts.push((atom(env, "override"), true).encode(env));
            idx += 1;
        } else {
            return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
        }
    }

    (tag, args[0], src_keys, opts).encode(env)
}

fn make_flow_create_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_create");
    if args.len() < 3 {
        return (tag, wrong_number_error(env, b"flow.create")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_create_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_create_many_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_create_many");
    if args.len() < 6 {
        return (tag, wrong_number_error(env, b"flow.create_many")).encode(env);
    }

    let mixed = ascii_eq_ignore_case(arg_bytes[0], b"MIXED");

    let Some(items_idx) = flow_find_option(arg_bytes, 1, b"ITEMS") else {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR flow items are required"),
        )
            .encode(env);
    };

    let item_width = if mixed { 3 } else { 2 };
    if items_idx == args.len() - 1 || (args.len() - items_idx - 1) % item_width != 0 {
        return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
    }

    let opts =
        match parse_flow_options_until(env, args, arg_bytes, 1, items_idx, flow_create_option) {
            Ok(opts) => opts,
            Err(err) => return (tag, args[0], err).encode(env),
        };

    let mut items = Vec::with_capacity((args.len() - items_idx - 1) / item_width);
    let mut idx = items_idx + 1;
    while idx < args.len() {
        if mixed {
            let item_opts = vec![
                (atom(env, "partition_key"), args[idx + 1]).encode(env),
                (atom(env, "payload"), args[idx + 2]).encode(env),
            ];
            items.push((args[idx], item_opts).encode(env));
        } else {
            items.push(
                (
                    atom(env, "id"),
                    args[idx],
                    atom(env, "payload"),
                    args[idx + 1],
                )
                    .encode(env),
            );
        }
        idx += item_width;
    }

    let partition = if mixed {
        atoms::nil().encode(env)
    } else {
        args[0]
    };

    (tag, partition, items, opts).encode(env)
}

fn make_flow_get_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    let tag = atom(env, "flow_get");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.get")).encode(env);
    }

    match parse_flow_read_options(env, args, arg_bytes, 1, flow_partition_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_policy_set_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_policy_set");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.policy.set")).encode(env);
    }

    match parse_flow_policy_set_options(env, args, arg_bytes, 1) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_policy_get_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_policy_get");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.policy.get")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_policy_get_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_claim_due_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_claim_due");
    if args.len() < 3 {
        return (tag, wrong_number_error(env, b"flow.claim_due")).encode(env);
    }

    match parse_flow_read_options(env, args, arg_bytes, 1, flow_claim_due_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_reclaim_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_reclaim");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.reclaim")).encode(env);
    }

    match parse_flow_read_options(env, args, arg_bytes, 1, flow_claim_due_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_complete_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_complete");
    if args.len() < 4 {
        return (tag, wrong_number_error(env, b"flow.complete")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 2, flow_terminal_option) {
        Ok(opts) => (tag, args[0], args[1], opts).encode(env),
        Err(err) => (tag, args[0], args[1], err).encode(env),
    }
}

fn make_flow_complete_many_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_complete_many");
    if args.len() < 5 {
        return (tag, wrong_number_error(env, b"flow.complete_many")).encode(env);
    }

    let mixed = ascii_eq_ignore_case(arg_bytes[0], b"MIXED");

    let Some(items_idx) = flow_find_option(arg_bytes, 1, b"ITEMS") else {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR flow items are required"),
        )
            .encode(env);
    };

    let item_width = if mixed { 4 } else { 3 };
    if items_idx == args.len() - 1 || (args.len() - items_idx - 1) % item_width != 0 {
        return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
    }

    let opts = match parse_flow_options_until(
        env,
        args,
        arg_bytes,
        1,
        items_idx,
        flow_complete_many_option,
    ) {
        Ok(opts) => opts,
        Err(err) => return (tag, args[0], err).encode(env),
    };

    let mut items = Vec::with_capacity((args.len() - items_idx - 1) / item_width);
    let mut idx = items_idx + 1;
    while idx < args.len() {
        let lease_idx = if mixed { idx + 2 } else { idx + 1 };
        let fencing_idx = if mixed { idx + 3 } else { idx + 2 };

        let fencing_token = match parse_int_bytes(arg_bytes[fencing_idx]) {
            Some(value) if value >= 0 => value,
            _ => {
                return (
                    tag,
                    args[0],
                    generic_ast_error(env, b"ERR value is not an integer or out of range"),
                )
                    .encode(env)
            }
        };

        if mixed {
            let item_opts = vec![
                (atom(env, "partition_key"), args[idx + 1]).encode(env),
                (atom(env, "lease_token"), args[lease_idx]).encode(env),
                (atom(env, "fencing_token"), fencing_token).encode(env),
            ];
            items.push((args[idx], item_opts).encode(env));
        } else {
            items.push(
                (
                    atom(env, "id"),
                    args[idx],
                    atom(env, "lease_token"),
                    args[lease_idx],
                    atom(env, "fencing_token"),
                    fencing_token,
                )
                    .encode(env),
            );
        }
        idx += item_width;
    }

    let partition = if mixed {
        atoms::nil().encode(env)
    } else {
        args[0]
    };

    (tag, partition, items, opts).encode(env)
}

fn make_flow_fail_many_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_fail_many");
    if args.len() < 5 {
        return (tag, wrong_number_error(env, b"flow.fail_many")).encode(env);
    }

    let mixed = ascii_eq_ignore_case(arg_bytes[0], b"MIXED");

    let Some(items_idx) = flow_find_option(arg_bytes, 1, b"ITEMS") else {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR flow items are required"),
        )
            .encode(env);
    };

    let item_width = if mixed { 4 } else { 3 };
    if items_idx == args.len() - 1 || (args.len() - items_idx - 1) % item_width != 0 {
        return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
    }

    let opts =
        match parse_flow_options_until(env, args, arg_bytes, 1, items_idx, flow_fail_many_option) {
            Ok(opts) => opts,
            Err(err) => return (tag, args[0], err).encode(env),
        };

    let mut items = Vec::with_capacity((args.len() - items_idx - 1) / item_width);
    let mut idx = items_idx + 1;
    while idx < args.len() {
        let lease_idx = if mixed { idx + 2 } else { idx + 1 };
        let fencing_idx = if mixed { idx + 3 } else { idx + 2 };

        let fencing_token = match parse_int_bytes(arg_bytes[fencing_idx]) {
            Some(value) if value >= 0 => value,
            _ => {
                return (
                    tag,
                    args[0],
                    generic_ast_error(env, b"ERR value is not an integer or out of range"),
                )
                    .encode(env)
            }
        };

        if mixed {
            let item_opts = vec![
                (atom(env, "partition_key"), args[idx + 1]).encode(env),
                (atom(env, "lease_token"), args[lease_idx]).encode(env),
                (atom(env, "fencing_token"), fencing_token).encode(env),
            ];
            items.push((args[idx], item_opts).encode(env));
        } else {
            items.push(
                (
                    atom(env, "id"),
                    args[idx],
                    atom(env, "lease_token"),
                    args[lease_idx],
                    atom(env, "fencing_token"),
                    fencing_token,
                )
                    .encode(env),
            );
        }
        idx += item_width;
    }

    let partition = if mixed {
        atoms::nil().encode(env)
    } else {
        args[0]
    };

    (tag, partition, items, opts).encode(env)
}

fn make_flow_retry_many_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_retry_many");
    if args.len() < 5 {
        return (tag, wrong_number_error(env, b"flow.retry_many")).encode(env);
    }

    let mixed = ascii_eq_ignore_case(arg_bytes[0], b"MIXED");

    let Some(items_idx) = flow_find_option(arg_bytes, 1, b"ITEMS") else {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR flow items are required"),
        )
            .encode(env);
    };

    let item_width = if mixed { 4 } else { 3 };
    if items_idx == args.len() - 1 || (args.len() - items_idx - 1) % item_width != 0 {
        return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
    }

    let opts = match parse_flow_options_until_with_retry_policy(
        env,
        args,
        arg_bytes,
        1,
        items_idx,
        flow_retry_many_option,
    ) {
        Ok(opts) => opts,
        Err(err) => return (tag, args[0], err).encode(env),
    };

    let mut items = Vec::with_capacity((args.len() - items_idx - 1) / item_width);
    let mut idx = items_idx + 1;
    while idx < args.len() {
        let lease_idx = if mixed { idx + 2 } else { idx + 1 };
        let fencing_idx = if mixed { idx + 3 } else { idx + 2 };

        let fencing_token = match parse_int_bytes(arg_bytes[fencing_idx]) {
            Some(value) if value >= 0 => value,
            _ => {
                return (
                    tag,
                    args[0],
                    generic_ast_error(env, b"ERR value is not an integer or out of range"),
                )
                    .encode(env)
            }
        };

        if mixed {
            let item_opts = vec![
                (atom(env, "partition_key"), args[idx + 1]).encode(env),
                (atom(env, "lease_token"), args[lease_idx]).encode(env),
                (atom(env, "fencing_token"), fencing_token).encode(env),
            ];
            items.push((args[idx], item_opts).encode(env));
        } else {
            items.push(
                (
                    atom(env, "id"),
                    args[idx],
                    atom(env, "lease_token"),
                    args[lease_idx],
                    atom(env, "fencing_token"),
                    fencing_token,
                )
                    .encode(env),
            );
        }
        idx += item_width;
    }

    let partition = if mixed {
        atoms::nil().encode(env)
    } else {
        args[0]
    };

    (tag, partition, items, opts).encode(env)
}

fn make_flow_cancel_many_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_cancel_many");
    if args.len() < 4 {
        return (tag, wrong_number_error(env, b"flow.cancel_many")).encode(env);
    }

    let mixed = ascii_eq_ignore_case(arg_bytes[0], b"MIXED");

    let Some(items_idx) = flow_find_option(arg_bytes, 1, b"ITEMS") else {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR flow items are required"),
        )
            .encode(env);
    };

    let item_width = if mixed { 3 } else { 2 };
    if items_idx == args.len() - 1 || (args.len() - items_idx - 1) % item_width != 0 {
        return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
    }

    let opts =
        match parse_flow_options_until(env, args, arg_bytes, 1, items_idx, flow_cancel_many_option)
        {
            Ok(opts) => opts,
            Err(err) => return (tag, args[0], err).encode(env),
        };

    let mut items = Vec::with_capacity((args.len() - items_idx - 1) / item_width);
    let mut idx = items_idx + 1;
    while idx < args.len() {
        let fencing_idx = if mixed { idx + 2 } else { idx + 1 };

        let fencing_token = match parse_int_bytes(arg_bytes[fencing_idx]) {
            Some(value) if value >= 0 => value,
            _ => {
                return (
                    tag,
                    args[0],
                    generic_ast_error(env, b"ERR value is not an integer or out of range"),
                )
                    .encode(env)
            }
        };

        if mixed {
            items.push(
                (
                    atom(env, "id"),
                    args[idx],
                    atom(env, "partition_key"),
                    args[idx + 1],
                    atom(env, "fencing_token"),
                    fencing_token,
                )
                    .encode(env),
            );
        } else {
            items.push(
                (
                    atom(env, "id"),
                    args[idx],
                    atom(env, "fencing_token"),
                    fencing_token,
                )
                    .encode(env),
            );
        }
        idx += item_width;
    }

    let partition = if mixed {
        atoms::nil().encode(env)
    } else {
        args[0]
    };

    (tag, partition, items, opts).encode(env)
}

fn make_flow_transition_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_transition");
    if args.len() < 5 {
        return (tag, wrong_number_error(env, b"flow.transition")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 3, flow_transition_option) {
        Ok(opts) => (tag, args[0], args[1], args[2], opts).encode(env),
        Err(err) => (tag, args[0], args[1], args[2], err).encode(env),
    }
}

fn make_flow_transition_many_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_transition_many");
    if args.len() < 7 {
        return (tag, wrong_number_error(env, b"flow.transition_many")).encode(env);
    }

    let mixed = ascii_eq_ignore_case(arg_bytes[0], b"MIXED");

    let Some(items_idx) = flow_find_option(arg_bytes, 3, b"ITEMS") else {
        return (
            tag,
            args[0],
            args[1],
            args[2],
            generic_ast_error(env, b"ERR flow items are required"),
        )
            .encode(env);
    };

    let item_width = if mixed { 4 } else { 3 };
    if items_idx == args.len() - 1 || (args.len() - items_idx - 1) % item_width != 0 {
        return (
            tag,
            args[0],
            args[1],
            args[2],
            generic_ast_error(env, b"ERR syntax error"),
        )
            .encode(env);
    }

    let opts = match parse_flow_options_until(
        env,
        args,
        arg_bytes,
        3,
        items_idx,
        flow_transition_many_option,
    ) {
        Ok(opts) => opts,
        Err(err) => return (tag, args[0], args[1], args[2], err).encode(env),
    };

    let mut items = Vec::with_capacity((args.len() - items_idx - 1) / item_width);
    let mut idx = items_idx + 1;
    while idx < args.len() {
        let fencing_idx = if mixed { idx + 2 } else { idx + 1 };
        let lease_idx = if mixed { idx + 3 } else { idx + 2 };

        let fencing_token = match parse_int_bytes(arg_bytes[fencing_idx]) {
            Some(value) if value >= 0 => value,
            _ => {
                return (
                    tag,
                    args[0],
                    args[1],
                    args[2],
                    generic_ast_error(env, b"ERR value is not an integer or out of range"),
                )
                    .encode(env)
            }
        };

        let lease_token = if ascii_eq_ignore_case(arg_bytes[lease_idx], b"-") {
            atoms::nil().encode(env)
        } else {
            args[lease_idx]
        };

        if mixed {
            let mut item_opts = vec![
                (atom(env, "partition_key"), args[idx + 1]).encode(env),
                (atom(env, "fencing_token"), fencing_token).encode(env),
            ];
            if !ascii_eq_ignore_case(arg_bytes[lease_idx], b"-") {
                item_opts.push((atom(env, "lease_token"), lease_token).encode(env));
            }
            items.push((args[idx], item_opts).encode(env));
        } else {
            items.push(
                (
                    atom(env, "id"),
                    args[idx],
                    atom(env, "fencing_token"),
                    fencing_token,
                    atom(env, "lease_token"),
                    lease_token,
                )
                    .encode(env),
            )
        }
        idx += item_width;
    }

    let partition = if mixed {
        atoms::nil().encode(env)
    } else {
        args[0]
    };

    (tag, partition, args[1], args[2], items, opts).encode(env)
}

fn make_flow_retry_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_retry");
    if args.len() < 4 {
        return (tag, wrong_number_error(env, b"flow.retry")).encode(env);
    }

    match parse_flow_options_with_retry_policy(env, args, arg_bytes, 2, flow_retry_option) {
        Ok(opts) => (tag, args[0], args[1], opts).encode(env),
        Err(err) => (tag, args[0], args[1], err).encode(env),
    }
}

fn make_flow_fail_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_fail");
    if args.len() < 4 {
        return (tag, wrong_number_error(env, b"flow.fail")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 2, flow_fail_option) {
        Ok(opts) => (tag, args[0], args[1], opts).encode(env),
        Err(err) => (tag, args[0], args[1], err).encode(env),
    }
}

fn make_flow_cancel_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_cancel");
    if args.len() < 3 {
        return (tag, wrong_number_error(env, b"flow.cancel")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_cancel_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_rewind_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_rewind");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.rewind")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_rewind_option) {
        Ok(opts) => {
            if flow_has_option(arg_bytes, 1, b"TO_EVENT") {
                (tag, args[0], opts).encode(env)
            } else {
                (
                    tag,
                    args[0],
                    generic_ast_error(env, b"ERR flow to_event is required"),
                )
                    .encode(env)
            }
        }
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_list_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_list");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.list")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_list_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_index_query_command_ast<'a>(
    env: Env<'a>,
    tag_name: &str,
    command_name: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, tag_name);
    if args.is_empty() {
        return (tag, wrong_number_error(env, command_name)).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_index_query_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_info_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_info");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.info")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_partition_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_stuck_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_stuck");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.stuck")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_stuck_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_history_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_history");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.history")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_history_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

type FlowOptionParser<'a> =
    fn(Env<'a>, &[Term<'a>], &[&[u8]], usize) -> Result<Option<Term<'a>>, Term<'a>>;

fn parse_flow_options<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    parser: FlowOptionParser<'a>,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    if (args.len() - start) % 2 != 0 {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    let mut opts = Vec::with_capacity((args.len() - start) / 2);
    let mut idx = start;
    while idx < args.len() {
        if let Some(opt) = parser(env, args, arg_bytes, idx)? {
            opts.push(opt);
        }
        idx += 2;
    }
    Ok(opts)
}

fn parse_flow_read_options<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    parser: FlowOptionParser<'a>,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::with_capacity((args.len().saturating_sub(start)) / 2 + 2);
    let mut idx = start;

    while idx < args.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], b"FULL") {
            let full = if idx + 1 < args.len() {
                match parse_bool_bytes(arg_bytes[idx + 1]) {
                    Some(value) => {
                        idx += 2;
                        value
                    }
                    None => {
                        idx += 1;
                        true
                    }
                }
            } else {
                idx += 1;
                true
            };

            opts.push((atom(env, "full"), full).encode(env));
            continue;
        }

        if ascii_eq_ignore_case(arg_bytes[idx], b"NOPAYLOAD") {
            opts.push((atom(env, "payload"), false).encode(env));
            idx += 1;
            continue;
        }

        if ascii_eq_ignore_case(arg_bytes[idx], b"PAYLOAD") {
            opts.push((atom(env, "payload"), true).encode(env));

            if idx + 1 < args.len() && ascii_eq_ignore_case(arg_bytes[idx + 1], b"MAXBYTES") {
                if idx + 2 >= args.len() {
                    return Err(generic_ast_error(env, b"ERR syntax error"));
                }

                if let Some(opt) = flow_option_value(
                    env,
                    "payload_max_bytes",
                    FlowOptType::NonNegative,
                    args[idx + 2],
                    arg_bytes[idx + 2],
                )? {
                    opts.push(opt);
                }

                idx += 3;
            } else {
                idx += 1;
            }

            continue;
        }

        if idx + 1 >= args.len() {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }

        if let Some(opt) = parser(env, args, arg_bytes, idx)? {
            opts.push(opt);
        }
        idx += 2;
    }

    Ok(opts)
}

fn parse_flow_options_until<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    end: usize,
    parser: FlowOptionParser<'a>,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    if (end - start) % 2 != 0 {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    let mut opts = Vec::with_capacity((end - start) / 2);
    let mut idx = start;
    while idx < end {
        if let Some(opt) = parser(env, args, arg_bytes, idx)? {
            opts.push(opt);
        }
        idx += 2;
    }
    Ok(opts)
}

fn parse_flow_options_with_retry_policy<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    parser: FlowOptionParser<'a>,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    parse_flow_options_until_with_retry_policy(env, args, arg_bytes, start, args.len(), parser)
}

fn parse_flow_options_until_with_retry_policy<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    end: usize,
    parser: FlowOptionParser<'a>,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    if (end - start) % 2 != 0 {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    let mut opts = Vec::with_capacity((end - start) / 2);
    let mut retry_opts = Vec::new();
    let mut backoff_opts = Vec::new();
    let mut idx = start;

    while idx < end {
        if flow_retry_policy_option_name(arg_bytes[idx]) {
            let (term, is_backoff) = flow_policy_retry_option(env, args, arg_bytes, idx)?;
            if is_backoff {
                backoff_opts.push(term);
            } else {
                retry_opts.push(term);
            }
        } else if let Some(opt) = parser(env, args, arg_bytes, idx)? {
            opts.push(opt);
        }

        idx += 2;
    }

    if !backoff_opts.is_empty() {
        retry_opts.push((atom(env, "backoff"), backoff_opts).encode(env));
    }

    if !retry_opts.is_empty() {
        opts.push((atom(env, "retry"), retry_opts).encode(env));
    }

    Ok(opts)
}

fn flow_retry_policy_option_name(value: &[u8]) -> bool {
    ascii_eq_ignore_case(value, b"MAX_RETRIES")
        || ascii_eq_ignore_case(value, b"BACKOFF")
        || ascii_eq_ignore_case(value, b"BASE_MS")
        || ascii_eq_ignore_case(value, b"MAX_MS")
        || ascii_eq_ignore_case(value, b"JITTER_PCT")
        || ascii_eq_ignore_case(value, b"EXHAUSTED_TO")
}

fn flow_find_option(arg_bytes: &[&[u8]], start: usize, name: &[u8]) -> Option<usize> {
    let mut idx = start;
    while idx < arg_bytes.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], name) {
            return Some(idx);
        }
        idx += 2;
    }
    None
}

fn flow_has_option(arg_bytes: &[&[u8]], start: usize, name: &[u8]) -> bool {
    let mut idx = start;
    while idx < arg_bytes.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], name) {
            return true;
        }
        idx += 2;
    }
    false
}

#[derive(Clone, Copy)]
enum FlowOptType<'a> {
    Binary,
    Boolean,
    Ref(&'a [u8]),
    NonNegative,
    Positive(&'a [u8]),
    Partition,
}

fn flow_create_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"TYPE", "type", FlowOptType::Binary),
            (b"STATE", "state", FlowOptType::Binary),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (
                b"PARENT_FLOW_ID",
                "parent_flow_id",
                FlowOptType::Ref(b"parent_flow_id"),
            ),
            (
                b"ROOT_FLOW_ID",
                "root_flow_id",
                FlowOptType::Ref(b"root_flow_id"),
            ),
            (
                b"CORRELATION_ID",
                "correlation_id",
                FlowOptType::Ref(b"correlation_id"),
            ),
            (b"RUN_AT", "run_at_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PRIORITY", "priority", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"TTL", "ttl_ms", FlowOptType::NonNegative),
            (
                b"HISTORY_MAX_EVENTS",
                "history_max_events",
                FlowOptType::Positive(b"history_max_events"),
            ),
            (b"IDEMPOTENT", "idempotent", FlowOptType::Boolean),
        ],
    )
}

fn parse_flow_policy_set_options<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::new();
    let mut retry_opts = Vec::new();
    let mut backoff_opts = Vec::new();
    let mut states = Vec::new();
    let mut idx = start;

    while idx < args.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], b"STATE") {
            if idx + 1 >= args.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }

            let mut end = idx + 2;
            while end < args.len() {
                if ascii_eq_ignore_case(arg_bytes[end], b"STATE") {
                    break;
                }
                if end + 1 >= args.len() {
                    return Err(generic_ast_error(env, b"ERR syntax error"));
                }
                end += 2;
            }

            let state_retry = parse_flow_policy_retry_options(env, args, arg_bytes, idx + 2, end)?;
            states.push(
                (
                    args[idx + 1],
                    vec![(atom(env, "retry"), state_retry).encode(env)],
                )
                    .encode(env),
            );
            idx = end;
            continue;
        }

        let (term, is_backoff) = flow_policy_retry_option(env, args, arg_bytes, idx)?;
        if is_backoff {
            backoff_opts.push(term);
        } else {
            retry_opts.push(term);
        }
        idx += 2;
    }

    if !backoff_opts.is_empty() {
        retry_opts.push((atom(env, "backoff"), backoff_opts).encode(env));
    }

    if !retry_opts.is_empty() {
        opts.push((atom(env, "retry"), retry_opts).encode(env));
    }

    if !states.is_empty() {
        opts.push((atom(env, "states"), states).encode(env));
    }

    Ok(opts)
}

fn parse_flow_policy_retry_options<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    end: usize,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    if (end - start) % 2 != 0 {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    let mut retry_opts = Vec::new();
    let mut backoff_opts = Vec::new();
    let mut idx = start;

    while idx < end {
        let (term, is_backoff) = flow_policy_retry_option(env, args, arg_bytes, idx)?;
        if is_backoff {
            backoff_opts.push(term);
        } else {
            retry_opts.push(term);
        }
        idx += 2;
    }

    if !backoff_opts.is_empty() {
        retry_opts.push((atom(env, "backoff"), backoff_opts).encode(env));
    }

    Ok(retry_opts)
}

fn flow_policy_retry_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<(Term<'a>, bool), Term<'a>> {
    if idx + 1 >= args.len() {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    if ascii_eq_ignore_case(arg_bytes[idx], b"MAX_RETRIES") {
        match parse_int_bytes(arg_bytes[idx + 1]) {
            Some(value) if value >= 0 => Ok(((atom(env, "max_retries"), value).encode(env), false)),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        }
    } else if ascii_eq_ignore_case(arg_bytes[idx], b"BACKOFF") {
        match parse_flow_policy_backoff_kind(env, arg_bytes[idx + 1]) {
            Ok(kind) => Ok(((atom(env, "kind"), kind).encode(env), true)),
            Err(err) => Err(err),
        }
    } else if ascii_eq_ignore_case(arg_bytes[idx], b"BASE_MS") {
        match parse_int_bytes(arg_bytes[idx + 1]) {
            Some(value) if value >= 0 => Ok(((atom(env, "base_ms"), value).encode(env), true)),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        }
    } else if ascii_eq_ignore_case(arg_bytes[idx], b"MAX_MS") {
        match parse_int_bytes(arg_bytes[idx + 1]) {
            Some(value) if value >= 0 => Ok(((atom(env, "max_ms"), value).encode(env), true)),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        }
    } else if ascii_eq_ignore_case(arg_bytes[idx], b"JITTER_PCT") {
        match parse_int_bytes(arg_bytes[idx + 1]) {
            Some(value) if value >= 0 => Ok(((atom(env, "jitter_pct"), value).encode(env), true)),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        }
    } else if ascii_eq_ignore_case(arg_bytes[idx], b"EXHAUSTED_TO") {
        Ok((
            (atom(env, "exhausted_to"), args[idx + 1]).encode(env),
            false,
        ))
    } else {
        Err(generic_ast_error(env, b"ERR syntax error"))
    }
}

fn parse_flow_policy_backoff_kind<'a>(env: Env<'a>, value: &[u8]) -> Result<Atom, Term<'a>> {
    if ascii_eq_ignore_case(value, b"NONE") {
        Ok(atom(env, "none"))
    } else if ascii_eq_ignore_case(value, b"FIXED") {
        Ok(atom(env, "fixed"))
    } else if ascii_eq_ignore_case(value, b"LINEAR") {
        Ok(atom(env, "linear"))
    } else if ascii_eq_ignore_case(value, b"EXPONENTIAL") {
        Ok(atom(env, "exponential"))
    } else {
        Err(generic_ast_error(
            env,
            b"ERR flow retry backoff kind must be none, fixed, linear, or exponential",
        ))
    }
}

fn flow_policy_get_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[(b"STATE", "state", FlowOptType::Binary)],
    )
}

fn flow_claim_due_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"WORKER", "worker", FlowOptType::Binary),
            (b"STATE", "state", FlowOptType::Binary),
            (b"LEASE_MS", "lease_ms", FlowOptType::Positive(b"lease_ms")),
            (b"LIMIT", "limit", FlowOptType::Positive(b"limit")),
            (b"PRIORITY", "priority", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_terminal_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"FENCING", "fencing_token", FlowOptType::NonNegative),
            (b"RESULT", "result", FlowOptType::Binary),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (b"TTL", "ttl_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_complete_many_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"RESULT", "result", FlowOptType::Binary),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (b"TTL", "ttl_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
        ],
    )
}

fn flow_transition_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"FENCING", "fencing_token", FlowOptType::NonNegative),
            (b"LEASE_TOKEN", "lease_token", FlowOptType::Binary),
            (b"RUN_AT", "run_at_ms", FlowOptType::NonNegative),
            (b"PRIORITY", "priority", FlowOptType::NonNegative),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_retry_many_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"ERROR", "error", FlowOptType::Binary),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (b"RUN_AT", "run_at_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_fail_many_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"ERROR", "error", FlowOptType::Binary),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (b"TTL", "ttl_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_cancel_many_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"REASON_REF", "reason_ref", FlowOptType::Ref(b"reason_ref")),
            (b"TTL", "ttl_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_transition_many_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"RUN_AT", "run_at_ms", FlowOptType::NonNegative),
            (b"PRIORITY", "priority", FlowOptType::NonNegative),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
        ],
    )
}

fn flow_retry_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"FENCING", "fencing_token", FlowOptType::NonNegative),
            (b"RUN_AT", "run_at_ms", FlowOptType::NonNegative),
            (b"ERROR", "error", FlowOptType::Binary),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_fail_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"FENCING", "fencing_token", FlowOptType::NonNegative),
            (b"ERROR", "error", FlowOptType::Binary),
            (b"PAYLOAD", "payload", FlowOptType::Binary),
            (b"TTL", "ttl_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_cancel_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"FENCING", "fencing_token", FlowOptType::NonNegative),
            (b"LEASE_TOKEN", "lease_token", FlowOptType::Binary),
            (b"REASON_REF", "reason_ref", FlowOptType::Ref(b"reason_ref")),
            (b"TTL", "ttl_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_rewind_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"TO_EVENT", "to_event", FlowOptType::Binary),
            (b"RUN_AT", "run_at_ms", FlowOptType::NonNegative),
            (b"EXPECT_STATE", "expect_state", FlowOptType::Binary),
            (b"REASON_REF", "reason_ref", FlowOptType::Ref(b"reason_ref")),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_list_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"STATE", "state", FlowOptType::Binary),
            (b"COUNT", "count", FlowOptType::Positive(b"count")),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"INCLUDE_COLD", "include_cold", FlowOptType::Boolean),
            (
                b"CONSISTENT_PROJECTION",
                "consistent_projection",
                FlowOptType::Boolean,
            ),
        ],
    )
}

fn flow_index_query_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"COUNT", "count", FlowOptType::Positive(b"count")),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"INCLUDE_COLD", "include_cold", FlowOptType::Boolean),
            (
                b"CONSISTENT_PROJECTION",
                "consistent_projection",
                FlowOptType::Boolean,
            ),
        ],
    )
}

fn flow_stuck_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"COUNT", "count", FlowOptType::Positive(b"count")),
            (b"OLDER_THAN", "older_than_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_history_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"COUNT", "count", FlowOptType::Positive(b"count")),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"INCLUDE_COLD", "include_cold", FlowOptType::Boolean),
            (
                b"CONSISTENT_PROJECTION",
                "consistent_projection",
                FlowOptType::Boolean,
            ),
            (b"VALUES", "values", FlowOptType::Boolean),
        ],
    )
}

fn flow_partition_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    flow_option(
        env,
        args,
        arg_bytes,
        idx,
        &[
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"INCLUDE_COLD", "include_cold", FlowOptType::Boolean),
            (
                b"CONSISTENT_PROJECTION",
                "consistent_projection",
                FlowOptType::Boolean,
            ),
        ],
    )
}

fn flow_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
    specs: &[(&[u8], &str, FlowOptType<'_>)],
) -> Result<Option<Term<'a>>, Term<'a>> {
    for (wire, key, opt_type) in specs {
        if ascii_eq_ignore_case(arg_bytes[idx], *wire) {
            return flow_option_value(env, *key, *opt_type, args[idx + 1], arg_bytes[idx + 1]);
        }
    }
    Err(generic_ast_error(env, b"ERR syntax error"))
}

fn flow_option_value<'a>(
    env: Env<'a>,
    key: &str,
    opt_type: FlowOptType<'_>,
    value_term: Term<'a>,
    value_bytes: &[u8],
) -> Result<Option<Term<'a>>, Term<'a>> {
    let key_atom = atom(env, key);

    match opt_type {
        FlowOptType::Binary => Ok(Some((key_atom, value_term).encode(env))),
        FlowOptType::Boolean => match parse_bool_bytes(value_bytes) {
            Some(value) => Ok(Some((key_atom, value).encode(env))),
            None => Err(generic_ast_error(
                env,
                b"ERR flow idempotent must be a boolean",
            )),
        },
        FlowOptType::Ref(_label) if value_bytes.len() <= FLOW_MAX_REF_SIZE => {
            Ok(Some((key_atom, value_term).encode(env)))
        }
        FlowOptType::Ref(label) => {
            let mut msg = b"ERR flow ".to_vec();
            msg.extend_from_slice(label);
            msg.extend_from_slice(b" too large (max 4096 bytes)");
            Err(generic_ast_error(env, &msg))
        }
        FlowOptType::Partition if ascii_eq_ignore_case(value_bytes, b"GLOBAL") => Ok(None),
        FlowOptType::Partition => Ok(Some((key_atom, value_term).encode(env))),
        FlowOptType::NonNegative => match parse_int_bytes(value_bytes) {
            Some(value) if value >= 0 => Ok(Some((key_atom, value).encode(env))),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        },
        FlowOptType::Positive(label) => match parse_int_bytes(value_bytes) {
            Some(value) if value > 0 => Ok(Some((key_atom, value).encode(env))),
            _ => {
                let mut msg = b"ERR flow ".to_vec();
                msg.extend_from_slice(label);
                msg.extend_from_slice(b" must be a positive integer");
                Err(generic_ast_error(env, &msg))
            }
        },
    }
}

fn parse_element_count_pairs_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    count_error: &[u8],
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
    let mut idx = 1;
    while idx < args.len() {
        match parse_int_bytes(arg_bytes[idx + 1]) {
            Some(count) if count >= 1 => pairs.push((args[idx], count).encode(env)),
            _ => return Err(generic_ast_error(env, count_error)),
        }
        idx += 2;
    }
    Ok(pairs)
}

fn parse_named_pos_int_ast<'a>(env: Env<'a>, data: &[u8], label: &[u8]) -> Result<i64, Term<'a>> {
    match parse_int_bytes(data) {
        Some(value) if value > 0 => Ok(value),
        Some(_) => {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(label);
            msg.extend_from_slice(b" must be a positive integer");
            Err(generic_ast_error(env, &msg))
        }
        None => {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(label);
            msg.extend_from_slice(b" is not an integer or out of range");
            Err(generic_ast_error(env, &msg))
        }
    }
}

fn parse_named_float_ast<'a>(
    env: Env<'a>,
    data: &[u8],
    label: &[u8],
    positive: bool,
) -> Result<f64, Term<'a>> {
    match parse_float_value_ast(env, data) {
        Ok(value) if !positive || value > 0.0 => Ok(value),
        Ok(_) => {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(label);
            msg.extend_from_slice(b" must be a positive number");
            Err(generic_ast_error(env, &msg))
        }
        Err(_) => {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(label);
            msg.extend_from_slice(b" is not a valid float");
            Err(generic_ast_error(env, &msg))
        }
    }
}

fn parse_named_prob_float_ast<'a>(
    env: Env<'a>,
    data: &[u8],
    label: &[u8],
) -> Result<f64, Term<'a>> {
    match parse_float_value_ast(env, data) {
        Ok(value) if value > 0.0 && value < 1.0 => Ok(value),
        Ok(_) => {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(label);
            msg.extend_from_slice(b" must be between 0 and 1 exclusive");
            Err(generic_ast_error(env, &msg))
        }
        Err(_) => {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(label);
            msg.extend_from_slice(b" is not a valid number");
            Err(generic_ast_error(env, &msg))
        }
    }
}

fn parse_decay_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<f64, Term<'a>> {
    match parse_float_value_ast(env, data) {
        Ok(value) if (0.0..=1.0).contains(&value) => Ok(value),
        Ok(_) => Err(generic_ast_error(env, b"ERR decay must be between 0 and 1")),
        Err(_) => Err(generic_ast_error(env, b"ERR decay is not a valid number")),
    }
}

fn parse_float_value_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<f64, Term<'a>> {
    std::str::from_utf8(data)
        .ok()
        .and_then(|value| f64::from_str(value).ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| generic_ast_error(env, b"ERR value is not a valid float"))
}

fn parse_quantile_float_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<f64, Term<'a>> {
    match parse_float_value_ast(env, data) {
        Ok(value) if (0.0..=1.0).contains(&value) => Ok(value),
        Ok(_) => Err(generic_ast_error(
            env,
            b"ERR quantile must be between 0 and 1",
        )),
        Err(err) => Err(err),
    }
}

fn split_once_byte(data: &[u8], needle: u8) -> Option<(&[u8], &[u8])> {
    let idx = data.iter().position(|byte| *byte == needle)?;
    Some((&data[..idx], &data[idx + 1..]))
}

fn parse_bitmap_offset_ast_arg<'a>(env: Env<'a>, data: &[u8]) -> Result<i64, Term<'a>> {
    match parse_int_bytes(data) {
        Some(value) if (0..=4_294_967_295_i64).contains(&value) => Ok(value),
        _ => Err(generic_ast_error(
            env,
            b"ERR bit offset is not an integer or out of range",
        )),
    }
}

fn parse_bit_value_ast_arg<'a>(env: Env<'a>, data: &[u8]) -> Result<i64, Term<'a>> {
    if data == b"0" {
        Ok(0)
    } else if data == b"1" {
        Ok(1)
    } else {
        Err(generic_ast_error(
            env,
            b"ERR bit is not an integer or out of range",
        ))
    }
}

fn parse_bitmap_mode_ast_arg<'a>(env: Env<'a>, data: &[u8]) -> Result<Atom, Term<'a>> {
    if ascii_eq_ignore_case(data, b"BYTE") {
        Ok(atoms::byte())
    } else if ascii_eq_ignore_case(data, b"BIT") {
        Ok(atoms::bit())
    } else {
        Err(generic_ast_error(env, b"ERR syntax error"))
    }
}

fn parse_bitop_atom_ast_arg<'a>(
    env: Env<'a>,
    data: &[u8],
    source_count: usize,
) -> Result<Atom, Term<'a>> {
    if ascii_eq_ignore_case(data, b"AND") {
        Ok(atoms::band())
    } else if ascii_eq_ignore_case(data, b"OR") {
        Ok(atoms::bor())
    } else if ascii_eq_ignore_case(data, b"XOR") {
        Ok(atoms::bxor())
    } else if ascii_eq_ignore_case(data, b"NOT") {
        if source_count == 1 {
            Ok(atoms::bnot())
        } else {
            Err(generic_ast_error(
                env,
                b"ERR BITOP NOT requires one and only one key",
            ))
        }
    } else {
        Err(generic_ast_error(env, b"ERR syntax error"))
    }
}

fn parse_int_ast_arg<'a>(env: Env<'a>, data: &[u8]) -> Result<i64, Term<'a>> {
    parse_int_bytes(data)
        .ok_or_else(|| generic_ast_error(env, b"ERR value is not an integer or out of range"))
}

fn parse_float_ast_arg<'a>(env: Env<'a>, data: &[u8]) -> Result<f64, Term<'a>> {
    std::str::from_utf8(data)
        .ok()
        .and_then(|value| f64::from_str(value).ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| generic_ast_error(env, b"ERR value is not a valid float"))
}

fn parse_expiry_flag_ast<'a>(env: Env<'a>, data: &[u8]) -> Result<Atom, Term<'a>> {
    if ascii_eq_ignore_case(data, b"NX") {
        Ok(atoms::nx())
    } else if ascii_eq_ignore_case(data, b"XX") {
        Ok(atoms::xx())
    } else if ascii_eq_ignore_case(data, b"GT") {
        Ok(atoms::gt())
    } else if ascii_eq_ignore_case(data, b"LT") {
        Ok(atoms::lt())
    } else {
        let mut msg = b"ERR Unsupported option ".to_vec();
        msg.extend_from_slice(data);
        Err(generic_ast_error(env, &msg))
    }
}

fn generic_ast_error<'a>(env: Env<'a>, msg: &[u8]) -> Term<'a> {
    (atoms::error(), make_binary_term(env, msg)).encode(env)
}

fn atom(env: Env<'_>, name: &str) -> Atom {
    Atom::from_str(env, name).unwrap()
}

fn make_set_command_ast<'a>(env: Env<'a>, args: &[Term<'a>], arg_bytes: &[&[u8]]) -> Term<'a> {
    debug_assert!(args.len() >= 2);

    match parse_set_options_ast(env, &args[2..], &arg_bytes[2..]) {
        Ok(opts) => (atoms::set(), args[0], args[1], opts).encode(env),
        Err(err) => (atoms::set(), args[0], args[1], err).encode(env),
    }
}

fn parse_set_options_ast<'a>(
    env: Env<'a>,
    _opt_terms: &[Term<'a>],
    opt_bytes: &[&[u8]],
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::new();
    let mut idx = 0;
    let mut has_expiry = false;
    let mut has_nx = false;
    let mut has_xx = false;

    while idx < opt_bytes.len() {
        let opt = opt_bytes[idx];

        if ascii_eq_ignore_case(opt, b"NX") {
            has_nx = true;
            opts.push(atoms::nx().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(opt, b"XX") {
            has_xx = true;
            opts.push(atoms::xx().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(opt, b"GET") {
            opts.push(atoms::get().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(opt, b"KEEPTTL") {
            if has_expiry {
                return Err(set_ast_error(env, b"ERR syntax error"));
            }
            has_expiry = true;
            opts.push(atoms::keepttl().encode(env));
            idx += 1;
        } else if ascii_eq_ignore_case(opt, b"EX")
            || ascii_eq_ignore_case(opt, b"PX")
            || ascii_eq_ignore_case(opt, b"EXAT")
            || ascii_eq_ignore_case(opt, b"PXAT")
        {
            if has_expiry || idx + 1 >= opt_bytes.len() {
                return Err(set_ast_error(env, b"ERR syntax error"));
            }

            let value = match parse_int_bytes(opt_bytes[idx + 1]) {
                Some(value) if value > 0 => value,
                Some(_) => {
                    return Err(set_ast_error(
                        env,
                        b"ERR invalid expire time in 'set' command",
                    ))
                }
                None => {
                    return Err(set_ast_error(
                        env,
                        b"ERR value is not an integer or out of range",
                    ))
                }
            };

            has_expiry = true;
            let term = if ascii_eq_ignore_case(opt, b"EX") {
                (atoms::ex(), value).encode(env)
            } else if ascii_eq_ignore_case(opt, b"PX") {
                (atoms::px(), value).encode(env)
            } else if ascii_eq_ignore_case(opt, b"EXAT") {
                (atoms::exat(), value).encode(env)
            } else {
                (atoms::pxat(), value).encode(env)
            };

            opts.push(term);
            idx += 2;
        } else {
            let mut msg = b"ERR syntax error, option '".to_vec();
            msg.extend_from_slice(opt);
            msg.extend_from_slice(b"' not recognized");
            return Err(set_ast_error(env, &msg));
        }
    }

    if has_nx && has_xx {
        return Err(set_ast_error(
            env,
            b"ERR XX and NX options at the same time are not compatible",
        ));
    }

    Ok(opts)
}

fn set_ast_error<'a>(env: Env<'a>, msg: &[u8]) -> Term<'a> {
    (atoms::error(), make_binary_term(env, msg)).encode(env)
}

fn make_command_keys<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let keys: Vec<Term<'a>> = command_key_indices(cmd, arg_bytes)
        .into_iter()
        .filter_map(|idx| args.get(idx).copied())
        .collect();

    keys.encode(env)
}

fn command_key_indices(cmd: &[u8], arg_bytes: &[&[u8]]) -> Vec<usize> {
    let argc = arg_bytes.len();
    if argc == 0 {
        return Vec::new();
    }

    match cmd {
        b"DEL" | b"EXISTS" | b"MGET" | b"UNLINK" | b"PFCOUNT" | b"SDIFF" | b"SINTER"
        | b"SUNION" | b"WATCH" => all_indices(argc),

        b"MSET" | b"MSETNX" => stepped_indices(0, argc, 2),

        b"RENAME" | b"RENAMENX" | b"COPY" | b"LMOVE" | b"RPOPLPUSH" | b"GEOSEARCHSTORE" => {
            first_n_indices(argc, 2)
        }

        b"BITOP" => range_indices(1, argc),
        b"PFMERGE" | b"SDIFFSTORE" | b"SINTERSTORE" | b"SUNIONSTORE" => all_indices(argc),
        b"SINTERCARD" => counted_key_indices(arg_bytes, 0, 1),
        b"BLPOP" | b"BRPOP" => argc
            .checked_sub(1)
            .map_or_else(Vec::new, |end| range_indices(0, end)),
        b"BLMOVE" => first_n_indices(argc, 2),
        b"BLMPOP" => counted_key_indices(arg_bytes, 1, 2),
        b"ZUNIONSTORE" | b"ZINTERSTORE" => counted_key_indices(arg_bytes, 1, 2),
        b"XREAD" | b"XREADGROUP" => stream_key_indices(arg_bytes),
        b"XGROUP" | b"XINFO" => {
            if argc > 1 {
                vec![1]
            } else {
                Vec::new()
            }
        }
        b"JSON.MGET" => argc
            .checked_sub(1)
            .map_or_else(Vec::new, |end| range_indices(0, end)),
        b"CMS.MERGE" => counted_key_indices_with_destination(arg_bytes, 1, 2),
        b"TDIGEST.MERGE" => counted_key_indices_with_destination(arg_bytes, 1, 2),
        b"RATELIMIT.ADD" => vec![0],
        b"FLOW.CREATE_MANY" => flow_create_many_key_indices(arg_bytes),
        b"FLOW.COMPLETE_MANY" => flow_complete_many_key_indices(arg_bytes),
        b"FLOW.RETRY_MANY" => flow_retry_many_key_indices(arg_bytes),
        b"FLOW.FAIL_MANY" => flow_fail_many_key_indices(arg_bytes),
        b"FLOW.CANCEL_MANY" => flow_cancel_many_key_indices(arg_bytes),
        b"FLOW.TRANSITION_MANY" => flow_transition_many_key_indices(arg_bytes),
        b"FLOW.CREATE" | b"FLOW.GET" | b"FLOW.COMPLETE" | b"FLOW.TRANSITION" | b"FLOW.RETRY"
        | b"FLOW.FAIL" | b"FLOW.CANCEL" | b"FLOW.REWIND" | b"FLOW.HISTORY" => {
            vec![0]
        }
        b"MEMORY" => {
            if argc > 1 && ascii_eq_ignore_case(arg_bytes[0], b"USAGE") {
                vec![1]
            } else {
                Vec::new()
            }
        }
        b"OBJECT" => object_key_indices(arg_bytes),

        b"GET"
        | b"SET"
        | b"GETSET"
        | b"GETDEL"
        | b"GETEX"
        | b"SETNX"
        | b"SETEX"
        | b"PSETEX"
        | b"GETRANGE"
        | b"SETRANGE"
        | b"INCR"
        | b"DECR"
        | b"INCRBY"
        | b"DECRBY"
        | b"INCRBYFLOAT"
        | b"APPEND"
        | b"STRLEN"
        | b"EXPIRE"
        | b"EXPIREAT"
        | b"PEXPIRE"
        | b"PEXPIREAT"
        | b"TTL"
        | b"PTTL"
        | b"PERSIST"
        | b"TYPE"
        | b"EXPIRETIME"
        | b"PEXPIRETIME"
        | b"PFADD"
        | b"SETBIT"
        | b"GETBIT"
        | b"BITCOUNT"
        | b"BITPOS"
        | b"HSET"
        | b"HGET"
        | b"HDEL"
        | b"HMGET"
        | b"HGETALL"
        | b"HEXISTS"
        | b"HKEYS"
        | b"HVALS"
        | b"HLEN"
        | b"HINCRBY"
        | b"HINCRBYFLOAT"
        | b"HSETNX"
        | b"HSTRLEN"
        | b"HRANDFIELD"
        | b"HSCAN"
        | b"HEXPIRE"
        | b"HTTL"
        | b"HPERSIST"
        | b"HPEXPIRE"
        | b"HPTTL"
        | b"HEXPIRETIME"
        | b"HGETDEL"
        | b"HGETEX"
        | b"HSETEX"
        | b"LPUSH"
        | b"RPUSH"
        | b"LPOP"
        | b"RPOP"
        | b"LRANGE"
        | b"LLEN"
        | b"LINDEX"
        | b"LSET"
        | b"LREM"
        | b"LTRIM"
        | b"LPOS"
        | b"LINSERT"
        | b"LPUSHX"
        | b"RPUSHX"
        | b"SADD"
        | b"SREM"
        | b"SMEMBERS"
        | b"SISMEMBER"
        | b"SMISMEMBER"
        | b"SCARD"
        | b"SRANDMEMBER"
        | b"SPOP"
        | b"SMOVE"
        | b"SSCAN"
        | b"ZADD"
        | b"ZREM"
        | b"ZSCORE"
        | b"ZRANK"
        | b"ZREVRANK"
        | b"ZRANGE"
        | b"ZREVRANGE"
        | b"ZCARD"
        | b"ZINCRBY"
        | b"ZCOUNT"
        | b"ZPOPMIN"
        | b"ZPOPMAX"
        | b"ZRANDMEMBER"
        | b"ZSCAN"
        | b"ZMSCORE"
        | b"ZRANGEBYSCORE"
        | b"ZREVRANGEBYSCORE"
        | b"GEOADD"
        | b"GEOPOS"
        | b"GEODIST"
        | b"GEOHASH"
        | b"GEOSEARCH"
        | b"XADD"
        | b"XLEN"
        | b"XRANGE"
        | b"XREVRANGE"
        | b"XTRIM"
        | b"XDEL"
        | b"XACK"
        | b"JSON.SET"
        | b"JSON.GET"
        | b"JSON.DEL"
        | b"JSON.NUMINCRBY"
        | b"JSON.TYPE"
        | b"JSON.STRLEN"
        | b"JSON.OBJKEYS"
        | b"JSON.OBJLEN"
        | b"JSON.ARRAPPEND"
        | b"JSON.ARRLEN"
        | b"JSON.TOGGLE"
        | b"JSON.CLEAR"
        | b"CAS"
        | b"LOCK"
        | b"UNLOCK"
        | b"EXTEND"
        | b"FETCH_OR_COMPUTE"
        | b"FETCH_OR_COMPUTE_RESULT"
        | b"FETCH_OR_COMPUTE_ERROR"
        | b"BF.RESERVE"
        | b"BF.ADD"
        | b"BF.MADD"
        | b"BF.EXISTS"
        | b"BF.MEXISTS"
        | b"BF.CARD"
        | b"BF.INFO"
        | b"CF.RESERVE"
        | b"CF.ADD"
        | b"CF.ADDNX"
        | b"CF.DEL"
        | b"CF.EXISTS"
        | b"CF.MEXISTS"
        | b"CF.COUNT"
        | b"CF.INFO"
        | b"CMS.INITBYDIM"
        | b"CMS.INITBYPROB"
        | b"CMS.INCRBY"
        | b"CMS.QUERY"
        | b"CMS.INFO"
        | b"TOPK.RESERVE"
        | b"TOPK.ADD"
        | b"TOPK.INCRBY"
        | b"TOPK.QUERY"
        | b"TOPK.LIST"
        | b"TOPK.COUNT"
        | b"TOPK.INFO"
        | b"TDIGEST.CREATE"
        | b"TDIGEST.ADD"
        | b"TDIGEST.RESET"
        | b"TDIGEST.QUANTILE"
        | b"TDIGEST.CDF"
        | b"TDIGEST.TRIMMED_MEAN"
        | b"TDIGEST.MIN"
        | b"TDIGEST.MAX"
        | b"TDIGEST.INFO"
        | b"TDIGEST.RANK"
        | b"TDIGEST.REVRANK"
        | b"TDIGEST.BYRANK"
        | b"TDIGEST.BYREVRANK"
        | b"CLUSTER.KEYSLOT"
        | b"FERRICSTORE.KEY_INFO" => vec![0],

        _ => Vec::new(),
    }
}

fn flow_create_many_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    if arg_bytes.is_empty() || !ascii_eq_ignore_case(arg_bytes[0], b"MIXED") {
        return vec![0];
    }

    let Some(items_idx) = option_index(arg_bytes, 1, b"ITEMS") else {
        return vec![0];
    };

    let mut keys = Vec::new();
    let mut idx = items_idx + 1;
    while idx + 2 < arg_bytes.len() {
        keys.push(idx + 1);
        idx += 3;
    }
    keys
}

fn flow_transition_many_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    if arg_bytes.is_empty() || !ascii_eq_ignore_case(arg_bytes[0], b"MIXED") {
        return vec![0];
    }

    let Some(items_idx) = option_index(arg_bytes, 3, b"ITEMS") else {
        return vec![0];
    };

    let mut keys = Vec::new();
    let mut idx = items_idx + 1;
    while idx + 3 < arg_bytes.len() {
        keys.push(idx + 1);
        idx += 4;
    }
    keys
}

fn flow_complete_many_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    if arg_bytes.is_empty() || !ascii_eq_ignore_case(arg_bytes[0], b"MIXED") {
        return vec![0];
    }

    let Some(items_idx) = option_index(arg_bytes, 1, b"ITEMS") else {
        return vec![0];
    };

    let mut keys = Vec::new();
    let mut idx = items_idx + 1;
    while idx + 3 < arg_bytes.len() {
        keys.push(idx + 1);
        idx += 4;
    }
    keys
}

fn flow_fail_many_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    flow_complete_many_key_indices(arg_bytes)
}

fn flow_retry_many_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    flow_complete_many_key_indices(arg_bytes)
}

fn flow_cancel_many_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    if arg_bytes.is_empty() || !ascii_eq_ignore_case(arg_bytes[0], b"MIXED") {
        return vec![0];
    }

    let Some(items_idx) = option_index(arg_bytes, 1, b"ITEMS") else {
        return vec![0];
    };

    let mut keys = Vec::new();
    let mut idx = items_idx + 1;
    while idx + 2 < arg_bytes.len() {
        keys.push(idx + 1);
        idx += 3;
    }
    keys
}

fn option_index(arg_bytes: &[&[u8]], start: usize, name: &[u8]) -> Option<usize> {
    let mut idx = start;
    while idx < arg_bytes.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], name) {
            return Some(idx);
        }
        idx += 2;
    }
    None
}

fn all_indices(argc: usize) -> Vec<usize> {
    range_indices(0, argc)
}

fn first_n_indices(argc: usize, n: usize) -> Vec<usize> {
    range_indices(0, argc.min(n))
}

fn range_indices(start: usize, end: usize) -> Vec<usize> {
    if start >= end {
        Vec::new()
    } else {
        (start..end).collect()
    }
}

fn stepped_indices(start: usize, end: usize, step: usize) -> Vec<usize> {
    if step == 0 || start >= end {
        Vec::new()
    } else {
        (start..end).step_by(step).collect()
    }
}

fn counted_key_indices(arg_bytes: &[&[u8]], count_idx: usize, keys_start: usize) -> Vec<usize> {
    match arg_bytes
        .get(count_idx)
        .and_then(|bytes| parse_int_bytes(bytes))
    {
        Some(count) if count > 0 => {
            range_indices(keys_start, arg_bytes.len().min(keys_start + count as usize))
        }
        _ => Vec::new(),
    }
}

fn counted_key_indices_with_destination(
    arg_bytes: &[&[u8]],
    count_idx: usize,
    keys_start: usize,
) -> Vec<usize> {
    let mut indices = vec![0];
    indices.extend(counted_key_indices(arg_bytes, count_idx, keys_start));
    indices
}

fn stream_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    let Some(streams_idx) = arg_bytes
        .iter()
        .position(|arg| ascii_eq_ignore_case(arg, b"STREAMS"))
    else {
        return Vec::new();
    };

    let tail = arg_bytes.len().saturating_sub(streams_idx + 1);
    if tail < 2 {
        return Vec::new();
    }

    let key_count = tail / 2;
    range_indices(streams_idx + 1, streams_idx + 1 + key_count)
}

fn object_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    if arg_bytes.len() > 1 && !ascii_eq_ignore_case(arg_bytes[0], b"HELP") {
        vec![1]
    } else {
        Vec::new()
    }
}

fn ascii_eq_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(&l, &r)| l.to_ascii_uppercase() == r)
}

fn command_tag_name(cmd: &[u8]) -> Option<&'static str> {
    match cmd {
        b"GET" => Some("get"),
        b"SET" => Some("set"),
        b"DEL" => Some("del"),
        b"EXISTS" => Some("exists"),
        b"MGET" => Some("mget"),
        b"MSET" => Some("mset"),
        b"INCR" => Some("incr"),
        b"DECR" => Some("decr"),
        b"INCRBY" => Some("incrby"),
        b"DECRBY" => Some("decrby"),
        b"INCRBYFLOAT" => Some("incrbyfloat"),
        b"APPEND" => Some("append"),
        b"STRLEN" => Some("strlen"),
        b"GETSET" => Some("getset"),
        b"GETDEL" => Some("getdel"),
        b"GETEX" => Some("getex"),
        b"SETNX" => Some("setnx"),
        b"SETEX" => Some("setex"),
        b"PSETEX" => Some("psetex"),
        b"GETRANGE" => Some("getrange"),
        b"SETRANGE" => Some("setrange"),
        b"MSETNX" => Some("msetnx"),
        b"EXPIRE" => Some("expire"),
        b"EXPIREAT" => Some("expireat"),
        b"PEXPIRE" => Some("pexpire"),
        b"PEXPIREAT" => Some("pexpireat"),
        b"TTL" => Some("ttl"),
        b"PTTL" => Some("pttl"),
        b"PERSIST" => Some("persist"),
        b"TYPE" => Some("type"),
        b"UNLINK" => Some("unlink"),
        b"RENAME" => Some("rename"),
        b"RENAMENX" => Some("renamenx"),
        b"COPY" => Some("copy"),
        b"RANDOMKEY" => Some("randomkey"),
        b"SCAN" => Some("scan"),
        b"EXPIRETIME" => Some("expiretime"),
        b"PEXPIRETIME" => Some("pexpiretime"),
        b"OBJECT" => Some("object"),
        b"WAIT" => Some("wait"),
        b"SETBIT" => Some("setbit"),
        b"GETBIT" => Some("getbit"),
        b"BITCOUNT" => Some("bitcount"),
        b"BITPOS" => Some("bitpos"),
        b"BITOP" => Some("bitop"),
        b"PFADD" => Some("pfadd"),
        b"PFCOUNT" => Some("pfcount"),
        b"PFMERGE" => Some("pfmerge"),
        b"HSET" => Some("hset"),
        b"HGET" => Some("hget"),
        b"HDEL" => Some("hdel"),
        b"HMGET" => Some("hmget"),
        b"HGETALL" => Some("hgetall"),
        b"HEXISTS" => Some("hexists"),
        b"HKEYS" => Some("hkeys"),
        b"HVALS" => Some("hvals"),
        b"HLEN" => Some("hlen"),
        b"HINCRBY" => Some("hincrby"),
        b"HINCRBYFLOAT" => Some("hincrbyfloat"),
        b"HSETNX" => Some("hsetnx"),
        b"HSTRLEN" => Some("hstrlen"),
        b"HRANDFIELD" => Some("hrandfield"),
        b"HSCAN" => Some("hscan"),
        b"HEXPIRE" => Some("hexpire"),
        b"HTTL" => Some("httl"),
        b"HPERSIST" => Some("hpersist"),
        b"HPEXPIRE" => Some("hpexpire"),
        b"HPTTL" => Some("hpttl"),
        b"HEXPIRETIME" => Some("hexpiretime"),
        b"HGETDEL" => Some("hgetdel"),
        b"HGETEX" => Some("hgetex"),
        b"HSETEX" => Some("hsetex"),
        b"LPUSH" => Some("lpush"),
        b"RPUSH" => Some("rpush"),
        b"LPOP" => Some("lpop"),
        b"RPOP" => Some("rpop"),
        b"LRANGE" => Some("lrange"),
        b"LLEN" => Some("llen"),
        b"LINDEX" => Some("lindex"),
        b"LSET" => Some("lset"),
        b"LREM" => Some("lrem"),
        b"LTRIM" => Some("ltrim"),
        b"LPOS" => Some("lpos"),
        b"LINSERT" => Some("linsert"),
        b"LMOVE" => Some("lmove"),
        b"LPUSHX" => Some("lpushx"),
        b"RPUSHX" => Some("rpushx"),
        b"RPOPLPUSH" => Some("rpoplpush"),
        b"SADD" => Some("sadd"),
        b"SREM" => Some("srem"),
        b"SMEMBERS" => Some("smembers"),
        b"SISMEMBER" => Some("sismember"),
        b"SMISMEMBER" => Some("smismember"),
        b"SCARD" => Some("scard"),
        b"SRANDMEMBER" => Some("srandmember"),
        b"SPOP" => Some("spop"),
        b"SDIFF" => Some("sdiff"),
        b"SINTER" => Some("sinter"),
        b"SUNION" => Some("sunion"),
        b"SDIFFSTORE" => Some("sdiffstore"),
        b"SINTERSTORE" => Some("sinterstore"),
        b"SUNIONSTORE" => Some("sunionstore"),
        b"SINTERCARD" => Some("sintercard"),
        b"SMOVE" => Some("smove"),
        b"SSCAN" => Some("sscan"),
        b"ZADD" => Some("zadd"),
        b"ZREM" => Some("zrem"),
        b"ZSCORE" => Some("zscore"),
        b"ZRANK" => Some("zrank"),
        b"ZREVRANK" => Some("zrevrank"),
        b"ZRANGE" => Some("zrange"),
        b"ZREVRANGE" => Some("zrevrange"),
        b"ZCARD" => Some("zcard"),
        b"ZINCRBY" => Some("zincrby"),
        b"ZCOUNT" => Some("zcount"),
        b"ZPOPMIN" => Some("zpopmin"),
        b"ZPOPMAX" => Some("zpopmax"),
        b"ZRANDMEMBER" => Some("zrandmember"),
        b"ZSCAN" => Some("zscan"),
        b"ZMSCORE" => Some("zmscore"),
        b"ZRANGEBYSCORE" => Some("zrangebyscore"),
        b"ZREVRANGEBYSCORE" => Some("zrevrangebyscore"),
        b"GEOADD" => Some("geoadd"),
        b"GEOPOS" => Some("geopos"),
        b"GEODIST" => Some("geodist"),
        b"GEOHASH" => Some("geohash"),
        b"GEOSEARCH" => Some("geosearch"),
        b"GEOSEARCHSTORE" => Some("geosearchstore"),
        b"XADD" => Some("xadd"),
        b"XLEN" => Some("xlen"),
        b"XRANGE" => Some("xrange"),
        b"XREVRANGE" => Some("xrevrange"),
        b"XREAD" => Some("xread"),
        b"XTRIM" => Some("xtrim"),
        b"XDEL" => Some("xdel"),
        b"XINFO" => Some("xinfo"),
        b"XGROUP" => Some("xgroup"),
        b"XREADGROUP" => Some("xreadgroup"),
        b"XACK" => Some("xack"),
        b"JSON.SET" => Some("json_set"),
        b"JSON.GET" => Some("json_get"),
        b"JSON.DEL" => Some("json_del"),
        b"JSON.NUMINCRBY" => Some("json_numincrby"),
        b"JSON.TYPE" => Some("json_type"),
        b"JSON.STRLEN" => Some("json_strlen"),
        b"JSON.OBJKEYS" => Some("json_objkeys"),
        b"JSON.OBJLEN" => Some("json_objlen"),
        b"JSON.ARRAPPEND" => Some("json_arrappend"),
        b"JSON.ARRLEN" => Some("json_arrlen"),
        b"JSON.TOGGLE" => Some("json_toggle"),
        b"JSON.CLEAR" => Some("json_clear"),
        b"JSON.MGET" => Some("json_mget"),
        b"CAS" => Some("cas"),
        b"LOCK" => Some("lock"),
        b"UNLOCK" => Some("unlock"),
        b"EXTEND" => Some("extend"),
        b"FETCH_OR_COMPUTE" => Some("fetch_or_compute"),
        b"FETCH_OR_COMPUTE_RESULT" => Some("fetch_or_compute_result"),
        b"FETCH_OR_COMPUTE_ERROR" => Some("fetch_or_compute_error"),
        b"BF.RESERVE" => Some("bf_reserve"),
        b"BF.ADD" => Some("bf_add"),
        b"BF.MADD" => Some("bf_madd"),
        b"BF.EXISTS" => Some("bf_exists"),
        b"BF.MEXISTS" => Some("bf_mexists"),
        b"BF.CARD" => Some("bf_card"),
        b"BF.INFO" => Some("bf_info"),
        b"CF.RESERVE" => Some("cf_reserve"),
        b"CF.ADD" => Some("cf_add"),
        b"CF.ADDNX" => Some("cf_addnx"),
        b"CF.DEL" => Some("cf_del"),
        b"CF.EXISTS" => Some("cf_exists"),
        b"CF.MEXISTS" => Some("cf_mexists"),
        b"CF.COUNT" => Some("cf_count"),
        b"CF.INFO" => Some("cf_info"),
        b"CMS.INITBYDIM" => Some("cms_initbydim"),
        b"CMS.INITBYPROB" => Some("cms_initbyprob"),
        b"CMS.INCRBY" => Some("cms_incrby"),
        b"CMS.QUERY" => Some("cms_query"),
        b"CMS.MERGE" => Some("cms_merge"),
        b"CMS.INFO" => Some("cms_info"),
        b"TOPK.RESERVE" => Some("topk_reserve"),
        b"TOPK.ADD" => Some("topk_add"),
        b"TOPK.INCRBY" => Some("topk_incrby"),
        b"TOPK.QUERY" => Some("topk_query"),
        b"TOPK.LIST" => Some("topk_list"),
        b"TOPK.COUNT" => Some("topk_count"),
        b"TOPK.INFO" => Some("topk_info"),
        b"TDIGEST.CREATE" => Some("tdigest_create"),
        b"TDIGEST.ADD" => Some("tdigest_add"),
        b"TDIGEST.RESET" => Some("tdigest_reset"),
        b"TDIGEST.QUANTILE" => Some("tdigest_quantile"),
        b"TDIGEST.CDF" => Some("tdigest_cdf"),
        b"TDIGEST.TRIMMED_MEAN" => Some("tdigest_trimmed_mean"),
        b"TDIGEST.MIN" => Some("tdigest_min"),
        b"TDIGEST.MAX" => Some("tdigest_max"),
        b"TDIGEST.INFO" => Some("tdigest_info"),
        b"TDIGEST.RANK" => Some("tdigest_rank"),
        b"TDIGEST.REVRANK" => Some("tdigest_revrank"),
        b"TDIGEST.BYRANK" => Some("tdigest_byrank"),
        b"TDIGEST.BYREVRANK" => Some("tdigest_byrevrank"),
        b"TDIGEST.MERGE" => Some("tdigest_merge"),
        b"PUBLISH" => Some("publish"),
        b"PUBSUB" => Some("pubsub"),
        b"PING" => Some("ping"),
        b"ECHO" => Some("echo"),
        b"DBSIZE" => Some("dbsize"),
        b"KEYS" => Some("keys"),
        b"FLUSHDB" => Some("flushdb"),
        b"FLUSHALL" => Some("flushall"),
        b"INFO" => Some("info"),
        b"COMMAND" => Some("command"),
        b"SELECT" => Some("select"),
        b"LOLWUT" => Some("lolwut"),
        b"DEBUG" => Some("debug"),
        b"SLOWLOG" => Some("slowlog"),
        b"SAVE" => Some("save"),
        b"BGSAVE" => Some("bgsave"),
        b"LASTSAVE" => Some("lastsave"),
        b"CONFIG" => Some("config"),
        b"MODULE" => Some("module"),
        b"WAITAOF" => Some("waitaof"),
        b"RATELIMIT.ADD" => Some("ratelimit_add"),
        b"CLUSTER.HEALTH" => Some("cluster_health"),
        b"CLUSTER.STATS" => Some("cluster_stats"),
        b"CLUSTER.KEYSLOT" => Some("cluster_keyslot"),
        b"CLUSTER.SLOTS" => Some("cluster_slots"),
        b"CLUSTER.STATUS" => Some("cluster_status"),
        b"CLUSTER.JOIN" => Some("cluster_join"),
        b"CLUSTER.ENABLE" => Some("cluster_enable"),
        b"CLUSTER.LEAVE" => Some("cluster_leave"),
        b"CLUSTER.FAILOVER" => Some("cluster_failover"),
        b"CLUSTER.PROMOTE" => Some("cluster_promote"),
        b"CLUSTER.DEMOTE" => Some("cluster_demote"),
        b"CLUSTER.ROLE" => Some("cluster_role"),
        b"FERRICSTORE.HOTNESS" => Some("ferricstore_hotness"),
        b"FERRICSTORE.CONFIG" => Some("ferricstore_config"),
        b"FERRICSTORE.METRICS" => Some("ferricstore_metrics"),
        b"FERRICSTORE.KEY_INFO" => Some("ferricstore_key_info"),
        b"MEMORY" => Some("memory"),
        b"HELLO" => Some("hello"),
        b"CLIENT" => Some("client"),
        b"QUIT" => Some("quit"),
        b"AUTH" => Some("auth"),
        b"ACL" => Some("acl"),
        b"RESET" => Some("reset"),
        b"SANDBOX" => Some("sandbox"),
        b"MULTI" => Some("multi"),
        b"EXEC" => Some("exec"),
        b"DISCARD" => Some("discard"),
        b"WATCH" => Some("watch"),
        b"UNWATCH" => Some("unwatch"),
        b"SUBSCRIBE" => Some("subscribe"),
        b"UNSUBSCRIBE" => Some("unsubscribe"),
        b"PSUBSCRIBE" => Some("psubscribe"),
        b"PUNSUBSCRIBE" => Some("punsubscribe"),
        b"BLPOP" => Some("blpop"),
        b"BRPOP" => Some("brpop"),
        b"BLMOVE" => Some("blmove"),
        b"BLMPOP" => Some("blmpop"),
        _ => None,
    }
}

fn make_uppercase_term<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    start: usize,
    len: usize,
) -> Term<'a> {
    let bytes = &buf[start..start + len];

    if bytes
        .first()
        .is_some_and(|first| first.is_ascii_uppercase())
    {
        unsafe { data.make_subbinary_unchecked(start, len) }.encode(env)
    } else {
        let mut bin = NewBinary::new(env, len);
        for (idx, &byte) in bytes.iter().enumerate() {
            bin.as_mut_slice()[idx] = byte.to_ascii_uppercase();
        }
        bin.into()
    }
}

fn uppercase_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|byte| byte.to_ascii_uppercase()).collect()
}

fn uppercase_binary_term<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    make_binary_term(env, &uppercase_bytes(bytes))
}

fn parse_inline_token_bytes(line: &[u8]) -> Result<Vec<Vec<u8>>, &'static [u8]> {
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < line.len() {
        while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
            i += 1;
        }

        if i >= line.len() {
            break;
        }

        let quote = line[i];

        if quote == b'"' || quote == b'\'' {
            i += 1;
            let mut token = Vec::new();
            let mut closed = false;

            while i < line.len() {
                match line[i] {
                    b if b == quote => {
                        i += 1;
                        closed = true;
                        break;
                    }
                    b'\\' if quote == b'"' => {
                        i += 1;

                        if i >= line.len() {
                            return Err(b"ERR Protocol error: unbalanced quotes in request");
                        }

                        token.push(match line[i] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 8,
                            b'a' => 7,
                            other => other,
                        });
                        i += 1;
                    }
                    byte => {
                        token.push(byte);
                        i += 1;
                    }
                }
            }

            if !closed {
                return Err(b"ERR Protocol error: unbalanced quotes in request");
            }

            tokens.push(token);
        } else {
            let start = i;

            while i < line.len() && line[i] != b' ' && line[i] != b'\t' {
                i += 1;
            }

            tokens.push(line[start..i].to_vec());
        }
    }

    Ok(tokens)
}

fn parse_non_negative_count(data: &[u8]) -> Result<usize, String> {
    match parse_int_bytes(data) {
        Some(n) if (0..=MAX_ARRAY_COUNT).contains(&n) => Ok(n as usize),
        _ => Err(lossy_str(data)),
    }
}

fn find_crlf(buf: &[u8], start: usize) -> Option<(usize, usize)> {
    if buf.len() < start + 2 {
        return None;
    }
    let end = buf.len() - 1;
    let mut i = start;
    while i < end {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some((i, i + 2));
        }
        i += 1;
    }
    None
}

fn parse_int_bytes(data: &[u8]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }

    let (negative, start) = if data[0] == b'-' {
        (true, 1)
    } else if data[0] == b'+' {
        (false, 1)
    } else {
        (false, 0)
    };

    if start >= data.len() {
        return None;
    }

    let mut result: i64 = 0;
    for &b in &data[start..] {
        if !b.is_ascii_digit() {
            return None;
        }
        let digit = (b - b'0') as i64;
        result = if negative {
            result.checked_mul(10)?.checked_sub(digit)?
        } else {
            result.checked_mul(10)?.checked_add(digit)?
        };
    }

    Some(result)
}

fn parse_bool_bytes(data: &[u8]) -> Option<bool> {
    if ascii_eq_ignore_case(data, b"TRUE") || data == b"1" {
        Some(true)
    } else if ascii_eq_ignore_case(data, b"FALSE") || data == b"0" {
        Some(false)
    } else {
        None
    }
}

fn make_binary_term<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    let mut bin = NewBinary::new(env, data.len());
    bin.as_mut_slice().copy_from_slice(data);
    bin.into()
}

fn lower_ascii_binary_term<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    let lowered = lower_ascii_bytes(data);
    make_binary_term(env, &lowered)
}

fn lower_ascii_bytes(data: &[u8]) -> Vec<u8> {
    data.iter().map(u8::to_ascii_lowercase).collect()
}

fn lossy_str(data: &[u8]) -> String {
    String::from_utf8_lossy(data).into_owned()
}

rustler::init!("Elixir.Ferricstore.Resp.ParserNif");

#[cfg(test)]
mod tests {
    use super::resp::*;
    use super::*;

    const MAX_SIZE: usize = 512 * 1024 * 1024; // 512 MB — generous limit for tests

    // Helper: parse single value, assert Ok, return (value, consumed_bytes)
    fn parse_ok(input: &[u8]) -> (RespValue<'_>, usize) {
        match parse_one_resp(input, 0, MAX_SIZE) {
            RespParseResult::Ok(v, pos) => (v, pos),
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    // =========================================================================
    // parse_int_bytes
    // =========================================================================

    #[test]
    fn parse_int_positive() {
        assert_eq!(parse_int_bytes(b"123"), Some(123));
        assert_eq!(parse_int_bytes(b"0"), Some(0));
        assert_eq!(parse_int_bytes(b"1"), Some(1));
        assert_eq!(parse_int_bytes(b"9999999999"), Some(9_999_999_999));
    }

    #[test]
    fn parse_int_negative() {
        assert_eq!(parse_int_bytes(b"-1"), Some(-1));
        assert_eq!(parse_int_bytes(b"-123"), Some(-123));
        assert_eq!(parse_int_bytes(b"-0"), Some(0));
    }

    #[test]
    fn parse_int_explicit_positive_sign() {
        assert_eq!(parse_int_bytes(b"+42"), Some(42));
        assert_eq!(parse_int_bytes(b"+0"), Some(0));
    }

    #[test]
    fn parse_int_empty() {
        assert_eq!(parse_int_bytes(b""), None);
    }

    #[test]
    fn parse_int_sign_only() {
        assert_eq!(parse_int_bytes(b"-"), None);
        assert_eq!(parse_int_bytes(b"+"), None);
    }

    #[test]
    fn parse_int_non_digit() {
        assert_eq!(parse_int_bytes(b"12a3"), None);
        assert_eq!(parse_int_bytes(b"abc"), None);
        assert_eq!(parse_int_bytes(b"1.5"), None);
        assert_eq!(parse_int_bytes(b" 1"), None);
        assert_eq!(parse_int_bytes(b"1 "), None);
    }

    #[test]
    fn parse_int_i64_max() {
        // i64::MAX = 9223372036854775807
        assert_eq!(parse_int_bytes(b"9223372036854775807"), Some(i64::MAX));
    }

    #[test]
    fn parse_int_overflow() {
        // i64::MAX + 1 overflows
        assert_eq!(parse_int_bytes(b"9223372036854775808"), None);
        // Way beyond i64
        assert_eq!(parse_int_bytes(b"99999999999999999999"), None);
    }

    #[test]
    fn parse_int_negative_overflow() {
        assert_eq!(parse_int_bytes(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_int_bytes(b"-9223372036854775809"), None);
    }

    #[test]
    fn parse_int_leading_zeros() {
        assert_eq!(parse_int_bytes(b"007"), Some(7));
        assert_eq!(parse_int_bytes(b"00"), Some(0));
    }

    // =========================================================================
    // find_crlf
    // =========================================================================

    #[test]
    fn find_crlf_basic() {
        assert_eq!(find_crlf(b"hello\r\n", 0), Some((5, 7)));
        assert_eq!(find_crlf(b"OK\r\n", 0), Some((2, 4)));
    }

    #[test]
    fn find_crlf_at_start() {
        assert_eq!(find_crlf(b"\r\n", 0), Some((0, 2)));
        assert_eq!(find_crlf(b"\r\nrest", 0), Some((0, 2)));
    }

    #[test]
    fn find_crlf_with_offset() {
        assert_eq!(find_crlf(b"skip\r\n", 2), Some((4, 6)));
        assert_eq!(find_crlf(b"AB\r\nCD\r\n", 4), Some((6, 8)));
    }

    #[test]
    fn find_crlf_not_found() {
        assert_eq!(find_crlf(b"no crlf here", 0), None);
        assert_eq!(find_crlf(b"only\n", 0), None);
        assert_eq!(find_crlf(b"only\r", 0), None);
    }

    #[test]
    fn find_crlf_buffer_too_short() {
        assert_eq!(find_crlf(b"", 0), None);
        assert_eq!(find_crlf(b"x", 0), None);
        assert_eq!(find_crlf(b"ab", 1), None);
    }

    #[test]
    fn find_crlf_cr_without_lf() {
        assert_eq!(find_crlf(b"a\rb", 0), None);
        assert_eq!(find_crlf(b"\r\r\r", 0), None);
    }

    #[test]
    fn find_crlf_lf_without_cr() {
        assert_eq!(find_crlf(b"\n\n", 0), None);
        assert_eq!(find_crlf(b"a\nb\n", 0), None);
    }

    #[test]
    fn find_crlf_multiple_takes_first() {
        assert_eq!(find_crlf(b"a\r\nb\r\n", 0), Some((1, 3)));
    }

    #[test]
    fn find_crlf_start_beyond_buffer() {
        assert_eq!(find_crlf(b"ab\r\n", 10), None);
    }

    // =========================================================================
    // lossy_str
    // =========================================================================

    #[test]
    fn lossy_str_valid_utf8() {
        assert_eq!(lossy_str(b"hello"), "hello");
        assert_eq!(lossy_str(b""), "");
        assert_eq!(lossy_str(b"123"), "123");
    }

    #[test]
    fn lossy_str_invalid_utf8() {
        let result = lossy_str(&[0xFF, 0xFE, b'a']);
        assert!(result.contains('\u{FFFD}'));
        assert!(result.contains('a'));
    }

    #[test]
    fn lossy_str_mixed_valid_invalid() {
        let input = b"hello\xFFworld";
        let result = lossy_str(input);
        assert!(result.starts_with("hello"));
        assert!(result.ends_with("world"));
        assert!(result.contains('\u{FFFD}'));
    }

    // =========================================================================
    // MAX_ARRAY_COUNT constant
    // =========================================================================

    #[test]
    fn max_array_count_value() {
        assert_eq!(MAX_ARRAY_COUNT, 1_048_576);
    }

    // =========================================================================
    // Basic RESP types — parse_one_resp
    // =========================================================================

    #[test]
    fn bulk_string_basic() {
        let (val, pos) = parse_ok(b"$3\r\nfoo\r\n");
        assert_eq!(val, RespValue::BulkString(b"foo"));
        assert_eq!(pos, 9);
    }

    #[test]
    fn bulk_string_empty() {
        let (val, pos) = parse_ok(b"$0\r\n\r\n");
        assert_eq!(val, RespValue::BulkString(b""));
        assert_eq!(pos, 6);
    }

    #[test]
    fn bulk_string_nil() {
        let (val, _) = parse_ok(b"$-1\r\n");
        assert_eq!(val, RespValue::Nil);
    }

    #[test]
    fn bulk_string_binary_data() {
        // Bulk string containing bytes that are NOT valid utf-8
        let mut input = b"$4\r\n".to_vec();
        input.extend_from_slice(&[0x00, 0xFF, 0xFE, 0x01]);
        input.extend_from_slice(b"\r\n");
        let (val, pos) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&[0x00, 0xFF, 0xFE, 0x01]));
        assert_eq!(pos, input.len());
    }

    #[test]
    fn bulk_string_with_crlf_inside() {
        // Bulk string containing \r\n within its payload: "he\r\nlo" = 6 bytes
        let (val, pos) = parse_ok(b"$6\r\nhe\r\nlo\r\n");
        assert_eq!(val, RespValue::BulkString(b"he\r\nlo"));
        assert_eq!(pos, 12); // $6\r\n(4) + he\r\nlo(6) + \r\n(2) = 12
    }

    #[test]
    fn simple_string() {
        let (val, pos) = parse_ok(b"+OK\r\n");
        assert_eq!(val, RespValue::SimpleString(b"OK"));
        assert_eq!(pos, 5);
    }

    #[test]
    fn simple_string_empty() {
        let (val, _) = parse_ok(b"+\r\n");
        assert_eq!(val, RespValue::SimpleString(b""));
    }

    #[test]
    fn simple_string_with_spaces() {
        let (val, _) = parse_ok(b"+hello world\r\n");
        assert_eq!(val, RespValue::SimpleString(b"hello world"));
    }

    #[test]
    fn simple_error() {
        let (val, pos) = parse_ok(b"-ERR unknown command\r\n");
        assert_eq!(val, RespValue::SimpleError(b"ERR unknown command"));
        assert_eq!(pos, 22);
    }

    #[test]
    fn simple_error_empty() {
        let (val, _) = parse_ok(b"-\r\n");
        assert_eq!(val, RespValue::SimpleError(b""));
    }

    #[test]
    fn integer_positive() {
        let (val, pos) = parse_ok(b":42\r\n");
        assert_eq!(val, RespValue::Integer(42));
        assert_eq!(pos, 5);
    }

    #[test]
    fn integer_zero() {
        let (val, _) = parse_ok(b":0\r\n");
        assert_eq!(val, RespValue::Integer(0));
    }

    #[test]
    fn integer_negative() {
        let (val, _) = parse_ok(b":-1\r\n");
        assert_eq!(val, RespValue::Integer(-1));
    }

    #[test]
    fn integer_large() {
        let (val, _) = parse_ok(b":9223372036854775807\r\n");
        assert_eq!(val, RespValue::Integer(i64::MAX));
    }

    #[test]
    fn null_resp3() {
        let (val, pos) = parse_ok(b"_\r\n");
        assert_eq!(val, RespValue::Nil);
        assert_eq!(pos, 3);
    }

    #[test]
    fn array_empty() {
        let (val, pos) = parse_ok(b"*0\r\n");
        assert_eq!(val, RespValue::Array(vec![]));
        assert_eq!(pos, 4);
    }

    #[test]
    fn array_nil() {
        let (val, _) = parse_ok(b"*-1\r\n");
        assert_eq!(val, RespValue::Nil);
    }

    #[test]
    fn array_single_bulk_string() {
        let (val, _) = parse_ok(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(val, RespValue::Array(vec![RespValue::BulkString(b"PING")]));
    }

    #[test]
    fn array_mixed_types() {
        // Array with bulk string, integer, simple string
        let input = b"*3\r\n$3\r\nfoo\r\n:42\r\n+OK\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo"),
                RespValue::Integer(42),
                RespValue::SimpleString(b"OK"),
            ])
        );
    }

    #[test]
    fn array_nested() {
        // *2\r\n *1\r\n$1\r\na\r\n *1\r\n$1\r\nb\r\n
        let input = b"*2\r\n*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::Array(vec![RespValue::BulkString(b"a")]),
                RespValue::Array(vec![RespValue::BulkString(b"b")]),
            ])
        );
    }

    // =========================================================================
    // Full commands via parse_resp
    // =========================================================================

    #[test]
    fn full_command_get() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![
                RespValue::BulkString(b"GET"),
                RespValue::BulkString(b"foo"),
            ])
        );
    }

    #[test]
    fn full_command_set() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![
                RespValue::BulkString(b"SET"),
                RespValue::BulkString(b"foo"),
                RespValue::BulkString(b"bar"),
            ])
        );
    }

    #[test]
    fn multiple_commands_in_buffer() {
        // Two pipelined commands
        let input = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 2);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![RespValue::BulkString(b"PING")])
        );
        assert_eq!(
            cmds[1],
            RespValue::Array(vec![
                RespValue::BulkString(b"GET"),
                RespValue::BulkString(b"k"),
            ])
        );
    }

    #[test]
    fn command_with_trailing_partial() {
        // One complete command + partial second command
        // *1\r\n$4\r\nPING\r\n = 4+4+4+2 = 14 bytes
        let input = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 1); // only the PING command
        assert_eq!(consumed, 14); // consumed the first command
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![RespValue::BulkString(b"PING")])
        );
    }

    // =========================================================================
    // Inline commands
    // =========================================================================

    #[test]
    fn inline_ping() {
        let (val, pos) = parse_ok(b"PING\r\n");
        assert_eq!(val, RespValue::Inline(vec![b"PING".as_slice()]));
        assert_eq!(pos, 6);
    }

    #[test]
    fn inline_set_with_args() {
        let (val, _) = parse_ok(b"SET foo bar\r\n");
        assert_eq!(
            val,
            RespValue::Inline(vec![
                b"SET".as_slice(),
                b"foo".as_slice(),
                b"bar".as_slice(),
            ])
        );
    }

    #[test]
    fn inline_extra_whitespace() {
        let (val, _) = parse_ok(b"SET  foo \t bar\r\n");
        assert_eq!(
            val,
            RespValue::Inline(vec![
                b"SET".as_slice(),
                b"foo".as_slice(),
                b"bar".as_slice(),
            ])
        );
    }

    #[test]
    fn inline_via_parse_resp() {
        let input = b"PING\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0], RespValue::Inline(vec![b"PING".as_slice()]));
    }

    #[test]
    fn inline_multiple_commands() {
        let input = b"PING\r\nINFO\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0], RespValue::Inline(vec![b"PING".as_slice()]));
        assert_eq!(cmds[1], RespValue::Inline(vec![b"INFO".as_slice()]));
    }

    // =========================================================================
    // Incomplete messages
    // =========================================================================

    #[test]
    fn incomplete_empty_input() {
        let (cmds, consumed) = parse_resp(b"", MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 0);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn incomplete_partial_bulk_header() {
        // "$3\r\n" without the data
        let result = parse_one_resp(b"$3\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_bulk_data() {
        // "$3\r\nfo" — data truncated
        let result = parse_one_resp(b"$3\r\nfo", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_bulk_no_trailing_crlf() {
        // "$3\r\nfoo" — data present but missing trailing \r\n
        let result = parse_one_resp(b"$3\r\nfoo", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_array_header() {
        // "*2\r\n" without any elements
        let result = parse_one_resp(b"*2\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_array_one_of_two() {
        // "*2\r\n$3\r\nfoo\r\n" — first element present, second missing
        let result = parse_one_resp(b"*2\r\n$3\r\nfoo\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_truncated_header_no_crlf() {
        let result = parse_one_resp(b"$3", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_just_type_byte() {
        assert_eq!(
            parse_one_resp(b"*", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b"$", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b"+", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b"-", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b":", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b"_", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
    }

    #[test]
    fn incomplete_simple_string_no_crlf() {
        let result = parse_one_resp(b"+OK", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_integer_no_crlf() {
        let result = parse_one_resp(b":42", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_inline_no_crlf() {
        let result = parse_one_resp(b"PING", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_preserves_rest_in_parse_resp() {
        // One complete command + incomplete fragment
        let input = b"+OK\r\n$3\r\nfo";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(consumed, 5); // only "+OK\r\n"
        assert_eq!(cmds[0], RespValue::SimpleString(b"OK"));
    }

    // =========================================================================
    // Malformed input
    // =========================================================================

    #[test]
    fn malformed_invalid_array_count() {
        let result = parse_one_resp(b"*abc\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidArrayCount("abc".into()))
        );
    }

    #[test]
    fn malformed_negative_array_count() {
        // *-2 is invalid (only *-1 for nil array)
        let result = parse_one_resp(b"*-2\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidArrayCount("-2".into()))
        );
    }

    #[test]
    fn malformed_array_too_large() {
        // MAX_ARRAY_COUNT + 1
        let input = format!("*{}\r\n", MAX_ARRAY_COUNT + 1);
        let result = parse_one_resp(input.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Error(RespError::ArrayTooLarge));
    }

    #[test]
    fn malformed_bulk_string_exceeds_max_value_size() {
        // max_value_size = 10, but bulk string claims length 100
        let result = parse_one_resp(b"$100\r\n", 0, 10);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::ValueTooLarge { len: 100, max: 10 })
        );
    }

    #[test]
    fn malformed_invalid_bulk_length() {
        let result = parse_one_resp(b"$xyz\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidBulkLength("xyz".into()))
        );
    }

    #[test]
    fn malformed_negative_bulk_length() {
        // $-2 is invalid (only $-1 for nil)
        let result = parse_one_resp(b"$-2\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidBulkLength("-2".into()))
        );
    }

    #[test]
    fn malformed_bulk_string_missing_crlf_terminator() {
        // Data present but \r\n replaced with something else
        let result = parse_one_resp(b"$3\r\nfooXY", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Error(RespError::BulkCrlfMissing));
    }

    #[test]
    fn malformed_invalid_integer() {
        let result = parse_one_resp(b":abc\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidInteger("abc".into()))
        );
    }

    #[test]
    fn malformed_invalid_null() {
        // "_" should be followed immediately by \r\n, not extra data
        let result = parse_one_resp(b"_extra\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidNull("extra".into()))
        );
    }

    #[test]
    fn malformed_error_propagates_through_parse_resp() {
        let result = parse_resp(b"$xyz\r\n", MAX_SIZE);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            RespError::InvalidBulkLength("xyz".into())
        );
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn edge_bulk_string_large_valid() {
        let data = vec![b'x'; 1000];
        let mut input = format!("${}\r\n", data.len()).into_bytes();
        input.extend_from_slice(&data);
        input.extend_from_slice(b"\r\n");
        let (val, pos) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&data));
        assert_eq!(pos, input.len());
    }

    #[test]
    fn edge_array_with_nil_elements() {
        // Array containing nil bulk strings
        let input = b"*2\r\n$-1\r\n$-1\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(val, RespValue::Array(vec![RespValue::Nil, RespValue::Nil]));
    }

    #[test]
    fn edge_position_tracking_across_multiple() {
        // Verify parse_resp consumes exactly the right number of bytes
        let input = b":1\r\n:2\r\n:3\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 3);
        assert_eq!(cmds[0], RespValue::Integer(1));
        assert_eq!(cmds[1], RespValue::Integer(2));
        assert_eq!(cmds[2], RespValue::Integer(3));
    }

    #[test]
    fn edge_bulk_string_with_zero_bytes() {
        // Bulk string containing null bytes
        let mut input = b"$3\r\n".to_vec();
        input.extend_from_slice(&[0x00, 0x00, 0x00]);
        input.extend_from_slice(b"\r\n");
        let (val, _) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&[0x00, 0x00, 0x00]));
    }

    #[test]
    fn edge_max_value_size_boundary() {
        // Exactly at the limit should succeed
        let data = vec![b'a'; 100];
        let mut input = format!("${}\r\n", data.len()).into_bytes();
        input.extend_from_slice(&data);
        input.extend_from_slice(b"\r\n");
        let (val, _) = match parse_one_resp(&input, 0, 100) {
            RespParseResult::Ok(v, p) => (v, p),
            other => panic!("expected Ok, got {:?}", other),
        };
        assert_eq!(val, RespValue::BulkString(data.as_slice()));

        // One over the limit should fail
        let data2 = vec![b'a'; 101];
        let mut input2 = format!("${}\r\n", data2.len()).into_bytes();
        input2.extend_from_slice(&data2);
        input2.extend_from_slice(b"\r\n");
        let result = parse_one_resp(&input2, 0, 100);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::ValueTooLarge { len: 101, max: 100 })
        );
    }

    #[test]
    fn edge_array_at_max_count_boundary() {
        // Array count exactly at MAX_ARRAY_COUNT should be accepted (structurally)
        // but will be Incomplete since we don't provide the elements
        let input = format!("*{}\r\n", MAX_ARRAY_COUNT);
        let result = parse_one_resp(input.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);

        // One over should be rejected immediately
        let input2 = format!("*{}\r\n", MAX_ARRAY_COUNT + 1);
        let result2 = parse_one_resp(input2.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result2, RespParseResult::Error(RespError::ArrayTooLarge));
    }

    #[test]
    fn edge_deeply_nested_array() {
        // *1\r\n *1\r\n *1\r\n $1\r\na\r\n
        let input = b"*1\r\n*1\r\n*1\r\n$1\r\na\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![RespValue::Array(vec![RespValue::Array(vec![
                RespValue::BulkString(b"a")
            ])])])
        );
    }

    #[test]
    fn edge_nested_array_at_depth_limit() {
        let input = nested_array(MAX_RESP_NESTING_DEPTH);
        let result = parse_one_resp(&input, 0, MAX_SIZE);
        assert!(matches!(result, RespParseResult::Ok(_, _)));
    }

    #[test]
    fn malformed_nested_array_exceeds_depth_limit() {
        let input = nested_array(MAX_RESP_NESTING_DEPTH + 1);
        let result = parse_one_resp(&input, 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Error(RespError::NestingTooDeep));
    }

    #[test]
    fn edge_array_containing_error_element() {
        let input = b"*2\r\n$3\r\nfoo\r\n-ERR bad\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo"),
                RespValue::SimpleError(b"ERR bad"),
            ])
        );
    }

    #[test]
    fn edge_inline_single_char() {
        let (val, _) = parse_ok(b"Q\r\n");
        assert_eq!(val, RespValue::Inline(vec![b"Q".as_slice()]));
    }

    #[test]
    fn edge_parse_resp_empty_yields_no_commands() {
        let (cmds, consumed) = parse_resp(b"", MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 0);
        assert_eq!(consumed, 0);
    }

    fn nested_array(depth: usize) -> Vec<u8> {
        let mut input = Vec::new();
        for _ in 0..depth {
            input.extend_from_slice(b"*1\r\n");
        }
        input.extend_from_slice(b"$1\r\na\r\n");
        input
    }

    #[test]
    fn ast_classifies_hot_string_commands_by_arity() {
        assert_eq!(classify_command_ast(b"GET", 1), CommandAstKind::Get);
        assert_eq!(classify_command_ast(b"SET", 2), CommandAstKind::Set);
        assert_eq!(classify_command_ast(b"PING", 0), CommandAstKind::Ping0);
        assert_eq!(classify_command_ast(b"PING", 1), CommandAstKind::Ping1);
        assert_eq!(classify_command_ast(b"DEL", 3), CommandAstKind::Del);
        assert_eq!(classify_command_ast(b"EXISTS", 2), CommandAstKind::Exists);
        assert_eq!(classify_command_ast(b"MGET", 4), CommandAstKind::Mget);
        assert_eq!(classify_command_ast(b"MSET", 4), CommandAstKind::Mset);
        assert_eq!(classify_command_ast(b"INCR", 1), CommandAstKind::Incr);
        assert_eq!(classify_command_ast(b"DECR", 1), CommandAstKind::Decr);
        assert_eq!(classify_command_ast(b"INCRBY", 2), CommandAstKind::Incrby);
        assert_eq!(classify_command_ast(b"DECRBY", 2), CommandAstKind::Decrby);
        assert_eq!(
            classify_command_ast(b"INCRBYFLOAT", 2),
            CommandAstKind::Incrbyfloat
        );
        assert_eq!(classify_command_ast(b"APPEND", 2), CommandAstKind::Append);
        assert_eq!(classify_command_ast(b"STRLEN", 1), CommandAstKind::Strlen);
    }

    #[test]
    fn ast_rejects_wrong_arity_to_unknown_for_fixed_arity_commands() {
        assert_eq!(classify_command_ast(b"GET", 0), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"GET", 2), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"SET", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"PING", 2), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"INCRBY", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"SETEX", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_knows_catalog_commands_even_when_semantic_shape_is_generic() {
        assert_eq!(command_tag_name(b"XADD"), Some("xadd"));
        assert_eq!(command_tag_name(b"JSON.SET"), Some("json_set"));
        assert_eq!(command_tag_name(b"BF.ADD"), Some("bf_add"));
        assert_eq!(command_tag_name(b"TDIGEST.MERGE"), Some("tdigest_merge"));
        assert_eq!(command_tag_name(b"CLUSTER.HEALTH"), Some("cluster_health"));
        assert_eq!(command_tag_name(b"CLUSTER.ENABLE"), Some("cluster_enable"));
        assert_eq!(command_tag_name(b"NO_SUCH_COMMAND"), None);
    }

    #[test]
    fn ast_classifies_json_commands_even_for_error_arity() {
        assert_eq!(
            classify_command_ast(b"JSON.SET", 4),
            CommandAstKind::JsonSet
        );
        assert_eq!(
            classify_command_ast(b"JSON.SET", 1),
            CommandAstKind::JsonSet
        );
        assert_eq!(
            classify_command_ast(b"JSON.GET", 2),
            CommandAstKind::JsonGet
        );
        assert_eq!(
            classify_command_ast(b"JSON.GET", 0),
            CommandAstKind::JsonGet
        );
        assert_eq!(
            classify_command_ast(b"JSON.DEL", 2),
            CommandAstKind::JsonDel
        );
        assert_eq!(
            classify_command_ast(b"JSON.NUMINCRBY", 3),
            CommandAstKind::JsonNumincrby
        );
        assert_eq!(
            classify_command_ast(b"JSON.TYPE", 2),
            CommandAstKind::JsonType
        );
        assert_eq!(
            classify_command_ast(b"JSON.STRLEN", 2),
            CommandAstKind::JsonStrlen
        );
        assert_eq!(
            classify_command_ast(b"JSON.OBJKEYS", 2),
            CommandAstKind::JsonObjkeys
        );
        assert_eq!(
            classify_command_ast(b"JSON.OBJLEN", 2),
            CommandAstKind::JsonObjlen
        );
        assert_eq!(
            classify_command_ast(b"JSON.ARRAPPEND", 4),
            CommandAstKind::JsonArrappend
        );
        assert_eq!(
            classify_command_ast(b"JSON.ARRLEN", 2),
            CommandAstKind::JsonArrlen
        );
        assert_eq!(
            classify_command_ast(b"JSON.TOGGLE", 2),
            CommandAstKind::JsonToggle
        );
        assert_eq!(
            classify_command_ast(b"JSON.CLEAR", 2),
            CommandAstKind::JsonClear
        );
        assert_eq!(
            classify_command_ast(b"JSON.MGET", 1),
            CommandAstKind::JsonMget
        );
    }

    #[test]
    fn ast_classifies_geo_commands_even_for_error_arity() {
        assert_eq!(classify_command_ast(b"GEOADD", 4), CommandAstKind::Geoadd);
        assert_eq!(classify_command_ast(b"GEOADD", 1), CommandAstKind::Geoadd);
        assert_eq!(classify_command_ast(b"GEOPOS", 2), CommandAstKind::Geopos);
        assert_eq!(classify_command_ast(b"GEODIST", 4), CommandAstKind::Geodist);
        assert_eq!(classify_command_ast(b"GEOHASH", 2), CommandAstKind::Geohash);
        assert_eq!(
            classify_command_ast(b"GEOSEARCH", 8),
            CommandAstKind::Geosearch
        );
        assert_eq!(
            classify_command_ast(b"GEOSEARCHSTORE", 9),
            CommandAstKind::Geosearchstore
        );
    }

    #[test]
    fn ast_classifies_hll_commands_even_for_error_arity() {
        assert_eq!(classify_command_ast(b"PFADD", 0), CommandAstKind::Pfadd);
        assert_eq!(classify_command_ast(b"PFCOUNT", 0), CommandAstKind::Pfcount);
        assert_eq!(classify_command_ast(b"PFMERGE", 1), CommandAstKind::Pfmerge);
    }

    #[test]
    fn ast_classifies_blocking_commands_even_for_error_arity() {
        assert_eq!(classify_command_ast(b"BLPOP", 0), CommandAstKind::Blpop);
        assert_eq!(classify_command_ast(b"BRPOP", 1), CommandAstKind::Brpop);
        assert_eq!(classify_command_ast(b"BLMOVE", 3), CommandAstKind::Blmove);
        assert_eq!(classify_command_ast(b"BLMPOP", 2), CommandAstKind::Blmpop);
    }

    #[test]
    fn ast_classifies_probabilistic_commands_even_for_error_arity() {
        assert_eq!(
            classify_command_ast(b"BF.RESERVE", 0),
            CommandAstKind::BfReserve
        );
        assert_eq!(classify_command_ast(b"BF.ADD", 1), CommandAstKind::BfAdd);
        assert_eq!(
            classify_command_ast(b"CF.RESERVE", 0),
            CommandAstKind::CfReserve
        );
        assert_eq!(
            classify_command_ast(b"CMS.INITBYDIM", 0),
            CommandAstKind::CmsInitbydim
        );
        assert_eq!(
            classify_command_ast(b"CMS.INCRBY", 2),
            CommandAstKind::CmsIncrby
        );
        assert_eq!(
            classify_command_ast(b"CMS.MERGE", 1),
            CommandAstKind::CmsMerge
        );
        assert_eq!(
            classify_command_ast(b"TOPK.RESERVE", 1),
            CommandAstKind::TopkReserve
        );
        assert_eq!(
            classify_command_ast(b"TOPK.LIST", 2),
            CommandAstKind::TopkList
        );
        assert_eq!(
            classify_command_ast(b"TDIGEST.CREATE", 0),
            CommandAstKind::TdigestCreate
        );
        assert_eq!(
            classify_command_ast(b"TDIGEST.MERGE", 1),
            CommandAstKind::TdigestMerge
        );
    }

    #[test]
    fn ast_classifies_flow_commands_even_for_error_arity() {
        assert_eq!(
            classify_command_ast(b"FLOW.CREATE", 0),
            CommandAstKind::FlowCreate
        );
        assert_eq!(
            classify_command_ast(b"FLOW.CREATE_MANY", 0),
            CommandAstKind::FlowCreateMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.COMPLETE_MANY", 0),
            CommandAstKind::FlowCompleteMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.RETRY_MANY", 0),
            CommandAstKind::FlowRetryMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.FAIL_MANY", 0),
            CommandAstKind::FlowFailMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.CANCEL_MANY", 0),
            CommandAstKind::FlowCancelMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.CLAIM_DUE", 0),
            CommandAstKind::FlowClaimDue
        );
        assert_eq!(
            classify_command_ast(b"FLOW.RECLAIM", 0),
            CommandAstKind::FlowReclaim
        );
        assert_eq!(
            classify_command_ast(b"FLOW.BY_PARENT", 0),
            CommandAstKind::FlowByParent
        );
        assert_eq!(
            classify_command_ast(b"FLOW.BY_ROOT", 0),
            CommandAstKind::FlowByRoot
        );
        assert_eq!(
            classify_command_ast(b"FLOW.BY_CORRELATION", 0),
            CommandAstKind::FlowByCorrelation
        );
    }

    #[test]
    fn ast_classifies_extended_string_commands() {
        assert_eq!(classify_command_ast(b"GETSET", 2), CommandAstKind::Getset);
        assert_eq!(classify_command_ast(b"GETDEL", 1), CommandAstKind::Getdel);
        assert_eq!(classify_command_ast(b"GETEX", 1), CommandAstKind::Getex);
        assert_eq!(classify_command_ast(b"GETEX", 3), CommandAstKind::Getex);
        assert_eq!(classify_command_ast(b"GETEX", 4), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"SETNX", 2), CommandAstKind::Setnx);
        assert_eq!(classify_command_ast(b"SETEX", 3), CommandAstKind::Setex);
        assert_eq!(classify_command_ast(b"PSETEX", 3), CommandAstKind::Psetex);
        assert_eq!(
            classify_command_ast(b"GETRANGE", 3),
            CommandAstKind::Getrange
        );
        assert_eq!(
            classify_command_ast(b"SETRANGE", 3),
            CommandAstKind::Setrange
        );
        assert_eq!(classify_command_ast(b"MSETNX", 6), CommandAstKind::Msetnx);
    }

    #[test]
    fn ast_classifies_expiry_commands_by_arity() {
        assert_eq!(classify_command_ast(b"EXPIRE", 2), CommandAstKind::Expire);
        assert_eq!(classify_command_ast(b"EXPIRE", 3), CommandAstKind::Expire);
        assert_eq!(classify_command_ast(b"PEXPIRE", 2), CommandAstKind::Pexpire);
        assert_eq!(classify_command_ast(b"PEXPIRE", 3), CommandAstKind::Pexpire);
        assert_eq!(
            classify_command_ast(b"EXPIREAT", 2),
            CommandAstKind::Expireat
        );
        assert_eq!(
            classify_command_ast(b"EXPIREAT", 3),
            CommandAstKind::Expireat
        );
        assert_eq!(
            classify_command_ast(b"PEXPIREAT", 2),
            CommandAstKind::Pexpireat
        );
        assert_eq!(
            classify_command_ast(b"PEXPIREAT", 3),
            CommandAstKind::Pexpireat
        );
        assert_eq!(classify_command_ast(b"TTL", 1), CommandAstKind::Ttl);
        assert_eq!(classify_command_ast(b"PTTL", 1), CommandAstKind::Pttl);
        assert_eq!(classify_command_ast(b"PERSIST", 1), CommandAstKind::Persist);
        assert_eq!(classify_command_ast(b"EXPIRE", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"TTL", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_list_commands_by_arity() {
        assert_eq!(classify_command_ast(b"LPUSH", 2), CommandAstKind::Lpush);
        assert_eq!(classify_command_ast(b"RPUSH", 3), CommandAstKind::Rpush);
        assert_eq!(classify_command_ast(b"LPOP", 1), CommandAstKind::Lpop);
        assert_eq!(classify_command_ast(b"LPOP", 2), CommandAstKind::Lpop);
        assert_eq!(classify_command_ast(b"LRANGE", 3), CommandAstKind::Lrange);
        assert_eq!(classify_command_ast(b"LINDEX", 2), CommandAstKind::Lindex);
        assert_eq!(classify_command_ast(b"LSET", 3), CommandAstKind::Lset);
        assert_eq!(classify_command_ast(b"LREM", 3), CommandAstKind::Lrem);
        assert_eq!(classify_command_ast(b"LTRIM", 3), CommandAstKind::Ltrim);
        assert_eq!(classify_command_ast(b"LINSERT", 4), CommandAstKind::Linsert);
        assert_eq!(classify_command_ast(b"LMOVE", 4), CommandAstKind::Lmove);
        assert_eq!(
            classify_command_ast(b"RPOPLPUSH", 2),
            CommandAstKind::Rpoplpush
        );
        assert_eq!(classify_command_ast(b"LPUSH", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"LRANGE", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_hash_commands_by_arity() {
        assert_eq!(classify_command_ast(b"HSET", 3), CommandAstKind::Hset);
        assert_eq!(classify_command_ast(b"HGET", 2), CommandAstKind::Hget);
        assert_eq!(classify_command_ast(b"HDEL", 2), CommandAstKind::Hdel);
        assert_eq!(classify_command_ast(b"HMGET", 2), CommandAstKind::Hmget);
        assert_eq!(classify_command_ast(b"HGETALL", 1), CommandAstKind::Hgetall);
        assert_eq!(classify_command_ast(b"HEXISTS", 2), CommandAstKind::Hexists);
        assert_eq!(classify_command_ast(b"HKEYS", 1), CommandAstKind::Hkeys);
        assert_eq!(classify_command_ast(b"HVALS", 1), CommandAstKind::Hvals);
        assert_eq!(classify_command_ast(b"HLEN", 1), CommandAstKind::Hlen);
        assert_eq!(classify_command_ast(b"HINCRBY", 3), CommandAstKind::Hincrby);
        assert_eq!(
            classify_command_ast(b"HINCRBYFLOAT", 3),
            CommandAstKind::Hincrbyfloat
        );
        assert_eq!(classify_command_ast(b"HSETNX", 3), CommandAstKind::Hsetnx);
        assert_eq!(classify_command_ast(b"HSTRLEN", 2), CommandAstKind::Hstrlen);
        assert_eq!(
            classify_command_ast(b"HRANDFIELD", 3),
            CommandAstKind::Hrandfield
        );
        assert_eq!(classify_command_ast(b"HSCAN", 2), CommandAstKind::Hscan);
        assert_eq!(classify_command_ast(b"HEXPIRE", 4), CommandAstKind::Hexpire);
        assert_eq!(classify_command_ast(b"HTTL", 3), CommandAstKind::Httl);
        assert_eq!(
            classify_command_ast(b"HPERSIST", 3),
            CommandAstKind::Hpersist
        );
        assert_eq!(
            classify_command_ast(b"HPEXPIRE", 4),
            CommandAstKind::Hpexpire
        );
        assert_eq!(classify_command_ast(b"HPTTL", 3), CommandAstKind::Hpttl);
        assert_eq!(
            classify_command_ast(b"HEXPIRETIME", 3),
            CommandAstKind::Hexpiretime
        );
        assert_eq!(classify_command_ast(b"HGETDEL", 3), CommandAstKind::Hgetdel);
        assert_eq!(classify_command_ast(b"HGETEX", 4), CommandAstKind::Hgetex);
        assert_eq!(classify_command_ast(b"HSETEX", 4), CommandAstKind::Hsetex);
        assert_eq!(classify_command_ast(b"HGET", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"HINCRBY", 2), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"HGETEX", 3), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_set_commands_by_arity() {
        assert_eq!(classify_command_ast(b"SADD", 2), CommandAstKind::Sadd);
        assert_eq!(classify_command_ast(b"SREM", 2), CommandAstKind::Srem);
        assert_eq!(
            classify_command_ast(b"SMEMBERS", 1),
            CommandAstKind::Smembers
        );
        assert_eq!(
            classify_command_ast(b"SISMEMBER", 2),
            CommandAstKind::Sismember
        );
        assert_eq!(
            classify_command_ast(b"SMISMEMBER", 2),
            CommandAstKind::Smismember
        );
        assert_eq!(classify_command_ast(b"SCARD", 1), CommandAstKind::Scard);
        assert_eq!(classify_command_ast(b"SINTER", 2), CommandAstKind::Sinter);
        assert_eq!(classify_command_ast(b"SUNION", 2), CommandAstKind::Sunion);
        assert_eq!(classify_command_ast(b"SDIFF", 2), CommandAstKind::Sdiff);
        assert_eq!(
            classify_command_ast(b"SDIFFSTORE", 2),
            CommandAstKind::Sdiffstore
        );
        assert_eq!(
            classify_command_ast(b"SINTERSTORE", 2),
            CommandAstKind::Sinterstore
        );
        assert_eq!(
            classify_command_ast(b"SUNIONSTORE", 2),
            CommandAstKind::Sunionstore
        );
        assert_eq!(
            classify_command_ast(b"SINTERCARD", 2),
            CommandAstKind::Sintercard
        );
        assert_eq!(
            classify_command_ast(b"SRANDMEMBER", 2),
            CommandAstKind::Srandmember
        );
        assert_eq!(classify_command_ast(b"SPOP", 2), CommandAstKind::Spop);
        assert_eq!(classify_command_ast(b"SMOVE", 3), CommandAstKind::Smove);
        assert_eq!(classify_command_ast(b"SSCAN", 2), CommandAstKind::Sscan);
        assert_eq!(classify_command_ast(b"SADD", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"SMOVE", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_bitmap_commands_by_arity() {
        assert_eq!(classify_command_ast(b"SETBIT", 3), CommandAstKind::Setbit);
        assert_eq!(classify_command_ast(b"GETBIT", 2), CommandAstKind::Getbit);
        assert_eq!(
            classify_command_ast(b"BITCOUNT", 1),
            CommandAstKind::Bitcount
        );
        assert_eq!(
            classify_command_ast(b"BITCOUNT", 4),
            CommandAstKind::Bitcount
        );
        assert_eq!(classify_command_ast(b"BITPOS", 2), CommandAstKind::Bitpos);
        assert_eq!(classify_command_ast(b"BITPOS", 5), CommandAstKind::Bitpos);
        assert_eq!(classify_command_ast(b"BITOP", 3), CommandAstKind::Bitop);
        assert_eq!(classify_command_ast(b"SETBIT", 2), CommandAstKind::Unknown);
        assert_eq!(
            classify_command_ast(b"BITCOUNT", 2),
            CommandAstKind::Unknown
        );
        assert_eq!(classify_command_ast(b"BITPOS", 6), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"BITOP", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_generic_commands_by_arity() {
        assert_eq!(classify_command_ast(b"TYPE", 1), CommandAstKind::Type);
        assert_eq!(classify_command_ast(b"UNLINK", 2), CommandAstKind::Unlink);
        assert_eq!(classify_command_ast(b"RENAME", 2), CommandAstKind::Rename);
        assert_eq!(
            classify_command_ast(b"RENAMENX", 2),
            CommandAstKind::Renamenx
        );
        assert_eq!(classify_command_ast(b"COPY", 3), CommandAstKind::Copy);
        assert_eq!(
            classify_command_ast(b"RANDOMKEY", 0),
            CommandAstKind::Randomkey
        );
        assert_eq!(classify_command_ast(b"SCAN", 5), CommandAstKind::Scan);
        assert_eq!(
            classify_command_ast(b"EXPIRETIME", 1),
            CommandAstKind::Expiretime
        );
        assert_eq!(
            classify_command_ast(b"PEXPIRETIME", 1),
            CommandAstKind::Pexpiretime
        );
        assert_eq!(classify_command_ast(b"OBJECT", 2), CommandAstKind::Object);
        assert_eq!(classify_command_ast(b"WAIT", 2), CommandAstKind::Wait);
        assert_eq!(classify_command_ast(b"TYPE", 0), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"RENAME", 3), CommandAstKind::Unknown);
        assert_eq!(
            classify_command_ast(b"RANDOMKEY", 1),
            CommandAstKind::Unknown
        );
        assert_eq!(classify_command_ast(b"WAIT", 1), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_zset_commands_by_arity() {
        assert_eq!(classify_command_ast(b"ZADD", 3), CommandAstKind::Zadd);
        assert_eq!(classify_command_ast(b"ZREM", 2), CommandAstKind::Zrem);
        assert_eq!(classify_command_ast(b"ZSCORE", 2), CommandAstKind::Zscore);
        assert_eq!(classify_command_ast(b"ZRANK", 2), CommandAstKind::Zrank);
        assert_eq!(
            classify_command_ast(b"ZREVRANK", 2),
            CommandAstKind::Zrevrank
        );
        assert_eq!(classify_command_ast(b"ZRANGE", 4), CommandAstKind::Zrange);
        assert_eq!(
            classify_command_ast(b"ZREVRANGE", 4),
            CommandAstKind::Zrevrange
        );
        assert_eq!(classify_command_ast(b"ZCARD", 1), CommandAstKind::Zcard);
        assert_eq!(classify_command_ast(b"ZINCRBY", 3), CommandAstKind::Zincrby);
        assert_eq!(classify_command_ast(b"ZCOUNT", 3), CommandAstKind::Zcount);
        assert_eq!(classify_command_ast(b"ZPOPMIN", 2), CommandAstKind::Zpopmin);
        assert_eq!(classify_command_ast(b"ZPOPMAX", 2), CommandAstKind::Zpopmax);
        assert_eq!(
            classify_command_ast(b"ZRANDMEMBER", 3),
            CommandAstKind::Zrandmember
        );
        assert_eq!(classify_command_ast(b"ZSCAN", 2), CommandAstKind::Zscan);
        assert_eq!(classify_command_ast(b"ZMSCORE", 2), CommandAstKind::Zmscore);
        assert_eq!(
            classify_command_ast(b"ZRANGEBYSCORE", 3),
            CommandAstKind::Zrangebyscore
        );
        assert_eq!(
            classify_command_ast(b"ZREVRANGEBYSCORE", 3),
            CommandAstKind::Zrevrangebyscore
        );
        assert_eq!(classify_command_ast(b"ZADD", 2), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"ZRANGE", 5), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_extracts_acl_keys_for_common_shapes() {
        assert_eq!(command_key_indices(b"GET", &[b"k"]), vec![0]);
        assert_eq!(command_key_indices(b"SET", &[b"k", b"v"]), vec![0]);
        assert_eq!(
            command_key_indices(b"DEL", &[b"a", b"b", b"c"]),
            vec![0, 1, 2]
        );
        assert_eq!(
            command_key_indices(b"MSET", &[b"a", b"1", b"b", b"2"]),
            vec![0, 2]
        );
        assert_eq!(command_key_indices(b"COPY", &[b"src", b"dst"]), vec![0, 1]);
        assert_eq!(
            command_key_indices(b"BITOP", &[b"AND", b"dst", b"a", b"b"]),
            vec![1, 2, 3]
        );
        assert_eq!(
            command_key_indices(b"JSON.MGET", &[b"a", b"b", b"$"]),
            vec![0, 1]
        );
    }

    #[test]
    fn ast_extracts_acl_keys_for_counted_and_stream_shapes() {
        assert_eq!(
            command_key_indices(
                b"XREAD",
                &[b"COUNT", b"2", b"STREAMS", b"s1", b"s2", b"0", b"0"]
            ),
            vec![3, 4]
        );
        assert_eq!(
            command_key_indices(
                b"XREADGROUP",
                &[b"GROUP", b"g", b"c", b"BLOCK", b"1", b"STREAMS", b"s1", b"s2", b">", b"0",]
            ),
            vec![6, 7]
        );
        assert_eq!(
            command_key_indices(
                b"BLMPOP",
                &[b"1.5", b"2", b"k1", b"k2", b"LEFT", b"COUNT", b"1"]
            ),
            vec![2, 3]
        );
        assert_eq!(
            command_key_indices(b"CMS.MERGE", &[b"dst", b"2", b"src1", b"src2", b"WEIGHTS"]),
            vec![0, 2, 3]
        );
        assert_eq!(
            command_key_indices(b"TDIGEST.MERGE", &[b"dst", b"2", b"src1", b"src2"]),
            vec![0, 2, 3]
        );
        assert_eq!(
            command_key_indices(b"CF.MEXISTS", &[b"cf", b"a", b"b"]),
            vec![0]
        );
        assert_eq!(
            command_key_indices(b"SINTERCARD", &[b"2", b"s1", b"s2", b"LIMIT", b"1"]),
            vec![1, 2]
        );
    }
}
