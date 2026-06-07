
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

