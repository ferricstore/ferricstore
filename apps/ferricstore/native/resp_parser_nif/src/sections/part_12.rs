fn dedup_indices(indices: Vec<usize>) -> Vec<usize> {
    let mut deduped = Vec::with_capacity(indices.len());
    for idx in indices {
        if !deduped.contains(&idx) {
            deduped.push(idx);
        }
    }
    deduped
}

fn flow_create_many_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    if arg_bytes.is_empty() || !ascii_eq_ignore_case(arg_bytes[0], b"MIXED") {
        return vec![0];
    }

    let Some(items_idx) = option_index(arg_bytes, 1, b"ITEMS") else {
        return vec![0];
    };

    let shared_payload_ref = flow_has_option_until(arg_bytes, 1, items_idx, b"PAYLOAD_REF");
    let item_width = if shared_payload_ref { 2 } else { 3 };
    let mut keys = Vec::new();
    let mut idx = items_idx + 1;
    while idx + item_width - 1 < arg_bytes.len() {
        keys.push(idx + 1);
        idx += item_width;
    }
    keys
}

fn flow_spawn_children_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    let option_end = option_index(arg_bytes, 1, b"ITEMS").unwrap_or(arg_bytes.len());
    let mut partition_keys = flow_partition_key_indices_until(arg_bytes, 1, option_end);

    if option_end + 1 < arg_bytes.len() && ascii_eq_ignore_case(arg_bytes[option_end + 1], b"MIXED")
    {
        let mut idx = option_end + 2;
        while idx + 3 < arg_bytes.len() {
            partition_keys.push(idx + 1);
            idx += 4;
        }
    }

    if partition_keys.is_empty() {
        first_n_indices(arg_bytes.len(), 1)
    } else {
        dedup_indices(partition_keys)
    }
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

fn pubsub_key_indices(arg_bytes: &[&[u8]]) -> Vec<usize> {
    if arg_bytes.is_empty() {
        return Vec::new();
    }

    if ascii_eq_ignore_case(arg_bytes[0], b"NUMSUB") {
        range_indices(1, arg_bytes.len())
    } else if ascii_eq_ignore_case(arg_bytes[0], b"CHANNELS") && arg_bytes.len() > 1 {
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
        b"CLUSTER.LEAVE" => Some("cluster_leave"),
        b"CLUSTER.FAILOVER" => Some("cluster_failover"),
        b"CLUSTER.PROMOTE" => Some("cluster_promote"),
        b"CLUSTER.DEMOTE" => Some("cluster_demote"),
        b"CLUSTER.ROLE" => Some("cluster_role"),
        b"FERRICSTORE.HOTNESS" => Some("ferricstore_hotness"),
        b"FERRICSTORE.CONFIG" => Some("ferricstore_config"),
        b"FERRICSTORE.METRICS" => Some("ferricstore_metrics"),
        b"FERRICSTORE.BLOBGC" => Some("ferricstore_blobgc"),
        b"FERRICSTORE.DOCTOR" => Some("ferricstore_doctor"),
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

