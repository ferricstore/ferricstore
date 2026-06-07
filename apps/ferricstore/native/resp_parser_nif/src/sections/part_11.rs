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

        b"RENAME" | b"RENAMENX" | b"COPY" | b"LMOVE" | b"RPOPLPUSH" | b"SMOVE"
        | b"GEOSEARCHSTORE" => first_n_indices(argc, 2),

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
        b"PUBLISH" => first_n_indices(argc, 1),
        b"SUBSCRIBE" | b"UNSUBSCRIBE" | b"PSUBSCRIBE" | b"PUNSUBSCRIBE" => all_indices(argc),
        b"PUBSUB" => pubsub_key_indices(arg_bytes),
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
        b"FLOW.VALUE.PUT" => flow_partition_key_indices(arg_bytes, 1),
        b"FLOW.SIGNAL" => flow_partition_or_first_key_indices(arg_bytes, 1),
        b"FLOW.SPAWN_CHILDREN" => flow_spawn_children_key_indices(arg_bytes),
        b"FLOW.COMPLETE_MANY" => flow_complete_many_key_indices(arg_bytes),
        b"FLOW.RETRY_MANY" => flow_retry_many_key_indices(arg_bytes),
        b"FLOW.FAIL_MANY" => flow_fail_many_key_indices(arg_bytes),
        b"FLOW.CANCEL_MANY" => flow_cancel_many_key_indices(arg_bytes),
        b"FLOW.TRANSITION_MANY" => flow_transition_many_key_indices(arg_bytes),
        b"FLOW.CREATE" | b"FLOW.GET" | b"FLOW.HISTORY" => {
            flow_partition_or_first_key_indices(arg_bytes, 1)
        }
        b"FLOW.COMPLETE" | b"FLOW.RETRY" | b"FLOW.FAIL" | b"FLOW.EXTEND_LEASE" => {
            flow_partition_or_first_key_indices(arg_bytes, 2)
        }
        b"FLOW.TRANSITION" => flow_partition_or_first_key_indices(arg_bytes, 3),
        b"FLOW.CANCEL" | b"FLOW.REWIND" => flow_partition_or_first_key_indices(arg_bytes, 1),
        b"FLOW.BY_PARENT" | b"FLOW.BY_ROOT" | b"FLOW.BY_CORRELATION" => {
            flow_partition_or_first_key_indices(arg_bytes, 1)
        }
        b"FLOW.CLAIM_DUE" | b"FLOW.RECLAIM" | b"FLOW.LIST" | b"FLOW.TERMINALS"
        | b"FLOW.FAILURES" | b"FLOW.INFO" | b"FLOW.STUCK" => {
            flow_partition_or_first_key_indices(arg_bytes, 1)
        }
        b"FLOW.POLICY.SET" | b"FLOW.POLICY.GET" => first_n_indices(arg_bytes.len(), 1),
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

fn flow_partition_or_first_key_indices(arg_bytes: &[&[u8]], option_start: usize) -> Vec<usize> {
    if arg_bytes.is_empty() {
        return Vec::new();
    }

    let partition_keys = flow_partition_key_indices(arg_bytes, option_start);
    if partition_keys.is_empty() {
        vec![0]
    } else {
        dedup_indices(partition_keys)
    }
}

fn flow_partition_key_indices(arg_bytes: &[&[u8]], option_start: usize) -> Vec<usize> {
    flow_partition_key_indices_until(arg_bytes, option_start, arg_bytes.len())
}

fn flow_partition_key_indices_until(
    arg_bytes: &[&[u8]],
    option_start: usize,
    option_end: usize,
) -> Vec<usize> {
    let mut keys = Vec::new();
    let mut idx = option_start;
    let end = option_end.min(arg_bytes.len());

    while idx + 1 < end {
        if ascii_eq_ignore_case(arg_bytes[idx], b"PARTITION") {
            keys.push(idx + 1);
        } else if ascii_eq_ignore_case(arg_bytes[idx], b"PARTITIONS") {
            if let Some(count) = parse_int_bytes(arg_bytes[idx + 1]) {
                if count > 0 {
                    let first = idx + 2;
                    let last = first + count as usize;
                    if last <= end {
                        for key_idx in first..last {
                            keys.push(key_idx);
                        }
                    }
                }
            }
        }
        idx += 1;
    }

    keys
}

