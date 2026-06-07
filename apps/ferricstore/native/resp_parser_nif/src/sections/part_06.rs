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

