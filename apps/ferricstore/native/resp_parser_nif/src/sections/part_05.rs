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

