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
