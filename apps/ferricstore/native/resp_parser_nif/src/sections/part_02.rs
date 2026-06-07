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
        return ParseResult::Error(make_binary_term(
            env,
            b"ERR protocol error: empty command array",
        ));
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

