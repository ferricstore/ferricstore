type FlowOptionParser<'a> =
    fn(Env<'a>, &[Term<'a>], &[&[u8]], usize) -> Result<Option<Term<'a>>, Term<'a>>;

struct FlowAttributeOptions<'a> {
    attributes: Term<'a>,
    has_attributes: bool,
    attributes_merge: Term<'a>,
    has_attributes_merge: bool,
    attributes_delete: Vec<Term<'a>>,
}

impl<'a> FlowAttributeOptions<'a> {
    fn new(env: Env<'a>) -> Self {
        Self {
            attributes: Term::map_new(env),
            has_attributes: false,
            attributes_merge: Term::map_new(env),
            has_attributes_merge: false,
            attributes_delete: Vec::new(),
        }
    }
}

fn parse_flow_attribute_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
    attribute_opts: &mut FlowAttributeOptions<'a>,
) -> Result<Option<usize>, Term<'a>> {
    if ascii_eq_ignore_case(arg_bytes[idx], b"ATTRIBUTE") {
        if idx + 2 >= args.len() {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }
        attribute_opts.attributes = attribute_opts
            .attributes
            .map_put(args[idx + 1], args[idx + 2])
            .map_err(|_| generic_ast_error(env, b"ERR syntax error"))?;
        attribute_opts.has_attributes = true;
        return Ok(Some(idx + 3));
    }

    if ascii_eq_ignore_case(arg_bytes[idx], b"ATTRIBUTE_MERGE") {
        if idx + 2 >= args.len() {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }
        attribute_opts.attributes_merge = attribute_opts
            .attributes_merge
            .map_put(args[idx + 1], args[idx + 2])
            .map_err(|_| generic_ast_error(env, b"ERR syntax error"))?;
        attribute_opts.has_attributes_merge = true;
        return Ok(Some(idx + 3));
    }

    if ascii_eq_ignore_case(arg_bytes[idx], b"ATTRIBUTE_DELETE") {
        if idx + 1 >= args.len() {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }
        attribute_opts.attributes_delete.push(args[idx + 1]);
        return Ok(Some(idx + 2));
    }

    Ok(None)
}

fn append_flow_attribute_opts<'a>(
    env: Env<'a>,
    opts: &mut Vec<Term<'a>>,
    attribute_opts: FlowAttributeOptions<'a>,
) {
    if attribute_opts.has_attributes {
        opts.push((atom(env, "attributes"), attribute_opts.attributes).encode(env));
    }
    if attribute_opts.has_attributes_merge {
        opts.push((
            atom(env, "attributes_merge"),
            attribute_opts.attributes_merge,
        )
            .encode(env));
    }
    if !attribute_opts.attributes_delete.is_empty() {
        opts.push((
            atom(env, "attributes_delete"),
            attribute_opts.attributes_delete,
        )
            .encode(env));
    }
}

fn parse_flow_options<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    parser: FlowOptionParser<'a>,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::with_capacity((args.len() - start) / 2 + 3);
    let mut attribute_opts = FlowAttributeOptions::new(env);
    let mut idx = start;
    while idx < args.len() {
        if let Some(next_idx) =
            parse_flow_attribute_option(env, args, arg_bytes, idx, &mut attribute_opts)?
        {
            idx = next_idx;
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
    append_flow_attribute_opts(env, &mut opts, attribute_opts);
    Ok(opts)
}

fn parse_flow_read_options<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    parser: FlowOptionParser<'a>,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::with_capacity((args.len().saturating_sub(start)) / 2 + 5);
    let mut attribute_opts = FlowAttributeOptions::new(env);
    let mut idx = start;

    while idx < args.len() {
        if let Some(next_idx) =
            parse_flow_attribute_option(env, args, arg_bytes, idx, &mut attribute_opts)?
        {
            idx = next_idx;
            continue;
        }

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

        if ascii_eq_ignore_case(arg_bytes[idx], b"VALUE") {
            if idx + 1 >= args.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }

            opts.push((atom(env, "values"), vec![args[idx + 1]]).encode(env));
            idx += 2;
            continue;
        }

        if ascii_eq_ignore_case(arg_bytes[idx], b"PARTITIONS") {
            if idx + 1 >= args.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }

            let Some(count) = parse_int_bytes(arg_bytes[idx + 1]) else {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            };

            if count <= 0 {
                return Err(generic_ast_error(
                    env,
                    b"ERR flow partition_keys must be a non-empty list",
                ));
            }

            let count = count as usize;
            let first = idx + 2;
            let end = first + count;

            if end > args.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }

            opts.push((atom(env, "partition_keys"), args[first..end].to_vec()).encode(env));
            idx = end;
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

    append_flow_attribute_opts(env, &mut opts, attribute_opts);

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
    let mut opts = Vec::with_capacity((end - start) / 2 + 3);
    let mut attribute_opts = FlowAttributeOptions::new(env);
    let mut idx = start;
    while idx < end {
        if let Some(next_idx) =
            parse_flow_attribute_option(env, args, arg_bytes, idx, &mut attribute_opts)?
        {
            if next_idx > end {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }
            idx = next_idx;
            continue;
        }
        if idx + 1 >= end {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }
        if let Some(opt) = parser(env, args, arg_bytes, idx)? {
            opts.push(opt);
        }
        idx += 2;
    }
    append_flow_attribute_opts(env, &mut opts, attribute_opts);
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
    let mut opts = Vec::with_capacity((end - start) / 2 + 3);
    let mut retry_opts = Vec::new();
    let mut backoff_opts = Vec::new();
    let mut attribute_opts = FlowAttributeOptions::new(env);
    let mut idx = start;

    while idx < end {
        if let Some(next_idx) =
            parse_flow_attribute_option(env, args, arg_bytes, idx, &mut attribute_opts)?
        {
            if next_idx > end {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }
            idx = next_idx;
            continue;
        }
        if idx + 1 >= end {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }
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

    append_flow_attribute_opts(env, &mut opts, attribute_opts);

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

fn flow_has_option_until(arg_bytes: &[&[u8]], start: usize, end: usize, name: &[u8]) -> bool {
    let mut idx = start;
    while idx < end {
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
    NonNegativeNamed(&'a [u8]),
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
                b"PAYLOAD_REF",
                "payload_ref",
                FlowOptType::Ref(b"payload_ref"),
            ),
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
            (
                b"RETENTION_TTL",
                "retention_ttl_ms",
                FlowOptType::Positive(b"retention_ttl_ms"),
            ),
            (
                b"RETENTION_TTL_MS",
                "retention_ttl_ms",
                FlowOptType::Positive(b"retention_ttl_ms"),
            ),
            (
                b"HISTORY_HOT_MAX_EVENTS",
                "history_hot_max_events",
                FlowOptType::NonNegativeNamed(b"history_hot_max_events"),
            ),
            (
                b"HISTORY_MAX_EVENTS",
                "history_max_events",
                FlowOptType::Positive(b"history_max_events"),
            ),
            (b"IDEMPOTENT", "idempotent", FlowOptType::Boolean),
            (b"RETURN", "return", FlowOptType::Binary),
        ],
    )
}

fn flow_create_many_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<Option<Term<'a>>, Term<'a>> {
    if ascii_eq_ignore_case(arg_bytes[idx], b"INDEPENDENT") {
        flow_option(
            env,
            args,
            arg_bytes,
            idx,
            &[(b"INDEPENDENT", "independent", FlowOptType::Boolean)],
        )
    } else {
        flow_create_option(env, args, arg_bytes, idx)
    }
}

fn flow_value_put_option<'a>(
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
            (
                b"OWNER_FLOW_ID",
                "owner_flow_id",
                FlowOptType::Ref(b"owner_flow_id"),
            ),
            (b"TTL", "ttl_ms", FlowOptType::Positive(b"ttl_ms")),
            (b"TTL_MS", "ttl_ms", FlowOptType::Positive(b"ttl_ms")),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
        ],
    )
}

fn parse_flow_signal_options<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
) -> Result<Vec<Term<'a>>, Term<'a>> {
    let mut opts = Vec::with_capacity((args.len().saturating_sub(start)) / 2 + 2);
    let mut values = Vec::new();
    let mut value_refs = Vec::new();
    let mut drop_values = Vec::new();
    let mut override_values = Vec::new();
    let mut idx = start;

    while idx < args.len() {
        if ascii_eq_ignore_case(arg_bytes[idx], b"VALUE") {
            if idx + 2 >= args.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }
            values.push((args[idx + 1], args[idx + 2]).encode(env));
            idx += 3;
            continue;
        }

        if ascii_eq_ignore_case(arg_bytes[idx], b"VALUE_REF") {
            if idx + 2 >= args.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }
            value_refs.push((args[idx + 1], args[idx + 2]).encode(env));
            idx += 3;
            continue;
        }

        if ascii_eq_ignore_case(arg_bytes[idx], b"DROP_VALUE") {
            if idx + 1 >= args.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }
            drop_values.push(args[idx + 1]);
            idx += 2;
            continue;
        }

        if ascii_eq_ignore_case(arg_bytes[idx], b"OVERRIDE_VALUE") {
            if idx + 1 >= args.len() {
                return Err(generic_ast_error(env, b"ERR syntax error"));
            }
            override_values.push(args[idx + 1]);
            idx += 2;
            continue;
        }

        if idx + 1 >= args.len() {
            return Err(generic_ast_error(env, b"ERR syntax error"));
        }

        if let Some(opt) = flow_signal_option(env, args, arg_bytes, idx)? {
            opts.push(opt);
        }
        idx += 2;
    }

    if !values.is_empty() {
        opts.push((atom(env, "values"), values).encode(env));
    }
    if !value_refs.is_empty() {
        opts.push((atom(env, "value_refs"), value_refs).encode(env));
    }
    if !drop_values.is_empty() {
        opts.push((atom(env, "drop_values"), drop_values).encode(env));
    }
    if !override_values.is_empty() {
        opts.push((atom(env, "override_values"), override_values).encode(env));
    }

    Ok(opts)
}

fn flow_signal_option<'a>(
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
            (b"SIGNAL", "signal", FlowOptType::Binary),
            (b"IDEMPOTENCY", "idempotency_key", FlowOptType::Binary),
            (b"IDEMPOTENCY_KEY", "idempotency_key", FlowOptType::Binary),
            (b"IF_STATE", "if_state", FlowOptType::Binary),
            (b"TRANSITION_TO", "transition_to", FlowOptType::Binary),
            (b"RUN_AT", "run_at_ms", FlowOptType::NonNegative),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
        ],
    )
}

fn flow_spawn_children_option<'a>(
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
            (b"GROUP", "group_id", FlowOptType::Binary),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"FENCING", "fencing_token", FlowOptType::NonNegative),
            (b"WAIT", "wait", FlowOptType::Binary),
            (b"ON_CHILD_FAILED", "on_child_failed", FlowOptType::Binary),
            (b"ON_PARENT_CLOSED", "on_parent_closed", FlowOptType::Binary),
            (b"SUCCESS", "success", FlowOptType::Binary),
            (b"FAILURE", "failure", FlowOptType::Binary),
            (b"FROM_STATE", "from_state", FlowOptType::Binary),
            (b"WAIT_STATE", "wait_state", FlowOptType::Binary),
            (b"LEASE_TOKEN", "lease_token", FlowOptType::Binary),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (
                b"RETENTION_TTL",
                "retention_ttl_ms",
                FlowOptType::Positive(b"retention_ttl_ms"),
            ),
            (
                b"RETENTION_TTL_MS",
                "retention_ttl_ms",
                FlowOptType::Positive(b"retention_ttl_ms"),
            ),
            (
                b"HISTORY_HOT_MAX_EVENTS",
                "history_hot_max_events",
                FlowOptType::NonNegativeNamed(b"history_hot_max_events"),
            ),
            (
                b"HISTORY_MAX_EVENTS",
                "history_max_events",
                FlowOptType::Positive(b"history_max_events"),
            ),
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
    let mut retention_opts = Vec::new();
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

            let (state_retry, state_retention) =
                parse_flow_policy_options(env, args, arg_bytes, idx + 2, end)?;
            let mut state_policy = Vec::new();
            if !state_retry.is_empty() {
                state_policy.push((atom(env, "retry"), state_retry).encode(env));
            }
            if !state_retention.is_empty() {
                state_policy.push((atom(env, "retention"), state_retention).encode(env));
            }
            states.push((args[idx + 1], state_policy).encode(env));
            idx = end;
            continue;
        }

        match flow_policy_option(env, args, arg_bytes, idx)? {
            FlowPolicyOpt::Retry(term) => retry_opts.push(term),
            FlowPolicyOpt::Backoff(term) => backoff_opts.push(term),
            FlowPolicyOpt::Retention(term) => retention_opts.push(term),
        }
        idx += 2;
    }

    if !backoff_opts.is_empty() {
        retry_opts.push((atom(env, "backoff"), backoff_opts).encode(env));
    }

    if !retry_opts.is_empty() {
        opts.push((atom(env, "retry"), retry_opts).encode(env));
    }

    if !retention_opts.is_empty() {
        opts.push((atom(env, "retention"), retention_opts).encode(env));
    }

    if !states.is_empty() {
        opts.push((atom(env, "states"), states).encode(env));
    }

    Ok(opts)
}

enum FlowPolicyOpt<'a> {
    Retry(Term<'a>),
    Backoff(Term<'a>),
    Retention(Term<'a>),
}
