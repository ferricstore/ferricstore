fn parse_flow_policy_options<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    start: usize,
    end: usize,
) -> Result<(Vec<Term<'a>>, Vec<Term<'a>>), Term<'a>> {
    if (end - start) % 2 != 0 {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    let mut retry_opts = Vec::new();
    let mut backoff_opts = Vec::new();
    let mut retention_opts = Vec::new();
    let mut idx = start;

    while idx < end {
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

    Ok((retry_opts, retention_opts))
}

fn flow_policy_option<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
    idx: usize,
) -> Result<FlowPolicyOpt<'a>, Term<'a>> {
    if idx + 1 >= args.len() {
        return Err(generic_ast_error(env, b"ERR syntax error"));
    }

    if ascii_eq_ignore_case(arg_bytes[idx], b"RETENTION_TTL")
        || ascii_eq_ignore_case(arg_bytes[idx], b"RETENTION_TTL_MS")
    {
        match parse_int_bytes(arg_bytes[idx + 1]) {
            Some(value) if value > 0 => Ok(FlowPolicyOpt::Retention(
                (atom(env, "ttl_ms"), value).encode(env),
            )),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        }
    } else if ascii_eq_ignore_case(arg_bytes[idx], b"HISTORY_HOT_MAX_EVENTS") {
        Err(generic_ast_error(
            env,
            b"ERR flow retention history_hot_max_events is internal",
        ))
    } else if ascii_eq_ignore_case(arg_bytes[idx], b"HISTORY_MAX_EVENTS") {
        match parse_int_bytes(arg_bytes[idx + 1]) {
            Some(value) if value > 0 => Ok(FlowPolicyOpt::Retention(
                (atom(env, "history_max_events"), value).encode(env),
            )),
            _ => Err(generic_ast_error(
                env,
                b"ERR value is not an integer or out of range",
            )),
        }
    } else {
        let (term, is_backoff) = flow_policy_retry_option(env, args, arg_bytes, idx)?;
        if is_backoff {
            Ok(FlowPolicyOpt::Backoff(term))
        } else {
            Ok(FlowPolicyOpt::Retry(term))
        }
    }
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
            (b"BLOCK", "block_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"RETURN", "return", FlowOptType::Binary),
            (b"RECLAIM_EXPIRED", "reclaim_expired", FlowOptType::Boolean),
            (b"RECLAIM_RATIO", "reclaim_ratio", FlowOptType::NonNegative),
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
            (b"TTL", "ttl_ms", FlowOptType::Positive(b"ttl_ms")),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
        ],
    )
}

fn flow_extend_lease_option<'a>(
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
            (b"LEASE_MS", "lease_ms", FlowOptType::Positive(b"lease_ms")),
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
            (b"TTL", "ttl_ms", FlowOptType::Positive(b"ttl_ms")),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"INDEPENDENT", "independent", FlowOptType::Boolean),
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
            (
                b"PAYLOAD_REF",
                "payload_ref",
                FlowOptType::Ref(b"payload_ref"),
            ),
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
            (b"INDEPENDENT", "independent", FlowOptType::Boolean),
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
            (b"TTL", "ttl_ms", FlowOptType::Positive(b"ttl_ms")),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"INDEPENDENT", "independent", FlowOptType::Boolean),
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
            (b"REASON", "reason", FlowOptType::Binary),
            (b"REASON_REF", "reason_ref", FlowOptType::Ref(b"reason_ref")),
            (b"TTL", "ttl_ms", FlowOptType::Positive(b"ttl_ms")),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"PARTITION", "partition_key", FlowOptType::Partition),
            (b"INDEPENDENT", "independent", FlowOptType::Boolean),
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
            (
                b"PAYLOAD_REF",
                "payload_ref",
                FlowOptType::Ref(b"payload_ref"),
            ),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
            (b"INDEPENDENT", "independent", FlowOptType::Boolean),
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
            (b"TTL", "ttl_ms", FlowOptType::Positive(b"ttl_ms")),
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
            (b"REASON", "reason", FlowOptType::Binary),
            (b"REASON_REF", "reason_ref", FlowOptType::Ref(b"reason_ref")),
            (b"TTL", "ttl_ms", FlowOptType::Positive(b"ttl_ms")),
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
            (b"FROM_MS", "from_ms", FlowOptType::NonNegative),
            (b"TO_MS", "to_ms", FlowOptType::NonNegative),
            (b"REV", "rev", FlowOptType::Boolean),
            (b"STATE", "state", FlowOptType::Binary),
            (b"TERMINAL_ONLY", "terminal_only", FlowOptType::Boolean),
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
            (b"FROM_EVENT", "from_event", FlowOptType::Binary),
            (b"TO_EVENT", "to_event", FlowOptType::Binary),
            (b"FROM_MS", "from_ms", FlowOptType::NonNegative),
            (b"TO_MS", "to_ms", FlowOptType::NonNegative),
            (b"FROM_VERSION", "from_version", FlowOptType::NonNegative),
            (b"TO_VERSION", "to_version", FlowOptType::NonNegative),
            (b"REV", "rev", FlowOptType::Boolean),
            (b"EVENT", "event", FlowOptType::Binary),
            (b"WORKER", "worker", FlowOptType::Binary),
            (b"INCLUDE_COLD", "include_cold", FlowOptType::Boolean),
            (
                b"CONSISTENT_PROJECTION",
                "consistent_projection",
                FlowOptType::Boolean,
            ),
            (b"VALUES", "values", FlowOptType::Boolean),
            (
                b"PAYLOAD_MAX_BYTES",
                "payload_max_bytes",
                FlowOptType::NonNegative,
            ),
            (b"MAXBYTES", "payload_max_bytes", FlowOptType::NonNegative),
        ],
    )
}

fn flow_failures_option<'a>(
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
            (b"FROM_MS", "from_ms", FlowOptType::NonNegative),
            (b"TO_MS", "to_ms", FlowOptType::NonNegative),
            (b"REV", "rev", FlowOptType::Boolean),
            (b"INCLUDE_COLD", "include_cold", FlowOptType::Boolean),
            (
                b"CONSISTENT_PROJECTION",
                "consistent_projection",
                FlowOptType::Boolean,
            ),
        ],
    )
}

fn flow_terminals_option<'a>(
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
            (b"FROM_MS", "from_ms", FlowOptType::NonNegative),
            (b"TO_MS", "to_ms", FlowOptType::NonNegative),
            (b"REV", "rev", FlowOptType::Boolean),
            (b"INCLUDE_COLD", "include_cold", FlowOptType::Boolean),
            (
                b"CONSISTENT_PROJECTION",
                "consistent_projection",
                FlowOptType::Boolean,
            ),
        ],
    )
}

fn flow_retention_cleanup_option<'a>(
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
            (b"LIMIT", "limit", FlowOptType::Positive(b"limit")),
            (b"NOW", "now_ms", FlowOptType::NonNegative),
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
            None => {
                let mut msg = b"ERR flow ".to_vec();
                msg.extend_from_slice(key.as_bytes());
                msg.extend_from_slice(b" must be a boolean");
                Err(generic_ast_error(env, &msg))
            }
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
        FlowOptType::NonNegativeNamed(label) => match parse_int_bytes(value_bytes) {
            Some(value) if value >= 0 => Ok(Some((key_atom, value).encode(env))),
            _ => {
                let mut msg = b"ERR flow ".to_vec();
                msg.extend_from_slice(label);
                msg.extend_from_slice(b" must be a non-negative integer");
                Err(generic_ast_error(env, &msg))
            }
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

