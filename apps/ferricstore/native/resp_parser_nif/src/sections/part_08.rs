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

fn make_flow_value_put_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_value_put");
    if args.is_empty() || args.len() % 2 == 0 {
        return (tag, wrong_number_error(env, b"flow.value.put")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_value_put_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_signal_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_signal");
    if args.len() < 3 {
        return (tag, wrong_number_error(env, b"flow.signal")).encode(env);
    }

    match parse_flow_signal_options(env, args, arg_bytes, 1) {
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
    if args.len() < 4 {
        return (tag, wrong_number_error(env, b"flow.create_many")).encode(env);
    }

    let mixed = ascii_eq_ignore_case(arg_bytes[0], b"MIXED");
    let auto = ascii_eq_ignore_case(arg_bytes[0], b"AUTO");

    let Some(items_idx) = flow_find_option(arg_bytes, 1, b"ITEMS") else {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR flow items are required"),
        )
            .encode(env);
    };

    let shared_payload_ref = flow_has_option_until(arg_bytes, 1, items_idx, b"PAYLOAD_REF");
    let item_width = match (mixed, shared_payload_ref) {
        (true, true) => 2,
        (true, false) => 3,
        (false, true) => 1,
        (false, false) => 2,
    };
    if items_idx == args.len() - 1 || (args.len() - items_idx - 1) % item_width != 0 {
        return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
    }

    let opts =
        match parse_flow_options_until(env, args, arg_bytes, 1, items_idx, flow_create_many_option)
        {
            Ok(opts) => opts,
            Err(err) => return (tag, args[0], err).encode(env),
        };

    let mut items = Vec::with_capacity((args.len() - items_idx - 1) / item_width);
    let mut idx = items_idx + 1;
    while idx < args.len() {
        if mixed {
            let mut item_opts = vec![(atom(env, "partition_key"), args[idx + 1]).encode(env)];
            if !shared_payload_ref {
                item_opts.push((atom(env, "payload"), args[idx + 2]).encode(env));
            }
            items.push((args[idx], item_opts).encode(env));
        } else if shared_payload_ref {
            items.push(args[idx]);
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

    let partition = if mixed || auto {
        atoms::nil().encode(env)
    } else {
        args[0]
    };

    (tag, partition, items, opts).encode(env)
}

fn make_flow_spawn_children_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_spawn_children");
    if args.len() < 5 {
        return (tag, wrong_number_error(env, b"flow.spawn_children")).encode(env);
    }

    let Some(items_idx) = flow_find_option(arg_bytes, 1, b"ITEMS") else {
        return (
            tag,
            args[0],
            generic_ast_error(env, b"ERR flow children are required"),
        )
            .encode(env);
    };

    let mixed =
        items_idx + 1 < args.len() && ascii_eq_ignore_case(arg_bytes[items_idx + 1], b"MIXED");
    let item_start = if mixed { items_idx + 2 } else { items_idx + 1 };
    let item_width = if mixed { 4 } else { 3 };

    if item_start >= args.len() || (args.len() - item_start) % item_width != 0 {
        return (tag, args[0], generic_ast_error(env, b"ERR syntax error")).encode(env);
    }

    let opts = match parse_flow_options_until(
        env,
        args,
        arg_bytes,
        1,
        items_idx,
        flow_spawn_children_option,
    ) {
        Ok(opts) => opts,
        Err(err) => return (tag, args[0], err).encode(env),
    };

    let mut items = Vec::with_capacity((args.len() - item_start) / item_width);
    let mut idx = item_start;
    while idx < args.len() {
        let item_opts = if mixed {
            vec![
                (atom(env, "partition_key"), args[idx + 1]).encode(env),
                (atom(env, "type"), args[idx + 2]).encode(env),
                (atom(env, "payload"), args[idx + 3]).encode(env),
            ]
        } else {
            vec![
                (atom(env, "type"), args[idx + 1]).encode(env),
                (atom(env, "payload"), args[idx + 2]).encode(env),
            ]
        };
        items.push((args[idx], item_opts).encode(env));
        idx += item_width;
    }

    (tag, args[0], items, opts).encode(env)
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

fn make_flow_extend_lease_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_extend_lease");
    if args.len() < 4 {
        return (tag, wrong_number_error(env, b"flow.extend_lease")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 2, flow_extend_lease_option) {
        Ok(opts) => (tag, args[0], args[1], opts).encode(env),
        Err(err) => (tag, args[0], args[1], err).encode(env),
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

fn make_flow_failures_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_failures");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.failures")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_failures_option) {
        Ok(opts) => (tag, args[0], opts).encode(env),
        Err(err) => (tag, args[0], err).encode(env),
    }
}

fn make_flow_terminals_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_terminals");
    if args.is_empty() {
        return (tag, wrong_number_error(env, b"flow.terminals")).encode(env);
    }

    match parse_flow_options(env, args, arg_bytes, 1, flow_terminals_option) {
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

fn make_flow_retention_cleanup_command_ast<'a>(
    env: Env<'a>,
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let tag = atom(env, "flow_retention_cleanup");

    match parse_flow_options(env, args, arg_bytes, 0, flow_retention_cleanup_option) {
        Ok(opts) => (tag, opts).encode(env),
        Err(err) => (tag, err).encode(env),
    }
}

