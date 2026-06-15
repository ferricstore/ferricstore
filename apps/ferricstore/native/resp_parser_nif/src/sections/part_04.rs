fn make_command_ast<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    match classify_command_ast(cmd, args.len()) {
        CommandAstKind::Get => (atoms::get(), args[0]).encode(env),
        CommandAstKind::Set => (atoms::set(), args[0], args[1]).encode(env),
        CommandAstKind::Ping0 => atoms::ping().encode(env),
        CommandAstKind::Ping1 => (atoms::ping(), args[0]).encode(env),
        CommandAstKind::Del => (atoms::del(), args).encode(env),
        CommandAstKind::Exists => (atoms::exists(), args).encode(env),
        CommandAstKind::Mget => (atoms::mget(), args).encode(env),
        CommandAstKind::Mset => (atoms::mset(), args).encode(env),
        CommandAstKind::Incr => (atoms::incr(), args[0]).encode(env),
        CommandAstKind::Decr => (atoms::decr(), args[0]).encode(env),
        CommandAstKind::Incrby => {
            make_one_int_command_ast(env, atoms::incrby(), args, arg_bytes, 1)
        }
        CommandAstKind::Decrby => {
            make_one_int_command_ast(env, atoms::decrby(), args, arg_bytes, 1)
        }
        CommandAstKind::Incrbyfloat => make_float_command_ast(env, args, arg_bytes),
        CommandAstKind::Append => (atoms::append(), args[0], args[1]).encode(env),
        CommandAstKind::Strlen => (atoms::strlen(), args[0]).encode(env),
        CommandAstKind::Getset => (atoms::getset(), args[0], args[1]).encode(env),
        CommandAstKind::Getdel => (atoms::getdel(), args[0]).encode(env),
        CommandAstKind::Getex => make_getex_command_ast(env, args, arg_bytes),
        CommandAstKind::Setnx => (atoms::setnx(), args[0], args[1]).encode(env),
        CommandAstKind::Setex => make_ttl_value_command_ast(env, atoms::setex(), args, arg_bytes),
        CommandAstKind::Psetex => make_ttl_value_command_ast(env, atoms::psetex(), args, arg_bytes),
        CommandAstKind::Getrange => make_getrange_command_ast(env, args, arg_bytes),
        CommandAstKind::Setrange => {
            make_one_int_command_ast(env, atoms::setrange(), args, arg_bytes, 1)
        }
        CommandAstKind::Msetnx => (atoms::msetnx(), args).encode(env),
        CommandAstKind::Expire => make_expiry_command_ast(env, atoms::expire(), args, arg_bytes),
        CommandAstKind::Pexpire => make_expiry_command_ast(env, atoms::pexpire(), args, arg_bytes),
        CommandAstKind::Expireat => {
            make_expiry_command_ast(env, atoms::expireat(), args, arg_bytes)
        }
        CommandAstKind::Pexpireat => {
            make_expiry_command_ast(env, atoms::pexpireat(), args, arg_bytes)
        }
        CommandAstKind::Ttl => (atoms::ttl(), args[0]).encode(env),
        CommandAstKind::Pttl => (atoms::pttl(), args[0]).encode(env),
        CommandAstKind::Persist => (atoms::persist(), args[0]).encode(env),
        CommandAstKind::Lpush => (atoms::lpush(), args).encode(env),
        CommandAstKind::Rpush => (atoms::rpush(), args).encode(env),
        CommandAstKind::Lpop => {
            make_optional_count_command_ast(env, atoms::lpop(), args, arg_bytes)
        }
        CommandAstKind::Rpop => {
            make_optional_count_command_ast(env, atoms::rpop(), args, arg_bytes)
        }
        CommandAstKind::Lrange => make_two_int_command_ast(env, atoms::lrange(), args, arg_bytes),
        CommandAstKind::Llen => (atoms::llen(), args[0]).encode(env),
        CommandAstKind::Lindex => {
            make_one_int_command_ast(env, atoms::lindex(), args, arg_bytes, 1)
        }
        CommandAstKind::Lset => make_one_int_command_ast(env, atoms::lset(), args, arg_bytes, 1),
        CommandAstKind::Lrem => make_one_int_command_ast(env, atoms::lrem(), args, arg_bytes, 1),
        CommandAstKind::Ltrim => make_two_int_command_ast(env, atoms::ltrim(), args, arg_bytes),
        CommandAstKind::Linsert => make_linsert_command_ast(env, args, arg_bytes),
        CommandAstKind::Lmove => make_lmove_command_ast(env, args, arg_bytes),
        CommandAstKind::Lpushx => (atoms::lpushx(), args).encode(env),
        CommandAstKind::Rpushx => (atoms::rpushx(), args).encode(env),
        CommandAstKind::Rpoplpush => (atoms::rpoplpush(), args[0], args[1]).encode(env),
        CommandAstKind::Hset => (atoms::hset(), args).encode(env),
        CommandAstKind::Hget => (atoms::hget(), args[0], args[1]).encode(env),
        CommandAstKind::Hdel => (atoms::hdel(), args).encode(env),
        CommandAstKind::Hmget => (atoms::hmget(), args).encode(env),
        CommandAstKind::Hgetall => (atoms::hgetall(), args[0]).encode(env),
        CommandAstKind::Hexists => (atoms::hexists(), args[0], args[1]).encode(env),
        CommandAstKind::Hkeys => (atoms::hkeys(), args[0]).encode(env),
        CommandAstKind::Hvals => (atoms::hvals(), args[0]).encode(env),
        CommandAstKind::Hlen => (atoms::hlen(), args[0]).encode(env),
        CommandAstKind::Hincrby => {
            make_hash_one_int_command_ast(env, atoms::hincrby(), args, arg_bytes)
        }
        CommandAstKind::Hincrbyfloat => make_hash_float_command_ast(env, args, arg_bytes),
        CommandAstKind::Hsetnx => (atoms::hsetnx(), args[0], args[1], args[2]).encode(env),
        CommandAstKind::Hstrlen => (atoms::hstrlen(), args[0], args[1]).encode(env),
        CommandAstKind::Hrandfield => make_hrandfield_command_ast(env, args, arg_bytes),
        CommandAstKind::Hscan => make_scan_command_ast(env, atoms::hscan(), args, arg_bytes),
        CommandAstKind::Hexpire => {
            make_hash_ttl_fields_ast(env, atoms::hexpire(), args, arg_bytes, b"seconds")
        }
        CommandAstKind::Httl => make_hash_fields_ast(env, atoms::httl(), args, arg_bytes),
        CommandAstKind::Hpersist => make_hash_fields_ast(env, atoms::hpersist(), args, arg_bytes),
        CommandAstKind::Hpexpire => {
            make_hash_ttl_fields_ast(env, atoms::hpexpire(), args, arg_bytes, b"milliseconds")
        }
        CommandAstKind::Hpttl => make_hash_fields_ast(env, atoms::hpttl(), args, arg_bytes),
        CommandAstKind::Hexpiretime => {
            make_hash_fields_ast(env, atoms::hexpiretime(), args, arg_bytes)
        }
        CommandAstKind::Hgetdel => make_hash_fields_ast(env, atoms::hgetdel(), args, arg_bytes),
        CommandAstKind::Hgetex => make_hgetex_command_ast(env, args, arg_bytes),
        CommandAstKind::Hsetex => make_hsetex_command_ast(env, args, arg_bytes),
        CommandAstKind::Sadd => (atoms::sadd(), args).encode(env),
        CommandAstKind::Srem => (atoms::srem(), args).encode(env),
        CommandAstKind::Smembers => (atoms::smembers(), args[0]).encode(env),
        CommandAstKind::Sismember => (atoms::sismember(), args[0], args[1]).encode(env),
        CommandAstKind::Smismember => (atoms::smismember(), args).encode(env),
        CommandAstKind::Scard => (atoms::scard(), args[0]).encode(env),
        CommandAstKind::Sinter => (atoms::sinter(), args).encode(env),
        CommandAstKind::Sunion => (atoms::sunion(), args).encode(env),
        CommandAstKind::Sdiff => (atoms::sdiff(), args).encode(env),
        CommandAstKind::Sdiffstore => (atoms::sdiffstore(), args).encode(env),
        CommandAstKind::Sinterstore => (atoms::sinterstore(), args).encode(env),
        CommandAstKind::Sunionstore => (atoms::sunionstore(), args).encode(env),
        CommandAstKind::Sintercard => make_sintercard_command_ast(env, args, arg_bytes),
        CommandAstKind::Srandmember => {
            make_set_optional_count_command_ast(env, atoms::srandmember(), args, arg_bytes, true)
        }
        CommandAstKind::Spop => {
            make_set_optional_count_command_ast(env, atoms::spop(), args, arg_bytes, false)
        }
        CommandAstKind::Smove => (atoms::smove(), args[0], args[1], args[2]).encode(env),
        CommandAstKind::Sscan => make_sscan_command_ast(env, args, arg_bytes),
        CommandAstKind::Zadd => make_zadd_command_ast(env, args, arg_bytes),
        CommandAstKind::Zrem => (atoms::zrem(), args).encode(env),
        CommandAstKind::Zscore => (atoms::zscore(), args[0], args[1]).encode(env),
        CommandAstKind::Zrank => (atoms::zrank(), args[0], args[1]).encode(env),
        CommandAstKind::Zrevrank => (atoms::zrevrank(), args[0], args[1]).encode(env),
        CommandAstKind::Zrange => make_zrange_command_ast(env, atoms::zrange(), args, arg_bytes),
        CommandAstKind::Zrevrange => {
            make_zrange_command_ast(env, atoms::zrevrange(), args, arg_bytes)
        }
        CommandAstKind::Zcard => (atoms::zcard(), args[0]).encode(env),
        CommandAstKind::Zincrby => make_zincrby_command_ast(env, args, arg_bytes),
        CommandAstKind::Zcount => make_zcount_command_ast(env, args, arg_bytes),
        CommandAstKind::Zpopmin => {
            make_set_optional_count_command_ast(env, atoms::zpopmin(), args, arg_bytes, false)
        }
        CommandAstKind::Zpopmax => {
            make_set_optional_count_command_ast(env, atoms::zpopmax(), args, arg_bytes, false)
        }
        CommandAstKind::Zrandmember => make_zrandmember_command_ast(env, args, arg_bytes),
        CommandAstKind::Zscan => make_zscan_command_ast(env, args, arg_bytes),
        CommandAstKind::Zmscore => (atoms::zmscore(), args).encode(env),
        CommandAstKind::Zrangebyscore => {
            make_zrangebyscore_command_ast(env, atoms::zrangebyscore(), args, arg_bytes)
        }
        CommandAstKind::Zrevrangebyscore => {
            make_zrangebyscore_command_ast(env, atoms::zrevrangebyscore(), args, arg_bytes)
        }
        CommandAstKind::Setbit => make_setbit_command_ast(env, args, arg_bytes),
        CommandAstKind::Getbit => make_getbit_command_ast(env, args, arg_bytes),
        CommandAstKind::Bitcount => make_bitcount_command_ast(env, args, arg_bytes),
        CommandAstKind::Bitpos => make_bitpos_command_ast(env, args, arg_bytes),
        CommandAstKind::Bitop => make_bitop_command_ast(env, args, arg_bytes),
        CommandAstKind::Type => (atom(env, "type"), args[0]).encode(env),
        CommandAstKind::Unlink => (atom(env, "unlink"), args).encode(env),
        CommandAstKind::Rename => (atom(env, "rename"), args[0], args[1]).encode(env),
        CommandAstKind::Renamenx => (atom(env, "renamenx"), args[0], args[1]).encode(env),
        CommandAstKind::Copy => make_copy_command_ast(env, args, arg_bytes),
        CommandAstKind::Randomkey => (atom(env, "randomkey"), Vec::<Term<'a>>::new()).encode(env),
        CommandAstKind::Scan => make_generic_scan_command_ast(env, args, arg_bytes),
        CommandAstKind::Expiretime => (atom(env, "expiretime"), args[0]).encode(env),
        CommandAstKind::Pexpiretime => (atom(env, "pexpiretime"), args[0]).encode(env),
        CommandAstKind::Object => make_object_command_ast(env, args, arg_bytes),
        CommandAstKind::Wait => (atom(env, "wait"), args[0], args[1]).encode(env),
        CommandAstKind::Xadd => make_xadd_command_ast(env, args, arg_bytes),
        CommandAstKind::Xlen => (atom(env, "xlen"), args[0]).encode(env),
        CommandAstKind::Xrange => {
            make_xrange_command_ast(env, atom(env, "xrange"), args, arg_bytes)
        }
        CommandAstKind::Xrevrange => make_xrevrange_command_ast(env, args, arg_bytes),
        CommandAstKind::Xread => make_xread_command_ast(env, args, arg_bytes),
        CommandAstKind::Xtrim => make_xtrim_command_ast(env, args, arg_bytes),
        CommandAstKind::Xdel => (atom(env, "xdel"), args[0], args[1..].to_vec()).encode(env),
        CommandAstKind::Xinfo => make_xinfo_command_ast(env, args, arg_bytes),
        CommandAstKind::Xgroup => make_xgroup_command_ast(env, args, arg_bytes),
        CommandAstKind::Xreadgroup => make_xreadgroup_command_ast(env, args, arg_bytes),
        CommandAstKind::Xack => {
            (atom(env, "xack"), args[0], args[1], args[2..].to_vec()).encode(env)
        }
        CommandAstKind::Geoadd => make_geoadd_command_ast(env, args, arg_bytes),
        CommandAstKind::Geopos => make_geo_members_command_ast(env, atom(env, "geopos"), args),
        CommandAstKind::Geodist => make_geodist_command_ast(env, args, arg_bytes),
        CommandAstKind::Geohash => make_geo_members_command_ast(env, atom(env, "geohash"), args),
        CommandAstKind::Geosearch => make_geosearch_command_ast(env, args, arg_bytes),
        CommandAstKind::Geosearchstore => make_geosearchstore_command_ast(env, args, arg_bytes),
        CommandAstKind::Pfadd => make_hll_command_ast(env, atom(env, "pfadd"), args, b"pfadd", 1),
        CommandAstKind::Pfcount => {
            make_hll_command_ast(env, atom(env, "pfcount"), args, b"pfcount", 1)
        }
        CommandAstKind::Pfmerge => {
            make_hll_command_ast(env, atom(env, "pfmerge"), args, b"pfmerge", 2)
        }
        CommandAstKind::BfReserve => make_bf_reserve_command_ast(env, args, arg_bytes),
        CommandAstKind::BfAdd => make_min_arity_list_ast(env, "bf_add", args, b"bf.add", 2),
        CommandAstKind::BfMadd => make_min_arity_list_ast(env, "bf_madd", args, b"bf.madd", 2),
        CommandAstKind::BfExists => {
            make_min_arity_list_ast(env, "bf_exists", args, b"bf.exists", 2)
        }
        CommandAstKind::BfMexists => {
            make_min_arity_list_ast(env, "bf_mexists", args, b"bf.mexists", 2)
        }
        CommandAstKind::BfCard => make_exact_arity_list_ast(env, "bf_card", args, b"bf.card", 1),
        CommandAstKind::BfInfo => make_exact_arity_list_ast(env, "bf_info", args, b"bf.info", 1),
        CommandAstKind::CfReserve => make_cf_reserve_command_ast(env, args, arg_bytes),
        CommandAstKind::CfAdd => make_min_arity_list_ast(env, "cf_add", args, b"cf.add", 2),
        CommandAstKind::CfAddnx => make_min_arity_list_ast(env, "cf_addnx", args, b"cf.addnx", 2),
        CommandAstKind::CfDel => make_min_arity_list_ast(env, "cf_del", args, b"cf.del", 2),
        CommandAstKind::CfExists => {
            make_min_arity_list_ast(env, "cf_exists", args, b"cf.exists", 2)
        }
        CommandAstKind::CfMexists => {
            make_min_arity_list_ast(env, "cf_mexists", args, b"cf.mexists", 2)
        }
        CommandAstKind::CfCount => make_exact_arity_list_ast(env, "cf_count", args, b"cf.count", 2),
        CommandAstKind::CfInfo => make_exact_arity_list_ast(env, "cf_info", args, b"cf.info", 1),
        CommandAstKind::CmsInitbydim => make_cms_initbydim_command_ast(env, args, arg_bytes),
        CommandAstKind::CmsInitbyprob => make_cms_initbyprob_command_ast(env, args, arg_bytes),
        CommandAstKind::CmsIncrby => make_count_pairs_command_ast(
            env,
            "cms_incrby",
            args,
            arg_bytes,
            b"cms.incrby",
            b"ERR CMS: invalid count value",
        ),
        CommandAstKind::CmsQuery => {
            make_min_arity_list_ast(env, "cms_query", args, b"cms.query", 2)
        }
        CommandAstKind::CmsMerge => make_cms_merge_command_ast(env, args, arg_bytes),
        CommandAstKind::CmsInfo => make_exact_arity_list_ast(env, "cms_info", args, b"cms.info", 1),
        CommandAstKind::TopkReserve => make_topk_reserve_command_ast(env, args, arg_bytes),
        CommandAstKind::TopkAdd => make_min_arity_list_ast(env, "topk_add", args, b"topk.add", 2),
        CommandAstKind::TopkIncrby => make_count_pairs_command_ast(
            env,
            "topk_incrby",
            args,
            arg_bytes,
            b"topk.incrby",
            b"ERR TOPK: invalid count value",
        ),
        CommandAstKind::TopkQuery => {
            make_min_arity_list_ast(env, "topk_query", args, b"topk.query", 2)
        }
        CommandAstKind::TopkList => make_topk_list_command_ast(env, args, arg_bytes),
        CommandAstKind::TopkCount => {
            make_min_arity_list_ast(env, "topk_count", args, b"topk.count", 2)
        }
        CommandAstKind::TopkInfo => {
            make_exact_arity_list_ast(env, "topk_info", args, b"topk.info", 1)
        }
        CommandAstKind::TdigestCreate => make_tdigest_create_command_ast(env, args, arg_bytes),
        CommandAstKind::TdigestAdd => make_float_list_command_ast(
            env,
            "tdigest_add",
            args,
            arg_bytes,
            b"tdigest.add",
            2,
            false,
        ),
        CommandAstKind::TdigestReset => {
            make_exact_arity_list_ast(env, "tdigest_reset", args, b"tdigest.reset", 1)
        }
        CommandAstKind::TdigestQuantile => make_float_list_command_ast(
            env,
            "tdigest_quantile",
            args,
            arg_bytes,
            b"tdigest.quantile",
            2,
            true,
        ),
        CommandAstKind::TdigestCdf => make_float_list_command_ast(
            env,
            "tdigest_cdf",
            args,
            arg_bytes,
            b"tdigest.cdf",
            2,
            false,
        ),
        CommandAstKind::TdigestRank => make_float_list_command_ast(
            env,
            "tdigest_rank",
            args,
            arg_bytes,
            b"tdigest.rank",
            2,
            false,
        ),
        CommandAstKind::TdigestRevrank => make_float_list_command_ast(
            env,
            "tdigest_revrank",
            args,
            arg_bytes,
            b"tdigest.revrank",
            2,
            false,
        ),
        CommandAstKind::TdigestByrank => {
            make_int_list_command_ast(env, "tdigest_byrank", args, arg_bytes, b"tdigest.byrank", 2)
        }
        CommandAstKind::TdigestByrevrank => make_int_list_command_ast(
            env,
            "tdigest_byrevrank",
            args,
            arg_bytes,
            b"tdigest.byrevrank",
            2,
        ),
        CommandAstKind::TdigestTrimmedMean => {
            make_tdigest_trimmed_mean_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::TdigestMin => {
            make_exact_arity_list_ast(env, "tdigest_min", args, b"tdigest.min", 1)
        }
        CommandAstKind::TdigestMax => {
            make_exact_arity_list_ast(env, "tdigest_max", args, b"tdigest.max", 1)
        }
        CommandAstKind::TdigestInfo => {
            make_exact_arity_list_ast(env, "tdigest_info", args, b"tdigest.info", 1)
        }
        CommandAstKind::TdigestMerge => make_tdigest_merge_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCreate => make_flow_create_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCreateMany => make_flow_create_many_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowValuePut => make_flow_value_put_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowSignal => make_flow_signal_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowSpawnChildren => {
            make_flow_spawn_children_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::FlowGet => make_flow_get_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowPolicySet => make_flow_policy_set_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowPolicyGet => make_flow_policy_get_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowClaimDue => make_flow_claim_due_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowReclaim => make_flow_reclaim_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowExtendLease => make_flow_extend_lease_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowComplete => make_flow_complete_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCompleteMany => {
            make_flow_complete_many_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::FlowTransition => make_flow_transition_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowTransitionMany => {
            make_flow_transition_many_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::FlowRetry => make_flow_retry_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowRetryMany => make_flow_retry_many_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowFail => make_flow_fail_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowFailMany => make_flow_fail_many_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCancel => make_flow_cancel_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowCancelMany => make_flow_cancel_many_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowRewind => make_flow_rewind_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowList => make_flow_list_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowTerminals => make_flow_terminals_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowFailures => make_flow_failures_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowByParent => make_flow_index_query_command_ast(
            env,
            "flow_by_parent",
            b"flow.by_parent",
            args,
            arg_bytes,
        ),
        CommandAstKind::FlowByRoot => {
            make_flow_index_query_command_ast(env, "flow_by_root", b"flow.by_root", args, arg_bytes)
        }
        CommandAstKind::FlowByCorrelation => make_flow_index_query_command_ast(
            env,
            "flow_by_correlation",
            b"flow.by_correlation",
            args,
            arg_bytes,
        ),
        CommandAstKind::FlowInfo => make_flow_info_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowStuck => make_flow_stuck_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowHistory => make_flow_history_command_ast(env, args, arg_bytes),
        CommandAstKind::FlowRetentionCleanup => {
            make_flow_retention_cleanup_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::Blpop => make_blocking_pop_command_ast(
            env,
            atoms::blpop(),
            b"ERR wrong number of arguments for 'blpop' command",
            args,
            arg_bytes,
        ),
        CommandAstKind::Brpop => make_blocking_pop_command_ast(
            env,
            atoms::brpop(),
            b"ERR wrong number of arguments for 'brpop' command",
            args,
            arg_bytes,
        ),
        CommandAstKind::Blmove => make_blmove_command_ast(env, args, arg_bytes),
        CommandAstKind::Blmpop => make_blmpop_command_ast(env, args, arg_bytes),
        CommandAstKind::Hello => make_hello_command_ast(env, args, arg_bytes),
        CommandAstKind::Auth => make_auth_command_ast(env, args),
        CommandAstKind::Acl => make_upper_subcommand_ast(
            env,
            "acl",
            b"ERR wrong number of arguments for 'acl' command",
            args,
            arg_bytes,
        ),
        CommandAstKind::Client => make_client_command_ast(env, args, arg_bytes),
        CommandAstKind::Sandbox => make_upper_subcommand_ast(
            env,
            "sandbox",
            b"ERR unknown SANDBOX subcommand",
            args,
            arg_bytes,
        ),
        CommandAstKind::Subscribe => make_nonempty_list_ast(
            env,
            "subscribe",
            b"ERR wrong number of arguments for 'subscribe' command",
            args,
        ),
        CommandAstKind::Unsubscribe => (atom(env, "unsubscribe"), args).encode(env),
        CommandAstKind::Psubscribe => make_nonempty_list_ast(
            env,
            "psubscribe",
            b"ERR wrong number of arguments for 'psubscribe' command",
            args,
        ),
        CommandAstKind::Punsubscribe => (atom(env, "punsubscribe"), args).encode(env),
        CommandAstKind::Multi => make_zero_arity_atom_or_error(
            env,
            "multi",
            b"ERR wrong number of arguments for 'multi' command",
            args,
        ),
        CommandAstKind::Exec => make_zero_arity_atom_or_error(
            env,
            "exec",
            b"ERR wrong number of arguments for 'exec' command",
            args,
        ),
        CommandAstKind::Discard => make_zero_arity_atom_or_error(
            env,
            "discard",
            b"ERR wrong number of arguments for 'discard' command",
            args,
        ),
        CommandAstKind::Watch => make_nonempty_list_ast(
            env,
            "watch",
            b"ERR wrong number of arguments for 'watch' command",
            args,
        ),
        CommandAstKind::Unwatch => make_zero_arity_atom_or_error(
            env,
            "unwatch",
            b"ERR wrong number of arguments for 'unwatch' command",
            args,
        ),
        CommandAstKind::Reset => make_zero_arity_atom_or_error(
            env,
            "reset",
            b"ERR wrong number of arguments for 'reset' command",
            args,
        ),
        CommandAstKind::Quit => make_zero_arity_atom_or_error(
            env,
            "quit",
            b"ERR wrong number of arguments for 'quit' command",
            args,
        ),
        CommandAstKind::Unknown if cmd == b"SET" && args.len() >= 2 => {
            make_set_command_ast(env, args, arg_bytes)
        }
        CommandAstKind::Unknown if upper_first_arg_command(cmd) => {
            make_upper_first_arg_ast(env, cmd, args, arg_bytes)
        }
        CommandAstKind::Unknown if upper_all_args_command(cmd) => {
            make_upper_all_args_ast(env, cmd, args, arg_bytes)
        }
        CommandAstKind::Unknown if native_typed_command(cmd) => {
            make_native_typed_ast(env, cmd, args, arg_bytes)
        }
        CommandAstKind::Unknown => match command_tag_name(cmd) {
            Some(tag) => (Atom::from_str(env, tag).unwrap(), args).encode(env),
            None => (atoms::unknown(), make_binary_term(env, cmd), args).encode(env),
        },
    }
}

fn upper_first_arg_command(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"COMMAND"
            | b"DEBUG"
            | b"CONFIG"
            | b"MODULE"
            | b"SLOWLOG"
            | b"MEMORY"
            | b"FERRICSTORE.CONFIG"
            | b"PUBSUB"
            | b"LOLWUT"
    )
}

fn upper_all_args_command(cmd: &[u8]) -> bool {
    matches!(cmd, b"FLUSHDB" | b"FLUSHALL")
}

fn native_typed_command(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"CAS"
            | b"LOCK"
            | b"UNLOCK"
            | b"EXTEND"
            | b"RATELIMIT.ADD"
            | b"FETCH_OR_COMPUTE"
            | b"FETCH_OR_COMPUTE_RESULT"
            | b"FETCH_OR_COMPUTE_ERROR"
            | b"FERRICSTORE.KEY_INFO"
    )
}

fn make_upper_first_arg_ast<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let Some(tag) = command_tag_name(cmd) else {
        return (atoms::unknown(), make_binary_term(env, cmd), args).encode(env);
    };

    if args.is_empty() {
        return (Atom::from_str(env, tag).unwrap(), Vec::<Term<'a>>::new()).encode(env);
    }

    let mut parsed = Vec::with_capacity(args.len());
    parsed.push(uppercase_binary_term(env, arg_bytes[0]));
    parsed.extend_from_slice(&args[1..]);
    (Atom::from_str(env, tag).unwrap(), parsed).encode(env)
}

fn make_upper_all_args_ast<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    let Some(tag) = command_tag_name(cmd) else {
        return (atoms::unknown(), make_binary_term(env, cmd), args).encode(env);
    };

    let parsed: Vec<Term<'a>> = arg_bytes
        .iter()
        .map(|arg| uppercase_binary_term(env, arg))
        .collect();
    (Atom::from_str(env, tag).unwrap(), parsed).encode(env)
}

fn make_native_typed_ast<'a>(
    env: Env<'a>,
    cmd: &[u8],
    args: &[Term<'a>],
    arg_bytes: &[&[u8]],
) -> Term<'a> {
    match cmd {
        b"CAS" => make_cas_command_ast(env, args, arg_bytes),
        b"LOCK" => make_lock_like_command_ast(env, atom(env, "lock"), b"lock", args, arg_bytes),
        b"UNLOCK" => make_unlock_command_ast(env, args),
        b"EXTEND" => {
            make_lock_like_command_ast(env, atom(env, "extend"), b"extend", args, arg_bytes)
        }
        b"RATELIMIT.ADD" => make_ratelimit_add_command_ast(env, args, arg_bytes),
        b"FETCH_OR_COMPUTE" => make_fetch_or_compute_command_ast(env, args, arg_bytes),
        b"FETCH_OR_COMPUTE_RESULT" => {
            make_fetch_or_compute_result_command_ast(env, args, arg_bytes)
        }
        b"FETCH_OR_COMPUTE_ERROR" => make_fetch_or_compute_error_command_ast(env, args),
        b"FERRICSTORE.KEY_INFO" => make_key_info_command_ast(env, args),
        _ => (atoms::unknown(), make_binary_term(env, cmd), args).encode(env),
    }
}
