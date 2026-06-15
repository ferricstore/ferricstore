    fn nested_array(depth: usize) -> Vec<u8> {
        let mut input = Vec::new();
        for _ in 0..depth {
            input.extend_from_slice(b"*1\r\n");
        }
        input.extend_from_slice(b"$1\r\na\r\n");
        input
    }

    #[test]
    fn ast_classifies_hot_string_commands_by_arity() {
        assert_eq!(classify_command_ast(b"GET", 1), CommandAstKind::Get);
        assert_eq!(classify_command_ast(b"SET", 2), CommandAstKind::Set);
        assert_eq!(classify_command_ast(b"PING", 0), CommandAstKind::Ping0);
        assert_eq!(classify_command_ast(b"PING", 1), CommandAstKind::Ping1);
        assert_eq!(classify_command_ast(b"DEL", 3), CommandAstKind::Del);
        assert_eq!(classify_command_ast(b"EXISTS", 2), CommandAstKind::Exists);
        assert_eq!(classify_command_ast(b"MGET", 4), CommandAstKind::Mget);
        assert_eq!(classify_command_ast(b"MSET", 4), CommandAstKind::Mset);
        assert_eq!(classify_command_ast(b"INCR", 1), CommandAstKind::Incr);
        assert_eq!(classify_command_ast(b"DECR", 1), CommandAstKind::Decr);
        assert_eq!(classify_command_ast(b"INCRBY", 2), CommandAstKind::Incrby);
        assert_eq!(classify_command_ast(b"DECRBY", 2), CommandAstKind::Decrby);
        assert_eq!(
            classify_command_ast(b"INCRBYFLOAT", 2),
            CommandAstKind::Incrbyfloat
        );
        assert_eq!(classify_command_ast(b"APPEND", 2), CommandAstKind::Append);
        assert_eq!(classify_command_ast(b"STRLEN", 1), CommandAstKind::Strlen);
    }

    #[test]
    fn ast_rejects_wrong_arity_to_unknown_for_fixed_arity_commands() {
        assert_eq!(classify_command_ast(b"GET", 0), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"GET", 2), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"SET", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"PING", 2), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"INCRBY", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"SETEX", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_knows_catalog_commands_even_when_semantic_shape_is_generic() {
        assert_eq!(command_tag_name(b"XADD"), Some("xadd"));
        assert_eq!(command_tag_name(b"BF.ADD"), Some("bf_add"));
        assert_eq!(command_tag_name(b"TDIGEST.MERGE"), Some("tdigest_merge"));
        assert_eq!(command_tag_name(b"CLUSTER.HEALTH"), Some("cluster_health"));
        assert_eq!(
            command_tag_name(b"FERRICSTORE.BLOBGC"),
            Some("ferricstore_blobgc")
        );
        assert_eq!(
            command_tag_name(b"FERRICSTORE.DOCTOR"),
            Some("ferricstore_doctor")
        );
        assert_eq!(command_tag_name(b"CLUSTER.ENABLE"), None);
        assert_eq!(command_tag_name(b"CLUSTER.DURABILITY"), None);
        assert_eq!(command_tag_name(b"NO_SUCH_COMMAND"), None);
    }

    #[test]
    fn ast_classifies_geo_commands_even_for_error_arity() {
        assert_eq!(classify_command_ast(b"GEOADD", 4), CommandAstKind::Geoadd);
        assert_eq!(classify_command_ast(b"GEOADD", 1), CommandAstKind::Geoadd);
        assert_eq!(classify_command_ast(b"GEOPOS", 2), CommandAstKind::Geopos);
        assert_eq!(classify_command_ast(b"GEODIST", 4), CommandAstKind::Geodist);
        assert_eq!(classify_command_ast(b"GEOHASH", 2), CommandAstKind::Geohash);
        assert_eq!(
            classify_command_ast(b"GEOSEARCH", 8),
            CommandAstKind::Geosearch
        );
        assert_eq!(
            classify_command_ast(b"GEOSEARCHSTORE", 9),
            CommandAstKind::Geosearchstore
        );
    }

    #[test]
    fn ast_classifies_hll_commands_even_for_error_arity() {
        assert_eq!(classify_command_ast(b"PFADD", 0), CommandAstKind::Pfadd);
        assert_eq!(classify_command_ast(b"PFCOUNT", 0), CommandAstKind::Pfcount);
        assert_eq!(classify_command_ast(b"PFMERGE", 1), CommandAstKind::Pfmerge);
    }

    #[test]
    fn ast_classifies_blocking_commands_even_for_error_arity() {
        assert_eq!(classify_command_ast(b"BLPOP", 0), CommandAstKind::Blpop);
        assert_eq!(classify_command_ast(b"BRPOP", 1), CommandAstKind::Brpop);
        assert_eq!(classify_command_ast(b"BLMOVE", 3), CommandAstKind::Blmove);
        assert_eq!(classify_command_ast(b"BLMPOP", 2), CommandAstKind::Blmpop);
    }

    #[test]
    fn ast_classifies_probabilistic_commands_even_for_error_arity() {
        assert_eq!(
            classify_command_ast(b"BF.RESERVE", 0),
            CommandAstKind::BfReserve
        );
        assert_eq!(classify_command_ast(b"BF.ADD", 1), CommandAstKind::BfAdd);
        assert_eq!(
            classify_command_ast(b"CF.RESERVE", 0),
            CommandAstKind::CfReserve
        );
        assert_eq!(
            classify_command_ast(b"CMS.INITBYDIM", 0),
            CommandAstKind::CmsInitbydim
        );
        assert_eq!(
            classify_command_ast(b"CMS.INCRBY", 2),
            CommandAstKind::CmsIncrby
        );
        assert_eq!(
            classify_command_ast(b"CMS.MERGE", 1),
            CommandAstKind::CmsMerge
        );
        assert_eq!(
            classify_command_ast(b"TOPK.RESERVE", 1),
            CommandAstKind::TopkReserve
        );
        assert_eq!(
            classify_command_ast(b"TOPK.LIST", 2),
            CommandAstKind::TopkList
        );
        assert_eq!(
            classify_command_ast(b"TDIGEST.CREATE", 0),
            CommandAstKind::TdigestCreate
        );
        assert_eq!(
            classify_command_ast(b"TDIGEST.MERGE", 1),
            CommandAstKind::TdigestMerge
        );
    }

    #[test]
    fn ast_classifies_flow_commands_even_for_error_arity() {
        assert_eq!(
            classify_command_ast(b"FLOW.CREATE", 0),
            CommandAstKind::FlowCreate
        );
        assert_eq!(
            classify_command_ast(b"FLOW.CREATE_MANY", 0),
            CommandAstKind::FlowCreateMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.VALUE.PUT", 0),
            CommandAstKind::FlowValuePut
        );
        assert_eq!(
            classify_command_ast(b"FLOW.SIGNAL", 0),
            CommandAstKind::FlowSignal
        );
        assert_eq!(
            classify_command_ast(b"FLOW.SPAWN_CHILDREN", 0),
            CommandAstKind::FlowSpawnChildren
        );
        assert_eq!(
            classify_command_ast(b"FLOW.COMPLETE_MANY", 0),
            CommandAstKind::FlowCompleteMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.RETRY_MANY", 0),
            CommandAstKind::FlowRetryMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.FAIL_MANY", 0),
            CommandAstKind::FlowFailMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.CANCEL_MANY", 0),
            CommandAstKind::FlowCancelMany
        );
        assert_eq!(
            classify_command_ast(b"FLOW.CLAIM_DUE", 0),
            CommandAstKind::FlowClaimDue
        );
        assert_eq!(
            classify_command_ast(b"FLOW.RECLAIM", 0),
            CommandAstKind::FlowReclaim
        );
        assert_eq!(
            classify_command_ast(b"FLOW.EXTEND_LEASE", 0),
            CommandAstKind::FlowExtendLease
        );
        assert_eq!(
            classify_command_ast(b"FLOW.BY_PARENT", 0),
            CommandAstKind::FlowByParent
        );
        assert_eq!(
            classify_command_ast(b"FLOW.BY_ROOT", 0),
            CommandAstKind::FlowByRoot
        );
        assert_eq!(
            classify_command_ast(b"FLOW.BY_CORRELATION", 0),
            CommandAstKind::FlowByCorrelation
        );
        assert_eq!(
            classify_command_ast(b"FLOW.RETENTION_CLEANUP", 0),
            CommandAstKind::FlowRetentionCleanup
        );
    }

    #[test]
    fn ast_classifies_extended_string_commands() {
        assert_eq!(classify_command_ast(b"GETSET", 2), CommandAstKind::Getset);
        assert_eq!(classify_command_ast(b"GETDEL", 1), CommandAstKind::Getdel);
        assert_eq!(classify_command_ast(b"GETEX", 1), CommandAstKind::Getex);
        assert_eq!(classify_command_ast(b"GETEX", 3), CommandAstKind::Getex);
        assert_eq!(classify_command_ast(b"GETEX", 4), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"SETNX", 2), CommandAstKind::Setnx);
        assert_eq!(classify_command_ast(b"SETEX", 3), CommandAstKind::Setex);
        assert_eq!(classify_command_ast(b"PSETEX", 3), CommandAstKind::Psetex);
        assert_eq!(
            classify_command_ast(b"GETRANGE", 3),
            CommandAstKind::Getrange
        );
        assert_eq!(
            classify_command_ast(b"SETRANGE", 3),
            CommandAstKind::Setrange
        );
        assert_eq!(classify_command_ast(b"MSETNX", 6), CommandAstKind::Msetnx);
    }

    #[test]
    fn ast_classifies_expiry_commands_by_arity() {
        assert_eq!(classify_command_ast(b"EXPIRE", 2), CommandAstKind::Expire);
        assert_eq!(classify_command_ast(b"EXPIRE", 3), CommandAstKind::Expire);
        assert_eq!(classify_command_ast(b"PEXPIRE", 2), CommandAstKind::Pexpire);
        assert_eq!(classify_command_ast(b"PEXPIRE", 3), CommandAstKind::Pexpire);
        assert_eq!(
            classify_command_ast(b"EXPIREAT", 2),
            CommandAstKind::Expireat
        );
        assert_eq!(
            classify_command_ast(b"EXPIREAT", 3),
            CommandAstKind::Expireat
        );
        assert_eq!(
            classify_command_ast(b"PEXPIREAT", 2),
            CommandAstKind::Pexpireat
        );
        assert_eq!(
            classify_command_ast(b"PEXPIREAT", 3),
            CommandAstKind::Pexpireat
        );
        assert_eq!(classify_command_ast(b"TTL", 1), CommandAstKind::Ttl);
        assert_eq!(classify_command_ast(b"PTTL", 1), CommandAstKind::Pttl);
        assert_eq!(classify_command_ast(b"PERSIST", 1), CommandAstKind::Persist);
        assert_eq!(classify_command_ast(b"EXPIRE", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"TTL", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_list_commands_by_arity() {
        assert_eq!(classify_command_ast(b"LPUSH", 2), CommandAstKind::Lpush);
        assert_eq!(classify_command_ast(b"RPUSH", 3), CommandAstKind::Rpush);
        assert_eq!(classify_command_ast(b"LPOP", 1), CommandAstKind::Lpop);
        assert_eq!(classify_command_ast(b"LPOP", 2), CommandAstKind::Lpop);
        assert_eq!(classify_command_ast(b"LRANGE", 3), CommandAstKind::Lrange);
        assert_eq!(classify_command_ast(b"LINDEX", 2), CommandAstKind::Lindex);
        assert_eq!(classify_command_ast(b"LSET", 3), CommandAstKind::Lset);
        assert_eq!(classify_command_ast(b"LREM", 3), CommandAstKind::Lrem);
        assert_eq!(classify_command_ast(b"LTRIM", 3), CommandAstKind::Ltrim);
        assert_eq!(classify_command_ast(b"LINSERT", 4), CommandAstKind::Linsert);
        assert_eq!(classify_command_ast(b"LMOVE", 4), CommandAstKind::Lmove);
        assert_eq!(
            classify_command_ast(b"RPOPLPUSH", 2),
            CommandAstKind::Rpoplpush
        );
        assert_eq!(classify_command_ast(b"LPUSH", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"LRANGE", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_hash_commands_by_arity() {
        assert_eq!(classify_command_ast(b"HSET", 3), CommandAstKind::Hset);
        assert_eq!(classify_command_ast(b"HGET", 2), CommandAstKind::Hget);
        assert_eq!(classify_command_ast(b"HDEL", 2), CommandAstKind::Hdel);
        assert_eq!(classify_command_ast(b"HMGET", 2), CommandAstKind::Hmget);
        assert_eq!(classify_command_ast(b"HGETALL", 1), CommandAstKind::Hgetall);
        assert_eq!(classify_command_ast(b"HEXISTS", 2), CommandAstKind::Hexists);
        assert_eq!(classify_command_ast(b"HKEYS", 1), CommandAstKind::Hkeys);
        assert_eq!(classify_command_ast(b"HVALS", 1), CommandAstKind::Hvals);
        assert_eq!(classify_command_ast(b"HLEN", 1), CommandAstKind::Hlen);
        assert_eq!(classify_command_ast(b"HINCRBY", 3), CommandAstKind::Hincrby);
        assert_eq!(
            classify_command_ast(b"HINCRBYFLOAT", 3),
            CommandAstKind::Hincrbyfloat
        );
        assert_eq!(classify_command_ast(b"HSETNX", 3), CommandAstKind::Hsetnx);
        assert_eq!(classify_command_ast(b"HSTRLEN", 2), CommandAstKind::Hstrlen);
        assert_eq!(
            classify_command_ast(b"HRANDFIELD", 3),
            CommandAstKind::Hrandfield
        );
        assert_eq!(classify_command_ast(b"HSCAN", 2), CommandAstKind::Hscan);
        assert_eq!(classify_command_ast(b"HEXPIRE", 4), CommandAstKind::Hexpire);
        assert_eq!(classify_command_ast(b"HTTL", 3), CommandAstKind::Httl);
        assert_eq!(
            classify_command_ast(b"HPERSIST", 3),
            CommandAstKind::Hpersist
        );
        assert_eq!(
            classify_command_ast(b"HPEXPIRE", 4),
            CommandAstKind::Hpexpire
        );
        assert_eq!(classify_command_ast(b"HPTTL", 3), CommandAstKind::Hpttl);
        assert_eq!(
            classify_command_ast(b"HEXPIRETIME", 3),
            CommandAstKind::Hexpiretime
        );
        assert_eq!(classify_command_ast(b"HGETDEL", 3), CommandAstKind::Hgetdel);
        assert_eq!(classify_command_ast(b"HGETEX", 4), CommandAstKind::Hgetex);
        assert_eq!(classify_command_ast(b"HSETEX", 4), CommandAstKind::Hsetex);
        assert_eq!(classify_command_ast(b"HGET", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"HINCRBY", 2), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"HGETEX", 3), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_set_commands_by_arity() {
        assert_eq!(classify_command_ast(b"SADD", 2), CommandAstKind::Sadd);
        assert_eq!(classify_command_ast(b"SREM", 2), CommandAstKind::Srem);
        assert_eq!(
            classify_command_ast(b"SMEMBERS", 1),
            CommandAstKind::Smembers
        );
        assert_eq!(
            classify_command_ast(b"SISMEMBER", 2),
            CommandAstKind::Sismember
        );
        assert_eq!(
            classify_command_ast(b"SMISMEMBER", 2),
            CommandAstKind::Smismember
        );
        assert_eq!(classify_command_ast(b"SCARD", 1), CommandAstKind::Scard);
        assert_eq!(classify_command_ast(b"SINTER", 2), CommandAstKind::Sinter);
        assert_eq!(classify_command_ast(b"SUNION", 2), CommandAstKind::Sunion);
        assert_eq!(classify_command_ast(b"SDIFF", 2), CommandAstKind::Sdiff);
        assert_eq!(
            classify_command_ast(b"SDIFFSTORE", 2),
            CommandAstKind::Sdiffstore
        );
        assert_eq!(
            classify_command_ast(b"SINTERSTORE", 2),
            CommandAstKind::Sinterstore
        );
        assert_eq!(
            classify_command_ast(b"SUNIONSTORE", 2),
            CommandAstKind::Sunionstore
        );
        assert_eq!(
            classify_command_ast(b"SINTERCARD", 2),
            CommandAstKind::Sintercard
        );
        assert_eq!(
            classify_command_ast(b"SRANDMEMBER", 2),
            CommandAstKind::Srandmember
        );
        assert_eq!(classify_command_ast(b"SPOP", 2), CommandAstKind::Spop);
        assert_eq!(classify_command_ast(b"SMOVE", 3), CommandAstKind::Smove);
        assert_eq!(classify_command_ast(b"SSCAN", 2), CommandAstKind::Sscan);
        assert_eq!(classify_command_ast(b"SADD", 1), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"SMOVE", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_bitmap_commands_by_arity() {
        assert_eq!(classify_command_ast(b"SETBIT", 3), CommandAstKind::Setbit);
        assert_eq!(classify_command_ast(b"GETBIT", 2), CommandAstKind::Getbit);
        assert_eq!(
            classify_command_ast(b"BITCOUNT", 1),
            CommandAstKind::Bitcount
        );
        assert_eq!(
            classify_command_ast(b"BITCOUNT", 4),
            CommandAstKind::Bitcount
        );
        assert_eq!(classify_command_ast(b"BITPOS", 2), CommandAstKind::Bitpos);
        assert_eq!(classify_command_ast(b"BITPOS", 5), CommandAstKind::Bitpos);
        assert_eq!(classify_command_ast(b"BITOP", 3), CommandAstKind::Bitop);
        assert_eq!(classify_command_ast(b"SETBIT", 2), CommandAstKind::Unknown);
        assert_eq!(
            classify_command_ast(b"BITCOUNT", 2),
            CommandAstKind::Unknown
        );
        assert_eq!(classify_command_ast(b"BITPOS", 6), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"BITOP", 2), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_generic_commands_by_arity() {
        assert_eq!(classify_command_ast(b"TYPE", 1), CommandAstKind::Type);
        assert_eq!(classify_command_ast(b"UNLINK", 2), CommandAstKind::Unlink);
        assert_eq!(classify_command_ast(b"RENAME", 2), CommandAstKind::Rename);
        assert_eq!(
            classify_command_ast(b"RENAMENX", 2),
            CommandAstKind::Renamenx
        );
        assert_eq!(classify_command_ast(b"COPY", 3), CommandAstKind::Copy);
        assert_eq!(
            classify_command_ast(b"RANDOMKEY", 0),
            CommandAstKind::Randomkey
        );
        assert_eq!(classify_command_ast(b"SCAN", 5), CommandAstKind::Scan);
        assert_eq!(
            classify_command_ast(b"EXPIRETIME", 1),
            CommandAstKind::Expiretime
        );
        assert_eq!(
            classify_command_ast(b"PEXPIRETIME", 1),
            CommandAstKind::Pexpiretime
        );
        assert_eq!(classify_command_ast(b"OBJECT", 2), CommandAstKind::Object);
        assert_eq!(classify_command_ast(b"WAIT", 2), CommandAstKind::Wait);
        assert_eq!(classify_command_ast(b"TYPE", 0), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"RENAME", 3), CommandAstKind::Unknown);
        assert_eq!(
            classify_command_ast(b"RANDOMKEY", 1),
            CommandAstKind::Unknown
        );
        assert_eq!(classify_command_ast(b"WAIT", 1), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_classifies_zset_commands_by_arity() {
        assert_eq!(classify_command_ast(b"ZADD", 3), CommandAstKind::Zadd);
        assert_eq!(classify_command_ast(b"ZREM", 2), CommandAstKind::Zrem);
        assert_eq!(classify_command_ast(b"ZSCORE", 2), CommandAstKind::Zscore);
        assert_eq!(classify_command_ast(b"ZRANK", 2), CommandAstKind::Zrank);
        assert_eq!(
            classify_command_ast(b"ZREVRANK", 2),
            CommandAstKind::Zrevrank
        );
        assert_eq!(classify_command_ast(b"ZRANGE", 4), CommandAstKind::Zrange);
        assert_eq!(
            classify_command_ast(b"ZREVRANGE", 4),
            CommandAstKind::Zrevrange
        );
        assert_eq!(classify_command_ast(b"ZCARD", 1), CommandAstKind::Zcard);
        assert_eq!(classify_command_ast(b"ZINCRBY", 3), CommandAstKind::Zincrby);
        assert_eq!(classify_command_ast(b"ZCOUNT", 3), CommandAstKind::Zcount);
        assert_eq!(classify_command_ast(b"ZPOPMIN", 2), CommandAstKind::Zpopmin);
        assert_eq!(classify_command_ast(b"ZPOPMAX", 2), CommandAstKind::Zpopmax);
        assert_eq!(
            classify_command_ast(b"ZRANDMEMBER", 3),
            CommandAstKind::Zrandmember
        );
        assert_eq!(classify_command_ast(b"ZSCAN", 2), CommandAstKind::Zscan);
        assert_eq!(classify_command_ast(b"ZMSCORE", 2), CommandAstKind::Zmscore);
        assert_eq!(
            classify_command_ast(b"ZRANGEBYSCORE", 3),
            CommandAstKind::Zrangebyscore
        );
        assert_eq!(
            classify_command_ast(b"ZREVRANGEBYSCORE", 3),
            CommandAstKind::Zrevrangebyscore
        );
        assert_eq!(classify_command_ast(b"ZADD", 2), CommandAstKind::Unknown);
        assert_eq!(classify_command_ast(b"ZRANGE", 5), CommandAstKind::Unknown);
    }

    #[test]
    fn ast_extracts_acl_keys_for_common_shapes() {
        assert_eq!(command_key_indices(b"GET", &[b"k"]), vec![0]);
        assert_eq!(command_key_indices(b"SET", &[b"k", b"v"]), vec![0]);
        assert_eq!(
            command_key_indices(b"PUBLISH", &[b"tenant:a", b"msg"]),
            vec![0]
        );
        assert_eq!(
            command_key_indices(b"SUBSCRIBE", &[b"tenant:a", b"tenant:b"]),
            vec![0, 1]
        );
        assert_eq!(command_key_indices(b"PSUBSCRIBE", &[b"tenant:*"]), vec![0]);
        assert_eq!(
            command_key_indices(b"PUBSUB", &[b"NUMSUB", b"tenant:a", b"tenant:b"]),
            vec![1, 2]
        );
        assert_eq!(
            command_key_indices(b"PUBSUB", &[b"CHANNELS", b"tenant:*"]),
            vec![1]
        );
        assert_eq!(
            command_key_indices(b"DEL", &[b"a", b"b", b"c"]),
            vec![0, 1, 2]
        );
        assert_eq!(
            command_key_indices(b"MSET", &[b"a", b"1", b"b", b"2"]),
            vec![0, 2]
        );
        assert_eq!(command_key_indices(b"COPY", &[b"src", b"dst"]), vec![0, 1]);
        assert_eq!(
            command_key_indices(b"BITOP", &[b"AND", b"dst", b"a", b"b"]),
            vec![1, 2, 3]
        );
        assert_eq!(
            command_key_indices(b"SMOVE", &[b"src", b"dst", b"member"]),
            vec![0, 1]
        );
    }

    #[test]
    fn ast_extracts_acl_keys_for_counted_and_stream_shapes() {
        assert_eq!(
            command_key_indices(
                b"XREAD",
                &[b"COUNT", b"2", b"STREAMS", b"s1", b"s2", b"0", b"0"]
            ),
            vec![3, 4]
        );
        assert_eq!(
            command_key_indices(
                b"XREADGROUP",
                &[b"GROUP", b"g", b"c", b"BLOCK", b"1", b"STREAMS", b"s1", b"s2", b">", b"0",]
            ),
            vec![6, 7]
        );
        assert_eq!(
            command_key_indices(
                b"BLMPOP",
                &[b"1.5", b"2", b"k1", b"k2", b"LEFT", b"COUNT", b"1"]
            ),
            vec![2, 3]
        );
        assert_eq!(
            command_key_indices(b"CMS.MERGE", &[b"dst", b"2", b"src1", b"src2", b"WEIGHTS"]),
            vec![0, 2, 3]
        );
        assert_eq!(
            command_key_indices(b"TDIGEST.MERGE", &[b"dst", b"2", b"src1", b"src2"]),
            vec![0, 2, 3]
        );
        assert_eq!(
            command_key_indices(b"CF.MEXISTS", &[b"cf", b"a", b"b"]),
            vec![0]
        );
        assert_eq!(
            command_key_indices(b"SINTERCARD", &[b"2", b"s1", b"s2", b"LIMIT", b"1"]),
            vec![1, 2]
        );
    }

    #[test]
    fn ast_extracts_acl_keys_for_flow_partition_shapes() {
        assert_eq!(
            command_key_indices(
                b"FLOW.GET",
                &[b"flow-1", b"FULL", b"PARTITION", b"tenant-a"]
            ),
            vec![3]
        );
        assert_eq!(
            command_key_indices(
                b"FLOW.CLAIM_DUE",
                &[
                    b"checkout",
                    b"WORKER",
                    b"w",
                    b"PAYLOAD",
                    b"MAXBYTES",
                    b"4096",
                    b"PARTITION",
                    b"tenant-a",
                ]
            ),
            vec![7]
        );
        assert_eq!(
            command_key_indices(b"FLOW.POLICY.SET", &[b"checkout", b"MAX_RETRIES", b"3"]),
            vec![0]
        );
        assert_eq!(
            command_key_indices(
                b"FLOW.SPAWN_CHILDREN",
                &[
                    b"parent",
                    b"GROUP",
                    b"g",
                    b"PARTITION",
                    b"parent-p",
                    b"FENCING",
                    b"1",
                    b"ITEMS",
                    b"MIXED",
                    b"child-a",
                    b"device-a",
                    b"child",
                    b"payload-a",
                    b"child-b",
                    b"device-b",
                    b"child",
                    b"payload-b",
                ]
            ),
            vec![4, 10, 14]
        );
    }
