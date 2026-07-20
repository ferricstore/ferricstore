defmodule Ferricstore.Commands.NativeAstParser do
  @moduledoc """
  Parses native command frames into FerricStore command ASTs.

  Native protocol already separates command name from arguments, so this module
  intentionally does not parse text-protocol bytes. It converts the command name to the
  same AST dispatcher shape used internally and extracts keys through the
  command catalog.
  """

  alias Ferricstore.Commands.{Catalog, CollectionScan, Extension}
  alias Ferricstore.Flow.Query.Limits

  @max_flow_ref_size 4096
  @max_flow_query_parameters Limits.max_parameters()
  @min_int64 -9_223_372_036_854_775_808
  @max_int64 9_223_372_036_854_775_807
  @max_blocking_timeout_ms 0xFFFFFFFF
  @flow_repeated_option_keys ~w(attributes_delete values value_refs drop_values override_values)a
  @extra_first_key_commands ~w(
    GET SET SETEX PSETEX SETNX GETSET GETEX GETDEL APPEND STRLEN
    INCR DECR INCRBY DECRBY INCRBYFLOAT GETRANGE SETRANGE
    EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST EXPIRETIME PEXPIRETIME
    TYPE OBJECT MEMORY
    LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LLEN LRANGE LTRIM LINDEX LSET LREM LINSERT LPOS
    HGET HGETALL HKEYS HVALS HLEN HEXISTS HSTRLEN HSET HSETNX HDEL HMGET
    HINCRBY HINCRBYFLOAT HRANDFIELD HSCAN HEXPIRE HPEXPIRE HTTL HPTTL HPERSIST
    HEXPIRETIME HGETDEL HGETEX HSETEX
    SADD SREM SMEMBERS SCARD SISMEMBER SMISMEMBER SRANDMEMBER SPOP SSCAN SINTERCARD
    ZADD ZINCRBY ZSCORE ZRANK ZREVRANK ZPOPMIN ZPOPMAX ZRANDMEMBER ZCOUNT
    ZRANGE ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZSCAN ZREM ZMSCORE ZCARD
    PFADD PFCOUNT
    GETBIT SETBIT BITCOUNT BITPOS
    XADD XLEN XRANGE XREVRANGE XTRIM XDEL XINFO XGROUP XREADGROUP XACK
    GEOADD GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE
    BF.RESERVE BF.ADD BF.MADD BF.EXISTS BF.MEXISTS BF.CARD BF.INFO
    CF.RESERVE CF.ADD CF.ADDNX CF.DEL CF.EXISTS CF.MEXISTS CF.COUNT CF.INFO
    CMS.INITBYDIM CMS.INITBYPROB CMS.INCRBY CMS.QUERY CMS.MERGE CMS.INFO
    TOPK.RESERVE TOPK.ADD TOPK.INCRBY TOPK.QUERY TOPK.LIST TOPK.COUNT TOPK.INFO
    TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET TDIGEST.QUANTILE TDIGEST.CDF
    TDIGEST.RANK TDIGEST.REVRANK TDIGEST.BYRANK TDIGEST.BYREVRANK
    TDIGEST.TRIMMED_MEAN TDIGEST.MIN TDIGEST.MAX TDIGEST.INFO TDIGEST.MERGE
    CAS LOCK UNLOCK EXTEND RATELIMIT.ADD KEY_INFO
    FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT FETCH_OR_COMPUTE_ERROR
  )
  @management_scoped_commands ~w(FERRICSTORE.NAMESPACE FERRICSTORE.QUOTA FERRICSTORE.TELEMETRY)

  @command_tags Map.new(
                  Enum.map(Catalog.names(), &String.upcase/1),
                  fn upper ->
                    tag =
                      upper |> String.downcase() |> String.replace(".", "_") |> String.to_atom()

                    {upper, tag}
                  end
                )
  @supported_command_names MapSet.new(Map.keys(@command_tags))

  @doc """
  Returns the command names accepted by the native AST parser.
  """
  @spec supported_command_names() :: MapSet.t(String.t())
  def supported_command_names do
    MapSet.union(@supported_command_names, Extension.command_names_upper())
  end

  @doc false
  @spec command_matches_ast?(binary(), term()) :: boolean()
  def command_matches_ast?(command, ast) when is_binary(command) do
    case Map.fetch(@command_tags, command) do
      {:ok, expected_tag} -> ast_tag(ast) == expected_tag
      :error -> false
    end
  end

  def command_matches_ast?(_command, _ast), do: false

  defp ast_tag(tag) when is_atom(tag), do: tag
  defp ast_tag(tuple) when is_tuple(tuple) and tuple_size(tuple) > 0, do: elem(tuple, 0)
  defp ast_tag(_ast), do: nil

  @doc false
  @spec conservative_command_keys(binary(), [binary()]) :: [binary()]
  def conservative_command_keys(cmd, args) when cmd in @management_scoped_commands,
    do: management_command_keys(cmd, args)

  def conservative_command_keys(cmd, args), do: extra_command_keys(cmd, args)

  @single_key_tags ~w(
    get incr decr strlen getdel ttl pttl persist llen hgetall hkeys hvals hlen
    smembers scard zcard type expiretime pexpiretime
  )a

  @list_arg_tags ~w(
    del exists mget mset msetnx lpush rpush lpushx rpushx lpos hset hdel hmget
    sadd srem smismember sinter sunion sdiff sdiffstore sinterstore sunionstore
    zrem zmscore pfadd pfcount pfmerge publish pubsub
  )a

  @flow_option_specs %{
    create: [
      {"TYPE", :type, :binary},
      {"STATE", :state, :binary},
      {"PAYLOAD", :payload, :binary},
      {"PAYLOAD_REF", :payload_ref, {:ref, :payload_ref}},
      {"PARENT_FLOW_ID", :parent_flow_id, {:ref, :parent_flow_id}},
      {"ROOT_FLOW_ID", :root_flow_id, {:ref, :root_flow_id}},
      {"CORRELATION_ID", :correlation_id, {:ref, :correlation_id}},
      {"RUN_AT", :run_at_ms, :non_negative},
      {"NOW", :now_ms, :non_negative},
      {"PRIORITY", :priority, :non_negative},
      {"PARTITION", :partition_key, :partition},
      {"RETENTION_TTL", :retention_ttl_ms, {:positive, :retention_ttl_ms}},
      {"RETENTION_TTL_MS", :retention_ttl_ms, {:positive, :retention_ttl_ms}},
      {"MAX_ACTIVE_MS", :max_active_ms, :positive_or_infinity},
      {"HISTORY_HOT_MAX_EVENTS", :history_hot_max_events,
       {:non_negative_named, :history_hot_max_events}},
      {"HISTORY_MAX_EVENTS", :history_max_events, {:positive, :history_max_events}},
      {"IDEMPOTENT", :idempotent, :boolean},
      {"RETURN", :return, :binary}
    ],
    value_put: [
      {"PARTITION", :partition_key, :partition},
      {"OWNER_FLOW_ID", :owner_flow_id, {:ref, :owner_flow_id}},
      {"TTL", :ttl_ms, {:positive, :ttl_ms}},
      {"TTL_MS", :ttl_ms, {:positive, :ttl_ms}},
      {"NOW", :now_ms, :non_negative}
    ],
    signal: [
      {"PARTITION", :partition_key, :partition},
      {"SIGNAL", :signal, :binary},
      {"IDEMPOTENCY", :idempotency_key, :binary},
      {"IDEMPOTENCY_KEY", :idempotency_key, :binary},
      {"IF_STATE", :if_state, :binary},
      {"TRANSITION_TO", :transition_to, :binary},
      {"RUN_AT", :run_at_ms, :non_negative},
      {"NOW", :now_ms, :non_negative}
    ],
    spawn_children: [
      {"GROUP", :group_id, :binary},
      {"PARTITION", :partition_key, :partition},
      {"FENCING", :fencing_token, :non_negative},
      {"WAIT", :wait, :binary},
      {"ON_CHILD_FAILED", :on_child_failed, :binary},
      {"ON_PARENT_CLOSED", :on_parent_closed, :binary},
      {"SUCCESS", :success, :binary},
      {"FAILURE", :failure, :binary},
      {"FROM_STATE", :from_state, :binary},
      {"WAIT_STATE", :wait_state, :binary},
      {"LEASE_TOKEN", :lease_token, :binary},
      {"NOW", :now_ms, :non_negative},
      {"RETENTION_TTL", :retention_ttl_ms, {:positive, :retention_ttl_ms}},
      {"RETENTION_TTL_MS", :retention_ttl_ms, {:positive, :retention_ttl_ms}},
      {"MAX_ACTIVE_MS", :max_active_ms, :positive_or_infinity},
      {"HISTORY_HOT_MAX_EVENTS", :history_hot_max_events,
       {:non_negative_named, :history_hot_max_events}},
      {"HISTORY_MAX_EVENTS", :history_max_events, {:positive, :history_max_events}}
    ],
    policy_get: [
      {"STATE", :state, :binary}
    ],
    claim_due: [
      {"WORKER", :worker, :binary},
      {"STATE", :state, :binary},
      {"LEASE_MS", :lease_ms, {:positive, :lease_ms}},
      {"LIMIT", :limit, {:positive, :limit}},
      {"PRIORITY", :priority, :non_negative},
      {"NOW", :now_ms, :non_negative},
      {"BLOCK", :block_ms, :non_negative},
      {"PARTITION", :partition_key, :partition},
      {"RETURN", :return, :binary},
      {"RECLAIM_EXPIRED", :reclaim_expired, :boolean},
      {"RECLAIM_RATIO", :reclaim_ratio, :non_negative}
    ],
    terminal: [
      {"FENCING", :fencing_token, :non_negative},
      {"RESULT", :result, :binary},
      {"PAYLOAD", :payload, :binary},
      {"TTL", :ttl_ms, {:positive, :ttl_ms}},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition}
    ],
    extend_lease: [
      {"FENCING", :fencing_token, :non_negative},
      {"LEASE_MS", :lease_ms, {:positive, :lease_ms}},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition},
      {"RETURN", :return, :binary}
    ],
    transition: [
      {"FENCING", :fencing_token, :non_negative},
      {"LEASE_TOKEN", :lease_token, :binary},
      {"RUN_AT", :run_at_ms, :non_negative},
      {"PRIORITY", :priority, :non_negative},
      {"PAYLOAD", :payload, :binary},
      {"PAYLOAD_REF", :payload_ref, {:ref, :payload_ref}},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition}
    ],
    retry: [
      {"FENCING", :fencing_token, :non_negative},
      {"RUN_AT", :run_at_ms, :non_negative},
      {"ERROR", :error, :binary},
      {"PAYLOAD", :payload, :binary},
      {"MAX_RETRIES", :max_retries, :non_negative},
      {"EXHAUSTED_TO", :exhausted_to, :binary},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition}
    ],
    fail: [
      {"FENCING", :fencing_token, :non_negative},
      {"ERROR", :error, :binary},
      {"PAYLOAD", :payload, :binary},
      {"TTL", :ttl_ms, {:positive, :ttl_ms}},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition}
    ],
    cancel: [
      {"FENCING", :fencing_token, :non_negative},
      {"LEASE_TOKEN", :lease_token, :binary},
      {"REASON", :reason, :binary},
      {"REASON_REF", :reason_ref, {:ref, :reason_ref}},
      {"TTL", :ttl_ms, {:positive, :ttl_ms}},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition}
    ],
    rewind: [
      {"TO_EVENT", :to_event, :binary},
      {"RUN_AT", :run_at_ms, :non_negative},
      {"EXPECT_STATE", :expect_state, :binary},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition}
    ],
    many_terminal: [
      {"RESULT", :result, :binary},
      {"PAYLOAD", :payload, :binary},
      {"TTL", :ttl_ms, {:positive, :ttl_ms}},
      {"NOW", :now_ms, :non_negative},
      {"INDEPENDENT", :independent, :boolean},
      {"RETURN", :return, :binary}
    ],
    many_retry: [
      {"ERROR", :error, :binary},
      {"PAYLOAD", :payload, :binary},
      {"RUN_AT", :run_at_ms, :non_negative},
      {"MAX_RETRIES", :max_retries, :non_negative},
      {"EXHAUSTED_TO", :exhausted_to, :binary},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition},
      {"INDEPENDENT", :independent, :boolean}
    ],
    many_fail: [
      {"ERROR", :error, :binary},
      {"PAYLOAD", :payload, :binary},
      {"TTL", :ttl_ms, {:positive, :ttl_ms}},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition},
      {"INDEPENDENT", :independent, :boolean}
    ],
    many_cancel: [
      {"REASON", :reason, :binary},
      {"REASON_REF", :reason_ref, {:ref, :reason_ref}},
      {"TTL", :ttl_ms, {:positive, :ttl_ms}},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition},
      {"INDEPENDENT", :independent, :boolean}
    ],
    many_transition: [
      {"RUN_AT", :run_at_ms, :non_negative},
      {"PRIORITY", :priority, :non_negative},
      {"PAYLOAD", :payload, :binary},
      {"PAYLOAD_REF", :payload_ref, {:ref, :payload_ref}},
      {"NOW", :now_ms, :non_negative},
      {"INDEPENDENT", :independent, :boolean}
    ],
    list: [
      {"STATE", :state, :binary},
      {"COUNT", :count, {:positive, :count}},
      {"PARTITION", :partition_key, :partition},
      {"INCLUDE_COLD", :include_cold, :boolean},
      {"CONSISTENT_PROJECTION", :consistent_projection, :boolean}
    ],
    search: [
      {"TYPE", :type, :binary},
      {"STATE", :state, :binary},
      {"COUNT", :count, {:positive, :count}},
      {"PARTITION", :partition_key, :partition},
      {"FROM_MS", :from_ms, :non_negative},
      {"TO_MS", :to_ms, :non_negative},
      {"REV", :rev, :boolean},
      {"TERMINAL_ONLY", :terminal_only, :boolean},
      {"CONSISTENT_PROJECTION", :consistent_projection, :boolean}
    ],
    index_query: [
      {"COUNT", :count, {:positive, :count}},
      {"PARTITION", :partition_key, :partition},
      {"FROM_MS", :from_ms, :non_negative},
      {"TO_MS", :to_ms, :non_negative},
      {"REV", :rev, :boolean},
      {"STATE", :state, :binary},
      {"TERMINAL_ONLY", :terminal_only, :boolean},
      {"INCLUDE_COLD", :include_cold, :boolean},
      {"CONSISTENT_PROJECTION", :consistent_projection, :boolean}
    ],
    history: [
      {"COUNT", :count, {:positive, :count}},
      {"PARTITION", :partition_key, :partition},
      {"FROM_EVENT", :from_event, :binary},
      {"TO_EVENT", :to_event, :binary},
      {"FROM_MS", :from_ms, :non_negative},
      {"TO_MS", :to_ms, :non_negative},
      {"FROM_VERSION", :from_version, :non_negative},
      {"TO_VERSION", :to_version, :non_negative},
      {"REV", :rev, :boolean},
      {"EVENT", :event, :binary},
      {"WORKER", :worker, :binary},
      {"INCLUDE_COLD", :include_cold, :boolean},
      {"CONSISTENT_PROJECTION", :consistent_projection, :boolean},
      {"VALUES", :values, :boolean},
      {"PAYLOAD_MAX_BYTES", :payload_max_bytes, :non_negative},
      {"MAXBYTES", :payload_max_bytes, :non_negative}
    ],
    stuck: [
      {"COUNT", :count, {:positive, :count}},
      {"OLDER_THAN", :older_than_ms, :non_negative},
      {"NOW", :now_ms, :non_negative},
      {"PARTITION", :partition_key, :partition}
    ],
    retention_cleanup: [
      {"LIMIT", :limit, {:positive, :limit}},
      {"NOW", :now_ms, :non_negative}
    ],
    partition: [
      {"PARTITION", :partition_key, :partition},
      {"INCLUDE_COLD", :include_cold, :boolean},
      {"CONSISTENT_PROJECTION", :consistent_projection, :boolean}
    ]
  }

  @type result :: {:ok, binary(), [binary()], term(), [binary()]} | {:error, binary()}

  @spec parse(binary(), [term()]) :: result()
  def parse("", _args), do: {:error, "ERR unknown command '', with args beginning with: "}

  def parse(name, args) when is_binary(name) and is_list(args) do
    with {:ok, parsed_args} <- normalize_command_args(args) do
      cmd = String.upcase(name)
      parsed_args = normalize_scoped_management_subcommand(cmd, parsed_args)
      ast = make_ast(cmd, Map.get(@command_tags, cmd), parsed_args)
      keys = command_keys(cmd, parsed_args, ast)
      {:ok, cmd, parsed_args, ast, keys}
    end
  end

  def parse(_name, _args), do: {:error, "ERR protocol error"}

  defp make_ast(cmd, nil, args), do: {:unknown, cmd, args}
  defp make_ast("PING", :ping, []), do: :ping
  defp make_ast("PING", :ping, [arg]), do: {:ping, arg}
  defp make_ast("PING", :ping, args), do: {:ping, args}
  defp make_ast("ECHO", :echo, args), do: {:echo, args}

  defp make_ast("HELLO", :hello, []), do: :hello
  defp make_ast("HELLO", :hello, ["3"]), do: {:hello, 3}

  defp make_ast("HELLO", :hello, [_version | _]),
    do: {:hello, {:error, "NOPROTO this server does not support the requested protocol version"}}

  defp make_ast("AUTH", :auth, [password]), do: {:auth, "default", password}
  defp make_ast("AUTH", :auth, [username, password]), do: {:auth, username, password}

  defp make_ast("AUTH", :auth, _),
    do: {:auth, {:error, "ERR wrong number of arguments for 'auth' command"}}

  defp make_ast("ACL", :acl, [subcmd | rest]), do: {:acl, String.upcase(subcmd), rest}

  defp make_ast("ACL", :acl, _),
    do: {:acl, {:error, "ERR wrong number of arguments for 'acl' command"}}

  defp make_ast("CLIENT", :client, ["hello", "3"]), do: {:hello, 3}
  defp make_ast("CLIENT", :client, ["HELLO", "3"]), do: {:hello, 3}
  defp make_ast("CLIENT", :client, [subcmd | rest]), do: {:client, String.upcase(subcmd), rest}

  defp make_ast("CLIENT", :client, _),
    do: {:client, {:error, "ERR wrong number of arguments for 'client' command"}}

  defp make_ast("SANDBOX", :sandbox, [subcmd | rest]), do: {:sandbox, String.upcase(subcmd), rest}

  defp make_ast("SANDBOX", :sandbox, _),
    do: {:sandbox, {:error, "ERR wrong number of arguments for 'sandbox' command"}}

  defp make_ast("QUIT", :quit, []), do: :quit

  defp make_ast("QUIT", :quit, _),
    do: {:quit, {:error, "ERR wrong number of arguments for 'quit' command"}}

  defp make_ast("RESET", :reset, []), do: :reset

  defp make_ast("RESET", :reset, _),
    do: {:reset, {:error, "ERR wrong number of arguments for 'reset' command"}}

  defp make_ast("MULTI", :multi, []), do: :multi

  defp make_ast("MULTI", :multi, _),
    do: {:multi, {:error, "ERR wrong number of arguments for 'multi' command"}}

  defp make_ast("EXEC", :exec, []), do: :exec

  defp make_ast("EXEC", :exec, _),
    do: {:exec, {:error, "ERR wrong number of arguments for 'exec' command"}}

  defp make_ast("DISCARD", :discard, []), do: :discard

  defp make_ast("DISCARD", :discard, _),
    do: {:discard, {:error, "ERR wrong number of arguments for 'discard' command"}}

  defp make_ast("WATCH", :watch, []),
    do: {:watch, {:error, "ERR wrong number of arguments for 'watch' command"}}

  defp make_ast("WATCH", :watch, keys), do: {:watch, keys}
  defp make_ast("UNWATCH", :unwatch, []), do: :unwatch

  defp make_ast("UNWATCH", :unwatch, _),
    do: {:unwatch, {:error, "ERR wrong number of arguments for 'unwatch' command"}}

  defp make_ast("SUBSCRIBE", :subscribe, []),
    do: {:subscribe, {:error, "ERR wrong number of arguments for 'subscribe' command"}}

  defp make_ast("SUBSCRIBE", :subscribe, channels), do: {:subscribe, channels}
  defp make_ast("UNSUBSCRIBE", :unsubscribe, channels), do: {:unsubscribe, channels}

  defp make_ast("PSUBSCRIBE", :psubscribe, []),
    do: {:psubscribe, {:error, "ERR wrong number of arguments for 'psubscribe' command"}}

  defp make_ast("PSUBSCRIBE", :psubscribe, patterns), do: {:psubscribe, patterns}
  defp make_ast("PUNSUBSCRIBE", :punsubscribe, patterns), do: {:punsubscribe, patterns}

  defp make_ast("BLPOP", :blpop, args), do: parse_blocking_pop(:blpop, args)
  defp make_ast("BRPOP", :brpop, args), do: parse_blocking_pop(:brpop, args)

  defp make_ast("BLMOVE", :blmove, [source, destination, from, to, timeout]) do
    from_dir = parse_direction(from)
    to_dir = parse_direction(to)
    timeout_ms = parse_timeout_ms(timeout)

    cond do
      match?({:error, _}, from_dir) -> {:blmove, from_dir}
      match?({:error, _}, to_dir) -> {:blmove, to_dir}
      match?({:error, _}, timeout_ms) -> {:blmove, timeout_ms}
      true -> {:blmove, source, destination, from_dir, to_dir, timeout_ms}
    end
  end

  defp make_ast("BLMOVE", :blmove, _args),
    do: {:blmove, {:error, "ERR wrong number of arguments for 'blmove' command"}}

  defp make_ast("BLMPOP", :blmpop, args), do: parse_blmpop(args)

  defp make_ast(_cmd, tag, [key]) when tag in @single_key_tags, do: {tag, key}
  defp make_ast(_cmd, tag, args) when tag in @list_arg_tags, do: {tag, args}

  defp make_ast(_cmd, :set, [key, value]), do: {:set, key, value}
  defp make_ast(_cmd, :set, [key, value | opts]), do: {:set, key, value, parse_set_opts(opts)}

  defp make_ast(_cmd, tag, [key, value]) when tag in ~w(append getset setnx)a,
    do: {tag, key, value}

  defp make_ast(_cmd, tag, [key, amount]) when tag in ~w(incrby decrby setex psetex)a,
    do: {tag, key, parse_int(amount)}

  defp make_ast(_cmd, tag, [key, amount, value]) when tag in ~w(setex psetex)a,
    do: {tag, key, parse_int(amount), value}

  defp make_ast(_cmd, :incrbyfloat, [key, amount]), do: {:incrbyfloat, key, parse_float(amount)}

  defp make_ast(_cmd, :getrange, [key, start, stop]),
    do: {:getrange, key, parse_int(start), parse_int(stop)}

  defp make_ast(_cmd, :setrange, [key, offset, value]),
    do: {:setrange, key, parse_int(offset), value}

  defp make_ast(_cmd, :getex, [key]), do: {:getex, key}
  defp make_ast(_cmd, :getex, [key | opts]), do: {:getex, key, parse_getex_opts(opts)}

  defp make_ast(_cmd, tag, [key, ttl]) when tag in ~w(expire pexpire expireat pexpireat)a,
    do: {tag, key, parse_int(ttl)}

  defp make_ast(_cmd, tag, [key, ttl, flag]) when tag in ~w(expire pexpire expireat pexpireat)a,
    do: {tag, key, parse_int(ttl), parse_expiry_flag(flag)}

  defp make_ast(_cmd, tag, [key]) when tag in ~w(lpop rpop)a, do: {tag, key}

  defp make_ast(_cmd, tag, [key, count]) when tag in ~w(lpop rpop)a,
    do: {tag, key, parse_int(count)}

  defp make_ast(_cmd, tag, [key, start, stop]) when tag in ~w(lrange ltrim)a,
    do: {tag, key, parse_int(start), parse_int(stop)}

  defp make_ast(_cmd, :lindex, [key, index]), do: {:lindex, key, parse_int(index)}
  defp make_ast(_cmd, :lset, [key, index, element]), do: {:lset, key, parse_int(index), element}
  defp make_ast(_cmd, :lrem, [key, count, element]), do: {:lrem, key, parse_int(count), element}

  defp make_ast(_cmd, :linsert, [key, dir, pivot, element]),
    do: {:linsert, key, parse_direction(dir), pivot, element}

  defp make_ast(_cmd, :lmove, [source, destination, from, to]),
    do: {:lmove, source, destination, parse_direction(from), parse_direction(to)}

  defp make_ast(_cmd, :rpoplpush, [source, destination]), do: {:rpoplpush, source, destination}

  defp make_ast(_cmd, tag, [key, field]) when tag in ~w(hget hexists hstrlen)a,
    do: {tag, key, field}

  defp make_ast(_cmd, tag, [key, field, value]) when tag in ~w(hsetnx)a,
    do: {tag, key, field, value}

  defp make_ast(_cmd, :hincrby, [key, field, amount]),
    do: {:hincrby, key, field, parse_int(amount)}

  defp make_ast(_cmd, :hincrbyfloat, [key, field, amount]),
    do: {:hincrbyfloat, key, field, parse_float(amount)}

  defp make_ast(_cmd, :hrandfield, [key]), do: {:hrandfield, key}
  defp make_ast(_cmd, :hrandfield, [key, count]), do: {:hrandfield, key, parse_int(count)}

  defp make_ast(_cmd, :hrandfield, [key, count, withvalues]),
    do: {:hrandfield, key, parse_int(count), parse_withvalues(withvalues)}

  defp make_ast(_cmd, :hscan, [key, cursor | opts]),
    do: {:hscan, key, parse_scan_cursor(cursor), parse_collection_scan_opts(opts)}

  defp make_ast(_cmd, tag, [key, ttl, option, count | fields])
       when tag in ~w(hexpire hpexpire)a do
    if String.upcase(option) == "FIELDS" do
      parse_hash_expire(tag, key, ttl, count, fields)
    else
      {tag, key, {:error, "ERR syntax error"}, []}
    end
  end

  defp make_ast(_cmd, tag, [key, option, count | fields])
       when tag in ~w(httl hpttl hpersist hexpiretime hgetdel)a do
    if String.upcase(option) == "FIELDS" do
      parse_hash_field_list(tag, key, count, fields)
    else
      {tag, key, {:error, "ERR syntax error"}}
    end
  end

  defp make_ast(_cmd, :hgetex, [key | opts]), do: parse_hgetex(key, opts)

  defp make_ast(_cmd, :hsetex, [key, seconds | field_value_pairs]),
    do: {:hsetex, key, parse_pos_int(seconds), field_value_pairs}

  defp make_ast(_cmd, :sismember, [key, member]), do: {:sismember, key, member}

  defp make_ast(_cmd, tag, [key]) when tag in ~w(srandmember spop)a, do: {tag, key}

  defp make_ast(_cmd, tag, [key, count]) when tag in ~w(srandmember spop)a,
    do: {tag, key, parse_int(count)}

  defp make_ast(_cmd, :smove, [source, destination, member]),
    do: {:smove, source, destination, member}

  defp make_ast(_cmd, :sscan, [key, cursor | opts]),
    do: {:sscan, key, parse_scan_cursor(cursor), parse_collection_scan_opts(opts)}

  defp make_ast(_cmd, :sintercard, args), do: parse_sintercard(args)

  defp make_ast(_cmd, :zadd, [key | rest]), do: parse_zadd(key, rest)

  defp make_ast(_cmd, :zincrby, [key, amount, member]),
    do: {:zincrby, key, parse_float(amount), member}

  defp make_ast(_cmd, tag, [key, member]) when tag in ~w(zscore zrank zrevrank)a,
    do: {tag, key, member}

  defp make_ast(_cmd, tag, [key, count]) when tag in ~w(zpopmin zpopmax)a,
    do: {tag, key, parse_int(count)}

  defp make_ast(_cmd, :zrandmember, [key, count]),
    do: {:zrandmember, key, parse_int(count), false}

  defp make_ast(_cmd, :zrandmember, [key, count, option]) do
    if String.upcase(option) == "WITHSCORES",
      do: {:zrandmember, key, parse_int(count), true},
      else: {:zrandmember, key, parse_int(count), {:error, "ERR syntax error"}}
  end

  defp make_ast(_cmd, tag, [key]) when tag in ~w(zpopmin zpopmax zrandmember)a, do: {tag, key}

  defp make_ast(_cmd, tag, [key, min, max])
       when tag in ~w(zcount zrangebyscore zrevrangebyscore)a, do: {tag, key, min, max}

  defp make_ast(_cmd, tag, [key, min, max | opts])
       when tag in ~w(zrangebyscore zrevrangebyscore)a, do: {tag, key, min, max, opts}

  defp make_ast(_cmd, tag, [key, cursor | opts]) when tag in ~w(zscan)a,
    do: {tag, key, parse_scan_cursor(cursor), parse_collection_scan_opts(opts)}

  defp make_ast(_cmd, tag, [key, start, stop]) when tag in ~w(zrange zrevrange)a,
    do: {tag, key, parse_int(start), parse_int(stop), false}

  defp make_ast(_cmd, tag, [key, start, stop, option]) when tag in ~w(zrange zrevrange)a do
    with_scores =
      if String.upcase(option) == "WITHSCORES",
        do: true,
        else: {:error, "ERR syntax error"}

    {tag, key, parse_int(start), parse_int(stop), with_scores}
  end

  defp make_ast("MEMORY", :memory, [subcmd | rest]), do: {:memory, [String.upcase(subcmd) | rest]}
  defp make_ast("MEMORY", :memory, []), do: {:memory, []}

  defp make_ast(_cmd, :getbit, [key, offset]), do: {:getbit, key, parse_int(offset)}

  defp make_ast(_cmd, :setbit, [key, offset, bit]),
    do: {:setbit, key, parse_int(offset), parse_int(bit)}

  defp make_ast(_cmd, :bitcount, [key]), do: {:bitcount, key}

  defp make_ast(_cmd, :bitcount, [key, start, stop]),
    do: {:bitcount, key, parse_int(start), parse_int(stop)}

  defp make_ast(_cmd, :bitpos, [key, bit]), do: {:bitpos, key, parse_int(bit), :all}

  defp make_ast(_cmd, :bitpos, [key, bit, start]),
    do: {:bitpos, key, parse_int(bit), {:start, parse_int(start)}}

  defp make_ast(_cmd, :bitpos, [key, bit, start, stop]),
    do: {:bitpos, key, parse_int(bit), {parse_int(start), parse_int(stop), :byte}}

  defp make_ast(_cmd, :bitop, [operation, destination | sources]),
    do: parse_bitop(operation, destination, sources)

  defp make_ast(_cmd, :bitop, args), do: {:bitop, args}

  defp make_ast(_cmd, :unlink, args), do: {:unlink, args}
  defp make_ast(_cmd, tag, [key, newkey]) when tag in ~w(rename renamenx)a, do: {tag, key, newkey}

  defp make_ast(_cmd, :copy, [source, destination | opts]),
    do: {:copy, source, destination, parse_copy_opts(opts)}

  defp make_ast(_cmd, :randomkey, []), do: {:randomkey, []}
  defp make_ast(_cmd, :scan, [cursor | opts]), do: {:scan, cursor, parse_scan_opts(opts)}
  defp make_ast(_cmd, :object, args), do: parse_object(args)

  defp make_ast(_cmd, :wait, [replicas, timeout]),
    do: {:wait, parse_int(replicas), parse_int(timeout)}

  defp make_ast("FLOW.CREATE", :flow_create, [id | opts]),
    do: {:flow_create, id, parse_flow_options(opts, spec(:create))}

  defp make_ast("FLOW.VALUE.PUT", :flow_value_put, [value | opts]),
    do: {:flow_value_put, value, parse_flow_options(opts, spec(:value_put))}

  defp make_ast("FLOW.SIGNAL", :flow_signal, [id | opts]),
    do: {:flow_signal, id, parse_flow_signal_opts(opts)}

  defp make_ast("FLOW.SPAWN_CHILDREN", :flow_spawn_children, args),
    do: parse_flow_spawn_children(args)

  defp make_ast("FLOW.GET", :flow_get, [id | opts]),
    do: {:flow_get, id, parse_flow_read_options(opts, spec(:partition))}

  defp make_ast("FLOW.POLICY.SET", :flow_policy_set, [type | opts]),
    do: {:flow_policy_set, type, parse_flow_policy_set_options(opts)}

  defp make_ast("FLOW.POLICY.GET", :flow_policy_get, [type | opts]),
    do: {:flow_policy_get, type, parse_flow_options(opts, spec(:policy_get))}

  defp make_ast("FLOW.CLAIM_DUE", :flow_claim_due, [type | opts]),
    do: {:flow_claim_due, type, parse_flow_read_options(opts, spec(:claim_due))}

  defp make_ast("FLOW.RECLAIM", :flow_reclaim, [type | opts]),
    do: {:flow_reclaim, type, parse_flow_read_options(opts, spec(:claim_due))}

  defp make_ast("FLOW.EXTEND_LEASE", :flow_extend_lease, [id, lease | opts]),
    do: {:flow_extend_lease, id, lease, parse_flow_options(opts, spec(:extend_lease))}

  defp make_ast("FLOW.COMPLETE", :flow_complete, [id, lease | opts]),
    do: {:flow_complete, id, lease, parse_flow_options(opts, spec(:terminal))}

  defp make_ast("FLOW.TRANSITION", :flow_transition, [id, from, to | opts]),
    do: {:flow_transition, id, from, to, parse_flow_options(opts, spec(:transition))}

  defp make_ast("FLOW.RETRY", :flow_retry, [id, lease | opts]),
    do: {:flow_retry, id, lease, parse_flow_retry_options(opts, spec(:retry))}

  defp make_ast("FLOW.FAIL", :flow_fail, [id, lease | opts]),
    do: {:flow_fail, id, lease, parse_flow_options(opts, spec(:fail))}

  defp make_ast("FLOW.CANCEL", :flow_cancel, [id | opts]),
    do: {:flow_cancel, id, parse_flow_options(opts, spec(:cancel))}

  defp make_ast("FLOW.REWIND", :flow_rewind, [id | opts]),
    do: {:flow_rewind, id, parse_flow_options(opts, spec(:rewind))}

  defp make_ast("FLOW.LIST", :flow_list, [type | opts]),
    do: {:flow_list, type, parse_flow_options(opts, spec(:list))}

  defp make_ast("FLOW.SEARCH", :flow_search, opts),
    do: {:flow_search, parse_flow_search_options(opts)}

  defp make_ast("FLOW.QUERY", :flow_query, [version, query | params]) do
    case parse_flow_query_params(params) do
      {:ok, parsed_params} -> {:flow_query, version, query, parsed_params}
      {:error, reason} -> {:flow_query, {:error, reason}}
    end
  end

  defp make_ast("FLOW.QUERY", :flow_query, _args),
    do: {:flow_query, {:error, "ERR wrong number of arguments for 'flow.query' command"}}

  defp make_ast("FLOW.ATTRIBUTES", :flow_attributes, [type | opts]),
    do: {:flow_attributes, type, parse_flow_options(opts, spec(:index_query))}

  defp make_ast("FLOW.ATTRIBUTE_VALUES", :flow_attribute_values, [type, attr | opts]),
    do: {:flow_attribute_values, type, attr, parse_flow_options(opts, spec(:index_query))}

  defp make_ast("FLOW.STATS", :flow_stats, [type | opts]),
    do: {:flow_stats, type, parse_flow_options(opts, spec(:list))}

  defp make_ast("FLOW.TERMINALS", :flow_terminals, [type | opts]),
    do: {:flow_terminals, type, parse_flow_options(opts, spec(:index_query))}

  defp make_ast("FLOW.FAILURES", :flow_failures, [type | opts]),
    do: {:flow_failures, type, parse_flow_options(opts, spec(:index_query))}

  defp make_ast("FLOW.BY_PARENT", :flow_by_parent, [id | opts]),
    do: {:flow_by_parent, id, parse_flow_options(opts, spec(:index_query))}

  defp make_ast("FLOW.BY_ROOT", :flow_by_root, [id | opts]),
    do: {:flow_by_root, id, parse_flow_options(opts, spec(:index_query))}

  defp make_ast("FLOW.BY_CORRELATION", :flow_by_correlation, [id | opts]),
    do: {:flow_by_correlation, id, parse_flow_options(opts, spec(:index_query))}

  defp make_ast("FLOW.INFO", :flow_info, [type | opts]),
    do: {:flow_info, type, parse_flow_options(opts, spec(:partition))}

  defp make_ast("FLOW.STUCK", :flow_stuck, [type | opts]),
    do: {:flow_stuck, type, parse_flow_options(opts, spec(:stuck))}

  defp make_ast("FLOW.HISTORY", :flow_history, [id | opts]),
    do: {:flow_history, id, parse_flow_options(opts, spec(:history))}

  defp make_ast("FLOW.RETENTION_CLEANUP", :flow_retention_cleanup, opts),
    do: {:flow_retention_cleanup, parse_flow_options(opts, spec(:retention_cleanup))}

  defp make_ast("FLOW.CREATE_MANY", :flow_create_many, args), do: parse_flow_create_many(args)

  defp make_ast("FLOW.COMPLETE_MANY", :flow_complete_many, args),
    do: parse_flow_terminal_many(:flow_complete_many, args, spec(:many_terminal))

  defp make_ast("FLOW.RETRY_MANY", :flow_retry_many, args),
    do: parse_flow_terminal_many(:flow_retry_many, args, spec(:many_retry))

  defp make_ast("FLOW.FAIL_MANY", :flow_fail_many, args),
    do: parse_flow_terminal_many(:flow_fail_many, args, spec(:many_fail))

  defp make_ast("FLOW.CANCEL_MANY", :flow_cancel_many, args), do: parse_flow_cancel_many(args)

  defp make_ast("FLOW.TRANSITION_MANY", :flow_transition_many, args),
    do: parse_flow_transition_many(args)

  defp make_ast("BF.RESERVE", :bf_reserve, [key, error_rate, capacity]),
    do:
      with_parsed_numbers(:bf_reserve, [parse_float(error_rate), parse_pos_int(capacity)], fn [
                                                                                                parsed_error_rate,
                                                                                                parsed_capacity
                                                                                              ] ->
        {:bf_reserve, key, parsed_error_rate, parsed_capacity}
      end)

  defp make_ast("BF.RESERVE", :bf_reserve, _args),
    do: {:bf_reserve, {:error, "ERR wrong number of arguments for 'bf.reserve' command"}}

  defp make_ast("CF.RESERVE", :cf_reserve, [key, capacity]),
    do:
      with_parsed_numbers(:cf_reserve, [parse_pos_int(capacity)], fn [parsed_capacity] ->
        {:cf_reserve, key, parsed_capacity}
      end)

  defp make_ast("CF.RESERVE", :cf_reserve, _args),
    do: {:cf_reserve, {:error, "ERR wrong number of arguments for 'cf.reserve' command"}}

  defp make_ast("CMS.INITBYDIM", :cms_initbydim, [key, width, depth]),
    do:
      with_parsed_numbers(:cms_initbydim, [parse_pos_int(width), parse_pos_int(depth)], fn [
                                                                                             parsed_width,
                                                                                             parsed_depth
                                                                                           ] ->
        {:cms_initbydim, key, parsed_width, parsed_depth}
      end)

  defp make_ast("CMS.INITBYDIM", :cms_initbydim, _args),
    do: {:cms_initbydim, {:error, "ERR wrong number of arguments for 'cms.initbydim' command"}}

  defp make_ast("CMS.INITBYPROB", :cms_initbyprob, [key, error, prob]),
    do:
      with_parsed_numbers(
        :cms_initbyprob,
        [parse_pos_float(error), parse_probability_float(prob)],
        fn [
             parsed_error,
             parsed_prob
           ] ->
          {:cms_initbyprob, key, parsed_error, parsed_prob}
        end
      )

  defp make_ast("CMS.INITBYPROB", :cms_initbyprob, _args),
    do: {:cms_initbyprob, {:error, "ERR wrong number of arguments for 'cms.initbyprob' command"}}

  defp make_ast("CMS.INCRBY", :cms_incrby, [key | rest]) when rest != [] do
    case parse_element_count_pairs(:cms_incrby, rest) do
      {:ok, pairs} -> {:cms_incrby, key, pairs}
      {:error, msg} -> {:cms_incrby, key, {:error, msg}}
    end
  end

  defp make_ast("CMS.INCRBY", :cms_incrby, _args),
    do: {:cms_incrby, {:error, "ERR wrong number of arguments for 'cms.incrby' command"}}

  defp make_ast("CMS.MERGE", :cms_merge, [dst, count | rest]) do
    with parsed_count when is_integer(parsed_count) and parsed_count > 0 <- parse_int(count),
         {:ok, src_keys, weights} <- parse_cms_merge_args(rest, parsed_count) do
      {:cms_merge, dst, src_keys, weights}
    else
      {:error, msg} -> {:cms_merge, {:error, msg}}
      _ -> {:cms_merge, {:error, "ERR numkeys must be a positive integer"}}
    end
  end

  defp make_ast("CMS.MERGE", :cms_merge, _args),
    do: {:cms_merge, {:error, "ERR wrong number of arguments for 'cms.merge' command"}}

  defp make_ast("TOPK.RESERVE", :topk_reserve, [key, k]),
    do:
      with_parsed_numbers(:topk_reserve, [parse_pos_int(k)], fn [parsed_k] ->
        {:topk_reserve, key, parsed_k, 8, 7}
      end)

  defp make_ast("TOPK.RESERVE", :topk_reserve, [key, k, width, depth]),
    do:
      with_parsed_numbers(
        :topk_reserve,
        [
          parse_pos_int(k),
          parse_pos_int(width),
          parse_pos_int(depth)
        ],
        fn [parsed_k, parsed_width, parsed_depth] ->
          {:topk_reserve, key, parsed_k, parsed_width, parsed_depth}
        end
      )

  defp make_ast("TOPK.RESERVE", :topk_reserve, _args),
    do: {:topk_reserve, {:error, "ERR wrong number of arguments for 'topk.reserve' command"}}

  defp make_ast("TOPK.INCRBY", :topk_incrby, [key | rest]) when rest != [] do
    case parse_element_count_pairs(:topk_incrby, rest) do
      {:ok, pairs} -> {:topk_incrby, key, pairs}
      {:error, msg} -> {:topk_incrby, key, {:error, msg}}
    end
  end

  defp make_ast("TOPK.INCRBY", :topk_incrby, _args),
    do: {:topk_incrby, {:error, "ERR wrong number of arguments for 'topk.incrby' command"}}

  defp make_ast("TOPK.LIST", :topk_list, [key]), do: {:topk_list, key, false}

  defp make_ast("TOPK.LIST", :topk_list, [key, option]) do
    if String.upcase(option) == "WITHCOUNT",
      do: {:topk_list, key, true},
      else: {:topk_list, {:error, "ERR syntax error"}}
  end

  defp make_ast("TOPK.LIST", :topk_list, _args),
    do: {:topk_list, {:error, "ERR wrong number of arguments for 'topk.list' command"}}

  defp make_ast("TDIGEST.CREATE", :tdigest_create, [key]), do: {:tdigest_create, key, nil}

  defp make_ast("TDIGEST.CREATE", :tdigest_create, [key, option, compression]) do
    if String.upcase(option) == "COMPRESSION" do
      with_parsed_numbers(:tdigest_create, [parse_pos_int(compression)], fn [parsed_compression] ->
        {:tdigest_create, key, parsed_compression}
      end)
    else
      {:tdigest_create, {:error, "ERR syntax error"}}
    end
  end

  defp make_ast("TDIGEST.CREATE", :tdigest_create, _args),
    do: {:tdigest_create, {:error, "ERR wrong number of arguments for 'tdigest.create' command"}}

  defp make_ast("TDIGEST.ADD", :tdigest_add, [key | values]) when values != [] do
    case parse_float_list(values) do
      {:ok, floats} -> {:tdigest_add, key, floats}
      {:error, msg} -> {:tdigest_add, key, {:error, msg}}
    end
  end

  defp make_ast("TDIGEST.ADD", :tdigest_add, _args),
    do: {:tdigest_add, {:error, "ERR wrong number of arguments for 'tdigest.add' command"}}

  defp make_ast("TDIGEST.RESET", :tdigest_reset, [key]), do: {:tdigest_reset, [key]}

  defp make_ast("TDIGEST.RESET", :tdigest_reset, _args),
    do: {:tdigest_reset, {:error, "ERR wrong number of arguments for 'tdigest.reset' command"}}

  defp make_ast("TDIGEST.QUANTILE", :tdigest_quantile, [key | values]) when values != [] do
    case parse_list(values, &parse_probability_inclusive_float/1) do
      {:ok, quantiles} -> {:tdigest_quantile, key, quantiles}
      {:error, msg} -> {:tdigest_quantile, key, {:error, msg}}
    end
  end

  defp make_ast(_cmd, tag, [key | values])
       when tag in ~w(tdigest_cdf tdigest_rank tdigest_revrank)a and values != [] do
    case parse_float_list(values) do
      {:ok, floats} -> {tag, key, floats}
      {:error, msg} -> {tag, key, {:error, msg}}
    end
  end

  defp make_ast(_cmd, tag, _args)
       when tag in ~w(tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank)a,
       do: {tag, {:error, "ERR wrong number of arguments for 'tdigest' command"}}

  defp make_ast(_cmd, tag, [key | ranks])
       when tag in ~w(tdigest_byrank tdigest_byrevrank)a and ranks != [] do
    case parse_int_list(ranks) do
      {:ok, parsed_ranks} -> {tag, key, parsed_ranks}
      {:error, msg} -> {tag, key, {:error, msg}}
    end
  end

  defp make_ast(_cmd, tag, _args) when tag in ~w(tdigest_byrank tdigest_byrevrank)a,
    do: {tag, {:error, "ERR wrong number of arguments for 'tdigest' command"}}

  defp make_ast("TDIGEST.TRIMMED_MEAN", :tdigest_trimmed_mean, [key, low, high]),
    do: make_tdigest_trimmed_mean_ast(key, parse_float(low), parse_float(high))

  defp make_ast("TDIGEST.TRIMMED_MEAN", :tdigest_trimmed_mean, _args),
    do:
      {:tdigest_trimmed_mean,
       {:error, "ERR wrong number of arguments for 'tdigest.trimmed_mean' command"}}

  defp make_ast(_cmd, tag, [key]) when tag in ~w(tdigest_min tdigest_max tdigest_info)a,
    do: {tag, [key]}

  defp make_ast(_cmd, tag, _args) when tag in ~w(tdigest_min tdigest_max tdigest_info)a,
    do: {tag, {:error, "ERR wrong number of arguments for 'tdigest' command"}}

  defp make_ast("TDIGEST.MERGE", :tdigest_merge, [dest, count | rest]) do
    with parsed_count when is_integer(parsed_count) and parsed_count > 0 <- parse_int(count),
         {:ok, src_keys, opts} <- parse_tdigest_merge_args(rest, parsed_count) do
      {:tdigest_merge, dest, src_keys, opts}
    else
      {:error, msg} -> {:tdigest_merge, {:error, msg}}
      _ -> {:tdigest_merge, {:error, "ERR numkeys must be a positive integer"}}
    end
  end

  defp make_ast("TDIGEST.MERGE", :tdigest_merge, _args),
    do: {:tdigest_merge, {:error, "ERR wrong number of arguments for 'tdigest.merge' command"}}

  defp make_ast(_cmd, tag, args), do: {tag, args}

  defp command_keys(_cmd, _args, {:unknown, _unknown_command, _unknown_args}), do: []

  defp command_keys("FLOW." <> _rest, _args, ast) do
    case flow_ast_keys(ast) do
      keys when is_list(keys) and keys != [] -> Enum.uniq(keys)
      _unclassified_or_invalid -> ["*"]
    end
  end

  defp command_keys(cmd, args, _ast), do: command_keys(cmd, args)

  defp command_keys(cmd, args) when cmd in @management_scoped_commands do
    management_command_keys(cmd, args)
  end

  defp command_keys(cmd, args) do
    case Catalog.get_keys_upper(cmd, args) do
      {:ok, keys} -> keys
      {:error, _} -> static_command_keys(cmd, args)
    end
  end

  defp flow_ast_keys({:flow_value_put, _value, opts}) when is_list(opts) do
    case Keyword.get(opts, :partition_key) do
      partition when is_binary(partition) and partition != "" ->
        [flow_acl_partition_key(partition)]

      _none ->
        flow_nonempty_key(Keyword.get(opts, :owner_flow_id))
    end
  end

  defp flow_ast_keys({tag, _id, opts})
       when tag in [:flow_get, :flow_history] and is_list(opts),
       do: [flow_partition_or_global(opts)]

  defp flow_ast_keys({tag, id, opts})
       when tag in [
              :flow_create,
              :flow_signal,
              :flow_cancel,
              :flow_rewind
            ] and is_binary(id) and is_list(opts),
       do: [flow_partition_or_id(opts, id)]

  defp flow_ast_keys({tag, id, _token, opts})
       when tag in [:flow_extend_lease, :flow_complete, :flow_retry, :flow_fail] and
              is_binary(id) and is_list(opts),
       do: [flow_partition_or_id(opts, id)]

  defp flow_ast_keys({:flow_transition, id, _from, _to, opts})
       when is_binary(id) and is_list(opts),
       do: [flow_partition_or_id(opts, id)]

  defp flow_ast_keys({tag, type, opts})
       when tag in [:flow_policy_set, :flow_policy_get] and is_binary(type) and is_list(opts),
       do: [type]

  defp flow_ast_keys({tag, type, _invalid_opts})
       when tag in [:flow_policy_set, :flow_policy_get] and is_binary(type),
       do: [type]

  defp flow_ast_keys({tag, _selector, opts})
       when tag in [:flow_claim_due, :flow_reclaim] and is_list(opts),
       do: flow_partition_query_keys(opts, :claim)

  defp flow_ast_keys({tag, _selector, opts})
       when tag in [
              :flow_list,
              :flow_attributes,
              :flow_stats,
              :flow_terminals,
              :flow_failures,
              :flow_info,
              :flow_stuck,
              :flow_by_parent,
              :flow_by_root,
              :flow_by_correlation
            ] and is_list(opts),
       do: flow_partition_query_keys(opts, :query)

  defp flow_ast_keys({:flow_attribute_values, _type, _attribute, opts}) when is_list(opts),
    do: flow_partition_query_keys(opts, :query)

  defp flow_ast_keys({:flow_search, opts}) when is_list(opts),
    do: flow_partition_query_keys(opts, :query)

  defp flow_ast_keys({:flow_retention_cleanup, opts}) when is_list(opts), do: ["*"]

  defp flow_ast_keys({tag, partition_key, items, opts})
       when tag in [
              :flow_create_many,
              :flow_complete_many,
              :flow_retry_many,
              :flow_fail_many,
              :flow_cancel_many
            ] and is_list(items) and is_list(opts),
       do: flow_batch_ast_keys(partition_key, items)

  defp flow_ast_keys({:flow_transition_many, partition_key, _from, _to, items, opts})
       when is_list(items) and is_list(opts),
       do: flow_batch_ast_keys(partition_key, items)

  defp flow_ast_keys({:flow_spawn_children, parent_id, children, opts})
       when is_binary(parent_id) and is_list(children) and is_list(opts) do
    parent_key = flow_partition_or_id(opts, parent_id)

    child_keys =
      children
      |> Enum.map(&flow_item_partition/1)
      |> Enum.reject(&is_nil/1)

    [parent_key | child_keys]
  end

  defp flow_ast_keys({tag, [_opaque_id | _args]})
       when tag in [
              :flow_schedule_get,
              :flow_schedule_delete,
              :flow_schedule_fire,
              :flow_schedule_pause,
              :flow_schedule_resume,
              :flow_approval_approve,
              :flow_approval_reject,
              :flow_approval_get
            ],
       do: ["*"]

  defp flow_ast_keys({tag, [key | _args]})
       when tag in [
              :flow_step_continue,
              :flow_start_and_claim,
              :flow_effect_reserve,
              :flow_effect_confirm,
              :flow_effect_fail,
              :flow_effect_compensate,
              :flow_effect_get,
              :flow_governance_ledger,
              :flow_circuit_open,
              :flow_circuit_close,
              :flow_circuit_get,
              :flow_budget_reserve,
              :flow_budget_commit,
              :flow_budget_release,
              :flow_budget_get,
              :flow_limit_lease,
              :flow_limit_spend,
              :flow_limit_release,
              :flow_limit_get
            ] and is_binary(key) and key != "",
       do: [key]

  defp flow_ast_keys({tag, _args})
       when tag in [
              :flow_schedule_fire_due,
              :flow_schedule_list,
              :flow_approval_list,
              :flow_governance_overview,
              :flow_budget_list,
              :flow_limit_list
            ],
       do: ["*"]

  defp flow_ast_keys(_ast), do: nil

  defp flow_partition_or_id(opts, id) do
    case Keyword.get(opts, :partition_key) do
      partition when is_binary(partition) and partition != "" -> flow_acl_partition_key(partition)
      _none -> id
    end
  end

  defp flow_partition_or_global(opts) do
    case Keyword.get(opts, :partition_key) do
      partition when is_binary(partition) and partition != "" -> flow_acl_partition_key(partition)
      _none -> "*"
    end
  end

  defp flow_partition_query_keys(opts, mode) do
    list_keys =
      case Keyword.get(opts, :partition_keys) do
        partitions when is_list(partitions) -> partitions
        _none -> []
      end

    keys =
      case Keyword.get(opts, :partition_key) do
        partition when is_binary(partition) and partition != "" -> [partition | list_keys]
        _none -> list_keys
      end

    case keys do
      [] -> ["*"]
      keys -> Enum.map(keys, &flow_query_acl_partition(&1, mode))
    end
  end

  defp flow_query_acl_partition(partition, :claim) when is_binary(partition) do
    if String.upcase(partition) in ["AUTO", "ANY", "GLOBAL"],
      do: "*",
      else: partition
  end

  defp flow_query_acl_partition(partition, _mode), do: flow_acl_partition_key(partition)

  defp flow_batch_ast_keys(partition_key, _items)
       when is_binary(partition_key) and partition_key != "",
       do: [flow_acl_partition_key(partition_key)]

  defp flow_batch_ast_keys(_partition_key, items) do
    items
    |> Enum.map(fn item -> flow_item_partition(item) || flow_item_acl_id(item) || "*" end)
    |> case do
      [] -> ["*"]
      keys -> keys
    end
  end

  defp flow_item_partition({_id, opts}) when is_list(opts) do
    case Keyword.get(opts, :partition_key) do
      partition when is_binary(partition) and partition != "" -> flow_acl_partition_key(partition)
      _none -> nil
    end
  end

  defp flow_item_partition({:id, _id, :partition_key, partition_key})
       when is_binary(partition_key),
       do: flow_acl_partition_key(partition_key)

  defp flow_item_partition(item) when is_tuple(item) do
    case tagged_tuple_value(Tuple.to_list(item), :partition_key) do
      partition_key when is_binary(partition_key) and partition_key != "" ->
        flow_acl_partition_key(partition_key)

      _none ->
        nil
    end
  end

  defp flow_item_partition(_item), do: nil

  defp flow_item_acl_id({id, _opts}) when is_binary(id), do: id

  defp flow_item_acl_id(item) when is_tuple(item) do
    case Tuple.to_list(item) do
      [:id, id | _rest] when is_binary(id) -> id
      [id | _rest] when is_binary(id) -> id
      _other -> nil
    end
  end

  defp flow_item_acl_id(id) when is_binary(id), do: id
  defp flow_item_acl_id(_item), do: nil

  defp tagged_tuple_value([key, value | _rest], key), do: value
  defp tagged_tuple_value([_value | rest], key), do: tagged_tuple_value(rest, key)
  defp tagged_tuple_value([], _key), do: nil

  defp flow_nonempty_key(value) when is_binary(value) and value != "", do: [value]
  defp flow_nonempty_key(_value), do: ["*"]

  defp static_command_keys(cmd, args), do: extra_command_keys(cmd, args)

  defp management_command_keys("FERRICSTORE.NAMESPACE", ["ENSURE", prefix | _]),
    do: [scope_boundary_key(prefix)]

  defp management_command_keys("FERRICSTORE.NAMESPACE", ["GET", prefix]),
    do: [scope_boundary_key(prefix)]

  defp management_command_keys("FERRICSTORE.NAMESPACE", ["DELETE", prefix]),
    do: [scope_boundary_key(prefix)]

  defp management_command_keys("FERRICSTORE.NAMESPACE", ["LIST"]), do: ["*"]

  defp management_command_keys("FERRICSTORE.QUOTA", [subcmd, scope | _])
       when subcmd in ["SET", "GET", "USAGE"],
       do: [scope_boundary_key(scope)]

  defp management_command_keys("FERRICSTORE.TELEMETRY", ["CLUSTER_INFO"]), do: ["*"]

  defp management_command_keys("FERRICSTORE.TELEMETRY", ["NAMESPACE_USAGE", prefix]),
    do: [scope_boundary_key(prefix)]

  defp management_command_keys("FERRICSTORE.TELEMETRY", ["FLOW_QUERY" | attrs]),
    do: scoped_attr_keys(attrs)

  defp management_command_keys("FERRICSTORE.TELEMETRY", ["FLOW_HISTORY", _id | attrs]),
    do: scoped_attr_keys(attrs)

  defp management_command_keys(_cmd, _args), do: []

  defp scoped_attr_keys(attrs) do
    keys =
      attrs
      |> Enum.chunk_every(2)
      |> Enum.flat_map(fn
        [key, value] when is_binary(key) and is_binary(value) ->
          case String.downcase(key) do
            key when key in ["prefix", "namespace", "scope", "partition", "partition_key"] ->
              [scope_boundary_key(value)]

            _ ->
              []
          end

        _ ->
          []
      end)

    case keys do
      [] -> ["*"]
      keys -> keys
    end
  end

  defp scope_boundary_key("*"), do: "*"

  defp scope_boundary_key(scope) when is_binary(scope) do
    cond do
      String.ends_with?(scope, "*") -> scope
      String.ends_with?(scope, ":") -> scope <> "*"
      true -> scope <> ":*"
    end
  end

  defp extra_command_keys(cmd, args)
       when cmd in ~w(DEL UNLINK EXISTS MGET MSET MSETNX SINTER SUNION SDIFF),
       do: every_nth(args, 0, if(cmd in ~w(MSET MSETNX), do: 2, else: 1))

  defp extra_command_keys(cmd, args) when cmd in ~w(SDIFFSTORE SINTERSTORE SUNIONSTORE PFMERGE),
    do: args

  defp extra_command_keys(cmd, [source, destination | _])
       when cmd in ~w(RENAME RENAMENX COPY LMOVE BLMOVE RPOPLPUSH SMOVE),
       do: [source, destination]

  defp extra_command_keys("BITOP", [_operation, destination | sources]),
    do: [destination | sources]

  defp extra_command_keys("PUBLISH", [channel | _]), do: [channel]

  defp extra_command_keys(cmd, channels)
       when cmd in ~w(SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE), do: channels

  defp extra_command_keys("PUBSUB", [subcmd | channels]) when is_binary(subcmd) do
    if String.upcase(subcmd) == "NUMSUB", do: channels, else: []
  end

  defp extra_command_keys("FLOW.ATTRIBUTES", args), do: flow_partition_keys_or_global(args, 1)

  defp extra_command_keys("FLOW.ATTRIBUTE_VALUES", args),
    do: flow_partition_keys_or_global(args, 2)

  defp extra_command_keys("WATCH", keys), do: keys
  defp extra_command_keys("BLPOP", args), do: drop_last(args)
  defp extra_command_keys("BRPOP", args), do: drop_last(args)

  defp extra_command_keys("BLMPOP", [_timeout, count | rest]),
    do: take_count(rest, parse_int(count))

  defp extra_command_keys("XREAD", args), do: xread_keys(args)

  defp extra_command_keys("XREADGROUP", [option, _group, _consumer | args]) do
    if String.upcase(option) == "GROUP", do: xread_keys(args), else: []
  end

  defp extra_command_keys("XREADGROUP", _args), do: []
  defp extra_command_keys("GEOSEARCHSTORE", [destination, source | _]), do: [destination, source]

  defp extra_command_keys(cmd, [key | _args]) when cmd in @extra_first_key_commands, do: [key]
  defp extra_command_keys(_cmd, _args), do: []

  defp flow_partition_keys_or_global(args, option_start) do
    case flow_partition_key_values(args, option_start) do
      [] -> ["*"]
      keys -> keys
    end
  end

  defp flow_partition_key_values(args, option_start) do
    args
    |> Enum.drop(option_start)
    |> collect_flow_partition_keys(MapSet.new(), [])
  end

  defp collect_flow_partition_keys([name, value | rest], seen, acc) do
    key =
      if is_binary(name) and is_binary(value) and String.upcase(name) == "PARTITION",
        do: flow_acl_partition_key(value),
        else: nil

    {seen, acc} =
      if is_binary(key) and not MapSet.member?(seen, key),
        do: {MapSet.put(seen, key), [key | acc]},
        else: {seen, acc}

    collect_flow_partition_keys([value | rest], seen, acc)
  end

  defp collect_flow_partition_keys(_args, _seen, acc), do: Enum.reverse(acc)

  defp flow_acl_partition_key(value) do
    if String.upcase(value) == "GLOBAL", do: "*", else: value
  end

  defp parse_set_opts(opts), do: parse_set_opts(opts, [])

  defp parse_set_opts([], acc) do
    if :nx in acc and :xx in acc do
      {:error, "ERR XX and NX options at the same time are not compatible"}
    else
      Enum.reverse(acc)
    end
  end

  defp parse_set_opts([opt | rest], acc) do
    case String.upcase(opt) do
      "NX" -> parse_set_opts(rest, [:nx | acc])
      "XX" -> parse_set_opts(rest, [:xx | acc])
      "GET" -> parse_set_opts(rest, [:get | acc])
      "KEEPTTL" -> parse_set_opts(rest, [:keepttl | acc])
      "EX" -> parse_set_expiry(rest, :ex, acc)
      "PX" -> parse_set_expiry(rest, :px, acc)
      "EXAT" -> parse_set_expiry(rest, :exat, acc)
      "PXAT" -> parse_set_expiry(rest, :pxat, acc)
      _ -> {:error, "ERR syntax error"}
    end
  end

  defp parse_set_opts(_opts, _acc), do: {:error, "ERR syntax error"}

  defp parse_set_expiry([value | rest], tag, acc) do
    case parse_int(value) do
      int when is_integer(int) and int > 0 ->
        parse_set_opts(rest, [{tag, int} | acc])

      int when is_integer(int) ->
        parse_set_opts(rest, [{tag, {:error, "ERR invalid expire time in 'set' command"}} | acc])

      error ->
        parse_set_opts(rest, [{tag, error} | acc])
    end
  end

  defp parse_set_expiry([], _tag, _acc), do: {:error, "ERR syntax error"}

  defp parse_getex_opts([option]) when is_binary(option) do
    if String.upcase(option) == "PERSIST", do: :persist, else: {:error, "ERR syntax error"}
  end

  defp parse_getex_opts([option, value]) do
    case String.upcase(option) do
      "EX" -> {:ex, parse_int(value)}
      "PX" -> {:px, parse_int(value)}
      "EXAT" -> {:exat, parse_int(value)}
      "PXAT" -> {:pxat, parse_int(value)}
      _ -> {:error, "ERR syntax error"}
    end
  end

  defp parse_getex_opts(_), do: {:error, "ERR syntax error"}

  defp parse_expiry_flag(flag) do
    case String.upcase(flag) do
      "NX" -> :nx
      "XX" -> :xx
      "GT" -> :gt
      "LT" -> :lt
      _ -> {:error, "ERR syntax error"}
    end
  end

  defp parse_direction(value) do
    case String.upcase(value) do
      "LEFT" -> :left
      "RIGHT" -> :right
      "BEFORE" -> :before
      "AFTER" -> :after
      _ -> {:error, "ERR syntax error"}
    end
  end

  defp parse_withvalues(value) do
    if String.upcase(value) == "WITHVALUES", do: :withvalues, else: {:error, "ERR syntax error"}
  end

  defp parse_scan_opts(opts), do: parse_scan_opts(opts, true, [])
  defp parse_collection_scan_opts(opts), do: parse_scan_opts(opts, false, [])

  defp parse_scan_opts([], _allow_type?, acc), do: Enum.reverse(acc)

  defp parse_scan_opts([option, value | rest], allow_type?, acc) do
    case String.upcase(option) do
      "MATCH" ->
        parse_scan_opts(rest, allow_type?, Keyword.put(acc, :match, value))

      "COUNT" ->
        case parse_int(value) do
          count when is_integer(count) and count > 0 and count <= 10_000 ->
            parse_scan_opts(rest, allow_type?, Keyword.put(acc, :count, count))

          _invalid ->
            {:error, "ERR value is not an integer or out of range"}
        end

      "TYPE" when allow_type? ->
        parse_scan_opts(rest, allow_type?, Keyword.put(acc, :type, String.downcase(value)))

      _unsupported ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_scan_opts(_invalid, _allow_type?, _acc), do: {:error, "ERR syntax error"}

  defp parse_hash_expire(tag, key, ttl, count, fields) do
    with ttl when is_integer(ttl) <- parse_pos_int(ttl),
         parsed_fields when is_list(parsed_fields) <- parse_hash_fields(count, fields) do
      {tag, key, ttl, parsed_fields}
    else
      {:error, _} = error -> {tag, key, error, []}
    end
  end

  defp parse_hash_field_list(tag, key, count, fields) do
    case parse_hash_fields(count, fields) do
      parsed_fields when is_list(parsed_fields) -> {tag, key, parsed_fields}
      {:error, _} = error -> {tag, key, error}
    end
  end

  defp parse_hgetex(key, [option | rest]) do
    case {String.upcase(option), rest} do
      {"FIELDS", [count | fields]} ->
        parse_hgetex_fields(key, :none, count, fields)

      {"PERSIST", [fields_option, count | fields]} ->
        if String.upcase(fields_option) == "FIELDS",
          do: parse_hgetex_fields(key, :persist, count, fields),
          else: {:hgetex, key, {:error, "ERR syntax error"}}

      {mode, [value, fields_option, count | fields]} when mode in ~w(EX PX EXAT PXAT) ->
        if String.upcase(fields_option) == "FIELDS" do
          case parse_hgetex_expiry(mode, value) do
            expiry when is_tuple(expiry) -> parse_hgetex_fields(key, expiry, count, fields)
            {:error, _} = error -> {:hgetex, key, error}
          end
        else
          {:hgetex, key, {:error, "ERR syntax error"}}
        end

      _invalid ->
        {:hgetex, key, {:error, "ERR syntax error"}}
    end
  end

  defp parse_hgetex(key, _opts),
    do: {:hgetex, key, {:error, "ERR wrong number of arguments for 'hgetex' command"}}

  defp parse_hgetex_fields(key, expiry, count, fields) do
    case parse_hash_fields(count, fields) do
      parsed_fields when is_list(parsed_fields) -> {:hgetex, key, expiry, parsed_fields}
      {:error, _} = error -> {:hgetex, key, error}
    end
  end

  defp parse_hgetex_expiry("EX", value), do: wrap_expiry(:ex, parse_pos_int(value))
  defp parse_hgetex_expiry("PX", value), do: wrap_expiry(:px, parse_pos_int(value))
  defp parse_hgetex_expiry("EXAT", value), do: wrap_expiry(:exat, parse_pos_int(value))
  defp parse_hgetex_expiry("PXAT", value), do: wrap_expiry(:pxat, parse_pos_int(value))

  defp wrap_expiry(kind, value) when is_integer(value), do: {kind, value}
  defp wrap_expiry(_kind, {:error, _} = error), do: error

  defp parse_hash_fields(count, fields) do
    case parse_pos_int(count) do
      count when is_integer(count) and length(fields) == count ->
        fields

      count when is_integer(count) ->
        {:error, "ERR wrong number of arguments for hash field command"}

      {:error, _} = error ->
        error
    end
  end

  defp parse_sintercard([count | rest]) do
    case parse_int(count) do
      count when is_integer(count) and count > 0 -> parse_sintercard_keys(count, rest)
      _ -> {:sintercard, {:error, "ERR numkeys can't be non-positive value"}}
    end
  end

  defp parse_sintercard(_args),
    do: {:sintercard, {:error, "ERR wrong number of arguments for 'sintercard' command"}}

  defp parse_sintercard_keys(count, rest) do
    {keys, tail} = Enum.split(rest, count)

    if length(keys) < count do
      {:sintercard, {:error, "ERR Number of keys can't be greater than number of args"}}
    else
      parse_sintercard_limit(keys, tail)
    end
  end

  defp parse_sintercard_limit(keys, []), do: {:sintercard, keys, 0}

  defp parse_sintercard_limit(keys, [option, value]) do
    if String.upcase(option) == "LIMIT" do
      case parse_int(value) do
        limit when is_integer(limit) and limit >= 0 -> {:sintercard, keys, limit}
        _ -> {:sintercard, {:error, "ERR value is not an integer or out of range"}}
      end
    else
      {:sintercard, {:error, "ERR syntax error"}}
    end
  end

  defp parse_sintercard_limit(_keys, _tail),
    do: {:sintercard, {:error, "ERR syntax error"}}

  defp parse_zadd(key, rest) do
    {opts, pairs} = split_zadd_opts(rest, [])
    {:zadd, key, Enum.reverse(opts), parse_score_member_pairs(pairs)}
  end

  defp split_zadd_opts([opt | rest] = args, acc) do
    case String.upcase(opt) do
      normalized when normalized in ["NX", "XX", "GT", "LT", "CH"] ->
        split_zadd_opts(rest, [normalized |> String.downcase() |> String.to_atom() | acc])

      _not_option ->
        {acc, args}
    end
  end

  defp split_zadd_opts(rest, acc), do: {acc, rest}

  defp parse_score_member_pairs(args), do: parse_score_member_pairs(args, [])
  defp parse_score_member_pairs([], acc), do: Enum.reverse(acc)

  defp parse_score_member_pairs([score, member | rest], acc),
    do: parse_score_member_pairs(rest, [{parse_float(score), member} | acc])

  defp parse_score_member_pairs(_args, _acc), do: {:error, "ERR syntax error"}

  defp parse_copy_opts([]), do: false

  defp parse_copy_opts([option]) do
    if String.upcase(option) == "REPLACE", do: true, else: {:error, "ERR syntax error"}
  end

  defp parse_copy_opts(_opts), do: {:error, "ERR syntax error"}

  defp parse_object([subcommand]) do
    if String.upcase(subcommand) == "HELP",
      do: {:object, :help},
      else: {:object, {:error, "ERR syntax error"}}
  end

  defp parse_object([subcmd, key]) do
    case String.upcase(subcmd) do
      "ENCODING" -> {:object, :encoding, key}
      "FREQ" -> {:object, :freq, key}
      "IDLETIME" -> {:object, :idletime, key}
      "REFCOUNT" -> {:object, :refcount, key}
      _ -> {:object, {:error, "ERR syntax error"}}
    end
  end

  defp parse_object(_), do: {:object, {:error, "ERR syntax error"}}

  defp parse_blocking_pop(tag, args) do
    case Enum.split(args, max(length(args) - 1, 0)) do
      {[_ | _] = keys, [timeout]} ->
        case parse_timeout_ms(timeout) do
          {:error, _} = error -> {tag, error}
          timeout_ms -> {tag, keys, timeout_ms}
        end

      _ ->
        {tag, {:error, "ERR wrong number of arguments for blocking pop command"}}
    end
  end

  defp parse_blmpop([timeout, count | rest]) do
    key_count = parse_int(count)
    timeout_ms = parse_timeout_ms(timeout)

    cond do
      match?({:error, _}, timeout_ms) ->
        {:blmpop, timeout_ms}

      not (is_integer(key_count) and key_count > 0) ->
        {:blmpop, {:error, "ERR value is not an integer or out of range"}}

      true ->
        {keys, tail} = Enum.split(rest, key_count)

        case tail do
          [where] ->
            case parse_direction(where) do
              {:error, _} = error -> {:blmpop, error}
              direction -> {:blmpop, keys, direction, 1, timeout_ms}
            end

          [where, option, count] ->
            direction = parse_direction(where)
            parsed_count = parse_int(count)

            cond do
              String.upcase(option) != "COUNT" ->
                {:blmpop, {:error, "ERR syntax error"}}

              match?({:error, _}, direction) ->
                {:blmpop, direction}

              not (is_integer(parsed_count) and parsed_count > 0) ->
                {:blmpop, {:error, "ERR syntax error"}}

              true ->
                {:blmpop, keys, direction, parsed_count, timeout_ms}
            end

          _ ->
            {:blmpop, {:error, "ERR syntax error"}}
        end
    end
  end

  defp parse_blmpop(_args), do: {:blmpop, {:error, "ERR syntax error"}}

  defp parse_timeout_ms(value) do
    case Float.parse(value) do
      {timeout, ""}
      when timeout >= 0 and timeout <= @max_blocking_timeout_ms / 1_000 ->
        trunc(timeout * 1_000)

      _ ->
        {:error, "ERR timeout is not a float or out of range"}
    end
  rescue
    ArgumentError -> {:error, "ERR timeout is not a float or out of range"}
    ArithmeticError -> {:error, "ERR timeout is not a float or out of range"}
  end

  defp parse_bitop(operation, destination, sources) do
    case String.upcase(operation) do
      "AND" -> {:bitop, :band, destination, sources}
      "OR" -> {:bitop, :bor, destination, sources}
      "XOR" -> {:bitop, :bxor, destination, sources}
      "NOT" when length(sources) == 1 -> {:bitop, :bnot, destination, sources}
      "NOT" -> {:bitop, {:error, "ERR BITOP NOT requires one and only one key"}}
      _ -> {:bitop, {:error, "ERR syntax error"}}
    end
  end

  defp every_nth(args, start, step) do
    args
    |> Enum.with_index()
    |> Enum.flat_map(fn {arg, idx} ->
      if idx >= start and rem(idx - start, step) == 0, do: [arg], else: []
    end)
  end

  defp drop_last([]), do: []
  defp drop_last(args), do: Enum.take(args, length(args) - 1)

  defp take_count(args, count) when is_integer(count) and count >= 0, do: Enum.take(args, count)
  defp take_count(_args, _count), do: []

  defp xread_keys(args) do
    case Enum.find_index(args, &(String.upcase(&1) == "STREAMS")) do
      nil ->
        []

      idx ->
        tail = Enum.drop(args, idx + 1)
        key_count = div(length(tail), 2)
        Enum.take(tail, key_count)
    end
  end

  defp spec(name), do: Map.fetch!(@flow_option_specs, name)

  defp parse_flow_options(args, specs), do: parse_flow_options(args, specs, [])

  defp parse_flow_options([], _specs, acc), do: finalize_flow_list_opts(Enum.reverse(acc))

  defp parse_flow_options([name | rest], specs, acc) do
    case {String.upcase(name), rest} do
      {"ATTRIBUTE", [key, value | tail]} ->
        parse_flow_options(tail, specs, merge_map_opt(:attributes, key, value, acc))

      {"ATTRIBUTE_MERGE", [key, value | tail]} ->
        parse_flow_options(tail, specs, merge_map_opt(:attributes_merge, key, value, acc))

      {"ATTRIBUTE_DELETE", [key | tail]} ->
        parse_flow_options(tail, specs, append_list_opt(:attributes_delete, key, acc))

      {"STATE_META", [key, value | tail]} ->
        parse_flow_options(tail, specs, merge_map_opt(:state_meta, key, value, acc))

      {_option, [value | tail]} ->
        case parse_flow_option(name, value, specs) do
          {:ok, nil} -> parse_flow_options(tail, specs, acc)
          {:ok, opt} -> parse_flow_options(tail, specs, [opt | acc])
          {:error, reason} -> {:error, reason}
        end

      _invalid ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_flow_options(_args, _specs, _acc), do: {:error, "ERR syntax error"}

  defp parse_flow_retry_options(args, specs) do
    case parse_flow_options(args, specs) do
      opts when is_list(opts) -> nest_flow_retry_policy(opts)
      {:error, _reason} = error -> error
    end
  end

  defp nest_flow_retry_policy(opts) do
    {retry_opts, rest} = Keyword.split(opts, [:max_retries, :exhausted_to])

    if retry_opts == [] do
      rest
    else
      {existing_retry, rest} = Keyword.pop(rest, :retry, [])
      Keyword.put(rest, :retry, existing_retry ++ retry_opts)
    end
  end

  defp parse_flow_read_options(args, specs), do: parse_flow_read_options(args, specs, [])

  defp parse_flow_read_options([], _specs, acc), do: finalize_flow_list_opts(Enum.reverse(acc))

  defp parse_flow_read_options([name | rest], specs, acc) do
    case {String.upcase(name), rest} do
      {"FULL", [value | tail]} ->
        case parse_bool(value) do
          {:ok, bool} -> parse_flow_read_options(tail, specs, [{:full, bool} | acc])
          {:error, _} -> parse_flow_read_options(rest, specs, [{:full, true} | acc])
        end

      {"FULL", []} ->
        parse_flow_read_options([], specs, [{:full, true} | acc])

      {"NOPAYLOAD", tail} ->
        parse_flow_read_options(tail, specs, [{:payload, false} | acc])

      {"PAYLOAD", [maxbytes, bytes | tail]} ->
        if String.upcase(maxbytes) == "MAXBYTES" do
          case parse_int(bytes) do
            parsed when is_integer(parsed) and parsed >= 0 ->
              parse_flow_read_options(tail, specs, [
                {:payload_max_bytes, parsed},
                {:payload, true} | acc
              ])

            _invalid ->
              {:error, "ERR value is not an integer or out of range"}
          end
        else
          parse_flow_read_options(rest, specs, [{:payload, true} | acc])
        end

      {"PAYLOAD", tail} ->
        parse_flow_read_options(tail, specs, [{:payload, true} | acc])

      {"VALUE", [value | tail]} ->
        parse_flow_read_options(tail, specs, [{:values, [value]} | acc])

      {"PARTITIONS", [count | tail]} ->
        parse_flow_partitions(count, tail, specs, acc)

      {"ATTRIBUTE", [key, value | tail]} ->
        parse_flow_read_options(tail, specs, merge_map_opt(:attributes, key, value, acc))

      {"ATTRIBUTE_MERGE", [key, value | tail]} ->
        parse_flow_read_options(tail, specs, merge_map_opt(:attributes_merge, key, value, acc))

      {"ATTRIBUTE_DELETE", [key | tail]} ->
        parse_flow_read_options(tail, specs, append_list_opt(:attributes_delete, key, acc))

      {_option, [value | tail]} ->
        case parse_flow_option(name, value, specs) do
          {:ok, nil} -> parse_flow_read_options(tail, specs, acc)
          {:ok, opt} -> parse_flow_read_options(tail, specs, [opt | acc])
          {:error, reason} -> {:error, reason}
        end

      _invalid ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_flow_read_options(_args, _specs, _acc), do: {:error, "ERR syntax error"}

  defp parse_flow_partitions(count, rest, specs, acc) do
    case parse_int(count) do
      parsed_count when is_integer(parsed_count) and parsed_count > 0 ->
        {partitions, tail} = Enum.split(rest, parsed_count)

        if length(partitions) == parsed_count do
          parse_flow_read_options(tail, specs, [{:partition_keys, partitions} | acc])
        else
          {:error, "ERR flow partition_keys count mismatch"}
        end

      _invalid ->
        {:error, "ERR flow partition_keys must be a non-empty list"}
    end
  end

  defp parse_flow_signal_opts(args), do: parse_flow_signal_opts(args, [])
  defp parse_flow_signal_opts([], acc), do: finalize_flow_list_opts(Enum.reverse(acc))

  defp parse_flow_signal_opts([name | rest], acc) do
    case {String.upcase(name), rest} do
      {"VALUE", [key, value | tail]} ->
        parse_flow_signal_opts(tail, append_list_opt(:values, {key, value}, acc))

      {"VALUE_REF", [key, value | tail]} ->
        parse_flow_signal_opts(tail, append_list_opt(:value_refs, {key, value}, acc))

      {"DROP_VALUE", [key | tail]} ->
        parse_flow_signal_opts(tail, append_list_opt(:drop_values, key, acc))

      {"OVERRIDE_VALUE", [key | tail]} ->
        parse_flow_signal_opts(tail, append_list_opt(:override_values, key, acc))

      {_option, [value | tail]} ->
        case parse_flow_option(name, value, spec(:signal)) do
          {:ok, nil} -> parse_flow_signal_opts(tail, acc)
          {:ok, opt} -> parse_flow_signal_opts(tail, [opt | acc])
          {:error, reason} -> {:error, reason}
        end

      _invalid ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_flow_signal_opts(_args, _acc), do: {:error, "ERR syntax error"}

  defp parse_flow_search_options(args), do: parse_flow_search_options(args, [])

  defp parse_flow_search_options([], acc), do: Enum.reverse(acc)

  defp parse_flow_search_options([name | rest], acc) do
    case {String.upcase(name), rest} do
      {"ATTRIBUTE", [key, value | tail]} ->
        parse_flow_search_options(tail, merge_map_opt(:attributes, key, value, acc))

      {"STATE_META", [state, key, value | tail]} ->
        parse_flow_search_options(tail, merge_nested_map_opt(:state_meta, state, key, value, acc))

      {_option, [value | tail]} ->
        case parse_flow_option(name, value, spec(:search)) do
          {:ok, nil} -> parse_flow_search_options(tail, acc)
          {:ok, opt} -> parse_flow_search_options(tail, [opt | acc])
          {:error, reason} -> {:error, reason}
        end

      _invalid ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_flow_search_options(_args, _acc), do: {:error, "ERR syntax error"}

  defp parse_flow_query_params(params), do: parse_flow_query_params(params, %{})

  defp parse_flow_query_params([], params), do: {:ok, params}

  defp parse_flow_query_params([_name, _value | _rest], params)
       when map_size(params) >= @max_flow_query_parameters,
       do: {:error, "ERR FLOW.QUERY accepts at most 64 named parameters"}

  defp parse_flow_query_params([name, value | rest], params)
       when is_binary(name) and name != "" do
    if Map.has_key?(params, name) do
      {:error, "ERR FLOW.QUERY parameter names must be unique"}
    else
      parse_flow_query_params(rest, Map.put(params, name, value))
    end
  end

  defp parse_flow_query_params(_params, _acc),
    do: {:error, "ERR FLOW.QUERY parameters must be name/value pairs"}

  defp parse_flow_policy_set_options(args),
    do: parse_flow_policy_set_options(args, [], [], [], [], [])

  defp parse_flow_policy_set_options(
         [],
         policy_opts,
         retry_opts,
         backoff_opts,
         retention_opts,
         states
       ) do
    retry_opts =
      if backoff_opts == [] do
        retry_opts
      else
        [{:backoff, Enum.reverse(backoff_opts)} | retry_opts]
      end

    Enum.reverse(policy_opts)
    |> maybe_put_policy(:retry, retry_opts)
    |> maybe_put_policy(:retention, retention_opts)
    |> maybe_put_policy(:states, states)
    |> Enum.reverse()
  end

  defp parse_flow_policy_set_options(
         ["STATE", state | rest],
         policy_opts,
         retry_opts,
         backoff_opts,
         retention_opts,
         states
       ) do
    {state_args, tail} = Enum.split_while(rest, &(String.upcase(&1) != "STATE"))

    case parse_flow_policy_state_options(state_args) do
      {:ok, state_policy} ->
        parse_flow_policy_set_options(
          tail,
          policy_opts,
          retry_opts,
          backoff_opts,
          retention_opts,
          [{state, state_policy} | states]
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_flow_policy_set_options(
         [name, value | rest],
         policy_opts,
         retry_opts,
         backoff_opts,
         retention_opts,
         states
       ) do
    if String.upcase(name) == "STATE" do
      {state_args, tail} = Enum.split_while(rest, &(String.upcase(&1) != "STATE"))

      case parse_flow_policy_state_options(state_args) do
        {:ok, state_policy} ->
          parse_flow_policy_set_options(
            tail,
            policy_opts,
            retry_opts,
            backoff_opts,
            retention_opts,
            [{value, state_policy} | states]
          )

        {:error, reason} ->
          {:error, reason}
      end
    else
      case parse_flow_policy_option(name, value) do
        {:retry, opt} ->
          parse_flow_policy_set_options(
            rest,
            policy_opts,
            [opt | retry_opts],
            backoff_opts,
            retention_opts,
            states
          )

        {:backoff, opt} ->
          parse_flow_policy_set_options(
            rest,
            policy_opts,
            retry_opts,
            [opt | backoff_opts],
            retention_opts,
            states
          )

        {:retention, opt} ->
          parse_flow_policy_set_options(
            rest,
            policy_opts,
            retry_opts,
            backoff_opts,
            [opt | retention_opts],
            states
          )

        {:policy, opt} ->
          parse_flow_policy_set_options(
            rest,
            [opt | policy_opts],
            retry_opts,
            backoff_opts,
            retention_opts,
            states
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp parse_flow_policy_set_options(
         _args,
         _policy_opts,
         _retry_opts,
         _backoff_opts,
         _retention_opts,
         _states
       ),
       do: {:error, "ERR syntax error"}

  defp parse_flow_policy_state_options(args) do
    case parse_flow_policy_set_options(args) do
      opts when is_list(opts) ->
        cond do
          Keyword.has_key?(opts, :indexed_state_meta) ->
            {:error, "ERR flow indexed_state_meta is type-level only"}

          Keyword.has_key?(opts, :indexed_attributes) ->
            {:error, "ERR flow indexed_attributes is type-level only"}

          Keyword.has_key?(opts, :max_active_ms) ->
            {:error, "ERR flow max_active_ms is type-level only"}

          Keyword.has_key?(opts, :expected_generation) ->
            {:error, "ERR flow expected_generation is type-level only"}

          Keyword.has_key?(opts, :replace) ->
            {:error, "ERR flow replace is type-level only"}

          true ->
            {:ok, opts}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_put_policy(acc, _key, []), do: acc
  defp maybe_put_policy(acc, key, value), do: [{key, Enum.reverse(value)} | acc]

  defp parse_flow_policy_option(name, value) do
    case String.upcase(name) do
      "RETENTION_TTL" ->
        policy_positive(:retention, :ttl_ms, value)

      "RETENTION_TTL_MS" ->
        policy_positive(:retention, :ttl_ms, value)

      "HISTORY_MAX_EVENTS" ->
        policy_positive(:retention, :history_max_events, value)

      "HISTORY_HOT_MAX_EVENTS" ->
        {:error, "ERR flow retention history_hot_max_events is internal"}

      "MAX_ACTIVE_MS" ->
        policy_positive_or_infinity(:policy, :max_active_ms, value)

      "EXPECTED_GENERATION" ->
        policy_non_negative(:policy, :expected_generation, value)

      "REPLACE" ->
        case parse_bool(value) do
          {:ok, replace?} -> {:policy, {:replace, replace?}}
          {:error, _reason} -> {:error, "ERR flow replace must be boolean"}
        end

      "INDEXED_STATE_META" ->
        {:policy, {:indexed_state_meta, value}}

      "INDEXED_ATTRIBUTES" ->
        {:policy, {:indexed_attributes, flow_policy_indexed_attributes(value)}}

      "MODE" ->
        parse_flow_state_mode(value)

      "MAX_RETRIES" ->
        policy_non_negative(:retry, :max_retries, value)

      "EXHAUSTED_TO" ->
        {:retry, {:exhausted_to, value}}

      "BACKOFF" ->
        parse_flow_backoff_kind(value)

      "BASE_MS" ->
        policy_non_negative(:backoff, :base_ms, value)

      "MAX_MS" ->
        policy_non_negative(:backoff, :max_ms, value)

      "JITTER_PCT" ->
        policy_non_negative(:backoff, :jitter_pct, value)

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp flow_policy_indexed_attributes(value) when is_list(value), do: value

  defp flow_policy_indexed_attributes(value) when is_binary(value) do
    trimmed = String.trim(value)

    cond do
      String.starts_with?(trimmed, "[") ->
        case Jason.decode(trimmed) do
          {:ok, values} when is_list(values) -> values
          _ -> [value]
        end

      String.contains?(value, ",") ->
        value
        |> String.split(",")
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))

      true ->
        [value]
    end
  end

  defp flow_policy_indexed_attributes(value), do: [value]

  defp parse_flow_state_mode(value) when is_binary(value) do
    case String.upcase(value) do
      "PARALLEL" -> {:policy, {:mode, :parallel}}
      "FIFO" -> {:policy, {:mode, :fifo}}
      _other -> {:error, "ERR flow state mode must be parallel or fifo"}
    end
  end

  defp parse_flow_state_mode(_value),
    do: {:error, "ERR flow state mode must be parallel or fifo"}

  defp policy_positive(scope, key, value) do
    case parse_int(value) do
      int when is_integer(int) and int > 0 -> {scope, {key, int}}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp policy_positive_or_infinity(scope, key, value) do
    case String.upcase(value) do
      "INFINITY" ->
        {scope, {key, :infinity}}

      _other ->
        policy_positive(scope, key, value)
    end
  end

  defp policy_non_negative(scope, key, value) do
    case parse_int(value) do
      int when is_integer(int) and int >= 0 -> {scope, {key, int}}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_flow_backoff_kind(value) do
    case String.upcase(value) do
      "NONE" -> {:backoff, {:kind, :none}}
      "FIXED" -> {:backoff, {:kind, :fixed}}
      "LINEAR" -> {:backoff, {:kind, :linear}}
      "EXPONENTIAL" -> {:backoff, {:kind, :exponential}}
      _ -> {:error, "ERR flow retry backoff kind must be none, fixed, linear, or exponential"}
    end
  end

  defp parse_flow_option(name, value, specs) do
    upper = String.upcase(name)

    case Enum.find(specs, fn {wire, _key, _type} -> wire == upper end) do
      nil -> {:error, "ERR syntax error"}
      {_wire, key, type} -> flow_value(key, type, value)
    end
  end

  defp flow_value(key, :binary, value), do: {:ok, {key, value}}

  defp flow_value(key, :boolean, value) do
    case parse_bool(value) do
      {:ok, bool} -> {:ok, {key, bool}}
      {:error, _} -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  defp flow_value(key, :partition, value) do
    if String.upcase(value) == "GLOBAL", do: {:ok, nil}, else: {:ok, {key, value}}
  end

  defp flow_value(key, :non_negative, value), do: non_negative_value(key, value)
  defp flow_value(key, {:non_negative_named, _label}, value), do: non_negative_value(key, value)
  defp flow_value(key, {:positive, _label}, value), do: positive_value(key, value)
  defp flow_value(key, :positive_or_infinity, value), do: positive_or_infinity_value(key, value)

  defp flow_value(key, {:ref, label}, value) do
    if byte_size(value) <= @max_flow_ref_size do
      {:ok, {key, value}}
    else
      {:error, "ERR flow #{label} too large (max 4096 bytes)"}
    end
  end

  defp non_negative_value(key, value) do
    case parse_int(value) do
      int when is_integer(int) and int >= 0 -> {:ok, {key, int}}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp positive_value(key, value) do
    case parse_int(value) do
      int when is_integer(int) and int > 0 -> {:ok, {key, int}}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp positive_or_infinity_value(key, value) do
    case String.upcase(value) do
      "INFINITY" ->
        {:ok, {key, :infinity}}

      _other ->
        positive_value(key, value)
    end
  end

  defp parse_flow_create_many([]),
    do:
      {:flow_create_many,
       {:error, "ERR wrong number of arguments for 'flow.create_many' command"}}

  defp parse_flow_create_many([partition | rest]) do
    with {:ok, before_items, raw_items} <- split_items(rest),
         opts when is_list(opts) <-
           parse_flow_options(
             before_items,
             spec(:create) ++ [{"INDEPENDENT", :independent, :boolean}]
           ) do
      shared_payload? = Keyword.has_key?(opts, :payload_ref)
      mixed? = String.upcase(partition) == "MIXED"
      auto? = String.upcase(partition) == "AUTO"
      item_width = create_many_item_width(mixed?, shared_payload?)

      items =
        parse_fixed_items(raw_items, item_width, &create_many_item(&1, mixed?, shared_payload?))

      {:flow_create_many, if(mixed? or auto?, do: nil, else: partition), items, opts}
    else
      {:error, reason} -> {:flow_create_many, partition, {:error, reason}}
      error -> {:flow_create_many, partition, error}
    end
  end

  defp parse_flow_spawn_children([parent | rest]) do
    with {:ok, before_items, raw_items} <- split_items(rest),
         opts when is_list(opts) <- parse_flow_options(before_items, spec(:spawn_children)) do
      mixed? = raw_items != [] and String.upcase(hd(raw_items)) == "MIXED"
      item_values = if mixed?, do: tl(raw_items), else: raw_items
      width = if mixed?, do: 4, else: 3
      items = parse_fixed_items(item_values, width, &spawn_child_item(&1, mixed?))
      {:flow_spawn_children, parent, items, opts}
    else
      {:error, reason} -> {:flow_spawn_children, parent, {:error, reason}}
      error -> {:flow_spawn_children, parent, error}
    end
  end

  defp parse_flow_spawn_children([]),
    do:
      {:flow_spawn_children,
       {:error, "ERR wrong number of arguments for 'flow.spawn_children' command"}}

  defp parse_flow_terminal_many(tag, [], _specs),
    do: {tag, {:error, "ERR wrong number of arguments for '#{flow_command_name(tag)}' command"}}

  defp parse_flow_terminal_many(tag, [partition | rest], specs) do
    mixed? = String.upcase(partition) == "MIXED"

    with {:ok, before_items, raw_items} <- split_items(rest),
         opts when is_list(opts) <- parse_flow_options(before_items, specs),
         opts when is_list(opts) <- maybe_nest_flow_many_retry_policy(tag, opts),
         items when is_list(items) <- parse_terminal_items(raw_items, mixed?) do
      {tag, if(mixed?, do: nil, else: partition), items, opts}
    else
      {:error, reason} -> {tag, partition, {:error, reason}}
      error -> {tag, partition, error}
    end
  end

  defp maybe_nest_flow_many_retry_policy(:flow_retry_many, opts), do: nest_flow_retry_policy(opts)
  defp maybe_nest_flow_many_retry_policy(_tag, opts), do: opts

  defp parse_flow_cancel_many([]),
    do:
      {:flow_cancel_many,
       {:error, "ERR wrong number of arguments for 'flow.cancel_many' command"}}

  defp parse_flow_cancel_many([partition | rest]) do
    mixed? = String.upcase(partition) == "MIXED"

    with {:ok, before_items, raw_items} <- split_items(rest),
         opts when is_list(opts) <- parse_flow_options(before_items, spec(:many_cancel)),
         items when is_list(items) <- parse_cancel_items(raw_items, mixed?) do
      {:flow_cancel_many, if(mixed?, do: nil, else: partition), items, opts}
    else
      {:error, reason} -> {:flow_cancel_many, partition, {:error, reason}}
      error -> {:flow_cancel_many, partition, error}
    end
  end

  defp parse_flow_transition_many(args) when length(args) < 3,
    do:
      {:flow_transition_many,
       {:error, "ERR wrong number of arguments for 'flow.transition_many' command"}}

  defp parse_flow_transition_many([partition, from, to | rest]) do
    mixed? = String.upcase(partition) == "MIXED"

    with {:ok, before_items, raw_items} <- split_items(rest),
         opts when is_list(opts) <- parse_flow_options(before_items, spec(:many_transition)),
         items when is_list(items) <- parse_transition_items(raw_items, mixed?) do
      {:flow_transition_many, if(mixed?, do: nil, else: partition), from, to, items, opts}
    else
      {:error, reason} -> {:flow_transition_many, partition, from, to, {:error, reason}}
      error -> {:flow_transition_many, partition, from, to, error}
    end
  end

  defp flow_command_name(tag) do
    tag
    |> Atom.to_string()
    |> String.replace("_", ".")
  end

  defp split_items(args) do
    case Enum.split_while(args, &(String.upcase(&1) != "ITEMS")) do
      {_before, []} -> {:error, "ERR flow items are required"}
      {before_items, [_items | raw_items]} when raw_items != [] -> {:ok, before_items, raw_items}
      {_before, _} -> {:error, "ERR syntax error"}
    end
  end

  defp create_many_item_width(true, true), do: 2
  defp create_many_item_width(true, false), do: 3
  defp create_many_item_width(false, true), do: 1
  defp create_many_item_width(false, false), do: 2

  defp parse_fixed_items(items, width, fun) when rem(length(items), width) == 0 do
    items
    |> Enum.chunk_every(width)
    |> Enum.map(fun)
  end

  defp parse_fixed_items(_items, _width, _fun), do: {:error, "ERR syntax error"}

  defp create_many_item([id, partition], true, true), do: {id, [partition_key: partition]}

  defp create_many_item([id, partition, payload], true, false),
    do: {id, [partition_key: partition, payload: payload]}

  defp create_many_item([id], false, true), do: id
  defp create_many_item([id, payload], false, false), do: {:id, id, :payload, payload}

  defp spawn_child_item([id, partition, type, payload], true),
    do: {id, [partition_key: partition, type: type, payload: payload]}

  defp spawn_child_item([id, type, payload], false), do: {id, [type: type, payload: payload]}

  defp parse_terminal_items(items, mixed?) do
    width = if mixed?, do: 4, else: 3

    parse_fixed_items(items, width, fn
      [id, partition, lease, fencing] when mixed? ->
        {id, [partition_key: partition, lease_token: lease, fencing_token: parse_int(fencing)]}

      [id, lease, fencing] ->
        {:id, id, :lease_token, lease, :fencing_token, parse_int(fencing)}
    end)
  end

  defp parse_cancel_items(items, mixed?) do
    width = if mixed?, do: 3, else: 2

    parse_fixed_items(items, width, fn
      [id, partition, fencing] when mixed? ->
        {:id, id, :partition_key, partition, :fencing_token, parse_int(fencing)}

      [id, fencing] ->
        {:id, id, :fencing_token, parse_int(fencing)}
    end)
  end

  defp parse_transition_items(items, mixed?) do
    width = if mixed?, do: 4, else: 3

    parse_fixed_items(items, width, fn
      [id, partition, fencing, lease] when mixed? ->
        opts = [partition_key: partition, fencing_token: parse_int(fencing)]
        {id, maybe_put_lease(opts, lease)}

      [id, fencing, lease] ->
        {:id, id, :fencing_token, parse_int(fencing), :lease_token, dash_nil(lease)}
    end)
  end

  defp maybe_put_lease(opts, "-"), do: opts
  defp maybe_put_lease(opts, lease), do: Keyword.put(opts, :lease_token, lease)
  defp dash_nil("-"), do: nil
  defp dash_nil(value), do: value

  defp merge_map_opt(key, attr_key, attr_value, acc) do
    Keyword.update(acc, key, %{attr_key => attr_value}, &Map.put(&1, attr_key, attr_value))
  end

  defp merge_nested_map_opt(key, outer_key, inner_key, inner_value, acc) do
    Keyword.update(acc, key, %{outer_key => %{inner_key => inner_value}}, fn existing ->
      Map.update(existing, outer_key, %{inner_key => inner_value}, fn nested ->
        Map.put(nested, inner_key, inner_value)
      end)
    end)
  end

  defp append_list_opt(key, value, acc), do: Keyword.update(acc, key, [value], &[value | &1])

  defp finalize_flow_list_opts(opts) do
    Enum.map(opts, fn
      {key, values} when key in @flow_repeated_option_keys and is_list(values) ->
        {key, Enum.reverse(values)}

      option ->
        option
    end)
  end

  defp parse_bool(value) do
    case String.upcase(value) do
      "1" -> {:ok, true}
      "TRUE" -> {:ok, true}
      "YES" -> {:ok, true}
      "ON" -> {:ok, true}
      "0" -> {:ok, false}
      "FALSE" -> {:ok, false}
      "NO" -> {:ok, false}
      "OFF" -> {:ok, false}
      _ -> {:error, :invalid_bool}
    end
  end

  defp parse_int(value) do
    case Integer.parse(value) do
      {int, ""} when int >= @min_int64 and int <= @max_int64 -> int
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_scan_cursor(value) do
    case CollectionScan.parse_cursor(value) do
      {:ok, cursor} -> cursor
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_pos_int(value) do
    case parse_int(value) do
      int when is_integer(int) and int > 0 -> int
      {:error, _} = error -> error
      _ -> {:error, "ERR value must be a positive integer"}
    end
  end

  defp parse_float(value) do
    case Float.parse(value) do
      {float, ""} -> float
      _ -> {:error, "ERR value is not a valid float"}
    end
  rescue
    ArgumentError -> {:error, "ERR value is not a valid float"}
  end

  defp parse_pos_float(value) do
    case parse_float(value) do
      float when is_float(float) and float > 0.0 -> float
      {:error, _} = error -> error
      _ -> {:error, "ERR value must be a positive number"}
    end
  end

  defp parse_probability_float(value) do
    case parse_float(value) do
      float when is_float(float) and float > 0.0 and float < 1.0 -> float
      {:error, _} = error -> error
      _ -> {:error, "ERR value must be between 0 and 1 exclusive"}
    end
  end

  defp parse_probability_inclusive_float(value) do
    case parse_float(value) do
      float when is_float(float) and float >= 0.0 and float <= 1.0 -> float
      {:error, _} = error -> error
      _ -> {:error, "ERR value must be between 0 and 1"}
    end
  end

  defp with_parsed_numbers(tag, values, fun) do
    case Enum.find(values, &match?({:error, _}, &1)) do
      {:error, msg} -> {tag, {:error, msg}}
      nil -> fun.(values)
    end
  end

  defp parse_int_list(values), do: parse_list(values, &parse_int/1)
  defp parse_float_list(values), do: parse_list(values, &parse_float/1)

  defp parse_list(values, parser) do
    values
    |> Enum.reduce_while([], fn value, acc ->
      case parser.(value) do
        {:error, msg} -> {:halt, {:error, msg}}
        parsed -> {:cont, [parsed | acc]}
      end
    end)
    |> case do
      {:error, _} = error -> error
      parsed -> {:ok, Enum.reverse(parsed)}
    end
  end

  defp parse_element_count_pairs(tag, args) do
    if rem(length(args), 2) != 0 do
      {:error, "ERR wrong number of arguments for '#{prob_command_name(tag)}' command"}
    else
      args
      |> Enum.chunk_every(2)
      |> Enum.reduce_while([], fn [element, count], acc ->
        case parse_pos_int(count) do
          parsed_count when is_integer(parsed_count) -> {:cont, [{element, parsed_count} | acc]}
          {:error, msg} -> {:halt, {:error, msg}}
        end
      end)
      |> case do
        {:error, _} = error -> error
        parsed -> {:ok, Enum.reverse(parsed)}
      end
    end
  end

  defp parse_cms_merge_args(args, count) when length(args) < count,
    do: {:error, "ERR wrong number of arguments for 'cms.merge' command"}

  defp parse_cms_merge_args(args, count) do
    {src_keys, tail} = Enum.split(args, count)

    case tail do
      [] ->
        {:ok, src_keys, List.duplicate(1, count)}

      [option | weights] ->
        cond do
          String.upcase(option) != "WEIGHTS" ->
            {:error, "ERR syntax error in 'cms.merge' command"}

          length(weights) != count ->
            {:error, "ERR wrong number of weights for 'cms.merge' command"}

          true ->
            case parse_int_list(weights) do
              {:ok, parsed_weights} -> {:ok, src_keys, parsed_weights}
              {:error, msg} -> {:error, msg}
            end
        end

      _ ->
        {:error, "ERR syntax error in 'cms.merge' command"}
    end
  end

  defp parse_tdigest_merge_args(args, count) when length(args) < count,
    do: {:error, "ERR wrong number of arguments for 'tdigest.merge' command"}

  defp parse_tdigest_merge_args(args, count) do
    {src_keys, tail} = Enum.split(args, count)
    parse_tdigest_merge_opts(src_keys, tail, [])
  end

  defp parse_tdigest_merge_opts(src_keys, [], opts), do: {:ok, src_keys, Enum.reverse(opts)}

  defp parse_tdigest_merge_opts(src_keys, [option | rest], opts) do
    case {String.upcase(option), rest} do
      {"OVERRIDE", rest} ->
        parse_tdigest_merge_opts(src_keys, rest, [{:override, true} | opts])

      {"COMPRESSION", [value | rest]} ->
        case parse_pos_int(value) do
          compression when is_integer(compression) ->
            parse_tdigest_merge_opts(src_keys, rest, [{:compression, compression} | opts])

          {:error, msg} ->
            {:error, msg}
        end

      _invalid ->
        {:error, "ERR syntax error in 'tdigest.merge' command"}
    end
  end

  defp make_tdigest_trimmed_mean_ast(key, low, high)
       when is_float(low) and is_float(high) and low >= 0.0 and high <= 1.0 and low < high,
       do: {:tdigest_trimmed_mean, key, low, high}

  defp make_tdigest_trimmed_mean_ast(_key, {:error, msg}, _high),
    do: {:tdigest_trimmed_mean, {:error, msg}}

  defp make_tdigest_trimmed_mean_ast(_key, _low, {:error, msg}),
    do: {:tdigest_trimmed_mean, {:error, msg}}

  defp make_tdigest_trimmed_mean_ast(_key, _low, _high),
    do:
      {:tdigest_trimmed_mean,
       {:error, "ERR TDIGEST: low_quantile must be less than high_quantile in [0, 1]"}}

  defp prob_command_name(:cms_incrby), do: "cms.incrby"
  defp prob_command_name(:topk_incrby), do: "topk.incrby"

  defp normalize_scoped_management_subcommand(cmd, [subcommand | rest])
       when cmd in @management_scoped_commands,
       do: [String.upcase(subcommand) | rest]

  defp normalize_scoped_management_subcommand(_cmd, args), do: args

  defp normalize_command_args(args) do
    args
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case safe_command_binary(value) do
        {:ok, binary} -> {:cont, {:ok, [binary | acc]}}
        :error -> {:halt, {:error, "ERR protocol error"}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _message} = error -> error
    end
  end

  defp safe_command_binary(value) do
    {:ok, to_command_binary(value)}
  rescue
    _exception -> :error
  end

  defp to_command_binary(value) when is_binary(value), do: value
  defp to_command_binary(value), do: to_string(value)
end
