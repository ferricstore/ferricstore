defmodule Ferricstore.Commands.Dispatcher do
  @moduledoc """
  Routes Redis command names to the appropriate handler module.

  The dispatcher normalises the command name to uppercase and delegates to one of:

    * `Ferricstore.Commands.Strings` — GET, SET, DEL, EXISTS, MGET, MSET,
      INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETSET, GETDEL,
      GETEX, SETNX, SETEX, PSETEX, GETRANGE, SETRANGE, MSETNX
    * `Ferricstore.Commands.Expiry` — EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST
    * `Ferricstore.Commands.Generic` — TYPE, UNLINK, RENAME, RENAMENX, COPY,
      RANDOMKEY, SCAN, EXPIRETIME, PEXPIRETIME, OBJECT, WAIT
    * `Ferricstore.Commands.Server` — PING, ECHO, DBSIZE, KEYS, FLUSHDB, FLUSHALL,
      INFO, COMMAND, SELECT, LOLWUT, DEBUG

  Multi-word commands (`CLIENT`, `COMMAND`) are routed based on the first word, then
  the subcommand is extracted from args. `CLIENT` subcommands require connection
  state and are dispatched via `dispatch_client/3`.

  Unknown commands return `{:error, "ERR unknown command ..."}`.
  """

  alias Ferricstore.Commands.{
    Bitmap,
    Bloom,
    Cluster,
    Cuckoo,
    Expiry,
    Flow,
    Generic,
    Geo,
    Hash,
    HyperLogLog,
    Json,
    List,
    Memory,
    Namespace,
    Native,
    PubSub,
    Server,
    Set,
    SortedSet,
    Stream,
    Strings
  }

  alias Ferricstore.Commands.CMS
  alias Ferricstore.Commands.TDigest
  alias Ferricstore.Commands.TopK

  @ast_command_names %{
    get: "get",
    set: "set",
    del: "del",
    exists: "exists",
    mget: "mget",
    mset: "mset",
    incr: "incr",
    decr: "decr",
    incrby: "incrby",
    decrby: "decrby",
    incrbyfloat: "incrbyfloat",
    append: "append",
    strlen: "strlen",
    getset: "getset",
    getdel: "getdel",
    getex: "getex",
    setnx: "setnx",
    setex: "setex",
    psetex: "psetex",
    getrange: "getrange",
    setrange: "setrange",
    msetnx: "msetnx",
    expire: "expire",
    pexpire: "pexpire",
    expireat: "expireat",
    pexpireat: "pexpireat",
    ttl: "ttl",
    pttl: "pttl",
    persist: "persist",
    lpush: "lpush",
    rpush: "rpush",
    lpop: "lpop",
    rpop: "rpop",
    lrange: "lrange",
    llen: "llen",
    lindex: "lindex",
    lset: "lset",
    lrem: "lrem",
    ltrim: "ltrim",
    linsert: "linsert",
    lmove: "lmove",
    lpushx: "lpushx",
    rpushx: "rpushx",
    rpoplpush: "rpoplpush",
    hset: "hset",
    hget: "hget",
    hdel: "hdel",
    hmget: "hmget",
    hgetall: "hgetall",
    hexists: "hexists",
    hkeys: "hkeys",
    hvals: "hvals",
    hlen: "hlen",
    hincrby: "hincrby",
    hincrbyfloat: "hincrbyfloat",
    hsetnx: "hsetnx",
    hstrlen: "hstrlen",
    hrandfield: "hrandfield",
    hscan: "hscan",
    hexpire: "hexpire",
    httl: "httl",
    hpersist: "hpersist",
    hpexpire: "hpexpire",
    hpttl: "hpttl",
    hexpiretime: "hexpiretime",
    hgetdel: "hgetdel",
    hgetex: "hgetex",
    hsetex: "hsetex",
    sadd: "sadd",
    srem: "srem",
    smembers: "smembers",
    sismember: "sismember",
    smismember: "smismember",
    scard: "scard",
    sinter: "sinter",
    sunion: "sunion",
    sdiff: "sdiff",
    sdiffstore: "sdiffstore",
    sinterstore: "sinterstore",
    sunionstore: "sunionstore",
    sintercard: "sintercard",
    srandmember: "srandmember",
    spop: "spop",
    smove: "smove",
    sscan: "sscan",
    zadd: "zadd",
    zrem: "zrem",
    zscore: "zscore",
    zrank: "zrank",
    zrevrank: "zrevrank",
    zrange: "zrange",
    zrevrange: "zrevrange",
    zcard: "zcard",
    zincrby: "zincrby",
    zcount: "zcount",
    zpopmin: "zpopmin",
    zpopmax: "zpopmax",
    zrandmember: "zrandmember",
    zscan: "zscan",
    zmscore: "zmscore",
    zrangebyscore: "zrangebyscore",
    zrevrangebyscore: "zrevrangebyscore",
    setbit: "setbit",
    getbit: "getbit",
    bitcount: "bitcount",
    bitpos: "bitpos",
    bitop: "bitop",
    type: "type",
    unlink: "unlink",
    rename: "rename",
    renamenx: "renamenx",
    copy: "copy",
    randomkey: "randomkey",
    scan: "scan",
    expiretime: "expiretime",
    pexpiretime: "pexpiretime",
    object: "object",
    wait: "wait",
    xadd: "xadd",
    xlen: "xlen",
    xrange: "xrange",
    xrevrange: "xrevrange",
    xread: "xread",
    xtrim: "xtrim",
    xdel: "xdel",
    xinfo: "xinfo",
    xgroup: "xgroup",
    xreadgroup: "xreadgroup",
    xack: "xack",
    json_set: "json.set",
    json_get: "json.get",
    json_del: "json.del",
    json_numincrby: "json.numincrby",
    json_type: "json.type",
    json_strlen: "json.strlen",
    json_objkeys: "json.objkeys",
    json_objlen: "json.objlen",
    json_arrappend: "json.arrappend",
    json_arrlen: "json.arrlen",
    json_toggle: "json.toggle",
    json_clear: "json.clear",
    json_mget: "json.mget",
    geoadd: "geoadd",
    geopos: "geopos",
    geodist: "geodist",
    geohash: "geohash",
    geosearch: "geosearch",
    geosearchstore: "geosearchstore",
    bf_reserve: "bf.reserve",
    bf_add: "bf.add",
    bf_madd: "bf.madd",
    bf_exists: "bf.exists",
    bf_mexists: "bf.mexists",
    bf_card: "bf.card",
    bf_info: "bf.info",
    cf_reserve: "cf.reserve",
    cf_add: "cf.add",
    cf_addnx: "cf.addnx",
    cf_del: "cf.del",
    cf_exists: "cf.exists",
    cf_mexists: "cf.mexists",
    cf_count: "cf.count",
    cf_info: "cf.info",
    cms_initbydim: "cms.initbydim",
    cms_initbyprob: "cms.initbyprob",
    cms_incrby: "cms.incrby",
    cms_query: "cms.query",
    cms_merge: "cms.merge",
    cms_info: "cms.info",
    topk_reserve: "topk.reserve",
    topk_add: "topk.add",
    topk_incrby: "topk.incrby",
    topk_query: "topk.query",
    topk_list: "topk.list",
    topk_count: "topk.count",
    topk_info: "topk.info",
    tdigest_create: "tdigest.create",
    tdigest_add: "tdigest.add",
    tdigest_reset: "tdigest.reset",
    tdigest_quantile: "tdigest.quantile",
    tdigest_cdf: "tdigest.cdf",
    tdigest_rank: "tdigest.rank",
    tdigest_revrank: "tdigest.revrank",
    tdigest_byrank: "tdigest.byrank",
    tdigest_byrevrank: "tdigest.byrevrank",
    tdigest_trimmed_mean: "tdigest.trimmed_mean",
    tdigest_min: "tdigest.min",
    tdigest_max: "tdigest.max",
    tdigest_info: "tdigest.info",
    tdigest_merge: "tdigest.merge"
  }

  @single_value_ast_tags ~w(get incr decr strlen getdel getex ttl pttl persist expiretime pexpiretime llen lpop rpop hgetall hkeys hvals hlen hrandfield hscan httl hpersist hpttl hexpiretime hgetdel smembers scard srandmember spop zscore zrank zrevrank zcard zpopmin zpopmax zrandmember bitcount bitpos type xlen)a

  @doc """
  Dispatches a command AST produced by the Rust RESP parser.
  """
  @spec dispatch_ast(term(), map()) :: term()
  def dispatch_ast(:ping, store), do: Server.handle("PING", [], store)

  def dispatch_ast({:ping, args}, store) when is_list(args),
    do: Server.handle("PING", args, store)

  def dispatch_ast({tag, args}, _store) when tag in @single_value_ast_tags and is_list(args),
    do: wrong_arity_ast(tag)

  def dispatch_ast({:ping, arg}, store), do: Server.handle("PING", [arg], store)

  def dispatch_ast({:get, args}, store) when is_list(args),
    do: Strings.handle_ast({:get, args}, store)

  def dispatch_ast({:get, key}, store), do: Strings.handle_ast({:get, key}, store)
  def dispatch_ast({:set, _key, _value} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:set, _key, _value, _opts} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:del, _args} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:exists, _args} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:mget, _args} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:mset, _args} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:incr, _key} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:decr, _key} = ast, store), do: Strings.handle_ast(ast, store)

  def dispatch_ast({:incrby, _key, _delta} = ast, store),
    do: Strings.handle_ast(ast, store)

  def dispatch_ast({:decrby, _key, _delta} = ast, store),
    do: Strings.handle_ast(ast, store)

  def dispatch_ast({:incrbyfloat, _key, _delta} = ast, store),
    do: Strings.handle_ast(ast, store)

  def dispatch_ast({:append, _key, _value} = ast, store), do: Strings.handle_ast(ast, store)

  def dispatch_ast({:strlen, _key} = ast, store), do: Strings.handle_ast(ast, store)

  def dispatch_ast({:getset, _key, _value} = ast, store), do: Strings.handle_ast(ast, store)

  def dispatch_ast({:getdel, _key} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:getex, _key} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:getex, _key, _opts} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:setnx, _key, _value} = ast, store), do: Strings.handle_ast(ast, store)

  def dispatch_ast({:setex, _key, _seconds, _value} = ast, store),
    do: Strings.handle_ast(ast, store)

  def dispatch_ast({:psetex, _key, _ms, _value} = ast, store),
    do: Strings.handle_ast(ast, store)

  def dispatch_ast({:getrange, _key, _start, _stop} = ast, store),
    do: Strings.handle_ast(ast, store)

  def dispatch_ast({:setrange, _key, _offset, _value} = ast, store),
    do: Strings.handle_ast(ast, store)

  def dispatch_ast({:msetnx, _args} = ast, store), do: Strings.handle_ast(ast, store)
  def dispatch_ast({:expire, _key, _ttl} = ast, store), do: Expiry.handle_ast(ast, store)
  def dispatch_ast({:expire, _key, _ttl, _flag} = ast, store), do: Expiry.handle_ast(ast, store)
  def dispatch_ast({:pexpire, _key, _ttl} = ast, store), do: Expiry.handle_ast(ast, store)
  def dispatch_ast({:pexpire, _key, _ttl, _flag} = ast, store), do: Expiry.handle_ast(ast, store)
  def dispatch_ast({:expireat, _key, _ttl} = ast, store), do: Expiry.handle_ast(ast, store)
  def dispatch_ast({:expireat, _key, _ttl, _flag} = ast, store), do: Expiry.handle_ast(ast, store)
  def dispatch_ast({:pexpireat, _key, _ttl} = ast, store), do: Expiry.handle_ast(ast, store)

  def dispatch_ast({:pexpireat, _key, _ttl, _flag} = ast, store),
    do: Expiry.handle_ast(ast, store)

  def dispatch_ast({:ttl, key}, store), do: Expiry.handle_ast({:ttl, key}, store)
  def dispatch_ast({:pttl, key}, store), do: Expiry.handle_ast({:pttl, key}, store)
  def dispatch_ast({:persist, key}, store), do: Expiry.handle_ast({:persist, key}, store)
  def dispatch_ast({:lpush, _args} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:rpush, _args} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:lpop, _key} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:rpop, _key} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:lpop, _key, _count} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:rpop, _key, _count} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:lrange, _key, _start, _stop} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:llen, _key} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:lindex, _key, _index} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:lset, _key, _index, _element} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:lrem, _key, _count, _element} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:ltrim, _key, _start, _stop} = ast, store), do: List.handle_ast(ast, store)

  def dispatch_ast({:linsert, _key, _direction, _pivot, _element} = ast, store),
    do: List.handle_ast(ast, store)

  def dispatch_ast({:lmove, _source, _destination, _from_dir, _to_dir} = ast, store),
    do: List.handle_ast(ast, store)

  def dispatch_ast({:lpushx, _args} = ast, store), do: List.handle_ast(ast, store)
  def dispatch_ast({:rpushx, _args} = ast, store), do: List.handle_ast(ast, store)

  def dispatch_ast({:rpoplpush, _source, _destination} = ast, store),
    do: List.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(hset hdel hmget hgetall hkeys hvals hlen hrandfield hscan httl hpersist hpttl hexpiretime hgetdel)a,
      do: Hash.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(hget hexists hstrlen hrandfield hscan hexpire hpexpire httl hpersist hpttl hexpiretime hgetdel hgetex)a,
      do: Hash.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(hincrby hincrbyfloat hsetnx hrandfield hscan hexpire hpexpire hgetex hsetex)a,
      do: Hash.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(sadd srem smembers smismember scard sinter sunion sdiff sdiffstore sinterstore sunionstore sintercard srandmember spop)a,
      do: Set.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(sismember srandmember spop sscan sintercard)a,
      do: Set.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(smove sscan)a,
      do: Set.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(zrem zcard zpopmin zpopmax zrandmember zmscore)a,
      do: SortedSet.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(zscore zrank zrevrank zadd zcount zpopmin zpopmax zscan zrangebyscore zrevrangebyscore)a,
      do: SortedSet.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(zadd zincrby zcount zrange zrevrange zrandmember zscan zrangebyscore zrevrangebyscore)a,
      do: SortedSet.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _, _} = ast, store)
      when tag in ~w(zrange zrevrange zrangebyscore zrevrangebyscore)a,
      do: SortedSet.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(bitcount bitop)a,
      do: Bitmap.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(getbit bitcount bitpos)a,
      do: Bitmap.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(setbit bitpos bitop)a,
      do: Bitmap.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(type unlink randomkey expiretime pexpiretime object)a,
      do: Generic.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(rename renamenx scan object wait)a,
      do: Generic.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(copy object)a,
      do: Generic.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(xadd xlen xread xinfo_stream xinfo xgroup xreadgroup)a,
      do: Stream.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(xadd xrange xrevrange xtrim xdel xgroup)a,
      do: Stream.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(xadd xread xreadgroup xack)a,
      do: Stream.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _, _} = ast, store)
      when tag in ~w(xrange xrevrange xgroup_create)a,
      do: Stream.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(json_set json_get json_del json_numincrby json_type json_strlen json_objkeys json_objlen json_arrappend json_arrlen json_toggle json_clear json_mget)a,
      do: Json.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(json_get json_del json_numincrby json_type json_strlen json_objkeys json_objlen json_arrlen json_toggle json_clear json_mget)a,
      do: Json.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(json_numincrby json_arrappend)a,
      do: Json.handle_ast(ast, store)

  def dispatch_ast({:json_set, _, _, _, _} = ast, store), do: Json.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(geoadd geopos geohash)a,
      do: Geo.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(geosearch)a,
      do: Geo.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(geoadd geosearchstore)a,
      do: Geo.handle_ast(ast, store)

  def dispatch_ast({:geodist, _, _, _, _} = ast, store), do: Geo.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(pfadd pfcount pfmerge)a,
      do: HyperLogLog.handle_ast(ast, store)

  def dispatch_ast({tag, _} = ast, store)
      when tag in ~w(bf_reserve bf_add bf_madd bf_exists bf_mexists bf_card bf_info cf_reserve cf_add cf_addnx cf_del cf_exists cf_mexists cf_count cf_info cms_initbydim cms_initbyprob cms_incrby cms_query cms_merge cms_info topk_reserve topk_add topk_incrby topk_query topk_list topk_count topk_info tdigest_create tdigest_add tdigest_reset tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank tdigest_byrank tdigest_byrevrank tdigest_trimmed_mean tdigest_min tdigest_max tdigest_info tdigest_merge)a,
      do: dispatch_prob_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(bf_reserve cf_reserve cms_initbydim cms_initbyprob cms_incrby cms_merge topk_reserve topk_incrby topk_list tdigest_create tdigest_add tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank tdigest_byrank tdigest_byrevrank tdigest_trimmed_mean tdigest_merge)a,
      do: dispatch_prob_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(bf_reserve cms_initbydim cms_initbyprob cms_merge tdigest_trimmed_mean tdigest_merge)a,
      do: dispatch_prob_ast(ast, store)

  def dispatch_ast({:topk_reserve, _, _, _, _, _} = ast, store), do: TopK.handle_ast(ast, store)

  def dispatch_ast({tag, args}, store)
      when tag in ~w(ping echo dbsize keys flushdb flushall info command select lolwut debug slowlog save bgsave lastsave config module waitaof)a,
      do: Server.handle(ast_command_name(tag), args, store)

  def dispatch_ast({tag, _args} = ast, store)
      when tag in ~w(cas lock unlock extend ratelimit_add ferricstore_key_info fetch_or_compute fetch_or_compute_result fetch_or_compute_error)a,
      do: Native.handle_ast(ast, store)

  def dispatch_ast({tag, _args} = ast, store)
      when tag in ~w(flow_create flow_get flow_claim_due flow_reclaim flow_complete flow_transition flow_retry flow_fail flow_cancel flow_rewind flow_list flow_info flow_stuck flow_history)a,
      do: Flow.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(unlock ferricstore_key_info fetch_or_compute_error)a,
      do: Native.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(flow_create flow_get flow_claim_due flow_reclaim flow_cancel flow_rewind flow_list flow_by_parent flow_by_root flow_by_correlation flow_info flow_stuck flow_history)a,
      do: Flow.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(lock extend fetch_or_compute fetch_or_compute_result fetch_or_compute_error)a,
      do: Native.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(flow_complete flow_retry flow_fail)a,
      do: Flow.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(flow_create_many flow_complete_many)a,
      do: Flow.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _, _} = ast, store)
      when tag in ~w(cas ratelimit_add)a,
      do: Native.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _, _} = ast, store)
      when tag in ~w(flow_transition flow_transition_many)a,
      do: Flow.handle_ast(ast, store)

  def dispatch_ast({:flow_transition_many, _, _, _, _, _} = ast, store),
    do: Flow.handle_ast(ast, store)

  def dispatch_ast({tag, _args} = ast, store)
      when tag in ~w(cluster_health cluster_stats cluster_keyslot cluster_slots cluster_status cluster_join cluster_leave cluster_failover cluster_promote cluster_demote cluster_role ferricstore_hotness)a,
      do: Cluster.handle(ast_command_name(tag), ast_args(ast), store)

  def dispatch_ast({:ferricstore_config, args}, store),
    do: Namespace.handle("FERRICSTORE.CONFIG", args, store)

  def dispatch_ast({:ferricstore_metrics, args}, _store),
    do: Ferricstore.Metrics.handle("FERRICSTORE.METRICS", args)

  def dispatch_ast({:memory, []}, _store),
    do: {:error, "ERR wrong number of arguments for 'memory' command"}

  def dispatch_ast({:memory, [subcmd | rest]}, store),
    do: Memory.handle(subcmd, rest, store)

  def dispatch_ast({:publish, args}, _store), do: PubSub.handle_ast({:publish, args})
  def dispatch_ast({:pubsub, args}, _store), do: PubSub.handle_ast({:pubsub, args})

  def dispatch_ast({:unknown, cmd, _args}, _store) when is_binary(cmd),
    do: unknown_command(cmd)

  def dispatch_ast({tag, args}, _store) when is_atom(tag) and is_list(args) do
    wrong_arity_ast(tag)
  end

  def dispatch_ast(_ast, _store), do: {:error, "ERR unsupported command AST"}

  defp wrong_arity_ast(tag) do
    case @ast_command_names do
      %{^tag => name} -> {:error, "ERR wrong number of arguments for '#{name}' command"}
      _ -> {:error, "ERR unsupported command AST"}
    end
  end

  defp unknown_command(cmd) when is_binary(cmd),
    do: {:error, "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "}

  defp dispatch_prob_ast({tag, _} = ast, store)
       when tag in ~w(bf_reserve bf_add bf_madd bf_exists bf_mexists bf_card bf_info)a,
       do: Bloom.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _, _} = ast, store) when tag in ~w(bf_reserve)a,
    do: Bloom.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _, _, _} = ast, store) when tag in ~w(bf_reserve)a,
    do: Bloom.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _} = ast, store)
       when tag in ~w(cf_reserve cf_add cf_addnx cf_del cf_exists cf_mexists cf_count cf_info)a,
       do: Cuckoo.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _, _} = ast, store) when tag in ~w(cf_reserve)a,
    do: Cuckoo.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _} = ast, store)
       when tag in ~w(cms_initbydim cms_initbyprob cms_incrby cms_query cms_merge cms_info)a,
       do: CMS.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _, _} = ast, store)
       when tag in ~w(cms_initbydim cms_initbyprob cms_incrby cms_merge)a,
       do: CMS.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _, _, _} = ast, store)
       when tag in ~w(cms_initbydim cms_initbyprob cms_merge)a,
       do: CMS.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _} = ast, store)
       when tag in ~w(topk_reserve topk_add topk_incrby topk_query topk_list topk_count topk_info)a,
       do: TopK.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _, _} = ast, store)
       when tag in ~w(topk_reserve topk_incrby topk_list)a,
       do: TopK.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _} = ast, store)
       when tag in ~w(tdigest_create tdigest_add tdigest_reset tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank tdigest_byrank tdigest_byrevrank tdigest_trimmed_mean tdigest_min tdigest_max tdigest_info tdigest_merge)a,
       do: TDigest.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _, _} = ast, store)
       when tag in ~w(tdigest_create tdigest_add tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank tdigest_byrank tdigest_byrevrank tdigest_trimmed_mean tdigest_merge)a,
       do: TDigest.handle_ast(ast, store)

  defp dispatch_prob_ast({tag, _, _, _} = ast, store)
       when tag in ~w(tdigest_trimmed_mean tdigest_merge)a,
       do: TDigest.handle_ast(ast, store)

  defp ast_args({_tag, args}) when is_list(args), do: args

  defp ast_command_name(:ping), do: "PING"
  defp ast_command_name(:echo), do: "ECHO"
  defp ast_command_name(:dbsize), do: "DBSIZE"
  defp ast_command_name(:keys), do: "KEYS"
  defp ast_command_name(:flushdb), do: "FLUSHDB"
  defp ast_command_name(:flushall), do: "FLUSHALL"
  defp ast_command_name(:info), do: "INFO"
  defp ast_command_name(:command), do: "COMMAND"
  defp ast_command_name(:select), do: "SELECT"
  defp ast_command_name(:lolwut), do: "LOLWUT"
  defp ast_command_name(:debug), do: "DEBUG"
  defp ast_command_name(:slowlog), do: "SLOWLOG"
  defp ast_command_name(:save), do: "SAVE"
  defp ast_command_name(:bgsave), do: "BGSAVE"
  defp ast_command_name(:lastsave), do: "LASTSAVE"
  defp ast_command_name(:config), do: "CONFIG"
  defp ast_command_name(:module), do: "MODULE"
  defp ast_command_name(:waitaof), do: "WAITAOF"
  defp ast_command_name(:cluster_health), do: "CLUSTER.HEALTH"
  defp ast_command_name(:cluster_stats), do: "CLUSTER.STATS"
  defp ast_command_name(:cluster_keyslot), do: "CLUSTER.KEYSLOT"
  defp ast_command_name(:cluster_slots), do: "CLUSTER.SLOTS"
  defp ast_command_name(:cluster_status), do: "CLUSTER.STATUS"
  defp ast_command_name(:cluster_join), do: "CLUSTER.JOIN"
  defp ast_command_name(:cluster_leave), do: "CLUSTER.LEAVE"
  defp ast_command_name(:cluster_failover), do: "CLUSTER.FAILOVER"
  defp ast_command_name(:cluster_promote), do: "CLUSTER.PROMOTE"
  defp ast_command_name(:cluster_demote), do: "CLUSTER.DEMOTE"
  defp ast_command_name(:cluster_role), do: "CLUSTER.ROLE"
  defp ast_command_name(:ferricstore_hotness), do: "FERRICSTORE.HOTNESS"

  if Mix.env() == :test do
    @doc """
    Test-only convenience wrapper. It encodes the given command as one RESP
    frame, lets the Rust parser build the AST, then dispatches that AST.
    """
    @spec dispatch(binary(), [binary()], map()) :: term()
    def dispatch("", _args, _store), do: unknown_command("")

    def dispatch(name, args, store) do
      start = System.monotonic_time(:microsecond)
      frame = encode_test_command(name, args)
      parser = Ferricstore.Resp.Parser

      result =
        case apply(parser, :parse_commands, [frame]) do
          {:ok, [{:command, cmd, parsed_args, ast, _keys}], ""} ->
            dispatch_ast(ast, store)
            |> tap(fn _ -> log_test_dispatch(cmd, parsed_args, start) end)

          {:ok, _other, _rest} ->
            {:error, "ERR protocol error"}

          {:error, reason} ->
            {:error, "ERR protocol error #{inspect(reason)}"}
        end

      result
    end

    defp log_test_dispatch(cmd, args, start) do
      duration = System.monotonic_time(:microsecond) - start
      Ferricstore.SlowLog.maybe_log([cmd | args], duration)
    end

    defp encode_test_command(name, args) do
      parts = [to_command_binary(name) | Enum.map(args, &to_command_binary/1)]

      IO.iodata_to_binary([
        "*",
        Integer.to_string(length(parts)),
        "\r\n",
        Enum.map(parts, fn part ->
          ["$", Integer.to_string(byte_size(part)), "\r\n", part, "\r\n"]
        end)
      ])
    end

    defp to_command_binary(value) when is_binary(value), do: value
    defp to_command_binary(value), do: to_string(value)
  end
end
