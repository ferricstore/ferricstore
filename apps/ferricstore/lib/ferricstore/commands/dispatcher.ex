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
    Extension,
    Flow,
    Generic,
    Geo,
    Hash,
    HyperLogLog,
    List,
    Management,
    Memory,
    Namespace,
    Native,
    PreparedCommand,
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
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Store.Router

  @string_raw_commands ~w(GET SET DEL EXISTS MGET MSET INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND STRLEN GETSET GETDEL GETEX SETNX SETEX PSETEX GETRANGE SETRANGE MSETNX)
  @expiry_raw_commands ~w(EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST)
  @list_raw_commands ~w(LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX LSET LREM LTRIM LPOS LINSERT LMOVE RPOPLPUSH LPUSHX RPUSHX)
  @hash_raw_commands ~w(HSET HDEL HMGET HGET HGETALL HEXISTS HKEYS HVALS HLEN HSETNX HSTRLEN HSCAN HINCRBY HINCRBYFLOAT HRANDFIELD HEXPIRE HPEXPIRE HGETEX HSETEX HTTL HPTTL HPERSIST HEXPIRETIME HGETDEL)
  @set_raw_commands ~w(SADD SREM SMISMEMBER SINTER SUNION SDIFF SDIFFSTORE SINTERSTORE SUNIONSTORE SMEMBERS SCARD SISMEMBER SRANDMEMBER SPOP SMOVE SSCAN SINTERCARD)
  @zset_raw_commands ~w(ZADD ZREM ZSCORE ZRANK ZREVRANK ZCARD ZINCRBY ZCOUNT ZPOPMIN ZPOPMAX ZRANDMEMBER ZMSCORE ZRANGE ZREVRANGE ZSCAN ZRANGEBYSCORE ZREVRANGEBYSCORE)
  @bitmap_raw_commands ~w(GETBIT SETBIT BITCOUNT BITPOS BITOP)
  @generic_raw_commands ~w(TYPE UNLINK RENAME RENAMENX COPY RANDOMKEY SCAN EXPIRETIME PEXPIRETIME OBJECT WAIT)
  @server_raw_commands ~w(PING ECHO DBSIZE KEYS FLUSHDB FLUSHALL SELECT INFO COMMAND LOLWUT DEBUG SLOWLOG SAVE BGSAVE LASTSAVE CONFIG MODULE WAITAOF FERRICSTORE.BLOBGC FERRICSTORE.DOCTOR)
  @stream_raw_commands ~w(XADD XLEN XRANGE XREVRANGE XREAD XTRIM XDEL XINFO XGROUP XREADGROUP XACK)
  @geo_raw_commands ~w(GEOADD GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE)
  @hll_raw_commands ~w(PFADD PFCOUNT PFMERGE)
  @prob_raw_commands ~w(BF.RESERVE BF.ADD BF.MADD BF.EXISTS BF.MEXISTS BF.CARD BF.INFO CF.RESERVE CF.ADD CF.ADDNX CF.DEL CF.EXISTS CF.MEXISTS CF.COUNT CF.INFO CMS.INITBYDIM CMS.INITBYPROB CMS.INCRBY CMS.QUERY CMS.MERGE CMS.INFO TOPK.RESERVE TOPK.ADD TOPK.INCRBY TOPK.QUERY TOPK.LIST TOPK.COUNT TOPK.INFO TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET TDIGEST.QUANTILE TDIGEST.CDF TDIGEST.RANK TDIGEST.REVRANK TDIGEST.BYRANK TDIGEST.BYREVRANK TDIGEST.TRIMMED_MEAN TDIGEST.MIN TDIGEST.MAX TDIGEST.INFO TDIGEST.MERGE)
  @tdigest_rmw_commands ~w(TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET TDIGEST.MERGE)
  @native_raw_commands ~w(CAS LOCK UNLOCK EXTEND RATELIMIT.ADD KEY_INFO FERRICSTORE.KEY_INFO FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT FETCH_OR_COMPUTE_ERROR)
  @management_raw_commands ~w(ACL FERRICSTORE.CAPABILITIES FERRICSTORE.NAMESPACE FERRICSTORE.QUOTA FERRICSTORE.TELEMETRY)
  @cluster_raw_commands ~w(CLUSTER.HEALTH CLUSTER.STATS CLUSTER.KEYSLOT CLUSTER.SLOTS CLUSTER.STATUS CLUSTER.JOIN CLUSTER.LEAVE CLUSTER.FAILOVER CLUSTER.PROMOTE CLUSTER.DEMOTE CLUSTER.ROLE FERRICSTORE.HOTNESS)
  @raw_fallback_ast_tags ~w(xadd xlen xrange xrevrange xread xtrim xdel xinfo xgroup xreadgroup xack geoadd geopos geodist geohash geosearch geosearchstore cas lock unlock extend ratelimit_add ferricstore_key_info fetch_or_compute fetch_or_compute_result fetch_or_compute_error)a
  @wrong_arity_list_ast_tags ~w(get incr decr strlen getdel getex ttl pttl persist lpop rpop llen hgetall hkeys hvals hlen hrandfield smembers scard srandmember spop zcard zpopmin zpopmax zrandmember type expiretime pexpiretime)a

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
    lpos: "lpos",
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
    tdigest_merge: "tdigest.merge",
    cas: "cas",
    lock: "lock",
    unlock: "unlock",
    extend: "extend",
    ratelimit_add: "ratelimit.add",
    ferricstore_key_info: "ferricstore.key_info",
    fetch_or_compute: "fetch_or_compute",
    fetch_or_compute_result: "fetch_or_compute_result",
    fetch_or_compute_error: "fetch_or_compute_error",
    ferricstore_capabilities: "ferricstore.capabilities",
    ferricstore_namespace: "ferricstore.namespace",
    ferricstore_quota: "ferricstore.quota",
    ferricstore_telemetry: "ferricstore.telemetry",
    ferricstore_blobgc: "ferricstore.blobgc",
    ferricstore_doctor: "ferricstore.doctor"
  }

  @doc """
  Dispatches a command AST produced by the native AST parser.
  """
  @spec dispatch_ast(term(), map()) :: term()
  def dispatch_ast(:ping, store), do: Server.handle("PING", [], store)

  def dispatch_ast({:ping, args}, store) when is_list(args),
    do: Server.handle("PING", args, store)

  def dispatch_ast({tag, args}, store) when tag in @raw_fallback_ast_tags and is_list(args),
    do: dispatch_raw_handler(ast_command_name(tag), args, store)

  def dispatch_ast({tag, args}, _store)
      when tag in @wrong_arity_list_ast_tags and is_list(args),
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
  def dispatch_ast({:lpos, _args} = ast, store), do: List.handle_ast(ast, store)

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

  def dispatch_ast({tag, _key, {:error, reason}}, _store)
      when tag in ~w(zcount zrangebyscore zrevrangebyscore)a,
      do: {:error, reason}

  def dispatch_ast({tag, key, min, max}, store)
      when tag in ~w(zcount zrangebyscore zrevrangebyscore)a and
             (is_binary(min) or is_binary(max)),
      do: SortedSet.handle(ast_command_name(tag), [key, min, max], store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(zcount zrangebyscore zrevrangebyscore)a,
      do: SortedSet.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(zscore zrank zrevrank zadd zpopmin zpopmax zscan)a,
      do: SortedSet.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(zadd zincrby zrange zrevrange zrandmember zscan)a,
      do: SortedSet.handle_ast(ast, store)

  def dispatch_ast({tag, key, min, max, opts}, store)
      when tag in ~w(zrangebyscore zrevrangebyscore)a and is_list(opts) and
             (is_binary(min) or is_binary(max)),
      do: SortedSet.handle(ast_command_name(tag), [key, min, max | opts], store)

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

  def dispatch_ast({:topk_reserve, _, _, _, _} = ast, store), do: TopK.handle_ast(ast, store)

  def dispatch_ast({tag, args}, store)
      when tag in ~w(ping echo dbsize keys flushdb flushall info command select lolwut debug slowlog save bgsave lastsave config module waitaof)a,
      do: Server.handle(ast_command_name(tag), args, store)

  def dispatch_ast({tag, _args} = ast, store)
      when tag in ~w(cas lock unlock extend ratelimit_add ferricstore_key_info fetch_or_compute fetch_or_compute_result fetch_or_compute_error)a,
      do: Native.handle_ast(ast, store)

  def dispatch_ast({tag, _args} = ast, store)
      when tag in ~w(flow_create flow_value_put flow_signal flow_get flow_claim_due flow_reclaim flow_complete flow_transition flow_retry flow_fail flow_cancel flow_rewind flow_list flow_search flow_attributes flow_terminals flow_failures flow_info flow_stuck flow_history flow_retention_cleanup)a,
      do: Flow.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(unlock ferricstore_key_info fetch_or_compute_error)a,
      do: Native.handle_ast(ast, store)

  def dispatch_ast({tag, _, _} = ast, store)
      when tag in ~w(flow_create flow_value_put flow_signal flow_get flow_policy_set flow_policy_get flow_claim_due flow_reclaim flow_cancel flow_rewind flow_list flow_attributes flow_stats flow_terminals flow_failures flow_by_parent flow_by_root flow_by_correlation flow_info flow_stuck flow_history flow_create_many flow_complete_many flow_retry_many flow_fail_many flow_cancel_many)a,
      do: Flow.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(lock extend fetch_or_compute fetch_or_compute_result fetch_or_compute_error)a,
      do: Native.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(flow_extend_lease flow_complete flow_retry flow_fail flow_attribute_values)a,
      do: Flow.handle_ast(ast, store)

  def dispatch_ast({tag, _, _, _} = ast, store)
      when tag in ~w(flow_create_many flow_spawn_children flow_complete_many flow_retry_many flow_fail_many flow_cancel_many)a,
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
    do: safe_ferricstore_metrics(args)

  def dispatch_ast({:acl, {:error, _} = err}, _store), do: err

  def dispatch_ast({:acl, subcmd, args}, store) when is_binary(subcmd) and is_list(args),
    do: Management.handle("ACL", [subcmd | args], store)

  def dispatch_ast({:ferricstore_capabilities, args}, store),
    do: Management.handle("FERRICSTORE.CAPABILITIES", args, store)

  def dispatch_ast({:ferricstore_namespace, args}, store),
    do: Management.handle("FERRICSTORE.NAMESPACE", args, store)

  def dispatch_ast({:ferricstore_quota, args}, store),
    do: Management.handle("FERRICSTORE.QUOTA", args, store)

  def dispatch_ast({:ferricstore_telemetry, args}, store),
    do: Management.handle("FERRICSTORE.TELEMETRY", args, store)

  def dispatch_ast({:ferricstore_blobgc, args}, store),
    do: Server.handle("FERRICSTORE.BLOBGC", args, store)

  def dispatch_ast({:ferricstore_doctor, args}, store),
    do: Server.handle("FERRICSTORE.DOCTOR", args, store)

  def dispatch_ast({:memory, []}, _store),
    do: {:error, "ERR wrong number of arguments for 'memory' command"}

  def dispatch_ast({:memory, [subcmd | rest]}, store),
    do: Memory.handle(subcmd, rest, store)

  def dispatch_ast({:publish, args}, _store), do: PubSub.handle_ast({:publish, args})
  def dispatch_ast({:pubsub, args}, _store), do: PubSub.handle_ast({:pubsub, args})

  def dispatch_ast({:extension_command, module, cmd, args, access}, store)
      when is_atom(module) and is_binary(cmd) and is_list(args) and
             access in [:read, :write, :rw],
      do: Extension.handle_prepared(module, cmd, args, store)

  def dispatch_ast({:structured_native_command, cmd}, _store) when is_binary(cmd),
    do: {:error, "ERR command '#{String.downcase(cmd)}' requires its structured native opcode"}

  def dispatch_ast({:raw_command, cmd, args}, store) when is_binary(cmd) and is_list(args),
    do: dispatch_raw_handler(cmd, args, store)

  def dispatch_ast({:unknown, cmd, _args}, _store) when is_binary(cmd),
    do: unknown_command(cmd)

  def dispatch_ast({tag, args}, _store) when is_atom(tag) and is_list(args),
    do: wrong_arity_ast(tag)

  def dispatch_ast(_ast, _store), do: {:error, "ERR unsupported command AST"}

  defp safe_ferricstore_metrics(args) do
    Ferricstore.Metrics.handle("FERRICSTORE.METRICS", args)
  rescue
    _ -> ""
  catch
    :exit, _ -> ""
  end

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

  defp ast_command_name(tag) when is_atom(tag) do
    case @ast_command_names do
      %{^tag => name} -> String.upcase(name)
      _ -> nil
    end
  end

  defp dispatch_raw_handler(cmd, args, store) do
    cond do
      cmd in @string_raw_commands -> Strings.handle(cmd, args, store)
      cmd in @expiry_raw_commands -> Expiry.handle(cmd, args, store)
      cmd in @list_raw_commands -> List.handle(cmd, args, store)
      cmd in @hash_raw_commands -> Hash.handle(cmd, args, store)
      cmd in @set_raw_commands -> Set.handle(cmd, args, store)
      cmd in @zset_raw_commands -> SortedSet.handle(cmd, args, store)
      cmd in @bitmap_raw_commands -> Bitmap.handle(cmd, args, store)
      cmd in @generic_raw_commands -> Generic.handle(cmd, args, store)
      cmd == "FERRICSTORE.METRICS" -> safe_ferricstore_metrics(args)
      cmd in @server_raw_commands -> Server.handle(cmd, args, store)
      cmd in @stream_raw_commands -> Stream.handle(cmd, args, store)
      cmd in @geo_raw_commands -> Geo.handle(cmd, args, store)
      cmd in @hll_raw_commands -> HyperLogLog.handle(cmd, args, store)
      cmd in @native_raw_commands -> Native.handle(native_raw_command_name(cmd), args, store)
      cmd in @management_raw_commands -> Management.handle(cmd, args, store)
      cmd in @cluster_raw_commands -> Cluster.handle(cmd, args, store)
      cmd in @prob_raw_commands -> dispatch_prob_raw(cmd, args, store)
      cmd == "FERRICSTORE.CONFIG" -> Namespace.handle(cmd, args, store)
      cmd == "MEMORY" -> dispatch_memory_raw(args, store)
      cmd == "PUBLISH" or cmd == "PUBSUB" -> PubSub.handle(cmd, args)
      true -> dispatch_extension_raw(cmd, args, store)
    end
  end

  defp dispatch_extension_raw(cmd, args, store) do
    case Extension.handle(cmd, args, store) do
      :not_found -> unknown_command(cmd)
      result -> result
    end
  end

  defp native_raw_command_name("FERRICSTORE.KEY_INFO"), do: "KEY_INFO"
  defp native_raw_command_name(cmd), do: cmd

  defp dispatch_prob_raw("BF." <> _ = cmd, args, store), do: Bloom.handle(cmd, args, store)
  defp dispatch_prob_raw("CF." <> _ = cmd, args, store), do: Cuckoo.handle(cmd, args, store)
  defp dispatch_prob_raw("CMS." <> _ = cmd, args, store), do: CMS.handle(cmd, args, store)
  defp dispatch_prob_raw("TOPK." <> _ = cmd, args, store), do: TopK.handle(cmd, args, store)
  defp dispatch_prob_raw("TDIGEST." <> _ = cmd, args, store), do: TDigest.handle(cmd, args, store)

  defp dispatch_memory_raw([subcmd | rest], store), do: Memory.handle(subcmd, rest, store)

  defp dispatch_memory_raw([], _store),
    do: {:error, "ERR wrong number of arguments for 'memory' command"}

  @doc """
  Prepares a raw command once for parsing, key authorization, and routing.
  """
  @spec prepare_raw(binary(), [term()]) :: {:ok, PreparedCommand.t()} | {:error, binary()}
  def prepare_raw(name, args), do: PreparedCommand.prepare(name, args)

  @doc """
  Dispatches a raw command name and argument list through the canonical command
  AST dispatcher.
  """
  @spec dispatch_raw(binary(), [term()], map()) :: term()
  def dispatch_raw(name, args, store) do
    start = System.monotonic_time(:microsecond)

    case prepare_raw(name, args) do
      {:ok, prepared} ->
        dispatch_prepared(prepared, store, started_at_us: start)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Dispatches an already prepared command without repeating parsing or key discovery.
  """
  @spec dispatch_prepared(PreparedCommand.t(), map(), keyword()) :: term()
  def dispatch_prepared(%PreparedCommand{} = prepared, store, opts \\ []) do
    start = Keyword.get_lazy(opts, :started_at_us, fn -> System.monotonic_time(:microsecond) end)

    result =
      case authorize_public_keys(prepared.command, prepared.acl_keys) do
        :ok ->
          case check_raw_resource_limits(
                 prepared.command,
                 prepared.args,
                 prepared.acl_keys,
                 store
               ) do
            :ok ->
              result = dispatch_prepared_ast(prepared, store)
              record_raw_activity(result, prepared.command, prepared.acl_keys, store)
              result

            {:error, reason} ->
              {:error, FerricStore.ResourceLimits.error_message(reason)}
          end

        {:error, reason} ->
          {:error, reason}
      end

    log_raw_dispatch(prepared.command, prepared.args, start)
    result
  end

  defp dispatch_prepared_ast(
         %PreparedCommand{command: command, ast: ast, write_keys: [key | _rest]},
         %FerricStore.Instance{} = store
       )
       when command in @tdigest_rmw_commands do
    Router.with_key_latch(store, key, fn -> dispatch_ast(ast, store) end)
  end

  defp dispatch_prepared_ast(%PreparedCommand{ast: ast}, store), do: dispatch_ast(ast, store)

  if Mix.env() == :test do
    @doc """
    Test-only convenience wrapper.
    """
    @spec dispatch(binary(), [binary()], map()) :: term()
    def dispatch(name, args, store), do: dispatch_raw(name, args, store)
  end

  defp log_raw_dispatch(cmd, args, start) do
    duration = System.monotonic_time(:microsecond) - start
    Ferricstore.SlowLog.maybe_log([cmd | args], duration)
  end

  defp authorize_public_keys(cmd, keys), do: InternalKey.authorize_command(cmd, keys)

  defp record_raw_activity({:error, _reason}, _cmd, _keys, _store), do: :ok
  defp record_raw_activity(_result, _cmd, [], _store), do: :ok

  defp record_raw_activity(_result, cmd, keys, store) do
    if data_plane_activity_command?(cmd) do
      FerricStore.ResourceLimits.record_activity(keys, store: store)
    else
      :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp check_raw_resource_limits(_cmd, _args, [], _store), do: :ok

  defp check_raw_resource_limits(cmd, args, keys, store) do
    if data_plane_activity_command?(cmd) do
      FerricStore.ResourceLimits.check_command(cmd, args, keys, store: store)
    else
      :ok
    end
  rescue
    _error -> {:error, :resource_limit_check_failed}
  catch
    _kind, _reason -> {:error, :resource_limit_check_failed}
  end

  defp data_plane_activity_command?(cmd) when cmd in @management_raw_commands, do: false
  defp data_plane_activity_command?(cmd) when cmd in @server_raw_commands, do: false
  defp data_plane_activity_command?(cmd) when cmd in @cluster_raw_commands, do: false
  defp data_plane_activity_command?("MEMORY"), do: false
  defp data_plane_activity_command?("FERRICSTORE.CONFIG"), do: false
  defp data_plane_activity_command?("FERRICSTORE.METRICS"), do: false
  defp data_plane_activity_command?(_cmd), do: true
end
