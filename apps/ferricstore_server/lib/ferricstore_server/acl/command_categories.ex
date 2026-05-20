defmodule FerricstoreServer.Acl.CommandCategories do
  @moduledoc false

  @string_read ~w(GET MGET GETRANGE STRLEN)
  @string_write ~w(
    SET SETNX SETEX PSETEX MSET MSETNX APPEND SETRANGE
    INCR DECR INCRBY DECRBY INCRBYFLOAT
    GETSET GETDEL GETEX
  )

  @hash_read ~w(
    HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS HRANDFIELD HSCAN HSTRLEN
    HTTL HPTTL HEXPIRETIME
  )
  @hash_write ~w(
    HSET HDEL HINCRBY HINCRBYFLOAT HSETNX
    HEXPIRE HPEXPIRE HPERSIST HGETDEL HGETEX HSETEX
  )

  @list_read ~w(LRANGE LLEN LINDEX LPOS)
  @list_write ~w(
    LPUSH RPUSH LPOP RPOP LSET LINSERT LTRIM LREM LMOVE LPUSHX RPUSHX RPOPLPUSH
    BLPOP BRPOP BLMOVE BLMPOP
  )

  @set_read ~w(
    SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER SSCAN
    SINTER SUNION SDIFF SINTERCARD
  )
  @set_write ~w(SADD SREM SPOP SMOVE SDIFFSTORE SINTERSTORE SUNIONSTORE)

  @sorted_set_read ~w(
    ZSCORE ZRANK ZREVRANK ZRANGE ZREVRANGE ZCARD ZCOUNT ZRANDMEMBER ZMSCORE
    ZSCAN ZRANGEBYSCORE ZREVRANGEBYSCORE
  )
  @sorted_set_write ~w(ZADD ZREM ZINCRBY ZPOPMIN ZPOPMAX)

  @bitmap_read ~w(GETBIT BITCOUNT BITPOS)
  @bitmap_write ~w(SETBIT BITOP)

  @hyperloglog_read ~w(PFCOUNT)
  @hyperloglog_write ~w(PFADD PFMERGE)

  @geo_read ~w(GEOHASH GEOPOS GEODIST GEOSEARCH)
  @geo_write ~w(GEOADD GEOSEARCHSTORE)

  @stream_read ~w(XLEN XRANGE XREVRANGE XREAD XINFO)
  @stream_write ~w(XADD XTRIM XDEL XGROUP XREADGROUP XACK)

  @json_read ~w(
    JSON.GET JSON.TYPE JSON.STRLEN JSON.OBJKEYS JSON.OBJLEN JSON.ARRLEN JSON.MGET
  )
  @json_write ~w(
    JSON.SET JSON.DEL JSON.NUMINCRBY JSON.TOGGLE JSON.CLEAR JSON.ARRAPPEND
  )

  @probabilistic_read ~w(
    BF.EXISTS BF.MEXISTS BF.CARD BF.INFO
    CF.EXISTS CF.MEXISTS CF.COUNT CF.INFO
    CMS.QUERY CMS.INFO
    TOPK.QUERY TOPK.LIST TOPK.COUNT TOPK.INFO
    TDIGEST.QUANTILE TDIGEST.CDF TDIGEST.RANK TDIGEST.REVRANK TDIGEST.BYRANK
    TDIGEST.BYREVRANK TDIGEST.TRIMMED_MEAN TDIGEST.MIN TDIGEST.MAX TDIGEST.INFO
  )
  @probabilistic_write ~w(
    BF.RESERVE BF.ADD BF.MADD
    CF.RESERVE CF.ADD CF.ADDNX CF.DEL
    CMS.INITBYDIM CMS.INITBYPROB CMS.INCRBY CMS.MERGE
    TOPK.RESERVE TOPK.ADD TOPK.INCRBY
    TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET TDIGEST.MERGE
  )

  @flow_read ~w(
    FLOW.GET FLOW.VALUE.MGET FLOW.POLICY.GET FLOW.LIST FLOW.TERMINALS FLOW.FAILURES
    FLOW.BY_PARENT FLOW.BY_ROOT FLOW.BY_CORRELATION FLOW.INFO FLOW.STUCK FLOW.HISTORY
  )
  @flow_write ~w(
    FLOW.CREATE FLOW.CREATE_MANY FLOW.VALUE.PUT FLOW.SPAWN_CHILDREN FLOW.POLICY.SET FLOW.CLAIM_DUE
    FLOW.RECLAIM FLOW.EXTEND_LEASE FLOW.COMPLETE FLOW.COMPLETE_MANY FLOW.TRANSITION
    FLOW.TRANSITION_MANY FLOW.SIGNAL FLOW.RETRY FLOW.RETRY_MANY FLOW.FAIL FLOW.FAIL_MANY FLOW.CANCEL
    FLOW.CANCEL_MANY FLOW.REWIND FLOW.RETENTION_CLEANUP
  )

  @generic_read ~w(
    EXISTS TYPE RANDOMKEY SCAN OBJECT DBSIZE KEYS TTL PTTL EXPIRETIME PEXPIRETIME
  )
  @generic_write ~w(
    DEL UNLINK RENAME RENAMENX COPY EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
  )

  @native_read ~w(FERRICSTORE.KEY_INFO)
  @native_write ~w(
    CAS LOCK UNLOCK EXTEND RATELIMIT.ADD
    FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT FETCH_OR_COMPUTE_ERROR
  )

  @server_commands ~w(
    PING ECHO INFO COMMAND SELECT LOLWUT DEBUG SLOWLOG SAVE BGSAVE LASTSAVE MODULE MEMORY
    WAIT WAITAOF FLUSHDB FLUSHALL
  )

  @admin_commands ~w(
    ACL CLIENT CONFIG DEBUG SLOWLOG SAVE BGSAVE LASTSAVE
    FLUSHDB FLUSHALL INFO COMMAND MODULE MEMORY SELECT WAIT WAITAOF
    CLUSTER.HEALTH CLUSTER.STATS CLUSTER.KEYSLOT CLUSTER.SLOTS CLUSTER.STATUS
    CLUSTER.JOIN CLUSTER.LEAVE CLUSTER.FAILOVER
    CLUSTER.PROMOTE CLUSTER.DEMOTE CLUSTER.ROLE
    FERRICSTORE.CONFIG FERRICSTORE.HOTNESS FERRICSTORE.METRICS FERRICSTORE.BLOBGC FLOW.RETENTION_CLEANUP
  )

  @dangerous_commands ~w(
    FLUSHDB FLUSHALL DEBUG CONFIG KEYS SHUTDOWN SORT MIGRATE RESTORE DUMP
    CLUSTER.JOIN CLUSTER.LEAVE CLUSTER.FAILOVER CLUSTER.PROMOTE
    CLUSTER.DEMOTE FERRICSTORE.CONFIG FERRICSTORE.BLOBGC FLOW.RETENTION_CLEANUP
  )

  @connection_commands ~w(AUTH HELLO QUIT RESET CLIENT SANDBOX)
  @transaction_commands ~w(MULTI EXEC DISCARD WATCH UNWATCH)
  @pubsub_commands ~w(PUBLISH PUBSUB SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE)
  @blocking_commands ~w(BLPOP BRPOP BLMOVE BLMPOP)

  @read_commands MapSet.new(
                   @string_read ++
                     @hash_read ++
                     @list_read ++
                     @set_read ++
                     @sorted_set_read ++
                     @bitmap_read ++
                     @hyperloglog_read ++
                     @geo_read ++
                     @stream_read ++
                     @json_read ++
                     @probabilistic_read ++
                     @flow_read ++ @generic_read ++ @native_read
                 )

  @write_commands MapSet.new(
                    @string_write ++
                      @hash_write ++
                      @list_write ++
                      @set_write ++
                      @sorted_set_write ++
                      @bitmap_write ++
                      @hyperloglog_write ++
                      @geo_write ++
                      @stream_write ++
                      @json_write ++
                      @probabilistic_write ++
                      @flow_write ++ @generic_write ++ @native_write
                  )

  @read_key_commands MapSet.new(
                       @read_commands
                       |> MapSet.to_list()
                       |> Kernel.--(~w(DBSIZE KEYS RANDOMKEY SCAN))
                     )

  @write_key_commands MapSet.new(@write_commands)

  @read_write_key_commands MapSet.new(~w(GETSET GETDEL GETEX HGETDEL HGETEX CAS))

  @category_map %{
    "READ" => @read_commands,
    "WRITE" => @write_commands,
    "ADMIN" => MapSet.new(@admin_commands),
    "DANGEROUS" => MapSet.new(@dangerous_commands),
    "KEYSPACE" => MapSet.new(@generic_read ++ @generic_write),
    "STRING" => MapSet.new(@string_read ++ @string_write),
    "HASH" => MapSet.new(@hash_read ++ @hash_write),
    "LIST" => MapSet.new(@list_read ++ @list_write),
    "SET" => MapSet.new(@set_read ++ @set_write),
    "SORTEDSET" => MapSet.new(@sorted_set_read ++ @sorted_set_write),
    "BITMAP" => MapSet.new(@bitmap_read ++ @bitmap_write),
    "HYPERLOGLOG" => MapSet.new(@hyperloglog_read ++ @hyperloglog_write),
    "GEO" => MapSet.new(@geo_read ++ @geo_write),
    "STREAM" => MapSet.new(@stream_read ++ @stream_write),
    "JSON" => MapSet.new(@json_read ++ @json_write),
    "PROBABILISTIC" => MapSet.new(@probabilistic_read ++ @probabilistic_write),
    "FLOW" => MapSet.new(@flow_read ++ @flow_write),
    "NATIVE" => MapSet.new(@native_read ++ @native_write),
    "PUBSUB" => MapSet.new(@pubsub_commands),
    "BLOCKING" => MapSet.new(@blocking_commands),
    "CONNECTION" => MapSet.new(@connection_commands),
    "TRANSACTION" => MapSet.new(@transaction_commands),
    "SERVER" => MapSet.new(@server_commands),
    "GENERIC" => MapSet.new(@generic_read ++ @generic_write),
    "FAST" => MapSet.new(~w(PING ECHO GET SET EXISTS TTL PTTL TYPE STRLEN HLEN LLEN SCARD ZCARD)),
    "SLOW" => MapSet.new(~w(KEYS SCAN HSCAN SSCAN ZSCAN)),
    "ALL" =>
      MapSet.new(
        @read_commands
        |> MapSet.union(@write_commands)
        |> MapSet.to_list()
        |> Kernel.++(
          @admin_commands ++
            @dangerous_commands ++
            @connection_commands ++
            @transaction_commands ++
            @pubsub_commands ++ @blocking_commands ++ @server_commands
        )
      )
  }

  @spec categories() :: %{binary() => MapSet.t(binary())}
  def categories, do: @category_map

  @spec category_names() :: [binary()]
  def category_names, do: Map.keys(@category_map) |> Enum.sort()

  @spec category_names_lower() :: [binary()]
  def category_names_lower, do: Enum.map(category_names(), &String.downcase/1)

  @spec category_commands(binary()) :: {:ok, MapSet.t(binary())} | :error
  def category_commands(category) when is_binary(category) do
    Map.fetch(@category_map, String.upcase(category))
  end

  @spec read_commands() :: MapSet.t(binary())
  def read_commands, do: @read_commands

  @spec write_commands() :: MapSet.t(binary())
  def write_commands, do: @write_commands

  @spec acl_supported_commands() :: MapSet.t(binary())
  def acl_supported_commands, do: Map.fetch!(@category_map, "ALL")

  @spec command_access_type(binary()) :: :read | :write | :rw
  def command_access_type(cmd) when is_binary(cmd) do
    cmd = String.upcase(cmd)

    cond do
      MapSet.member?(@read_write_key_commands, cmd) -> :rw
      MapSet.member?(@read_key_commands, cmd) -> :read
      MapSet.member?(@write_key_commands, cmd) -> :write
      true -> :rw
    end
  end
end
