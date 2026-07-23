defmodule FerricstoreServer.Acl.CommandCategories do
  @moduledoc false

  alias Ferricstore.Commands.{Extension, KeyDiscovery}

  @string_read ~w(GET MGET GETRANGE STRLEN)
  @string_write ~w(
    SET SETNX SETEX PSETEX MSET MSETNX PIPELINE APPEND SETRANGE
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
    FLOW.GET FLOW.POLICY.GET FLOW.QUERY FLOW.INFO FLOW.STATS FLOW.ATTRIBUTES FLOW.ATTRIBUTE_VALUES FLOW.EFFECT.GET
    FLOW.GOVERNANCE.LEDGER FLOW.GOVERNANCE.OVERVIEW FLOW.APPROVAL.GET FLOW.APPROVAL.LIST
    FLOW.CIRCUIT.GET FLOW.BUDGET.GET FLOW.BUDGET.LIST FLOW.LIMIT.GET FLOW.LIMIT.LIST
    FLOW.HISTORY FLOW.SCHEDULE.GET FLOW.SCHEDULE.LIST
  )
  @flow_write ~w(
    FLOW.CREATE FLOW.CREATE_MANY FLOW.VALUE.PUT FLOW.SIGNAL FLOW.SPAWN_CHILDREN FLOW.POLICY.SET FLOW.CLAIM_DUE
    FLOW.RECLAIM FLOW.EXTEND_LEASE FLOW.COMPLETE FLOW.COMPLETE_MANY FLOW.TRANSITION
    FLOW.STEP_CONTINUE FLOW.START_AND_CLAIM FLOW.RUN_STEPS_MANY FLOW.TRANSITION_MANY FLOW.RETRY FLOW.RETRY_MANY FLOW.FAIL FLOW.FAIL_MANY FLOW.CANCEL
    FLOW.CANCEL_MANY FLOW.REWIND FLOW.RETENTION_CLEANUP FLOW.EFFECT.RESERVE FLOW.EFFECT.CONFIRM FLOW.EFFECT.FAIL FLOW.EFFECT.COMPENSATE FLOW.APPROVAL.REQUEST FLOW.APPROVAL.APPROVE FLOW.APPROVAL.REJECT FLOW.CIRCUIT.OPEN FLOW.CIRCUIT.CLOSE FLOW.BUDGET.RESERVE FLOW.BUDGET.COMMIT FLOW.BUDGET.RELEASE FLOW.LIMIT.LEASE FLOW.LIMIT.SPEND FLOW.LIMIT.RELEASE FLOW.SCHEDULE.CREATE FLOW.SCHEDULE.DELETE FLOW.SCHEDULE.FIRE FLOW.SCHEDULE.PAUSE FLOW.SCHEDULE.RESUME FLOW.SCHEDULE.FIRE_DUE
  )
  @scheduler_read ~w(FLOW.SCHEDULE.GET FLOW.SCHEDULE.LIST)
  @scheduler_write ~w(
    FLOW.SCHEDULE.CREATE FLOW.SCHEDULE.DELETE FLOW.SCHEDULE.FIRE FLOW.SCHEDULE.PAUSE FLOW.SCHEDULE.RESUME FLOW.SCHEDULE.FIRE_DUE
  )

  @generic_read ~w(
    EXISTS TYPE RANDOMKEY SCAN OBJECT DBSIZE KEYS TTL PTTL EXPIRETIME PEXPIRETIME
  )
  @generic_write ~w(
    DEL UNLINK RENAME RENAMENX COPY EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
  )

  @native_read ~w(KEY_INFO FERRICSTORE.KEY_INFO FERRICSTORE.CAPABILITIES FERRICSTORE.TELEMETRY)
  @native_write ~w(
    CAS LOCK UNLOCK EXTEND RATELIMIT.ADD
    FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT FETCH_OR_COMPUTE_ERROR
  )

  @server_commands ~w(
    PING ECHO INFO COMMAND SELECT LOLWUT DEBUG SLOWLOG SAVE BGSAVE LASTSAVE MODULE MEMORY
    WAIT WAITAOF FLUSHDB FLUSHALL
  )

  @client_connection_commands ~w(
    CLIENT.ID CLIENT.SETNAME CLIENT.GETNAME CLIENT.INFO CLIENT.TRACKING CLIENT.CACHING
    CLIENT.TRACKINGINFO CLIENT.GETREDIR CLIENT.PAUSE CLIENT.UNPAUSE CLIENT.NO-EVICT
    CLIENT.NO-TOUCH
  )

  @client_admin_commands ~w(CLIENT CLIENT.LIST CLIENT.KILL)
  @acl_subcommands ~w(
    ACL.CAT ACL.DELUSER ACL.GETUSER ACL.LIST ACL.LOAD ACL.LOG ACL.SAVE ACL.SETUSER
    ACL.WHOAMI
  )

  @admin_commands ~w(
    ACL CONFIG DEBUG SLOWLOG SAVE BGSAVE LASTSAVE
    FLUSHDB FLUSHALL INFO COMMAND MODULE MEMORY SELECT WAIT WAITAOF
    CLUSTER.HEALTH CLUSTER.STATS CLUSTER.KEYSLOT CLUSTER.SLOTS CLUSTER.STATUS
    CLUSTER.JOIN CLUSTER.LEAVE CLUSTER.FAILOVER
    CLUSTER.PROMOTE CLUSTER.DEMOTE CLUSTER.ROLE
    FERRICSTORE.CONFIG FERRICSTORE.HOTNESS FERRICSTORE.METRICS FERRICSTORE.BLOBGC FERRICSTORE.DOCTOR FERRICSTORE.NAMESPACE FERRICSTORE.QUOTA FLOW.RETENTION_CLEANUP FLOW.QUERY.EXPLAIN
  ) ++ @client_admin_commands ++ @acl_subcommands

  @dangerous_commands ~w(
    FLUSHDB FLUSHALL DEBUG CONFIG KEYS SHUTDOWN SORT MIGRATE RESTORE DUMP CLIENT.KILL
    CLUSTER.JOIN CLUSTER.LEAVE CLUSTER.FAILOVER CLUSTER.PROMOTE
    CLUSTER.DEMOTE FERRICSTORE.CONFIG FERRICSTORE.BLOBGC FERRICSTORE.DOCTOR FERRICSTORE.NAMESPACE FERRICSTORE.QUOTA FLOW.RETENTION_CLEANUP
  )

  @connection_commands ~w(AUTH HELLO QUIT RESET SANDBOX) ++ @client_connection_commands
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
                      @probabilistic_write ++
                      @flow_write ++ @generic_write ++ @native_write
                  )

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
    "PROBABILISTIC" => MapSet.new(@probabilistic_read ++ @probabilistic_write),
    "FLOW" => MapSet.new(@flow_read ++ @flow_write),
    "SCHEDULER" => MapSet.new(@scheduler_read ++ @scheduler_write),
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
  def categories do
    Enum.reduce(Extension.non_shadowing_commands(), @category_map, fn command, categories ->
      command_name = String.upcase(command.name)
      categories = Map.update!(categories, "ALL", &MapSet.put(&1, command_name))

      Enum.reduce(command.acl_categories, categories, fn category, categories ->
        case Map.fetch(categories, category) do
          {:ok, commands} -> Map.put(categories, category, MapSet.put(commands, command_name))
          :error -> categories
        end
      end)
    end)
  end

  @spec category_names() :: [binary()]
  def category_names, do: Map.keys(@category_map) |> Enum.sort()

  @spec category_names_lower() :: [binary()]
  def category_names_lower, do: Enum.map(category_names(), &String.downcase/1)

  @spec category_commands(binary()) :: {:ok, MapSet.t(binary())} | :error
  def category_commands(category) when is_binary(category) do
    case String.upcase(category) do
      "ALL" ->
        {:ok, MapSet.union(Map.fetch!(@category_map, "ALL"), extension_commands())}

      category ->
        case Map.fetch(@category_map, category) do
          {:ok, commands} ->
            {:ok, MapSet.union(commands, extension_commands_in_acl_category(category))}

          :error ->
            :error
        end
    end
  end

  @spec read_commands() :: MapSet.t(binary())
  def read_commands,
    do: MapSet.union(@read_commands, extension_commands_in_acl_category("READ"))

  @spec write_commands() :: MapSet.t(binary())
  def write_commands,
    do: MapSet.union(@write_commands, extension_commands_in_acl_category("WRITE"))

  @spec acl_supported_commands() :: MapSet.t(binary())
  def acl_supported_commands,
    do: MapSet.union(Map.fetch!(@category_map, "ALL"), extension_commands())

  @spec command_access_type(binary()) :: :read | :write | :rw
  def command_access_type(cmd) when is_binary(cmd), do: KeyDiscovery.command_access_type(cmd)

  defp extension_commands do
    Extension.non_shadowing_command_names_upper()
  end

  defp extension_commands_in_acl_category(category),
    do: Extension.non_shadowing_command_names_in_acl_category(category)
end
