defmodule Ferricstore.Commands.KeyDiscovery do
  @moduledoc false

  alias Ferricstore.Commands.{Extension, NativeAstParser, TransactionPolicy}

  @type result :: {:ok, [binary()]} | :not_dynamic
  @type access :: :read | :write | :rw
  @type description :: %{
          acl_keys: [binary()],
          routing_scope: :none | :keys | :coordinated,
          routing_keys: [binary()],
          read_keys: [binary()],
          write_keys: [binary()],
          transaction_mode: Ferricstore.Commands.TransactionPolicy.mode()
        }
  @type prepared_description :: %{
          command: binary(),
          args: [binary()],
          ast: term(),
          acl_keys: [binary()],
          routing_scope: :none | :keys | :coordinated,
          routing_keys: [binary()],
          read_keys: [binary()],
          write_keys: [binary()],
          transaction_mode: Ferricstore.Commands.TransactionPolicy.mode()
        }

  @read_key_commands MapSet.new(~w(
    GET MGET GETRANGE STRLEN
    HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS HRANDFIELD HSCAN HSTRLEN HTTL HPTTL
    HEXPIRETIME
    LRANGE LLEN LINDEX LPOS
    SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER SSCAN SINTER SUNION SDIFF SINTERCARD
    ZSCORE ZRANK ZREVRANK ZRANGE ZREVRANGE ZCARD ZCOUNT ZRANDMEMBER ZMSCORE ZSCAN
    ZRANGEBYSCORE ZREVRANGEBYSCORE
    GETBIT BITCOUNT BITPOS PFCOUNT GEOHASH GEOPOS GEODIST GEOSEARCH
    XLEN XRANGE XREVRANGE XREAD XINFO
    BF.EXISTS BF.MEXISTS BF.CARD BF.INFO CF.EXISTS CF.MEXISTS CF.COUNT CF.INFO
    CMS.QUERY CMS.INFO TOPK.QUERY TOPK.LIST TOPK.COUNT TOPK.INFO
    TDIGEST.QUANTILE TDIGEST.CDF TDIGEST.RANK TDIGEST.REVRANK TDIGEST.BYRANK
    TDIGEST.BYREVRANK TDIGEST.TRIMMED_MEAN TDIGEST.MIN TDIGEST.MAX TDIGEST.INFO
    EXISTS TYPE OBJECT MEMORY TTL PTTL EXPIRETIME PEXPIRETIME
    KEY_INFO ROUTE ROUTE_BATCH CLUSTER.KEYSLOT FERRICSTORE.KEY_INFO FERRICSTORE.CAPABILITIES
    FERRICSTORE.TELEMETRY WATCH
    FLOW.GET FLOW.POLICY.GET FLOW.LIST FLOW.SEARCH FLOW.BY_PARENT FLOW.BY_ROOT
    FLOW.BY_CORRELATION FLOW.INFO FLOW.STUCK FLOW.STATS FLOW.ATTRIBUTES
    FLOW.ATTRIBUTE_VALUES FLOW.EFFECT.GET FLOW.GOVERNANCE.LEDGER FLOW.GOVERNANCE.OVERVIEW
    FLOW.APPROVAL.GET FLOW.APPROVAL.LIST FLOW.CIRCUIT.GET FLOW.BUDGET.GET FLOW.BUDGET.LIST
    FLOW.LIMIT.GET FLOW.LIMIT.LIST FLOW.HISTORY FLOW.TERMINALS FLOW.FAILURES
    FLOW.SCHEDULE.GET FLOW.SCHEDULE.LIST FLOW.VALUE.MGET
  ))

  @write_key_commands MapSet.new(~w(
    SET SETNX SETEX PSETEX MSET MSETNX PIPELINE APPEND SETRANGE INCR DECR INCRBY DECRBY
    INCRBYFLOAT GETSET GETDEL GETEX
    HSET HDEL HINCRBY HINCRBYFLOAT HSETNX HEXPIRE HPEXPIRE HPERSIST HGETDEL HGETEX HSETEX
    LPUSH RPUSH LPOP RPOP LSET LINSERT LTRIM LREM LMOVE LPUSHX RPUSHX RPOPLPUSH
    BLPOP BRPOP BLMOVE BLMPOP
    SADD SREM SPOP SMOVE SDIFFSTORE SINTERSTORE SUNIONSTORE
    ZADD ZREM ZINCRBY ZPOPMIN ZPOPMAX SETBIT BITOP PFADD PFMERGE GEOADD GEOSEARCHSTORE
    XADD XTRIM XDEL XGROUP XREADGROUP XACK
    BF.RESERVE BF.ADD BF.MADD CF.RESERVE CF.ADD CF.ADDNX CF.DEL
    CMS.INITBYDIM CMS.INITBYPROB CMS.INCRBY CMS.MERGE
    TOPK.RESERVE TOPK.ADD TOPK.INCRBY
    TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET TDIGEST.MERGE
    DEL UNLINK RENAME RENAMENX COPY EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
    CAS LOCK UNLOCK EXTEND RATELIMIT.ADD FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT
    FETCH_OR_COMPUTE_ERROR
    FLOW.CREATE FLOW.CREATE_MANY FLOW.VALUE.PUT FLOW.SIGNAL FLOW.SPAWN_CHILDREN FLOW.POLICY.SET
    FLOW.CLAIM_DUE FLOW.RECLAIM FLOW.EXTEND_LEASE FLOW.COMPLETE FLOW.COMPLETE_MANY
    FLOW.TRANSITION FLOW.STEP_CONTINUE FLOW.START_AND_CLAIM FLOW.RUN_STEPS_MANY
    FLOW.TRANSITION_MANY FLOW.RETRY FLOW.RETRY_MANY FLOW.FAIL FLOW.FAIL_MANY FLOW.CANCEL
    FLOW.CANCEL_MANY FLOW.REWIND FLOW.RETENTION_CLEANUP FLOW.EFFECT.RESERVE FLOW.EFFECT.CONFIRM
    FLOW.EFFECT.FAIL FLOW.EFFECT.COMPENSATE FLOW.APPROVAL.REQUEST FLOW.APPROVAL.APPROVE
    FLOW.APPROVAL.REJECT FLOW.CIRCUIT.OPEN FLOW.CIRCUIT.CLOSE FLOW.BUDGET.RESERVE
    FLOW.BUDGET.COMMIT FLOW.BUDGET.RELEASE FLOW.LIMIT.LEASE FLOW.LIMIT.SPEND FLOW.LIMIT.RELEASE
    FLOW.SCHEDULE.CREATE FLOW.SCHEDULE.DELETE FLOW.SCHEDULE.FIRE FLOW.SCHEDULE.PAUSE
    FLOW.SCHEDULE.RESUME FLOW.SCHEDULE.FIRE_DUE
  ))

  @read_write_key_commands MapSet.new(~w(
    APPEND SETRANGE INCR DECR INCRBY DECRBY INCRBYFLOAT GETSET GETDEL GETEX
    HSET HDEL HINCRBY HINCRBYFLOAT HSETNX HEXPIRE HPEXPIRE HPERSIST HGETDEL HGETEX HSETEX
    LPUSH RPUSH LPOP RPOP LSET LINSERT LTRIM LREM LPUSHX RPUSHX BLPOP BRPOP BLMPOP
    SADD SREM SPOP ZADD ZREM ZINCRBY ZPOPMIN ZPOPMAX SETBIT PFADD GEOADD
    XADD XTRIM XDEL XACK XREADGROUP EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
    CAS LOCK UNLOCK EXTEND RATELIMIT.ADD FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT
    FETCH_OR_COMPUTE_ERROR BF.ADD BF.MADD CF.ADD CF.ADDNX CF.DEL CMS.INCRBY
    TOPK.ADD TOPK.INCRBY TDIGEST.ADD TDIGEST.RESET
  ))

  @destination_source_commands ~w(
    BITOP PFMERGE SDIFFSTORE SINTERSTORE SUNIONSTORE GEOSEARCHSTORE CMS.MERGE TDIGEST.MERGE
    ZINTERSTORE ZUNIONSTORE
  )

  @source_destination_commands ~w(RENAME RENAMENX LMOVE BLMOVE RPOPLPUSH SMOVE)
  @read_write_source_destination_commands ~w(LMOVE BLMOVE RPOPLPUSH SMOVE)

  @acl_only_routing_commands ~w(
    PUBLISH PUBSUB SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE
    FERRICSTORE.NAMESPACE FERRICSTORE.QUOTA
  )

  @structured_native_commands MapSet.new(~w(
    FLOW.STEP_CONTINUE FLOW.START_AND_CLAIM FLOW.RUN_STEPS_MANY
    FLOW.SCHEDULE.CREATE FLOW.SCHEDULE.GET FLOW.SCHEDULE.DELETE FLOW.SCHEDULE.FIRE_DUE
    FLOW.SCHEDULE.LIST FLOW.SCHEDULE.FIRE FLOW.SCHEDULE.PAUSE FLOW.SCHEDULE.RESUME
    FLOW.EFFECT.RESERVE FLOW.EFFECT.CONFIRM FLOW.EFFECT.FAIL FLOW.EFFECT.COMPENSATE
    FLOW.EFFECT.GET FLOW.GOVERNANCE.LEDGER FLOW.GOVERNANCE.OVERVIEW
    FLOW.APPROVAL.REQUEST FLOW.APPROVAL.APPROVE FLOW.APPROVAL.REJECT FLOW.APPROVAL.GET
    FLOW.APPROVAL.LIST FLOW.CIRCUIT.OPEN FLOW.CIRCUIT.CLOSE FLOW.CIRCUIT.GET
    FLOW.BUDGET.RESERVE FLOW.BUDGET.COMMIT FLOW.BUDGET.RELEASE FLOW.BUDGET.GET
    FLOW.BUDGET.LIST FLOW.LIMIT.LEASE FLOW.LIMIT.SPEND FLOW.LIMIT.RELEASE
    FLOW.LIMIT.GET FLOW.LIMIT.LIST
  ))

  # These commands can mutate data or control-plane state outside one data
  # shard. Treat read-only subcommands conservatively because routing metadata
  # is intentionally command-level and immutable after parsing.
  @coordinated_routing_commands ~w(
    ACL CONFIG DBSIZE DEBUG FLUSHALL FLUSHDB KEYS MODULE RANDOMKEY SANDBOX SCAN SLOWLOG
    SAVE BGSAVE WAITAOF
    CLUSTER.JOIN CLUSTER.LEAVE CLUSTER.FAILOVER CLUSTER.PROMOTE CLUSTER.DEMOTE
    FERRICSTORE.CONFIG FERRICSTORE.BLOBGC FERRICSTORE.DOCTOR FERRICSTORE.TELEMETRY
    FERRICSTORE.NAMESPACE FERRICSTORE.QUOTA
  )

  @spec prepare(binary(), [term()]) :: {:ok, prepared_description()} | {:error, binary()}
  def prepare(name, args) do
    case NativeAstParser.parse(name, args) do
      {:ok, command, parsed_args, {:unknown, unknown_command, _unknown_args}, _parser_keys}
      when unknown_command == command ->
        prepare_extension(command, parsed_args)

      {:ok, command, parsed_args, ast, acl_keys} ->
        prepared_ast =
          if MapSet.member?(@structured_native_commands, command) do
            {:structured_native_command, command}
          else
            ast
          end

        {:ok, describe_prepared(command, parsed_args, prepared_ast, acl_keys)}

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_extension(command, parsed_args) do
    case Extension.prepare(command, parsed_args) do
      {:ok, %{ast: ast, keys: keys}} when is_list(keys) ->
        if Enum.all?(keys, &is_binary/1) do
          {:ok, describe_prepared(command, parsed_args, ast, keys)}
        else
          invalid_extension_keys(command)
        end

      {:error, :invalid_keys} ->
        invalid_extension_keys(command)

      :error ->
        unknown_command(command)
    end
  end

  defp describe_prepared(command, args, ast, acl_keys) do
    {read_keys, write_keys, routing_scope, routing_keys, transaction_mode} =
      discovery_metadata(command, ast, acl_keys)

    %{
      command: command,
      args: args,
      ast: ast,
      acl_keys: acl_keys,
      routing_scope: routing_scope,
      routing_keys: routing_keys,
      read_keys: read_keys,
      write_keys: write_keys,
      transaction_mode: transaction_mode
    }
  end

  @spec extract(binary(), [binary()]) :: result()
  def extract(command, args)

  def extract(command, [destination, count | rest])
      when command in ["CMS.MERGE", "TDIGEST.MERGE"] do
    {:ok, [destination | counted_keys(count, rest)]}
  end

  def extract(command, [destination | _args])
      when command in ["CMS.MERGE", "TDIGEST.MERGE"],
      do: {:ok, [destination]}

  def extract(command, []) when command in ["CMS.MERGE", "TDIGEST.MERGE"], do: {:ok, []}

  def extract("SINTERCARD", [count | rest]), do: {:ok, counted_keys(count, rest)}
  def extract("SINTERCARD", []), do: {:ok, []}
  def extract("PFCOUNT", keys), do: {:ok, keys}

  def extract("OBJECT", [subcommand, key | _args]) do
    if String.upcase(subcommand) in ["ENCODING", "FREQ", "IDLETIME", "REFCOUNT"] do
      {:ok, [key]}
    else
      {:ok, []}
    end
  end

  def extract("OBJECT", _args), do: {:ok, []}

  def extract("MEMORY", [subcommand, key | _args]) do
    if String.upcase(subcommand) == "USAGE", do: {:ok, [key]}, else: {:ok, []}
  end

  def extract("MEMORY", _args), do: {:ok, []}

  def extract("XINFO", [subcommand, key | _args]) do
    if String.upcase(subcommand) in ["STREAM", "GROUPS", "CONSUMERS"] do
      {:ok, [key]}
    else
      {:ok, []}
    end
  end

  def extract("XINFO", _args), do: {:ok, []}

  def extract("XGROUP", [subcommand, key | _args]) do
    if String.upcase(subcommand) in [
         "CREATE",
         "SETID",
         "DESTROY",
         "CREATECONSUMER",
         "DELCONSUMER"
       ] do
      {:ok, [key]}
    else
      {:ok, []}
    end
  end

  def extract("XGROUP", _args), do: {:ok, []}
  def extract(_command, _args), do: :not_dynamic

  @spec describe(binary(), [binary()]) :: description()
  def describe(command, acl_keys) when is_binary(command) and is_list(acl_keys) do
    describe(command, nil, acl_keys)
  end

  @spec describe(binary(), term(), [binary()]) :: description()
  def describe(command, ast, acl_keys) when is_binary(command) and is_list(acl_keys) do
    command = String.upcase(command)

    {read_keys, write_keys, routing_scope, routing_keys, transaction_mode} =
      discovery_metadata(command, ast, acl_keys)

    %{
      acl_keys: acl_keys,
      routing_scope: routing_scope,
      routing_keys: routing_keys,
      read_keys: read_keys,
      write_keys: write_keys,
      transaction_mode: transaction_mode
    }
  end

  defp discovery_metadata(command, ast, acl_keys) do
    {read_keys, write_keys} = footprint(command, ast, acl_keys)
    {routing_scope, routing_keys} = routing(command, acl_keys)
    transaction_mode = TransactionPolicy.mode(command, ast, routing_scope)
    {read_keys, write_keys, routing_scope, routing_keys, transaction_mode}
  end

  @spec access_keys(binary(), [binary()]) :: {[binary()], [binary()]}
  def access_keys(command, keys) when is_binary(command) and is_list(keys),
    do: footprint(String.upcase(command), keys)

  @spec route_keys(binary(), [binary()]) :: [binary()]
  def route_keys(command, keys) when is_binary(command) and is_list(keys),
    do: command |> String.upcase() |> routing(keys) |> elem(1)

  @spec command_access_type(binary()) :: access()
  def command_access_type(command) when is_binary(command) do
    command = String.upcase(command)

    cond do
      MapSet.member?(@read_write_key_commands, command) -> :rw
      MapSet.member?(@read_key_commands, command) -> :read
      MapSet.member?(@write_key_commands, command) -> :write
      access = Extension.non_shadowing_command_access_type(command) -> access
      true -> :rw
    end
  end

  defp footprint("SET", {:set, _key, _value, opts}, keys) when is_list(opts) do
    if :get in opts, do: {keys, keys}, else: {[], keys}
  end

  defp footprint(
         _command,
         {:extension_command, _module, _name, _args, access},
         keys
       )
       when access in [:read, :write, :rw],
       do: access_footprint(access, keys)

  defp footprint(command, _ast, keys), do: footprint(command, keys)

  defp footprint(command, [source, destination | _rest])
       when command == "COPY",
       do: {[source], [destination]}

  defp footprint("PFMERGE", [destination | sources]) do
    {Enum.uniq([destination | sources]), [destination]}
  end

  defp footprint(command, [destination | sources])
       when command in @destination_source_commands,
       do: {sources, [destination]}

  defp footprint(command, [source, destination | _rest])
       when command in @read_write_source_destination_commands do
    keys = Enum.uniq([source, destination])
    {keys, keys}
  end

  defp footprint(command, [source, destination | _rest])
       when command in @source_destination_commands,
       do: {[source], [source, destination]}

  defp footprint(command, keys) do
    command |> command_access_type() |> access_footprint(keys)
  end

  defp access_footprint(:read, keys), do: {keys, []}
  defp access_footprint(:write, keys), do: {[], keys}
  defp access_footprint(:rw, keys), do: {keys, keys}

  defp routing("FLOW." <> _rest, _keys), do: {:coordinated, []}

  defp routing(command, _keys) when command in @coordinated_routing_commands,
    do: {:coordinated, []}

  defp routing(command, _keys) when command in @acl_only_routing_commands, do: {:none, []}
  defp routing(_command, []), do: {:none, []}
  defp routing(_command, keys), do: {:keys, keys}

  defp counted_keys(count, keys) do
    case Integer.parse(count) do
      {parsed, ""} when parsed >= 0 -> Enum.take(keys, parsed)
      _invalid -> []
    end
  end

  defp invalid_extension_keys(command) do
    {:error, "ERR invalid key metadata for extension command '#{String.downcase(command)}'"}
  end

  defp unknown_command(command) do
    {:error, "ERR unknown command '#{String.downcase(command)}', with args beginning with: "}
  end
end
