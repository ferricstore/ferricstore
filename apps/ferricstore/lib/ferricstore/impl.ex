defmodule FerricStore.Impl do
  @moduledoc "Elixir-native implementation of all FerricStore data-type operations, delegating to Router and command handlers."

  alias Ferricstore.EmbeddedStringValidation
  alias Ferricstore.Store.{ReadResult, Router, TypeRegistry, ValueCodec}

  alias Ferricstore.Commands.{
    Bloom,
    CMS,
    Cuckoo,
    Expiry,
    Hash,
    Set,
    SortedSet,
    Strings,
    TDigest,
    TopK
  }

  # ---------------------------------------------------------------
  # Strings
  # ---------------------------------------------------------------

  @spec set(FerricStore.Instance.t(), binary(), binary(), keyword()) ::
          :ok | nil | {:ok, binary() | nil} | {:ok, boolean()} | {:error, term()}
  def set(ctx, key, value, opts \\ []) do
    with {:ok, parsed} <- EmbeddedStringValidation.parse_set_options(opts),
         :ok <- EmbeddedStringValidation.validate_value_size(ctx, value) do
      set_inner(ctx, key, value, parsed)
    end
  end

  defp set_inner(ctx, key, value, parsed) do
    result = Strings.handle_ast({:set, key, value, set_command_options(parsed)}, ctx)
    normalize_set_result(result, parsed.get, parsed.nx)
  end

  defp set_command_options(parsed) do
    []
    |> maybe_add_set_option(
      parsed.has_expiry and not parsed.keepttl,
      {:pxat, parsed.expire_at_ms}
    )
    |> maybe_add_set_option(parsed.nx, :nx)
    |> maybe_add_set_option(parsed.xx, :xx)
    |> maybe_add_set_option(parsed.get, :get)
    |> maybe_add_set_option(parsed.keepttl, :keepttl)
  end

  defp maybe_add_set_option(options, true, option), do: [option | options]
  defp maybe_add_set_option(options, false, _option), do: options

  defp normalize_set_result({:error, _reason} = error, _get, _nx), do: error
  defp normalize_set_result(result, true, _nx), do: {:ok, result}
  defp normalize_set_result(:ok, false, true), do: {:ok, true}
  defp normalize_set_result(nil, false, true), do: {:ok, false}
  defp normalize_set_result(result, false, _nx), do: result

  @spec get(FerricStore.Instance.t(), binary(), keyword()) ::
          {:ok, binary() | nil} | ReadResult.failure()
  def get(ctx, key, opts \\ [])

  def get(_ctx, "", _opts), do: {:error, "ERR empty key"}
  def get(_ctx, key, _opts) when byte_size(key) > 65_535, do: {:error, "ERR key too large"}

  def get(ctx, key, _opts) do
    case Router.get(ctx, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      nil ->
        case TypeRegistry.get_type(key, build_store(ctx)) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          type when type in ["none", "string"] -> {:ok, nil}
          _compound_type -> wrongtype_error()
        end

      value ->
        {:ok, value}
    end
  end

  @spec del(FerricStore.Instance.t(), [binary()]) :: {:ok, non_neg_integer()} | {:error, term()}
  def del(_ctx, []), do: {:ok, 0}

  def del(ctx, keys) when is_list(keys) do
    ctx
    |> build_store()
    |> then(&Strings.handle_ast({:del, keys}, &1))
    |> wrap_result()
  end

  @spec exists?(FerricStore.Instance.t(), binary()) :: {:ok, boolean()}
  def exists?(ctx, key) do
    {:ok, Router.exists?(ctx, key)}
  end

  @spec incr(FerricStore.Instance.t(), binary(), integer()) ::
          {:ok, integer()} | {:error, binary()}
  def incr(ctx, key, delta) do
    Strings.handle_ast({:incrby, key, delta}, ctx)
  end

  @spec incr_float(FerricStore.Instance.t(), binary(), number()) ::
          {:ok, binary()} | {:error, binary()}
  def incr_float(ctx, key, delta) do
    case ValueCodec.number_to_float(delta) do
      {:ok, float_delta} -> Strings.handle_ast({:incrbyfloat, key, float_delta}, ctx)
      :error -> {:error, "ERR value is not a valid float"}
    end
  end

  @spec mget(FerricStore.Instance.t(), [binary()]) ::
          {:ok, [binary() | nil]} | ReadResult.failure()
  def mget(ctx, keys) do
    results = Router.batch_get(ctx, keys)

    case ReadResult.first_failure(results) do
      nil -> {:ok, results}
      failure -> failure
    end
  end

  @spec mset(FerricStore.Instance.t(), %{binary() => binary()} | [{binary(), binary()}]) ::
          :ok | {:error, term()}
  def mset(ctx, pairs) when is_map(pairs) do
    Router.atomic_mset(ctx, Map.to_list(pairs))
  end

  def mset(ctx, pairs) when is_list(pairs) do
    Router.atomic_mset(ctx, pairs)
  end

  @spec append(FerricStore.Instance.t(), binary(), binary()) :: {:ok, non_neg_integer()}
  def append(ctx, key, suffix) do
    ctx
    |> then(&Strings.handle_ast({:append, key, suffix}, &1))
    |> wrap_result()
  end

  @spec strlen(FerricStore.Instance.t(), binary()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def strlen(ctx, key) do
    case Router.value_size(ctx, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      nil ->
        case TypeRegistry.get_type(key, build_store(ctx)) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          type when type in ["none", "string"] -> {:ok, 0}
          _compound_type -> wrongtype_error()
        end

      size ->
        {:ok, size}
    end
  end

  @spec getset(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, binary() | nil} | {:error, term()}
  def getset(ctx, key, value) do
    ctx
    |> then(&Strings.handle_ast({:getset, key, value}, &1))
    |> wrap_result()
  end

  @spec getdel(FerricStore.Instance.t(), binary()) ::
          {:ok, binary() | nil} | {:error, term()}
  def getdel(ctx, key) do
    ctx
    |> then(&Strings.handle_ast({:getdel, key}, &1))
    |> wrap_result()
  end

  @spec getex(FerricStore.Instance.t(), binary(), keyword()) ::
          {:ok, binary() | nil} | {:error, term()}
  def getex(ctx, key, opts) do
    with {:ok, expire_at_ms} <- EmbeddedStringValidation.parse_getex_options(opts) do
      command =
        case expire_at_ms do
          nil -> {:getex, key}
          0 -> {:getex, key, :persist}
          expire_at_ms -> {:getex, key, {:pxat, expire_at_ms}}
        end

      ctx
      |> then(&Strings.handle_ast(command, &1))
      |> wrap_result()
    end
  end

  @spec setnx(FerricStore.Instance.t(), binary(), binary()) :: {:ok, boolean()} | {:error, term()}
  def setnx(ctx, key, value), do: set(ctx, key, value, nx: true)

  @spec setex(FerricStore.Instance.t(), binary(), pos_integer(), binary()) ::
          :ok | {:error, term()}
  def setex(ctx, key, seconds, value) do
    with :ok <- EmbeddedStringValidation.validate_positive_expiry(seconds, "setex"),
         :ok <- EmbeddedStringValidation.validate_value_size(ctx, value) do
      Strings.handle_ast({:setex, key, seconds, value}, ctx)
    end
  end

  @spec psetex(FerricStore.Instance.t(), binary(), pos_integer(), binary()) ::
          :ok | {:error, term()}
  def psetex(ctx, key, milliseconds, value) do
    with :ok <- EmbeddedStringValidation.validate_positive_expiry(milliseconds, "psetex"),
         :ok <- EmbeddedStringValidation.validate_value_size(ctx, value) do
      Strings.handle_ast({:psetex, key, milliseconds, value}, ctx)
    end
  end

  @spec getrange(FerricStore.Instance.t(), binary(), integer(), integer()) ::
          {:ok, binary()} | {:error, term()}
  def getrange(ctx, key, start, stop) do
    ctx
    |> then(&Strings.handle_ast({:getrange, key, start, stop}, &1))
    |> wrap_result()
  end

  @spec setrange(FerricStore.Instance.t(), binary(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer()}
  def setrange(ctx, key, offset, value) do
    ctx
    |> then(&Strings.handle_ast({:setrange, key, offset, value}, &1))
    |> wrap_result()
  end

  # ---------------------------------------------------------------
  # TTL / expiry
  # ---------------------------------------------------------------

  @spec expire(FerricStore.Instance.t(), binary(), integer()) :: 0 | 1 | {:error, binary()}
  def expire(ctx, key, seconds) do
    store = build_store(ctx)
    Expiry.handle_ast({:pexpire, key, seconds * 1000}, store)
  end

  @spec pexpire(FerricStore.Instance.t(), binary(), integer()) :: 0 | 1 | {:error, binary()}
  def pexpire(ctx, key, milliseconds) do
    store = build_store(ctx)
    Expiry.handle_ast({:pexpire, key, milliseconds}, store)
  end

  @spec ttl(FerricStore.Instance.t(), binary()) :: {:ok, integer()} | {:error, term()}
  def ttl(ctx, key) do
    store = build_store(ctx)

    case Expiry.handle_ast({:pttl, key}, store) do
      {:error, _reason} = error -> error
      ms when is_integer(ms) and ms > 0 -> {:ok, ms}
      -1 -> {:ok, -1}
      -2 -> {:ok, -2}
      other -> {:ok, other}
    end
  end

  @spec pttl(FerricStore.Instance.t(), binary()) :: {:ok, integer()} | {:error, term()}
  def pttl(ctx, key) do
    ttl(ctx, key)
  end

  @spec persist(FerricStore.Instance.t(), binary()) :: 0 | 1
  def persist(ctx, key) do
    store = build_store(ctx)
    Expiry.handle_ast({:persist, key}, store)
  end

  # ---------------------------------------------------------------
  # Hash
  # ---------------------------------------------------------------

  @spec hset(FerricStore.Instance.t(), binary(), map()) :: {:ok, term()} | {:error, binary()}
  def hset(ctx, key, fields) when is_map(fields) do
    store = build_store(ctx)
    args = [key | Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)]
    result = Hash.handle_ast({:hset, args}, store)
    wrap_result(result)
  end

  @spec hget(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, binary() | nil} | {:error, binary()}
  def hget(ctx, key, field) do
    store = build_store(ctx)
    result = Hash.handle_ast({:hget, key, to_string(field)}, store)
    wrap_result(result)
  end

  @spec hgetall(FerricStore.Instance.t(), binary()) :: {:ok, map()} | {:error, binary()}
  def hgetall(ctx, key) do
    store = build_store(ctx)
    result = Hash.handle_ast({:hgetall, key}, store)

    case result do
      list when is_list(list) ->
        map = list |> Enum.chunk_every(2) |> Enum.into(%{}, fn [k, v] -> {k, v} end)
        {:ok, map}

      {:error, _} = err ->
        err

      _ ->
        {:ok, %{}}
    end
  end

  @spec hmget(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, [binary() | nil]} | {:error, binary()}
  def hmget(ctx, key, fields) when is_list(fields) do
    store = build_store(ctx)
    str_fields = Enum.map(fields, &to_string/1)

    case Hash.handle_ast({:hmget, [key | str_fields]}, store) do
      {:error, _} = err -> err
      values -> {:ok, values}
    end
  end

  @spec hdel(FerricStore.Instance.t(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def hdel(ctx, key, fields) when is_list(fields) do
    store = build_store(ctx)
    args = [key | Enum.map(fields, &to_string/1)]
    result = Hash.handle_ast({:hdel, args}, store)
    wrap_result(result)
  end

  @spec hexists(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, boolean()} | {:error, binary()}
  def hexists(ctx, key, field) do
    store = build_store(ctx)
    result = Hash.handle_ast({:hexists, key, to_string(field)}, store)

    case result do
      {:error, _} = err -> err
      value -> {:ok, value == 1}
    end
  end

  @spec hlen(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def hlen(ctx, key) do
    store = build_store(ctx)
    result = Hash.handle_ast({:hlen, key}, store)
    wrap_result(result)
  end

  @spec hincrby(FerricStore.Instance.t(), binary(), binary(), integer()) ::
          integer() | {:error, binary()}
  def hincrby(ctx, key, field, amount) do
    store = build_store(ctx)
    Hash.handle_ast({:hincrby, key, to_string(field), amount}, store)
  end

  # ---------------------------------------------------------------
  # Set
  # ---------------------------------------------------------------

  @spec sadd(FerricStore.Instance.t(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def sadd(ctx, key, members) when is_list(members) do
    store = build_store(ctx)
    args = [key | Enum.map(members, &to_string/1)]
    result = Set.handle_ast({:sadd, args}, store)
    wrap_result(result)
  end

  @spec srem(FerricStore.Instance.t(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def srem(ctx, key, members) when is_list(members) do
    store = build_store(ctx)
    args = [key | Enum.map(members, &to_string/1)]
    result = Set.handle_ast({:srem, args}, store)
    wrap_result(result)
  end

  @spec smembers(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def smembers(ctx, key) do
    store = build_store(ctx)
    result = Set.handle_ast({:smembers, key}, store)
    wrap_result(result)
  end

  @spec sismember(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, boolean()} | {:error, binary()}
  def sismember(ctx, key, member) do
    store = build_store(ctx)
    result = Set.handle_ast({:sismember, key, to_string(member)}, store)

    case result do
      {:error, _} = err -> err
      value -> {:ok, value == 1}
    end
  end

  @spec scard(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def scard(ctx, key) do
    store = build_store(ctx)
    result = Set.handle_ast({:scard, key}, store)
    wrap_result(result)
  end

  @spec spop(FerricStore.Instance.t(), binary(), pos_integer()) ::
          {:ok, term()} | {:error, binary()}
  def spop(ctx, key, count) do
    ctx
    |> Router.spop(key, count)
    |> wrap_result()
  end

  # ---------------------------------------------------------------
  # List
  # ---------------------------------------------------------------

  @spec lpush(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def lpush(ctx, key, values) when is_list(values) do
    store = build_store(ctx)
    args = [key | Enum.map(values, &to_string/1)]
    result = Ferricstore.Commands.List.handle_ast({:lpush, args}, store)
    wrap_result(result)
  end

  @spec rpush(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def rpush(ctx, key, values) when is_list(values) do
    store = build_store(ctx)
    args = [key | Enum.map(values, &to_string/1)]
    result = Ferricstore.Commands.List.handle_ast({:rpush, args}, store)
    wrap_result(result)
  end

  @spec lpop(FerricStore.Instance.t(), binary(), pos_integer()) ::
          {:ok, term()} | {:error, binary()}
  def lpop(ctx, key, count) do
    store = build_store(ctx)

    if count == 1 do
      result = Ferricstore.Commands.List.handle_ast({:lpop, key}, store)
      wrap_result(result)
    else
      result = Ferricstore.Commands.List.handle_ast({:lpop, key, count}, store)
      wrap_result(result)
    end
  end

  @spec rpop(FerricStore.Instance.t(), binary(), pos_integer()) ::
          {:ok, term()} | {:error, binary()}
  def rpop(ctx, key, count) do
    store = build_store(ctx)

    if count == 1 do
      result = Ferricstore.Commands.List.handle_ast({:rpop, key}, store)
      wrap_result(result)
    else
      result = Ferricstore.Commands.List.handle_ast({:rpop, key, count}, store)
      wrap_result(result)
    end
  end

  @spec lrange(FerricStore.Instance.t(), binary(), integer(), integer()) ::
          {:ok, term()} | {:error, binary()}
  def lrange(ctx, key, start, stop) do
    store = build_store(ctx)

    result =
      Ferricstore.Commands.List.handle_ast({:lrange, key, start, stop}, store)

    wrap_result(result)
  end

  @spec llen(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def llen(ctx, key) do
    store = build_store(ctx)
    result = Ferricstore.Commands.List.handle_ast({:llen, key}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # Flow
  # ---------------------------------------------------------------

  def flow_create(ctx, id, opts), do: Ferricstore.Flow.create(ctx, id, opts)

  def flow_create_many(ctx, partition_key, items, opts),
    do: Ferricstore.Flow.create_many(ctx, partition_key, items, opts)

  def flow_spawn_children(ctx, parent_id, children, opts \\ []),
    do: Ferricstore.Flow.spawn_children(ctx, parent_id, children, opts)

  def flow_get(ctx, id, opts \\ []), do: Ferricstore.Flow.get(ctx, id, opts)

  def flow_query(ctx, query, params \\ %{}),
    do: Ferricstore.Flow.Query.execute_reference(ctx, "FQL1", query, params)

  def flow_policy_set(ctx, type, opts), do: Ferricstore.Flow.policy_set(ctx, type, opts)
  def flow_policy_get(ctx, type, opts \\ []), do: Ferricstore.Flow.policy_get(ctx, type, opts)
  def flow_claim_due(ctx, type, opts), do: Ferricstore.Flow.claim_due(ctx, type, opts)
  def flow_reclaim(ctx, type, opts), do: Ferricstore.Flow.reclaim(ctx, type, opts)

  def flow_extend_lease(ctx, id, lease_token, opts \\ []),
    do: Ferricstore.Flow.extend_lease(ctx, id, lease_token, opts)

  def flow_complete(ctx, id, lease_token, opts \\ []),
    do: Ferricstore.Flow.complete(ctx, id, lease_token, opts)

  def flow_run_steps_many(ctx, items, opts \\ []),
    do: Ferricstore.Flow.run_steps_many(ctx, items, opts)

  def flow_transition(ctx, id, from_state, to_state, opts \\ []),
    do: Ferricstore.Flow.transition(ctx, id, from_state, to_state, opts)

  def flow_start_and_claim(ctx, id, type, initial_state, opts \\ []),
    do: Ferricstore.Flow.start_and_claim(ctx, id, type, initial_state, opts)

  def flow_step_continue(ctx, id, lease_token, from_state, to_state, opts \\ []),
    do: Ferricstore.Flow.step_continue(ctx, id, lease_token, from_state, to_state, opts)

  def flow_transition_many(ctx, partition_key, from_state, to_state, items, opts \\ []),
    do: Ferricstore.Flow.transition_many(ctx, partition_key, from_state, to_state, items, opts)

  def flow_retry(ctx, id, lease_token, opts),
    do: Ferricstore.Flow.retry(ctx, id, lease_token, opts)

  def flow_schedule_create(ctx, id, opts),
    do: Ferricstore.Flow.Schedule.create(ctx, id, opts)

  def flow_schedule_get(ctx, id, opts \\ []),
    do: Ferricstore.Flow.Schedule.get(ctx, id, opts)

  def flow_schedule_fire(ctx, id, opts \\ []),
    do: Ferricstore.Flow.Schedule.fire(ctx, id, opts)

  def flow_schedule_pause(ctx, id, opts \\ []),
    do: Ferricstore.Flow.Schedule.pause(ctx, id, opts)

  def flow_schedule_resume(ctx, id, opts \\ []),
    do: Ferricstore.Flow.Schedule.resume(ctx, id, opts)

  def flow_schedule_list(ctx, opts \\ []),
    do: Ferricstore.Flow.Schedule.list(ctx, opts)

  def flow_schedule_delete(ctx, id, opts \\ []),
    do: Ferricstore.Flow.Schedule.delete(ctx, id, opts)

  def flow_schedule_fire_due(ctx, opts \\ []),
    do: Ferricstore.Flow.Schedule.fire_due(ctx, opts)

  def flow_fail(ctx, id, lease_token, opts \\ []),
    do: Ferricstore.Flow.fail(ctx, id, lease_token, opts)

  def flow_cancel(ctx, id, opts \\ []), do: Ferricstore.Flow.cancel(ctx, id, opts)
  def flow_retention_cleanup(ctx, opts \\ []), do: Ferricstore.Flow.retention_cleanup(ctx, opts)
  def flow_rewind(ctx, id, opts), do: Ferricstore.Flow.rewind(ctx, id, opts)
  def flow_list(ctx, type, opts \\ []), do: Ferricstore.Flow.list(ctx, type, opts)
  def flow_search(ctx, opts \\ []), do: Ferricstore.Flow.search(ctx, opts)
  def flow_attributes(ctx, type, opts \\ []), do: Ferricstore.Flow.attributes(ctx, type, opts)

  def flow_attribute_values(ctx, type, attr_name, opts \\ []),
    do: Ferricstore.Flow.attribute_values(ctx, type, attr_name, opts)

  def flow_effect_reserve(ctx, id, effect_key, effect_type, opts \\ []),
    do: Ferricstore.Flow.effect_reserve(ctx, id, effect_key, effect_type, opts)

  def flow_effect_confirm(ctx, id, effect_key, opts \\ []),
    do: Ferricstore.Flow.effect_confirm(ctx, id, effect_key, opts)

  def flow_effect_fail(ctx, id, effect_key, opts \\ []),
    do: Ferricstore.Flow.effect_fail(ctx, id, effect_key, opts)

  def flow_effect_compensate(ctx, id, effect_key, opts \\ []),
    do: Ferricstore.Flow.effect_compensate(ctx, id, effect_key, opts)

  def flow_effect_get(ctx, id, effect_key, opts \\ []),
    do: Ferricstore.Flow.effect_get(ctx, id, effect_key, opts)

  def flow_governance_ledger(ctx, id, opts \\ []),
    do: Ferricstore.Flow.governance_ledger(ctx, id, opts)

  def flow_approval_request(ctx, id, opts \\ []),
    do: Ferricstore.Flow.approval_request(ctx, id, opts)

  def flow_approval_approve(ctx, id, opts \\ []),
    do: Ferricstore.Flow.approval_approve(ctx, id, opts)

  def flow_approval_reject(ctx, id, opts \\ []),
    do: Ferricstore.Flow.approval_reject(ctx, id, opts)

  def flow_approval_get(ctx, id, opts \\ []),
    do: Ferricstore.Flow.approval_get(ctx, id, opts)

  def flow_approval_list(ctx, opts \\ []),
    do: Ferricstore.Flow.approval_list(ctx, opts)

  def flow_governance_overview(ctx, opts \\ []),
    do: Ferricstore.Flow.governance_overview(ctx, opts)

  def flow_circuit_open(ctx, scope, opts \\ []),
    do: Ferricstore.Flow.circuit_open(ctx, scope, opts)

  def flow_circuit_close(ctx, scope, opts \\ []),
    do: Ferricstore.Flow.circuit_close(ctx, scope, opts)

  def flow_circuit_get(ctx, scope, opts \\ []),
    do: Ferricstore.Flow.circuit_get(ctx, scope, opts)

  def flow_circuit_list(ctx, opts \\ []),
    do: Ferricstore.Flow.circuit_list(ctx, opts)

  def flow_budget_reserve(ctx, scope, amount, opts \\ []),
    do: Ferricstore.Flow.budget_reserve(ctx, scope, amount, opts)

  def flow_budget_commit(ctx, scope, reservation_id, actual_amount, opts \\ []),
    do: Ferricstore.Flow.budget_commit(ctx, scope, reservation_id, actual_amount, opts)

  def flow_budget_release(ctx, scope, reservation_id, opts \\ []),
    do: Ferricstore.Flow.budget_release(ctx, scope, reservation_id, opts)

  def flow_budget_get(ctx, scope, opts \\ []),
    do: Ferricstore.Flow.budget_get(ctx, scope, opts)

  def flow_budget_list(ctx, opts \\ []),
    do: Ferricstore.Flow.budget_list(ctx, opts)

  def flow_limit_lease(ctx, scope, opts \\ []),
    do: Ferricstore.Flow.limit_lease(ctx, scope, opts)

  def flow_limit_spend(ctx, scope, opts \\ []),
    do: Ferricstore.Flow.limit_spend(ctx, scope, opts)

  def flow_limit_release(ctx, scope, opts \\ []),
    do: Ferricstore.Flow.limit_release(ctx, scope, opts)

  def flow_limit_get(ctx, scope, opts \\ []),
    do: Ferricstore.Flow.limit_get(ctx, scope, opts)

  def flow_limit_list(ctx, opts \\ []),
    do: Ferricstore.Flow.limit_list(ctx, opts)

  def flow_stats(ctx, type, opts \\ []), do: Ferricstore.Flow.stats(ctx, type, opts)
  def flow_terminals(ctx, type, opts \\ []), do: Ferricstore.Flow.terminals(ctx, type, opts)
  def flow_failures(ctx, type, opts \\ []), do: Ferricstore.Flow.failures(ctx, type, opts)

  def flow_by_parent(ctx, parent_flow_id, opts \\ []),
    do: Ferricstore.Flow.by_parent(ctx, parent_flow_id, opts)

  def flow_by_root(ctx, root_flow_id, opts \\ []),
    do: Ferricstore.Flow.by_root(ctx, root_flow_id, opts)

  def flow_by_correlation(ctx, correlation_id, opts \\ []),
    do: Ferricstore.Flow.by_correlation(ctx, correlation_id, opts)

  def flow_info(ctx, type, opts \\ []), do: Ferricstore.Flow.info(ctx, type, opts)
  def flow_stuck(ctx, type, opts \\ []), do: Ferricstore.Flow.stuck(ctx, type, opts)
  def flow_history(ctx, id, opts \\ []), do: Ferricstore.Flow.history(ctx, id, opts)

  # ---------------------------------------------------------------
  # Sorted Set
  # ---------------------------------------------------------------

  @spec zadd(FerricStore.Instance.t(), binary(), [{number(), binary()}]) ::
          {:ok, term()} | {:error, binary()}
  def zadd(ctx, key, members) when is_list(members) do
    store = build_store(ctx)

    pairs = Enum.map(members, fn {score, member} -> {score * 1.0, to_string(member)} end)
    result = SortedSet.handle_ast({:zadd, key, [], pairs}, store)
    wrap_result(result)
  end

  @spec zcard(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def zcard(ctx, key) do
    store = build_store(ctx)
    result = SortedSet.handle_ast({:zcard, key}, store)
    wrap_result(result)
  end

  @spec zscore(FerricStore.Instance.t(), binary(), binary()) :: {:ok, term()} | {:error, binary()}
  def zscore(ctx, key, member) do
    store = build_store(ctx)
    result = SortedSet.handle_ast({:zscore, key, to_string(member)}, store)
    wrap_result(result)
  end

  @spec zrank(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, non_neg_integer() | nil} | {:error, binary()}
  def zrank(ctx, key, member) do
    store = build_store(ctx)
    result = SortedSet.handle_ast({:zrank, key, to_string(member)}, store)
    wrap_result(result)
  end

  @spec zrange(FerricStore.Instance.t(), binary(), integer(), integer(), keyword()) ::
          {:ok, term()} | {:error, binary()}
  def zrange(ctx, key, start, stop, opts) do
    store = build_store(ctx)
    with_scores = Keyword.get(opts, :withscores, false)
    result = SortedSet.handle_ast({:zrange, key, start, stop, with_scores}, store)
    wrap_result(result)
  end

  @spec zrem(FerricStore.Instance.t(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def zrem(ctx, key, members) when is_list(members) do
    store = build_store(ctx)
    args = [key | Enum.map(members, &to_string/1)]
    result = SortedSet.handle_ast({:zrem, args}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # Bloom filter
  # ---------------------------------------------------------------

  @spec bf_reserve(FerricStore.Instance.t(), binary(), number(), pos_integer()) ::
          :ok | {:error, binary()}
  def bf_reserve(ctx, key, error_rate, capacity) do
    store = build_prob_store(ctx, key)
    Bloom.handle_ast({:bf_reserve, key, error_rate * 1.0, capacity}, store)
  end

  @spec bf_add(FerricStore.Instance.t(), binary(), binary()) :: {:ok, term()} | {:error, binary()}
  def bf_add(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle_ast({:bf_add, [key, element]}, store)
    wrap_result(result)
  end

  @spec bf_madd(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def bf_madd(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle_ast({:bf_madd, [key | elements]}, store)
    wrap_result(result)
  end

  @spec bf_exists(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def bf_exists(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle_ast({:bf_exists, [key, element]}, store)
    wrap_result(result)
  end

  @spec bf_mexists(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def bf_mexists(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle_ast({:bf_mexists, [key | elements]}, store)
    wrap_result(result)
  end

  @spec bf_card(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def bf_card(ctx, key) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle_ast({:bf_card, [key]}, store)
    wrap_result(result)
  end

  @spec bf_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def bf_info(ctx, key) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle_ast({:bf_info, [key]}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # CMS
  # ---------------------------------------------------------------

  @spec cms_initbydim(FerricStore.Instance.t(), binary(), pos_integer(), pos_integer()) ::
          :ok | {:error, binary()}
  def cms_initbydim(ctx, key, width, depth) do
    store = build_prob_store(ctx, key)
    CMS.handle_ast({:cms_initbydim, key, width, depth}, store)
  end

  @spec cms_initbyprob(FerricStore.Instance.t(), binary(), number(), number()) ::
          :ok | {:error, binary()}
  def cms_initbyprob(ctx, key, error, probability) do
    store = build_prob_store(ctx, key)
    CMS.handle_ast({:cms_initbyprob, key, error * 1.0, probability * 1.0}, store)
  end

  @spec cms_incrby(FerricStore.Instance.t(), binary(), [{binary(), pos_integer()}]) ::
          {:ok, term()} | {:error, binary()}
  def cms_incrby(ctx, key, pairs) do
    store = build_prob_store(ctx, key)
    result = CMS.handle_ast({:cms_incrby, key, pairs}, store)
    wrap_result(result)
  end

  @spec cms_query(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def cms_query(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = CMS.handle_ast({:cms_query, [key | elements]}, store)
    wrap_result(result)
  end

  @spec cms_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def cms_info(ctx, key) do
    store = build_prob_store(ctx, key)
    result = CMS.handle_ast({:cms_info, [key]}, store)
    wrap_result(result)
  end

  @spec cms_merge(FerricStore.Instance.t(), binary(), [binary()], keyword()) ::
          :ok | {:error, binary()}
  def cms_merge(ctx, dest, sources, _opts \\ []) do
    store = build_prob_store(ctx, dest)
    CMS.handle_ast({:cms_merge, dest, sources, List.duplicate(1, length(sources))}, store)
  end

  # ---------------------------------------------------------------
  # Cuckoo
  # ---------------------------------------------------------------

  @spec cf_reserve(FerricStore.Instance.t(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def cf_reserve(ctx, key, capacity) do
    store = build_prob_store(ctx, key)
    Cuckoo.handle_ast({:cf_reserve, key, capacity}, store)
  end

  @spec cf_add(FerricStore.Instance.t(), binary(), binary()) :: {:ok, term()} | {:error, binary()}
  def cf_add(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle_ast({:cf_add, [key, element]}, store)
    wrap_result(result)
  end

  @spec cf_addnx(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def cf_addnx(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle_ast({:cf_addnx, [key, element]}, store)
    wrap_result(result)
  end

  @spec cf_del(FerricStore.Instance.t(), binary(), binary()) :: {:ok, term()} | {:error, binary()}
  def cf_del(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle_ast({:cf_del, [key, element]}, store)
    wrap_result(result)
  end

  @spec cf_exists(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def cf_exists(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle_ast({:cf_exists, [key, element]}, store)
    wrap_result(result)
  end

  @spec cf_mexists(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def cf_mexists(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle_ast({:cf_mexists, [key | elements]}, store)
    wrap_result(result)
  end

  @spec cf_count(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def cf_count(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle_ast({:cf_count, [key, element]}, store)
    wrap_result(result)
  end

  @spec cf_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def cf_info(ctx, key) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle_ast({:cf_info, [key]}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # TopK
  # ---------------------------------------------------------------

  @spec topk_reserve(FerricStore.Instance.t(), binary(), pos_integer()) ::
          :ok | {:error, binary()}
  def topk_reserve(ctx, key, k) do
    store = build_prob_store(ctx, key)
    TopK.handle_ast({:topk_reserve, key, k, 8, 7}, store)
  end

  @spec topk_add(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def topk_add(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = TopK.handle_ast({:topk_add, [key | elements]}, store)
    wrap_result(result)
  end

  @spec topk_query(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def topk_query(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = TopK.handle_ast({:topk_query, [key | elements]}, store)
    wrap_result(result)
  end

  @spec topk_list(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def topk_list(ctx, key) do
    store = build_prob_store(ctx, key)
    result = TopK.handle_ast({:topk_list, key, false}, store)
    wrap_result(result)
  end

  @spec topk_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def topk_info(ctx, key) do
    store = build_prob_store(ctx, key)
    result = TopK.handle_ast({:topk_info, [key]}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # TDigest
  # ---------------------------------------------------------------

  @spec tdigest_create(FerricStore.Instance.t(), binary(), keyword()) :: :ok | {:error, binary()}
  def tdigest_create(ctx, key, opts \\ []) do
    store = build_store(ctx)

    compression = Keyword.get(opts, :compression)

    Router.with_key_latch(ctx, key, fn ->
      TDigest.handle_ast({:tdigest_create, key, compression}, store)
    end)
  end

  @spec tdigest_add(FerricStore.Instance.t(), binary(), [number()]) :: :ok | {:error, binary()}
  def tdigest_add(ctx, key, values) do
    store = build_store(ctx)

    Router.with_key_latch(ctx, key, fn ->
      TDigest.handle_ast({:tdigest_add, key, Enum.map(values, &(&1 * 1.0))}, store)
    end)
  end

  @spec tdigest_quantile(FerricStore.Instance.t(), binary(), [number()]) ::
          {:ok, term()} | {:error, binary()}
  def tdigest_quantile(ctx, key, quantiles) do
    store = build_store(ctx)
    result = TDigest.handle_ast({:tdigest_quantile, key, Enum.map(quantiles, &(&1 * 1.0))}, store)
    wrap_result(result)
  end

  @spec tdigest_cdf(FerricStore.Instance.t(), binary(), [number()]) ::
          {:ok, term()} | {:error, binary()}
  def tdigest_cdf(ctx, key, values) do
    store = build_store(ctx)
    result = TDigest.handle_ast({:tdigest_cdf, key, Enum.map(values, &(&1 * 1.0))}, store)
    wrap_result(result)
  end

  @spec tdigest_min(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def tdigest_min(ctx, key) do
    store = build_store(ctx)
    result = TDigest.handle_ast({:tdigest_min, [key]}, store)
    wrap_result(result)
  end

  @spec tdigest_max(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def tdigest_max(ctx, key) do
    store = build_store(ctx)
    result = TDigest.handle_ast({:tdigest_max, [key]}, store)
    wrap_result(result)
  end

  @spec tdigest_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def tdigest_info(ctx, key) do
    store = build_store(ctx)
    result = TDigest.handle_ast({:tdigest_info, [key]}, store)
    wrap_result(result)
  end

  @spec tdigest_reset(FerricStore.Instance.t(), binary()) :: :ok | {:error, binary()}
  def tdigest_reset(ctx, key) do
    store = build_store(ctx)

    Router.with_key_latch(ctx, key, fn ->
      TDigest.handle_ast({:tdigest_reset, [key]}, store)
    end)
  end

  # ---------------------------------------------------------------
  # Server / utility
  # ---------------------------------------------------------------

  @spec keys(FerricStore.Instance.t(), keyword()) ::
          {:ok, [binary()]} | Ferricstore.Store.ReadResult.failure()
  def keys(ctx, _opts \\ []) do
    case Router.keys(ctx) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      keys ->
        {:ok, Ferricstore.Store.CompoundKey.user_visible_keys(keys)}
    end
  end

  @spec dbsize(FerricStore.Instance.t()) ::
          {:ok, non_neg_integer()} | Ferricstore.Store.ReadResult.failure()
  def dbsize(ctx) do
    case Router.dbsize(ctx) do
      size when is_integer(size) and size >= 0 -> {:ok, size}
      {:error, {:storage_read_failed, _reason}} = failure -> failure
    end
  end

  @spec flushdb(FerricStore.Instance.t()) :: :ok | {:error, term()}
  def flushdb(ctx) do
    with :ok <- Ferricstore.Store.Ops.flush(ctx) do
      Ferricstore.ProbCleanup.flush_all(ctx.data_dir, ctx.shard_count)
    end
  end

  # ---------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------

  defp wrongtype_error do
    {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
  end

  defp build_store(ctx) do
    %{
      __instance_ctx__: ctx,
      get: fn key -> Router.get(ctx, key) end,
      get_meta: fn key -> Router.get_meta(ctx, key) end,
      batch_get: fn keys -> Router.batch_get(ctx, keys) end,
      expire_at_ms: fn key -> Router.expire_at_ms(ctx, key) end,
      value_size: fn key -> Router.value_size(ctx, key) end,
      object_lfu: fn key -> Router.object_lfu(ctx, key) end,
      put: fn key, value, exp -> Router.put(ctx, key, value, exp) end,
      delete: fn key -> Router.delete(ctx, key) end,
      exists?: fn key -> Router.exists?(ctx, key) end,
      keys: fn -> Router.keys(ctx) end,
      flush: fn -> flushdb(ctx) end,
      persistence_barrier: fn ->
        Ferricstore.Commands.Dispatcher.dispatch_ast({:save, []}, ctx)
      end,
      dbsize: fn -> Router.dbsize(ctx) end,
      incr: fn key, delta -> Router.incr(ctx, key, delta) end,
      incr_float: fn key, delta -> Router.incr_float(ctx, key, delta) end,
      append: fn key, suffix -> Router.append(ctx, key, suffix) end,
      getset: fn key, value -> Router.getset(ctx, key, value) end,
      getdel: fn key -> Router.getdel(ctx, key) end,
      getex: fn key, exp -> Router.getex(ctx, key, exp) end,
      setrange: fn key, offset, value -> Router.setrange(ctx, key, offset, value) end,
      cas: fn key, exp, new_val, ttl -> Router.cas(ctx, key, exp, new_val, ttl) end,
      lock: fn key, owner, ttl -> Router.lock(ctx, key, owner, ttl) end,
      unlock: fn key, owner -> Router.unlock(ctx, key, owner) end,
      extend: fn key, owner, ttl -> Router.extend(ctx, key, owner, ttl) end,
      ratelimit_add: fn key, w, m, c -> Router.ratelimit_add(ctx, key, w, m, c) end,
      list_op: fn key, op -> Router.list_op(ctx, key, op) end,
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end,
      key_lifecycle: fn command -> Router.key_lifecycle(ctx, command) end,
      prob_dir: fn ->
        # For compound store, use shard 0's prob dir as default
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        Path.join(shard_path, "prob")
      end,
      prob_dir_for_key: fn key ->
        idx = Router.shard_for(ctx, key)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        Path.join(shard_path, "prob")
      end,
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        Router.compound_get_meta(ctx, redis_key, compound_key)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        Router.compound_batch_get(ctx, redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        Router.compound_batch_get_meta(ctx, redis_key, compound_keys)
      end,
      compound_type_claim: fn redis_key, type ->
        Router.compound_type_claim(ctx, redis_key, type)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        Router.compound_put(ctx, redis_key, compound_key, value, expire_at_ms)
      end,
      compound_batch_put: fn redis_key, entries ->
        Router.compound_batch_put(ctx, redis_key, entries)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end,
      compound_scan: fn redis_key, prefix ->
        Router.compound_scan(ctx, redis_key, prefix)
      end,
      compound_count: fn redis_key, prefix ->
        Router.compound_count(ctx, redis_key, prefix)
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        Router.compound_delete_prefix(ctx, redis_key, prefix)
      end
    }
  end

  defp build_prob_store(ctx, key) do
    idx = Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

    %{
      get: fn k -> Router.get(ctx, k) end,
      get_meta: fn k -> Router.get_meta(ctx, k) end,
      batch_get: fn keys -> Router.batch_get(ctx, keys) end,
      expire_at_ms: fn k -> Router.expire_at_ms(ctx, k) end,
      value_size: fn k -> Router.value_size(ctx, k) end,
      put: fn k, value, exp -> Router.put(ctx, k, value, exp) end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      prob_dir: fn -> Path.join(shard_path, "prob") end,
      prob_dir_for_key: fn k ->
        i = Router.shard_for(ctx, k)
        sp = Ferricstore.DataDir.shard_data_path(ctx.data_dir, i)
        Path.join(sp, "prob")
      end,
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end
    }
  end

  defp wrap_result({:error, _} = err), do: err
  defp wrap_result(result), do: {:ok, result}
end
