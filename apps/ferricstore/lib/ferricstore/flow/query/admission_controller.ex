defmodule Ferricstore.Flow.Query.AdmissionController do
  @moduledoc false

  use GenServer

  alias Ferricstore.Flow.Query.Limits
  alias Ferricstore.OperationalLimits
  alias Ferricstore.Flow.Query.{Budget, IndexRegistry}

  @default_max_scope 8
  @default_max_node 32
  @default_max_scope_memory_bytes 64 * 1_024 * 1_024
  @default_max_node_memory_bytes 256 * 1_024 * 1_024
  @default_node_memory_fraction 4
  @call_timeout 500
  @maximum_limit 4_096
  @maximum_memory_bytes 1_024 * 1_024 * 1_024 * 1_024
  @maximum_scope_bytes Limits.max_partition_key_bytes()
  @maximum_index_id_bytes 64
  @maximum_build_id_bytes 128
  @maximum_index_version 0xFFFF_FFFF_FFFF_FFFF
  @maximum_orphan_grace_ms 60_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    name =
      Keyword.get_lazy(opts, :name, fn ->
        opts
        |> Keyword.get(:instance_ctx, %{name: :default})
        |> server_name()
      end)

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec server_name(map() | atom()) :: atom()
  def server_name(%{name: name}), do: server_name(name)
  def server_name(:default), do: __MODULE__
  def server_name(name) when is_atom(name), do: :"#{name}.Flow.Query.AdmissionController"

  @spec acquire(GenServer.server(), atom() | map(), binary()) ::
          {:ok, reference()}
          | {:error,
             :query_concurrency_exceeded
             | :invalid_query_admission_scope
             | :query_engine_failure}
  def acquire(server, instance, scope)
      when is_binary(scope) and scope != "" and byte_size(scope) <= @maximum_scope_bytes,
      do: acquire(server, instance, scope, Budget.default().planner_memory_bytes)

  def acquire(_server, _instance, _scope), do: {:error, :invalid_query_admission_scope}

  @spec acquire(GenServer.server(), atom() | map(), binary(), pos_integer()) ::
          {:ok, reference()}
          | {:error,
             :query_concurrency_exceeded
             | :invalid_query_admission_scope
             | :invalid_query_admission_memory
             | :query_engine_failure}
  def acquire(server, instance, scope, memory_bytes)
      when is_binary(scope) and scope != "" and byte_size(scope) <= @maximum_scope_bytes do
    with true <- valid_memory_bytes?(memory_bytes),
         {:ok, instance_name} <- instance_name(instance) do
      lease = make_ref()

      case safe_call(server, {:acquire, lease, instance_name, scope, memory_bytes}) do
        {:ok, ^lease} = admitted ->
          admitted

        {:error, :query_concurrency_exceeded} = rejected ->
          rejected

        {:error, _reason} = error ->
          cancel_failed_acquire(server, lease)
          error

        _invalid ->
          cancel_failed_acquire(server, lease)
          {:error, :query_engine_failure}
      end
    else
      false -> {:error, :invalid_query_admission_memory}
      {:error, _reason} = error -> error
    end
  end

  def acquire(_server, _instance, _scope, _memory_bytes),
    do: {:error, :invalid_query_admission_scope}

  @spec resize_memory(GenServer.server(), reference(), pos_integer()) ::
          :ok
          | {:error,
             :query_concurrency_exceeded
             | :invalid_query_admission_lease
             | :invalid_query_admission_memory
             | :query_engine_failure}
  def resize_memory(server, lease, memory_bytes)
      when is_reference(lease) and is_integer(memory_bytes) and memory_bytes > 0 and
             memory_bytes <= @maximum_memory_bytes,
      do: safe_call(server, {:resize_memory, lease, memory_bytes})

  def resize_memory(_server, lease, _memory_bytes) when is_reference(lease),
    do: {:error, :invalid_query_admission_memory}

  def resize_memory(_server, _lease, _memory_bytes),
    do: {:error, :invalid_query_admission_lease}

  @spec release(GenServer.server(), reference()) ::
          :ok | {:error, :invalid_query_admission_lease | :query_engine_failure}
  def release(server, lease) when is_reference(lease),
    do: safe_call(server, {:release, lease})

  def release(_server, _lease), do: {:error, :invalid_query_admission_lease}

  @spec pin_index(GenServer.server(), reference(), atom() | map(), tuple()) ::
          :ok
          | {:error,
             :invalid_query_admission_lease
             | :invalid_query_admission_scope
             | :invalid_query_index_identity
             | :query_index_retired
             | :query_engine_failure}
  def pin_index(server, lease, instance, index) when is_reference(lease) do
    with {:ok, instance_name} <- instance_name(instance),
         {:ok, index_digest} <- index_digest(index) do
      safe_call(server, {:pin_index, lease, instance_name, index, index_digest})
    end
  end

  def pin_index(_server, _lease, _instance, _index),
    do: {:error, :invalid_query_admission_lease}

  @spec fence_index(GenServer.server(), atom() | map(), tuple()) ::
          :ok
          | {:error,
             :invalid_query_admission_scope
             | :invalid_query_index_identity
             | :query_engine_failure}
  def fence_index(server, instance, index) do
    with {:ok, instance_name} <- instance_name(instance),
         {:ok, index_digest} <- index_digest(index) do
      safe_call(server, {:fence_index, instance_name, index_digest})
    end
  end

  @spec unfence_index(GenServer.server(), atom() | map(), tuple()) ::
          :ok
          | {:error,
             :invalid_query_admission_scope
             | :invalid_query_index_identity
             | :query_engine_failure}
  def unfence_index(server, instance, index) do
    with {:ok, instance_name} <- instance_name(instance),
         {:ok, index_digest} <- index_digest(index) do
      safe_call(server, {:unfence_index, instance_name, index_digest})
    end
  end

  @spec drained?(GenServer.server(), atom() | map()) ::
          {:ok, boolean()} | {:error, :invalid_query_admission_scope | :query_engine_failure}
  def drained?(server, instance) do
    with {:ok, instance_name} <- instance_name(instance) do
      safe_call(server, {:drained?, instance_name})
    end
  end

  @spec drained?(GenServer.server(), atom() | map(), tuple()) ::
          {:ok, boolean()}
          | {:error,
             :invalid_query_admission_scope
             | :invalid_query_index_identity
             | :query_engine_failure}
  def drained?(server, instance, index) do
    with {:ok, instance_name} <- instance_name(instance),
         {:ok, index_digest} <- index_digest(index) do
      safe_call(server, {:index_drained?, instance_name, index_digest})
    end
  end

  @spec with_permit(GenServer.server(), atom() | map(), binary(), (-> result)) ::
          result | {:error, atom()}
        when result: term()
  def with_permit(server, instance, scope, fun) when is_function(fun, 0) do
    with_permit(server, instance, scope, Budget.default().planner_memory_bytes, fun)
  end

  def with_permit(server, instance, scope, fun) when is_function(fun, 1) do
    with_permit(server, instance, scope, Budget.default().planner_memory_bytes, fun)
  end

  @spec with_permit(
          GenServer.server(),
          atom() | map(),
          binary(),
          pos_integer(),
          (-> result) | (reference() -> result)
        ) :: result | {:error, atom()}
        when result: term()
  def with_permit(server, instance, scope, memory_bytes, fun) when is_function(fun, 0) do
    case acquire(server, instance, scope, memory_bytes) do
      {:ok, lease} ->
        try do
          fun.()
        after
          release_after_work(server, lease)
        end

      {:error, _reason} = error ->
        error
    end
  end

  def with_permit(server, instance, scope, memory_bytes, fun) when is_function(fun, 1) do
    case acquire(server, instance, scope, memory_bytes) do
      {:ok, lease} ->
        try do
          fun.(lease)
        after
          release_after_work(server, lease)
        end

      {:error, _reason} = error ->
        error
    end
  end

  @impl true
  def init(opts) do
    max_scope = Keyword.get(opts, :max_scope, @default_max_scope)
    max_node = Keyword.get(opts, :max_node, @default_max_node)

    memory_limit_bytes =
      Keyword.get_lazy(opts, :memory_limit_bytes, &OperationalLimits.memory_limit_bytes/0)

    max_node_memory_bytes =
      Keyword.get(
        opts,
        :max_node_memory_bytes,
        default_node_memory_bytes(memory_limit_bytes)
      )

    default_scope_memory_bytes =
      if is_integer(max_node_memory_bytes),
        do: min(@default_max_scope_memory_bytes, max_node_memory_bytes),
        else: @default_max_scope_memory_bytes

    max_scope_memory_bytes =
      Keyword.get(opts, :max_scope_memory_bytes, default_scope_memory_bytes)

    index_active_fun = Keyword.get(opts, :index_active_fun, &IndexRegistry.active_identity?/2)
    orphan_grace_ms = Keyword.get(opts, :orphan_grace_ms, Budget.default().wall_time_ms)
    clock_ms = Keyword.get(opts, :clock_ms, fn -> System.monotonic_time(:millisecond) end)

    with true <- valid_limit?(max_scope),
         true <- valid_limit?(max_node),
         true <- valid_memory_bytes?(max_scope_memory_bytes),
         true <- valid_memory_bytes?(max_node_memory_bytes),
         true <- max_scope_memory_bytes <= max_node_memory_bytes,
         true <- is_function(index_active_fun, 2),
         true <- valid_orphan_grace?(orphan_grace_ms),
         true <- is_function(clock_ms, 0),
         {:ok, started_at_ms} <- call_clock(clock_ms) do
      {:ok,
       %{
         max_scope: max_scope,
         max_node: max_node,
         max_scope_memory_bytes: max_scope_memory_bytes,
         max_node_memory_bytes: max_node_memory_bytes,
         index_active_fun: index_active_fun,
         orphan_grace_until_ms: started_at_ms + orphan_grace_ms,
         clock_ms: clock_ms,
         node_count: 0,
         node_memory_bytes: 0,
         instance_counts: %{},
         scope_counts: %{},
         scope_memory_bytes: %{},
         index_counts: %{},
         fenced_indexes: MapSet.new(),
         leases: %{},
         monitors: %{}
       }}
    else
      _invalid -> {:stop, :invalid_query_admission_options}
    end
  end

  @impl true
  def handle_call({:drained?, instance}, _from, state) do
    drained? =
      Map.get(state.instance_counts, instance, 0) == 0 and orphan_grace_elapsed?(state)

    {:reply, {:ok, drained?}, state}
  end

  def handle_call({:index_drained?, instance, index_digest}, _from, state) do
    drained? =
      Map.get(state.index_counts, {instance, index_digest}, 0) == 0 and
        orphan_grace_elapsed?(state)

    {:reply, {:ok, drained?}, state}
  end

  def handle_call({:fence_index, instance, index_digest}, _from, state) do
    key = {instance, index_digest}
    {:reply, :ok, %{state | fenced_indexes: MapSet.put(state.fenced_indexes, key)}}
  end

  def handle_call({:unfence_index, instance, index_digest}, _from, state) do
    key = {instance, index_digest}
    {:reply, :ok, %{state | fenced_indexes: MapSet.delete(state.fenced_indexes, key)}}
  end

  def handle_call({:acquire, lease, instance, scope, memory_bytes}, {owner, _tag}, state) do
    scope_key = {instance, scope_digest(scope)}
    scope_count = Map.get(state.scope_counts, scope_key, 0)
    scope_memory_bytes = Map.get(state.scope_memory_bytes, scope_key, 0)

    if state.node_count >= state.max_node or scope_count >= state.max_scope or
         state.node_memory_bytes + memory_bytes > state.max_node_memory_bytes or
         scope_memory_bytes + memory_bytes > state.max_scope_memory_bytes do
      {:reply, {:error, :query_concurrency_exceeded}, state}
    else
      monitor = Process.monitor(owner)

      state = %{
        state
        | node_count: state.node_count + 1,
          node_memory_bytes: state.node_memory_bytes + memory_bytes,
          instance_counts: Map.update(state.instance_counts, instance, 1, &(&1 + 1)),
          scope_counts: Map.put(state.scope_counts, scope_key, scope_count + 1),
          scope_memory_bytes:
            Map.put(state.scope_memory_bytes, scope_key, scope_memory_bytes + memory_bytes),
          leases:
            Map.put(state.leases, lease, %{
              owner: owner,
              instance: instance,
              scope_key: scope_key,
              memory_bytes: memory_bytes,
              index_keys: MapSet.new(),
              monitor: monitor
            }),
          monitors: Map.put(state.monitors, monitor, lease)
      }

      {:reply, {:ok, lease}, state}
    end
  end

  def handle_call({:resize_memory, lease, memory_bytes}, {owner, _tag}, state) do
    case Map.get(state.leases, lease) do
      %{owner: ^owner, scope_key: scope_key, memory_bytes: current_bytes} = lease_entry ->
        scope_memory_bytes = Map.fetch!(state.scope_memory_bytes, scope_key)
        node_memory_bytes = state.node_memory_bytes - current_bytes + memory_bytes
        resized_scope_memory_bytes = scope_memory_bytes - current_bytes + memory_bytes

        if node_memory_bytes <= state.max_node_memory_bytes and
             resized_scope_memory_bytes <= state.max_scope_memory_bytes do
          state = %{
            state
            | node_memory_bytes: node_memory_bytes,
              scope_memory_bytes:
                Map.put(state.scope_memory_bytes, scope_key, resized_scope_memory_bytes),
              leases: Map.put(state.leases, lease, %{lease_entry | memory_bytes: memory_bytes})
          }

          {:reply, :ok, state}
        else
          {:reply, {:error, :query_concurrency_exceeded}, state}
        end

      _unknown_or_foreign ->
        {:reply, {:error, :invalid_query_admission_lease}, state}
    end
  end

  def handle_call(
        {:pin_index, lease, instance, index_identity, index_digest},
        {owner, _tag},
        state
      ) do
    index_key = {instance, index_digest}

    case Map.get(state.leases, lease) do
      %{owner: ^owner, instance: ^instance, index_keys: index_keys} = lease_entry ->
        cond do
          MapSet.member?(state.fenced_indexes, index_key) ->
            {:reply, {:error, :query_index_retired}, state}

          MapSet.member?(index_keys, index_key) ->
            {:reply, :ok, state}

          true ->
            pin_active_index(state, lease, lease_entry, index_key, instance, index_identity)
        end

      _unknown_or_foreign ->
        {:reply, {:error, :invalid_query_admission_lease}, state}
    end
  end

  def handle_call({:release, lease}, {owner, _tag}, state) do
    case Map.get(state.leases, lease) do
      %{owner: ^owner} ->
        {:reply, :ok, drop_lease(state, lease, true)}

      _unknown_or_foreign ->
        {:reply, {:error, :invalid_query_admission_lease}, state}
    end
  end

  @impl true
  def handle_cast({:cancel_acquire, lease, owner}, state) do
    case Map.get(state.leases, lease) do
      %{owner: ^owner} -> {:noreply, drop_lease(state, lease, true)}
      _missing_or_foreign -> {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _owner, _reason}, state) do
    case Map.get(state.monitors, monitor) do
      nil -> {:noreply, state}
      lease -> {:noreply, drop_lease(state, lease, false)}
    end
  end

  defp drop_lease(state, lease, demonitor?) do
    case Map.pop(state.leases, lease) do
      {nil, _leases} ->
        state

      {%{
         instance: instance,
         scope_key: scope_key,
         memory_bytes: memory_bytes,
         index_keys: index_keys,
         monitor: monitor
       }, leases} ->
        if demonitor?, do: Process.demonitor(monitor, [:flush])

        instance_counts = decrement_count(state.instance_counts, instance)
        scope_counts = decrement_scope(state.scope_counts, scope_key)
        scope_memory_bytes = decrement_memory(state.scope_memory_bytes, scope_key, memory_bytes)

        index_counts =
          Enum.reduce(index_keys, state.index_counts, fn index_key, counts ->
            decrement_count(counts, index_key)
          end)

        %{
          state
          | node_count: max(state.node_count - 1, 0),
            node_memory_bytes: max(state.node_memory_bytes - memory_bytes, 0),
            instance_counts: instance_counts,
            scope_counts: scope_counts,
            scope_memory_bytes: scope_memory_bytes,
            index_counts: index_counts,
            leases: leases,
            monitors: Map.delete(state.monitors, monitor)
        }
    end
  end

  defp decrement_scope(counts, scope_key) do
    decrement_count(counts, scope_key)
  end

  defp decrement_count(counts, key) do
    case Map.get(counts, key, 0) do
      count when count > 1 -> Map.put(counts, key, count - 1)
      _last_or_missing -> Map.delete(counts, key)
    end
  end

  defp decrement_memory(counts, key, bytes) do
    case Map.get(counts, key, 0) - bytes do
      remaining when remaining > 0 -> Map.put(counts, key, remaining)
      _last_or_missing -> Map.delete(counts, key)
    end
  end

  defp pin_active_index(state, lease, lease_entry, index_key, instance, index_identity) do
    case index_active?(state.index_active_fun, instance, index_identity) do
      {:ok, true} ->
        lease_entry = %{
          lease_entry
          | index_keys: MapSet.put(lease_entry.index_keys, index_key)
        }

        state = %{
          state
          | index_counts: Map.update(state.index_counts, index_key, 1, &(&1 + 1)),
            leases: Map.put(state.leases, lease, lease_entry)
        }

        {:reply, :ok, state}

      {:ok, false} ->
        {:reply, {:error, :query_index_retired}, state}

      {:error, _reason} ->
        {:reply, {:error, :query_engine_failure}, state}
    end
  end

  defp index_active?(fun, instance, identity) do
    case fun.(instance, identity) do
      {:ok, active?} when is_boolean(active?) -> {:ok, active?}
      _invalid -> {:error, :query_index_registry_unavailable}
    end
  rescue
    _error -> {:error, :query_index_registry_unavailable}
  catch
    _kind, _reason -> {:error, :query_index_registry_unavailable}
  end

  defp orphan_grace_elapsed?(state) do
    case call_clock(state.clock_ms) do
      {:ok, now_ms} -> now_ms >= state.orphan_grace_until_ms
      {:error, _reason} -> false
    end
  end

  defp call_clock(clock_ms) do
    case clock_ms.() do
      value when is_integer(value) -> {:ok, value}
      _invalid -> {:error, :invalid_query_admission_clock}
    end
  rescue
    _error -> {:error, :invalid_query_admission_clock}
  catch
    _kind, _reason -> {:error, :invalid_query_admission_clock}
  end

  defp scope_digest(scope),
    do: :crypto.hash(:sha256, ["ferric.flow.query.admission.scope/v1", scope])

  defp release_after_work(server, lease) do
    _result = release(server, lease)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp cancel_failed_acquire(server, lease) do
    GenServer.cast(server, {:cancel_acquire, lease, self()})
    :ok
  rescue
    _error -> :ok
  catch
    :exit, _reason -> :ok
  end

  defp safe_call(server, request) do
    GenServer.call(server, request, @call_timeout)
  rescue
    _error -> {:error, :query_engine_failure}
  catch
    :exit, _reason -> {:error, :query_engine_failure}
  end

  defp instance_name(%{name: name}), do: instance_name(name)
  defp instance_name(name) when is_atom(name), do: {:ok, name}
  defp instance_name(_instance), do: {:error, :invalid_query_admission_scope}

  defp index_digest({id, version, build_id})
       when is_binary(id) and id != "" and byte_size(id) <= @maximum_index_id_bytes and
              is_integer(version) and version > 0 and version <= @maximum_index_version and
              is_binary(build_id) and build_id != "" and
              byte_size(build_id) <= @maximum_build_id_bytes do
    digest =
      :crypto.hash(:sha256, [
        "ferric.flow.query.index/v1",
        <<version::unsigned-big-64>>,
        <<byte_size(id)::unsigned-big-16>>,
        id,
        build_id
      ])

    {:ok, digest}
  end

  defp index_digest(_index), do: {:error, :invalid_query_index_identity}

  defp valid_limit?(value),
    do: is_integer(value) and value > 0 and value <= @maximum_limit

  defp valid_memory_bytes?(value),
    do: is_integer(value) and value > 0 and value <= @maximum_memory_bytes

  defp default_node_memory_bytes(memory_limit_bytes)
       when is_integer(memory_limit_bytes) and memory_limit_bytes > 0 do
    memory_limit_bytes
    |> div(@default_node_memory_fraction)
    |> max(1)
    |> min(@default_max_node_memory_bytes)
  end

  defp default_node_memory_bytes(_invalid), do: @default_max_node_memory_bytes

  defp valid_orphan_grace?(value),
    do: is_integer(value) and value >= 0 and value <= @maximum_orphan_grace_ms
end
