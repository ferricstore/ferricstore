defmodule Ferricstore.FetchOrCompute do
  @moduledoc """
  Coordinates fenced cache-aside computation leases.

  Keys are routed through a fixed `PartitionSupervisor`, so unrelated cache
  misses do not serialize through one process. Each partition coalesces local
  waiters per key and monitors both the compute owner and any shared remote
  poller. Failures are replicated as short-lived opaque outcomes so waiters on
  other nodes observe them without issuing writes while polling.
  """

  alias Ferricstore.FetchOrCompute.Worker

  @default_compute_timeout_ms 30_000
  @max_default_partitions 16

  @type compute_token :: binary()
  @type fetch_result ::
          {:hit, binary()}
          | {:compute, binary(), compute_token()}
          | {:ok, binary()}
          | {:error, term()}

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    {partitions, worker_opts} =
      Keyword.pop_lazy(opts, :partitions, fn ->
        Application.get_env(:ferricstore, :fetch_or_compute_partitions, default_partitions())
      end)

    PartitionSupervisor.start_link(
      name: __MODULE__,
      partitions: max(partitions, 1),
      child_spec: Supervisor.child_spec({Worker, worker_opts}, []),
      with_arguments: fn [partition_opts], partition ->
        [Keyword.put(partition_opts, :partition, partition)]
      end
    )
  end

  @spec fetch_or_compute(binary(), pos_integer(), binary()) :: fetch_result()
  def fetch_or_compute(key, ttl_ms, hint) do
    with {:ok, ctx} <- default_instance() do
      fetch_or_compute(ctx, key, ttl_ms, hint)
    end
  end

  @spec fetch_or_compute(FerricStore.Instance.t(), binary(), pos_integer(), binary()) ::
          fetch_result()
  def fetch_or_compute(%FerricStore.Instance{} = ctx, key, ttl_ms, hint) do
    entry_key = entry_key(ctx, key)

    GenServer.call(
      worker(entry_key),
      {:fetch_or_compute, ctx, entry_key, key, ttl_ms, hint, self()},
      :infinity
    )
  end

  @spec fetch_or_compute_result(binary(), binary(), compute_token(), non_neg_integer()) ::
          :ok | {:error, term()}
  def fetch_or_compute_result(key, value, token, ttl_ms) do
    with {:ok, ctx} <- default_instance() do
      fetch_or_compute_result(ctx, key, value, token, ttl_ms)
    end
  end

  @spec fetch_or_compute_result(
          FerricStore.Instance.t(),
          binary(),
          binary(),
          compute_token(),
          non_neg_integer()
        ) :: :ok | {:error, term()}
  def fetch_or_compute_result(%FerricStore.Instance{} = ctx, key, value, token, ttl_ms) do
    entry_key = entry_key(ctx, key)

    GenServer.call(
      worker(entry_key),
      {:fetch_or_compute_result, ctx, entry_key, key, value, token, ttl_ms}
    )
  end

  @spec fetch_or_compute_error(binary(), compute_token(), binary()) ::
          :ok | {:error, term()}
  def fetch_or_compute_error(key, token, error_msg) do
    with {:ok, ctx} <- default_instance() do
      fetch_or_compute_error(ctx, key, token, error_msg)
    end
  end

  @spec fetch_or_compute_error(
          FerricStore.Instance.t(),
          binary(),
          compute_token(),
          binary()
        ) :: :ok | {:error, term()}
  def fetch_or_compute_error(%FerricStore.Instance{} = ctx, key, token, error_msg) do
    entry_key = entry_key(ctx, key)

    GenServer.call(
      worker(entry_key),
      {:fetch_or_compute_error, ctx, entry_key, key, token, error_msg}
    )
  end

  @doc false
  @spec coordinator_pid(binary()) :: pid()
  def coordinator_pid(key), do: coordinator_pid(default_instance!(), key)

  @doc false
  @spec coordinator_pid(FerricStore.Instance.t(), binary()) :: pid()
  def coordinator_pid(%FerricStore.Instance{} = ctx, key),
    do: GenServer.call(worker(entry_key(ctx, key)), :coordinator_pid)

  @doc false
  @spec debug_entry(binary()) :: map() | nil
  def debug_entry(key), do: debug_entry(default_instance!(), key)

  @doc false
  @spec debug_entry(FerricStore.Instance.t(), binary()) :: map() | nil
  def debug_entry(%FerricStore.Instance{} = ctx, key) do
    entry_key = entry_key(ctx, key)
    GenServer.call(worker(entry_key), {:debug_entry, entry_key})
  end

  defp worker(entry_key), do: {:via, PartitionSupervisor, {__MODULE__, entry_key}}

  defp entry_key(%FerricStore.Instance{name: name}, key), do: {name, key}

  defp default_instance do
    case FerricStore.Instance.fetch(:default) do
      {:ok, ctx} -> {:ok, ctx}
      :error -> {:error, "instance not initialized"}
    end
  end

  defp default_instance! do
    {:ok, ctx} = default_instance()
    ctx
  end

  defp default_partitions do
    System.schedulers_online()
    |> min(@max_default_partitions)
    |> max(2)
  end

  def default_compute_timeout_ms, do: @default_compute_timeout_ms
end

defmodule Ferricstore.FetchOrCompute.Worker do
  @moduledoc false

  use GenServer

  alias Ferricstore.Store.Router

  @default_compute_timeout_ms Ferricstore.FetchOrCompute.default_compute_timeout_ms()
  @minimum_outcome_ttl_ms 5_000
  @default_outcome_ttl_ms 30_000
  @default_max_waiters_per_key 1_024
  @default_max_active_entries 1_024
  @waiter_limit_error "ERR max fetch_or_compute waiters per key reached"
  @active_entry_limit_error "ERR max active fetch_or_compute keys per partition reached"
  @poll_interval_ms 50

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    timeout_ms =
      opts
      |> Keyword.get(:compute_timeout_ms, @default_compute_timeout_ms)
      |> positive_limit(@default_compute_timeout_ms)

    max_waiters_per_key =
      opts
      |> Keyword.get(
        :max_waiters_per_key,
        Application.get_env(
          :ferricstore,
          :fetch_or_compute_max_waiters_per_key,
          @default_max_waiters_per_key
        )
      )
      |> positive_limit(@default_max_waiters_per_key)

    max_active_entries =
      opts
      |> Keyword.get(
        :max_active_entries,
        Application.get_env(
          :ferricstore,
          :fetch_or_compute_max_active_entries_per_partition,
          @default_max_active_entries
        )
      )
      |> positive_limit(@default_max_active_entries)

    {:ok,
     %{
       compute_timeout_ms: timeout_ms,
       max_waiters_per_key: max_waiters_per_key,
       max_active_entries: max_active_entries,
       storage_module: Keyword.get(opts, :storage_module, Router),
       entries: %{},
       monitor_index: %{},
       partition: Keyword.get(opts, :partition)
     }}
  end

  @impl true
  def handle_call(:coordinator_pid, _from, state), do: {:reply, self(), state}

  def handle_call({:debug_entry, key}, _from, state) do
    {:reply, Map.get(state.entries, key), state}
  end

  def handle_call(
        {:fetch_or_compute, ctx, entry_key, key, ttl_ms, hint, _caller_pid},
        from,
        state
      ) do
    caller_pid = elem(from, 0)

    with :ok <- validate_lease_ttl(ttl_ms) do
      case Map.get(state.entries, entry_key) do
        nil ->
          if map_size(state.entries) >= state.max_active_entries do
            {:reply, {:error, @active_entry_limit_error}, state}
          else
            start_acquisition(ctx, entry_key, key, ttl_ms, hint, caller_pid, from, state)
          end

        %{kind: :owner} = entry ->
          if owner_expired?(entry) do
            state = begin_owner_cleanup(state, entry_key, entry, :release, {:error, :timeout})
            cleanup_entry = Map.fetch!(state.entries, entry_key)
            add_waiter_or_reject(state, entry_key, cleanup_entry, from, caller_pid)
          else
            add_waiter_or_reject(state, entry_key, entry, from, caller_pid)
          end

        entry ->
          add_waiter_or_reject(state, entry_key, entry, from, caller_pid)
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:fetch_or_compute_result, ctx, entry_key, key, value, token, ttl_ms},
        from,
        state
      ) do
    parent = self()
    storage_module = state.storage_module

    spawn(fn ->
      result =
        safe_storage_call(fn ->
          storage_module.fetch_or_compute_publish(ctx, key, value, ttl_ms, token)
        end)

      send(parent, {:fetch_or_compute_publish_result, entry_key, from, value, result})
    end)

    {:noreply, state}
  end

  def handle_call(
        {:fetch_or_compute_error, ctx, entry_key, key, token, error_msg},
        from,
        state
      ) do
    outcome_ttl_ms = outcome_ttl_for(state, entry_key, token)

    parent = self()
    storage_module = state.storage_module

    spawn(fn ->
      result =
        safe_storage_call(fn ->
          storage_module.fetch_or_compute_fail(ctx, key, token, error_msg, outcome_ttl_ms)
        end)

      send(parent, {:fetch_or_compute_failure_result, entry_key, from, error_msg, result})
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:fetch_or_compute_publish_result, entry_key, from, value, result}, state) do
    case result do
      :ok ->
        state = complete_local_waiters(state, entry_key, {:ok, value})
        GenServer.reply(from, :ok)
        {:noreply, state}

      {:error, _reason} = error ->
        GenServer.reply(from, error)
        {:noreply, state}

      other ->
        GenServer.reply(from, {:error, other})
        {:noreply, state}
    end
  end

  def handle_info(
        {:fetch_or_compute_failure_result, entry_key, from, error_msg, result},
        state
      ) do
    case result do
      :ok ->
        state = complete_local_waiters(state, entry_key, {:error, error_msg})
        GenServer.reply(from, :ok)
        {:noreply, state}

      {:error, _reason} = error ->
        GenServer.reply(from, error)
        {:noreply, state}

      other ->
        GenServer.reply(from, {:error, other})
        {:noreply, state}
    end
  end

  def handle_info(
        {:fetch_or_compute_owner_cleanup_result, entry_key, cleanup_pid, _result},
        state
      ) do
    case Map.get(state.entries, entry_key) do
      %{kind: :cleanup, cleanup_pid: ^cleanup_pid} = entry ->
        state = remove_entry(state, entry_key, entry)
        reply_waiters(entry.waiters, entry.cleanup_reply)
        {:noreply, state}

      _missing_or_replaced ->
        {:noreply, state}
    end
  end

  def handle_info({:fetch_or_compute_owner_cleanup_timeout, entry_key, cleanup_pid}, state) do
    case Map.get(state.entries, entry_key) do
      %{kind: :cleanup, cleanup_pid: ^cleanup_pid} = entry ->
        Process.exit(cleanup_pid, :kill)
        state = remove_entry(state, entry_key, entry)
        reply_waiters(entry.waiters, entry.cleanup_reply)
        {:noreply, state}

      _missing_or_replaced ->
        {:noreply, state}
    end
  end

  def handle_info(
        {:fetch_or_compute_acquisition_result, entry_key, acquirer_pid, result},
        state
      ) do
    case Map.get(state.entries, entry_key) do
      %{kind: :acquiring, acquirer_pid: ^acquirer_pid} = entry ->
        {:noreply, complete_acquisition(state, entry_key, entry, result)}

      _missing_or_replaced ->
        {:noreply, state}
    end
  end

  def handle_info({:fetch_or_compute_acquisition_timeout, entry_key, acquirer_pid}, state) do
    case Map.get(state.entries, entry_key) do
      %{kind: :acquiring, acquirer_pid: ^acquirer_pid} = entry ->
        Process.exit(acquirer_pid, :kill)
        state = remove_entry(state, entry_key, entry)
        reply_waiters(entry.waiters, {:error, :timeout})
        {:noreply, state}

      _missing_or_replaced ->
        {:noreply, state}
    end
  end

  def handle_info({:fetch_or_compute_poll_result, entry_key, poller_pid, result}, state) do
    case Map.get(state.entries, entry_key) do
      %{kind: :remote, poller_pid: ^poller_pid} = entry ->
        state = remove_entry(state, entry_key, entry)
        reply_waiters(entry.waiters, result)
        {:noreply, state}

      _missing_or_replaced ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
    case Map.pop(state.monitor_index, monitor_ref) do
      {nil, _monitor_index} ->
        {:noreply, state}

      {{:acquirer, entry_key, ^pid}, monitor_index} ->
        state = %{state | monitor_index: monitor_index}

        case Map.get(state.entries, entry_key) do
          %{kind: :acquiring, monitor_ref: ^monitor_ref, acquirer_pid: ^pid} = entry ->
            state = remove_entry(state, entry_key, entry, demonitor?: false)
            reply_waiters(entry.waiters, {:error, {:acquirer_terminated, reason}})
            {:noreply, state}

          _missing_or_replaced ->
            {:noreply, state}
        end

      {{:cleanup, entry_key, ^pid}, monitor_index} ->
        state = %{state | monitor_index: monitor_index}

        case Map.get(state.entries, entry_key) do
          %{kind: :cleanup, monitor_ref: ^monitor_ref, cleanup_pid: ^pid} = entry ->
            state = remove_entry(state, entry_key, entry, demonitor?: false)

            reply_waiters(
              entry.waiters,
              {:error, {:owner_cleanup_terminated, reason}}
            )

            {:noreply, state}

          _missing_or_replaced ->
            {:noreply, state}
        end

      {{:poller, entry_key, ^pid}, monitor_index} ->
        state = %{state | monitor_index: monitor_index}

        case Map.get(state.entries, entry_key) do
          %{kind: :remote, monitor_ref: ^monitor_ref, poller_pid: ^pid} = entry ->
            state = remove_entry(state, entry_key, entry, demonitor?: false)
            reply_waiters(entry.waiters, {:error, {:poller_terminated, reason}})
            {:noreply, state}

          _missing_or_replaced ->
            {:noreply, state}
        end

      {{:owner, entry_key, ^pid, token}, monitor_index} ->
        state = %{state | monitor_index: monitor_index}

        case Map.get(state.entries, entry_key) do
          %{kind: :owner, monitor_ref: ^monitor_ref, token: ^token} = entry ->
            state =
              begin_owner_cleanup(
                state,
                entry_key,
                entry,
                {:fail, "compute owner terminated"},
                {:error, "compute owner terminated"},
                demonitor_owner?: false
              )

            {:noreply, state}

          _missing_or_replaced ->
            {:noreply, state}
        end

      {{:waiter, entry_key, ^pid}, monitor_index} ->
        state = %{state | monitor_index: monitor_index}

        case Map.get(state.entries, entry_key) do
          %{waiters: waiters} = entry ->
            case remove_waiter(waiters, monitor_ref) do
              {:ok, remaining_waiters} ->
                entry = %{
                  entry
                  | waiters: remaining_waiters,
                    waiter_count: max(entry.waiter_count - 1, 0)
                }

                case {entry.kind, entry.waiter_count} do
                  {:remote, 0} ->
                    stop_remote_poller(entry)
                    {:noreply, remove_entry(state, entry_key, entry)}

                  {:acquiring, 0} ->
                    Process.exit(entry.acquirer_pid, :shutdown)
                    {:noreply, remove_entry(state, entry_key, entry)}

                  _other ->
                    {:noreply, put_entry(state, entry_key, entry)}
                end

              :missing ->
                {:noreply, state}
            end

          _missing_or_replaced ->
            {:noreply, state}
        end

      {_stale_monitor, monitor_index} ->
        {:noreply, %{state | monitor_index: monitor_index}}
    end
  end

  def handle_info({:fetch_or_compute_owner_timeout, entry_key, token}, state) do
    case Map.get(state.entries, entry_key) do
      %{kind: :owner, token: ^token} = entry ->
        state = begin_owner_cleanup(state, entry_key, entry, :release, {:error, :timeout})
        {:noreply, state}

      _missing_or_replaced ->
        {:noreply, state}
    end
  end

  defp start_acquisition(ctx, entry_key, key, ttl_ms, hint, caller_pid, from, state) do
    lease_ttl_ms = min(ttl_ms, state.compute_timeout_ms)
    storage_module = state.storage_module
    parent = self()
    caller_monitor_ref = Process.monitor(caller_pid)

    {acquirer_pid, acquirer_monitor_ref} =
      spawn_monitor(fn ->
        result = acquire(storage_module, ctx, key, lease_ttl_ms)
        send(parent, {:fetch_or_compute_acquisition_result, entry_key, self(), result})
      end)

    timeout_ref =
      Process.send_after(
        self(),
        {:fetch_or_compute_acquisition_timeout, entry_key, acquirer_pid},
        lease_ttl_ms
      )

    entry = %{
      kind: :acquiring,
      acquirer_pid: acquirer_pid,
      monitor_ref: acquirer_monitor_ref,
      timeout_ref: timeout_ref,
      waiters: [{from, caller_pid, caller_monitor_ref}],
      waiter_count: 1,
      instance_ctx: ctx,
      key: key,
      hint: hint,
      lease_ttl_ms: lease_ttl_ms,
      storage_module: storage_module
    }

    state =
      state
      |> put_entry(entry_key, entry)
      |> put_monitor(acquirer_monitor_ref, {:acquirer, entry_key, acquirer_pid})
      |> put_monitor(caller_monitor_ref, {:waiter, entry_key, caller_pid})

    {:noreply, state}
  end

  defp acquire(storage_module, ctx, key, lease_ttl_ms) do
    case safe_storage_call(fn -> storage_module.get(ctx, key) end) do
      nil ->
        token = generate_compute_token()

        case safe_storage_call(fn ->
               storage_module.fetch_or_compute_lock(ctx, key, token, lease_ttl_ms)
             end) do
          :ok -> recheck_after_lock(storage_module, ctx, key, token)
          {:error, :keys_locked} -> :remote
          {:error, _reason} = error -> error
          other -> {:error, other}
        end

      {:error, _reason} = error ->
        error

      value ->
        {:hit, value}
    end
  end

  defp recheck_after_lock(storage_module, ctx, key, token) do
    case safe_storage_call(fn -> storage_module.get(ctx, key) end) do
      nil ->
        {:locked, token}

      {:error, _reason} = error ->
        release_acquired_lock(storage_module, ctx, key, token)
        error

      value ->
        release_acquired_lock(storage_module, ctx, key, token)
        {:hit, value}
    end
  end

  defp release_acquired_lock(storage_module, ctx, key, token) do
    _ =
      safe_storage_call(fn ->
        storage_module.fetch_or_compute_release(ctx, key, token)
      end)

    :ok
  end

  defp complete_acquisition(state, entry_key, entry, {:locked, token}) do
    state = finish_acquirer(state, entry)
    [{owner_from, owner_pid, owner_monitor_ref} | remaining_fifo] = Enum.reverse(entry.waiters)
    remaining_waiters = Enum.reverse(remaining_fifo)
    now = System.monotonic_time(:millisecond)

    timeout_ref =
      Process.send_after(
        self(),
        {:fetch_or_compute_owner_timeout, entry_key, token},
        entry.lease_ttl_ms
      )

    owner_entry = %{
      kind: :owner,
      computer_pid: owner_pid,
      monitor_ref: owner_monitor_ref,
      timeout_ref: timeout_ref,
      waiters: remaining_waiters,
      waiter_count: entry.waiter_count - 1,
      started_at: now,
      deadline_ms: now + entry.lease_ttl_ms,
      outcome_ttl_ms: max(entry.lease_ttl_ms, @minimum_outcome_ttl_ms),
      token: token,
      instance_ctx: entry.instance_ctx,
      key: entry.key,
      storage_module: entry.storage_module
    }

    GenServer.reply(owner_from, {:compute, entry.hint, token})

    state
    |> put_entry(entry_key, owner_entry)
    |> put_monitor(owner_monitor_ref, {:owner, entry_key, owner_pid, token})
  end

  defp complete_acquisition(state, entry_key, entry, :remote) do
    state = finish_acquirer(state, entry)

    {remote_entry, state} =
      start_remote_poller(
        entry.instance_ctx,
        entry_key,
        entry.key,
        entry.lease_ttl_ms,
        entry.storage_module,
        state
      )

    remote_entry = %{
      remote_entry
      | waiters: entry.waiters,
        waiter_count: entry.waiter_count
    }

    put_entry(state, entry_key, remote_entry)
  end

  defp complete_acquisition(state, entry_key, entry, reply) do
    state = remove_entry(state, entry_key, entry)
    reply_waiters(entry.waiters, reply)
    state
  end

  defp finish_acquirer(state, entry) do
    Process.cancel_timer(entry.timeout_ref)
    Process.demonitor(entry.monitor_ref, [:flush])
    %{state | monitor_index: Map.delete(state.monitor_index, entry.monitor_ref)}
  end

  defp start_remote_poller(ctx, entry_key, key, timeout_ms, storage_module, state) do
    parent = self()

    {poller_pid, monitor_ref} =
      spawn_monitor(fn ->
        result =
          poll_loop(
            storage_module,
            ctx,
            key,
            System.monotonic_time(:millisecond) + timeout_ms
          )

        send(parent, {:fetch_or_compute_poll_result, entry_key, self(), result})
      end)

    entry = %{
      kind: :remote,
      poller_pid: poller_pid,
      monitor_ref: monitor_ref,
      waiters: [],
      waiter_count: 0,
      started_at: System.monotonic_time(:millisecond)
    }

    {entry, put_monitor(state, monitor_ref, {:poller, entry_key, poller_pid})}
  end

  defp poll_loop(storage_module, ctx, key, deadline_ms) do
    remaining_ms = deadline_ms - System.monotonic_time(:millisecond)

    if remaining_ms <= 0 do
      {:error, :timeout}
    else
      case safe_storage_call(fn -> storage_module.get(ctx, key) end) do
        nil ->
          case safe_storage_call(fn -> storage_module.fetch_or_compute_outcome(ctx, key) end) do
            :pending ->
              Process.sleep(min(@poll_interval_ms, remaining_ms))
              poll_loop(storage_module, ctx, key, deadline_ms)

            {:failed, error} ->
              {:error, error}

            {:error, _reason} = error ->
              error
          end

        {:error, _reason} = error ->
          error

        value ->
          {:ok, value}
      end
    end
  end

  defp begin_owner_cleanup(state, entry_key, entry, action, waiter_reply, opts \\ []) do
    if Keyword.get(opts, :demonitor_owner?, true) do
      Process.demonitor(entry.monitor_ref, [:flush])
    end

    Process.cancel_timer(entry.timeout_ref)
    parent = self()

    {cleanup_pid, cleanup_monitor_ref} =
      spawn_monitor(fn ->
        result = owner_cleanup_call(entry, action)
        send(parent, {:fetch_or_compute_owner_cleanup_result, entry_key, self(), result})
      end)

    cleanup_timeout_ref =
      Process.send_after(
        self(),
        {:fetch_or_compute_owner_cleanup_timeout, entry_key, cleanup_pid},
        state.compute_timeout_ms
      )

    cleanup_entry =
      Map.merge(entry, %{
        kind: :cleanup,
        cleanup_pid: cleanup_pid,
        monitor_ref: cleanup_monitor_ref,
        timeout_ref: cleanup_timeout_ref,
        cleanup_reply: waiter_reply
      })

    state = %{
      state
      | monitor_index: Map.delete(state.monitor_index, entry.monitor_ref)
    }

    state
    |> put_entry(entry_key, cleanup_entry)
    |> put_monitor(cleanup_monitor_ref, {:cleanup, entry_key, cleanup_pid})
  end

  defp owner_cleanup_call(entry, :release) do
    safe_storage_call(fn ->
      entry.storage_module.fetch_or_compute_release(
        entry.instance_ctx,
        entry.key,
        entry.token
      )
    end)
  end

  defp owner_cleanup_call(entry, {:fail, error}) do
    safe_storage_call(fn ->
      entry.storage_module.fetch_or_compute_fail(
        entry.instance_ctx,
        entry.key,
        entry.token,
        error,
        entry.outcome_ttl_ms
      )
    end)
  end

  defp complete_local_waiters(state, key, reply) do
    case Map.get(state.entries, key) do
      %{kind: :acquiring} ->
        state

      %{kind: :cleanup} ->
        state

      %{kind: :remote} = entry ->
        stop_remote_poller(entry)
        state = remove_entry(state, key, entry)
        reply_waiters(entry.waiters, reply)
        state

      %{kind: :owner} = entry ->
        state = remove_entry(state, key, entry)
        reply_waiters(entry.waiters, reply)
        state

      nil ->
        state
    end
  end

  defp stop_remote_poller(entry) do
    if Process.alive?(entry.poller_pid) do
      Process.exit(entry.poller_pid, :shutdown)
    end
  end

  defp remove_entry(state, key, entry, opts \\ []) do
    demonitor? = Keyword.get(opts, :demonitor?, true)

    if demonitor? do
      Process.demonitor(entry.monitor_ref, [:flush])
    end

    if timeout_ref = Map.get(entry, :timeout_ref) do
      Process.cancel_timer(timeout_ref)
    end

    state = remove_waiter_monitors(state, entry.waiters)

    %{
      state
      | entries: Map.delete(state.entries, key),
        monitor_index: Map.delete(state.monitor_index, entry.monitor_ref)
    }
  end

  defp outcome_ttl_for(state, key, token) do
    case Map.get(state.entries, key) do
      %{kind: :owner, token: ^token, outcome_ttl_ms: ttl_ms} -> ttl_ms
      _missing_or_remote -> @default_outcome_ttl_ms
    end
  end

  defp generate_compute_token do
    random = :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
    "#{node()}:#{random}"
  end

  defp owner_expired?(entry) do
    System.monotonic_time(:millisecond) >= entry.deadline_ms
  end

  defp validate_lease_ttl(ttl_ms) when is_integer(ttl_ms) and ttl_ms > 0, do: :ok
  defp validate_lease_ttl(_ttl_ms), do: {:error, "ERR invalid fetch_or_compute ttl"}

  defp put_entry(state, key, entry), do: %{state | entries: Map.put(state.entries, key, entry)}

  defp put_monitor(state, monitor_ref, value) do
    %{state | monitor_index: Map.put(state.monitor_index, monitor_ref, value)}
  end

  defp add_waiter_or_reject(state, entry_key, entry, from, caller_pid) do
    if entry.waiter_count >= state.max_waiters_per_key do
      {:reply, {:error, @waiter_limit_error}, state}
    else
      monitor_ref = Process.monitor(caller_pid)

      entry = %{
        entry
        | waiters: [{from, caller_pid, monitor_ref} | entry.waiters],
          waiter_count: entry.waiter_count + 1
      }

      state =
        state
        |> put_entry(entry_key, entry)
        |> put_monitor(monitor_ref, {:waiter, entry_key, caller_pid})

      {:noreply, state}
    end
  end

  defp remove_waiter(waiters, monitor_ref) do
    case Enum.split_with(waiters, fn {_from, _pid, ref} -> ref == monitor_ref end) do
      {[_waiter], remaining} -> {:ok, remaining}
      {[], _unchanged} -> :missing
    end
  end

  defp remove_waiter_monitors(state, waiters) do
    Enum.reduce(waiters, state, fn {_from, _pid, monitor_ref}, acc ->
      Process.demonitor(monitor_ref, [:flush])
      %{acc | monitor_index: Map.delete(acc.monitor_index, monitor_ref)}
    end)
  end

  defp reply_waiters(waiters, reply) do
    waiters
    |> Enum.reverse()
    |> Enum.each(fn {waiter_from, _pid, _monitor_ref} -> GenServer.reply(waiter_from, reply) end)
  end

  defp safe_storage_call(fun) do
    fun.()
  catch
    kind, reason -> {:error, {:storage_call_failed, kind, reason}}
  end

  defp positive_limit(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_limit(_value, default), do: default
end
