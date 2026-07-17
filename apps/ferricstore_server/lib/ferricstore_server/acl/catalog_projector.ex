defmodule FerricstoreServer.Acl.CatalogProjector do
  @moduledoc false

  use GenServer

  require Logger

  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Management.ACL

  @default_poll_interval_ms 1_000
  @readiness_key {__MODULE__, :readiness}

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @spec readiness() :: :ready | {:stale, term()}
  def readiness do
    :persistent_term.get(@readiness_key, {:stale, :not_started})
  end

  @spec ready?() :: boolean()
  def ready?, do: readiness() == :ready

  @doc false
  @spec mark_ready() :: :ok
  def mark_ready, do: put_readiness(:ready)

  @doc false
  @spec mark_stale(term()) :: :ok
  def mark_stale(reason) do
    case :persistent_term.get(@readiness_key, :undefined) do
      {:stale, _existing_reason} -> :ok
      _other -> put_readiness({:stale, reason})
    end
  end

  @doc false
  @spec status(GenServer.server()) :: map()
  def status(server \\ __MODULE__), do: GenServer.call(server, :status)

  @doc false
  @spec poll_now(GenServer.server()) :: map()
  def poll_now(server \\ __MODULE__), do: GenServer.call(server, :poll_now)

  @doc false
  @spec require_revision(non_neg_integer(), GenServer.server()) :: map()
  def require_revision(revision, server \\ __MODULE__)
      when is_integer(revision) and revision >= 0 do
    GenServer.call(server, {:require_revision, revision})
  end

  @impl true
  def init(opts) do
    poll_interval_ms =
      Keyword.get_lazy(opts, :poll_interval_ms, fn ->
        Application.get_env(
          :ferricstore,
          :acl_catalog_poll_interval_ms,
          @default_poll_interval_ms
        )
      end)

    publish_status =
      Keyword.get(
        opts,
        :publish_status,
        Keyword.get(opts, :name, __MODULE__) == __MODULE__
      )

    join_invalidation_group =
      Keyword.get(opts, :join_invalidation_group, publish_status)

    if publish_status, do: mark_stale(:initializing)

    result =
      with true <- is_integer(poll_interval_ms) and poll_interval_ms > 0,
           {:ok, store} <- resolve_store(Keyword.get(opts, :store)),
           :ok <- ACL.ensure_default_catalog(store),
           {:ok, revision} <- read_revision(store),
           :ok <- ACL.reconcile_catalog(store, await_projector: false),
           ^revision <- Acl.catalog_projection_revision() do
        schedule_poll(poll_interval_ms)

        state = %{
          store: store,
          poll_interval_ms: poll_interval_ms,
          revision: revision,
          target_revision: revision,
          ready: true,
          last_error: nil,
          consecutive_failures: 0,
          reconciliations: 1,
          publish_status: publish_status,
          join_invalidation_group: join_invalidation_group
        }

        if join_invalidation_group, do: join_invalidation_group()
        publish_ready(state)
        {:ok, state}
      else
        false -> {:stop, :invalid_acl_catalog_poll_interval}
        actual when is_integer(actual) -> {:stop, {:acl_projection_revision_mismatch, actual}}
        {:error, reason} -> {:stop, {:acl_catalog_unavailable, reason}}
        :unavailable -> {:stop, {:acl_catalog_unavailable, :unavailable}}
      end

    case result do
      {:ok, _state} = ok ->
        ok

      {:stop, reason} = stopped ->
        if publish_status, do: mark_stale(reason)
        stopped
    end
  end

  @impl true
  def handle_info(:poll, state) do
    state = poll(state)
    schedule_poll(state.poll_interval_ms)
    {:noreply, state}
  end

  def handle_info(
        {:acl_catalog_changed, kind, username, previous_revision, revision},
        state
      )
      when kind in [:upsert, :delete, :all] and
             (is_binary(username) or username == :all) and
             is_integer(previous_revision) and previous_revision >= -1 and
             is_integer(revision) and revision >= 0 do
    {:noreply, project_catalog_change(state, kind, username, previous_revision, revision)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_call(:status, _from, state), do: {:reply, status_map(state), state}

  def handle_call(:poll_now, _from, state) do
    state = poll(state)
    {:reply, status_map(state), state}
  end

  def handle_call({:require_revision, revision}, _from, state)
      when is_integer(revision) and revision >= 0 do
    state = require_revision_state(state, revision)
    {:reply, status_map(state), state}
  end

  defp poll(state) do
    do_poll(state)
  rescue
    error -> projection_failed(state, {:exception, error})
  catch
    kind, reason -> projection_failed(state, {kind, reason})
  end

  defp do_poll(state) do
    with {:ok, store} <- resolve_store(state.store),
         {:ok, revision} <- read_revision(store) do
      cond do
        not revision_reaches_target?(revision, state.target_revision) ->
          projection_failed(
            state,
            {:acl_catalog_revision_behind, state.target_revision, revision}
          )

        Acl.catalog_projection_revision() == revision ->
          projection_ready(state, revision)

        true ->
          case ACL.reconcile_catalog(store, await_projector: false) do
            :ok ->
              if Acl.catalog_projection_revision() == revision do
                state
                |> Map.update!(:reconciliations, &(&1 + 1))
                |> projection_ready(revision)
              else
                projection_failed(state, :acl_projection_revision_mismatch)
              end

            {:error, reason} ->
              projection_failed(state, reason)

            other ->
              projection_failed(state, {:invalid_reconcile_result, other})
          end
      end
    else
      {:error, reason} -> projection_failed(state, reason)
      :unavailable -> projection_failed(state, :unavailable)
    end
  end

  defp projection_ready(state, revision) do
    recovered = not state.ready

    state = %{
      state
      | revision: revision,
        ready: true,
        last_error: nil,
        consecutive_failures: 0
    }

    publish_ready(state)

    if recovered do
      :telemetry.execute(
        [:ferricstore, :acl, :catalog_projection, :recovered],
        %{count: 1},
        %{revision: revision}
      )
    end

    state
  end

  defp projection_failed(state, reason) do
    first_failure = state.ready
    failures = state.consecutive_failures + 1
    state = %{state | ready: false, last_error: reason, consecutive_failures: failures}

    if state.publish_status, do: mark_stale(reason)

    :telemetry.execute(
      [:ferricstore, :acl, :catalog_projection, :failed],
      %{count: 1, consecutive_failures: failures},
      %{reason: reason, revision: state.revision}
    )

    if first_failure do
      Logger.error("ACL catalog projection became stale: #{inspect(reason)}")
    end

    state
  end

  defp publish_ready(%{publish_status: true}), do: mark_ready()
  defp publish_ready(_state), do: :ok

  defp status_map(state) do
    Map.take(state, [
      :revision,
      :target_revision,
      :ready,
      :last_error,
      :consecutive_failures,
      :reconciliations
    ])
  end

  defp require_revision_state(
         %{ready: true, revision: current, target_revision: target} = state,
         revision
       )
       when is_integer(current) and current >= revision and
              (is_nil(target) or current >= target) do
    if revision_fence_satisfied?(state, revision) do
      state
    else
      state
      |> await_revision_state(revision)
      |> poll()
    end
  end

  defp require_revision_state(state, revision) do
    state
    |> await_revision_state(revision)
    |> poll()
  end

  defp await_revision_state(state, revision) do
    target_revision = max(state.target_revision || 0, revision)

    state = %{
      state
      | target_revision: target_revision,
        ready: false,
        last_error: {:awaiting_acl_catalog_revision, target_revision}
    }

    if state.publish_status do
      mark_stale({:awaiting_acl_catalog_revision, target_revision})
    end

    state
  end

  defp revision_reaches_target?(revision, target)
       when is_integer(revision) and is_integer(target),
       do: revision >= target

  defp revision_reaches_target?(_revision, nil), do: true
  defp revision_reaches_target?(_revision, _target), do: false

  defp join_invalidation_group do
    scope = FerricstoreServer.Connection.Auth.acl_projector_scope()
    group = FerricstoreServer.Connection.Auth.acl_projector_group()
    :ok = :pg.join(scope, group, self())
  catch
    :error, _reason -> :ok
    :exit, _reason -> :ok
  end

  defp resolve_store(%{shard_count: shard_count} = store)
       when is_integer(shard_count) and shard_count > 0,
       do: {:ok, store}

  defp resolve_store(nil) do
    {:ok, FerricStore.Instance.get(:default)}
  rescue
    _error -> {:error, :store_unavailable}
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  defp resolve_store(_invalid), do: {:error, :store_unavailable}

  defp read_revision(store) do
    case Router.server_catalog_revision(store, "acl") do
      {:ok, nil} -> {:ok, nil}
      {:ok, encoded} -> ServerCatalog.decode_revision(encoded)
      :unavailable -> {:error, :catalog_unavailable}
      {:error, _reason} = error -> error
    end
  end

  defp schedule_poll(interval_ms) do
    Process.send_after(self(), :poll, interval_ms)
    :ok
  end

  defp put_readiness(readiness) do
    if :persistent_term.get(@readiness_key, :undefined) != readiness do
      :persistent_term.put(@readiness_key, readiness)
    end

    :ok
  end

  defp project_catalog_change(state, _kind, _username, _previous_revision, revision)
       when is_integer(state.revision) and state.revision >= revision do
    if revision_fence_satisfied?(state, revision) do
      state
    else
      require_revision_state(state, revision)
    end
  end

  defp project_catalog_change(state, :all, :all, _previous_revision, revision) do
    require_revision_state(state, revision)
  end

  defp project_catalog_change(state, kind, username, previous_revision, revision)
       when kind in [:upsert, :delete] and is_binary(username) do
    state = await_revision_state(state, revision)

    if incremental_projection_base?(state, previous_revision) do
      case stable_catalog_entry(state.store, kind, username, revision) do
        {:ok, encoded} ->
          project_stable_catalog_entry(
            state,
            username,
            encoded,
            previous_revision,
            revision
          )

        {:error, _reason} ->
          poll(state)
      end
    else
      poll(state)
    end
  rescue
    error -> projection_failed(state, {:exception, error})
  catch
    kind, reason -> projection_failed(state, {kind, reason})
  end

  defp project_catalog_change(state, _kind, _username, _previous_revision, revision),
    do: require_revision_state(state, revision)

  defp incremental_projection_base?(state, previous_revision) do
    state.revision == previous_revision and
      Acl.catalog_projection_revision() == previous_revision
  end

  defp stable_catalog_entry(store, kind, username, revision) do
    with {:ok, ^revision} <- read_revision(store),
         {:ok, encoded} <- read_catalog_entry(store, kind, username, revision),
         {:ok, ^revision} <- read_revision(store) do
      {:ok, encoded}
    else
      _changed_or_invalid -> {:error, :acl_catalog_changed_concurrently}
    end
  end

  defp read_catalog_entry(store, :upsert, username, revision) do
    case Router.server_catalog_entry(store, "acl", username) do
      {:ok, encoded} when is_binary(encoded) ->
        case ServerCatalog.decode_entry(encoded) do
          {:ok, %{version: ^revision, value: value}} when is_binary(value) -> {:ok, encoded}
          _invalid -> {:error, :invalid_acl_catalog_entry}
        end

      _missing_or_unavailable ->
        {:error, :missing_acl_catalog_entry}
    end
  end

  defp read_catalog_entry(store, :delete, username, revision) do
    case Router.server_catalog_entry(store, "acl", username) do
      {:ok, nil} -> {:ok, ServerCatalog.encode_entry(revision, :deleted)}
      _present_or_unavailable -> {:error, :acl_catalog_delete_not_stable}
    end
  end

  defp project_stable_catalog_entry(
         state,
         username,
         encoded,
         previous_revision,
         revision
       ) do
    expected_revision =
      if previous_revision == -1,
        do: nil,
        else: ServerCatalog.encode_revision(previous_revision)

    with :ok <- Acl.project_catalog_entry(username, encoded, expected_revision),
         {:ok, ^revision} <- read_revision(state.store),
         ^revision <- Acl.catalog_projection_revision() do
      projection_ready(state, revision)
    else
      _changed_or_failed -> poll(state)
    end
  end

  defp revision_fence_satisfied?(state, revision) do
    local_revision = Acl.catalog_projection_revision()
    readiness_valid? = not state.publish_status or ready?()
    readiness_valid? and is_integer(local_revision) and local_revision >= revision
  end
end
