defmodule Ferricstore.Flow.Query.IndexRegistry do
  @moduledoc false

  use GenServer

  alias Ferricstore.Bitcask.NIF

  alias Ferricstore.Flow.Query.{
    CompositeCounter,
    IndexDefinition,
    RegisteredIndex,
    RegistrySnapshot
  }

  alias Ferricstore.FS
  alias Ferricstore.TermCodec
  alias FerricStore.Flow.MetadataExtension

  alias Ferricstore.Flow.Query.{
    IndexCatalog,
    IndexRegistryJournal,
    IndexRegistryOverview
  }

  @snapshot_tag :flow_query_index_registry
  @snapshot_version 1
  @snapshot_relative_path "flow_query/index-registry.term"
  @max_cursor_bytes 511
  @phases [:snapshot, :backfill, :done]
  @validation_phases [:source, :index, :counter, :cleanup, :done]
  @retirement_phases [:fence, :index, :counter, :reverse, :cleanup, :done]
  @states [:building, :validating, :active, :retiring, :failed]
  @max_registry_entries 32
  @max_snapshot_bytes 64 * 1_024 * 1_024
  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.get(opts, :name) || server_name(Keyword.fetch!(opts, :instance_ctx))
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec server_name(map() | atom()) :: atom()
  def server_name(%{name: name}), do: server_name(name)
  def server_name(:default), do: __MODULE__
  def server_name(name) when is_atom(name), do: :"#{name}.Flow.Query.IndexRegistry"

  @spec cache_table(map() | atom()) :: atom()
  def cache_table(%{name: name}), do: cache_table(name)
  def cache_table(:default), do: Ferricstore.Flow.Query.IndexRegistry.Cache
  def cache_table(name) when is_atom(name), do: :"#{name}.Flow.Query.IndexRegistry.Cache"

  @spec snapshot_path(map()) :: binary()
  def snapshot_path(%{data_dir: data_dir}) when is_binary(data_dir) and data_dir != "",
    do: Path.join(data_dir, @snapshot_relative_path)

  @doc false
  @spec journal_path(map()) :: binary()
  defdelegate journal_path(ctx), to: IndexRegistryJournal, as: :path

  @spec snapshot(map(), non_neg_integer()) ::
          {:ok, RegistrySnapshot.t()} | {:error, :query_index_registry_unavailable}
  def snapshot(%{name: name, shard_count: shard_count}, shard_index)
      when is_atom(name) and is_integer(shard_count) and is_integer(shard_index) and
             shard_index >= 0 and shard_index < shard_count do
    case :ets.lookup(cache_table(name), :snapshot) do
      [{:snapshot, %RegistrySnapshot{} = snapshot}] -> {:ok, snapshot}
      _missing -> {:error, :query_index_registry_unavailable}
    end
  rescue
    ArgumentError -> {:error, :query_index_registry_unavailable}
  end

  def snapshot(_ctx, _shard_index), do: {:error, :query_index_registry_unavailable}

  @doc false
  @spec active_identity?(map() | atom(), {binary(), pos_integer(), binary()}) ::
          {:ok, boolean()} | {:error, :query_index_registry_unavailable}
  def active_identity?(%{name: name}, identity), do: active_identity?(name, identity)

  def active_identity?(name, {id, version, build_id} = identity)
      when is_atom(name) and is_binary(id) and id != "" and is_integer(version) and version > 0 and
             version <= @max_u64 and is_binary(build_id) and build_id != "" do
    case :ets.lookup(cache_table(name), :active_identities) do
      [{:active_identities, %MapSet{} = identities}] ->
        {:ok, MapSet.member?(identities, identity)}

      _missing ->
        {:error, :query_index_registry_unavailable}
    end
  rescue
    ArgumentError -> {:error, :query_index_registry_unavailable}
  end

  def active_identity?(_ctx, _identity), do: {:error, :query_index_registry_unavailable}

  @spec checkpoint_build(GenServer.server(), binary(), non_neg_integer(), keyword()) ::
          :ok | {:error, atom() | term()}
  def checkpoint_build(server, build_id, shard_index, progress),
    do: GenServer.call(server, {:checkpoint_build, build_id, shard_index, progress})

  @spec complete_build_shard(GenServer.server(), binary(), non_neg_integer(), keyword()) ::
          :ok | {:error, atom() | term()}
  def complete_build_shard(server, build_id, shard_index, progress \\ []),
    do: GenServer.call(server, {:complete_build_shard, build_id, shard_index, progress})

  @spec checkpoint_validation(GenServer.server(), binary(), non_neg_integer(), keyword()) ::
          :ok | {:error, atom() | term()}
  def checkpoint_validation(server, build_id, shard_index, progress),
    do: GenServer.call(server, {:checkpoint_validation, build_id, shard_index, progress})

  @spec restart_validation_shard(GenServer.server(), binary(), non_neg_integer()) ::
          :ok | {:error, atom() | term()}
  def restart_validation_shard(server, build_id, shard_index),
    do: GenServer.call(server, {:restart_validation_shard, build_id, shard_index})

  @spec complete_validation_shard(GenServer.server(), binary(), non_neg_integer(), keyword()) ::
          :ok | {:error, atom() | term()}
  def complete_validation_shard(server, build_id, shard_index, progress \\ []),
    do: GenServer.call(server, {:complete_validation_shard, build_id, shard_index, progress})

  @spec validation_failed(GenServer.server(), binary(), keyword()) ::
          :ok | {:error, atom() | term()}
  def validation_failed(server, build_id, evidence),
    do: GenServer.call(server, {:validation_failed, build_id, evidence})

  @spec activate_build(GenServer.server(), binary()) :: :ok | {:error, atom() | term()}
  def activate_build(server, build_id), do: GenServer.call(server, {:activate_build, build_id})

  @spec checkpoint_retirement(
          GenServer.server(),
          binary(),
          pos_integer(),
          non_neg_integer(),
          keyword()
        ) :: :ok | {:error, atom() | term()}
  def checkpoint_retirement(server, id, version, shard_index, progress),
    do: GenServer.call(server, {:checkpoint_retirement, id, version, shard_index, progress})

  @spec complete_retirement_shard(
          GenServer.server(),
          binary(),
          pos_integer(),
          non_neg_integer(),
          keyword()
        ) :: {:ok, :pending | :complete} | {:error, atom() | term()}
  def complete_retirement_shard(server, id, version, shard_index, progress \\ []),
    do:
      GenServer.call(
        server,
        {:complete_retirement_shard, id, version, shard_index, progress}
      )

  @spec status(GenServer.server(), binary(), pos_integer()) ::
          {:ok, map()} | {:error, :query_index_not_found}
  def status(server, id, version), do: GenServer.call(server, {:status, id, version})

  @spec build_status(GenServer.server(), binary()) ::
          {:ok, map()} | {:error, :query_index_build_not_found | :inconsistent_query_index_build}
  def build_status(server, build_id), do: GenServer.call(server, {:build_status, build_id})

  @spec overview(GenServer.server()) :: {:ok, map()}
  def overview(server), do: GenServer.call(server, :overview)

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    catalog_path = Keyword.get(opts, :catalog_path, IndexCatalog.default_path())

    with :ok <- validate_context(ctx),
         {:ok, metadata_contract} <- index_metadata_contract(ctx),
         {:ok, catalog} <-
           IndexCatalog.load(catalog_path, scope_bytes: metadata_contract.scope_bytes),
         {:ok, loaded, persist?} <- load_registry(ctx, catalog, metadata_contract),
         {:ok, table} <- create_cache(ctx),
         state = Map.merge(loaded, %{instance_ctx: ctx, cache_table: table}),
         :ok <- maybe_persist(state, persist?),
         :ok <- publish(state) do
      {:ok, state}
    else
      {:error, _reason} = error -> {:stop, elem(error, 1)}
    end
  end

  defp index_metadata_contract(%{
         flow_metadata_snapshot:
           %MetadataExtension.Snapshot{
             mode: mode,
             generation: generation,
             schema_digest: schema_digest
           } = snapshot
       })
       when mode in [:dedicated, :shared] and is_integer(generation) and generation >= 0 and
              generation <= @max_u64 and is_binary(schema_digest) and
              byte_size(schema_digest) == 32 do
    with {:ok, scope_bytes} <- MetadataExtension.fixed_scope_bytes(snapshot) do
      {:ok,
       %{
         mode: mode,
         generation: generation,
         schema_digest: schema_digest,
         scope_bytes: scope_bytes
       }}
    end
  end

  defp index_metadata_contract(_ctx), do: {:error, :query_index_metadata_snapshot_required}

  @impl true
  def handle_call(:overview, _from, state) do
    indexes = IndexRegistryOverview.build(state.entries, state.instance_ctx.shard_count)

    {:reply,
     {:ok,
      %{
        epoch: state.epoch,
        catalog_version: state.catalog_version,
        indexes: indexes
      }}, state}
  end

  def handle_call({:status, id, version}, _from, state) do
    reply =
      case Map.fetch(state.entries, {id, version}) do
        {:ok, entry} -> {:ok, public_status(entry, state.instance_ctx.shard_count)}
        :error -> {:error, :query_index_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:build_status, build_id}, _from, state) do
    entries = entries_for_build(state, build_id)

    reply =
      case entries do
        [] ->
          {:error, :query_index_build_not_found}

        entries ->
          checkpoint_sets = entries |> Enum.map(& &1.checkpoints) |> Enum.uniq()
          validation_sets = entries |> Enum.map(&validation_checkpoints/1) |> Enum.uniq()

          case {checkpoint_sets, validation_sets} do
            {[checkpoints], [validation_checkpoints]} ->
              {:ok,
               %{
                 build_id: build_id,
                 checkpoints: checkpoints,
                 validation_checkpoints: validation_checkpoints,
                 entries: Enum.map(entries, &public_status(&1, state.instance_ctx.shard_count))
               }}

            _divergent ->
              {:error, :inconsistent_query_index_build}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:checkpoint_build, build_id, shard_index, progress}, _from, state) do
    case prepare_build_checkpoint(state, build_id, shard_index, progress) do
      {:ok, next_state, checkpoint} ->
        commit_progress(next_state, {:build, build_id, shard_index, checkpoint}, :ok, state)

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:complete_build_shard, build_id, shard_index, progress}, _from, state) do
    entries = entries_for_build(state, build_id, :building)

    with false <- entries == [],
         :ok <- validate_shard(state, shard_index),
         {:ok, checkpoints} <- complete_group_checkpoints(entries, shard_index, progress) do
      next_entries =
        Enum.reduce(entries, state.entries, fn entry, acc ->
          key = {entry.definition.id, entry.definition.version}
          checkpoint = Map.fetch!(checkpoints, key)
          entry = put_in(entry.checkpoints[shard_index], checkpoint)

          entry =
            if all_shards_complete?(entry, state.instance_ctx.shard_count),
              do: %{entry | state: :validating, validation: new_validation()},
              else: entry

          Map.put(acc, key, entry)
        end)

      commit_with_epoch(%{state | entries: next_entries}, :ok, state)
    else
      true -> {:reply, {:error, :query_index_build_not_found}, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call({:checkpoint_validation, build_id, shard_index, progress}, _from, state) do
    case prepare_validation_checkpoint(state, build_id, shard_index, progress) do
      {:ok, next_state, checkpoint} ->
        commit_progress(next_state, {:validation, build_id, shard_index, checkpoint}, :ok, state)

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:restart_validation_shard, build_id, shard_index}, _from, state) do
    entries = entries_for_build(state, build_id, :validating)

    with false <- entries == [],
         :ok <- validate_shard(state, shard_index),
         :ok <- ensure_group_validation_restartable(entries, shard_index) do
      checkpoint = empty_validation_checkpoint()

      next_entries =
        Enum.reduce(entries, state.entries, fn entry, acc ->
          key = {entry.definition.id, entry.definition.version}
          Map.put(acc, key, put_in(entry.validation.checkpoints[shard_index], checkpoint))
        end)

      commit(%{state | entries: next_entries}, :ok, state)
    else
      true -> {:reply, {:error, :query_index_build_not_found}, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call({:complete_validation_shard, build_id, shard_index, progress}, _from, state) do
    entries = entries_for_build(state, build_id, :validating)

    with false <- entries == [],
         :ok <- validate_shard(state, shard_index),
         {:ok, checkpoints} <-
           complete_group_validation_checkpoints(entries, shard_index, progress),
         {:ok, next_entries} <-
           complete_validation_entries(
             state.entries,
             entries,
             checkpoints,
             shard_index,
             state.instance_ctx.shard_count
           ) do
      commit(%{state | entries: next_entries}, :ok, state)
    else
      true -> {:reply, {:error, :query_index_build_not_found}, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call({:validation_failed, build_id, evidence}, _from, state) do
    entries = entries_for_build(state, build_id, :validating)

    with false <- entries == [],
         {:ok, validation} <- normalize_failed_validation(evidence) do
      next_entries =
        Enum.reduce(entries, state.entries, fn entry, acc ->
          key = {entry.definition.id, entry.definition.version}
          validation = %{validation | checkpoints: entry.validation.checkpoints}

          Map.put(acc, key, %{
            entry
            | state: :failed,
              validation: validation,
              retirement: new_retirement()
          })
        end)

      commit_with_epoch(%{state | entries: next_entries}, :ok, state)
    else
      true -> {:reply, {:error, :query_index_build_not_found}, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call({:activate_build, build_id}, _from, state) do
    candidates = entries_for_build(state, build_id)

    with false <- candidates == [],
         true <-
           Enum.all?(candidates, fn entry ->
             entry.state == :validating and validation_passed?(entry.validation)
           end) do
      candidate_keys =
        MapSet.new(candidates, &{&1.definition.id, &1.definition.version})

      candidate_ids = MapSet.new(candidates, & &1.definition.id)

      entries =
        Map.new(state.entries, fn {key, entry} ->
          cond do
            MapSet.member?(candidate_keys, key) ->
              {key, %{entry | state: :active, retirement: nil}}

            entry.state == :active and MapSet.member?(candidate_ids, entry.definition.id) ->
              {key, %{entry | state: :retiring, retirement: new_retirement()}}

            true ->
              {key, entry}
          end
        end)

      commit_with_epoch(%{state | entries: entries}, :ok, state)
    else
      true -> {:reply, {:error, :query_index_build_not_found}, state}
      false -> {:reply, {:error, :query_index_not_validated}, state}
    end
  end

  def handle_call(
        {:checkpoint_retirement, id, version, shard_index, progress},
        _from,
        state
      ) do
    case prepare_retirement_checkpoint(state, id, version, shard_index, progress) do
      {:ok, next_state, checkpoint} ->
        event = {:retirement, id, version, shard_index, checkpoint}
        commit_progress(next_state, event, :ok, state)

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(
        {:complete_retirement_shard, id, version, shard_index, progress},
        _from,
        state
      ) do
    with {:ok, entry} <- fetch_entry(state, id, version),
         true <- entry.state in [:retiring, :failed],
         :ok <- validate_shard(state, shard_index),
         {:ok, checkpoint} <- complete_retirement_checkpoint(entry, shard_index, progress) do
      entry = put_in(entry.retirement.checkpoints[shard_index], checkpoint)
      retirement = entry.retirement

      if all_retirement_shards_complete?(retirement, state.instance_ctx.shard_count) do
        case entry.state do
          :retiring ->
            entries = Map.delete(state.entries, {id, version})
            commit_with_epoch(%{state | entries: entries}, {:ok, :complete}, state)

          :failed ->
            key = {id, version}

            if MapSet.member?(state.catalog_keys, key) do
              retirement = %{retirement | status: :complete}
              commit_entry(state, key, %{entry | retirement: retirement}, {:ok, :complete})
            else
              entries = Map.delete(state.entries, key)
              commit_with_epoch(%{state | entries: entries}, {:ok, :complete}, state)
            end
        end
      else
        commit_entry(state, {id, version}, entry, {:ok, :pending})
      end
    else
      false -> {:reply, {:error, :query_index_not_retiring}, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  defp validate_context(%{name: name, data_dir: data_dir, shard_count: shard_count})
       when is_atom(name) and is_binary(data_dir) and data_dir != "" and is_integer(shard_count) and
              shard_count > 0,
       do: :ok

  defp validate_context(_ctx), do: {:error, :invalid_query_index_registry_context}

  defp create_cache(ctx) do
    table = cache_table(ctx)

    try do
      {:ok,
       :ets.new(table, [
         :named_table,
         :set,
         :protected,
         read_concurrency: true
       ])}
    rescue
      ArgumentError -> {:error, :query_index_registry_cache_exists}
    end
  end

  defp load_registry(ctx, catalog, metadata_contract) do
    path = snapshot_path(ctx)

    case File.lstat(path) do
      {:ok, _stat} -> load_registry_snapshot(path, ctx, catalog, metadata_contract)
      {:error, :enoent} -> new_registry(catalog, metadata_contract)
      {:error, reason} -> {:error, {:query_index_registry_read_failed, reason}}
    end
  end

  defp load_registry_snapshot(path, ctx, catalog, metadata_contract) do
    with {:ok, encoded} <- Ferricstore.FS.read_nofollow(path, @max_snapshot_bytes),
         {:ok, persisted} <- decode_snapshot(encoded),
         :ok <- validate_metadata_contract(persisted.metadata_contract, metadata_contract),
         {:ok, persisted} <- replay_journal(Map.put(persisted, :instance_ctx, ctx)),
         {:ok, reconciled, changed?} <-
           reconcile_catalog(persisted, catalog, ctx.shard_count, metadata_contract) do
      {:ok, Map.put(reconciled, :catalog_keys, catalog_keys(catalog)), changed?}
    else
      {:error, {kind, _message} = reason}
      when kind in [
             :not_found,
             :already_exists,
             :permission_denied,
             :not_a_directory,
             :is_a_directory,
             :directory_not_empty,
             :invalid_path,
             :symlink,
             :too_large,
             :other
           ] ->
        {:error, {:query_index_registry_read_failed, reason}}

      {:error, _reason} = error ->
        error
    end
  end

  defp new_registry(catalog, metadata_contract) do
    build_id = new_build_id(catalog.digest)

    entries =
      Map.new(catalog.definitions, fn definition ->
        key = {definition.id, definition.version}
        {key, new_entry(definition, build_id)}
      end)

    {:ok,
     %{
       epoch: 1,
       metadata_contract: metadata_contract,
       catalog_version: catalog.version,
       catalog_digest: catalog.digest,
       catalog_keys: catalog_keys(catalog),
       entries: entries
     }, true}
  end

  defp reconcile_catalog(persisted, catalog, shard_count, metadata_contract) do
    with :ok <- validate_catalog_revision(persisted, catalog),
         :ok <- validate_persisted_shards(persisted.entries, shard_count),
         :ok <- validate_persisted_lifecycle_progress(persisted.entries, shard_count) do
      catalog_build_id = new_build_id(catalog.digest)

      catalog_entries =
        Enum.reduce_while(catalog.definitions, {:ok, %{}}, fn definition, {:ok, acc} ->
          key = {definition.id, definition.version}

          case Map.fetch(persisted.entries, key) do
            {:ok, %{definition: old_definition} = entry} ->
              if old_definition.fingerprint == definition.fingerprint do
                {:cont, {:ok, Map.put(acc, key, %{entry | definition: definition})}}
              else
                {:halt, {:error, :query_index_definition_changed_without_version}}
              end

            :error ->
              {:cont, {:ok, Map.put(acc, key, new_entry(definition, catalog_build_id))}}
          end
        end)

      with {:ok, catalog_entries} <- catalog_entries,
           removed_entries <- Map.drop(persisted.entries, Map.keys(catalog_entries)),
           replacement_ids <- MapSet.new(catalog.definitions, & &1.id),
           {:ok, removed} <- reconcile_removed_entries(removed_entries, replacement_ids) do
        entries = Map.merge(removed, catalog_entries)

        changed? =
          catalog.digest != persisted.catalog_digest or entries != persisted.entries or
            catalog.version != persisted.catalog_version

        if map_size(entries) <= @max_registry_entries do
          with {:ok, epoch} <- reconciled_epoch(persisted.epoch, changed?) do
            {:ok,
             %{
               epoch: epoch,
               metadata_contract: metadata_contract,
               catalog_version: catalog.version,
               catalog_digest: catalog.digest,
               entries: entries
             }, changed?}
          end
        else
          {:error, :query_index_registry_capacity_exceeded}
        end
      end
    end
  end

  defp validate_metadata_contract(contract, contract), do: :ok

  defp validate_metadata_contract(_persisted, _configured),
    do: {:error, :query_index_metadata_schema_mismatch}

  defp validate_catalog_revision(persisted, catalog) do
    cond do
      catalog.version < persisted.catalog_version ->
        {:error, :query_index_catalog_version_regressed}

      catalog.version == persisted.catalog_version and
          catalog.digest != persisted.catalog_digest ->
        {:error, :query_index_catalog_changed_without_version}

      true ->
        :ok
    end
  end

  defp catalog_keys(catalog) do
    MapSet.new(catalog.definitions, &{&1.id, &1.version})
  end

  defp removed_state(%{state: :active, definition: definition}, replacement_ids) do
    if MapSet.member?(replacement_ids, definition.id), do: :active, else: :retiring
  end

  defp removed_state(%{state: state}, _replacement_ids) when state in [:active, :retiring],
    do: :retiring

  defp removed_state(_entry, _replacement_ids), do: :failed

  defp reconcile_removed_entries(entries, replacement_ids) do
    entries
    |> Enum.reduce_while({:ok, %{}}, fn {key, entry}, {:ok, acc} ->
      next_state = removed_state(entry, replacement_ids)

      retirement =
        if next_state in [:retiring, :failed],
          do: entry.retirement || new_retirement(),
          else: entry.retirement

      case catalog_reconciled_validation(entry, next_state) do
        {:ok, validation} ->
          entry = %{entry | state: next_state, validation: validation, retirement: retirement}

          if entry.state == :failed and match?(%{status: :complete}, entry.retirement),
            do: {:cont, {:ok, acc}},
            else: {:cont, {:ok, Map.put(acc, key, entry)}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp catalog_reconciled_validation(%{validation: %{status: status} = validation}, :failed)
       when status in [:passed, :failed],
       do: {:ok, validation}

  defp catalog_reconciled_validation(entry, :failed) do
    checkpoints = validation_checkpoints(entry)

    with {:ok, checked_records, checked_entries} <- sum_validation_checkpoints(checkpoints),
         validated_at_ms <- System.system_time(:millisecond),
         true <- nonnegative_u64?(validated_at_ms) do
      {:ok,
       %{
         status: :failed,
         checkpoints: checkpoints,
         checked_records: checked_records,
         checked_entries: checked_entries,
         mismatches: 1,
         reason: :catalog_removed,
         validated_at_ms: validated_at_ms
       }}
    else
      false -> {:error, :query_index_validation_clock_out_of_range}
      {:error, _reason} = error -> error
    end
  end

  defp catalog_reconciled_validation(entry, _state), do: {:ok, entry.validation}

  defp new_entry(definition, build_id) do
    %{
      definition: definition,
      state: :building,
      build_id: build_id,
      checkpoints: %{},
      validation: nil,
      retirement: nil
    }
  end

  defp new_build_id(seed) do
    Base.url_encode64(
      :crypto.hash(
        :sha256,
        <<seed::binary, System.system_time(:nanosecond)::signed-big-64,
          :erlang.unique_integer([:positive, :monotonic])::unsigned-big-64>>
      ),
      padding: false
    )
  end

  defp fetch_entry(state, id, version)
       when is_binary(id) and is_integer(version) and version > 0 do
    case Map.fetch(state.entries, {id, version}) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, :query_index_not_found}
    end
  end

  defp fetch_entry(_state, _id, _version), do: {:error, :query_index_not_found}

  defp entries_for_build(state, build_id, required_state \\ nil)

  defp entries_for_build(state, build_id, required_state)
       when is_binary(build_id) and build_id != "" do
    state.entries
    |> Map.values()
    |> Enum.filter(fn entry ->
      entry.build_id == build_id and (is_nil(required_state) or entry.state == required_state)
    end)
    |> Enum.sort_by(fn entry -> {entry.definition.id, entry.definition.version} end)
  end

  defp entries_for_build(_state, _build_id, _required_state), do: []

  defp validate_shard(%{instance_ctx: %{shard_count: shard_count}}, shard_index)
       when is_integer(shard_index) and shard_index >= 0 and shard_index < shard_count,
       do: :ok

  defp validate_shard(_state, _shard_index), do: {:error, :invalid_query_index_shard}

  defp prepare_build_checkpoint(state, build_id, shard_index, progress) do
    entries = entries_for_build(state, build_id, :building)

    with false <- entries == [],
         :ok <- validate_shard(state, shard_index),
         {:ok, checkpoint} <- normalize_checkpoint(progress),
         :ok <- ensure_checkpoint_phase(checkpoint),
         :ok <- validate_group_checkpoint(entries, shard_index, checkpoint) do
      next_entries =
        Enum.reduce(entries, state.entries, fn entry, acc ->
          key = {entry.definition.id, entry.definition.version}
          Map.put(acc, key, put_in(entry.checkpoints[shard_index], checkpoint))
        end)

      {:ok, %{state | entries: next_entries}, checkpoint}
    else
      true -> {:error, :query_index_build_not_found}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_validation_checkpoint(state, build_id, shard_index, progress) do
    entries = entries_for_build(state, build_id, :validating)

    with false <- entries == [],
         :ok <- validate_shard(state, shard_index),
         {:ok, checkpoint} <- normalize_validation_checkpoint(progress),
         :ok <- ensure_validation_checkpoint_phase(checkpoint),
         :ok <- validate_group_validation_checkpoint(entries, shard_index, checkpoint) do
      next_entries =
        Enum.reduce(entries, state.entries, fn entry, acc ->
          key = {entry.definition.id, entry.definition.version}
          Map.put(acc, key, put_in(entry.validation.checkpoints[shard_index], checkpoint))
        end)

      {:ok, %{state | entries: next_entries}, checkpoint}
    else
      true -> {:error, :query_index_build_not_found}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_retirement_checkpoint(state, id, version, shard_index, progress) do
    with {:ok, entry} <- fetch_entry(state, id, version),
         true <- entry.state in [:retiring, :failed],
         :ok <- validate_shard(state, shard_index),
         {:ok, checkpoint} <- normalize_retirement_checkpoint(progress),
         :ok <- ensure_retirement_checkpoint_phase(checkpoint),
         old <- retirement_checkpoint(entry, shard_index),
         :ok <- ensure_monotonic_retirement_checkpoint(old, checkpoint) do
      entry = put_in(entry.retirement.checkpoints[shard_index], checkpoint)
      key = {id, version}
      {:ok, %{state | entries: Map.put(state.entries, key, entry)}, checkpoint}
    else
      false -> {:error, :query_index_not_retiring}
      {:error, _reason} = error -> error
    end
  end

  defp replay_journal(state) do
    with {:ok, events} <- IndexRegistryJournal.read(state.instance_ctx) do
      replay_journal_events(events, state)
    end
  end

  defp replay_journal_events(events, state) do
    Enum.reduce_while(events, {:ok, state}, fn event, {:ok, current} ->
      case apply_journal_event(current, event) do
        {:ok, next} ->
          {:cont, {:ok, next}}

        {:error, reason} ->
          {:halt, {:error, {:invalid_query_index_registry_journal, reason}}}
      end
    end)
  end

  defp apply_journal_event(state, {:build, build_id, shard_index, checkpoint}) do
    with true <- valid_journal_id?(build_id, 128),
         :ok <- validate_shard(state, shard_index),
         {:ok, progress} <- journal_progress(checkpoint),
         {:ok, checkpoint} <- normalize_checkpoint(progress),
         :ok <- ensure_checkpoint_phase(checkpoint) do
      replay_build_checkpoint(state, build_id, shard_index, checkpoint)
    else
      false -> {:error, :invalid_build_id}
      {:error, _reason} = error -> error
    end
  end

  defp apply_journal_event(state, {:validation, build_id, shard_index, checkpoint}) do
    with true <- valid_journal_id?(build_id, 128),
         :ok <- validate_shard(state, shard_index),
         {:ok, progress} <- journal_progress(checkpoint),
         {:ok, checkpoint} <- normalize_validation_checkpoint(progress),
         :ok <- ensure_validation_checkpoint_phase(checkpoint) do
      replay_validation_checkpoint(state, build_id, shard_index, checkpoint)
    else
      false -> {:error, :invalid_build_id}
      {:error, _reason} = error -> error
    end
  end

  defp apply_journal_event(
         state,
         {:retirement, id, version, shard_index, checkpoint}
       ) do
    with true <- valid_journal_id?(id, 64),
         true <- is_integer(version) and version > 0 and version <= @max_u64,
         :ok <- validate_shard(state, shard_index),
         {:ok, progress} <- journal_progress(checkpoint),
         {:ok, checkpoint} <- normalize_retirement_checkpoint(progress),
         :ok <- ensure_retirement_checkpoint_phase(checkpoint) do
      replay_retirement_checkpoint(state, id, version, shard_index, checkpoint)
    else
      false -> {:error, :invalid_index_identity}
      {:error, _reason} = error -> error
    end
  end

  defp apply_journal_event(_state, _event), do: {:error, :invalid_event}

  defp replay_build_checkpoint(state, build_id, shard_index, checkpoint) do
    entries = entries_for_build(state, build_id, :building)

    replay_group_checkpoint(
      state,
      entries,
      fn entry -> Map.get(entry.checkpoints, shard_index, empty_checkpoint()) end,
      checkpoint,
      fn -> prepare_build_checkpoint(state, build_id, shard_index, Map.to_list(checkpoint)) end,
      &ensure_monotonic_checkpoint/2
    )
  end

  defp replay_validation_checkpoint(state, build_id, shard_index, checkpoint) do
    entries = entries_for_build(state, build_id, :validating)
    definition_count = length(entries)

    replay_group_checkpoint(
      state,
      entries,
      fn entry ->
        Map.get(entry.validation.checkpoints, shard_index, empty_validation_checkpoint())
      end,
      checkpoint,
      fn ->
        prepare_validation_checkpoint(state, build_id, shard_index, Map.to_list(checkpoint))
      end,
      fn old, next -> ensure_monotonic_validation_checkpoint(old, next, definition_count) end
    )
  end

  defp replay_group_checkpoint(
         state,
         [],
         _current_fun,
         _checkpoint,
         _prepare_fun,
         _monotonic_fun
       ),
       do: {:ok, state}

  defp replay_group_checkpoint(
         state,
         entries,
         current_fun,
         checkpoint,
         prepare_fun,
         monotonic_fun
       ) do
    case entries |> Enum.map(current_fun) |> Enum.uniq() do
      [^checkpoint] ->
        {:ok, state}

      [current] ->
        case prepare_fun.() do
          {:ok, next_state, ^checkpoint} ->
            {:ok, next_state}

          {:error, _forward_reason} ->
            case monotonic_fun.(checkpoint, current) do
              :ok -> {:ok, state}
              {:error, _reason} -> {:error, :non_monotonic_checkpoint}
            end
        end

      _divergent ->
        {:error, :inconsistent_build_checkpoint}
    end
  end

  defp replay_retirement_checkpoint(state, id, version, shard_index, checkpoint) do
    case fetch_entry(state, id, version) do
      {:ok, %{state: lifecycle_state} = entry} when lifecycle_state in [:retiring, :failed] ->
        current = retirement_checkpoint(entry, shard_index)

        cond do
          current == checkpoint ->
            {:ok, state}

          true ->
            case prepare_retirement_checkpoint(
                   state,
                   id,
                   version,
                   shard_index,
                   Map.to_list(checkpoint)
                 ) do
              {:ok, next_state, ^checkpoint} ->
                {:ok, next_state}

              {:error, _forward_reason} ->
                case ensure_monotonic_retirement_checkpoint(checkpoint, current) do
                  :ok -> {:ok, state}
                  {:error, _reason} -> {:error, :non_monotonic_retirement_checkpoint}
                end
            end
        end

      {:ok, _advanced_entry} ->
        {:ok, state}

      {:error, :query_index_not_found} ->
        {:ok, state}
    end
  end

  defp journal_progress(progress) when is_map(progress), do: {:ok, Map.to_list(progress)}
  defp journal_progress(_progress), do: {:error, :invalid_progress}

  defp valid_journal_id?(value, max_bytes),
    do: is_binary(value) and value != "" and byte_size(value) <= max_bytes

  defp normalize_checkpoint(progress) when is_list(progress) do
    phase = Keyword.get(progress, :phase)
    cursor = Keyword.get(progress, :cursor, "")
    fenced = Keyword.get(progress, :fenced, false)
    scanned_records = Keyword.get(progress, :scanned_records, 0)
    written_entries = Keyword.get(progress, :written_entries, 0)
    written_bytes = Keyword.get(progress, :written_bytes, 0)

    if phase in @phases and is_binary(cursor) and byte_size(cursor) <= @max_cursor_bytes and
         is_boolean(fenced) and nonnegative_u64?(scanned_records) and
         nonnegative_u64?(written_entries) and nonnegative_u64?(written_bytes) do
      {:ok,
       %{
         phase: phase,
         cursor: cursor,
         fenced: fenced,
         scanned_records: scanned_records,
         written_entries: written_entries,
         written_bytes: written_bytes
       }}
    else
      {:error, :invalid_query_index_checkpoint}
    end
  end

  defp normalize_checkpoint(_progress), do: {:error, :invalid_query_index_checkpoint}

  defp empty_checkpoint do
    %{
      phase: :snapshot,
      cursor: "",
      fenced: false,
      scanned_records: 0,
      written_entries: 0,
      written_bytes: 0
    }
  end

  defp ensure_monotonic_checkpoint(old, next) do
    cond do
      not valid_build_fence_transition?(old, next) ->
        {:error, :query_index_build_not_fenced}

      phase_rank(next.phase) >= phase_rank(old.phase) and
        (not old.fenced or next.fenced) and
        next.scanned_records >= old.scanned_records and
        next.written_entries >= old.written_entries and
        next.written_bytes >= old.written_bytes and cursor_monotonic?(old, next) ->
        :ok

      true ->
        {:error, :non_monotonic_query_index_checkpoint}
    end
  end

  defp valid_build_fence_transition?(%{fenced: true}, %{fenced: true}), do: true

  defp valid_build_fence_transition?(
         %{fenced: false} = old,
         %{phase: :snapshot, cursor: "", fenced: true} = next
       ),
       do: checkpoint_counters_equal?(old, next)

  defp valid_build_fence_transition?(_old, _next), do: false

  defp ensure_checkpoint_phase(%{phase: :done}),
    do: {:error, :invalid_query_index_checkpoint_transition}

  defp ensure_checkpoint_phase(%{phase: :backfill, fenced: false}),
    do: {:error, :query_index_build_not_fenced}

  defp ensure_checkpoint_phase(_checkpoint), do: :ok

  defp cursor_monotonic?(
         %{phase: :backfill, cursor: old_cursor} = old,
         %{
           phase: :backfill,
           cursor: next_cursor
         } = next
       )
       when old_cursor != "" do
    next_cursor > old_cursor or
      (next_cursor == old_cursor and checkpoint_counters_equal?(old, next))
  end

  defp cursor_monotonic?(_old, _next), do: true

  defp checkpoint_counters_equal?(old, next) do
    old.scanned_records == next.scanned_records and
      old.written_entries == next.written_entries and old.written_bytes == next.written_bytes
  end

  defp validate_group_checkpoint(entries, shard_index, checkpoint) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      old = Map.get(entry.checkpoints, shard_index, empty_checkpoint())

      case ensure_monotonic_checkpoint(old, checkpoint) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp complete_group_checkpoints(entries, shard_index, []) do
    with :ok <- ensure_group_backfill_phase(entries, shard_index) do
      {:ok,
       Map.new(entries, fn entry ->
         key = {entry.definition.id, entry.definition.version}
         old = Map.fetch!(entry.checkpoints, shard_index)
         {key, %{old | phase: :done, cursor: ""}}
       end)}
    end
  end

  defp complete_group_checkpoints(entries, shard_index, progress) do
    progress =
      progress
      |> Keyword.put(:phase, :done)
      |> Keyword.put(:cursor, "")

    with :ok <- ensure_group_backfill_phase(entries, shard_index),
         {:ok, checkpoint} <- normalize_checkpoint(progress),
         :ok <- validate_group_checkpoint(entries, shard_index, checkpoint) do
      {:ok,
       Map.new(entries, fn entry ->
         {{entry.definition.id, entry.definition.version}, checkpoint}
       end)}
    end
  end

  defp ensure_group_backfill_phase(entries, shard_index) do
    if Enum.all?(entries, fn entry ->
         match?(%{phase: :backfill}, Map.get(entry.checkpoints, shard_index))
       end),
       do: :ok,
       else: {:error, :query_index_backfill_not_complete}
  end

  defp phase_rank(:snapshot), do: 0
  defp phase_rank(:backfill), do: 1
  defp phase_rank(:done), do: 2

  defp all_shards_complete?(entry, shard_count) do
    Enum.all?(0..(shard_count - 1), fn shard_index ->
      match?(%{phase: :done}, Map.get(entry.checkpoints, shard_index))
    end)
  end

  defp new_validation do
    %{
      status: :pending,
      checkpoints: %{},
      checked_records: 0,
      checked_entries: 0,
      mismatches: 0,
      validated_at_ms: nil
    }
  end

  defp validation_checkpoints(%{validation: %{checkpoints: checkpoints}}), do: checkpoints
  defp validation_checkpoints(_entry), do: %{}

  defp normalize_validation_checkpoint(progress) when is_list(progress) do
    phase = Keyword.get(progress, :phase)
    cursor = Keyword.get(progress, :cursor, "")
    fenced = Keyword.get(progress, :fenced, false)
    definition_position = Keyword.get(progress, :definition_position, 0)
    checked_records = Keyword.get(progress, :checked_records, 0)
    checked_entries = Keyword.get(progress, :checked_entries, 0)
    mismatches = Keyword.get(progress, :mismatches, 0)
    counter_runs = Keyword.get(progress, :counter_runs, [])

    if phase in @validation_phases and phase != :done and is_binary(cursor) and
         byte_size(cursor) <= @max_cursor_bytes and is_integer(definition_position) and
         definition_position >= 0 and definition_position <= 16 and is_boolean(fenced) and
         nonnegative_u64?(checked_records) and nonnegative_u64?(checked_entries) and
         mismatches == 0 and valid_counter_run_shape?(counter_runs) do
      {:ok,
       %{
         phase: phase,
         cursor: cursor,
         fenced: fenced,
         definition_position: definition_position,
         checked_records: checked_records,
         checked_entries: checked_entries,
         mismatches: 0,
         counter_runs: counter_runs
       }}
    else
      {:error, :invalid_query_index_validation_checkpoint}
    end
  end

  defp normalize_validation_checkpoint(_progress),
    do: {:error, :invalid_query_index_validation_checkpoint}

  defp empty_validation_checkpoint do
    %{
      phase: :source,
      cursor: "",
      fenced: false,
      definition_position: 0,
      checked_records: 0,
      checked_entries: 0,
      mismatches: 0,
      counter_runs: []
    }
  end

  defp ensure_validation_checkpoint_phase(%{phase: :done}),
    do: {:error, :invalid_query_index_validation_checkpoint_transition}

  defp ensure_validation_checkpoint_phase(%{phase: phase, fenced: false})
       when phase != :source,
       do: {:error, :query_index_validation_not_fenced}

  defp ensure_validation_checkpoint_phase(_checkpoint), do: :ok

  defp validate_group_validation_checkpoint(entries, shard_index, checkpoint) do
    definitions =
      entries
      |> Enum.map(& &1.definition)
      |> Enum.sort_by(&{&1.id, &1.version})

    definition_count = length(definitions)

    with :ok <- validate_validation_checkpoint_position(checkpoint, definitions) do
      Enum.reduce_while(entries, :ok, fn entry, :ok ->
        old =
          entry.validation.checkpoints
          |> Map.get(shard_index, empty_validation_checkpoint())

        case ensure_monotonic_validation_checkpoint(old, checkpoint, definition_count) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp ensure_group_validation_restartable(entries, shard_index) do
    checkpoints =
      entries
      |> Enum.map(fn entry ->
        entry.validation.checkpoints
        |> Map.get(shard_index, empty_validation_checkpoint())
      end)
      |> Enum.uniq()

    case checkpoints do
      [%{phase: :index, fenced: true}] -> :ok
      _other -> {:error, :query_index_validation_not_restartable}
    end
  end

  defp ensure_monotonic_validation_checkpoint(old, next, definition_count) do
    cond do
      invalid_validation_cursor_transition?(old, next) ->
        {:error, :invalid_query_index_validation_checkpoint}

      not valid_validation_fence_transition?(old, next) ->
        {:error, :query_index_validation_not_fenced}

      validation_phase_rank(next.phase) >= validation_phase_rank(old.phase) and
        (not old.fenced or next.fenced) and
        validation_definition_position_monotonic?(old, next) and
        next.checked_records >= old.checked_records and
        next.checked_entries >= old.checked_entries and next.mismatches == 0 and
        valid_validation_step?(old, next, definition_count) and
          validation_cursor_monotonic?(old, next) ->
        :ok

      true ->
        {:error, :non_monotonic_query_index_validation_checkpoint}
    end
  end

  defp invalid_validation_cursor_transition?(
         %{phase: phase, definition_position: old_position},
         %{phase: phase, definition_position: next_position, cursor: cursor}
       )
       when phase in [:index, :counter] and next_position != old_position,
       do: cursor != ""

  defp invalid_validation_cursor_transition?(_old, _next), do: false

  defp valid_validation_fence_transition?(%{fenced: true}, %{fenced: true}), do: true

  defp valid_validation_fence_transition?(
         %{fenced: false} = old,
         %{phase: :source, cursor: "", fenced: true, definition_position: 0} = next
       ),
       do: validation_checkpoint_counters_equal?(old, next)

  defp valid_validation_fence_transition?(_old, _next), do: false

  defp validation_cursor_monotonic?(
         %{phase: phase, definition_position: position, cursor: old_cursor} = old,
         %{phase: phase, definition_position: position, cursor: next_cursor} = next
       ) do
    next_cursor > old_cursor or
      (next_cursor == old_cursor and validation_checkpoint_counters_equal?(old, next))
  end

  defp validation_cursor_monotonic?(_old, _next), do: true

  defp validation_definition_position_monotonic?(
         %{phase: phase, definition_position: old_position},
         %{phase: phase, definition_position: next_position}
       ),
       do: next_position >= old_position

  defp validation_definition_position_monotonic?(
         %{phase: :index},
         %{phase: :counter, definition_position: 0}
       ),
       do: true

  defp validation_definition_position_monotonic?(_old, _next), do: true

  defp valid_validation_step?(%{fenced: false}, %{phase: :source}, _definition_count),
    do: true

  defp valid_validation_step?(
         %{phase: phase, definition_position: position},
         %{phase: phase, definition_position: position},
         _definition_count
       )
       when phase in [:source, :index, :counter],
       do: true

  defp valid_validation_step?(
         %{phase: :index, definition_position: position},
         %{phase: :index, definition_position: next_position, cursor: ""},
         _definition_count
       ),
       do: next_position == position + 1

  defp valid_validation_step?(
         %{phase: :counter, definition_position: position},
         %{phase: :counter, definition_position: next_position, cursor: ""},
         _definition_count
       ),
       do: next_position == position + 1

  defp valid_validation_step?(%{phase: phase} = old, %{phase: phase} = next, _definition_count)
       when phase in [:cleanup, :done],
       do: validation_checkpoint_counters_equal?(old, next)

  defp valid_validation_step?(
         %{phase: :source, definition_position: 0},
         %{phase: :index, definition_position: 0, cursor: ""},
         _definition_count
       ),
       do: true

  defp valid_validation_step?(
         %{phase: :index, definition_position: position},
         %{phase: :counter, definition_position: 0, cursor: "", counter_runs: []},
         definition_count
       ),
       do: position + 1 == definition_count

  defp valid_validation_step?(
         %{phase: :counter, definition_position: position},
         %{phase: :cleanup, definition_position: definition_count, cursor: "", counter_runs: []},
         definition_count
       ),
       do: position + 1 == definition_count

  defp valid_validation_step?(
         %{phase: :cleanup, definition_position: definition_count} = old,
         %{phase: :done, definition_position: definition_count, cursor: ""} = next,
         definition_count
       ),
       do: validation_checkpoint_counters_equal?(old, next)

  defp valid_validation_step?(_old, _next, _definition_count), do: false

  defp validate_validation_checkpoint_position(checkpoint, definitions) do
    definition_count = length(definitions)

    if valid_validation_phase_position?(
         checkpoint.phase,
         checkpoint.definition_position,
         checkpoint.cursor,
         definition_count
       ) and valid_validation_counter_runs?(checkpoint, definitions),
       do: :ok,
       else: {:error, :invalid_query_index_validation_checkpoint}
  end

  defp valid_validation_phase_position?(:source, 0, _cursor, definition_count),
    do: definition_count > 0

  defp valid_validation_phase_position?(:index, position, _cursor, definition_count),
    do: position < definition_count

  defp valid_validation_phase_position?(:counter, position, _cursor, definition_count),
    do: position < definition_count

  defp valid_validation_phase_position?(phase, definition_count, "", definition_count)
       when phase in [:cleanup, :done],
       do: definition_count > 0

  defp valid_validation_phase_position?(_phase, _position, _cursor, _definition_count), do: false

  defp valid_validation_counter_runs?(%{phase: phase, counter_runs: []}, _definitions)
       when phase in [:source, :counter, :cleanup, :done],
       do: true

  defp valid_validation_counter_runs?(
         %{phase: :index, cursor: "", counter_runs: []},
         _definitions
       ),
       do: true

  defp valid_validation_counter_runs?(
         %{phase: :index, definition_position: position, counter_runs: []},
         definitions
       ) do
    Enum.at(definitions, position).count_prefixes == []
  end

  defp valid_validation_counter_runs?(
         %{phase: :index, definition_position: position, cursor: cursor, counter_runs: runs},
         definitions
       )
       when is_binary(cursor) and cursor != "" and is_list(runs) do
    definition = Enum.at(definitions, position)

    case CompositeCounter.prefixes_for_key(definition, cursor) do
      {:ok, prefixes} when length(prefixes) == length(runs) ->
        Enum.zip(runs, prefixes)
        |> Enum.all?(fn
          {%{
             prefix: prefix,
             count: count,
             expiring_count: expiring_count,
             physical_count: physical_count,
             expected_count: expected
           } = run, expected_prefix} ->
            map_size(run) == 5 and prefix == expected_prefix and nonnegative_u64?(count) and
              count > 0 and nonnegative_u64?(expiring_count) and expiring_count <= count and
              nonnegative_u64?(physical_count) and physical_count >= count and
              nonnegative_u64?(expected) and count <= expected

          _invalid ->
            false
        end)

      _invalid ->
        false
    end
  end

  defp valid_validation_counter_runs?(_checkpoint, _definitions), do: false

  defp valid_counter_run_shape?(runs) when is_list(runs) and length(runs) <= 8 do
    Enum.all?(runs, fn
      %{
        prefix: prefix,
        count: count,
        expiring_count: expiring_count,
        physical_count: physical_count,
        expected_count: expected
      } = run ->
        map_size(run) == 5 and is_binary(prefix) and prefix != "" and
          byte_size(prefix) <= @max_cursor_bytes and nonnegative_u64?(count) and count > 0 and
          nonnegative_u64?(expiring_count) and expiring_count <= count and
          nonnegative_u64?(physical_count) and physical_count >= count and
          nonnegative_u64?(expected) and count <= expected

      _invalid ->
        false
    end)
  end

  defp valid_counter_run_shape?(_runs), do: false

  defp validation_checkpoint_counters_equal?(old, next) do
    old.checked_records == next.checked_records and old.checked_entries == next.checked_entries and
      old.mismatches == next.mismatches and old.counter_runs == next.counter_runs
  end

  defp complete_group_validation_checkpoints(entries, shard_index, []) do
    with :ok <- ensure_group_validation_cleanup_phase(entries, shard_index) do
      {:ok,
       Map.new(entries, fn entry ->
         key = {entry.definition.id, entry.definition.version}
         old = Map.fetch!(entry.validation.checkpoints, shard_index)
         {key, %{old | phase: :done, cursor: ""}}
       end)}
    end
  end

  defp complete_group_validation_checkpoints(entries, shard_index, progress) do
    progress = Keyword.put(progress, :phase, :done)

    with :ok <- ensure_group_validation_cleanup_phase(entries, shard_index),
         {:ok, checkpoint} <- normalize_completed_validation_checkpoint(progress),
         :ok <- validate_group_validation_checkpoint(entries, shard_index, checkpoint) do
      {:ok,
       Map.new(entries, fn entry ->
         {{entry.definition.id, entry.definition.version}, checkpoint}
       end)}
    end
  end

  defp normalize_completed_validation_checkpoint(progress) do
    case normalize_validation_checkpoint(Keyword.put(progress, :phase, :cleanup)) do
      {:ok, checkpoint} -> {:ok, %{checkpoint | phase: :done, cursor: ""}}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_group_validation_cleanup_phase(entries, shard_index) do
    if Enum.all?(entries, fn entry ->
         match?(%{phase: :cleanup}, Map.get(entry.validation.checkpoints, shard_index))
       end),
       do: :ok,
       else: {:error, :query_index_validation_not_complete}
  end

  defp all_validation_shards_complete?(validation, shard_count) do
    Enum.all?(0..(shard_count - 1), fn shard_index ->
      match?(%{phase: :done}, Map.get(validation.checkpoints, shard_index))
    end)
  end

  defp complete_validation_entries(
         all_entries,
         entries,
         checkpoints,
         shard_index,
         shard_count
       ) do
    Enum.reduce_while(entries, {:ok, all_entries}, fn entry, {:ok, acc} ->
      key = {entry.definition.id, entry.definition.version}
      checkpoint = Map.fetch!(checkpoints, key)
      entry = put_in(entry.validation.checkpoints[shard_index], checkpoint)

      result =
        if all_validation_shards_complete?(entry.validation, shard_count),
          do: complete_validation(entry.validation),
          else: {:ok, entry.validation}

      case result do
        {:ok, validation} -> {:cont, {:ok, Map.put(acc, key, %{entry | validation: validation})}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp complete_validation(validation) do
    with {:ok, checked_records, checked_entries} <-
           sum_validation_checkpoints(validation.checkpoints),
         validated_at_ms <- System.system_time(:millisecond),
         true <- nonnegative_u64?(validated_at_ms) do
      {:ok,
       %{
         validation
         | status: :passed,
           checked_records: checked_records,
           checked_entries: checked_entries,
           mismatches: 0,
           validated_at_ms: validated_at_ms
       }}
    else
      false -> {:error, :query_index_validation_clock_out_of_range}
      {:error, _reason} = error -> error
    end
  end

  defp sum_validation_checkpoints(checkpoints) do
    Enum.reduce_while(checkpoints, {:ok, 0, 0}, fn
      {_shard, checkpoint}, {:ok, records, entries} ->
        with {:ok, records} <- add_validation_counter(records, checkpoint.checked_records),
             {:ok, entries} <- add_validation_counter(entries, checkpoint.checked_entries) do
          {:cont, {:ok, records, entries}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
    end)
  end

  defp add_validation_counter(left, right)
       when is_integer(left) and left >= 0 and left <= @max_u64 and is_integer(right) and
              right >= 0 and right <= @max_u64 and left <= @max_u64 - right,
       do: {:ok, left + right}

  defp add_validation_counter(_left, _right),
    do: {:error, :query_index_validation_counter_overflow}

  defp normalize_failed_validation(evidence) when is_list(evidence) do
    checked_records = Keyword.get(evidence, :checked_records)
    checked_entries = Keyword.get(evidence, :checked_entries)
    mismatches = Keyword.get(evidence, :mismatches)
    reason = Keyword.get(evidence, :reason)

    if nonnegative_u64?(checked_records) and nonnegative_u64?(checked_entries) and
         is_integer(mismatches) and mismatches > 0 and mismatches <= @max_u64 and is_atom(reason) do
      {:ok,
       %{
         status: :failed,
         checkpoints: %{},
         checked_records: checked_records,
         checked_entries: checked_entries,
         mismatches: mismatches,
         reason: reason,
         validated_at_ms: System.system_time(:millisecond)
       }}
    else
      {:error, :query_index_validation_failed}
    end
  end

  defp normalize_failed_validation(_evidence), do: {:error, :query_index_validation_failed}

  defp validation_passed?(%{status: :passed, mismatches: 0}), do: true
  defp validation_passed?(_validation), do: false

  defp validation_phase_rank(:source), do: 0
  defp validation_phase_rank(:index), do: 1
  defp validation_phase_rank(:counter), do: 2
  defp validation_phase_rank(:cleanup), do: 3
  defp validation_phase_rank(:done), do: 4

  defp new_retirement do
    %{status: :pending, checkpoints: %{}}
  end

  defp retirement_checkpoint(%{retirement: %{checkpoints: checkpoints}}, shard_index),
    do: Map.get(checkpoints, shard_index, empty_retirement_checkpoint())

  defp empty_retirement_checkpoint do
    %{
      phase: :fence,
      cursor: "",
      deleted_entries: 0,
      deleted_bytes: 0,
      rewritten_reverse_rows: 0
    }
  end

  defp normalize_retirement_checkpoint(progress) when is_list(progress) do
    phase = Keyword.get(progress, :phase)
    cursor = Keyword.get(progress, :cursor, "")
    deleted_entries = Keyword.get(progress, :deleted_entries, 0)
    deleted_bytes = Keyword.get(progress, :deleted_bytes, 0)
    rewritten_reverse_rows = Keyword.get(progress, :rewritten_reverse_rows, 0)

    checkpoint = %{
      phase: phase,
      cursor: cursor,
      deleted_entries: deleted_entries,
      deleted_bytes: deleted_bytes,
      rewritten_reverse_rows: rewritten_reverse_rows
    }

    if phase != :done and valid_retirement_checkpoint?(checkpoint) do
      {:ok, checkpoint}
    else
      {:error, :invalid_query_index_retirement_checkpoint}
    end
  end

  defp normalize_retirement_checkpoint(_progress),
    do: {:error, :invalid_query_index_retirement_checkpoint}

  defp ensure_retirement_checkpoint_phase(%{phase: :done}),
    do: {:error, :invalid_query_index_retirement_checkpoint_transition}

  defp ensure_retirement_checkpoint_phase(_checkpoint), do: :ok

  defp ensure_monotonic_retirement_checkpoint(old, next) do
    if valid_retirement_step?(old, next) and
         next.deleted_entries >= old.deleted_entries and next.deleted_bytes >= old.deleted_bytes and
         next.rewritten_reverse_rows >= old.rewritten_reverse_rows and
         retirement_cursor_monotonic?(old, next),
       do: :ok,
       else: {:error, :non_monotonic_query_index_retirement_checkpoint}
  end

  defp retirement_cursor_monotonic?(
         %{phase: phase, cursor: old_cursor} = old,
         %{phase: phase, cursor: next_cursor} = next
       ) do
    next_cursor > old_cursor or
      (next_cursor == old_cursor and retirement_checkpoint_counters_equal?(old, next))
  end

  defp retirement_cursor_monotonic?(_old, _next), do: true

  defp retirement_checkpoint_counters_equal?(old, next) do
    old.deleted_entries == next.deleted_entries and old.deleted_bytes == next.deleted_bytes and
      old.rewritten_reverse_rows == next.rewritten_reverse_rows
  end

  defp valid_retirement_step?(%{phase: phase}, %{phase: phase})
       when phase in [:fence, :index, :counter, :reverse, :cleanup],
       do: true

  defp valid_retirement_step?(%{phase: :fence} = old, %{phase: :index, cursor: ""} = next),
    do: retirement_checkpoint_counters_equal?(old, next)

  defp valid_retirement_step?(
         %{phase: :index, rewritten_reverse_rows: rewritten},
         %{phase: :reverse, cursor: "", rewritten_reverse_rows: rewritten}
       ),
       do: true

  defp valid_retirement_step?(
         %{phase: :index, rewritten_reverse_rows: rewritten},
         %{phase: :counter, cursor: "", rewritten_reverse_rows: rewritten}
       ),
       do: true

  defp valid_retirement_step?(
         %{phase: :counter, rewritten_reverse_rows: rewritten},
         %{phase: :reverse, cursor: "", rewritten_reverse_rows: rewritten}
       ),
       do: true

  defp valid_retirement_step?(
         %{phase: :reverse, deleted_entries: entries, deleted_bytes: bytes},
         %{phase: :cleanup, cursor: "", deleted_entries: entries, deleted_bytes: bytes}
       ),
       do: true

  defp valid_retirement_step?(%{phase: :cleanup} = old, %{phase: :done, cursor: ""} = next),
    do: retirement_checkpoint_counters_equal?(old, next)

  defp valid_retirement_step?(_old, _next), do: false

  defp complete_retirement_checkpoint(entry, shard_index, []) do
    old = retirement_checkpoint(entry, shard_index)

    if old.phase == :cleanup,
      do: {:ok, %{old | phase: :done, cursor: ""}},
      else: {:error, :query_index_retirement_not_complete}
  end

  defp complete_retirement_checkpoint(entry, shard_index, progress) do
    old = retirement_checkpoint(entry, shard_index)

    with true <- old.phase == :cleanup,
         {:ok, checkpoint} <- normalize_retirement_checkpoint(progress),
         checkpoint = %{checkpoint | phase: :done, cursor: ""},
         :ok <- ensure_monotonic_retirement_checkpoint(old, checkpoint) do
      {:ok, checkpoint}
    else
      false -> {:error, :query_index_retirement_not_complete}
      {:error, _reason} = error -> error
    end
  end

  defp all_retirement_shards_complete?(retirement, shard_count) do
    Enum.all?(0..(shard_count - 1), fn shard_index ->
      match?(%{phase: :done}, Map.get(retirement.checkpoints, shard_index))
    end)
  end

  defp nonnegative_u64?(value),
    do: is_integer(value) and value >= 0 and value <= @max_u64

  defp reconciled_epoch(epoch, false), do: {:ok, epoch}
  defp reconciled_epoch(epoch, true) when epoch < @max_u64, do: {:ok, epoch + 1}
  defp reconciled_epoch(_epoch, true), do: {:error, :query_index_registry_epoch_exhausted}

  defp commit_with_epoch(next_state, reply, %{epoch: epoch} = old_state)
       when epoch < @max_u64 do
    commit(%{next_state | epoch: epoch + 1}, reply, old_state)
  end

  defp commit_with_epoch(_next_state, _reply, old_state) do
    {:reply, {:error, :query_index_registry_epoch_exhausted}, old_state}
  end

  defp commit_entry(state, key, entry, reply) do
    next_state = %{state | entries: Map.put(state.entries, key, entry)}
    commit(next_state, reply, state)
  end

  defp commit_progress(next_state, event, reply, old_state) do
    case append_journal_event(next_state, event) do
      :ok ->
        publish_commit(next_state, reply)

      {:error, {:too_large, _reason}} ->
        commit(next_state, reply, old_state)

      {:error, reason} ->
        {:reply, {:error, {:query_index_registry_journal_failed, reason}}, old_state}
    end
  end

  defp commit(next_state, reply, old_state) do
    case persist(next_state) do
      :ok ->
        publish_commit(next_state, reply)

      {:error, reason} ->
        {:reply, {:error, {:query_index_registry_persist_failed, reason}}, old_state}
    end
  end

  defp publish_commit(next_state, reply) do
    case publish(next_state) do
      :ok -> {:reply, reply, next_state}
      {:error, reason} -> {:stop, reason, {:error, reason}, next_state}
    end
  end

  defp publish(state) do
    indexes =
      state.entries
      |> Map.values()
      |> Enum.sort_by(fn entry -> {entry.definition.id, entry.definition.version} end)
      |> Enum.map(fn entry ->
        RegisteredIndex.new!(entry.definition, entry.state,
          build_id: entry.build_id,
          coverage: coverage(entry, state.instance_ctx.shard_count)
        )
      end)

    snapshot =
      RegistrySnapshot.new!(%{
        epoch: state.epoch,
        catalog_version: state.catalog_version,
        indexes: indexes
      })

    active_identities =
      indexes
      |> Enum.reduce(MapSet.new(), fn
        %RegisteredIndex{
          state: :active,
          definition: %{id: id, version: version},
          build_id: build_id
        },
        identities ->
          MapSet.put(identities, {id, version, build_id})

        %RegisteredIndex{}, identities ->
          identities
      end)

    true =
      :ets.insert(state.cache_table, [
        {:active_identities, active_identities},
        {:snapshot, snapshot}
      ])

    :ok
  rescue
    _error -> {:error, :query_index_registry_publish_failed}
  end

  defp coverage(entry, shard_count) do
    complete =
      Enum.count(0..(shard_count - 1), fn shard_index ->
        match?(%{phase: :done}, Map.get(entry.checkpoints, shard_index))
      end)

    %{
      complete_shards: complete,
      total_shards: shard_count,
      validation: validation_status(entry.validation)
    }
  end

  defp validation_status(%{status: :passed}), do: :passed
  defp validation_status(%{status: :failed}), do: :failed
  defp validation_status(_validation), do: :pending

  defp public_status(entry, shard_count) do
    %{
      id: entry.definition.id,
      version: entry.definition.version,
      state: entry.state,
      build_id: entry.build_id,
      checkpoints: entry.checkpoints,
      validation: entry.validation,
      retirement: entry.retirement,
      coverage: coverage(entry, shard_count)
    }
  end

  defp maybe_persist(_state, false), do: :ok
  defp maybe_persist(state, true), do: persist(state)

  defp persist(state) do
    path = snapshot_path(state.instance_ctx)
    directory = Path.dirname(path)
    tmp_path = path <> ".tmp-#{System.unique_integer([:positive, :monotonic])}"
    encoded = encode_snapshot(state)

    with true <- byte_size(encoded) <= @max_snapshot_bytes,
         :ok <- FS.mkdir_p(directory),
         :ok <- write_synced(tmp_path, encoded),
         :ok <- FS.rename(tmp_path, path),
         :ok <- fsync_dir(directory) do
      _ = reset_journal(state)
      :ok
    else
      false ->
        {:error, :query_index_registry_snapshot_too_large}

      {:error, _reason} = error ->
        FS.rm(tmp_path)
        error
    end
  end

  defp write_synced(path, encoded) do
    case File.open(path, [:write, :binary, :exclusive], fn io ->
           with :ok <- IO.binwrite(io, encoded), do: :file.sync(io)
         end) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fsync_dir(path) do
    case NIF.v2_fsync_dir(path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_dir_failed, reason}}
      other -> {:error, {:fsync_dir_failed, other}}
    end
  end

  defp append_journal_event(state, event) do
    IndexRegistryJournal.append(state.instance_ctx, event)
  end

  defp reset_journal(state) do
    IndexRegistryJournal.reset(state.instance_ctx)
  end

  defp encode_snapshot(state) do
    entries =
      state.entries
      |> Map.values()
      |> Enum.sort_by(fn entry -> {entry.definition.id, entry.definition.version} end)
      |> Enum.map(&encode_entry/1)

    TermCodec.encode({
      @snapshot_tag,
      @snapshot_version,
      state.metadata_contract,
      state.epoch,
      state.catalog_version,
      state.catalog_digest,
      entries
    })
  end

  defp encode_entry(entry) do
    definition = entry.definition

    %{
      definition: %{
        id: definition.id,
        version: definition.version,
        source: definition.source,
        fields: definition.fields,
        workloads: definition.workloads,
        count_prefixes: definition.count_prefixes,
        scope_bytes: definition.scope_bytes,
        fingerprint: definition.fingerprint
      },
      state: entry.state,
      build_id: entry.build_id,
      checkpoints: entry.checkpoints,
      validation: entry.validation,
      retirement: entry.retirement
    }
  end

  defp decode_snapshot(encoded) when is_binary(encoded) do
    case TermCodec.decode(encoded) do
      {:ok,
       {@snapshot_tag, @snapshot_version, metadata_contract, epoch, catalog_version,
        catalog_digest, entries}}
      when is_integer(epoch) and epoch >= 0 and epoch <= @max_u64 and
             is_integer(catalog_version) and catalog_version > 0 and
             catalog_version <= @max_u64 and is_binary(catalog_digest) and
             byte_size(catalog_digest) == 32 and is_list(entries) and
             length(entries) <= @max_registry_entries ->
        with :ok <- validate_stored_metadata_contract(metadata_contract),
             {:ok, entries} <- decode_entries(entries),
             :ok <- validate_persisted_validation_groups(entries) do
          {:ok,
           %{
             epoch: epoch,
             metadata_contract: metadata_contract,
             catalog_version: catalog_version,
             catalog_digest: catalog_digest,
             entries: entries
           }}
        end

      _invalid ->
        {:error, {:invalid_query_index_registry_snapshot, :decode_failed}}
    end
  rescue
    _error -> {:error, {:invalid_query_index_registry_snapshot, :decode_failed}}
  end

  defp validate_stored_metadata_contract(%{
         mode: mode,
         generation: generation,
         schema_digest: schema_digest,
         scope_bytes: scope_bytes
       })
       when mode in [:dedicated, :shared] and is_integer(generation) and generation >= 0 and
              generation <= @max_u64 and is_binary(schema_digest) and
              byte_size(schema_digest) == 32 and is_integer(scope_bytes) and scope_bytes >= 0 and
              scope_bytes <= 256 do
    cond do
      mode == :dedicated and scope_bytes == 0 -> :ok
      mode == :shared and scope_bytes > 0 -> :ok
      true -> {:error, {:invalid_query_index_registry_snapshot, :invalid_metadata_contract}}
    end
  end

  defp validate_stored_metadata_contract(_contract),
    do: {:error, {:invalid_query_index_registry_snapshot, :invalid_metadata_contract}}

  defp decode_entries(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn encoded, {:ok, acc} ->
      case decode_entry(encoded) do
        {:ok, entry} ->
          key = {entry.definition.id, entry.definition.version}

          if Map.has_key?(acc, key),
            do: {:halt, {:error, {:invalid_query_index_registry_snapshot, :duplicate_entry}}},
            else: {:cont, {:ok, Map.put(acc, key, entry)}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp decode_entry(%{
         definition: attrs,
         state: state,
         build_id: build_id,
         checkpoints: checkpoints,
         validation: validation,
         retirement: retirement
       })
       when state in @states and is_binary(build_id) and build_id != "" and
              byte_size(build_id) <= 128 and is_map(checkpoints) do
    with {:ok, definition} <- decode_definition(attrs),
         :ok <- validate_checkpoints(checkpoints),
         :ok <- validate_stored_validation(validation),
         :ok <- validate_stored_retirement(retirement),
         :ok <- validate_entry_lifecycle(state, validation, retirement) do
      {:ok,
       %{
         definition: definition,
         state: state,
         build_id: build_id,
         checkpoints: checkpoints,
         validation: validation,
         retirement: retirement
       }}
    else
      _invalid -> {:error, {:invalid_query_index_registry_snapshot, :invalid_entry}}
    end
  end

  defp decode_entry(_entry),
    do: {:error, {:invalid_query_index_registry_snapshot, :invalid_entry}}

  defp decode_definition(%{
         id: id,
         version: version,
         source: source,
         fields: fields,
         workloads: workloads,
         count_prefixes: count_prefixes,
         scope_bytes: scope_bytes,
         fingerprint: fingerprint
       }) do
    with {:ok, definition} <-
           IndexDefinition.new(%{
             id: id,
             version: version,
             source: source,
             fields: fields,
             workloads: workloads,
             count_prefixes: count_prefixes,
             scope_bytes: scope_bytes
           }),
         true <- definition.fingerprint == fingerprint do
      {:ok, definition}
    else
      _invalid -> {:error, :invalid_definition}
    end
  end

  defp decode_definition(_attrs), do: {:error, :invalid_definition}

  defp validate_checkpoints(checkpoints) do
    if Enum.all?(checkpoints, fn {shard, checkpoint} ->
         is_integer(shard) and shard >= 0 and valid_checkpoint?(checkpoint)
       end),
       do: :ok,
       else: {:error, :invalid_checkpoint}
  end

  defp valid_checkpoint?(%{
         phase: phase,
         cursor: cursor,
         fenced: fenced,
         scanned_records: scanned_records,
         written_entries: written_entries,
         written_bytes: written_bytes
       }) do
    phase in @phases and is_binary(cursor) and byte_size(cursor) <= @max_cursor_bytes and
      (phase != :done or cursor == "") and
      is_boolean(fenced) and
      nonnegative_u64?(scanned_records) and nonnegative_u64?(written_entries) and
      nonnegative_u64?(written_bytes)
  end

  defp valid_checkpoint?(_checkpoint), do: false

  defp validate_stored_validation(nil), do: :ok

  defp validate_stored_validation(%{
         status: :pending,
         checkpoints: checkpoints,
         checked_records: 0,
         checked_entries: 0,
         mismatches: 0,
         validated_at_ms: nil
       })
       when is_map(checkpoints),
       do: validate_validation_checkpoints(checkpoints)

  defp validate_stored_validation(%{
         status: :passed,
         checkpoints: checkpoints,
         checked_records: checked_records,
         checked_entries: checked_entries,
         mismatches: 0,
         validated_at_ms: validated_at_ms
       })
       when is_map(checkpoints) and is_integer(checked_records) and checked_records >= 0 and
              checked_records <= @max_u64 and is_integer(checked_entries) and
              checked_entries >= 0 and checked_entries <= @max_u64 and
              is_integer(validated_at_ms) and validated_at_ms >= 0 and
              validated_at_ms <= @max_u64,
       do: validate_validation_checkpoints(checkpoints)

  defp validate_stored_validation(%{
         status: :failed,
         checkpoints: checkpoints,
         checked_records: checked_records,
         checked_entries: checked_entries,
         mismatches: mismatches,
         reason: reason,
         validated_at_ms: validated_at_ms
       })
       when is_map(checkpoints) and is_integer(checked_records) and checked_records >= 0 and
              checked_records <= @max_u64 and is_integer(checked_entries) and
              checked_entries >= 0 and checked_entries <= @max_u64 and is_integer(mismatches) and
              mismatches > 0 and mismatches <= @max_u64 and is_atom(reason) and
              is_integer(validated_at_ms) and validated_at_ms >= 0 and
              validated_at_ms <= @max_u64,
       do: validate_validation_checkpoints(checkpoints)

  defp validate_stored_validation(_validation), do: {:error, :invalid_validation}

  defp validate_validation_checkpoints(checkpoints) do
    if Enum.all?(checkpoints, fn {shard, checkpoint} ->
         is_integer(shard) and shard >= 0 and valid_validation_checkpoint?(checkpoint)
       end),
       do: :ok,
       else: {:error, :invalid_validation_checkpoint}
  end

  defp valid_validation_checkpoint?(%{
         phase: phase,
         cursor: cursor,
         fenced: fenced,
         definition_position: definition_position,
         checked_records: checked_records,
         checked_entries: checked_entries,
         mismatches: 0,
         counter_runs: counter_runs
       }) do
    phase in @validation_phases and is_binary(cursor) and byte_size(cursor) <= @max_cursor_bytes and
      is_boolean(fenced) and
      is_integer(definition_position) and definition_position >= 0 and definition_position <= 16 and
      nonnegative_u64?(checked_records) and nonnegative_u64?(checked_entries) and
      valid_counter_run_shape?(counter_runs)
  end

  defp valid_validation_checkpoint?(_checkpoint), do: false

  defp validate_persisted_validation_groups(entries) do
    valid? =
      entries
      |> Map.values()
      |> Enum.filter(&match?(%{state: :validating, validation: %{status: :pending}}, &1))
      |> Enum.group_by(& &1.build_id)
      |> Enum.all?(fn {_build_id, group} -> valid_persisted_validation_group?(group) end)

    if valid?,
      do: :ok,
      else: {:error, {:invalid_query_index_registry_snapshot, :invalid_validation_group}}
  end

  defp valid_persisted_validation_group?(entries) do
    definitions =
      entries
      |> Enum.map(& &1.definition)
      |> Enum.sort_by(&{&1.id, &1.version})

    checkpoint_sets = entries |> Enum.map(& &1.validation.checkpoints) |> Enum.uniq()

    case checkpoint_sets do
      [checkpoints] ->
        Enum.all?(checkpoints, fn {_shard, checkpoint} ->
          validate_validation_checkpoint_position(checkpoint, definitions) == :ok and
            (checkpoint.fenced or checkpoint == empty_validation_checkpoint())
        end)

      _divergent ->
        false
    end
  end

  defp validate_stored_retirement(nil), do: :ok

  defp validate_stored_retirement(%{status: status, checkpoints: checkpoints})
       when status in [:pending, :complete] and is_map(checkpoints) do
    if Enum.all?(checkpoints, fn {shard, checkpoint} ->
         is_integer(shard) and shard >= 0 and valid_retirement_checkpoint?(checkpoint)
       end),
       do: :ok,
       else: {:error, :invalid_retirement_checkpoint}
  end

  defp validate_stored_retirement(_retirement), do: {:error, :invalid_retirement}

  defp valid_retirement_checkpoint?(%{
         phase: phase,
         cursor: cursor,
         deleted_entries: deleted_entries,
         deleted_bytes: deleted_bytes,
         rewritten_reverse_rows: rewritten_reverse_rows
       }) do
    checkpoint = %{
      phase: phase,
      cursor: cursor,
      deleted_entries: deleted_entries,
      deleted_bytes: deleted_bytes,
      rewritten_reverse_rows: rewritten_reverse_rows
    }

    phase in @retirement_phases and is_binary(cursor) and byte_size(cursor) <= @max_cursor_bytes and
      nonnegative_u64?(deleted_entries) and nonnegative_u64?(deleted_bytes) and
      nonnegative_u64?(rewritten_reverse_rows) and valid_retirement_phase_checkpoint?(checkpoint)
  end

  defp valid_retirement_checkpoint?(_checkpoint), do: false

  defp valid_retirement_phase_checkpoint?(%{
         phase: :fence,
         cursor: "",
         deleted_entries: 0,
         deleted_bytes: 0,
         rewritten_reverse_rows: 0
       }),
       do: true

  defp valid_retirement_phase_checkpoint?(%{phase: :index, rewritten_reverse_rows: 0}), do: true
  defp valid_retirement_phase_checkpoint?(%{phase: :counter, rewritten_reverse_rows: 0}), do: true
  defp valid_retirement_phase_checkpoint?(%{phase: :reverse}), do: true

  defp valid_retirement_phase_checkpoint?(%{phase: phase, cursor: ""})
       when phase in [:cleanup, :done],
       do: true

  defp valid_retirement_phase_checkpoint?(_checkpoint), do: false

  defp validate_entry_lifecycle(:building, nil, nil), do: :ok
  defp validate_entry_lifecycle(:validating, %{status: :pending}, nil), do: :ok
  defp validate_entry_lifecycle(:validating, %{status: :passed}, nil), do: :ok
  defp validate_entry_lifecycle(:active, %{status: :passed}, nil), do: :ok

  defp validate_entry_lifecycle(state, %{status: status}, %{status: retirement_status})
       when state in [:retiring, :failed] and status in [:passed, :failed] and
              retirement_status in [:pending, :complete],
       do: :ok

  defp validate_entry_lifecycle(_state, _validation, _retirement),
    do: {:error, :invalid_lifecycle}

  defp validate_persisted_shards(entries, shard_count) do
    if Enum.all?(entries, fn {_key, entry} ->
         Enum.all?(Map.keys(entry.checkpoints), &(&1 < shard_count)) and
           Enum.all?(Map.keys(validation_checkpoints(entry)), &(&1 < shard_count)) and
           Enum.all?(Map.keys(retirement_checkpoints(entry)), &(&1 < shard_count))
       end),
       do: :ok,
       else: {:error, :query_index_registry_shard_count_changed}
  end

  defp validate_persisted_lifecycle_progress(entries, shard_count) do
    valid? =
      Enum.all?(entries, fn
        {_key, %{state: :building}} ->
          true

        {_key, %{state: :validating} = entry} ->
          complete_build_proof?(entry.checkpoints, shard_count)

        {_key, %{state: state} = entry} when state in [:active, :retiring] ->
          complete_build_proof?(entry.checkpoints, shard_count) and
            complete_validation_proof?(entry.validation, shard_count)

        {_key, %{state: :failed}} ->
          true
      end)

    if valid?,
      do: :ok,
      else: {:error, {:invalid_query_index_registry_snapshot, :invalid_lifecycle_progress}}
  end

  defp complete_build_proof?(checkpoints, shard_count) do
    complete_shard_proof?(checkpoints, shard_count, fn
      %{phase: :done, cursor: "", fenced: true} -> true
      _checkpoint -> false
    end)
  end

  defp complete_validation_proof?(%{status: :passed, checkpoints: checkpoints}, shard_count) do
    complete_shard_proof?(checkpoints, shard_count, fn
      %{phase: :done, cursor: "", fenced: true, counter_runs: []} -> true
      _checkpoint -> false
    end)
  end

  defp complete_validation_proof?(_validation, _shard_count), do: false

  defp complete_shard_proof?(checkpoints, shard_count, predicate)
       when is_map(checkpoints) and is_integer(shard_count) and shard_count > 0 do
    map_size(checkpoints) == shard_count and
      Enum.all?(0..(shard_count - 1), fn shard_index ->
        checkpoints
        |> Map.get(shard_index)
        |> predicate.()
      end)
  end

  defp retirement_checkpoints(%{retirement: %{checkpoints: checkpoints}}), do: checkpoints
  defp retirement_checkpoints(_entry), do: %{}
end
