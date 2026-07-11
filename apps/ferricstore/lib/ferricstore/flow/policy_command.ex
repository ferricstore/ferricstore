defmodule Ferricstore.Flow.PolicyCommand do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Store.Router

  @internal_keys [
    :policy_generation,
    :policy_snapshot,
    :policy_snapshots,
    :policy_snapshot_captured
  ]

  @flow_commands [
    :flow_cancel,
    :flow_cancel_many,
    :flow_claim_due,
    :flow_complete,
    :flow_complete_many,
    :flow_create,
    :flow_create_many,
    :flow_create_pipeline_batch,
    :flow_cross_spawn_children,
    :flow_cross_terminal,
    :flow_cross_terminal_many,
    :flow_fail,
    :flow_fail_many,
    :flow_reschedule,
    :flow_retry,
    :flow_retry_many,
    :flow_rewind,
    :flow_run_steps_many,
    :flow_schedule_replace,
    :flow_spawn_children,
    :flow_start_and_claim,
    :flow_start_and_claim_pipeline_batch,
    :flow_step_continue,
    :flow_step_continue_many,
    :flow_terminal_pipeline_batch,
    :flow_transition,
    :flow_transition_many
  ]

  @spec requires_stamp?(tuple()) :: boolean()
  def requires_stamp?({:cross_shard_tx, shard_batches}) when is_list(shard_batches),
    do: shard_batches_require_stamp?(shard_batches)

  def requires_stamp?({:cross_shard_tx, shard_batches, _watched_keys})
      when is_list(shard_batches),
      do: shard_batches_require_stamp?(shard_batches)

  def requires_stamp?({:flow_shared_ref_write, _shard_index, command}) when is_tuple(command),
    do: requires_stamp?(command)

  def requires_stamp?(command) when is_tuple(command) and tuple_size(command) > 0,
    do: policy_sensitive_op?(elem(command, 0))

  def requires_stamp?(_command), do: false

  @spec policy_sensitive_op?(term()) :: boolean()
  def policy_sensitive_op?(op) when op in @flow_commands, do: true
  def policy_sensitive_op?(_op), do: false

  defp shard_batches_require_stamp?(shard_batches) do
    Enum.any?(shard_batches, fn
      {_shard_index, queue, _namespace} when is_list(queue) ->
        Enum.any?(queue, &queue_entry_requires_stamp?/1)

      _invalid ->
        false
    end)
  end

  defp queue_entry_requires_stamp?({index, command})
       when is_integer(index) and is_tuple(command),
       do: requires_stamp?(command)

  defp queue_entry_requires_stamp?(command) when is_tuple(command),
    do: requires_stamp?(command)

  defp queue_entry_requires_stamp?(_entry), do: false

  @spec stamp(FerricStore.Instance.t(), tuple()) :: {:ok, tuple()} | {:error, binary()}
  def stamp(ctx, command) when is_tuple(command) do
    if requires_stamp?(command) do
      do_stamp(ctx, command)
    else
      {:ok, command}
    end
  end

  defp do_stamp(ctx, command) do
    with {:ok, stamped, _cache} <- stamp_command(ctx, command, %{}),
         :ok <- validate_stamped_snapshot_size(stamped),
         :ok <- validate_stamped_snapshot_occurrence_size(stamped) do
      {:ok, stamped}
    end
  end

  @spec stamp_many(FerricStore.Instance.t(), [{binary(), tuple()}]) ::
          {:ok, [{binary(), tuple()}]} | {:error, binary()}
  def stamp_many(ctx, keyed_commands) when is_list(keyed_commands) do
    if Enum.any?(keyed_commands, fn {_key, command} -> requires_stamp?(command) end) do
      do_stamp_many(ctx, keyed_commands)
    else
      {:ok, keyed_commands}
    end
  end

  defp do_stamp_many(ctx, keyed_commands) do
    keyed_commands
    |> Enum.reduce_while({:ok, [], %{}}, fn {key, command}, {:ok, stamped, cache} ->
      case stamp_command(ctx, command, cache) do
        {:ok, next, next_cache} ->
          case validate_stamped_snapshot_size(next) do
            :ok -> {:cont, {:ok, [{key, next} | stamped], next_cache}}
            {:error, _reason} = error -> {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, stamped, _cache} ->
        stamped = Enum.reverse(stamped)

        with :ok <- validate_stamped_batch_snapshot_size(stamped) do
          {:ok, stamped}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp stamp_command(ctx, {:cross_shard_tx, shard_batches}, cache) do
    with {:ok, stamped, cache} <- stamp_shard_batches(ctx, shard_batches, cache) do
      {:ok, {:cross_shard_tx, stamped}, cache}
    end
  end

  defp stamp_command(ctx, {:cross_shard_tx, shard_batches, watched_keys}, cache) do
    with {:ok, stamped, cache} <- stamp_shard_batches(ctx, shard_batches, cache) do
      {:ok, {:cross_shard_tx, stamped, watched_keys}, cache}
    end
  end

  defp stamp_command(ctx, {:flow_shared_ref_write, shard_index, command}, cache) do
    with {:ok, stamped, cache} <- stamp_command(ctx, command, cache) do
      {:ok, {:flow_shared_ref_write, shard_index, stamped}, cache}
    end
  end

  defp stamp_command(_ctx, command, cache) when tuple_size(command) == 0,
    do: {:ok, command, cache}

  defp stamp_command(ctx, command, cache) do
    op = elem(command, 0)
    attrs = elem(command, tuple_size(command) - 1)

    if policy_sensitive_op?(op) and is_map(attrs) do
      with {:ok, attrs, cache} <- stamp_attrs(ctx, attrs, cache) do
        attrs =
          attrs
          |> compact_nested_policy_snapshots()
          |> Map.put(:policy_snapshot_captured, true)

        {:ok, put_elem(command, tuple_size(command) - 1, attrs), cache}
      end
    else
      {:ok, command, cache}
    end
  end

  defp stamp_shard_batches(ctx, shard_batches, cache) when is_list(shard_batches) do
    shard_batches
    |> Enum.reduce_while({:ok, [], cache}, fn
      {shard_index, queue, namespace}, {:ok, batches, cache} when is_list(queue) ->
        case stamp_queue(ctx, queue, cache) do
          {:ok, stamped, cache} ->
            {:cont, {:ok, [{shard_index, stamped, namespace} | batches], cache}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      _invalid, _acc ->
        {:halt, {:error, "ERR invalid cross-shard Flow command"}}
    end)
    |> case do
      {:ok, batches, cache} -> {:ok, Enum.reverse(batches), cache}
      {:error, _reason} = error -> error
    end
  end

  defp stamp_queue(ctx, queue, cache) do
    queue
    |> Enum.reduce_while({:ok, [], cache}, fn entry, {:ok, entries, cache} ->
      case stamp_queue_entry(ctx, entry, cache) do
        {:ok, stamped, cache} -> {:cont, {:ok, [stamped | entries], cache}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries, cache} -> {:ok, Enum.reverse(entries), cache}
      {:error, _reason} = error -> error
    end
  end

  defp stamp_queue_entry(ctx, {index, command}, cache)
       when is_integer(index) and is_tuple(command) do
    with {:ok, stamped, cache} <- stamp_command(ctx, command, cache) do
      {:ok, {index, stamped}, cache}
    end
  end

  defp stamp_queue_entry(ctx, command, cache) when is_tuple(command),
    do: stamp_command(ctx, command, cache)

  defp stamp_queue_entry(_ctx, entry, cache), do: {:ok, entry, cache}

  defp stamp_attrs(ctx, attrs, cache) do
    attrs = Map.drop(attrs, @internal_keys)

    with {:ok, attrs, cache} <- stamp_attrs_list(ctx, attrs, :records, cache),
         {:ok, attrs, cache} <- stamp_attrs_list(ctx, attrs, :children, cache),
         {:ok, type} <- attrs_type(ctx, attrs) do
      case type do
        nil ->
          {:ok, attrs, cache}

        type ->
          with {:ok, snapshot, cache} <- policy_snapshot(ctx, type, cache) do
            stamped =
              attrs
              |> Map.put(:policy_generation, snapshot.generation)
              |> Map.put(:policy_snapshot, snapshot.policy)

            {:ok, stamped, cache}
          end
      end
    end
  end

  defp compact_nested_policy_snapshots(attrs) do
    if Enum.any?([:records, :children], fn key -> is_list(Map.get(attrs, key)) end) do
      {attrs, snapshots} = extract_nested_policy_snapshots(attrs, %{})

      if map_size(snapshots) == 0 do
        attrs
      else
        Map.put(attrs, :policy_snapshots, snapshots)
      end
    else
      attrs
    end
  end

  defp extract_nested_policy_snapshots(attrs, snapshots) do
    {attrs, snapshots} = extract_direct_policy_snapshot(attrs, snapshots)

    Enum.reduce([:records, :children], {attrs, snapshots}, fn key, {attrs, snapshots} ->
      case Map.get(attrs, key) do
        entries when is_list(entries) ->
          {entries, snapshots} =
            Enum.map_reduce(entries, snapshots, fn
              entry, acc when is_map(entry) -> extract_nested_policy_snapshots(entry, acc)
              entry, acc -> {entry, acc}
            end)

          {Map.put(attrs, key, entries), snapshots}

        _other ->
          {attrs, snapshots}
      end
    end)
  end

  defp extract_direct_policy_snapshot(attrs, snapshots) do
    case {Map.get(attrs, :policy_generation), Map.get(attrs, :policy_snapshot)} do
      {generation, %{type: type} = policy}
      when is_integer(generation) and generation >= 0 and is_binary(type) and type != "" ->
        snapshot = %{generation: generation, policy: policy}

        {Map.drop(attrs, [:policy_generation, :policy_snapshot]),
         Map.put(snapshots, type, snapshot)}

      _none ->
        {attrs, snapshots}
    end
  end

  defp stamp_attrs_list(ctx, attrs, key, cache) do
    case Map.get(attrs, key) do
      entries when is_list(entries) ->
        entries
        |> Enum.reduce_while({:ok, [], cache}, fn
          entry, {:ok, stamped, cache} when is_map(entry) ->
            case stamp_attrs(ctx, entry, cache) do
              {:ok, next, cache} -> {:cont, {:ok, [next | stamped], cache}}
              {:error, _reason} = error -> {:halt, error}
            end

          entry, {:ok, stamped, cache} ->
            {:cont, {:ok, [entry | stamped], cache}}
        end)
        |> case do
          {:ok, stamped, cache} -> {:ok, Map.put(attrs, key, Enum.reverse(stamped)), cache}
          {:error, _reason} = error -> error
        end

      _other ->
        {:ok, attrs, cache}
    end
  end

  defp attrs_type(_ctx, %{type: type}) when is_binary(type) and type != "", do: {:ok, type}

  defp attrs_type(ctx, %{id: id} = attrs) when is_binary(id) and id != "" do
    case Router.flow_get_with_status(ctx, id, Map.get(attrs, :partition_key)) do
      nil ->
        {:ok, nil}

      :unavailable ->
        {:error, "ERR flow state shard not available"}

      value when is_binary(value) ->
        try do
          case Flow.decode_record(value) do
            %{type: type} when is_binary(type) and type != "" -> {:ok, type}
            _record -> {:error, "ERR stored flow type is invalid"}
          end
        rescue
          _error -> {:error, "ERR stored flow record is corrupt"}
        end

      _other ->
        {:error, "ERR stored flow record is corrupt"}
    end
  end

  defp attrs_type(_ctx, _attrs), do: {:ok, nil}

  defp policy_snapshot(ctx, type, cache) do
    case Map.fetch(cache, type) do
      {:ok, snapshot} ->
        {:ok, snapshot, cache}

      :error ->
        with {:ok, snapshot} <- read_policy_snapshot(ctx, type) do
          {:ok, snapshot, Map.put(cache, type, snapshot)}
        end
    end
  end

  defp read_policy_snapshot(ctx, type) do
    case Router.read_shard_value(ctx, 0, Keys.policy_key(type)) do
      {:ok, nil} ->
        {:ok, %{generation: 0, policy: %{type: type}}}

      {:ok, value} when is_binary(value) ->
        case RetryPolicy.decode_flow_policy_entry(value) do
          {:ok, {generation, %{type: ^type} = policy}} ->
            {:ok, %{generation: generation, policy: policy}}

          _invalid ->
            {:error, "ERR flow policy is corrupt"}
        end

      {:ok, _other} ->
        {:error, "ERR flow policy is corrupt"}

      :unavailable ->
        {:error, "ERR flow policy shard not available"}
    end
  end

  defp validate_stamped_snapshot_size(command) do
    command
    |> stamped_snapshot_policies(%{})
    |> Map.values()
    |> RetryPolicy.validate_flow_policy_snapshots_size()
  end

  defp validate_stamped_batch_snapshot_size(keyed_commands) do
    size =
      Enum.reduce(keyed_commands, 0, fn {_key, command}, total ->
        total + stamped_snapshot_occurrence_bytes(command)
      end)

    RetryPolicy.validate_flow_policy_snapshot_batch_size(size)
  end

  defp validate_stamped_snapshot_occurrence_size(command) do
    command
    |> stamped_snapshot_occurrence_bytes()
    |> RetryPolicy.validate_flow_policy_snapshot_batch_size()
  end

  defp stamped_snapshot_occurrence_bytes({:cross_shard_tx, shard_batches}),
    do: stamped_shard_batch_occurrence_bytes(shard_batches)

  defp stamped_snapshot_occurrence_bytes({:cross_shard_tx, shard_batches, _watched_keys}),
    do: stamped_shard_batch_occurrence_bytes(shard_batches)

  defp stamped_snapshot_occurrence_bytes({:flow_shared_ref_write, _shard_index, command}),
    do: stamped_snapshot_occurrence_bytes(command)

  defp stamped_snapshot_occurrence_bytes(command)
       when is_tuple(command) and tuple_size(command) > 0 do
    case elem(command, tuple_size(command) - 1) do
      attrs when is_map(attrs) -> stamped_attrs_occurrence_bytes(attrs)
      _other -> 0
    end
  end

  defp stamped_snapshot_occurrence_bytes(_command), do: 0

  defp stamped_shard_batch_occurrence_bytes(shard_batches) when is_list(shard_batches) do
    Enum.reduce(shard_batches, 0, fn
      {_shard_index, queue, _namespace}, total when is_list(queue) ->
        total +
          Enum.reduce(queue, 0, fn
            {_index, command}, acc when is_tuple(command) ->
              acc + stamped_snapshot_occurrence_bytes(command)

            command, acc when is_tuple(command) ->
              acc + stamped_snapshot_occurrence_bytes(command)

            _entry, acc ->
              acc
          end)

      _invalid, total ->
        total
    end)
  end

  defp stamped_attrs_occurrence_bytes(attrs) do
    direct_size =
      case Map.get(attrs, :policy_snapshot) do
        policy when is_map(policy) -> :erlang.external_size(policy)
        _none -> 0
      end

    compact_size =
      case Map.get(attrs, :policy_snapshots) do
        snapshots when is_map(snapshots) ->
          Enum.reduce(snapshots, 0, fn
            {_type, %{policy: policy}}, total when is_map(policy) ->
              total + :erlang.external_size(policy)

            _invalid, total ->
              total
          end)

        _none ->
          0
      end

    direct_size + compact_size
  end

  defp stamped_snapshot_policies({:cross_shard_tx, shard_batches}, policies),
    do: stamped_shard_batch_policies(shard_batches, policies)

  defp stamped_snapshot_policies(
         {:cross_shard_tx, shard_batches, _watched_keys},
         policies
       ),
       do: stamped_shard_batch_policies(shard_batches, policies)

  defp stamped_snapshot_policies(
         {:flow_shared_ref_write, _shard_index, command},
         policies
       ),
       do: stamped_snapshot_policies(command, policies)

  defp stamped_snapshot_policies(command, policies)
       when is_tuple(command) and tuple_size(command) > 0 do
    case elem(command, tuple_size(command) - 1) do
      attrs when is_map(attrs) -> stamped_attrs_policies(attrs, policies)
      _other -> policies
    end
  end

  defp stamped_snapshot_policies(_command, policies), do: policies

  defp stamped_shard_batch_policies(shard_batches, policies) when is_list(shard_batches) do
    Enum.reduce(shard_batches, policies, fn
      {_shard_index, queue, _namespace}, acc when is_list(queue) ->
        Enum.reduce(queue, acc, fn
          {_index, command}, inner when is_tuple(command) ->
            stamped_snapshot_policies(command, inner)

          command, inner when is_tuple(command) ->
            stamped_snapshot_policies(command, inner)

          _entry, inner ->
            inner
        end)

      _invalid, acc ->
        acc
    end)
  end

  defp stamped_attrs_policies(attrs, policies) do
    policies =
      case Map.get(attrs, :policy_snapshot) do
        %{type: type} = policy when is_binary(type) and type != "" ->
          Map.put(policies, type, policy)

        _none ->
          policies
      end

    case Map.get(attrs, :policy_snapshots) do
      snapshots when is_map(snapshots) ->
        Enum.reduce(snapshots, policies, fn
          {type, %{policy: %{type: type} = policy}}, acc -> Map.put(acc, type, policy)
          _invalid, acc -> acc
        end)

      _none ->
        policies
    end
  end
end
