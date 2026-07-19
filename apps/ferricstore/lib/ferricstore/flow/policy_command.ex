defmodule Ferricstore.Flow.PolicyCommand do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Store.Router

  @internal_keys [
    :policy_ref,
    :policy_refs,
    :policy_guard,
    :policy_reference_captured,
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
    :flow_fail,
    :flow_fail_many,
    :flow_reschedule,
    :flow_retry,
    :flow_retry_many,
    :flow_rewind,
    :flow_run_steps_many,
    :flow_schedule_replace,
    :flow_signal,
    :flow_signal_many,
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
  def requires_stamp?(command) when is_tuple(command) and tuple_size(command) > 0,
    do: policy_sensitive_op?(elem(command, 0))

  def requires_stamp?(_command), do: false

  @spec policy_sensitive_op?(term()) :: boolean()
  def policy_sensitive_op?(op) when op in @flow_commands, do: true
  def policy_sensitive_op?(_op), do: false

  @spec stamp(FerricStore.Instance.t(), tuple()) :: {:ok, tuple()} | {:error, binary()}
  def stamp(ctx, command) when is_tuple(command) do
    if requires_stamp?(command) do
      do_stamp(ctx, command)
    else
      {:ok, command}
    end
  end

  def stamp(_ctx, _command), do: {:error, "ERR flow command must be a tuple"}

  defp do_stamp(ctx, command) do
    with {:ok, stamped, cache} <- stamp_command(ctx, command, %{}),
         :ok <- validate_cached_policy_size(cache),
         :ok <- validate_stamped_snapshot_size(stamped),
         fenced = fence_command(stamped, policy_installs(cache)),
         :ok <- validate_stamped_snapshot_occurrence_size(fenced) do
      {:ok, fenced}
    end
  end

  @spec stamp_many(FerricStore.Instance.t(), [{binary(), tuple()}]) ::
          {:ok, [{binary(), tuple()}]} | {:error, binary()}
  def stamp_many(ctx, keyed_commands) when is_list(keyed_commands) do
    with :ok <- validate_keyed_commands(keyed_commands) do
      if Enum.any?(keyed_commands, fn {_key, command} -> requires_stamp?(command) end) do
        do_stamp_many(ctx, keyed_commands)
      else
        {:ok, keyed_commands}
      end
    end
  end

  def stamp_many(_ctx, _keyed_commands),
    do: {:error, "ERR flow keyed commands must be a list"}

  defp do_stamp_many(ctx, keyed_commands) do
    keyed_commands
    |> Enum.reduce_while({:ok, [], %{}, %{}}, fn {key, command}, {:ok, stamped, cache, targets} ->
      target = Map.get(targets, key, :lookup)

      case stamp_command(ctx, command, cache, target) do
        {:ok, next, next_cache} ->
          case validate_stamped_snapshot_size(next) do
            :ok ->
              next_targets = remember_batch_target(targets, key, next)
              {:cont, {:ok, [{key, next} | stamped], next_cache, next_targets}}

            {:error, _reason} = error ->
              {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, stamped, cache, _targets} ->
        stamped = Enum.reverse(stamped)
        stamped = fence_keyed_commands(ctx, stamped, cache)

        with :ok <- validate_cached_policy_size(cache),
             :ok <- validate_stamped_batch_snapshot_size(stamped) do
          {:ok, stamped}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp stamp_command(_ctx, command, cache) when tuple_size(command) == 0,
    do: {:ok, command, cache}

  defp stamp_command(ctx, command, cache), do: stamp_command(ctx, command, cache, :lookup)

  defp stamp_command(_ctx, command, cache, _target) when tuple_size(command) == 0,
    do: {:ok, command, cache}

  defp stamp_command(ctx, command, cache, target) do
    op = elem(command, 0)
    attrs = elem(command, tuple_size(command) - 1)

    if policy_sensitive_op?(op) do
      if is_map(attrs) do
        with {:ok, attrs, cache} <- stamp_attrs(ctx, attrs, cache, target) do
          attrs =
            attrs
            |> compact_nested_policy_refs()
            |> Map.put(:policy_reference_captured, true)

          {:ok, put_elem(command, tuple_size(command) - 1, attrs), cache}
        end
      else
        {:error, "ERR flow policy-sensitive command attrs must be a map"}
      end
    else
      {:ok, command, cache}
    end
  end

  defp validate_keyed_commands(keyed_commands) do
    if Enum.all?(keyed_commands, fn
         {key, command} when is_binary(key) and is_tuple(command) -> true
         _entry -> false
       end) do
      :ok
    else
      {:error, "ERR flow keyed command must be a {binary_key, tuple_command} pair"}
    end
  end

  defp stamp_attrs(ctx, attrs, cache) do
    stamp_attrs(ctx, attrs, cache, :lookup)
  end

  defp stamp_attrs(ctx, attrs, cache, target) do
    attrs = Map.drop(attrs, @internal_keys)

    with {:ok, attrs, cache} <- stamp_attrs_list(ctx, attrs, :records, cache),
         {:ok, attrs, cache} <- stamp_attrs_list(ctx, attrs, :children, cache),
         {:ok, target} <- attrs_policy_target(ctx, attrs, target) do
      case target do
        nil ->
          {:ok, attrs, cache}

        %{type: type} = target ->
          with {:ok, policy_ref, cache} <- policy_reference(ctx, type, cache) do
            stamped = attrs |> Map.put(:policy_ref, policy_ref) |> maybe_put_policy_guard(target)

            {:ok, stamped, cache}
          end

        %{guard: guard} when is_map(guard) ->
          {:ok, Map.put(attrs, :policy_guard, guard), cache}
      end
    end
  end

  defp remember_batch_target(targets, _key, command) when tuple_size(command) == 0, do: targets

  defp remember_batch_target(targets, key, command) do
    attrs = elem(command, tuple_size(command) - 1)

    case attrs do
      %{policy_ref: %{type: type}} when is_binary(type) and type != "" ->
        Map.put(targets, key, {:known, type})

      _other ->
        targets
    end
  end

  defp compact_nested_policy_refs(attrs) do
    if Enum.any?([:records, :children], fn key -> is_list(Map.get(attrs, key)) end) do
      {attrs, refs} = extract_nested_policy_refs(attrs, %{})

      if map_size(refs) == 0 do
        attrs
      else
        Map.put(attrs, :policy_refs, refs)
      end
    else
      attrs
    end
  end

  defp extract_nested_policy_refs(attrs, refs) do
    {attrs, refs} = extract_direct_policy_ref(attrs, refs)

    Enum.reduce([:records, :children], {attrs, refs}, fn key, {attrs, refs} ->
      case Map.get(attrs, key) do
        entries when is_list(entries) ->
          {entries, refs} =
            Enum.map_reduce(entries, refs, fn
              entry, acc when is_map(entry) -> extract_nested_policy_refs(entry, acc)
              entry, acc -> {entry, acc}
            end)

          {Map.put(attrs, key, entries), refs}

        _other ->
          {attrs, refs}
      end
    end)
  end

  defp extract_direct_policy_ref(attrs, refs) do
    case Map.get(attrs, :policy_ref) do
      %{type: type, generation: generation, digest: digest} = policy_ref
      when is_binary(type) and type != "" and is_integer(generation) and generation >= 0 and
             is_binary(digest) and byte_size(digest) == 32 ->
        {Map.delete(attrs, :policy_ref), Map.put(refs, type, policy_ref)}

      _none ->
        {attrs, refs}
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

  defp attrs_policy_target(_ctx, %{type: type}, _target)
       when is_binary(type) and type != "",
       do: {:ok, %{type: type}}

  defp attrs_policy_target(_ctx, _attrs, {:known, type})
       when is_binary(type) and type != "",
       do: {:ok, %{type: type}}

  defp attrs_policy_target(ctx, attrs, :lookup), do: attrs_policy_target(ctx, attrs)

  defp attrs_policy_target(_ctx, %{type: type}) when is_binary(type) and type != "",
    do: {:ok, %{type: type}}

  defp attrs_policy_target(ctx, %{id: id} = attrs) when is_binary(id) and id != "" do
    partition_key = Map.get(attrs, :partition_key)

    case Router.flow_get_with_status(ctx, id, partition_key) do
      nil ->
        {:ok,
         %{
           guard: %{
             state_key: Keys.state_key(id, partition_key),
             absent: true
           }
         }}

      :unavailable ->
        {:error, "ERR flow state shard not available"}

      value when is_binary(value) ->
        try do
          case Flow.decode_record(value) do
            %{type: type, incarnation: incarnation}
            when is_binary(type) and type != "" and is_integer(incarnation) and incarnation >= 0 ->
              {:ok,
               %{
                 type: type,
                 guard: %{
                   state_key: Keys.state_key(id, partition_key),
                   type: type,
                   incarnation: incarnation
                 }
               }}

            %{type: type} when is_binary(type) and type != "" ->
              {:error, "ERR stored flow incarnation is invalid"}

            _record ->
              {:error, "ERR stored flow type is invalid"}
          end
        rescue
          _error -> {:error, "ERR stored flow record is corrupt"}
        end

      _other ->
        {:error, "ERR stored flow record is corrupt"}
    end
  end

  defp attrs_policy_target(_ctx, _attrs), do: {:ok, nil}

  defp maybe_put_policy_guard(attrs, %{guard: guard}) when is_map(guard),
    do: Map.put(attrs, :policy_guard, guard)

  defp maybe_put_policy_guard(attrs, _target), do: attrs

  defp policy_reference(ctx, type, cache) do
    case Map.fetch(cache, type) do
      {:ok, %{ref: policy_ref}} ->
        {:ok, policy_ref, cache}

      :error ->
        with {:ok, policy_ref, policy, encoded} <- read_policy_reference(ctx, type) do
          entry = %{ref: policy_ref, policy: policy, encoded: encoded}
          {:ok, policy_ref, Map.put(cache, type, entry)}
        end
    end
  end

  defp read_policy_reference(ctx, type) do
    case Router.read_shard_value(ctx, 0, Keys.policy_key(type)) do
      {:ok, nil} ->
        policy = %{type: type}
        encoded = RetryPolicy.encode_flow_policy(policy, 0)
        {:ok, build_policy_ref(type, 0, encoded), policy, encoded}

      {:ok, value} when is_binary(value) ->
        case RetryPolicy.decode_flow_policy_entry(value) do
          {:ok, {generation, %{type: ^type} = policy}} ->
            {:ok, build_policy_ref(type, generation, value), policy, value}

          _invalid ->
            {:error, "ERR flow policy is corrupt"}
        end

      {:ok, _other} ->
        {:error, "ERR flow policy is corrupt"}

      :unavailable ->
        {:error, "ERR flow policy shard not available"}
    end
  end

  defp build_policy_ref(type, generation, encoded) do
    %{type: type, generation: generation, digest: :crypto.hash(:sha256, encoded)}
  end

  defp policy_installs(cache) do
    cache
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {type, %{encoded: encoded}} -> {Keys.policy_key(type), encoded, 0} end)
  end

  defp fence_command(command, []), do: command

  defp fence_command(command, installs),
    do: {:flow_policy_fence, installs, command}

  defp fence_keyed_commands(ctx, keyed_commands, cache) do
    {fenced, _seen} =
      Enum.map_reduce(keyed_commands, MapSet.new(), fn {key, command}, seen ->
        shard_index = policy_command_shard(ctx, key)

        {installs, seen} =
          command
          |> stamped_policy_refs(%{})
          |> Map.keys()
          |> Enum.sort()
          |> Enum.reduce({[], seen}, fn type, {installs, seen} ->
            identity = {shard_index, type}

            if MapSet.member?(seen, identity) do
              {installs, seen}
            else
              encoded = cache |> Map.fetch!(type) |> Map.fetch!(:encoded)
              install = {Keys.policy_key(type), encoded, 0}
              {[install | installs], MapSet.put(seen, identity)}
            end
          end)

        {{key, fence_command(command, Enum.reverse(installs))}, seen}
      end)

    fenced
  end

  defp policy_command_shard(%{slot_map: slot_map} = ctx, key)
       when is_tuple(slot_map) and is_binary(key),
       do: Router.shard_for(ctx, key)

  defp policy_command_shard(_ctx, _key), do: 0

  defp validate_cached_policy_size(cache) do
    cache
    |> Map.values()
    |> Enum.map(& &1.policy)
    |> RetryPolicy.validate_flow_policy_snapshots_size()
  end

  defp validate_stamped_snapshot_size(command) do
    command
    |> stamped_policy_refs(%{})
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

  defp stamped_snapshot_occurrence_bytes({:flow_policy_fence, installs, command})
       when is_list(installs) do
    :erlang.external_size(installs) + stamped_snapshot_occurrence_bytes(command)
  end

  defp stamped_snapshot_occurrence_bytes(command)
       when is_tuple(command) and tuple_size(command) > 0 do
    case elem(command, tuple_size(command) - 1) do
      attrs when is_map(attrs) -> stamped_attrs_occurrence_bytes(attrs)
      _other -> 0
    end
  end

  defp stamped_snapshot_occurrence_bytes(_command), do: 0

  defp stamped_attrs_occurrence_bytes(attrs) do
    direct_size =
      case Map.get(attrs, :policy_ref) do
        policy_ref when is_map(policy_ref) -> :erlang.external_size(policy_ref)
        _none -> 0
      end

    compact_size =
      case Map.get(attrs, :policy_refs) do
        refs when is_map(refs) ->
          Enum.reduce(refs, 0, fn
            {_type, policy_ref}, total when is_map(policy_ref) ->
              total + :erlang.external_size(policy_ref)

            _invalid, total ->
              total
          end)

        _none ->
          0
      end

    direct_size + compact_size
  end

  defp stamped_policy_refs({:flow_policy_fence, _installs, command}, refs),
    do: stamped_policy_refs(command, refs)

  defp stamped_policy_refs(command, refs)
       when is_tuple(command) and tuple_size(command) > 0 do
    case elem(command, tuple_size(command) - 1) do
      attrs when is_map(attrs) -> stamped_attrs_policy_refs(attrs, refs)
      _other -> refs
    end
  end

  defp stamped_policy_refs(_command, refs), do: refs

  defp stamped_attrs_policy_refs(attrs, refs) do
    refs =
      case Map.get(attrs, :policy_ref) do
        %{type: type} = policy_ref when is_binary(type) and type != "" ->
          Map.put(refs, type, policy_ref)

        _none ->
          refs
      end

    case Map.get(attrs, :policy_refs) do
      policy_refs when is_map(policy_refs) ->
        Enum.reduce(policy_refs, refs, fn
          {type, %{type: type} = policy_ref}, acc -> Map.put(acc, type, policy_ref)
          _invalid, acc -> acc
        end)

      _none ->
        refs
    end
  end
end
