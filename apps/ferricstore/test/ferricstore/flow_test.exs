Code.require_file("flow_test/sections/part_01.exs", __DIR__)
Code.require_file("flow_test/sections/part_02.exs", __DIR__)
Code.require_file("flow_test/sections/part_03.exs", __DIR__)
Code.require_file("flow_test/sections/part_04.exs", __DIR__)
Code.require_file("flow_test/sections/part_05.exs", __DIR__)
Code.require_file("flow_test/sections/part_06.exs", __DIR__)
Code.require_file("flow_test/sections/part_07.exs", __DIR__)
Code.require_file("flow_test/sections/part_08.exs", __DIR__)
Code.require_file("flow_test/sections/part_09.exs", __DIR__)
Code.require_file("flow_test/sections/part_10.exs", __DIR__)
Code.require_file("flow_test/sections/part_11.exs", __DIR__)
Code.require_file("flow_test/sections/part_12.exs", __DIR__)
Code.require_file("flow_test/sections/part_13.exs", __DIR__)

defmodule Ferricstore.FlowTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp attach_flow_telemetry(events) do
    test_pid = self()

    handler_ids =
      Enum.map(events, fn event ->
        handler_id = {__MODULE__, self(), event, System.unique_integer([:positive])}

        :ok =
          :telemetry.attach(
            handler_id,
            event,
            &__MODULE__.handle_telemetry/4,
            test_pid
          )

        handler_id
      end)

    on_exit(fn ->
      Enum.each(handler_ids, &:telemetry.detach/1)
    end)
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:flow_telemetry, event, measurements, metadata})
  end

  defp receive_segment_append_bytes(kind, acc) do
    receive do
      {:flow_telemetry, [:ferricstore, :waraft, :segment_log, :append], %{bytes: bytes},
       %{kind: ^kind}} ->
        receive_segment_append_bytes(kind, acc + bytes)

      {:flow_telemetry, [:ferricstore, :waraft, :segment_log, :append], _measurements, _metadata} ->
        receive_segment_append_bytes(kind, acc)
    after
      100 -> acc
    end
  end

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  defp yield_non_empty_claim(task) do
    case Task.yield(task, 0) do
      {:ok, {:ok, [_ | _] = claimed}} -> claimed
      _other -> nil
    end
  end

  defp flow_create_and_get(id, opts) do
    case FerricStore.flow_create(id, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_create_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_create_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_created_flow(&1, partition_key, opts))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_create_results(results, items, partition_key, opts)}

      other ->
        other
    end
  end

  defp flow_spawn_children_and_get(parent_id, children, opts) do
    case FerricStore.flow_spawn_children(parent_id, children, opts) do
      :ok ->
        FerricStore.flow_get(parent_id, flow_partition_opts(Keyword.get(opts, :partition_key)))

      other ->
        other
    end
  end

  defp flow_transition_and_get(id, from_state, to_state, opts \\ []) do
    case FerricStore.flow_transition(id, from_state, to_state, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_transition_many_and_get(partition_key, from_state, to_state, items, opts \\ []) do
    case FerricStore.flow_transition_many(partition_key, from_state, to_state, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp flow_complete_and_get(id, lease_token, opts \\ []) do
    case FerricStore.flow_complete(id, lease_token, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_complete_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_complete_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp flow_retry_and_get(id, lease_token, opts) do
    case FerricStore.flow_retry(id, lease_token, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_retry_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_retry_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp flow_fail_and_get(id, lease_token, opts \\ []) do
    case FerricStore.flow_fail(id, lease_token, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_fail_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_fail_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp flow_cancel_and_get(id, opts \\ []) do
    case FerricStore.flow_cancel(id, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_cancel_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_cancel_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp impl_flow_create_and_get(ctx, id, opts) do
    case FerricStore.Impl.flow_create(ctx, id, opts) do
      :ok ->
        FerricStore.Impl.flow_get(ctx, id, flow_partition_opts(Keyword.get(opts, :partition_key)))

      other ->
        other
    end
  end

  defp impl_flow_spawn_children_and_get(ctx, parent_id, children, opts) do
    case FerricStore.Impl.flow_spawn_children(ctx, parent_id, children, opts) do
      :ok ->
        FerricStore.Impl.flow_get(
          ctx,
          parent_id,
          flow_partition_opts(Keyword.get(opts, :partition_key))
        )

      other ->
        other
    end
  end

  defp impl_flow_retry_and_get(ctx, id, lease_token, opts) do
    case FerricStore.Impl.flow_retry(ctx, id, lease_token, opts) do
      :ok ->
        FerricStore.Impl.flow_get(ctx, id, flow_partition_opts(Keyword.get(opts, :partition_key)))

      other ->
        other
    end
  end

  defp fetch_created_flow(item, partition_key, opts) do
    {id, item_partition_key} = create_item_identity(item, partition_key, opts)
    {:ok, flow} = FerricStore.flow_get(id, flow_partition_opts(item_partition_key))
    flow
  end

  defp fetch_many_flow(item, partition_key) do
    {id, item_partition_key} = many_item_identity(item, partition_key)
    {:ok, flow} = FerricStore.flow_get(id, flow_partition_opts(item_partition_key))
    flow
  end

  defp hydrate_create_results(results, items, partition_key, opts) do
    results
    |> Enum.zip(items)
    |> Enum.map(fn
      {:ok, item} -> fetch_created_flow(item, partition_key, opts)
      {other, _item} -> other
    end)
  end

  defp hydrate_many_results(results, items, partition_key) do
    results
    |> Enum.zip(items)
    |> Enum.map(fn
      {:ok, item} -> fetch_many_flow(item, partition_key)
      {other, _item} -> other
    end)
  end

  defp create_item_identity(%{id: id} = item, partition_key, opts),
    do: {id, Map.get(item, :partition_key) || partition_key || Keyword.get(opts, :partition_key)}

  defp create_item_identity(%{"id" => id} = item, partition_key, opts),
    do: {id, Map.get(item, "partition_key") || partition_key || Keyword.get(opts, :partition_key)}

  defp create_item_identity({id, item_opts}, partition_key, opts)
       when is_binary(id) and is_list(item_opts),
       do:
         {id,
          Keyword.get(item_opts, :partition_key) || partition_key ||
            Keyword.get(opts, :partition_key)}

  defp create_item_identity(id, partition_key, opts) when is_binary(id),
    do: {id, partition_key || Keyword.get(opts, :partition_key)}

  defp many_item_identity(%{id: id} = item, partition_key),
    do: {id, Map.get(item, :partition_key) || partition_key}

  defp many_item_identity(%{"id" => id} = item, partition_key),
    do: {id, Map.get(item, "partition_key") || partition_key}

  defp many_item_identity({id, _lease_token, item_opts}, partition_key)
       when is_binary(id) and is_list(item_opts),
       do: {id, Keyword.get(item_opts, :partition_key) || partition_key}

  defp many_item_identity({id, item_opts}, partition_key)
       when is_binary(id) and is_list(item_opts),
       do: {id, Keyword.get(item_opts, :partition_key) || partition_key}

  defp many_item_identity(
         {:id, id, :partition_key, item_partition_key, :lease_token, _token, :fencing_token,
          _fencing_token},
         _partition_key
       ),
       do: {id, item_partition_key}

  defp many_item_identity(
         {:id, id, :lease_token, _token, :fencing_token, _fencing_token},
         partition_key
       ),
       do: {id, partition_key}

  defp flow_partition_opts(nil), do: []
  defp flow_partition_opts(partition_key), do: [partition_key: partition_key]

  defp encoded_value_size(value) do
    value
    |> Ferricstore.Flow.encode_value()
    |> byte_size()
  end

  defp shard_for(key) do
    Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), key)
  end

  defp different_partition_keys do
    base = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))

    first =
      1..64
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.find(fn key ->
        shard_for(Ferricstore.Flow.Keys.state_key("probe", key)) !=
          shard_for(Ferricstore.Flow.Keys.state_key("probe", nil))
      end)

    second =
      1..64
      |> Enum.map(&"#{base}:other:#{&1}")
      |> Enum.find(fn key ->
        shard_for(Ferricstore.Flow.Keys.state_key("probe", key)) !=
          shard_for(Ferricstore.Flow.Keys.state_key("probe", first))
      end)

    {first, second}
  end

  defp mixed_partition_keys do
    base = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))

    groups =
      1..256
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.group_by(fn key -> shard_for(Ferricstore.Flow.Keys.state_key("probe", key)) end)

    {same_shard, [same_a, same_b | _]} =
      Enum.find(groups, fn {_shard, keys} -> length(keys) >= 2 end)

    {other_shard, [other | _]} = Enum.find(groups, fn {shard, _keys} -> shard != same_shard end)

    assert same_shard != other_shard
    {same_a, same_b, other}
  end

  defp due_partition_keys_on_different_shards(type, state \\ "queued", priority \\ 0) do
    ctx = FerricStore.Instance.get(:default)
    base = "tenant:due:" <> Integer.to_string(System.unique_integer([:positive]))

    groups =
      1..512
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.group_by(fn partition_key ->
        due_key = Ferricstore.Flow.Keys.due_key(type, state, priority, partition_key)
        Ferricstore.Store.Router.shard_for(ctx, due_key)
      end)

    {_first_shard, [first | _]} = Enum.at(groups, 0)
    {_second_shard, [second | _]} = Enum.at(groups, 1)

    {first, second}
  end

  defp auto_ids_on_different_due_shards(type, state \\ "queued", priority \\ 0) do
    ctx = FerricStore.Instance.get(:default)
    base = "auto-due:" <> Integer.to_string(System.unique_integer([:positive]))

    groups =
      1..1_024
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.group_by(fn id ->
        partition_key = Ferricstore.Flow.Keys.auto_partition_key(id)
        due_key = Ferricstore.Flow.Keys.due_key(type, state, priority, partition_key)
        Ferricstore.Store.Router.shard_for(ctx, due_key)
      end)

    {_first_shard, [first | _]} = Enum.at(groups, 0)
    {_second_shard, [second | _]} = Enum.at(groups, 1)

    {first, second}
  end

  defp create_claimed_flow(id, partition_key, flow_type, worker) do
    assert {:ok, _} =
             flow_create_and_get(id,
               type: flow_type,
               partition_key: partition_key,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(flow_type,
               partition_key: partition_key,
               worker: worker,
               limit: 1,
               now_ms: 1_000
             )

    claimed
  end

  defp create_claimed_flow_child(id, partition_key, worker) do
    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("child",
               partition_key: partition_key,
               worker: worker,
               limit: 1,
               now_ms: 9_000_000_000_000
             )

    assert claimed.id == id
    claimed
  end

  use Ferricstore.FlowTest.Sections.Part01
  use Ferricstore.FlowTest.Sections.Part02
  use Ferricstore.FlowTest.Sections.Part03
  use Ferricstore.FlowTest.Sections.Part04
  use Ferricstore.FlowTest.Sections.Part05
  use Ferricstore.FlowTest.Sections.Part06
  use Ferricstore.FlowTest.Sections.Part07
  use Ferricstore.FlowTest.Sections.Part08
  use Ferricstore.FlowTest.Sections.Part09
  use Ferricstore.FlowTest.Sections.Part10
  use Ferricstore.FlowTest.Sections.Part11
  use Ferricstore.FlowTest.Sections.Part12
  use Ferricstore.FlowTest.Sections.Part13

defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp start_flow_restart_instance(name, data_dir) do
    ctx =
      FerricStore.Instance.build(name,
        data_dir: data_dir,
        shard_count: 1,
        max_memory_bytes: 256 * 1024 * 1024,
        keydir_max_ram: 64 * 1024 * 1024
      )

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    {:ok, _writer} =
      Ferricstore.Flow.LMDBWriter.start_link(
        shard_index: 0,
        data_dir: data_dir,
        instance_ctx: ctx
      )

    {:ok, _shard} =
      Ferricstore.Store.Shard.start_link(
        index: 0,
        data_dir: data_dir,
        instance_ctx: ctx
      )

    ShardHelpers.eventually(
      fn ->
        pid = Process.whereis(elem(ctx.shard_names, 0))

        is_pid(pid) and Process.alive?(pid) and
          match?(
            {:ok, _},
            try do
              {:ok, GenServer.call(elem(ctx.shard_names, 0), :shard_stats, 500)}
            catch
              :exit, _ -> :error
            end
          )
      end,
      "restart flow shard not ready",
      50,
      20
    )

    ctx
  end

  defp stop_flow_restart_instance(nil, _opts), do: :ok

  defp stop_flow_restart_instance(ctx, opts) do
    Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    stop_registered_process(elem(ctx.shard_names, 0))
    stop_registered_process(Ferricstore.Flow.LMDBWriter.name(ctx.name, 0))

    for table <- [elem(ctx.keydir_refs, 0), ctx.hotness_table, ctx.config_table] do
      try do
        :ets.delete(table)
      rescue
        _ -> :ok
      end
    end

    FerricStore.Instance.cleanup(ctx.name)

    if Keyword.get(opts, :delete?, false) do
      File.rm_rf!(ctx.data_dir)
    end

    :ok
  end

  defp stop_registered_process(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
    end
  end
end

