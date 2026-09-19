defmodule Ferricstore.Flow.LMDBRebuilderPruneRaceTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDBRebuilder}
  alias Ferricstore.Raft.WARaftSegmentReader

  for {replace?, before_prune?} <- [{false, false}, {true, false}, {true, true}] do
    @replace replace?
    @before_prune before_prune?
    test "terminal rebuild pruning accounts only the deleted row (replacement=#{replace?}, before_prune=#{before_prune?})" do
      data_dir =
        Path.join(System.tmp_dir!(), "rebuild-prune-#{System.unique_integer([:positive])}")

      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      keydir = :ets.new(:rebuild_prune_keydir, [:set])
      bytes = :atomics.new(1, signed: true)
      id = String.duplicate("prune", 20)
      key = Keys.state_key(id)

      record = %{
        id: id,
        type: "prune",
        state: "completed",
        version: 1,
        incarnation: 1,
        attempts: 0,
        fencing_token: 0,
        created_at_ms: 1,
        updated_at_ms: 2,
        next_run_at_ms: nil,
        priority: 0,
        partition_key: nil,
        root_flow_id: id
      }

      encoded = Ferricstore.Flow.encode_record(record)

      newer =
        Ferricstore.Flow.encode_record(%{
          record
          | state: if(@before_prune, do: "completed", else: "queued"),
            version: if(@before_prune, do: 1, else: 2),
            incarnation: 2
        })

      old_row = {key, encoded, 0, 0, {:waraft_apply_projection, 1}, 0, byte_size(encoded)}
      new_row = {key, newer, 0, 0, {:waraft_apply_projection, 2}, 0, byte_size(newer)}
      true = :ets.insert(keydir, old_row)
      :atomics.put(bytes, 1, offheap(key) + offheap(encoded))

      on_exit(fn ->
        WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
        File.rm_rf!(data_dir)
      end)

      assert :ok = WARaftSegmentReader.put_apply_projection(data_dir, 0, 1, [{key, encoded, 0}])
      assert :ok = WARaftSegmentReader.put_apply_projection(data_dir, 0, 2, [{key, newer, 0}])

      # Replace either during snapshot reading or pruning's later durability check.
      Process.put(:ferricstore_waraft_apply_projection_disk_read_hook, fn _, _, _ ->
        {:current_stacktrace, stack} = Process.info(self(), :current_stacktrace)

        pruning? =
          Enum.any?(stack, fn
            {LMDBRebuilder, :ensure_apply_projection_row_durable, _, _} -> true
            _ -> false
          end)

        if pruning? or (@before_prune and not Process.get(:prune_hook_called, false)) do
          Process.put(:prune_hook_called, true)

          if @replace do
            true = :ets.insert(keydir, new_row)
            :atomics.put(bytes, 1, offheap(key) + offheap(newer))
          end
        end

        :ok
      end)

      ctx = %{data_dir: data_dir, shard_count: 1, keydir_binary_bytes: bytes}

      try do
        assert :ok =
                 LMDBRebuilder.reconcile_shard(
                   shard_path,
                   keydir,
                   0,
                   ctx,
                   nil,
                   nil,
                   nil,
                   nil,
                   prune_terminal_keydir?: true
                 )

        assert Process.get(:prune_hook_called)

        if @replace do
          assert :ets.lookup(keydir, key) == [new_row]
          assert :atomics.get(bytes, 1) == offheap(key) + offheap(newer)
        else
          assert :ets.lookup(keydir, key) == []
          assert :atomics.get(bytes, 1) == 0
        end
      after
        Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
        Process.delete(:prune_hook_called)
      end
    end
  end

  defp offheap(binary) when byte_size(binary) > 64, do: byte_size(binary)
  defp offheap(_), do: 0
end
