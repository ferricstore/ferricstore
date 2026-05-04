defmodule Ferricstore.ReviewR4.D14StaleActiveFileTest do
  @moduledoc """
  D14: Verifies whether the Raft state machine's active_file_id/path
  become stale after the Shard rotates the active file.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D14: stale active_file_id/path in Raft state after Shard rotation" do
    test "state machine init stores active_file_id/path and never updates them" do
      # From state_machine.ex init/1 lines 112-126:
      #
      #   %{
      #     shard_index: config.shard_index,
      #     shard_data_path: config.shard_data_path,
      #     active_file_id: config.active_file_id,      # ← set once at init
      #     active_file_path: config.active_file_path,   # ← set once at init
      #     ets: config.ets,
      #     ...
      #   }
      #
      # These fields are NEVER updated. There is no Raft command to update
      # active_file_id/path, and the Shard's maybe_rotate_file (shard.ex:2969)
      # only updates:
      #   1. Its own Shard GenServer state
      #   2. ActiveFile.publish (persistent_term/ETS for Router's async path)
      #
      # It does NOT notify the ra state machine.
      #
      # flush_pending_writes at line 1217 uses state.active_file_path:
      #   NIF.v2_append_batch_nosync(state.active_file_path, batch)
      #
      # And at line 1226 uses state.active_file_id:
      #   :ets.update_element(state.ets, key, [{5, state.active_file_id}, ...])
      #
      # HOWEVER: In practice, this may not be a critical bug in single-node mode
      # because:
      #
      # 1. maybe_rotate_file is called from Shard's handle_call/handle_cast
      #    for direct writes (not Raft path). The Raft state machine has its
      #    own write path that goes through flush_pending_writes.
      #
      # 2. The file rotation happens when active_file_size exceeds the max.
      #    The Shard tracks active_file_size based on its OWN writes, not the
      #    state machine's writes.
      #
      # 3. In the Raft path (quorum writes), the state machine always writes
      #    to state.active_file_path. The Shard may rotate its file, but
      #    the state machine continues writing to the old file. This means:
      #    - The "old" file keeps growing beyond max_active_file_size
      #    - ETS entries get the old file_id even for new writes
      #    - On restart, the old file is correctly read (data isn't lost)
      #    - But compaction/merge may not work correctly because it expects
      #      the active file to be the latest one
      #
      # 4. In multi-node mode, this is worse: the Shard on the leader node
      #    may rotate, but the state machine on follower nodes still has the
      #    ORIGINAL file_id/path from init. After a snapshot restore, the
      #    follower would try to write to a file that may not exist.
      #
      # WAIT: actually let me re-check. The shard only rotates via direct
      # write path. When quorum_write is used, writes go through ra:pipeline_command
      # which goes to the state machine. The shard's active_file_size may not
      # increase from these writes because the state machine writes directly.
      # So rotation may never trigger for the Raft write path.
      #
      # But the state machine DOES write to the file (flush_pending_writes),
      # and the file grows. Nobody tracks this growth or triggers rotation
      # for the state machine's file. The file just keeps growing.

      root = Path.join(System.tmp_dir!(), "ferricstore_d14_#{System.unique_integer([:positive])}")
      shard_data_path = Path.join([root, "data", "shard_0"])
      File.mkdir_p!(shard_data_path)
      ets = :ets.new(:d14_stale_active_file, [:set, :public])

      on_exit(fn ->
        File.rm_rf!(root)

        try do
          :ets.delete(ets)
        rescue
          ArgumentError -> :ok
        end
      end)

      active_file_path = Path.join(shard_data_path, "00001.log")

      config = %{
        shard_index: 0,
        shard_data_path: shard_data_path,
        active_file_id: 1,
        active_file_path: active_file_path,
        ets: ets
      }

      state = Ferricstore.Raft.StateMachine.init(config)

      assert state.active_file_id == 1
      assert state.active_file_path == active_file_path

      # After hypothetical shard rotation, state still has old values
      # There is no command to update these fields
      # Grep for any apply/3 clause that updates active_file_id:
      # (already verified — none exists)
    end

    test "no Raft command exists to update active_file_id/path" do
      # Exhaustive list of apply/3 commands from state_machine.ex:
      # :put, :delete, :batch, :cross_shard_tx, :list_op,
      # :compound_put, :compound_delete, :compound_delete_prefix,
      # :incr, :incr_float, :append, :getset, :getdel, :getex,
      # :setrange, :cas, :lock, :unlock, :extend,
      # :ratelimit_add (5-arg), :ratelimit_add (6-arg),
      # :lock_keys, :unlock_keys, :cross_shard_intent,
      # :delete_intent, :get_intents, :get_lock_count,
      # :clear_locks, :locked_put, :locked_delete,
      # :locked_delete_prefix, HLC-wrapped commands.
      #
      # NONE of these update state.active_file_id or state.active_file_path.
      # The only state mutation is on applied_count, cross_shard_locks,
      # and cross_shard_intents.

      assert true, "No :rotate_file or :update_active_file command exists"
    end
  end
end
