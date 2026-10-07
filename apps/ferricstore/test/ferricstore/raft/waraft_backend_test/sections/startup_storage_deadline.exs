defmodule Ferricstore.Raft.WARaftBackendTest.Sections.StartupStorageDeadline do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.WARaftBackend

      @tag :startup_storage_deadline
      test "slow bootstrap honors its startup budget and restores ordinary storage-call timeout",
           %{ctx: ctx} do
        previous_timeout = Application.fetch_env(:wa_raft, :raft_storage_call_timeout)
        previous_budget = Application.get_env(:ferricstore, :waraft_start_wait_timeout_ms)
        previous_hook = Application.get_env(:ferricstore, :waraft_storage_fsync_dir_hook)
        Application.put_env(:wa_raft, :raft_storage_call_timeout, 50)
        Application.put_env(:ferricstore, :waraft_start_wait_timeout_ms, 1_000)
        once = :atomics.new(1, signed: false)
        parent = self()

        Application.put_env(:ferricstore, :waraft_storage_fsync_dir_hook, fn path ->
          if String.contains?(path, "snapshot.1.1.bootstrap.tmp") and
               :atomics.compare_exchange(once, 1, 0, 1) == :ok do
            send(
              parent,
              {:bootstrap_storage_timeout,
               Application.get_env(:wa_raft, :raft_storage_call_timeout)}
            )

            Process.sleep(250)
          end

          Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
        end)

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert_receive {:bootstrap_storage_timeout, 1_000}
          assert Application.get_env(:wa_raft, :raft_storage_call_timeout) == 50
          assert :ok = WARaftBackend.write(0, {:put, "startup:deadline", "value", 0})
        after
          case previous_timeout do
            :error -> Application.delete_env(:wa_raft, :raft_storage_call_timeout)
            {:ok, value} -> Application.put_env(:wa_raft, :raft_storage_call_timeout, value)
          end

          restore_env(:waraft_start_wait_timeout_ms, previous_budget)
          restore_env(:waraft_storage_fsync_dir_hook, previous_hook)
        end
      end

      @tag :startup_storage_deadline
      test "failed startup restores the dependency storage timeout", %{ctx: ctx, root: root} do
        previous_timeout = Application.fetch_env(:wa_raft, :raft_storage_call_timeout)
        previous_budget = Application.get_env(:ferricstore, :waraft_start_wait_timeout_ms)
        Application.put_env(:wa_raft, :raft_storage_call_timeout, 50)
        Application.put_env(:ferricstore, :waraft_start_wait_timeout_ms, 1_000)
        path = Path.join([root, "waraft", "ferricstore_waraft_backend.1", "segment_log"])
        File.rm_rf!(path)
        File.mkdir_p!(Path.dirname(path))
        File.ln_s!(Path.join(root, "missing-target"), path)

        try do
          assert {:error, {:unsafe_segment_log_dir, ^path}} = WARaftBackend.start(ctx)
          assert Application.get_env(:wa_raft, :raft_storage_call_timeout) == 50
          refute WARaftBackend.starting?()
        after
          case previous_timeout do
            :error -> Application.delete_env(:wa_raft, :raft_storage_call_timeout)
            {:ok, value} -> Application.put_env(:wa_raft, :raft_storage_call_timeout, value)
          end

          restore_env(:waraft_start_wait_timeout_ms, previous_budget)
        end
      end

      @tag :startup_storage_deadline
      test "invalid startup budget cannot stop an already running backend", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx)
        previous = Application.get_env(:ferricstore, :waraft_start_wait_timeout_ms)
        Application.put_env(:ferricstore, :waraft_start_wait_timeout_ms, 0)

        try do
          assert_raise ArgumentError, fn -> WARaftBackend.start(ctx) end
          refute WARaftBackend.starting?()
          assert :ok = WARaftBackend.write(0, {:put, "startup:still-running", "value", 0})
        after
          restore_env(:waraft_start_wait_timeout_ms, previous)
        end
      end

      @tag :startup_storage_deadline
      test "killed startup caller cannot leak the temporary storage timeout", %{ctx: ctx} do
        previous_timeout = Application.fetch_env(:wa_raft, :raft_storage_call_timeout)
        previous_budget = Application.get_env(:ferricstore, :waraft_start_wait_timeout_ms)
        previous_hook = Application.get_env(:ferricstore, :waraft_storage_fsync_dir_hook)
        Application.put_env(:wa_raft, :raft_storage_call_timeout, 50)
        Application.put_env(:ferricstore, :waraft_start_wait_timeout_ms, 1_000)
        once = :atomics.new(1, signed: false)
        parent = self()

        Application.put_env(:ferricstore, :waraft_storage_fsync_dir_hook, fn path ->
          if String.contains?(path, "snapshot.1.1.bootstrap.tmp") and
               :atomics.compare_exchange(once, 1, 0, 1) == :ok do
            send(parent, {:startup_paused, self()})

            receive do
              :resume_startup -> :ok
            after
              5_000 -> :ok
            end
          end

          Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
        end)

        {:ok, starter} = Task.start(fn -> WARaftBackend.start(ctx) end)
        assert_receive {:startup_paused, storage}, 5_000

        try do
          assert Application.get_env(:wa_raft, :raft_storage_call_timeout) == 1_000
          Process.exit(starter, :kill)

          Ferricstore.Test.ShardHelpers.eventually(
            fn ->
              Application.get_env(:wa_raft, :raft_storage_call_timeout) == 50
            end,
            "temporary startup timeout leaked after caller death"
          )
        after
          if Process.alive?(starter), do: Process.exit(starter, :kill)

          case previous_timeout do
            :error -> Application.delete_env(:wa_raft, :raft_storage_call_timeout)
            {:ok, value} -> Application.put_env(:wa_raft, :raft_storage_call_timeout, value)
          end

          restore_env(:waraft_start_wait_timeout_ms, previous_budget)
          restore_env(:waraft_storage_fsync_dir_hook, previous_hook)
          send(storage, :resume_startup)
          # Finish the owned storage callback while this test still owns its
          # context tables; caller death does not cancel native/snapshot work.
          :ok = WARaftBackend.stop()
        end
      end
    end
  end
end
