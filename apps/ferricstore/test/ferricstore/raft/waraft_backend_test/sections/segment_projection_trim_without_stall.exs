defmodule Ferricstore.Raft.WARaftBackendTest.Sections.SegmentProjectionTrimWithoutStall do
  @moduledoc false

  # A WARaft log trim must not rewrite every live key inside the Raft commit
  # path. Background checkpoints take a consistent cut, write the projection
  # off the commit path, and a trim adopts the finished checkpoint. Without a
  # usable checkpoint a trim defers (WARaft allows that) up to a bound.
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Store.Router

      @checkpoint_envs [
        :waraft_segment_projection_checkpoint_every,
        :waraft_segment_projection_checkpoint_min_interval_ms,
        :waraft_segment_projection_checkpoint_hook,
        :waraft_segment_projection_trim_max_deferred_entries
      ]

      @tag :bounded_checkpoint
      test "a background checkpoint cuts a consistent snapshot while writes continue", %{
        ctx: ctx,
        root: root
      } do
        parent = self()

        hook = fn
          phase, _metadata when phase in [:before_scan, :before_write] ->
            send(parent, {:checkpoint_phase, phase, self()})

            receive do
              :resume_checkpoint -> :ok
            after
              10_000 -> :ok
            end

          _phase, _metadata ->
            :ok
        end

        with_checkpoint_env(
          [every: :never, min_interval_ms: 0, hook: hook, max_deferred: 0],
          fn ->
            assert :ok =
                     WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

            Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1)
            assert :ok = WARaftBackend.write(0, {:put, "consistent-cut:k1", "v1", 0})
            assert_receive {:checkpoint_phase, :before_scan, checkpoint_pid}, 2_000
            Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, :never)

            # Lands after the checkpoint started but before its cut.
            assert :ok = WARaftBackend.write(0, {:put, "consistent-cut:k2", "v2", 0})
            {:raft_log_pos, k2_index, _} = applied_position(0)
            send(checkpoint_pid, :resume_checkpoint)

            assert_receive {:checkpoint_phase, :before_write, ^checkpoint_pid}, 2_000

            # Lands after the cut; the checkpoint must not contain it.
            assert :ok = WARaftBackend.write(0, {:put, "consistent-cut:k3", "v3", 0})
            {:raft_log_pos, k3_index, _} = applied_position(0)
            send(checkpoint_pid, :resume_checkpoint)

            assert_receive {:waraft_segment_projection_checkpoint,
                            [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
                            _measurements,
                            %{result: :ok, position: {:raft_log_pos, cut_index, _}}},
                           5_000

            assert cut_index >= k2_index
            assert cut_index < k3_index

            {cut_position, keys} = projection_keys(checkpoint_root(root))
            assert {:raft_log_pos, ^cut_index, _} = cut_position
            assert "consistent-cut:k1" in keys
            assert "consistent-cut:k2" in keys
            refute "consistent-cut:k3" in keys
          end
        )
      end

      @tag :bounded_checkpoint
      test "trim adopts the finished checkpoint instead of rewriting the projection", %{
        ctx: ctx,
        root: root
      } do
        with_checkpoint_env([every: :never, min_interval_ms: 0, max_deferred: 0], fn ->
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = WARaftBackend.write(0, {:put, "adopt:k1", "v1", 0})
          assert :ok = WARaftBackend.write(0, {:put, "adopt:k2", "v2", 0})
          checkpoint_index = run_checkpoint!(0, {:put, "adopt:k3", "v3", 0})

          checkpoint_files = file_inodes(checkpoint_root(root))
          assert map_size(checkpoint_files) > 0

          log = waraft_segment_log_record(0)

          assert {:ok, _state} =
                   :ferricstore_waraft_spike_segment_log.trim(log, checkpoint_index, %{})

          assert_receive {:waraft_segment_projection_trim,
                          [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse],
                          %{relocations: relocations}, %{checkpoint_index: ^checkpoint_index}},
                         2_000

          # k1 and k2 sit below the trim index; k3 is the trim index itself.
          assert relocations == 2

          # The checkpoint's own files became the projection; nothing was rewritten.
          refute File.exists?(checkpoint_root(root))
          projection_files = file_inodes(projection_root(root))

          for {name, inode} <- checkpoint_files do
            assert Map.get(projection_files, name) == inode, "#{name} was rewritten"
          end

          projection_dir = to_charlist(projection_root(root))

          assert "v3" == Router.get(ctx, "adopt:k3")

          for {key, value} <- [{"adopt:k1", "v1"}, {"adopt:k2", "v2"}] do
            assert value == Router.get(ctx, key)

            assert [{^key, _value, 0, _lfu, {:waraft_projection, index}, offset, _size}] =
                     :ets.lookup(elem(ctx.keydir_refs, 0), key)

            assert {:ok, {_ordinal, ^offset, _encoded_size}} =
                     :ferricstore_waraft_spike_segment_log.location_for_index(
                       projection_dir,
                       index
                     )
          end

          # The adopted projection is what recovery replays from.
          WARaftBackend.stop()
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert "v1" == Router.get(ctx, "adopt:k1")
          assert "v3" == Router.get(ctx, "adopt:k3")
        end)
      end

      @tag :bounded_checkpoint
      test "trim defers instead of waiting for a running checkpoint", %{ctx: ctx, root: root} do
        parent = self()

        hook = fn
          :before_write, _metadata ->
            send(parent, {:checkpoint_phase, :before_write, self()})

            receive do
              :resume_checkpoint -> :ok
            after
              10_000 -> :ok
            end

          _phase, _metadata ->
            :ok
        end

        with_checkpoint_env(
          [every: :never, min_interval_ms: 0, hook: hook, max_deferred: 1_000_000],
          fn ->
            assert :ok =
                     WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

            assert :ok = WARaftBackend.write(0, {:put, "defer-running:z", "z", 0})
            Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1)
            assert :ok = WARaftBackend.write(0, {:put, "defer-running:a", "a", 0})
            assert_receive {:checkpoint_phase, :before_write, checkpoint_pid}, 2_000
            Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, :never)

            log = waraft_segment_log_record(0)
            first_before = :ferricstore_waraft_spike_segment_log.first_index(log)
            trim_index = :ferricstore_waraft_spike_segment_log.last_index(log)
            trim_ref = make_ref()

            spawn(fn ->
              send(
                parent,
                {trim_ref, :ferricstore_waraft_spike_segment_log.trim(log, trim_index, %{})}
              )
            end)

            assert_receive {^trim_ref, {:ok, _state}}, 1_000

            assert_receive {:waraft_segment_projection_trim,
                            [:ferricstore, :waraft, :segment_projection_trim, :deferred], _,
                            %{reason: :checkpoint_running, trim_index: ^trim_index}},
                           1_000

            assert first_before == :ferricstore_waraft_spike_segment_log.first_index(log)
            assert segment_row?(ctx, "defer-running:z")

            send(checkpoint_pid, :resume_checkpoint)

            assert_receive {:waraft_segment_projection_checkpoint,
                            [:ferricstore, :waraft, :segment_projection_checkpoint, :stop], _,
                            %{result: :ok, position: {:raft_log_pos, checkpoint_index, _}}},
                           5_000

            assert {:ok, _state} =
                     :ferricstore_waraft_spike_segment_log.trim(log, checkpoint_index, %{})

            assert_receive {:waraft_segment_projection_trim,
                            [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse],
                            _, %{checkpoint_index: ^checkpoint_index}},
                           2_000

            refute segment_row?(ctx, "defer-running:z")
            assert "z" == Router.get(ctx, "defer-running:z")
          end
        )
      end

      @tag :bounded_checkpoint
      test "trim without a usable checkpoint defers and requests one", %{ctx: ctx, root: root} do
        with_checkpoint_env([every: :never, min_interval_ms: 0, max_deferred: 1_000_000], fn ->
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = WARaftBackend.write(0, {:put, "defer-missing:k1", "v1", 0})
          assert :ok = WARaftBackend.write(0, {:put, "defer-missing:k2", "v2", 0})

          log = waraft_segment_log_record(0)
          first_before = :ferricstore_waraft_spike_segment_log.first_index(log)
          trim_index = :ferricstore_waraft_spike_segment_log.last_index(log)

          assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, trim_index, %{})

          assert_receive {:waraft_segment_projection_trim,
                          [:ferricstore, :waraft, :segment_projection_trim, :deferred], _,
                          %{reason: :no_checkpoint, trim_index: ^trim_index}},
                         1_000

          assert first_before == :ferricstore_waraft_spike_segment_log.first_index(log)
          assert segment_row?(ctx, "defer-missing:k1")

          # The deferred trim asked for a checkpoint although the cadence is off.
          assert_receive {:waraft_segment_projection_checkpoint,
                          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop], _,
                          %{result: :ok, position: {:raft_log_pos, checkpoint_index, _}}},
                         5_000

          assert checkpoint_index >= trim_index
          assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, trim_index, %{})

          assert_receive {:waraft_segment_projection_trim,
                          [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse], _,
                          %{trim_index: ^trim_index}},
                         2_000

          assert :ferricstore_waraft_spike_segment_log.first_index(log) > first_before
          refute segment_row?(ctx, "defer-missing:k1")
          assert "v1" == Router.get(ctx, "defer-missing:k1")
        end)
      end

      @tag :bounded_checkpoint
      test "trim adopts a checkpoint that covers only part of it", %{ctx: ctx, root: root} do
        with_checkpoint_env([every: :never, min_interval_ms: 0, max_deferred: 1_000_000], fn ->
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = WARaftBackend.write(0, {:put, "partial:k1", "v1", 0})
          checkpoint_index = run_checkpoint!(0, {:put, "partial:k2", "v2", 0})
          assert :ok = WARaftBackend.write(0, {:put, "partial:k3", "v3", 0})
          assert :ok = WARaftBackend.write(0, {:put, "partial:k4", "v4", 0})

          log = waraft_segment_log_record(0)
          trim_index = :ferricstore_waraft_spike_segment_log.last_index(log)
          assert trim_index > checkpoint_index

          assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, trim_index, %{})

          # Adopted up to the checkpoint; the newer entries stay in the log.
          assert_receive {:waraft_segment_projection_trim,
                          [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse], _,
                          %{trim_index: ^checkpoint_index, checkpoint_index: ^checkpoint_index}},
                         2_000

          assert :ferricstore_waraft_spike_segment_log.first_index(log) == checkpoint_index
          refute segment_row?(ctx, "partial:k1")
          assert segment_row?(ctx, "partial:k3")

          # It asked for a newer checkpoint, which the next trim adopts in full.
          assert_receive {:waraft_segment_projection_checkpoint,
                          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop], _,
                          %{result: :ok, position: {:raft_log_pos, next_index, _}}},
                         5_000

          assert next_index >= trim_index
          assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, trim_index, %{})

          assert_receive {:waraft_segment_projection_trim,
                          [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse], _,
                          %{trim_index: ^trim_index}},
                         2_000

          refute segment_row?(ctx, "partial:k3")

          for i <- 1..4 do
            assert "v#{i}" == Router.get(ctx, "partial:k#{i}")
          end
        end)
      end

      @tag :bounded_checkpoint
      test "a checkpoint that lost the node slot retries without waiting out its interval", %{
        root: root
      } do
        parent = self()
        multi_root = Path.join(root, "slot-retry")
        Ferricstore.DataDir.ensure_layout!(multi_root, 2)
        Ferricstore.Store.ActiveFile.init(2)
        multi_ctx = build_ctx(multi_root, shard_count: 2)

        hook = fn
          :before_write, %{shard_index: 0} ->
            send(parent, {:checkpoint_phase, :before_write, self()})

            receive do
              :resume_checkpoint -> :ok
            after
              10_000 -> :ok
            end

          _phase, _metadata ->
            :ok
        end

        try do
          with_checkpoint_env(
            [every: :never, min_interval_ms: 30_000, hook: hook, max_deferred: 0],
            fn ->
              assert :ok =
                       WARaftBackend.start(multi_ctx,
                         log_module: :ferricstore_waraft_spike_segment_log
                       )

              key0 = key_for_shard(multi_ctx, 0, "slot-retry:0")
              key1 = key_for_shard(multi_ctx, 1, "slot-retry:1")

              Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1)
              assert :ok = WARaftBackend.write(0, {:put, key0, "v0", 0})
              assert_receive {:checkpoint_phase, :before_write, checkpoint_pid}, 2_000

              assert :ok = WARaftBackend.write(1, {:put, key1, "v1", 0})

              assert_receive {:waraft_segment_projection_checkpoint,
                              [:ferricstore, :waraft, :segment_projection_checkpoint, :stop], _,
                              %{result: :busy, shard_index: 1}},
                             2_000

              send(checkpoint_pid, :resume_checkpoint)

              assert_receive {:waraft_segment_projection_checkpoint,
                              [:ferricstore, :waraft, :segment_projection_checkpoint, :stop], _,
                              %{result: :ok, shard_index: 0}},
                             5_000

              # Past the short retry delay, the next write runs it.
              Process.sleep(1_100)
              assert :ok = WARaftBackend.write(1, {:put, key1, "v1b", 0})

              assert_receive {:waraft_segment_projection_checkpoint,
                              [:ferricstore, :waraft, :segment_projection_checkpoint, :stop], _,
                              %{result: :ok, shard_index: 1}},
                             5_000
            end
          )
        after
          FerricStore.Instance.cleanup(multi_ctx.name)
        end
      end

      @tag :bounded_checkpoint
      test "trim past the deferral bound rebuilds the projection synchronously", %{
        ctx: ctx,
        root: root
      } do
        with_checkpoint_env([every: :never, min_interval_ms: 0, max_deferred: 1], fn ->
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          for i <- 1..3 do
            assert :ok = WARaftBackend.write(0, {:put, "defer-bound:k#{i}", "v#{i}", 0})
          end

          log = waraft_segment_log_record(0)
          trim_index = :ferricstore_waraft_spike_segment_log.last_index(log)
          assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, trim_index, %{})

          refute_receive {:waraft_segment_projection_trim,
                          [:ferricstore, :waraft, :segment_projection_trim, :deferred], _, _},
                         100

          refute segment_row?(ctx, "defer-bound:k1")
          assert "v1" == Router.get(ctx, "defer-bound:k1")
        end)
      end

      defp with_checkpoint_env(settings, fun) do
        parent = self()
        handler_id = {__MODULE__, :trim_without_stall, make_ref()}

        :telemetry.attach_many(
          handler_id,
          [
            [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
            [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse],
            [:ferricstore, :waraft, :segment_projection_trim, :deferred]
          ],
          &__MODULE__.forward_trim_without_stall_telemetry/4,
          parent
        )

        previous = Map.new(@checkpoint_envs, &{&1, Application.get_env(:ferricstore, &1)})

        Application.put_env(
          :ferricstore,
          :waraft_segment_projection_checkpoint_every,
          Keyword.fetch!(settings, :every)
        )

        Application.put_env(
          :ferricstore,
          :waraft_segment_projection_checkpoint_min_interval_ms,
          Keyword.fetch!(settings, :min_interval_ms)
        )

        Application.put_env(
          :ferricstore,
          :waraft_segment_projection_trim_max_deferred_entries,
          Keyword.fetch!(settings, :max_deferred)
        )

        case Keyword.fetch(settings, :hook) do
          {:ok, hook} ->
            Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_hook, hook)

          :error ->
            Application.delete_env(:ferricstore, :waraft_segment_projection_checkpoint_hook)
        end

        try do
          fun.()
        after
          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, :never)
          WARaftBackend.stop()
          Enum.each(previous, fn {key, value} -> restore_env(key, value) end)
          :telemetry.detach(handler_id)
        end
      end

      def forward_trim_without_stall_telemetry(
            [:ferricstore, :waraft, :segment_projection_checkpoint, :stop] = event,
            measurements,
            metadata,
            parent
          ),
          do: send(parent, {:waraft_segment_projection_checkpoint, event, measurements, metadata})

      def forward_trim_without_stall_telemetry(event, measurements, metadata, parent),
        do: send(parent, {:waraft_segment_projection_trim, event, measurements, metadata})

      # Takes one background checkpoint with the given write and returns its index.
      defp run_checkpoint!(shard_index, command) do
        Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1)
        assert :ok = WARaftBackend.write(shard_index, command)

        assert_receive {:waraft_segment_projection_checkpoint,
                        [:ferricstore, :waraft, :segment_projection_checkpoint, :stop], _,
                        %{result: :ok, position: {:raft_log_pos, index, _}}},
                       5_000

        Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, :never)
        index
      end

      defp applied_position(shard_index),
        do: Keyword.fetch!(waraft_storage_status(shard_index), :applied_position)

      defp shard_waraft_root(root),
        do: Path.join([root, "waraft", "ferricstore_waraft_backend.1"])

      defp checkpoint_root(root),
        do: Path.join(shard_waraft_root(root), "segment_projection_checkpoint_log")

      defp projection_root(root), do: Path.join(shard_waraft_root(root), "segment_projection_log")

      defp projection_keys(dir) do
        {:ok, {position, keys}} =
          :ferricstore_waraft_spike_segment_log.fold_disk(
            to_charlist(dir),
            fn
              0, {0, {:ferricstore_segment_projection_header, position, _count}}, {_, keys} ->
                {position, keys}

              _index,
              {0, {:ferricstore_segment_projection_entry, key, _value, _expire}},
              {position, keys} ->
                {position, [key | keys]}
            end,
            {nil, []}
          )

        {position, keys}
      end

      # Projection roots keep their segment files in a segment_log directory.
      defp file_inodes(root) do
        dir = Path.join(root, "segment_log")

        dir
        |> File.ls!()
        |> Enum.map(&{&1, File.stat!(Path.join(dir, &1))})
        |> Enum.filter(fn {_name, stat} -> stat.type == :regular end)
        |> Map.new(fn {name, stat} -> {name, stat.inode} end)
      end

      defp segment_row?(ctx, key) do
        case :ets.lookup(elem(ctx.keydir_refs, 0), key) do
          [{^key, _value, _expire, _lfu, {:waraft_segment, _index}, _offset, _size}] -> true
          _other -> false
        end
      end
    end
  end
end
