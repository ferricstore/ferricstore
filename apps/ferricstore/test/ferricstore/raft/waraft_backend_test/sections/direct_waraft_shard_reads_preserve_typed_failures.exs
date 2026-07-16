defmodule Ferricstore.Raft.WARaftBackendTest.Sections.DirectWaraftShardReadsPreserveTypedFailures do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      test "direct WARaft shard reads preserve typed storage failures", %{ctx: ctx} do
        assert :ok =
                 Ferricstore.Raft.WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        good_key = "direct-waraft-read:good"
        bad_key = "direct-waraft-read:bad"
        bad_entry = {bad_key, nil, 0, Ferricstore.Store.LFU.initial(), :invalid, 0, 1}
        keydir = elem(ctx.keydir_refs, 0)

        :ets.insert(keydir, {good_key, "good", 0, Ferricstore.Store.LFU.initial(), :hot, 0, 4})
        :ets.insert(keydir, bad_entry)

        expected =
          Ferricstore.Store.ReadResult.failure({:invalid_keydir_entry, bad_entry})

        assert ^expected = Ferricstore.Store.Router.read_shard_value(ctx, 0, bad_key)
        assert ^expected = Ferricstore.Store.Router.read_shard_values(ctx, 0, [good_key, bad_key])
      end

      @tag :explicit_waraft_shard_read
      test "direct WARaft shard reads honor the explicitly selected shard" do
        assert :ok = Ferricstore.Raft.WARaftBackend.stop()
        ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 2)

        context_key =
          {{Ferricstore.Raft.WARaftBackend, :context}, :ferricstore_waraft_backend}

        previous_context = :persistent_term.get(context_key, :missing)

        on_exit(fn ->
          case previous_context do
            :missing -> :persistent_term.erase(context_key)
            previous -> :persistent_term.put(context_key, previous)
          end

          Ferricstore.Test.IsolatedInstance.checkin(ctx)
        end)

        :persistent_term.put(context_key, ctx)

        off_shard_key =
          Enum.find_value(1..10_000, fn suffix ->
            key = "explicit-shard-read:#{suffix}"
            if Ferricstore.Store.Router.shard_for(ctx, key) == 1, do: key
          end)

        bad_entry =
          {off_shard_key, nil, 0, Ferricstore.Store.LFU.initial(), :invalid, 0, 1}

        :ets.insert(elem(ctx.keydir_refs, 0), bad_entry)

        expected =
          Ferricstore.Store.ReadResult.failure({:invalid_keydir_entry, bad_entry})

        assert ^expected =
                 Ferricstore.Store.Router.read_shard_value(ctx, 0, off_shard_key)

        assert ^expected =
                 Ferricstore.Store.Router.read_shard_values(ctx, 0, [off_shard_key])
      end
    end
  end
end
