defmodule Ferricstore.Store.PromotionTest.Sections.PromotionBatchesSharedTombstonesAfterDedicatedCopy do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog
      alias Ferricstore.Commands.{Hash, List, Set, SortedSet, Strings}
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.HLC
      alias Ferricstore.Store.{CompoundKey, Promotion, Router}
      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Test.ShardHelpers

      test "promotion batches shared tombstones after dedicated copy" do
        source =
          __DIR__
          |> Path.join("../../../lib/ferricstore/store/promotion.ex")
          |> File.read!()

        [_, tombstone_section] =
          String.split(source, "# Step 3: tombstone compound keys in shared log", parts: 2)

        [tombstone_section, _] = String.split(tombstone_section, "Logger.info(", parts: 2)

        assert tombstone_section =~ "v2_append_ops_batch(active_path, tombstone_ops)"
        refute tombstone_section =~ "v2_append_ops_batch_nosync(active_path, tombstone_ops)"
        refute tombstone_section =~ "v2_fsync(active_path)"
      end

      test "promoted recovery reports leftover compact temp cleanup failures" do
        root =
          Path.join(
            System.tmp_dir!(),
            "promotion_compact_cleanup_fail_#{System.unique_integer([:positive])}"
          )

        keydir =
          :ets.new(:"promotion_compact_cleanup_fail_#{System.unique_integer([:positive])}", [
            :set,
            :public
          ])

        on_exit(fn ->
          try do
            :ets.delete(keydir)
          rescue
            ArgumentError -> :ok
          end

          File.rm_rf(root)
        end)

        Ferricstore.DataDir.ensure_layout!(root, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
        File.touch!(Path.join(shard_path, "00000.log"))
        redis_key = "recover-temp-cleanup-fail"
        marker_key = Promotion.marker_key(redis_key)
        dedicated_path = Promotion.dedicated_path(root, 0, :hash, redis_key)

        File.mkdir_p!(dedicated_path)
        File.touch!(Path.join(dedicated_path, "00000.log"))
        File.mkdir!(Path.join(dedicated_path, "compact_1.log"))

        :ets.insert(keydir, {marker_key, "hash", 0, Ferricstore.Store.LFU.initial(), 0, 0, 4})

        shared_key = CompoundKey.hash_field(redis_key, "field")

        :ets.insert(
          keydir,
          {shared_key, "shared", 0, Ferricstore.Store.LFU.initial(), 0, 0, 6}
        )

        type_key = CompoundKey.type_key(redis_key)

        :ets.insert(
          keydir,
          {type_key, "hash", 0, Ferricstore.Store.LFU.initial(), 0, 0, 4}
        )

        compact_dir = Path.join(dedicated_path, "compact_1.log")
        parent = self()
        handler_id = {:promotion_compact_temp_cleanup_failed, parent, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :promotion, :compact_temp_cleanup_failed],
          fn event, measurements, metadata, _config ->
            send(parent, {:compact_temp_cleanup_failed, event, measurements, metadata})
          end,
          nil
        )

        try do
          log =
            capture_log(fn ->
              assert %{} = Promotion.recover_promoted(shard_path, keydir, root, 0)
            end)

          assert log =~
                   "Promotion recovery: failed to remove leftover compact temp file compact_1.log"

          assert_receive {:compact_temp_cleanup_failed,
                          [:ferricstore, :promotion, :compact_temp_cleanup_failed], %{count: 1},
                          %{path: ^compact_dir, name: "compact_1.log", reason: {_kind, _message}}},
                         1_000
        after
          :telemetry.detach(handler_id)
        end
      end

      # Builds a store map backed by the real Router with promotion-aware
      # compound callbacks -- the same as what the connection module builds.
    end
  end
end
