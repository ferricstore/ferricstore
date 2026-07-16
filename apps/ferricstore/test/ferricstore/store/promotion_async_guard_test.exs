defmodule Ferricstore.Store.PromotionAsyncGuardTest do
  use ExUnit.Case, async: true

  @promotion_path Path.expand("../../../lib/ferricstore/store/promotion.ex", __DIR__)
  @state_machine_path Path.expand("../../../lib/ferricstore/raft/state_machine.ex", __DIR__)
  @state_machine_sections Path.expand(
                            "../../../lib/ferricstore/raft/state_machine/sections",
                            __DIR__
                          )
  @promoted_path Path.expand(
                   "../../../lib/ferricstore/store/shard/compound/promoted.ex",
                   __DIR__
                 )
  @shard_info_path Path.expand("../../../lib/ferricstore/store/shard/info.ex", __DIR__)
  @shard_flush_path Path.expand("../../../lib/ferricstore/store/shard/flush.ex", __DIR__)

  test "promotion recovery and compaction avoid blocking pread" do
    source = File.read!(@promotion_path)

    # Promotion recovery/compaction can scan many large cold entries. Keep those
    # reads async and keyed, so stale ETS offsets cannot promote another key's
    # value under the promoted collection.
    assert source =~ "ColdRead.pread_keyed(path, offset, key,",
           "expected promotion cold reads to use keyed ColdRead.pread_keyed/4"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected promotion cold reads to avoid blocking v2_pread_at/2"
  end

  test "replicated apply never runs collection layout migration" do
    source =
      [@state_machine_path | Path.wildcard(Path.join(@state_machine_sections, "*.ex"))]
      |> Enum.map_join("\n", &File.read!/1)

    refute source =~ "Promotion.promote_collection!",
           "replicated apply must not scan keydir, cold-read, and fsync a promotion inline"

    assert source =~ "dispatch_pending_compound_promotions(state)",
           "committed writes must hand promotion work to the shard after flush"
  end

  test "shard promotion is monitored worker work backed by the exact member index" do
    promoted_source = File.read!(@promoted_path)
    info_source = File.read!(@shard_info_path)

    maybe_promote_body =
      promoted_source
      |> String.split("def maybe_promote(state, redis_key, compound_key) do", parts: 2)
      |> List.last()
      |> String.split("def detect_compound_type", parts: 2)
      |> hd()

    refute maybe_promote_body =~ "Promotion.promote_collection!",
           "the shard mailbox must not cold-read and fsync a promotion inline"

    assert maybe_promote_body =~ "start_compound_promotion",
           "the threshold path must enqueue monitored promotion work"

    assert info_source =~ "spawn_compound_promotion_worker",
           "promotion needs a monitored worker lifecycle"

    assert info_source =~ "state.compound_member_index",
           "the worker must enumerate only the target collection's exact catalog"
  end

  test "live promotion failure is fail-closed and cannot race active-file rotation" do
    info_source = File.read!(@shard_info_path)
    flush_source = File.read!(@shard_flush_path)

    assert flush_source =~
             "def maybe_rotate_file(%{compound_promotion_worker: worker} = state)",
           "the active file captured by a promotion worker must not rotate underneath it"

    assert flush_source =~ "Promotion.try_acquire_shared_log_latch(state)",
           "Raft-owned rotation must share the promotion log latch"

    assert info_source =~ "Promotion.acquire_shared_log_latch(state)",
           "the promotion worker must hold the shard log latch before capturing its active path"

    assert info_source =~ "refresh_active_file_size_after_compound_promotion",
           "worker appends must be reconciled into shard file-size accounting"

    assert info_source =~ "{:stop, {:compound_promotion_failed, reason}, state}",
           "a reported partial promotion failure must restart through durable recovery"

    assert info_source =~ "{:stop, {:compound_promotion_worker_failed, reason}, state}",
           "an abnormal worker exit must restart through durable recovery"

    refute info_source =~ "defp finish_compound_promotion(state, worker, {:error, reason})",
           "a partial promotion must never be installed as a live promoted store"
  end
end
