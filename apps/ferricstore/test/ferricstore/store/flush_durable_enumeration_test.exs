defmodule Ferricstore.Store.FlushDurableEnumerationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Ops.Flush
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard

  test "FLUSHDB durably tombstones expired rows omitted by live key enumeration" do
    data_dir =
      Path.join(System.tmp_dir!(), "flush_durable_#{System.unique_integer([:positive])}")

    name = :"flush_durable_#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.build(name, data_dir: data_dir, shard_count: 1)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn -> cleanup(ctx, data_dir) end)

    {:ok, pid} = start_shard(ctx, data_dir)
    key = "flush:expired:#{System.unique_integer([:positive])}"
    expired_at_ms = Ferricstore.HLC.now_ms() - 1
    orphaned_dedicated = Path.join([data_dir, "dedicated", "shard_0", "hash:orphaned"])

    File.mkdir_p!(orphaned_dedicated)
    File.write!(Path.join(orphaned_dedicated, "00000.log"), "stale")

    assert :ok = Router.put(ctx, key, "durable", expired_at_ms)
    assert :ok = Flush.flush(ctx)
    assert [] == :ets.lookup(elem(ctx.keydir_refs, 0), key)
    refute File.exists?(orphaned_dedicated)

    Process.unlink(pid)
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 5_000

    {:ok, _restarted_pid} = start_shard(ctx, data_dir)

    assert [] == :ets.lookup(elem(ctx.keydir_refs, 0), key)
  end

  test "FLUSHDB uses constant-size replicated shard resets behind an all-shard gate" do
    source =
      Path.expand("../../../lib/ferricstore/store/ops/flush.ex", __DIR__)
      |> File.read!()

    assert source =~ "pause_writes_for_sync_all"
    assert source =~ "write_flush_shard_paused"
    assert source =~ "{:flush_shard_paused, flush_epoch}"
    refute source =~ "defp flush_storage_logical_keys"
    refute source =~ "defp flush_internal_keys"
    refute source =~ "defp flush_key"
    refute source =~ "defp clear_flow_projection_storage"
    refute source =~ "defp clear_promoted_storage"
  end

  test "distributed FLUSHDB targets the union of Raft participants by shard" do
    node_a = :"flush-a@127.0.0.1"
    node_b = :"flush-b@127.0.0.1"
    node_c = :"flush-c@127.0.0.1"

    statuses = %{
      0 => [
        config_index: 10,
        config: %{
          version: 1,
          membership: [s0_a: node_a],
          participants: [s0_a: node_a, s0_b: node_b],
          witness: []
        }
      ],
      1 => [
        config_index: 11,
        config: %{
          version: 1,
          membership: [s1_b: node_b],
          participants: [s1_b: node_b, s1_c: node_c],
          witness: []
        }
      ]
    }

    assert {:ok,
            %{
              ^node_a => [0],
              ^node_b => [0, 1],
              ^node_c => [1]
            }} = Flush.__durable_cleanup_targets_for_test__(statuses)
  end

  test "distributed FLUSHDB fails closed when shard membership is unavailable" do
    assert {:error, {:flush_membership_unavailable, 0, {:missing_config, nil}}} =
             Flush.__durable_cleanup_targets_for_test__(%{0 => [state: :leader]})
  end

  test "distributed FLUSHDB rejects sentinel and malformed participant identities" do
    statuses = %{
      0 => [
        config_index: 10,
        config: %{
          version: 1,
          membership: [s0: :"valid@127.0.0.1"],
          participants: [s0: :"valid@127.0.0.1", invalid: false],
          witness: []
        }
      ]
    }

    assert {:error, {:flush_membership_unavailable, 0, {:invalid_participant, {:invalid, false}}}} =
             Flush.__durable_cleanup_targets_for_test__(statuses)
  end

  test "distributed FLUSHDB pauses the initiating node even when it is not a participant" do
    origin = :"router-only@127.0.0.1"
    participant = :"storage@127.0.0.1"

    statuses = %{
      0 => [
        config_index: 10,
        config: %{
          version: 1,
          membership: [s0: participant],
          participants: [s0: participant],
          witness: []
        }
      ]
    }

    assert {:ok,
            %{
              cleanup_targets: %{^participant => [0]},
              pause_nodes: [^origin, ^participant]
            }} = Flush.__durable_pause_plan_for_test__(statuses, origin)
  end

  test "distributed FLUSHDB fails closed when membership changes after pausing" do
    node_a = :"flush-a@127.0.0.1"
    node_b = :"flush-b@127.0.0.1"

    before = %{
      0 => [
        config_index: 10,
        config: %{
          version: 1,
          membership: [s0_a: node_a],
          participants: [s0_a: node_a],
          witness: []
        }
      ]
    }

    after_reconfiguration = %{
      0 => [
        config_index: 11,
        config: %{
          version: 1,
          membership: [s0_a: node_a, s0_b: node_b],
          participants: [s0_a: node_a, s0_b: node_b],
          witness: []
        }
      ]
    }

    assert {:error, {:flush_membership_changed, 0, _before, _after}} =
             Flush.__verify_durable_membership_for_test__(before, after_reconfiguration)
  end

  test "distributed FLUSHDB runs every local cleanup and aggregates failures" do
    parent = self()
    node_a = :"cleanup-a@127.0.0.1"
    node_b = :"cleanup-b@127.0.0.1"

    assert {:error, {:flush_local_cleanup_failed, [{^node_b, {:error, :forced_cleanup_failure}}]}} =
             Flush.__run_cleanup_targets_for_test__(
               %{node_a => %{0 => {:raft_log_pos, 10, 2}}, node_b => %{}},
               fn target_node, positions ->
                 send(parent, {:cleanup_called, target_node, positions})

                 if target_node == node_b,
                   do: {:error, :forced_cleanup_failure},
                   else: :ok
               end
             )

    assert_received {:cleanup_called, ^node_a, %{0 => {:raft_log_pos, 10, 2}}}
    assert_received {:cleanup_called, ^node_b, %{}}
  end

  test "FLUSHDB restores mirror health after finalizing the empty projection" do
    data_dir =
      Path.join(System.tmp_dir!(), "flush_mirror_health_#{System.unique_integer([:positive])}")

    name = :"flush_mirror_health_#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.build(name, data_dir: data_dir, shard_count: 1)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn -> cleanup(ctx, data_dir) end)

    {:ok, _pid} = start_shard(ctx, data_dir)
    :atomics.put(ctx.flow_lmdb_mirror_degraded, 1, 1)

    assert :ok = Flush.flush(ctx)
    assert :atomics.get(ctx.flow_lmdb_mirror_degraded, 1) == 0
  end

  test "standalone FLUSHDB failure leaves writes fail-closed" do
    data_dir =
      Path.join(System.tmp_dir!(), "flush_fail_closed_#{System.unique_integer([:positive])}")

    name = :"flush_fail_closed_#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.build(name, data_dir: data_dir, shard_count: 1)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn -> cleanup(ctx, data_dir) end)

    {:ok, shard_pid} = start_shard(ctx, data_dir)
    key = "flush-fail-closed"
    assert :ok = Router.put(ctx, key, "value", 0)

    Application.put_env(
      :ferricstore,
      :flush_derived_lmdb_clear_hook,
      fn _path -> {:error, :forced_lmdb_clear_failure} end
    )

    failure =
      {:flush_shard_apply_failed,
       {:flush_derived_state_cleanup_failed, {:lmdb_clear_failed, :forced_lmdb_clear_failure}}}

    try do
      assert {:error, {:flush_shard_failed, 0, ^failure}} = Flush.flush(ctx)
      assert %{last_flush_error: ^failure, writes_paused: true} = :sys.get_state(shard_pid)

      assert {:error, "ERR shard writes paused for sync"} =
               GenServer.call(shard_pid, {:put, "blocked-after-flush", "value", 0})
    after
      Application.delete_env(:ferricstore, :flush_derived_lmdb_clear_hook)
    end
  end

  test "FLUSHDB caller death after pause releases the standalone write lease" do
    data_dir =
      Path.join(System.tmp_dir!(), "flush_owner_death_#{System.unique_integer([:positive])}")

    name = :"flush_owner_death_#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.build(name, data_dir: data_dir, shard_count: 1)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn -> cleanup(ctx, data_dir) end)

    {:ok, shard_pid} = start_shard(ctx, data_dir)

    latch_token =
      Ferricstore.Store.Promotion.acquire_shared_log_latch(%{
        instance_ctx: ctx,
        index: 0
      })

    try do
      {flush_pid, flush_monitor} =
        spawn_monitor(fn ->
          Flush.flush(ctx)
        end)

      assert eventually(fn -> waiting_on_promotion_latch?(shard_pid) end)
      assert Process.alive?(flush_pid)

      Process.exit(flush_pid, :kill)
      assert_receive {:DOWN, ^flush_monitor, :process, ^flush_pid, :killed}, 2_000
    after
      Ferricstore.Store.Promotion.release_compaction_latch(latch_token)
    end

    assert eventually(fn -> not :sys.get_state(shard_pid).writes_paused end)
    assert :ok = GenServer.call(shard_pid, {:put, "flush-owner-death", "value", 0})
    assert "value" == GenServer.call(shard_pid, {:get, "flush-owner-death"})
  end

  defp start_shard(ctx, data_dir) do
    Shard.start_link(
      index: 0,
      data_dir: data_dir,
      instance_ctx: ctx,
      flow_shared_ref_backfill?: false
    )
  end

  defp cleanup(ctx, data_dir) do
    case Process.whereis(elem(ctx.shard_names, 0)) do
      pid when is_pid(pid) -> GenServer.stop(pid, :normal, 5_000)
      nil -> :ok
    end

    FerricStore.Instance.cleanup(ctx.name)
    File.rm_rf!(data_dir)
  catch
    :exit, _reason -> :ok
  end

  defp eventually(fun, attempts \\ 100)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false

  defp waiting_on_promotion_latch?(pid) do
    case Process.info(pid, :current_stacktrace) do
      {:current_stacktrace, stacktrace} ->
        Enum.any?(stacktrace, fn
          {Ferricstore.Store.Promotion, function, _arity, _location}
          when function in [
                 :acquire_compaction_latch,
                 :wait_compaction_latch_clear!,
                 :do_wait_compaction_latch_clear
               ] ->
            true

          _frame ->
            false
        end)

      _unavailable ->
        false
    end
  end
end
