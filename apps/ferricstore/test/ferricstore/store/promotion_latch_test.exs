defmodule Ferricstore.Store.PromotionLatchTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ferricstore.Store.Promotion

  setup do
    original = Application.get_env(:ferricstore, :promotion_compaction_latch_timeout_ms)

    on_exit(fn ->
      case original do
        nil -> Application.delete_env(:ferricstore, :promotion_compaction_latch_timeout_ms)
        value -> Application.put_env(:ferricstore, :promotion_compaction_latch_timeout_ms, value)
      end
    end)

    :ok
  end

  test "await_compaction_latch times out with telemetry when owner stays alive" do
    Application.put_env(:ferricstore, :promotion_compaction_latch_timeout_ms, 5)

    tab = :ets.new(:promotion_latch_timeout, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}
    redis_key = "promotion_latch_timeout"
    latch_key = {:promoted_compaction, redis_key}
    holder = spawn(fn -> Process.sleep(:infinity) end)
    handler_id = {:promotion_latch_timeout, self(), make_ref()}
    test_pid = self()
    original_trap_exit = Process.flag(:trap_exit, true)

    :ets.insert(tab, {latch_key, holder})

    :telemetry.attach(
      handler_id,
      [:ferricstore, :promotion, :compaction_latch],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:promotion_latch_telemetry, event, measurements, metadata})
      end,
      nil
    )

    try do
      log =
        capture_log(fn ->
          task = Task.async(fn -> Promotion.await_compaction_latch(owner, redis_key) end)

          assert {:exit, {%RuntimeError{message: message}, _stack}} = Task.yield(task, 500)
          assert message =~ "compaction latch timeout"

          assert_receive {:promotion_latch_telemetry,
                          [:ferricstore, :promotion, :compaction_latch], %{wait_ms: wait_ms},
                          %{status: :timeout, shard_index: 0}},
                         1_000

          assert wait_ms >= 5
        end)

      assert log =~ "Promoted compaction latch timeout"
      assert log =~ inspect(latch_key)
    after
      Process.flag(:trap_exit, original_trap_exit)
      :telemetry.detach(handler_id)
      Process.exit(holder, :kill)
      :ets.delete(tab)
    end
  end
end
