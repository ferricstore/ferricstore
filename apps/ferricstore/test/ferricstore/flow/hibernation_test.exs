defmodule Ferricstore.Flow.HibernationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Hibernation, Locator}

  test "demotable requires far-future waiting unleased non-terminal flow" do
    now = 1_000

    record = %{
      id: "flow-1",
      type: "email",
      state: "waiting",
      run_state: "waiting",
      next_run_at_ms: now + 10 * 60 * 1_000,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0
    }

    assert Hibernation.demotable?(record, now, hot_window_ms: 60_000, safety_margin_ms: 60_000)
    refute Hibernation.demotable?(%{record | next_run_at_ms: now + 30_000}, now)
    refute Hibernation.demotable?(%{record | lease_owner: "worker"}, now)
    refute Hibernation.demotable?(Map.put(record, :terminal_retention_until_ms, now + 1_000), now)
  end

  test "default demotion threshold parks only flows due more than five minutes away" do
    now = 1_000

    record = %{
      id: "flow-1",
      type: "email",
      state: "waiting",
      run_state: "waiting",
      next_run_at_ms: now + 5 * 60 * 1_000,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0
    }

    assert Hibernation.enabled?()
    assert Hibernation.hot_window_ms() == 5 * 60 * 1_000
    assert Hibernation.safety_margin_ms() == 0
    refute Hibernation.demotable?(record, now)
    assert Hibernation.demotable?(%{record | next_run_at_ms: now + 5 * 60 * 1_000 + 1}, now)
  end

  test "enabled can be disabled from runtime config without recompiling" do
    previous = Application.get_env(:ferricstore, :flow_hibernation_enabled)

    try do
      Application.put_env(:ferricstore, :flow_hibernation_enabled, false)
      refute Hibernation.refresh_config!()
      refute Hibernation.enabled?()

      Application.put_env(:ferricstore, :flow_hibernation_enabled, true)
      assert Hibernation.refresh_config!()
      assert Hibernation.enabled?()
    after
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_hibernation_enabled)
        value -> Application.put_env(:ferricstore, :flow_hibernation_enabled, value)
      end

      Hibernation.refresh_config!()
    end
  end

  test "demotion writes cold rows before attempting hot eviction" do
    test_pid = self()
    candidate = candidate()

    result =
      Hibernation.demote_candidates([candidate],
        write_cold_fun: fn ops ->
          send(test_pid, {:cold_written, ops})
          :ok
        end,
        evict_hot_fun: fn locator ->
          assert_receive {:cold_written, _ops}
          send(test_pid, {:hot_evicted, locator})
          :ok
        end
      )

    assert {:ok, %{attempted: 1, cold_written: 1, hot_evicted: 1, hot_changed: 0}} = result
    assert_receive {:hot_evicted, %Locator{}}
  end

  test "demotion does not evict hot locator when cold write fails" do
    test_pid = self()

    result =
      Hibernation.demote_candidates([candidate()],
        write_cold_fun: fn _ops -> {:error, :lmdb_down} end,
        evict_hot_fun: fn _locator ->
          send(test_pid, :unexpected_evict)
          :ok
        end
      )

    assert {:error, :lmdb_down, %{attempted: 1, cold_written: 0, hot_evicted: 0}} = result
    refute_receive :unexpected_evict
  end

  test "demotion reports changed hot locator without treating it as evicted" do
    result =
      Hibernation.demote_candidates([candidate()],
        write_cold_fun: fn _ops -> :ok end,
        evict_hot_fun: fn _locator -> {:error, :changed} end
      )

    assert {:ok, %{attempted: 1, cold_written: 1, hot_evicted: 0, hot_changed: 1}} = result
  end

  test "demotion ops include park row due row and reverse segment row" do
    ops = Hibernation.demotion_ops(candidate())
    park_key = Ferricstore.Flow.LMDB.cold_park_key_for_state_key("flow/state/tenant-1/flow-1")

    assert length(ops) == 3
    assert Enum.any?(ops, &match?({:put, ^park_key, _value}, &1))

    assert Enum.any?(ops, fn
             {:put, key, ^park_key} -> String.starts_with?(key, "flow:due:v1:")
             _ -> false
           end)

    assert Enum.any?(ops, fn {:put, key, _value} ->
             String.starts_with?(key, "flow:cold:by-segment:v1:")
           end)

    assert Enum.any?(ops, &match?({:put, _reverse_key, ^park_key}, &1))
  end

  test "rebuild cold ops reconstructs only demotable far-future flows from durable candidates" do
    now = 1_000
    far = candidate()
    soon = put_in(candidate(), [:record, :next_run_at_ms], now + 30_000)
    terminal = put_in(candidate(), [:record, :terminal_retention_until_ms], now + 60_000)

    ops =
      Hibernation.rebuild_cold_ops([far, soon, terminal], now,
        hot_window_ms: 60_000,
        safety_margin_ms: 60_000
      )

    assert length(ops) == 3
    assert Enum.any?(ops, fn {:put, key, _value} -> String.starts_with?(key, "flow:park:v1:") end)
    assert Enum.any?(ops, fn {:put, key, _value} -> String.starts_with?(key, "flow:due:v1:") end)
  end

  test "promotion bucket prefixes cover current bucket through lookahead horizon" do
    assert Hibernation.promotion_bucket_prefixes(61_000, 181_000, 60_000) == [
             "flow:due:v1:00000000000000060000",
             "flow:due:v1:00000000000000120000",
             "flow:due:v1:00000000000000180000"
           ]
  end

  test "hot index keys cover lifecycle due any running and metadata indexes" do
    record =
      candidate().record
      |> Map.merge(%{
        state: "running",
        lease_owner: "worker-1",
        parent_flow_id: "parent-1",
        root_flow_id: "root-1",
        correlation_id: "corr-1"
      })

    keys = Hibernation.hot_index_keys(record)

    assert Enum.any?(keys, &String.contains?(&1, ":s:"))
    assert Enum.any?(keys, &String.contains?(&1, ":d:"))
    assert Enum.any?(keys, &String.contains?(&1, ":da:"))
    assert Enum.any?(keys, &String.contains?(&1, ":i:"))
    assert Enum.any?(keys, &String.contains?(&1, ":w:"))
    assert Enum.any?(keys, &String.contains?(&1, ":p:"))
    assert Enum.any?(keys, &String.contains?(&1, ":r:"))
    assert Enum.any?(keys, &String.contains?(&1, ":c:"))
  end

  test "promotion reads cold locator validates state installs hot then cleans cold rows" do
    test_pid = self()
    row = promotion_row()

    result =
      Hibernation.promote_candidates([row],
        read_state_fun: fn locator ->
          send(test_pid, {:read, locator})
          {:ok, %{id: "flow-1", version: 1, next_run_at_ms: 900_000, run_state: "waiting"}}
        end,
        install_hot_fun: fn locator, record ->
          assert_receive {:read, ^locator}
          send(test_pid, {:installed, locator, record})
          :ok
        end,
        cleanup_cold_fun: fn ops ->
          assert_receive {:installed, _locator, _record}
          send(test_pid, {:cleanup, ops})
          :ok
        end
      )

    assert {:ok, %{attempted: 1, read: 1, installed: 1, stale: 0, failed: 0}} = result
    assert_receive {:cleanup, cleanup_ops}
    assert Enum.any?(cleanup_ops, &match?({:delete, "flow:park:v1:key:abc"}, &1))
    assert Enum.any?(cleanup_ops, &match?({:delete, "due-key"}, &1))
  end

  test "promotion skips stale durable record and cleans stale cold rows" do
    test_pid = self()

    result =
      Hibernation.promote_candidates([promotion_row()],
        read_state_fun: fn _locator ->
          {:ok, %{id: "flow-1", version: 2, next_run_at_ms: 901_000, run_state: "waiting"}}
        end,
        install_hot_fun: fn _locator, _record ->
          send(test_pid, :unexpected_install)
          :ok
        end,
        cleanup_cold_fun: fn ops ->
          send(test_pid, {:cleanup, ops})
          :ok
        end
      )

    assert {:ok, %{attempted: 1, read: 1, installed: 0, stale: 1, failed: 0}} = result
    refute_receive :unexpected_install
    assert_receive {:cleanup, [_ | _]}
  end

  test "promotion does not clean cold row when durable read fails" do
    test_pid = self()

    result =
      Hibernation.promote_candidates([promotion_row()],
        read_state_fun: fn _locator -> {:error, :enoent} end,
        install_hot_fun: fn _locator, _record ->
          send(test_pid, :unexpected_install)
          :ok
        end,
        cleanup_cold_fun: fn ops ->
          send(test_pid, {:cleanup, ops})
          :ok
        end
      )

    assert {:ok, %{attempted: 1, read: 0, installed: 0, stale: 0, failed: 1}} = result
    refute_receive :unexpected_install
    assert_receive {:cleanup, []}
  end

  test "fetch_or_promote returns hot record without touching cold path" do
    test_pid = self()

    result =
      Hibernation.fetch_or_promote("flow-1",
        fetch_hot_fun: fn "flow-1" -> {:ok, %{id: "flow-1", hot: true}} end,
        fetch_cold_fun: fn _flow_id ->
          send(test_pid, :unexpected_cold_fetch)
          :not_found
        end,
        read_state_fun: fn _locator -> {:error, :unexpected} end,
        install_hot_fun: fn _locator, _record -> :ok end
      )

    assert {:ok, :hot, %{id: "flow-1", hot: true}} = result
    refute_receive :unexpected_cold_fetch
  end

  test "fetch_or_promote promotes one cold parked flow on hot miss" do
    test_pid = self()
    row = promotion_row()

    result =
      Hibernation.fetch_or_promote("flow-1",
        fetch_hot_fun: fn "flow-1" -> :not_found end,
        fetch_cold_fun: fn "flow-1" -> {:ok, row} end,
        read_state_fun: fn locator ->
          {:ok, %{id: locator.flow_id, version: locator.version, next_run_at_ms: 900_000}}
        end,
        install_hot_fun: fn locator, record ->
          send(test_pid, {:installed, locator, record})
          :ok
        end,
        cleanup_cold_fun: fn ops ->
          send(test_pid, {:cleanup, ops})
          :ok
        end
      )

    assert {:ok, :cold_promoted, %{id: "flow-1", version: 1}} = result
    assert_receive {:installed, %Locator{}, %{id: "flow-1"}}
    assert_receive {:cleanup, [_ | _]}
  end

  test "fetch_or_promote rejects stale cold locator instead of returning false not_found" do
    row = promotion_row()

    result =
      Hibernation.fetch_or_promote("flow-1",
        fetch_hot_fun: fn "flow-1" -> :not_found end,
        fetch_cold_fun: fn "flow-1" -> {:ok, row} end,
        read_state_fun: fn _locator ->
          {:ok, %{id: "flow-1", version: 2, next_run_at_ms: 901_000}}
        end,
        install_hot_fun: fn _locator, _record -> :ok end
      )

    assert {:error, :stale_cold_locator} = result
  end

  test "cold compaction relocation updates reverse segment row and park row" do
    old_row = promotion_row()

    assert {:ok, new_row} =
             Hibernation.relocate_cold_row(old_row,
               file_id: {:flow_state, 2},
               offset: 2_000,
               value_size: 300,
               segment_generation: 5
             )

    assert {:ok, ops} = Hibernation.cold_compaction_ops(old_row, new_row)

    assert {:delete, old_reverse_key} = Enum.at(ops, 0)
    assert {:put, new_reverse_key, "flow:park:v1:key:abc"} = Enum.at(ops, 1)
    assert {:put, "flow:park:v1:key:abc", encoded_park} = Enum.at(ops, 2)
    assert old_reverse_key != new_reverse_key

    assert {:ok, park} = Ferricstore.Flow.LMDB.decode_cold_park(encoded_park)
    assert park.locator == new_row.locator
  end

  test "cold compaction refuses to update locator across logical generation" do
    old_row = promotion_row()
    newer = %{old_row | locator: Locator.relocate!(old_row.locator, offset: 2_000)}
    newer = %{newer | locator: %{newer.locator | version: 2}}

    assert {:error, :logical_generation_mismatch} =
             Hibernation.cold_compaction_ops(old_row, newer)
  end

  test "property model never makes a live flow neither hot nor cold" do
    seed = {101, 202, 303}
    :rand.seed(:exsss, seed)

    initial = %{
      truth: locator(version: 1, raft_index: 1, offset: 1),
      hot: locator(version: 1, raft_index: 1, offset: 1),
      cold: nil
    }

    final =
      Enum.reduce(1..1_000, initial, fn step, model ->
        model
        |> random_model_step(step)
        |> assert_visible_model!()
      end)

    assert {:ok, _source, resolved} = Locator.resolve(final.hot, final.cold)
    assert Locator.compare_generation(resolved, final.truth) in [:eq, :gt]
  end

  defp candidate do
    locator =
      Locator.new!(
        flow_id: "flow-1",
        kind: :state,
        version: 1,
        raft_index: 10,
        file_id: {:flow_state, 0},
        offset: 128,
        value_size: 256,
        checksum: <<1>>
      )

    %{
      locator: locator,
      record: %{
        id: "flow-1",
        state_key: "flow/state/tenant-1/flow-1",
        type: "email",
        state: "waiting",
        run_state: "waiting",
        version: 1,
        next_run_at_ms: 900_000,
        priority: 0,
        partition_key: "tenant-1",
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        fencing_token: 1,
        value_refs: %{"payload" => "ref-1"}
      }
    }
  end

  defp locator(overrides) do
    defaults = [
      flow_id: "flow-1",
      kind: :state,
      version: 1,
      raft_index: 1,
      file_id: {:flow_state, 0},
      offset: 0,
      value_size: 1,
      checksum: <<0>>
    ]

    defaults
    |> Keyword.merge(overrides)
    |> Locator.new!()
  end

  defp random_model_step(model, step) do
    case :rand.uniform(6) do
      1 ->
        next =
          Locator.relocate!(model.truth,
            file_id: {:flow_state, rem(step, 7)},
            offset: step * 10,
            value_size: model.truth.value_size + 1
          )

        next = %{next | version: model.truth.version + 1, raft_index: model.truth.raft_index + 1}
        %{model | truth: next, hot: next}

      2 ->
        if model.hot do
          %{model | cold: model.hot}
        else
          model
        end

      3 ->
        if model.hot && model.cold && Locator.same_physical_record?(model.hot, model.cold) do
          %{model | hot: nil}
        else
          model
        end

      4 ->
        if model.cold && not Locator.stale_for?(model.cold, model.truth) do
          %{model | hot: model.cold}
        else
          model
        end

      5 ->
        cond do
          model.cold && Locator.stale_for?(model.cold, model.truth) ->
            %{model | cold: nil}

          model.cold ->
            {:ok, relocated} =
              Locator.relocate(model.cold,
                file_id: {:flow_state, rem(step, 9)},
                offset: step * 100
              )

            if Locator.same_logical_record?(relocated, model.truth) do
              %{model | cold: relocated, truth: relocated}
            else
              %{model | cold: relocated}
            end

          true ->
            model
        end

      _ ->
        model
    end
  end

  defp assert_visible_model!(model) do
    assert {:ok, _source, resolved} = Locator.resolve(model.hot, model.cold)
    refute Locator.stale_for?(resolved, model.truth)
    model
  end

  defp promotion_row do
    locator = candidate().locator

    %{
      locator: locator,
      park: %{due_at_ms: 900_000},
      park_key: "flow:park:v1:key:abc",
      due_key: "due-key"
    }
  end
end
