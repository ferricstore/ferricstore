defmodule Ferricstore.Cluster.ManagerNodeLifecycleTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Cluster.Manager

  setup do
    Process.put(:ferricstore_cluster_manager_established_cluster_hook, fn _shard_count ->
      false
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_cluster_manager_established_cluster_hook)
    end)

    :ok
  end

  test "configured node is not marked joined before membership succeeds" do
    target = :configured_join_target@localhost
    state = state(cluster_nodes: [target])

    assert {:noreply, next_state} = Manager.handle_info({:nodeup, target, []}, state)

    refute MapSet.member?(next_state.known_nodes, target)
    assert next_state.mode == :cluster
  end

  test "a repeated nodedown cancels the superseded removal timer" do
    target = :duplicate_nodedown_target@localhost
    state = state(remove_delay_ms: 60_000)

    assert {:noreply, first_state} = Manager.handle_info({:nodedown, target, []}, state)
    first_timer = timer_ref(first_state.remove_timers[target])
    assert is_integer(Process.read_timer(first_timer))

    assert {:noreply, second_state} =
             Manager.handle_info({:nodedown, target, []}, first_state)

    assert Process.read_timer(first_timer) == false
    Process.cancel_timer(timer_ref(second_state.remove_timers[target]))
  end

  test "failed delayed removal remains known and is retried" do
    target = :failed_delayed_remove_target@localhost
    parent = self()

    Process.put(:ferricstore_cluster_manager_do_remove_node_hook, fn ^target, _state ->
      send(parent, :remove_attempted)
      {:error, :membership_unavailable}
    end)

    token = make_ref()
    expired_timer = Process.send_after(self(), :ignore_expired_timer, 60_000)

    state =
      state(
        known_nodes: MapSet.new([target]),
        remove_timers: %{target => {expired_timer, token}}
      )

    assert {:noreply, next_state} =
             Manager.handle_info({:remove_timeout, target, token}, state)

    assert_receive :remove_attempted
    assert MapSet.member?(next_state.known_nodes, target)
    assert Map.has_key?(next_state.remove_timers, target)

    Process.cancel_timer(expired_timer)
    Process.cancel_timer(timer_ref(next_state.remove_timers[target]))
    Process.delete(:ferricstore_cluster_manager_do_remove_node_hook)
  end

  defp state(overrides) do
    Map.merge(
      %{
        mode: :cluster,
        role: :voter,
        cluster_nodes: [],
        remove_delay_ms: 60_000,
        known_nodes: MapSet.new(),
        remove_timers: %{},
        sync_status: :synced,
        shard_sync_status: %{},
        shard_count: 1
      },
      Map.new(overrides)
    )
  end

  defp timer_ref({ref, _token}), do: ref
  defp timer_ref(ref), do: ref
end
