defmodule Ferricstore.Flow.LMDBRebuilder.TerminalStateTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBRebuilder.TerminalState

  test "terminal state resolution batches only states missing from the hot keydir" do
    keydir = :ets.new(:terminal_state_batch_keydir, [:set])
    hot = record("hot-terminal", "partition-hot", "completed")
    cold = record("cold-active", "partition-cold", "queued")
    missing_key = Keys.state_key("missing", "partition-missing")
    hot_key = Keys.state_key(hot.id, hot.partition_key)
    cold_key = Keys.state_key(cold.id, cold.partition_key)

    true =
      :ets.insert(keydir, [
        {hot_key, "hot", 0, 0, :hot, 0, 3},
        {Keys.registry_key(cold.id, cold.partition_key), "owner", 0, 0, :hot, 0, 5}
      ])

    decode_entry_fun = fn
      {^hot_key, "hot", 0, 0, :hot, 0, 3} -> [{hot_key, "hot", 0, hot}]
    end

    parent = self()

    read_many_fun = fn keys ->
      send(parent, {:durable_state_keys, keys})

      {:ok, [{:ok, LMDB.encode_value(Flow.encode_record(cold), 0)}]}
    end

    assert {:ok,
            %{
              ^hot_key => :terminal,
              ^cold_key => :active,
              ^missing_key => :missing
            }} =
             TerminalState.statuses_with_reader(
               keydir,
               [hot_key, cold_key, missing_key],
               decode_entry_fun,
               read_many_fun
             )

    assert_receive {:durable_state_keys, [^cold_key]}
    refute_receive {:durable_state_keys, _other}
  end

  test "terminal state resolution fails closed when the authoritative keydir disappears" do
    keydir = :ets.new(:terminal_state_missing_keydir, [:set])
    state_key = Keys.state_key("flow", "partition")
    :ets.delete(keydir)

    assert {:error, :authoritative_flow_state_unavailable} =
             TerminalState.statuses_with_reader(
               keydir,
               [state_key],
               fn _entry -> flunk("deleted keydir cannot contain an entry") end,
               fn _keys -> flunk("durable reads must not run without authoritative ownership") end
             )
  end

  defp record(id, partition_key, state) do
    %{
      id: id,
      type: "job",
      state: state,
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 1,
      next_run_at_ms: if(state == "queued", do: 10_000, else: 0),
      priority: 0,
      partition_key: partition_key,
      state_enter_seq: 1,
      root_flow_id: id
    }
  end
end
