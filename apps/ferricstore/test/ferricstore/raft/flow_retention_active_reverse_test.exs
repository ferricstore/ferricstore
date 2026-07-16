defmodule Ferricstore.Raft.FlowRetentionActiveReverseTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Raft.StateMachine

  test "hibernated timeout cleanup accepts only the exact active-timeout reverse plan" do
    state_key = "state-key"
    deadline_ms = 123
    reverse_key = LMDB.active_by_state_key_key(state_key)

    active_key =
      LMDB.active_index_key(Keys.active_timeout_index_key(), state_key, deadline_ms)

    valid =
      {:ok,
       [
         {:compare, reverse_key, "reverse-value"},
         {:compare, active_key, "active-value"},
         {:delete, reverse_key},
         {:delete, active_key}
       ]}

    assert {:ok,
            [
              {:compare, ^reverse_key, "reverse-value"},
              {:compare, ^active_key, "active-value"},
              {:delete, ^reverse_key},
              {:delete, ^active_key}
            ]} =
             StateMachine.__flow_hibernated_timeout_active_index_delete_ops_result_for_test__(
               state_key,
               deadline_ms,
               valid
             )

    other_active_key = LMDB.active_index_key("other-index", state_key, deadline_ms)

    assert {:error, :invalid_active_index_reverse} =
             StateMachine.__flow_hibernated_timeout_active_index_delete_ops_result_for_test__(
               state_key,
               deadline_ms,
               {:ok,
                [
                  {:compare, reverse_key, "reverse-value"},
                  {:compare, other_active_key, "active-value"},
                  {:delete, reverse_key},
                  {:delete, other_active_key}
                ]}
             )
  end

  test "hibernated timeout cleanup fails closed for missing, corrupt, and unreadable reverses" do
    state_key = "state-key"
    deadline_ms = 123
    reverse_key = LMDB.active_by_state_key_key(state_key)

    assert {:error, :missing_active_index_reverse} =
             StateMachine.__flow_hibernated_timeout_active_index_delete_ops_result_for_test__(
               state_key,
               deadline_ms,
               {:ok, [{:compare_missing, reverse_key}]}
             )

    assert {:error, :invalid_active_index_reverse} =
             StateMachine.__flow_hibernated_timeout_active_index_delete_ops_result_for_test__(
               state_key,
               deadline_ms,
               {:error, :invalid_active_index_reverse}
             )

    assert {:error, :active_index_reverse_read_failed} =
             StateMachine.__flow_hibernated_timeout_active_index_delete_ops_result_for_test__(
               state_key,
               deadline_ms,
               {:error, :busy}
             )

    assert {:error, :active_index_reverse_read_failed} =
             StateMachine.__flow_hibernated_timeout_active_index_delete_ops_result_for_test__(
               state_key,
               deadline_ms,
               {:error, {:invalid_active_index_reverse_read, :unexpected}}
             )
  end
end
