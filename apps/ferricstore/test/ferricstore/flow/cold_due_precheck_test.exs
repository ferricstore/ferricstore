defmodule Ferricstore.Flow.ColdDuePrecheckTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.ColdDuePrecheck
  alias Ferricstore.Flow.LMDB

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-cold-due-precheck-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    %{path: path}
  end

  test "only an unrelated row or a schedule row with no park proves absence", %{path: path} do
    schedule_due = due_key("__ferricstore_schedule", "schedule")
    other_due = due_key("other-type", "other")

    assert :ok =
             LMDB.write_batch(path, [
               {:put, schedule_due, "flow:park:v1:missing-schedule"},
               {:put, other_due, "flow:park:v1:other"},
               {:put, "flow:park:v1:other", "present"}
             ])

    assert ColdDuePrecheck.empty_for_type?(path, "__ferricstore_schedule")
    refute ColdDuePrecheck.empty_for_type?(path, "other-type")

    assert :ok = LMDB.write_batch(path, [LMDB.flush_in_progress_put_op()])
    refute ColdDuePrecheck.empty_for_type?(path, "__ferricstore_schedule")
    assert :ok = LMDB.write_batch(path, [LMDB.flush_in_progress_delete_op()])

    assert :ok = LMDB.write_batch(path, [{:put, "flow:park:v1:missing-schedule", "present"}])
    refute ColdDuePrecheck.empty_for_type?(path, "__ferricstore_schedule")
  end

  test "an incomplete scan, invalid key, or missing environment cannot prove absence", %{
    path: path
  } do
    refute ColdDuePrecheck.empty_for_type?(path, "__ferricstore_schedule")

    rows =
      for i <- 1..33 do
        {:put, due_key("other-type", "#{i}"), "flow:park:v1:#{i}"}
      end

    assert :ok = LMDB.write_batch(path, rows)
    refute ColdDuePrecheck.empty_for_type?(path, "__ferricstore_schedule")

    assert :ok = LMDB.write_batch(path, Enum.map(rows, fn {:put, key, _} -> {:delete, key} end))
    assert :ok = LMDB.write_batch(path, [{:put, LMDB.cold_due_prefix() <> "bad", "park"}])
    refute ColdDuePrecheck.empty_for_type?(path, "__ferricstore_schedule")
  end

  defp due_key(type, id) do
    LMDB.cold_due_key(
      type: type,
      state: "queued",
      partition_key: "partition",
      priority: 0,
      due_at_ms: 1,
      flow_id: id,
      version: 1
    )
  end
end
