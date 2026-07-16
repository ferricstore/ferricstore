defmodule Ferricstore.Flow.LMDBWriter.EnqueueControlTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.LMDBWriter.EnqueueControl

  setup do
    previous = Application.get_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops)

    on_exit(fn ->
      restore_env(:flow_lmdb_writer_max_enqueue_ops, previous)
    end)
  end

  test "queued operation reservations enforce an aggregate atomic cap" do
    instance_name = :"enqueue_control_#{System.unique_integer([:positive])}"
    ref = :atomics.new(3, signed: false)
    EnqueueControl.publish_enqueue_seq(instance_name, 0, ref)
    Application.put_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops, 3)

    assert {:ok, {^ref, 2}} = EnqueueControl.reserve_queued_ops(instance_name, 0, 2)
    assert {:error, :queue_full} = EnqueueControl.reserve_queued_ops(instance_name, 0, 2)
    assert :atomics.get(ref, 3) == 2

    assert :ok = EnqueueControl.release_queued_ops({ref, 2})
    assert {:ok, {^ref, 3}} = EnqueueControl.reserve_queued_ops(instance_name, 0, 3)
    assert :atomics.get(ref, 3) == 3
  end

  test "reservation tokens release the original writer generation" do
    instance_name = :"enqueue_generation_#{System.unique_integer([:positive])}"
    old_ref = :atomics.new(3, signed: false)
    new_ref = :atomics.new(3, signed: false)
    EnqueueControl.publish_enqueue_seq(instance_name, 0, old_ref)
    Application.put_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops, 3)

    assert {:ok, {^old_ref, 2} = token} =
             EnqueueControl.reserve_queued_ops(instance_name, 0, 2)

    EnqueueControl.publish_enqueue_seq(instance_name, 0, new_ref)
    assert :ok = EnqueueControl.release_queued_ops(token)
    assert :atomics.get(old_ref, 3) == 0
    assert :atomics.get(new_ref, 3) == 0
  end

  test "missing aggregate reservation state fails closed" do
    instance_name = :"enqueue_control_missing_#{System.unique_integer([:positive])}"

    assert {:error, :writer_not_started} =
             EnqueueControl.reserve_queued_ops(instance_name, 0, 1)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
