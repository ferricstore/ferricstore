defmodule Ferricstore.Flow.LMDBWriter.ControlTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Flow.LMDBWriter.Control
  alias Ferricstore.Flow.LMDBWriter.Registry

  defmodule TestWriter do
    use GenServer

    @impl true
    def init(reply), do: {:ok, reply}

    @impl true
    def handle_call(request, _from, reply) when request in [:suspend, :discard],
      do: {:reply, reply, reply}
  end

  test "suspend_all and discard_all return a shard failure" do
    instance_name = unique_instance_name("control_failure")
    on_exit(fn -> Registry.clear_instance_suspended(instance_name) end)

    start_test_writer(instance_name, 0, :ok)
    start_test_writer(instance_name, 1, {:error, :flush_failed})

    assert Control.suspend_all(instance_name, 2, flush: true) == {:error, :flush_failed}
    assert Control.discard_all(instance_name, 2) == {:error, :flush_failed}
  end

  test "suspend returns a pending projection flush failure" do
    instance_name = unique_instance_name("suspend_flush")
    data_dir = Path.join(System.tmp_dir!(), Atom.to_string(instance_name))
    key = "flow:{flow:suspend-failure}:state:a"
    keydir = :ets.new(:flow_lmdb_suspend_failure_keydir, [:set, :public])

    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_source_retries = Application.get_env(:ferricstore, :flow_lmdb_source_pending_retries)

    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_source_pending_retries, 0)

    on_exit(fn ->
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_source_pending_retries, old_source_retries)
      Registry.clear_instance_suspended(instance_name)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    true = :ets.insert(keydir, {key, nil, 0, 0, :pending, 0, 0})

    instance_ctx = %{
      name: instance_name,
      keydir_refs: {keydir},
      flow_lmdb_writer_flush_failures: :atomics.new(1, signed: false)
    }

    start_supervised!(
      {LMDBWriter,
       shard_index: 0,
       data_dir: data_dir,
       instance_ctx: instance_ctx,
       instance_name: instance_name}
    )

    assert :ok = LMDBWriter.enqueue(instance_name, 0, [{:project_kv_from_source, key}])
    assert LMDBWriter.suspend(instance_name, 0) == {:error, {:source_pending, key}}
  end

  defp start_test_writer(instance_name, shard_index, reply) do
    name = Registry.name(instance_name, shard_index)
    {:ok, pid} = GenServer.start(TestWriter, reply, name: name)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    pid
  end

  defp unique_instance_name(prefix),
    do: String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
