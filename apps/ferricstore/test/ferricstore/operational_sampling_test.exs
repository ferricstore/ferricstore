defmodule Ferricstore.OperationalSamplingTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.OperationalLimits

  setup do
    keys = [:operational_memory_limit_bytes, :max_memory_bytes]
    previous = Enum.map(keys, &{&1, Application.fetch_env(:ferricstore, &1)})

    on_exit(fn ->
      for {key, value} <- previous do
        case value do
          {:ok, value} -> Application.put_env(:ferricstore, key, value)
          :error -> Application.delete_env(:ferricstore, key)
        end
      end
    end)

    :ok
  end

  test "an explicit memory limit does not probe host capacity" do
    calls =
      traced_calls(fn ->
        assert OperationalLimits.memory_limit_bytes(memory_bytes: 123) == 123
      end)

    refute Enum.any?(calls, &match?({Ferricstore.MemoryGuard, :detect_memory_limit, _}, &1))
  end

  test "configured memory limits retain precedence and skip detection" do
    Application.put_env(:ferricstore, :operational_memory_limit_bytes, 200)
    Application.put_env(:ferricstore, :max_memory_bytes, 300)

    calls =
      traced_calls(fn ->
        assert OperationalLimits.memory_limit_bytes(memory_bytes: 100) == 100
        assert OperationalLimits.memory_limit_bytes(memory_bytes: 0) == 200
        Application.put_env(:ferricstore, :operational_memory_limit_bytes, -1)
        assert OperationalLimits.memory_limit_bytes(memory_bytes: nil) == 300
      end)

    refute Enum.any?(calls, &match?({Ferricstore.MemoryGuard, :detect_memory_limit, _}, &1))
  end

  test "invalid configured limits still invoke fresh host detection" do
    Application.put_env(:ferricstore, :operational_memory_limit_bytes, 0)
    Application.put_env(:ferricstore, :max_memory_bytes, -1)

    calls =
      traced_calls(fn ->
        assert OperationalLimits.memory_limit_bytes(memory_bytes: :invalid) > 0
      end)

    assert Enum.any?(calls, &match?({Ferricstore.MemoryGuard, :detect_memory_limit, []}, &1))
  end

  test "detected disk capacity does not start a subprocess" do
    calls =
      traced_calls(fn ->
        snapshot =
          OperationalLimits.snapshot(data_dir: System.tmp_dir!(), memory_bytes: 123, rss_bytes: 1)

        assert snapshot.disk.total_bytes > 0
        assert snapshot.disk.available_bytes > 0
        assert snapshot.disk.used_bytes >= 0
      end)

    refute Enum.any?(calls, &match?({System, :cmd, _}, &1))
  end

  test "native disk capacity handles invalid paths without treating them as healthy" do
    assert {:error, _} =
             NIF.disk_capacity("/ferricstore-missing-#{System.unique_integer([:positive])}")

    assert {:error, _} = NIF.disk_capacity(<<0>>)
  end

  test "capacity detection resolves a not-yet-created data directory" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-missing-#{System.unique_integer([:positive])}/data"
      )

    snapshot = OperationalLimits.snapshot(data_dir: path, memory_bytes: 123, rss_bytes: 1)
    assert snapshot.disk.path == Path.expand(System.tmp_dir!())
    assert snapshot.disk.total_bytes > 0
  end

  test "unknown capacity is not reported as healthy" do
    snapshot =
      OperationalLimits.snapshot(
        memory_bytes: 123,
        rss_bytes: 1,
        disk: %{total_bytes: 0, available_bytes: 0}
      )

    assert snapshot.disk.level == :unknown
    assert snapshot.disk.used_ratio == nil
  end

  defp traced_calls(fun) do
    parent = self()

    worker =
      spawn_link(fn ->
        receive do
          :run ->
            fun.()
            send(parent, :finished)
            receive do: (:stop -> :ok)
        end
      end)

    session = :trace.session_create(:operational_sampling_test, self(), [])

    try do
      Code.ensure_loaded!(System)
      Code.ensure_loaded!(Ferricstore.MemoryGuard)
      assert :trace.function(session, {System, :cmd, 3}, true, [:local]) > 0

      assert :trace.function(session, {Ferricstore.MemoryGuard, :detect_memory_limit, 0}, true, [
               :local
             ]) > 0

      :trace.process(session, worker, true, [:call])
      send(worker, :run)
      assert_receive :finished, 5_000
      ref = :trace.delivered(session, worker)
      assert_receive {:trace_delivered, ^worker, ^ref}, 5_000
      drain_calls(worker, [])
    after
      :trace.session_destroy(session)
      send(worker, :stop)
    end
  end

  defp drain_calls(worker, acc) do
    receive do
      {:trace, ^worker, :call, call} -> drain_calls(worker, [call | acc])
    after
      0 -> acc
    end
  end
end
