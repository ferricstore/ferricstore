defmodule Ferricstore.Raft.WalWriteFailureRestartTest do
  use ExUnit.Case, async: false

  @moduletag :shard_kill

  @machine {:module, Ferricstore.Test.KvMachine, %{}}
  @query_mfa {Ferricstore.Test.KvMachine, :identity, []}
  @ra_timeout 5_000

  defmodule ControlledWalIo do
    @mode_key {__MODULE__, :mode}
    @attempts_key {__MODULE__, :write_attempts}

    def reset do
      :persistent_term.put(@mode_key, :healthy)
      :persistent_term.put(@attempts_key, :counters.new(1, []))
      :ok
    end

    def delete do
      :persistent_term.erase(@mode_key)
      :persistent_term.erase(@attempts_key)
      :ok
    end

    def set_mode(mode) when mode in [:healthy, :fail] do
      :persistent_term.put(@mode_key, mode)
      :ok
    end

    def write_attempts do
      @attempts_key
      |> :persistent_term.get(nil)
      |> case do
        nil -> 0
        counter -> :counters.get(counter, 1)
      end
    end

    def open(path, commit_delay_us, pre_allocate_bytes, max_buffer_bytes) do
      :ferricstore_wal_nif.open(path, commit_delay_us, pre_allocate_bytes, max_buffer_bytes)
    end

    def write(handle, data) do
      :counters.add(:persistent_term.get(@attempts_key), 1, 1)

      case mode() do
        :healthy -> :ferricstore_wal_nif.write(handle, data)
        :fail -> {:error, :forced_wal_write_failure}
      end
    end

    def sync(handle, caller_pid, ref) do
      :ferricstore_wal_nif.sync(handle, caller_pid, ref)
    end

    def close(handle), do: :ferricstore_wal_nif.close(handle)
    def position(handle), do: :ferricstore_wal_nif.position(handle)
    def pread(handle, offset, len), do: :ferricstore_wal_nif.pread(handle, offset, len)

    defp mode do
      :persistent_term.get(@mode_key, :healthy)
    end
  end

  setup do
    ControlledWalIo.reset()

    uniq = :erlang.unique_integer([:positive])
    system = :"wal_write_failure_restart_#{uniq}"
    cluster_name = :"wal_write_failure_restart_cluster_#{uniq}"
    member = {:"wal_write_failure_restart_member_#{uniq}", node()}
    tmp = Path.join(System.tmp_dir!(), "wal_write_failure_restart_#{uniq}")
    ra_dir = Path.join(tmp, "ra")
    File.mkdir_p!(ra_dir)

    names = :ra_system.derive_names(system)

    config = %{
      name: system,
      names: names,
      data_dir: to_charlist(ra_dir),
      wal_data_dir: to_charlist(ra_dir),
      segment_max_entries: 4096,
      wal_max_batch_size: 4096,
      wal_compute_checksums: false,
      wal_pre_allocate: false,
      wal_io_module: ControlledWalIo,
      wal_commit_delay_us: 0
    }

    assert {:ok, _pid} = :ra_system.start(config)
    assert {:ok, [^member], []} = :ra.start_cluster(system, cluster_name, @machine, [member])

    on_exit(fn ->
      ControlledWalIo.set_mode(:healthy)
      safe_stop_server(system, member)
      safe_delete_server(system, member)
      Process.sleep(200)
      File.rm_rf!(tmp)
      ControlledWalIo.delete()
    end)

    %{member: member, names: names}
  end

  test "WAL write failure is not falsely acknowledged and pending command survives restart",
       %{
         member: member,
         names: names
       } do
    old_wal_pid = wait_for_registered(names.wal)
    wal_ref = Process.monitor(old_wal_pid)

    assert {:returned, {:ok, :ok, _leader}} =
             command_result(member, {:put, "baseline", "ok"}, @ra_timeout)

    ControlledWalIo.set_mode(:fail)

    failed_result = command_result(member, {:put, "resend_after_restart", "ok"}, 1_000)
    refute command_succeeded?(failed_result)

    assert_receive {:DOWN, ^wal_ref, :process, ^old_wal_pid, _reason}, 5_000
    assert ControlledWalIo.write_attempts() >= 50

    ControlledWalIo.set_mode(:healthy)
    new_wal_pid = wait_for_new_registered(names.wal, old_wal_pid)
    assert is_pid(new_wal_pid)

    assert_eventually(fn ->
      command_succeeded?(command_result(member, {:put, "after_restart", "ok"}, 2_000))
    end)

    state = wait_for_state(member, fn state -> Map.get(state, "after_restart") == "ok" end)

    assert Map.get(state, "baseline") == "ok"
    assert Map.get(state, "resend_after_restart") == "ok"
    assert Map.get(state, "after_restart") == "ok"
  end

  defp command_result(member, command, timeout) do
    {:returned, :ra.process_command(member, command, timeout)}
  catch
    :exit, reason -> {:exit, reason}
    kind, reason -> {kind, reason}
  end

  defp command_succeeded?({:returned, {:ok, :ok, _leader}}), do: true
  defp command_succeeded?(_result), do: false

  defp wait_for_registered(name, timeout \\ 5_000) do
    assert_eventually(
      fn ->
        case Process.whereis(name) do
          pid when is_pid(pid) -> {:ok, pid}
          nil -> false
        end
      end,
      timeout
    )
  end

  defp wait_for_new_registered(name, old_pid, timeout \\ 5_000) do
    assert_eventually(
      fn ->
        case Process.whereis(name) do
          pid when is_pid(pid) and pid != old_pid -> {:ok, pid}
          _ -> false
        end
      end,
      timeout
    )
  end

  defp wait_for_state(member, predicate, timeout \\ 5_000) do
    assert_eventually(
      fn ->
        case :ra.consistent_query(member, @query_mfa, 1_000) do
          {:ok, {_, state}, _leader} ->
            if predicate.(state), do: {:ok, state}, else: false

          {:ok, state, _leader} ->
            if predicate.(state), do: {:ok, state}, else: false

          _ ->
            false
        end
      end,
      timeout
    )
  end

  defp assert_eventually(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline, nil)
  end

  defp do_assert_eventually(fun, deadline, last_result) do
    case fun.() do
      {:ok, value} ->
        value

      true ->
        true

      other ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("condition did not become true, last result: #{inspect(other || last_result)}")
        else
          Process.sleep(50)
          do_assert_eventually(fun, deadline, other)
        end
    end
  end

  defp safe_stop_server(system, member) do
    try do
      :ra.stop_server(system, member)
    catch
      _, _ -> :ok
    end
  end

  defp safe_delete_server(system, member) do
    try do
      :ra.force_delete_server(system, member)
    catch
      _, _ -> :ok
    end
  end
end
