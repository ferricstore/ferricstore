defmodule Ferricstore.Flow.Query.CursorKeyStoreTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Ferricstore.Flow.Query.CursorKeyStore

  setup do
    Process.flag(:trap_exit, true)
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_cursor_keys_#{suffix}")
    ctx = %{name: :"cursor_key_instance_#{suffix}", data_dir: data_dir}

    on_exit(fn -> File.rm_rf!(data_dir) end)
    %{ctx: ctx, data_dir: data_dir, suffix: suffix}
  end

  test "atomically creates a private key and reloads it across restarts", context do
    name = :"cursor_key_store_a_#{context.suffix}"
    assert {:ok, pid} = start_store(context.ctx, name)
    assert {:ok, first} = CursorKeyStore.key(name)
    assert byte_size(first) == 32

    path = CursorKeyStore.default_path(context.ctx)
    assert {:ok, %File.Stat{type: :regular, size: 32, mode: mode}} = File.stat(path)
    assert (mode &&& 0o077) == 0

    assert :ok = GenServer.stop(pid)
    assert {:ok, second_pid} = start_store(context.ctx, name)
    assert {:ok, ^first} = CursorKeyStore.key(name)
    assert :ok = GenServer.stop(second_pid)
  end

  test "uses a configured shared key without writing local secret material", context do
    name = :"cursor_key_store_b_#{context.suffix}"
    shared = :binary.copy(<<0x5A>>, 32)
    encoded = Base.url_encode64(shared, padding: false)

    assert {:ok, pid} = start_store(context.ctx, name, key: encoded)
    assert {:ok, ^shared} = CursorKeyStore.key(name)
    refute File.exists?(CursorKeyStore.default_path(context.ctx))
    assert :ok = GenServer.stop(pid)
  end

  test "concurrent first starts converge on the one durably published key", context do
    parent = self()

    tasks =
      for worker <- 1..16 do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :start ->
              name = :"cursor_key_store_race_#{context.suffix}_#{worker}"
              {:ok, pid} = start_store(context.ctx, name)
              {:ok, key} = CursorKeyStore.key(name)
              {pid, key}
          end
        end)
      end

    workers =
      for _worker <- tasks do
        assert_receive {:ready, worker}
        worker
      end

    Enum.each(workers, &send(&1, :start))
    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert results |> Enum.map(&elem(&1, 1)) |> Enum.uniq() |> length() == 1
    Enum.each(results, fn {pid, _key} -> GenServer.stop(pid) end)
  end

  test "fails closed instead of rotating corrupt or exposed persisted keys", context do
    path = CursorKeyStore.default_path(context.ctx)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "short")
    File.chmod!(path, 0o600)

    assert {:error, :invalid_query_cursor_key_file} =
             start_store(context.ctx, :"cursor_key_store_c_#{context.suffix}")

    File.write!(path, :binary.copy(<<1>>, 32))
    File.chmod!(path, 0o644)

    assert {:error, :insecure_query_cursor_key_file} =
             start_store(context.ctx, :"cursor_key_store_d_#{context.suffix}")
  end

  test "never follows a persisted key symlink", context do
    path = CursorKeyStore.default_path(context.ctx)
    target = Path.join(context.data_dir, "outside.key")
    File.mkdir_p!(Path.dirname(path))
    File.write!(target, :binary.copy(<<7>>, 32))
    File.chmod!(target, 0o600)
    File.ln_s!(target, path)

    assert {:error, :invalid_query_cursor_key_file} =
             start_store(context.ctx, :"cursor_key_store_symlink_#{context.suffix}")
  end

  test "rejects malformed context, key configuration, and unavailable servers", context do
    assert {:error, :invalid_query_cursor_key} =
             start_store(context.ctx, :"cursor_key_store_e_#{context.suffix}", key: "bad")

    assert {:error, :invalid_query_cursor_key_context} =
             CursorKeyStore.start_link(instance_ctx: %{name: :bad, data_dir: ""})

    assert {:error, :query_storage_unavailable} = CursorKeyStore.key(:not_started_cursor_store)
  end

  defp start_store(ctx, name, opts \\ []) do
    CursorKeyStore.start_link([instance_ctx: ctx, name: name] ++ opts)
  end
end
