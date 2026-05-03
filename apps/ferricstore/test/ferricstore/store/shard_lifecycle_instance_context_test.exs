defmodule Ferricstore.Store.ShardLifecycleInstanceContextTest do
  @moduledoc false

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle

  test "recover_keydir replays log files by numeric file id" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_order_#{System.unique_integer([:positive])}"
      )

    shard_path = Path.join(tmp, "shard_0")
    File.mkdir_p!(shard_path)

    keydir = :ets.new(:"lifecycle_order_#{System.unique_integer([:positive])}", [:set, :public])

    on_exit(fn ->
      try do
        :ets.delete(keydir)
      rescue
        _ -> :ok
      end

      File.rm_rf!(tmp)
    end)

    key = "recover_numeric_order_key"

    assert {:ok, _} = NIF.v2_append_record(Path.join(shard_path, "99999.log"), key, "old", 0)
    assert {:ok, _} = NIF.v2_append_record(Path.join(shard_path, "100000.log"), key, "new", 0)

    ShardLifecycle.recover_keydir(shard_path, keydir, 0)

    assert [{^key, nil, 0, _lfu, 100_000, offset, value_size}] = :ets.lookup(keydir, key)
    assert value_size == byte_size("new")
    assert {:ok, "new"} = NIF.v2_pread_at(Path.join(shard_path, "100000.log"), offset)
  end

  test "recover_keydir during custom shard startup does not mutate default accounting" do
    default_ctx = FerricStore.Instance.get(:default)
    default_before = keydir_binary_total(default_ctx)

    ctx = build_instance()
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    log_path = Path.join(shard_path, "00000.log")
    key = "recover_custom_instance_" <> String.duplicate("k", 80)

    assert {:ok, {_offset, _value_size}} = NIF.v2_append_record(log_path, key, "value", 0)

    custom_before = keydir_binary_total(ctx)

    {:ok, pid} =
      Ferricstore.Store.Shard.start_link(
        index: 0,
        data_dir: ctx.data_dir,
        instance_ctx: ctx
      )

    on_exit(fn -> cleanup_instance(ctx, pid) end)

    assert keydir_binary_total(default_ctx) == default_before
    assert keydir_binary_total(ctx) > custom_before
  end

  defp build_instance do
    name = :"lifecycle_instance_#{System.unique_integer([:positive])}"

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_instance_#{System.unique_integer([:positive])}"
      )

    ctx =
      FerricStore.Instance.build(name,
        data_dir: data_dir,
        shard_count: 1,
        max_memory_bytes: 256 * 1024 * 1024,
        keydir_max_ram: 64 * 1024 * 1024
      )

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    ctx
  end

  defp cleanup_instance(ctx, pid) do
    if is_pid(pid) and Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    try do
      :ets.delete(elem(ctx.keydir_refs, 0))
    rescue
      _ -> :ok
    end

    try do
      :ets.delete(ctx.hotness_table)
    rescue
      _ -> :ok
    end

    try do
      :ets.delete(ctx.config_table)
    rescue
      _ -> :ok
    end

    FerricStore.Instance.cleanup(ctx.name)
    File.rm_rf!(ctx.data_dir)
  end

  defp keydir_binary_total(ctx) do
    1..ctx.shard_count
    |> Enum.reduce(0, fn idx, acc -> acc + :atomics.get(ctx.keydir_binary_bytes, idx) end)
  end
end
